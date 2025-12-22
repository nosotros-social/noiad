use anyhow::{Error, Result, anyhow};
use std::env;
use std::sync::LazyLock;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio_postgres::Client;
use tokio_postgres::error::SqlState;

use differential_dataflow::{AsCollection, VecCollection};
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use timely::container::CapacityContainerBuilder;
use timely::dataflow::channels::pact::{Exchange, Pipeline};
use timely::dataflow::operators::Operator;
use timely::dataflow::{Scope, Stream};

use postgres_replication::LogicalReplicationStream;
use postgres_replication::protocol::{LogicalReplicationMessage, ReplicationMessage, TupleData};
use tokio_postgres::types::PgLsn;

use crate::config::Config;
use crate::event::{EventRow, Tags};
use crate::operators::builder_async::{AsyncOperatorBuilder, Event};
use crate::operators::probe::Handle;
use crate::sources::postgres::connection::PostgresConnection;
use crate::types::Diff;

const ONE: Diff = 1;
const MINUS_ONE: Diff = -1;

static PG_EPOCH: LazyLock<SystemTime> =
    LazyLock::new(|| UNIX_EPOCH + Duration::from_secs(946_684_800));

pub fn replication<G>(
    scope: &G,
    config: Config,
    lsn_stream: &Stream<G, u64>,
    probe: &Handle<u64>,
) -> VecCollection<G, EventRow, Diff>
where
    G: Scope<Timestamp = u64>,
{
    let mut builder = AsyncOperatorBuilder::new("PgReplicationDataflow".to_string(), scope.clone());

    let (raw_handle, raw_stream) =
        builder.new_output::<CapacityContainerBuilder<Vec<(EventParts, u64, Diff)>>>();

    let mut lsn_input = builder.new_disconnected_input(lsn_stream, Pipeline);

    let probe = probe.clone();

    let _ = builder.build_fallible(move |capabilities| {
        Box::pin(async move {
            let [raw_cap]: &mut [_; 1] = capabilities.try_into().unwrap();

            if config.worker_id != 0 {
                return Ok::<(), anyhow::Error>(());
            }

            let publication = env::var("DB_PUBLICATION").expect("DB_PUBLICATION must be set");
            let slot = env::var("DB_REPLICATION_SLOT").unwrap_or_else(|_| "slot".to_string());

            let replication_client = PostgresConnection::from_env()
                .connect_replication()
                .await
                .unwrap();

            ensure_replication_slot(&replication_client, &slot).await.unwrap();

            let mut resume_lsn_u64 = 0u64;
            while let Some(event) = lsn_input.next().await {
                match event {
                    Event::Data(_, data) => {
                        resume_lsn_u64 = *data.first().unwrap();
                    }
                    Event::Progress(_) => {}
                }
                if resume_lsn_u64 != 0 {
                    break;
                }
            }

            let resume_lsn_pg = PgLsn::from(resume_lsn_u64);

            let query = format!(
                "START_REPLICATION SLOT \"{}\" LOGICAL {} \
                 (\"proto_version\" '1', \"publication_names\" '{}')",
                slot, resume_lsn_pg, publication
            );

            let copy_stream = replication_client
                .copy_both_simple(&query)
                .await
                .unwrap();

            let logical_stream = LogicalReplicationStream::new(copy_stream);
            let mut logical_stream = std::pin::pin!(logical_stream);

            let mut keepalive_timer = tokio::time::interval(Duration::from_secs(1));
            keepalive_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

            let mut commit_lsn_u64: Option<u64> = None;
            let mut data_upper = resume_lsn_u64;

            tracing::info!(
                "PgReplication started (slot={}, publication={})",
                slot,
                publication
            );

            let mut total_inserted = 0;
            let mut total_deleted = 0;

            let mut current_round = resume_lsn_u64;
            let mut last_flushed_round: Option<u64> = None;

            let mut pending: Vec<(EventParts, Diff)> = Vec::new();
            let mut pending_flush = false;

            // Let the initial capability from snapshot through
            raw_cap.downgrade([current_round]);

            loop {
                let at_capacity = pending.len() >= config.replication_max_pending;
                let downstream_ready = match last_flushed_round {
                    None => true, 
                    Some(prev) => probe.with_frontier(|f| !f.less_equal(&prev)),
                };
                if pending_flush && downstream_ready && !pending.is_empty() {
                    tracing::info!(
                        "flushing round {} ({} events) -> next round {}",
                        current_round,
                        pending.len(),
                        data_upper,
                    );

                    for (parts, diff) in pending.drain(..) {
                        raw_handle.give(&raw_cap[0], (parts, current_round, diff));
                    }

                    last_flushed_round = Some(current_round);
                    current_round = data_upper;
                    raw_cap.downgrade([current_round]);
                    pending_flush = false;
                }

                tokio::select! {
                    biased;

                    _ = keepalive_timer.tick() => {
                        let ts: i64 = PG_EPOCH.elapsed().unwrap().as_micros().try_into().unwrap();
                        let lsn = PgLsn::from(data_upper);
                        logical_stream
                            .as_mut()
                            .standby_status_update(lsn, lsn, lsn, ts, 1)
                            .await?;
                    },

                    _ = probe.progressed(), if at_capacity => {},

                    Some(message) = logical_stream.next(), if !at_capacity => {
                        match message {
                            Ok(ReplicationMessage::XLogData(xlog)) => {
                                let logical = xlog.into_data();

                                match logical {
                                    LogicalReplicationMessage::Relation(_) => {}
                                    LogicalReplicationMessage::Begin(begin) => {
                                        commit_lsn_u64 = Some(begin.final_lsn());
                                    }
                                    LogicalReplicationMessage::Commit(_) => {
                                        if let Some(lsn) = commit_lsn_u64.take() {
                                            data_upper = lsn + 1;
                                            pending_flush = true;
                                            tracing::info!(
                                                "Worker {}] replication commit (inserted: {}, deleted: {}, LSN: {})",
                                                config.worker_id,
                                                total_inserted,
                                                total_deleted,
                                                lsn
                                            );
                                            total_inserted = 0;
                                            total_deleted = 0;
                                        }
                                    }
                                    LogicalReplicationMessage::Insert(insert) => {
                                        if  commit_lsn_u64.is_none() {
                                            continue
                                        };
                                        let Some(parts) = EventParts::from_tuple_fixed(insert.tuple().tuple_data()) else {
                                            continue
                                        };

                                        total_inserted += 1;
                                        pending.push((parts, ONE));
                                    }
                                    LogicalReplicationMessage::Delete(delete) => {
                                        if  commit_lsn_u64.is_none() {
                                            continue
                                        };
                                        let Some(old) = delete.old_tuple() else { continue };
                                        let Some(parts) = EventParts::from_tuple_fixed(old.tuple_data()) else {
                                            continue
                                        };

                                        total_deleted += 1;
                                        pending.push((parts, MINUS_ONE));
                                    }
                                    LogicalReplicationMessage::Update(_) => {}
                                    _ => {}
                                }
                            }

                            Ok(ReplicationMessage::PrimaryKeepAlive(keepalive)) => {
                                let wal_end: u64 = keepalive.wal_end();
                                if wal_end > data_upper {
                                    data_upper = wal_end;
                                }
                            }

                            Err(err) => {
                                tracing::error!("PgReplication error: {:?}", err);
                                return Err(anyhow::Error::new(err));
                            }
                            _ => {}
                        }
                    },

                    else => {
                        break;
                    }
                }
            }

            Ok::<(), anyhow::Error>(())
        })
    });

    // Decode replication data in multiple workers
    let mut next_worker = (0..(scope.peers() as u64))
        .flat_map(|w| std::iter::repeat_n(w, 1000))
        .cycle();
    let round_robin = Exchange::new(move |_| next_worker.next().unwrap());

    raw_stream
        .unary(round_robin, "PgReplicationDecode", |_, _| {
            move |input, output| {
                input.for_each_time(|cap, data| {
                    let mut session = output.session(&cap);
                    for (event_parts, time, diff) in data.flat_map(|batch| batch.drain(..)) {
                        match EventRow::try_from(event_parts) {
                            Ok(event) => {
                                session.give((event, time, diff));
                            }
                            Err(err) => {
                                tracing::warn!(
                                    "failed to decode EventParts at {}: {:?}",
                                    time,
                                    err
                                );
                            }
                        }
                    }
                });
            }
        })
        .as_collection()
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
struct EventParts {
    id: Vec<u8>,
    pubkey: Vec<u8>,
    created_at: Vec<u8>,
    kind: Vec<u8>,
    tags: Vec<u8>,
}

impl EventParts {
    fn from_tuple_fixed(tuple: &[TupleData]) -> Option<Self> {
        let id = tuple_text(tuple.first()?)?;
        let pubkey = tuple_text(tuple.get(1)?)?;
        let created_at = tuple_text(tuple.get(2)?)?;
        let kind = tuple_text(tuple.get(3)?)?;
        let tags = tuple_text(tuple.get(4)?)?;

        Some(Self {
            id: id.to_vec(),
            pubkey: pubkey.to_vec(),
            created_at: created_at.to_vec(),
            kind: kind.to_vec(),
            tags: tags.to_vec(),
        })
    }

    fn created_at_u64(&self) -> Result<u64> {
        let text = std::str::from_utf8(&self.created_at)?;
        atoi::atoi::<u64>(text.as_bytes()).ok_or_else(|| anyhow!("invalid created_at"))
    }

    fn kind_u16(&self) -> Result<u16> {
        let text = std::str::from_utf8(&self.kind)?;
        atoi::atoi::<u16>(text.as_bytes()).ok_or_else(|| anyhow!("invalid kind"))
    }

    fn id_32(&self) -> Result<[u8; 32]> {
        parse_hex_32(&self.id)
    }

    fn pubkey_32(&self) -> Result<[u8; 32]> {
        parse_hex_32(&self.pubkey)
    }
}

impl TryFrom<EventParts> for EventRow {
    type Error = Error;

    fn try_from(parts: EventParts) -> Result<Self> {
        let id = parts.id_32()?;
        let pubkey = parts.pubkey_32()?;
        let created_at = parts.created_at_u64()?;

        let kind_value = parts.kind_u16()?;
        let kind = nostr_sdk::Kind::from_u16(kind_value);
        let tags = Tags::parse_from_bytes(&parts.tags);

        Ok(EventRow {
            id,
            pubkey,
            created_at,
            kind,
            tags,
        })
    }
}

fn parse_hex_32(bytes: &[u8]) -> Result<[u8; 32]> {
    let text = std::str::from_utf8(bytes)?.trim();
    let mut out = [0u8; 32];
    hex::decode_to_slice(text, &mut out).map_err(|_| anyhow!("invalid 32-byte hex value"))?;
    Ok(out)
}

fn tuple_text(data: &TupleData) -> Option<&[u8]> {
    match data {
        TupleData::Text(bytes) => Some(bytes),
        TupleData::Null => Some(b""),
        TupleData::UnchangedToast => None,
        TupleData::Binary(_) => None,
    }
}

pub async fn ensure_replication_slot(client: &Client, slot_name: &str) -> Result<()> {
    let query = format!(
        "CREATE_REPLICATION_SLOT {} LOGICAL \"pgoutput\" NOEXPORT_SNAPSHOT",
        slot_name,
    );

    match client.simple_query(&query).await {
        Ok(_) => Ok(()),
        Err(e) if e.code() == Some(&SqlState::DUPLICATE_OBJECT) => {
            tracing::trace!("replication slot {} already existed", slot_name);
            Ok(())
        }
        Err(e) => Err(e.into()),
    }
}
