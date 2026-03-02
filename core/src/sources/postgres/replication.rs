use anyhow::Result;
use persist::event::EventRaw;
use std::env;
use std::sync::LazyLock;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio_postgres::Client;
use tokio_postgres::error::SqlState;
use types::types::Diff;

use differential_dataflow::{AsCollection, VecCollection};
use futures_util::StreamExt;
use timely::container::CapacityContainerBuilder;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::{Scope, Stream};

use postgres_replication::LogicalReplicationStream;
use postgres_replication::protocol::{LogicalReplicationMessage, ReplicationMessage, TupleData};
use tokio_postgres::types::PgLsn;

use crate::dataflow::DataflowConfig;
use crate::operators::builder_async::{AsyncOperatorBuilder, Event};
use crate::sources::postgres::connection::PostgresConnection;

const ONE: Diff = 1;
const MINUS_ONE: Diff = -1;

static PG_EPOCH: LazyLock<SystemTime> =
    LazyLock::new(|| UNIX_EPOCH + Duration::from_secs(946_684_800));

pub fn replication<G>(
    scope: &G,
    config: DataflowConfig,
    lsn_stream: &Stream<G, u64>,
) -> VecCollection<G, EventRaw, Diff>
where
    G: Scope<Timestamp = u64>,
{
    let mut builder = AsyncOperatorBuilder::new("PgReplicationDataflow".to_string(), scope.clone());

    let (raw_handle, raw_stream) =
        builder.new_output::<CapacityContainerBuilder<Vec<(EventRaw, u64, Diff)>>>();

    let mut lsn_input = builder.new_disconnected_input(lsn_stream, Pipeline);

    let worker_id = scope.index();

    let _ = builder.build_fallible(move |capabilities| {
        Box::pin(async move {
            let [raw_cap]: &mut [_; 1] = capabilities.try_into().unwrap();

            if worker_id != 0 {
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

            tracing::info!(
                "PgReplication started (slot={}, publication={} resume_lsn={})",
                slot,
                publication,
                resume_lsn_pg
            );

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

            raw_cap.downgrade([resume_lsn_u64]);

            loop {
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

                    Some(message) = logical_stream.next() => {
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
                                            raw_cap.downgrade([data_upper]);
                                        }
                                    }
                                    LogicalReplicationMessage::Insert(insert) => {
                                        let Some(tx_lsn) = commit_lsn_u64 else { continue; };
                                        let Some(event) = parse_event_raw(insert.tuple().tuple_data()) else {
                                            continue;
                                        };

                                        raw_handle.give(&raw_cap[0], (event, tx_lsn, ONE));
                                    }
                                    LogicalReplicationMessage::Delete(delete) => {
                                        let Some(tx_lsn) = commit_lsn_u64 else { continue; };
                                        let Some(old) = delete.old_tuple() else { continue; };
                                        let Some(event) = parse_event_raw(old.tuple_data()) else {
                                            continue;
                                        };

                                        raw_handle.give(&raw_cap[0], (event, tx_lsn, MINUS_ONE));
                                    }
                                    LogicalReplicationMessage::Update(_) => {}
                                    _ => {}
                                }
                            }

                            Ok(ReplicationMessage::PrimaryKeepAlive(keepalive)) => {}

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

    raw_stream.as_collection()
}

fn parse_u64(bytes: &[u8]) -> Option<u64> {
    atoi::atoi(bytes)
}

fn parse_u16(bytes: &[u8]) -> Option<u16> {
    atoi::atoi(bytes)
}

fn tuple_bytes(data: &TupleData) -> Option<&[u8]> {
    match data {
        TupleData::Text(bytes) => Some(bytes),
        TupleData::Null => Some(b""),
        TupleData::UnchangedToast | TupleData::Binary(_) => None,
    }
}

fn parse_event_raw(tuple: &[TupleData]) -> Option<EventRaw> {
    let id = parse_hex_32(tuple_bytes(tuple.first()?)?)?;
    let pubkey = parse_hex_32(tuple_bytes(tuple.get(1)?)?)?;
    let created_at = parse_u64(tuple_bytes(tuple.get(2)?)?)?;
    let kind = parse_u16(tuple_bytes(tuple.get(3)?)?)?;
    let tags_json = match tuple.get(4)? {
        TupleData::Text(bytes) => bytes.to_vec(),
        TupleData::Null => b"[]".to_vec(),
        TupleData::UnchangedToast | TupleData::Binary(_) => return None,
    };
    let content = match tuple.get(5)? {
        TupleData::Text(bytes) => bytes.to_vec(),
        TupleData::Null => Vec::new(),
        TupleData::UnchangedToast | TupleData::Binary(_) => return None,
    };
    let mut sig = [0u8; 64];
    hex::decode_to_slice(tuple_bytes(tuple.get(6)?)?, &mut sig).ok()?;

    Some(EventRaw {
        id,
        pubkey,
        created_at,
        kind,
        tags_json,
        content,
        sig,
    })
}

fn parse_hex_32(bytes: &[u8]) -> Option<[u8; 32]> {
    let text = std::str::from_utf8(bytes).ok()?.trim();
    let mut out = [0u8; 32];
    hex::decode_to_slice(text, &mut out).ok()?;
    Some(out)
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
