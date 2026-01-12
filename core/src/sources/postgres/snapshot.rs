use std::env;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Error, Result, anyhow};
use differential_dataflow::{AsCollection, VecCollection};
use futures::TryStreamExt;
use timely::dataflow::Stream;
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::Operator;
use timely::{container::CapacityContainerBuilder, dataflow::Scope};
use tokio_postgres::SimpleQueryMessage;
use tokio_postgres::types::PgLsn;
use types::event::EventRaw;

use crate::config::Config;
use crate::operators::builder_async::AsyncOperatorBuilder;
use crate::sources::postgres::connection::PostgresConnection;
use crate::sources::postgres::parser::{CopyParser, quote_ident};
use crate::sources::postgres::utils::get_publication_info;

pub fn snapshot<G>(scope: &G, config: Config) -> (VecCollection<G, EventRaw>, Stream<G, u64>)
where
    G: Scope<Timestamp = u64>,
{
    let mut builder = AsyncOperatorBuilder::new("PgSnapshotDataflow".to_string(), scope.clone());

    let (raw_handle, raw_stream) = builder.new_output::<CapacityContainerBuilder<Vec<Vec<u8>>>>();
    let (lsn_handle, lsn_stream) = builder.new_output::<CapacityContainerBuilder<Vec<u64>>>();

    let _ = builder.build_fallible(move |capabilities| {
        Box::pin(async move {
            let [raw_cap, lsn_cap]: &mut [_; 2] = capabilities.try_into().unwrap();

            let is_snapshot_leader = config.worker_id == 0;
            if is_snapshot_leader {
                if let Some(checkpoint) = config.persist.load_checkpoint()? {
                    tracing::info!(
                        "[Worker {}] POSTGRES Snapshot skipped, loaded checkpoint at LSN {}",
                        config.worker_id,
                        checkpoint
                    );
                    lsn_handle.give(&lsn_cap[0], checkpoint);
                    return Ok(());
                }
                let client = PostgresConnection::from_env().connect().await?;
                let db_publication =
                    env::var("DB_PUBLICATION").expect("DB_PUBLICATION must be set");
                let tables = get_publication_info(&client, &db_publication).await?;

                if tables.is_empty() {
                    panic!(
                        "No tables found in publication {}. \n\
                        Create a postgres publication with CREATE PUBLICATION <name> FOR TABLE ...",
                        db_publication
                    );
                }

                let replication_client =
                    PostgresConnection::from_env().connect_replication().await?;
                let snapshot_lsn = export_snapshot(&replication_client, config.worker_id).await?;
                lsn_handle.give(&lsn_cap[0], snapshot_lsn);

                for table in tables {
                    let namespace = quote_ident(&table.schema);
                    let table_name = quote_ident(&table.name);
                    let query = format!(
                        "COPY (
                            SELECT id, pubkey, created_at, kind, tags 
                            FROM {namespace}.{table_name}
                        )
                        TO STDOUT (FORMAT TEXT, DELIMITER '\t')"
                    );

                    let mut stream = std::pin::pin!(client.copy_out_simple(&query).await?);

                    let started = std::time::Instant::now();
                    let mut counter = 0usize;
                    while let Some(bytes) = stream.try_next().await? {
                        counter += 1;
                        raw_handle.give(&raw_cap[0], bytes.to_vec());
                    }

                    client.simple_query("COMMIT").await?;
                    tracing::info!(
                        "POSTGRES Snapshot completed in {:?} total: {}",
                        started.elapsed(),
                        counter
                    );
                }
            }

            Ok::<(), Error>(())
        })
    });

    // Decode snapshot data in multiple workers
    let mut next_worker = (0..(scope.peers() as u64))
        .flat_map(|w| std::iter::repeat_n(w, 1000))
        .cycle();
    let round_robin = Exchange::new(move |_| next_worker.next().unwrap());

    let snapshot_updates: VecCollection<_, EventRaw> = raw_stream
        .unary(round_robin, "PgSnapshotDecode", |_, _| {
            move |input, output| {
                input.for_each_time(|time, data| {
                    let mut session = output.session(&time);
                    for bytes in data.flat_map(|data| data.drain(..)) {
                        let decoder = CopyParser::new(&bytes, b'\t');
                        for event in decoder.iter_rows() {
                            session.give((event, *time, 1isize));
                        }
                    }
                });
            }
        })
        .as_collection();

    (snapshot_updates, lsn_stream)
}

fn build_tmp_slot_name(worker_id: usize) -> Result<String> {
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
    Ok(format!("slot_{worker_id}_{now}"))
}

async fn export_snapshot(client: &tokio_postgres::Client, worker_id: usize) -> Result<u64> {
    match export_snapshot_inner(client, worker_id).await {
        Ok(lsn) => Ok(lsn),
        Err(e) => {
            client.simple_query("ROLLBACK;").await?;
            panic!("{:?}", e)
        }
    }
}

async fn export_snapshot_inner(client: &tokio_postgres::Client, worker_id: usize) -> Result<u64> {
    client
        .simple_query("BEGIN READ ONLY ISOLATION LEVEL REPEATABLE READ;")
        .await?;

    let slot = build_tmp_slot_name(worker_id)?;
    let query = format!(
        "CREATE_REPLICATION_SLOT {} TEMPORARY LOGICAL \"pgoutput\" USE_SNAPSHOT",
        slot
    );

    let mut consistent_point: Option<PgLsn> = None;
    for msg in client.simple_query(&query).await? {
        if let SimpleQueryMessage::Row(row) = msg {
            consistent_point = Some(row.get("consistent_point").unwrap().parse().unwrap());
            break;
        }
    }

    let consistent_point = consistent_point
        .ok_or_else(|| anyhow!("CREATE_REPLICATION_SLOT returned no row with consistent_point"))?;

    let snapshot_lsn = u64::from(consistent_point)
        .checked_sub(1)
        .ok_or_else(|| anyhow!("consistent_point was zero"))?;

    Ok(snapshot_lsn)
}
