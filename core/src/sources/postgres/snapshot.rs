use std::env;

use anyhow::Error;
use futures::TryStreamExt;

use core::event::Event;

use differential_dataflow::{AsCollection, Collection};
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::Operator;
use timely::{container::CapacityContainerBuilder, dataflow::Scope};

use crate::config::Config;
use crate::operators::builder_async::AsyncOperatorBuilder;
use crate::sources::postgres::connection::PostgresConnection;
use crate::sources::postgres::parser::{CopyParser, quote_ident};
use crate::sources::postgres::utils::get_publication_info;

pub fn snapshot<G>(scope: G, config: Config) -> Collection<G, Event, isize>
where
    G: Scope<Timestamp = u64>,
{
    let mut builder = AsyncOperatorBuilder::new("PgSnapshotDataflow".to_string(), scope.clone());

    let (raw_handle, raw_stream) = builder.new_output::<CapacityContainerBuilder<Vec<Vec<u8>>>>();

    let _ = builder.build_fallible(move |capabilities| {
        Box::pin(async move {
            let [raw_cap]: &mut [_; 1] = capabilities.try_into().unwrap();

            let is_snapshot_leader = config.worker_id == 0;
            if is_snapshot_leader {
                let client = PostgresConnection::from_env().connect().await?;
                let db_publication =
                    env::var("DB_PUBLICATION").expect("DB_PUBLICATION must be set");
                let tables = get_publication_info(&client, &db_publication).await?;

                for table in tables {
                    client
                        .simple_query("BEGIN READ ONLY ISOLATION LEVEL REPEATABLE READ")
                        .await?;

                    let query = format!(
                        "COPY {}.{} (id, pubkey, created_at, kind, tags, content) 
                        TO STDOUT (FORMAT TEXT, DELIMITER '\t')",
                        quote_ident(&table.schema),
                        quote_ident(&table.name)
                    );

                    let mut stream = std::pin::pin!(client.copy_out_simple(&query).await?);

                    let started = std::time::Instant::now();
                    while let Some(bytes) = stream.try_next().await? {
                        raw_handle.give(&raw_cap[0], bytes.to_vec());
                    }

                    client.simple_query("COMMIT").await?;
                    tracing::info!("Copied completed in {:?}", started.elapsed());
                }
            }

            Ok::<(), Error>(())
        })
    });

    // Decode snapshot data in multiple workers
    let mut next_worker = (0..(scope.peers() as u64))
        .flat_map(|w| std::iter::repeat_n(w, 50))
        .cycle();
    let round_robin = Exchange::new(move |_| next_worker.next().unwrap());

    let snapshot_updates: Collection<_, Event, isize> = raw_stream
        .unary(round_robin, "PgSnapshotDecode", |_, _| {
            move |input, output| {
                while let Some((time, data)) = input.next() {
                    let mut session = output.session(&time);
                    for bytes in data.drain(..) {
                        for event in CopyParser::new(&bytes, b'\t').unwrap() {
                            session.give((event, *time, 1isize));
                        }
                    }
                }
            }
        })
        .as_collection();

    snapshot_updates
}
