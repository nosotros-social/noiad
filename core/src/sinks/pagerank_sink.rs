use differential_dataflow::VecCollection;
use timely::dataflow::{Scope, channels::pact::Pipeline};
use tokio_postgres::{binary_copy::BinaryCopyInWriter, types::Type};
use tokio_stream::StreamExt;

use crate::config::Config;
use crate::operators::builder_async::{AsyncOperatorBuilder, Event};
use crate::sources::postgres::connection::PostgresConnection;
use crate::types::{Diff, Node};

async fn ensure_pagerank_tables(client: &tokio_postgres::Client) {
    client
        .batch_execute(
            "
            SET client_min_messages TO WARNING;

            CREATE TABLE IF NOT EXISTS pageranks (
                pubkey TEXT PRIMARY KEY,
                rank   BIGINT NOT NULL
            );

            CREATE TEMP TABLE IF NOT EXISTS pagerank_staging (
                pubkey TEXT NOT NULL,
                rank   BIGINT NOT NULL
            );
            ",
        )
        .await
        .unwrap();
}

async fn copy_pagerank_batch(client: &mut tokio_postgres::Client, batch: &[(Node, i64)]) {
    if batch.is_empty() {
        return;
    }

    let tx = client.transaction().await.unwrap();

    let sink = tx
        .copy_in(
            "
            COPY pagerank_staging (pubkey, rank)
            FROM STDIN BINARY
            ",
        )
        .await
        .unwrap();

    let writer = BinaryCopyInWriter::new(sink, &[Type::TEXT, Type::INT8]);
    let mut writer = Box::pin(writer);

    for (pubkey, rank) in batch {
        let pubkey_hex = hex::encode(pubkey);
        writer.as_mut().write(&[&pubkey_hex, rank]).await.unwrap();
    }

    writer.as_mut().finish().await.unwrap();

    tx.batch_execute(
        "
            INSERT INTO pageranks (pubkey, rank)
            SELECT pubkey, rank
            FROM pagerank_staging
            ON CONFLICT (pubkey) DO UPDATE
            SET rank = EXCLUDED.rank;

            TRUNCATE pagerank_staging;
            ",
    )
    .await
    .unwrap();

    tx.commit().await.unwrap();
}

pub fn sink_pagerank<G>(input: &VecCollection<G, Node, Diff>, config: Config)
where
    G: Scope<Timestamp = u64>,
{
    let mut builder = AsyncOperatorBuilder::new("SinkPageRank".to_string(), input.inner.scope());
    let mut input = builder.new_disconnected_input(&input.inner, Pipeline);

    let _error_stream = builder.build_fallible(move |_caps| {
        Box::pin(async move {
            let mut client = PostgresConnection::from_env().connect().await.unwrap();
            ensure_pagerank_tables(&client).await;

            let mut batch: Vec<(Node, i64)> = Vec::with_capacity(100_000);

            while let Some(event) = input.next().await {
                match event {
                    Event::Data(_time, mut data) => {
                        for (pubkey, _t, diff) in data.drain(..) {
                            batch.push((pubkey, diff as i64));

                            if batch.len() >= 100_000 {
                                tracing::info!(
                                    "[Worker {}] Inserting ranks batch of {}",
                                    config.worker_id,
                                    batch.len()
                                );
                                copy_pagerank_batch(&mut client, &batch).await;
                                batch.clear();
                            }
                        }
                    }
                    Event::Progress(frontier) => {
                        if frontier.is_empty() {
                            if !batch.is_empty() {
                                copy_pagerank_batch(&mut client, &batch).await;
                                batch.clear();
                            }
                            tracing::info!("[Worker {}] SinkPageRank completed", config.worker_id);
                            break;
                        }
                    }
                }
            }

            Ok::<(), tokio_postgres::Error>(())
        })
    });
}
