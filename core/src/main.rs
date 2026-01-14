use clap::Parser;
use core::algorithms::pagerank::pagerank;
use core::algorithms::trusted_assertions::trusted_assertions;
use core::algorithms::trusted_assertions_event::{self, event_assertions_event};
use core::config::Config;
use core::operators::probe::{Handle, ProbeNotify};
use core::operators::top_k::TopK;
use core::sources::postgres::render;
use core::{cli::Cli, sources::persist::persist_source::persist_source};
use differential_dataflow::operators::CountTotal;
use nostr_sdk::Kind;
use persist::db::PersistStore;
use persist::edges::EdgeLabel;
use persist::query::PersistQuery;
use std::sync::Arc;
use timely::dataflow::operators::Exchange;
use timely::{
    container::CapacityContainerBuilder,
    dataflow::{channels::pact::Pipeline, operators::Operator},
};
use types::event::Node;

use dotenvy::dotenv;

fn main() {
    tracing_subscriber::fmt().init();
    dotenv().ok();

    let start = std::time::Instant::now();
    let cli = Cli::parse();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(cli.workers)
        .enable_all()
        .build()
        .unwrap();

    let runtime_handle = runtime.handle().clone();

    let rocksdb_config = persist::db::RocksDBConfig {
        max_total_wal_size: cli.max_total_wal_size,
    };
    let persist = Arc::new(
        PersistStore::open(&cli.persist_path, rocksdb_config).expect("Failed to open PersistStore"),
    );

    let _ = timely::execute(timely::Config::process(cli.workers), move |worker| {
        let _enter = runtime_handle.enter();

        let index = worker.index();
        let persist = persist.clone();

        tracing::info!("Worker {index} started");

        worker.dataflow::<u64, _, _>(|scope| {
            let config = Config {
                worker_id: scope.index(),
                worker_count: scope.peers(),
                persist: Arc::clone(&persist),
                pagerank_iterations: cli.pagerank_iterations,
                pagerank_sink_batch_size: cli.pagerank_sink_batch_size,
                replication_max_pending: cli.replication_max_pending,
            };

            let probe = Handle::default();

            let persisted_done = render(scope, config.clone());

            let events = persist_source(
                scope,
                config.clone(),
                PersistQuery::default().kinds(vec![
                    Kind::ContactList.as_u16(),
                    Kind::TextNote.as_u16(),
                    Kind::Comment.as_u16(),
                    Kind::LongFormTextNote.as_u16(),
                    Kind::Repost.as_u16(),
                    Kind::Reaction.as_u16(),
                    Kind::Reporting.as_u16(),
                    Kind::ZapReceipt.as_u16(),
                ]),
                &persisted_done,
                &probe,
            );

            let edges = events.flat_map(|e| e.to_edges());

            let follows = edges
                .filter(|(kind, _, _, label)| {
                    *kind == Kind::ContactList.as_u16() && *label == EdgeLabel::Pubkey
                })
                .map(|(_, src, dst, _)| (src, dst));

            let ranks = pagerank(cli.pagerank_iterations, &follows).consolidate();

            ranks
                .inner
                .exchange(|_| 0)
                .unary_frontier::<CapacityContainerBuilder<Vec<(Node, u64, isize)>>, _, _, _>(
                    Pipeline,
                    "PageRankLog",
                    |_cap, _info| {
                        let mut per_ts_counts: std::collections::BTreeMap<u64, usize> =
                            std::collections::BTreeMap::new();

                        move |(input, frontier), output| {
                            input.for_each(|cap, data| {
                                let ts = *cap.time();
                                *per_ts_counts.entry(ts).or_insert(0) += data.len();

                                let mut session = output.session(&cap);
                                session.give_iterator(data.drain(..));
                            });

                            while let Some((&ts, &count)) = per_ts_counts.iter().next() {
                                if frontier.less_equal(&ts) {
                                    break;
                                }
                                per_ts_counts.remove(&ts);
                                tracing::info!(
                                    "[Worker {}] pagerank finished @ {}: nodes={}",
                                    config.worker_id,
                                    ts,
                                    count
                                );
                            }
                        }
                    },
                )
                .probe_notify_with(vec![probe.clone()]);

            let ta_events = events.filter(|e| {
                !matches!(
                    nostr_sdk::Kind::from_u16(e.kind),
                    nostr_sdk::Kind::ContactList
                )
            });
            let ta_collection = trusted_assertions(&ta_events, &follows, &ranks);
            let ta_events_collection = event_assertions_event(&ta_events);

            ta_collection
                .map(|(pubkey, assertion)| (assertion.follower_cnt, pubkey, assertion))
                .top_k(5)
                .map(|(_follower_cnt, _pubkey, assertion)| assertion)
                .inspect(|(assertion, _t, diff)| {
                    if *diff > 0 {
                        tracing::info!("trusted assertion: {:?}", assertion);
                    }
                })
                .inner
                .probe_notify_with(vec![probe.clone()]);

            ta_events_collection
                .map(|(pubkey, assertion)| (assertion.rank, assertion))
                .top_k(5)
                .map(|(_rank, assertion)| assertion)
                .inspect(|(assertion, _t, diff)| {
                    if *diff > 0 {
                        tracing::info!("trusted events assertion: {:?}", assertion);
                    }
                })
                .inner
                .probe_notify_with(vec![probe.clone()]);

            edges
                .map(|_| ())
                .count_total()
                .inspect(move |(((), total), _t, diff)| {
                    if *diff > 0 {
                        tracing::info!("[Worker {}] total edges: {}", config.worker_id, total);
                    }
                });

            events
                .map(|_| ())
                .count_total()
                .inspect(move |(((), total), _t, diff)| {
                    if *diff > 0 {
                        tracing::info!("[Worker {}] total events: {}", config.worker_id, total);
                    }
                });
        });

        while worker.step_or_park(None) {}
    });

    tracing::info!("Completed in {:?}", start.elapsed());
}
