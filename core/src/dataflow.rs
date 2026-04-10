use std::{cell::RefCell, rc::Rc, sync::Arc};

use crate::{
    algorithms::trusted_assertions::trusted_assertions,
    operators::probe::Handle,
    sinks::{dataset_sink::parquet_sink, event_sink::event_sink},
    sources::{
        model_import_source::model_import_source, persist::persist_source::persist_source,
        postgres::render,
    },
    state::{QueryDataflow, build_query_dataflow},
    types::TRUSTED_USER,
    worker::Worker,
};
use differential_dataflow::operators::CountTotal;
use nostr_sdk::Kind;
use persist::{db::PersistStore, query::PersistQuery};
use timely::communication::Allocate;
use timely::dataflow::operators::{Exchange, Inspect};
use types::{
    edges::Edge,
    embeddings::{KIND_MODEL, KIND_USER_EMBEDDING},
    parquet::{EdgesParquet, FeaturesParquet, NodesParquet},
    types::Node,
};

#[derive(Debug, Clone)]
pub struct TimelyConfig {
    pub workers: usize,
}

#[derive(Debug, Clone)]
pub struct DataflowConfig {
    pub timely: TimelyConfig,
    /// PersistStore for accessing the database.
    pub persist: Arc<PersistStore>,
    /// Root path for the persisted database directory.
    pub persist_path: String,
    /// Number of PageRank iterations to perform, don't use less than 20 for meaningful results.
    pub pagerank_iterations: usize,
    /// Batch size for saving ranks to the database.
    pub pagerank_sink_batch_size: usize,
    /// Maximum number of pending replication messages before applying backpressure.
    pub replication_max_pending: usize,
    /// Disable Postgres logical replication after snapshot/checkpoint startup.
    pub no_replication: bool,
    /// Nsec to sign trusted assertions events.
    pub trusted_assertions_nsec: Option<String>,
    /// Optional manifest path for a one-shot embedding/model import after bootstrap completes.
    pub embedding_import_manifest: Option<String>,
    /// Optional output file or directory for the final parquet dataset snapshot.
    pub dataset_sink_path: Option<String>,
    /// Explicit one-off override to resign locally generated addressable events.
    pub recompute: bool,
    /// Trusted seed pubkeys for personalized pagerank / trust propagation.
    pub trusted_seeds: Vec<Node>,
    /// Maximum number of items in trusted lists.
    pub trusted_lists_ranks_k: usize,
}

pub fn build_dataflow<A: Allocate>(worker: &mut Worker<A>, config: DataflowConfig) {
    let timely_worker = &mut worker.timely_worker;

    timely_worker.dataflow::<u64, _, _>(|scope| {
        let worker_id = scope.index();
        let worker_count = config.timely.workers;
        let persist_probe = Handle::default();
        let persisted_done = render(scope, config.clone());
        let (raw_events, bootstrap_done) = persist_source(
            scope,
            config.clone(),
            PersistQuery::default().kinds(vec![
                Kind::Metadata.as_u16(),
                Kind::ContactList.as_u16(),
                Kind::MuteList.as_u16(),
                Kind::TextNote.as_u16(),
                Kind::Comment.as_u16(),
                Kind::LongFormTextNote.as_u16(),
                Kind::Repost.as_u16(),
                Kind::Reaction.as_u16(),
                Kind::Reporting.as_u16(),
                Kind::ZapReceipt.as_u16(),
                TRUSTED_USER.as_u16(),
                KIND_MODEL.as_u16(),
                KIND_USER_EMBEDDING.as_u16(),
            ]),
            &persisted_done,
            &persist_probe,
        );

        let QueryDataflow {
            query_events,
            event_tags,
            arrangements,
        } = build_query_dataflow(
            &raw_events,
            vec![
                TRUSTED_USER.as_u16(),
                KIND_MODEL.as_u16(),
                KIND_USER_EMBEDDING.as_u16(),
            ],
        );

        let mutes = raw_events
            .filter(|event| Kind::from_u16(event.kind) == Kind::MuteList)
            .flat_map(|event| {
                let muter = event.pubkey;
                let mut seen = std::collections::HashSet::new();

                event
                    .edges
                    .into_iter()
                    .filter_map(move |edge| match edge {
                        Edge::Pubkey(muted) if seen.insert(muted) => Some((muter, muted)),
                        _ => None,
                    })
                    .collect::<Vec<_>>()
            });

        let (ta_collection, follows) = trusted_assertions(&raw_events, &config);
        let persist = config.persist.clone();
        let trusted_users = ta_collection.flat_map(move |(node, user)| {
            persist
                .resolve_node(node)
                .ok()
                .flatten()
                .filter(|bytes| bytes.len() == 32)
                .map(|bytes| (node, hex::encode(bytes), user))
                .into_iter()
                .collect::<Vec<_>>()
        });
        let features = trusted_users.map(|(node, _node_pubkey, trusted_user)| (node, trusted_user));
        let nodes = trusted_users.map(|(node, node_pubkey, _trusted_user)| (node, node_pubkey));
        let trusted_events =
            trusted_users.map(|(_node, node_pubkey, trusted_user)| (node_pubkey, trusted_user));
        parquet_sink::<_, FeaturesParquet>(
            scope,
            config.clone(),
            &features,
            &bootstrap_done,
            "features",
        );
        parquet_sink::<_, NodesParquet>(scope, config.clone(), &nodes, &bootstrap_done, "nodes");
        event_sink(scope, config.clone(), &trusted_events);
        parquet_sink::<_, EdgesParquet>(scope, config.clone(), &follows, &bootstrap_done, "edges");
        parquet_sink::<_, EdgesParquet>(scope, config.clone(), &mutes, &bootstrap_done, "mutes");
        let imported_embeddings = model_import_source(scope, config.clone(), &bootstrap_done);
        event_sink(scope, config.clone(), &imported_embeddings);

        worker.state.set_query_arrangements(arrangements);

        let query_ready_count = Rc::new(RefCell::new(0usize));

        bootstrap_done.exchange(|_| 0).inspect({
            let query_ready_count = Rc::clone(&query_ready_count);
            move |_| {
                let mut ready = query_ready_count.borrow_mut();
                *ready += 1;
                if worker_id == 0 && *ready == worker_count {
                    tracing::info!(
                        "[Worker 0] query bootstrap complete across all workers; core queries are ready"
                    );
                }
            }
        });

        // DO NOT DELETE THIS
        query_events
            .map(|_| ())
            .count_total()
            .inspect(move |(((), total), t, diff)| {
                if *diff > 0 {
                    tracing::info!("[Worker {}] total events: {} at ts={}", worker_id, total, t);
                }
            });

        // DO NOT DELETE THIS
        event_tags
            .map(|_| ())
            .count_total()
            .inspect(move |(((), total), t, diff)| {
                if *diff > 0 {
                    tracing::info!("[Worker {}] total tags: {} at ts={}", worker_id, total, t);
                }
            });
    });
}
