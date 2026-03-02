use std::{cell::RefCell, rc::Rc, sync::Arc};

use differential_dataflow::{
    VecCollection,
    operators::{CountTotal, arrange::ArrangeByKey},
};
use nostr_sdk::Kind;
use persist::{db::PersistStore, query::PersistQuery};
use timely::{
    communication::Allocate,
    container::CapacityContainerBuilder,
    dataflow::{
        channels::pact::Pipeline,
        operators::{Exchange, Inspect, Operator},
    },
};
use types::{
    edges::EdgeLabel,
    trusted_assertions::IntoNostrEvent,
    types::{Diff, Node},
};

use crate::{
    algorithms::{
        pagerank::pagerank,
        trusted_assertions::trusted_assertions,
        trusted_lists::{trusted_list_followers, trusted_list_ranks},
    },
    operators::probe::{Handle, ProbeNotify},
    sinks::trusted_assertions_sink::trusted_assertion_sink,
    sources::{persist::persist_source::persist_source, postgres::render},
    state::State,
    types::TrustedPayload,
    worker::Worker,
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
    /// Number of PageRank iterations to perform, don't use less than 20 for meaningful results.
    pub pagerank_iterations: usize,
    /// Batch size for saving ranks to the database.
    pub pagerank_sink_batch_size: usize,
    /// Maximum number of pending replication messages before applying backpressure.
    pub replication_max_pending: usize,
    /// Nsec to sign trusted assertions events.
    pub trusted_assertions_nsec: Option<String>,
    /// Maximum number of items in trusted lists.
    pub trusted_lists_ranks_k: usize,
}

pub fn build_dataflow<A: Allocate>(worker: &mut Worker<A>, config: DataflowConfig) {
    let persist = config.persist.clone();
    let timely_worker = &mut worker.timely_worker;

    timely_worker.dataflow::<u64, _, _>(|scope| {
        let probe = Handle::default();
        let persisted_done = render(scope, config.clone());
        let worker_id = scope.index();

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
                Kind::Custom(30382).as_u16(), // trusted assertions user
                Kind::Custom(30383).as_u16(), // trusted aseertions event
                Kind::Custom(30392).as_u16(), // trusted lists
            ]),
            &persisted_done,
            &probe,
        );

        events
            .map(|_| ())
            .count_total()
            .inspect(move |(((), total), _t, diff)| {
                if *diff > 0 {
                    tracing::info!("[Worker {}] total events: {}", worker_id, total);
                }
            });

        let edges = events.flat_map(|e| e.to_edges());

        let follows = edges
            .filter(|(kind, _, _, label)| {
                *kind == Kind::ContactList.as_u16() && *label == EdgeLabel::Pubkey
            })
            .map(|(_, src, dst, _)| (src, dst));

        let ranks = pagerank(config.pagerank_iterations, &follows).consolidate();

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
                                worker_id,
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
        let ta_list_ranks = trusted_list_ranks(&ta_collection, config.trusted_lists_ranks_k)
            .map(|e| ("ranks".to_owned(), TrustedPayload::ListRanks(e)));

        let ta_list_followers =
            trusted_list_followers(&ta_collection, config.trusted_lists_ranks_k)
                .map(|e| ("followers".to_owned(), TrustedPayload::ListFollowers(e)));

        ta_collection.inner.probe_notify_with(vec![probe.clone()]);

        let ta_out: VecCollection<_, (String, TrustedPayload), Diff> =
            ta_collection.flat_map(move |(node, assertion)| {
                let bytes = persist.interner.resolve(&persist.db, node).ok().flatten()?;
                let identifier = hex::encode(bytes);
                Some((identifier, TrustedPayload::Assertion(assertion)))
            });

        let trusted_events = ta_out.concat(&ta_list_ranks).concat(&ta_list_followers);

        trusted_assertion_sink(scope, config.clone(), &trusted_events);

        let events_by_id = events.map(|e| (e.id, e)).arrange_by_key();
        let events_by_pubkey = events.map(|e| (e.pubkey, e)).arrange_by_key();
        let events_by_kind = events.map(|e| (e.kind, e)).arrange_by_key();
        let events_by_tags = events.flat_map(|e| e.tag_keys()).arrange_by_key();

        worker.state.set_events_by_id(events_by_id.trace);
        worker.state.set_events_by_author(events_by_pubkey.trace);
        worker.state.set_events_by_kind(events_by_kind.trace);
        worker.state.set_events_by_tags(events_by_tags.trace);
    });
}
