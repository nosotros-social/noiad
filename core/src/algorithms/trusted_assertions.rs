use std::collections::{HashMap, HashSet};

use crate::dataflow::DataflowConfig;
use differential_dataflow::{
    AsCollection, Hashable, VecCollection, input::Input, operators::CountTotal,
};
use nostr_sdk::Kind;
use serde::{Deserialize, Serialize};
use timely::{
    PartialOrder,
    dataflow::Scope,
    dataflow::channels::pact::Pipeline,
    dataflow::operators::{Capability, InputCapability},
    dataflow::operators::{Exchange, generic::builder_rc::OperatorBuilder},
    progress::Antichain,
};
use types::{
    edges::Edge,
    event::EventRow,
    trusted_assertions::ta_user::{Count, TrustedUser},
    types::{Diff, Node},
};

const PAGERANK_EXPONENT: f64 = 0.76;
const SCORE_CURVE_EXPONENT: f64 = 0.38;
const SCORE_BASELINE_OFFSET: f64 = 1.0 - PAGERANK_EXPONENT;
const AVERAGE_RANK: f64 = 4_000.0;
const MIN_NORMALIZED_RANK: Diff = 0;

fn normalize_pagerank(rank: Diff) -> Diff {
    if rank <= 0 {
        return 0;
    }

    let rank_f = rank as f64;
    let relative_strength = rank_f / AVERAGE_RANK;
    let denominator = relative_strength + SCORE_BASELINE_OFFSET;
    let value = 1.0 - (SCORE_BASELINE_OFFSET / denominator).powf(SCORE_CURVE_EXPONENT);
    let score = (value * 100.0).clamp(0.0, 100.0);
    score.round() as Diff
}

#[derive(Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
struct TrustedUpdate {
    assertion: TrustedUser,
    rank: Option<Diff>,
    zap_sent_day: Option<u32>,
    zap_recd_day: Option<u32>,
}

#[derive(Clone, Debug, Default)]
struct ZapSpanState {
    min_sent_day: Option<u32>,
    max_sent_day: Option<u32>,
    min_recd_day: Option<u32>,
    max_recd_day: Option<u32>,
}

macro_rules! ta_partial {
    ($($field:ident : $value:expr),+ $(,)?) => {
        TrustedUser {
            $($field: $value,)+
            ..TrustedUser::default()
        }
    };
}

pub fn trusted_assertions<G>(
    events: &VecCollection<G, EventRow, Diff>,
    config: &DataflowConfig,
) -> (
    VecCollection<G, (Node, TrustedUser), Diff>,
    VecCollection<G, (Node, Node), Diff>,
)
where
    G: Scope<Timestamp = u64>,
{
    let follows = events.flat_map(|event| {
        if Kind::from_u16(event.kind) != Kind::ContactList {
            return Vec::new();
        }

        let follower = event.pubkey;
        let mut seen = HashSet::new();

        event
            .edges
            .into_iter()
            .filter_map(move |edge| match edge {
                Edge::Pubkey(followee) if seen.insert(followee) => Some((follower, followee)),
                _ => None,
            })
            .collect::<Vec<_>>()
    });

    let mut seed_scope = events.inner.scope();
    let trusted_seeds = seed_scope
        .new_collection_from(config.trusted_seeds.clone())
        .1;

    let raw_ranks =
        crate::algorithms::pagerank::pagerank(config.pagerank_iterations, &follows, &trusted_seeds);

    let rank_totals = raw_ranks
        .count_total()
        .map(|(pubkey, rank)| (pubkey, normalize_pagerank(rank)));

    let follower_updates = filtered_follower_updates(&follows, &rank_totals);

    let rank_updates = rank_totals.map(|(pubkey, rank)| {
        (
            pubkey,
            TrustedUpdate {
                rank: Some(rank),
                ..TrustedUpdate::default()
            },
        )
    });

    let event_updates = events.flat_map(|event| {
        let mut out = Vec::new();
        let hour = ((event.created_at / 3600) % 24) as u8;
        let day = event.created_at / 86_400;

        match Kind::from_u16(event.kind) {
            Kind::TextNote => {
                let is_reply = event
                    .edges
                    .iter()
                    .any(|edge| matches!(edge, Edge::Reply(_) | Edge::RootReply(_)));

                out.push((
                    event.pubkey,
                    TrustedUpdate {
                        assertion: if is_reply {
                            ta_partial!(
                                first_created_at: Some(event.created_at),
                                active_hours_start: Some(hour),
                                active_hours_end: Some(hour),
                                reply_cnt: 1
                            )
                        } else {
                            ta_partial!(
                                first_created_at: Some(event.created_at),
                                active_hours_start: Some(hour),
                                active_hours_end: Some(hour),
                                post_cnt: 1
                            )
                        },
                        ..TrustedUpdate::default()
                    },
                ));
            }
            Kind::Reaction => {
                out.push((
                    event.pubkey,
                    TrustedUpdate {
                        assertion: ta_partial!(
                            first_created_at: Some(event.created_at),
                            active_hours_start: Some(hour),
                            active_hours_end: Some(hour),
                            reactions_cnt: 1
                        ),
                        ..TrustedUpdate::default()
                    },
                ));
            }
            Kind::Reporting => {
                out.push((
                    event.pubkey,
                    TrustedUpdate {
                        assertion: ta_partial!(
                            first_created_at: Some(event.created_at),
                            active_hours_start: Some(hour),
                            active_hours_end: Some(hour),
                            reports_cnt_sent: 1
                        ),
                        ..TrustedUpdate::default()
                    },
                ));
                for edge in &event.edges {
                    if let Edge::Pubkey(target) = edge {
                        out.push((
                            *target,
                            TrustedUpdate {
                                assertion: ta_partial!(reports_cnt_recd: 1),
                                ..TrustedUpdate::default()
                            },
                        ));
                    }
                }
            }
            Kind::ZapReceipt => {
                let mut sender = None;
                let mut recipient = None;
                let mut amount = 0u64;

                for edge in &event.edges {
                    match edge {
                        Edge::PubkeyUpper(node) => sender = Some(*node),
                        Edge::Pubkey(node) => recipient = Some(*node),
                        Edge::Bolt11(amt) => amount = *amt,
                        _ => {}
                    }
                }

                if let Some(s) = sender {
                    out.push((
                        s,
                        TrustedUpdate {
                            assertion: ta_partial!(zap_cnt_sent: 1, zap_amt_sent: amount),
                            zap_sent_day: Some(day),
                            ..TrustedUpdate::default()
                        },
                    ));
                }
                if let Some(r) = recipient {
                    out.push((
                        r,
                        TrustedUpdate {
                            assertion: ta_partial!(zap_cnt_recd: 1, zap_amt_recd: amount),
                            zap_recd_day: Some(day),
                            ..TrustedUpdate::default()
                        },
                    ));
                }
            }
            _ => {}
        }

        out
    });

    let updates = event_updates
        .concat(&rank_updates)
        .concat(&follower_updates);
    let exchanged = updates
        .inner
        .exchange(|((pubkey, _update), _time, _diff)| pubkey.hashed());

    (
        accumulate_trusted_assertions(events.inner.scope(), &exchanged),
        follows,
    )
}

#[derive(Clone, Debug, Default)]
struct FollowerState {
    normalized_rank: Diff,
    counts: bool,
    followees: HashSet<Node>,
}

fn filtered_follower_updates<G>(
    follows: &VecCollection<G, (Node, Node), Diff>,
    rank_totals: &VecCollection<G, (Node, Diff), Diff>,
) -> VecCollection<G, (Node, TrustedUpdate), Diff>
where
    G: Scope<Timestamp = u64>,
{
    let exchanged_follows = follows
        .inner
        .exchange(|((follower, _followee), _time, _diff)| follower.hashed());
    let exchanged_ranks = rank_totals
        .inner
        .exchange(|((follower, _rank), _time, _diff)| follower.hashed());

    let mut builder =
        OperatorBuilder::new("FilteredFollowerCounts".to_string(), follows.inner.scope());
    let mut follows_input = builder.new_input(&exchanged_follows, Pipeline);
    let mut ranks_input = builder.new_input(&exchanged_ranks, Pipeline);
    let (mut output_handle, output_stream) = builder.new_output();

    builder.build(move |_initial_capabilities| {
        let mut emitted_frontier = Antichain::from_elem(0u64);
        let mut retained_cap: Option<Capability<u64>> = None;
        let mut follower_states: HashMap<Node, FollowerState> = HashMap::new();
        let mut deltas: HashMap<Node, Count> = HashMap::new();
        let mut cleanup_candidates: HashSet<Node> = HashSet::new();

        move |frontiers| {
            follows_input.for_each(
                |capability: InputCapability<u64>, batches: &mut Vec<((Node, Node), u64, Diff)>| {
                    if retained_cap.is_none() {
                        retained_cap = Some(capability.retain());
                    }

                    for ((follower, followee), _time, diff) in batches.drain(..) {
                        let state = follower_states.entry(follower).or_default();

                        if diff > 0 {
                            if state.followees.insert(followee) && state.counts {
                                *deltas.entry(followee).or_default() += 1;
                            }
                        } else if state.followees.remove(&followee) && state.counts {
                            *deltas.entry(followee).or_default() -= 1;
                        }

                        if state.normalized_rank == 0 && state.followees.is_empty() {
                            cleanup_candidates.insert(follower);
                        } else {
                            cleanup_candidates.remove(&follower);
                        }
                    }
                },
            );

            ranks_input.for_each(
                |capability: InputCapability<u64>, batches: &mut Vec<((Node, Diff), u64, Diff)>| {
                    if retained_cap.is_none() {
                        retained_cap = Some(capability.retain());
                    }

                    for ((follower, normalized_rank), _time, diff) in batches.drain(..) {
                        let state = follower_states.entry(follower).or_default();
                        let old_counts = state.counts;

                        if diff > 0 {
                            state.normalized_rank = normalized_rank;
                        } else if state.normalized_rank == normalized_rank {
                            state.normalized_rank = 0;
                        }

                        state.counts = state.normalized_rank >= MIN_NORMALIZED_RANK;

                        if old_counts != state.counts {
                            let follower_diff = if state.counts { 1 } else { -1 };
                            for &followee in &state.followees {
                                *deltas.entry(followee).or_default() += follower_diff;
                            }
                        }

                        if state.normalized_rank == 0 && state.followees.is_empty() {
                            cleanup_candidates.insert(follower);
                        } else {
                            cleanup_candidates.remove(&follower);
                        }
                    }
                },
            );

            let frontier_min = frontiers
                .iter()
                .filter_map(|input| input.frontier().iter().min().copied())
                .min();

            let Some(frontier_min) = frontier_min else {
                retained_cap = None;
                emitted_frontier.clear();
                return;
            };

            let input_frontier = Antichain::from_elem(frontier_min);

            if !PartialOrder::less_than(&emitted_frontier.borrow(), &input_frontier.borrow()) {
                return;
            }

            emitted_frontier.clear();
            emitted_frontier.insert(frontier_min);

            let cap = match retained_cap.as_mut() {
                Some(c) => c,
                None => return,
            };

            let emit_time = frontier_min.saturating_sub(1).max(*cap.time());
            cap.downgrade(&emit_time);

            for follower in cleanup_candidates.drain() {
                let should_remove = follower_states
                    .get(&follower)
                    .map(|state| state.normalized_rank == 0 && state.followees.is_empty())
                    .unwrap_or(false);
                if should_remove {
                    follower_states.remove(&follower);
                }
            }

            if deltas.is_empty() {
                cap.downgrade(&frontier_min);
                return;
            }

            let mut output_activated = output_handle.activate();
            let mut updates: Vec<((Node, TrustedUpdate), u64, Diff)> = Vec::new();

            for (followee, delta) in deltas.drain() {
                if delta == 0 {
                    continue;
                }

                updates.push((
                    (
                        followee,
                        TrustedUpdate {
                            assertion: ta_partial!(follower_cnt: 1),
                            ..TrustedUpdate::default()
                        },
                    ),
                    emit_time,
                    delta as Diff,
                ));
            }

            if !updates.is_empty() {
                output_activated.give(cap, &mut updates);
            }

            cap.downgrade(&frontier_min);
        }
    });

    output_stream.as_collection()
}

fn recompute_zap_averages(assertion: &mut TrustedUser, span: &ZapSpanState) {
    assertion.zap_avg_amt_day_sent = match (span.min_sent_day, span.max_sent_day) {
        (Some(min), Some(max)) => {
            let days = (max - min + 1) as u64;
            assertion.zap_amt_sent / days.max(1)
        }
        _ => 0,
    };

    assertion.zap_avg_amt_day_recd = match (span.min_recd_day, span.max_recd_day) {
        (Some(min), Some(max)) => {
            let days = (max - min + 1) as u64;
            assertion.zap_amt_recd / days.max(1)
        }
        _ => 0,
    };
}

fn accumulate_trusted_assertions<G>(
    scope: G,
    exchanged: &timely::dataflow::StreamCore<G, Vec<((Node, TrustedUpdate), u64, Diff)>>,
) -> VecCollection<G, (Node, TrustedUser), Diff>
where
    G: Scope<Timestamp = u64>,
{
    let worker_id = scope.index();
    let mut builder = OperatorBuilder::new("TrustedAssertionsAccumulate".to_string(), scope);
    let mut partial_input = builder.new_input(exchanged, Pipeline);
    let (mut output_handle, output_stream) = builder.new_output();

    builder.build(move |_initial_capabilities| {
        let mut emitted_frontier = Antichain::from_elem(0u64);
        let mut retained_cap: Option<Capability<u64>> = None;
        let mut emitted: HashMap<Node, TrustedUser> = HashMap::new();
        let mut pending: HashMap<Node, TrustedUser> = HashMap::new();
        let mut zap_spans: HashMap<Node, ZapSpanState> = HashMap::new();
        let mut dirty_pubkeys: HashSet<Node> = HashSet::new();

        move |frontiers| {
            partial_input.for_each(
                |capability: InputCapability<u64>,
                 batches: &mut Vec<((Node, TrustedUpdate), u64, Diff)>| {
                    if retained_cap.is_none() {
                        retained_cap = Some(capability.retain());
                    }

                    for ((pubkey, update), _time, diff) in batches.drain(..) {
                        dirty_pubkeys.insert(pubkey);
                        let entry = pending
                            .entry(pubkey)
                            .or_insert_with(|| emitted.get(&pubkey).cloned().unwrap_or_default());
                        entry.apply(&update.assertion, diff);
                        if let Some(rank) = update.rank {
                            if diff > 0 {
                                entry.rank = Some(rank);
                            } else if entry.rank == Some(rank) {
                                entry.rank = None;
                            }
                        }

                        let has_zap_day =
                            update.zap_sent_day.is_some() || update.zap_recd_day.is_some();
                        let has_zap_amt =
                            update.assertion.zap_amt_sent > 0 || update.assertion.zap_amt_recd > 0;

                        if has_zap_day {
                            let span = zap_spans.entry(pubkey).or_default();
                            if diff > 0 {
                                if let Some(day) = update.zap_sent_day {
                                    span.min_sent_day =
                                        Some(span.min_sent_day.map_or(day, |v| v.min(day)));
                                    span.max_sent_day =
                                        Some(span.max_sent_day.map_or(day, |v| v.max(day)));
                                }
                                if let Some(day) = update.zap_recd_day {
                                    span.min_recd_day =
                                        Some(span.min_recd_day.map_or(day, |v| v.min(day)));
                                    span.max_recd_day =
                                        Some(span.max_recd_day.map_or(day, |v| v.max(day)));
                                }
                            }
                            recompute_zap_averages(entry, span);
                        } else if has_zap_amt {
                            if let Some(span) = zap_spans.get(&pubkey) {
                                recompute_zap_averages(entry, span);
                            }
                        }
                    }
                },
            );

            let frontier_min = frontiers
                .iter()
                .filter_map(|input| input.frontier().iter().min().copied())
                .min();

            let Some(frontier_min) = frontier_min else {
                retained_cap = None;
                emitted_frontier.clear();
                return;
            };

            let input_frontier = Antichain::from_elem(frontier_min);

            if !PartialOrder::less_than(&emitted_frontier.borrow(), &input_frontier.borrow()) {
                return;
            }

            emitted_frontier.clear();
            emitted_frontier.insert(frontier_min);

            let cap = match retained_cap.as_mut() {
                Some(c) => c,
                None => return,
            };

            let emit_time = frontier_min.saturating_sub(1).max(*cap.time());
            cap.downgrade(&emit_time);

            if dirty_pubkeys.is_empty() {
                cap.downgrade(&frontier_min);
                return;
            }

            let mut output_activated = output_handle.activate();
            let mut updates: Vec<((Node, TrustedUser), u64, Diff)> = Vec::new();

            for pubkey in dirty_pubkeys.drain() {
                let new_val = pending
                    .remove(&pubkey)
                    .unwrap_or_else(|| emitted.get(&pubkey).cloned().unwrap_or_default());
                let old_val = emitted.get(&pubkey).cloned().unwrap_or_default();

                if old_val == new_val {
                    continue;
                }

                if !old_val.is_empty() {
                    updates.push(((pubkey, old_val), emit_time, -1));
                }

                if new_val.is_empty() {
                    emitted.remove(&pubkey);
                    zap_spans.remove(&pubkey);
                } else {
                    updates.push(((pubkey, new_val.clone()), emit_time, 1));
                    emitted.insert(pubkey, new_val);
                }
            }

            if !updates.is_empty() {
                tracing::info!(
                    "[Worker {}] trusted_assertions emitting {} updates at ts={} next_frontier={}",
                    worker_id,
                    updates.len(),
                    emit_time,
                    frontier_min
                );
                output_activated.give(cap, &mut updates);
            }

            cap.downgrade(&frontier_min);
        }
    });

    output_stream.as_collection()
}

#[cfg(test)]
mod tests {
    use super::*;
    use differential_dataflow::input::InputSession;
    use persist::db::PersistStore;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::mpsc::channel;
    use std::sync::{Arc, Mutex};
    use timely::dataflow::operators::Capture;
    use timely::dataflow::operators::capture::Extract;
    use timely::dataflow::operators::probe::Handle as ProbeHandle;
    use types::edges::Edge;
    use types::event::EventRow;

    use crate::dataflow::{DataflowConfig, TimelyConfig};

    const PUBKEY1: Node = 1;
    const PUBKEY2: Node = 2;
    static TEST_DB_COUNTER: AtomicU64 = AtomicU64::new(0);

    fn test_config(trusted_seeds: Vec<Node>) -> DataflowConfig {
        let suffix = TEST_DB_COUNTER.fetch_add(1, Ordering::SeqCst);
        let path: PathBuf = std::env::temp_dir().join(format!(
            "noiad-trusted-assertions2-test-{}-{}",
            std::process::id(),
            suffix
        ));
        let persist = Arc::new(PersistStore::open(&path).expect("failed to open test persist"));

        DataflowConfig {
            timely: TimelyConfig { workers: 4 },
            persist,
            pagerank_iterations: 20,
            pagerank_sink_batch_size: 0,
            replication_max_pending: 0,
            trusted_assertions_nsec: None,
            embedding_import_manifest: None,
            dataset_sink_path: None,
            trusted_seeds,
            trusted_lists_ranks_k: 0,
        }
    }

    fn build_dataflow<FBuild>(
        trusted_seeds: Vec<Node>,
        build_inputs: FBuild,
    ) -> Vec<(Node, TrustedUser, u64, Diff)>
    where
        FBuild: FnOnce(&mut InputSession<u64, EventRow, Diff>) + Send + Sync + 'static,
    {
        let (tx, rx) = channel();
        let build_inputs = Arc::new(Mutex::new(Some(build_inputs)));
        let final_ts = Arc::new(AtomicU64::new(0));
        let config = test_config(trusted_seeds);

        let guards = timely::execute(timely::Config::process(4), move |worker| {
            let mut events_input: InputSession<u64, EventRow, Diff> = InputSession::new();
            let probe = ProbeHandle::new();
            let tx = tx.clone();
            let build_inputs = Arc::clone(&build_inputs);
            let final_ts = Arc::clone(&final_ts);
            let worker_idx = worker.index();
            let config = config.clone();

            worker.dataflow::<u64, _, _>(|scope| {
                let events = events_input.to_collection(scope);
                let (assertions, _) = trusted_assertions(&events, &config);
                assertions.probe_with(&probe).inner.capture_into(tx);
            });

            events_input.advance_to(1);
            if worker_idx == 0 {
                let f = build_inputs
                    .lock()
                    .expect("build_inputs mutex poisoned")
                    .take()
                    .expect("build_inputs should run once");
                f(&mut events_input);
            }
            events_input.flush();

            if worker_idx == 0 {
                let ts = (*events_input.time()).saturating_add(1);
                final_ts.store(ts, Ordering::SeqCst);
            }

            let target_ts = loop {
                let ts = final_ts.load(Ordering::SeqCst);
                if ts > 0 {
                    break ts;
                }
                worker.step();
            };

            events_input.advance_to(target_ts);
            events_input.flush();

            while probe.less_than(&target_ts) {
                worker.step();
            }
        })
        .expect("failed to execute timely workers");

        drop(guards);

        rx.extract()
            .into_iter()
            .flat_map(|(_cap_ts, batch)| {
                batch
                    .into_iter()
                    .map(|((pubkey, res), ts, diff)| (pubkey, res, ts, diff))
            })
            .collect()
    }

    fn latest_assertion_for_pubkey(
        captured: &[(Node, TrustedUser, u64, Diff)],
        pubkey: Node,
    ) -> TrustedUser {
        captured
            .iter()
            .filter(|(p, _res, _ts, diff)| *p == pubkey && *diff > 0)
            .max_by_key(|(_, _res, ts, _)| *ts)
            .map(|(_, res, _, _)| res.clone())
            .expect("expected assertion for pubkey")
    }

    #[test]
    fn assert_reports() {
        let captured = build_dataflow(vec![], |events_input| {
            events_input.insert(EventRow {
                id: 100,
                pubkey: PUBKEY1,
                created_at: 1_000,
                kind: Kind::Reporting.as_u16(),
                edges: vec![Edge::Pubkey(PUBKEY2)],
            });
            events_input.insert(EventRow {
                id: 101,
                pubkey: PUBKEY1,
                created_at: 2_000,
                kind: Kind::Reporting.as_u16(),
                edges: vec![Edge::Pubkey(PUBKEY2)],
            });
            events_input.flush();

            events_input.advance_to(2);
            events_input.remove(EventRow {
                id: 101,
                pubkey: PUBKEY1,
                created_at: 2_000,
                kind: Kind::Reporting.as_u16(),
                edges: vec![Edge::Pubkey(PUBKEY2)],
            });
        });

        let sender = latest_assertion_for_pubkey(&captured, PUBKEY1);
        assert_eq!(sender.reports_cnt_sent, 1);

        let recipient = latest_assertion_for_pubkey(&captured, PUBKEY2);
        assert_eq!(recipient.reports_cnt_recd, 1);
    }

    #[test]
    fn assert_follower_cnt() {
        let captured = build_dataflow(vec![PUBKEY1, 10, 11, 12], |events_input| {
            events_input.insert(EventRow {
                id: 110,
                pubkey: PUBKEY1,
                created_at: 1_000,
                kind: Kind::ContactList.as_u16(),
                edges: vec![
                    Edge::Pubkey(10),
                    Edge::Pubkey(11),
                    Edge::Pubkey(12),
                    Edge::Pubkey(PUBKEY2),
                    Edge::Pubkey(PUBKEY2),
                    Edge::Pubkey(3),
                ],
            });
            events_input.insert(EventRow {
                id: 111,
                pubkey: 10,
                created_at: 1_100,
                kind: Kind::ContactList.as_u16(),
                edges: vec![
                    Edge::Pubkey(PUBKEY1),
                    Edge::Pubkey(11),
                    Edge::Pubkey(12),
                    Edge::Pubkey(PUBKEY2),
                ],
            });
            events_input.insert(EventRow {
                id: 112,
                pubkey: 11,
                created_at: 1_200,
                kind: Kind::ContactList.as_u16(),
                edges: vec![
                    Edge::Pubkey(PUBKEY1),
                    Edge::Pubkey(10),
                    Edge::Pubkey(12),
                    Edge::Pubkey(PUBKEY2),
                ],
            });
            events_input.insert(EventRow {
                id: 113,
                pubkey: 12,
                created_at: 1_300,
                kind: Kind::ContactList.as_u16(),
                edges: vec![
                    Edge::Pubkey(PUBKEY1),
                    Edge::Pubkey(10),
                    Edge::Pubkey(11),
                    Edge::Pubkey(PUBKEY2),
                ],
            });
        });

        let followed_1 = latest_assertion_for_pubkey(&captured, PUBKEY2);
        assert_eq!(followed_1.follower_cnt, 4);

        let followed_2 = latest_assertion_for_pubkey(&captured, 3);
        assert_eq!(followed_2.follower_cnt, 1);
    }

    #[test]
    fn assert_follower_cnt_replacement() {
        let captured = build_dataflow(vec![PUBKEY1, 6], |events_input| {
            events_input.insert(EventRow {
                id: 120,
                pubkey: PUBKEY1,
                created_at: 1_000,
                kind: Kind::ContactList.as_u16(),
                edges: vec![
                    Edge::Pubkey(6),
                    Edge::Pubkey(PUBKEY2),
                    Edge::Pubkey(3),
                    Edge::Pubkey(4),
                ],
            });
            events_input.insert(EventRow {
                id: 122,
                pubkey: 6,
                created_at: 1_100,
                kind: Kind::ContactList.as_u16(),
                edges: vec![Edge::Pubkey(PUBKEY1)],
            });
            events_input.flush();

            events_input.advance_to(2);
            events_input.remove(EventRow {
                id: 120,
                pubkey: PUBKEY1,
                created_at: 1_000,
                kind: Kind::ContactList.as_u16(),
                edges: vec![
                    Edge::Pubkey(6),
                    Edge::Pubkey(PUBKEY2),
                    Edge::Pubkey(3),
                    Edge::Pubkey(4),
                ],
            });
            events_input.insert(EventRow {
                id: 121,
                pubkey: PUBKEY1,
                created_at: 2_000,
                kind: Kind::ContactList.as_u16(),
                edges: vec![
                    Edge::Pubkey(6),
                    Edge::Pubkey(PUBKEY2),
                    Edge::Pubkey(3),
                    Edge::Pubkey(5),
                ],
            });
        });

        let followed_1 = latest_assertion_for_pubkey(&captured, PUBKEY2);
        assert_eq!(followed_1.follower_cnt, 1);

        let followed_2 = latest_assertion_for_pubkey(&captured, 3);
        assert_eq!(followed_2.follower_cnt, 1);

        let followed_3 = latest_assertion_for_pubkey(&captured, 5);
        assert_eq!(followed_3.follower_cnt, 1);

        let removed = captured
            .iter()
            .filter(|(pubkey, _res, _ts, diff)| *pubkey == 4 && *diff > 0)
            .max_by_key(|(_, _res, ts, _)| *ts);
        assert!(removed.is_none());
    }

    #[test]
    fn assert_post_cnt_and_reply_cnt() {
        let counted = build_dataflow(vec![], |events_input| {
            events_input.insert(EventRow {
                id: 200,
                pubkey: PUBKEY1,
                created_at: 1_000,
                kind: Kind::TextNote.as_u16(),
                edges: vec![],
            });
            events_input.insert(EventRow {
                id: 201,
                pubkey: PUBKEY1,
                created_at: 2_000,
                kind: Kind::TextNote.as_u16(),
                edges: vec![Edge::Reply(200)],
            });
        });

        let res = latest_assertion_for_pubkey(&counted, PUBKEY1);
        assert_eq!(res.post_cnt, 1);
        assert_eq!(res.reply_cnt, 1);

        let retracted = build_dataflow(vec![], |events_input| {
            events_input.insert(EventRow {
                id: 200,
                pubkey: PUBKEY1,
                created_at: 1_000,
                kind: Kind::TextNote.as_u16(),
                edges: vec![],
            });
            events_input.insert(EventRow {
                id: 201,
                pubkey: PUBKEY1,
                created_at: 2_000,
                kind: Kind::TextNote.as_u16(),
                edges: vec![Edge::Reply(200)],
            });
            events_input.flush();

            events_input.advance_to(2);
            events_input.remove(EventRow {
                id: 200,
                pubkey: PUBKEY1,
                created_at: 1_000,
                kind: Kind::TextNote.as_u16(),
                edges: vec![],
            });
            events_input.flush();
        });

        let res = latest_assertion_for_pubkey(&retracted, PUBKEY1);
        assert_eq!(res.post_cnt, 0);
        assert_eq!(res.reply_cnt, 1);
    }

    #[test]
    fn assert_zaps() {
        let captured = build_dataflow(vec![], |events_input| {
            events_input.insert(EventRow {
                id: 300,
                pubkey: 9,
                created_at: 1_000,
                kind: Kind::ZapReceipt.as_u16(),
                edges: vec![
                    Edge::Pubkey(PUBKEY2),
                    Edge::PubkeyUpper(PUBKEY1),
                    Edge::Bolt11(1_000),
                ],
            });
        });

        let sender = latest_assertion_for_pubkey(&captured, PUBKEY1);
        assert_eq!(sender.zap_amt_sent, 1_000);
        assert_eq!(sender.zap_cnt_sent, 1);
        assert_eq!(sender.zap_avg_amt_day_sent, 1_000);

        let recipient = latest_assertion_for_pubkey(&captured, PUBKEY2);
        assert_eq!(recipient.zap_amt_recd, 1_000);
        assert_eq!(recipient.zap_cnt_recd, 1);
        assert_eq!(recipient.zap_avg_amt_day_recd, 1_000);
    }

    #[test]
    fn assert_reactions_cnt() {
        let captured = build_dataflow(vec![], |events_input| {
            events_input.insert(EventRow {
                id: 400,
                pubkey: PUBKEY1,
                created_at: 1_000,
                kind: Kind::Reaction.as_u16(),
                edges: vec![Edge::Mention(42)],
            });
            events_input.insert(EventRow {
                id: 401,
                pubkey: PUBKEY1,
                created_at: 2_000,
                kind: Kind::Reaction.as_u16(),
                edges: vec![Edge::Mention(43)],
            });
            events_input.insert(EventRow {
                id: 402,
                pubkey: PUBKEY2,
                created_at: 3_000,
                kind: Kind::Reaction.as_u16(),
                edges: vec![Edge::Mention(44)],
            });
        });

        let res1 = latest_assertion_for_pubkey(&captured, PUBKEY1);
        let res2 = latest_assertion_for_pubkey(&captured, PUBKEY2);

        assert_eq!(res1.reactions_cnt, 2);
        assert_eq!(res2.reactions_cnt, 1);
    }

    #[test]
    fn assert_active_hours() {
        let captured = build_dataflow(vec![], |events_input| {
            events_input.insert(EventRow {
                id: 500,
                pubkey: PUBKEY1,
                created_at: 2 * 3600,
                kind: Kind::TextNote.as_u16(),
                edges: vec![],
            });
            events_input.insert(EventRow {
                id: 501,
                pubkey: PUBKEY1,
                created_at: 14 * 3600,
                kind: Kind::Reaction.as_u16(),
                edges: vec![Edge::Mention(42)],
            });
            events_input.insert(EventRow {
                id: 502,
                pubkey: PUBKEY1,
                created_at: 23 * 3600,
                kind: Kind::Reporting.as_u16(),
                edges: vec![Edge::Pubkey(PUBKEY2)],
            });
        });

        let res = latest_assertion_for_pubkey(&captured, PUBKEY1);
        assert_eq!(res.active_hours_start, Some(2));
        assert_eq!(res.active_hours_end, Some(23));
        let res2 = latest_assertion_for_pubkey(&captured, PUBKEY2);
        assert_eq!(res2.active_hours_start, None);
        assert_eq!(res2.active_hours_end, None);
    }
}
