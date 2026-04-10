use crate::query::{
    filter::{DataflowFilter, DataflowFilterIndex},
    trace_iter::{PubkeyKey, TraceIter},
};
use differential_dataflow::{
    Hashable, VecCollection,
    operators::arrange::{ArrangeByKey, TraceAgent, arrangement::arrange_core},
    trace::{
        TraceReader,
        implementations::{ValBatcher, ValBuilder, ValSpine, ord_neu::OrdValSpine},
    },
};
use persist::db::PersistStore;
use std::sync::Arc;
use timely::dataflow::{Scope, channels::pact::Pipeline, operators::Exchange};
use timely::progress::Antichain;
use types::{
    event::{EventRow, EventRowCompactValue},
    tags::TagKey,
    types::{Diff, Node, Timestamp},
};

fn arrange_secondary_index<G, K>(
    collection: VecCollection<G, (K, Node), Diff>,
    name: &str,
) -> TraceAgent<OrdValSpine<K, Node, Timestamp, Diff>>
where
    G: Scope<Timestamp = Timestamp>,
    K: differential_dataflow::ExchangeData + Hashable + Ord,
{
    let exchanged = collection.inner.exchange(|update| {
        let event_id = (update.0).1;
        event_id.hashed().into()
    });

    arrange_core::<
        _,
        _,
        ValBatcher<_, _, _, _>,
        ValBuilder<_, _, _, _>,
        ValSpine<K, Node, _, _>,
    >(&exchanged, Pipeline, name)
    .trace
}

pub type EventsByIdTrace = TraceAgent<OrdValSpine<Node, EventRowCompactValue, Timestamp, Diff>>;
pub type EventsByPubkeyTrace = TraceAgent<OrdValSpine<PubkeyKey, Node, Timestamp, Diff>>;
pub type EventsByKindTrace = TraceAgent<OrdValSpine<u16, Node, Timestamp, Diff>>;
pub type EventsByTagsTrace = TraceAgent<OrdValSpine<TagKey, Node, Timestamp, Diff>>;

pub struct QueryArrangements {
    pub events_by_id: EventsByIdTrace,
    pub events_by_pubkey: EventsByPubkeyTrace,
    pub events_by_kind: EventsByKindTrace,
    pub events_by_tags: EventsByTagsTrace,
}

pub struct QueryDataflow<G: Scope<Timestamp = Timestamp>> {
    pub query_events: VecCollection<G, (Node, EventRowCompactValue), Diff>,
    pub event_tags: VecCollection<G, (Node, TagKey), Diff>,
    pub arrangements: QueryArrangements,
}

pub fn build_query_dataflow<G>(
    events: &VecCollection<G, EventRow, Diff>,
    query_kinds: Vec<u16>,
) -> QueryDataflow<G>
where
    G: Scope<Timestamp = Timestamp>,
{
    let query_event_kinds = query_kinds.clone();
    let query_events = events.flat_map(move |event| {
        (query_event_kinds.is_empty() || query_event_kinds.contains(&event.kind))
            .then_some((event.id, event.compact()))
            .into_iter()
            .collect::<Vec<_>>()
    });

    let event_tags = events.flat_map(move |event| {
        if !query_kinds.is_empty() && !query_kinds.contains(&event.kind) {
            return Vec::new();
        }

        let event_id = event.id;
        event
            .edges
            .into_iter()
            .filter_map(move |edge| TagKey::try_from(edge).ok().map(|tag| (event_id, tag)))
            .collect::<Vec<_>>()
    });

    let events_by_id_arrangement = query_events
        .map(|(id, compact)| (id, compact))
        .arrange_by_key();

    let events_by_pubkey_arrangement = arrange_secondary_index(
        query_events.map(|(id, compact)| (PubkeyKey(compact.pubkey), id)),
        "EventsByPubkey",
    );

    let events_by_kind_arrangement =
        arrange_secondary_index(query_events.map(|(id, compact)| (compact.kind, id)), "EventsByKind");

    let events_by_tags_arrangement =
        arrange_secondary_index(event_tags.map(|(event_id, tag)| (tag, event_id)), "EventsByTags");

    QueryDataflow {
        query_events,
        event_tags,
        arrangements: QueryArrangements {
            events_by_id: events_by_id_arrangement.trace,
            events_by_pubkey: events_by_pubkey_arrangement,
            events_by_kind: events_by_kind_arrangement,
            events_by_tags: events_by_tags_arrangement,
        },
    }
}

#[derive(Debug)]
pub enum QueryError {
    NotReady,
    NoIndex,
}

pub struct State {
    pub persist: Arc<PersistStore>,
    pub events_by_id: Option<EventsByIdTrace>,
    pub events_by_pubkey: Option<EventsByPubkeyTrace>,
    pub events_by_kind: Option<EventsByKindTrace>,
    pub events_by_tags: Option<EventsByTagsTrace>,
}

impl State {
    pub fn new(persist: Arc<PersistStore>) -> Self {
        Self {
            persist,
            events_by_id: None,
            events_by_pubkey: None,
            events_by_kind: None,
            events_by_tags: None,
        }
    }

    pub fn set_events_by_id(&mut self, trace: EventsByIdTrace) {
        self.events_by_id = Some(trace);
    }

    pub fn set_events_by_author(&mut self, trace: EventsByPubkeyTrace) {
        self.events_by_pubkey = Some(trace);
    }

    pub fn set_events_by_kind(&mut self, trace: EventsByKindTrace) {
        self.events_by_kind = Some(trace);
    }

    pub fn set_events_by_tags(&mut self, trace: EventsByTagsTrace) {
        self.events_by_tags = Some(trace);
    }

    pub fn set_query_arrangements(&mut self, arrangements: QueryArrangements) {
        self.events_by_id = Some(arrangements.events_by_id);
        self.events_by_pubkey = Some(arrangements.events_by_pubkey);
        self.events_by_kind = Some(arrangements.events_by_kind);
        self.events_by_tags = Some(arrangements.events_by_tags);
    }

    fn trace_ready_at<Tr>(trace: &mut Tr, time: u64) -> bool
    where
        Tr: TraceReader<Time = u64> + Clone,
    {
        let mut upper = Antichain::new();
        trace.read_upper(&mut upper);
        !upper.less_equal(&time)
    }

    fn is_ready(&mut self, filter: &DataflowFilter) -> bool {
        self.is_ready_at(filter, 0)
    }

    fn is_ready_at(&mut self, filter: &DataflowFilter, time: u64) -> bool {
        let index = filter.get_index();

        match index {
            DataflowFilterIndex::ById => match &mut self.events_by_id {
                Some(trace) => Self::trace_ready_at(trace, time),
                None => false,
            },
            DataflowFilterIndex::ByAuthor => {
                match (&mut self.events_by_pubkey, &mut self.events_by_id) {
                    (Some(secondary), Some(ids)) => {
                        Self::trace_ready_at(secondary, time) && Self::trace_ready_at(ids, time)
                    }
                    _ => false,
                }
            }
            DataflowFilterIndex::ByKind => {
                match (&mut self.events_by_kind, &mut self.events_by_id) {
                    (Some(secondary), Some(ids)) => {
                        Self::trace_ready_at(secondary, time) && Self::trace_ready_at(ids, time)
                    }
                    _ => false,
                }
            }
            DataflowFilterIndex::ByTag => {
                match (&mut self.events_by_tags, &mut self.events_by_id) {
                    (Some(secondary), Some(ids)) => {
                        Self::trace_ready_at(secondary, time) && Self::trace_ready_at(ids, time)
                    }
                    _ => false,
                }
            }
        }
    }

    fn candidate_budget(limit: Option<usize>) -> Option<usize> {
        limit.map(|limit| limit.saturating_mul(128).max(128))
    }

    fn collect_candidate_budget<I>(iter: I, limit: Option<usize>) -> Vec<Node>
    where
        I: Iterator<Item = Node>,
    {
        match Self::candidate_budget(limit) {
            Some(candidate_budget) => iter.take(candidate_budget).collect(),
            None => iter.collect(),
        }
    }

    fn secondary_only_filter(filter: &DataflowFilter, index: DataflowFilterIndex) -> bool {
        // Applying the client limit to the secondary scan is only safe when the
        // selected secondary index already satisfies the whole compact filter.
        // Mixed filters must refine through events_by_id before limiting.
        if filter.ids.is_some() || filter.since.is_some() || filter.until.is_some() {
            return false;
        }

        match index {
            DataflowFilterIndex::ById => false,
            DataflowFilterIndex::ByAuthor => {
                filter.has_authors() && !filter.has_kinds() && !filter.has_tags()
            }
            DataflowFilterIndex::ByKind => {
                filter.has_kinds() && !filter.has_authors() && !filter.has_tags()
            }
            DataflowFilterIndex::ByTag => {
                filter.has_tags() && !filter.has_authors() && !filter.has_kinds()
            }
        }
    }

    fn refine_candidates(
        events_by_id: &mut EventsByIdTrace,
        filter: &DataflowFilter,
        candidate_ids: Vec<Node>,
    ) -> Vec<Node> {
        if candidate_ids.is_empty() {
            return Vec::new();
        }

        let id_filter = filter.clone().ids(candidate_ids);
        Self::collect_candidate_budget(
            TraceIter::new(events_by_id, id_filter).map(|(id, _)| id),
            filter.limit,
        )
    }

    pub fn query(&mut self, filter: &DataflowFilter) -> Result<Vec<Node>, QueryError> {
        if !self.is_ready(filter) {
            return Err(QueryError::NotReady);
        }

        match filter.get_index() {
            DataflowFilterIndex::ById => match &mut self.events_by_id {
                Some(trace) => Ok(Self::collect_candidate_budget(
                    TraceIter::new(trace, filter.clone()).map(|(id, _)| id),
                    filter.limit,
                )),
                None => Err(QueryError::NoIndex),
            },
            DataflowFilterIndex::ByAuthor => match &mut self.events_by_pubkey {
                Some(trace) => {
                    if Self::secondary_only_filter(filter, DataflowFilterIndex::ByAuthor) {
                        return Ok(Self::collect_candidate_budget(
                            TraceIter::new(trace, filter.clone()),
                            filter.limit,
                        ));
                    }

                    let candidate_ids = TraceIter::new(trace, filter.clone()).collect::<Vec<_>>();
                    match &mut self.events_by_id {
                        Some(trace) => Ok(Self::refine_candidates(trace, filter, candidate_ids)),
                        None => Err(QueryError::NoIndex),
                    }
                }
                None => Err(QueryError::NoIndex),
            },
            DataflowFilterIndex::ByKind => match &mut self.events_by_kind {
                Some(trace) => {
                    if Self::secondary_only_filter(filter, DataflowFilterIndex::ByKind) {
                        return Ok(Self::collect_candidate_budget(
                            TraceIter::new(trace, filter.clone()),
                            filter.limit,
                        ));
                    }

                    let candidate_ids = TraceIter::new(trace, filter.clone()).collect::<Vec<_>>();
                    match &mut self.events_by_id {
                        Some(trace) => Ok(Self::refine_candidates(trace, filter, candidate_ids)),
                        None => Err(QueryError::NoIndex),
                    }
                }
                None => Err(QueryError::NoIndex),
            },
            DataflowFilterIndex::ByTag => match &mut self.events_by_tags {
                Some(trace) => {
                    if Self::secondary_only_filter(filter, DataflowFilterIndex::ByTag) {
                        return Ok(Self::collect_candidate_budget(
                            TraceIter::new(trace, filter.clone()),
                            filter.limit,
                        ));
                    }

                    let candidate_ids = TraceIter::new(trace, filter.clone()).collect::<Vec<_>>();
                    match &mut self.events_by_id {
                        Some(trace) => Ok(Self::refine_candidates(trace, filter, candidate_ids)),
                        None => Err(QueryError::NoIndex),
                    }
                }
                None => Err(QueryError::NoIndex),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        query::filter::DataflowFilter,
        state::{State, build_query_dataflow},
    };
    use differential_dataflow::input::InputSession;
    use persist::db::PersistStore;
    use std::sync::mpsc::channel;
    use std::sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    };
    use timely::{Config, dataflow::ProbeHandle, execute};
    use types::{
        edges::Edge,
        event::EventRow,
        event_row,
        tags::TagLabel,
        types::{Diff, Node},
    };

    static TEST_DB_COUNTER: AtomicU64 = AtomicU64::new(0);

    fn make_temp_persist_path() -> std::path::PathBuf {
        use std::time::{SystemTime, UNIX_EPOCH};
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let seq = TEST_DB_COUNTER.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!("noiad_state_test_{nanos}_{seq}"))
    }

    fn build_dataflow<FBuild>(filter: DataflowFilter, build_inputs: FBuild) -> Vec<Node>
    where
        FBuild: FnOnce(&mut InputSession<u64, EventRow, Diff>) + Send + Sync + 'static,
    {
        use std::sync::Mutex;

        let (tx, rx) = channel();
        let build_inputs = Arc::new(Mutex::new(Some(build_inputs)));
        let tx_for_workers = tx.clone();

        let guards = execute(Config::process(2), move |worker| {
            let mut input: InputSession<u64, EventRow, Diff> = InputSession::new();
            let probe = ProbeHandle::<u64>::new();
            let persist = Arc::new(PersistStore::open(make_temp_persist_path()).unwrap());
            let mut state = State::new(persist);
            let tx = tx_for_workers.clone();
            let build_inputs = Arc::clone(&build_inputs);
            let worker_idx = worker.index();

            let arrangements = worker.dataflow::<u64, _, _>(|scope| {
                let events = input.to_collection(scope).probe_with(&probe);
                build_query_dataflow(&events, Vec::new()).arrangements
            });

            input.advance_to(1);
            if worker_idx == 0 {
                let f = build_inputs
                    .lock()
                    .expect("build_inputs mutex poisoned")
                    .take()
                    .expect("build_inputs should run once");
                f(&mut input);
            }
            input.flush();

            let seal = (*input.time()).max(2) + 1;
            input.advance_to(seal);
            input.flush();
            while probe.less_than(&seal) {
                worker.step();
            }

            state.set_query_arrangements(arrangements);

            let last_update_time = seal.saturating_sub(1);
            while !state.is_ready_at(&filter, last_update_time) {
                worker.step();
            }

            tx.send(state.query(&filter).unwrap()).unwrap();
        })
        .expect("failed to execute timely workers");

        drop(guards);
        drop(tx);
        rx.into_iter().flatten().collect()
    }

    #[test]
    fn assert_author_index() {
        let filter = DataflowFilter::default().authors(vec![10, 30]);

        let mut result = build_dataflow(filter, |input| {
            input.insert(event_row!(id: 1, pubkey: 10, created_at: 1000));
            input.insert(event_row!(id: 2, pubkey: 20, created_at: 2000));
            input.insert(event_row!(id: 3, pubkey: 30, created_at: 3000));
        });
        result.sort();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], 1);
        assert_eq!(result[1], 3);
    }

    #[test]
    fn assert_id_index() {
        let mut result = build_dataflow(DataflowFilter::default().ids(vec![2, 3]), |input| {
            input.insert(event_row!(id: 1, pubkey: 10, kind: 1, created_at: 1000));
            input.insert(event_row!(id: 2, pubkey: 20, kind: 1, created_at: 2000));
            input.insert(event_row!(id: 3, pubkey: 30, kind: 3, created_at: 3000));
        });

        result.sort();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], 2);
        assert_eq!(result[1], 3);
    }

    #[test]
    fn assert_kind_index() {
        let mut result = build_dataflow(DataflowFilter::default().kinds(vec![1]), |input| {
            input.insert(event_row!(id: 1, pubkey: 10, kind: 1, created_at: 1000));
            input.insert(event_row!(id: 2, pubkey: 20, kind: 3, created_at: 2000));
            input.insert(event_row!(id: 3, pubkey: 30, kind: 1, created_at: 3000));
        });

        result.sort();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], 1);
        assert_eq!(result[1], 3);
    }

    #[test]
    fn assert_kind_limit_uses_bounded_candidate_budget_per_worker() {
        let mut result = build_dataflow(
            DataflowFilter::default().kinds(vec![30382]).limit(1),
            |input| {
                for id in 1..=400 {
                    input.insert(event_row!(
                        id: id,
                        pubkey: 10_000 + id,
                        kind: 30382,
                        created_at: 1_000 + id,
                    ));
                }
            },
        );

        result.sort();
        result.dedup();

        assert!(!result.is_empty());
        assert!(result.len() <= 256);
    }

    #[test]
    fn assert_tag_index() {
        let filter =
            DataflowFilter::default().add_tags(TagLabel::Pubkey, vec![100].into_iter().collect());

        let mut result = build_dataflow(filter, |input| {
            input.insert(event_row!(
                id: 1,
                pubkey: 10,
                kind: 1,
                created_at: 1000,
                edges: vec![Edge::Pubkey(100)]
            ));
            input.insert(event_row!(
                id: 2,
                pubkey: 20,
                kind: 1,
                created_at: 2000,
                edges: vec![Edge::Pubkey(200)]
            ));
            input.insert(event_row!(
                id: 3,
                pubkey: 30,
                kind: 1,
                created_at: 3000,
                edges: vec![Edge::Topic(999), Edge::Pubkey(100)]
            ));
        });

        result.sort();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], 1);
        assert_eq!(result[1], 3);
    }

    #[test]
    fn assert_tag_and_kind_filter() {
        let filter = DataflowFilter::default()
            .kinds(vec![30392])
            .add_tags(TagLabel::DTag, vec![100].into_iter().collect());

        let mut result = build_dataflow(filter, |input| {
            input.insert(event_row!(
                id: 1,
                pubkey: 10,
                kind: 30382,
                created_at: 1000,
                edges: vec![Edge::DTag(100)]
            ));
            input.insert(event_row!(
                id: 2,
                pubkey: 20,
                kind: 30392,
                created_at: 2000,
                edges: vec![Edge::DTag(100)]
            ));
            input.insert(event_row!(
                id: 3,
                pubkey: 30,
                kind: 30392,
                created_at: 3000,
                edges: vec![Edge::DTag(200)]
            ));
        });

        result.sort();
        assert_eq!(result, vec![2]);
    }

    #[test]
    fn assert_addressable_replacements_retract_old_ids() {
        let filter = DataflowFilter::default()
            .kinds(vec![30382])
            .add_tags(TagLabel::DTag, vec![100].into_iter().collect());

        let mut result = build_dataflow(filter, |input| {
            let old_1 = event_row!(
                id: 1,
                pubkey: 10,
                kind: 30382,
                created_at: 1000,
                edges: vec![Edge::DTag(100)]
            );
            let old_2 = event_row!(
                id: 2,
                pubkey: 20,
                kind: 30382,
                created_at: 1000,
                edges: vec![Edge::DTag(100)]
            );
            let new_1 = event_row!(
                id: 101,
                pubkey: 10,
                kind: 30382,
                created_at: 2000,
                edges: vec![Edge::DTag(100)]
            );
            let new_2 = event_row!(
                id: 102,
                pubkey: 20,
                kind: 30382,
                created_at: 2000,
                edges: vec![Edge::DTag(100)]
            );

            input.insert(old_1.clone());
            input.insert(old_2.clone());
            input.flush();
            input.advance_to(2);
            input.remove(old_1);
            input.remove(old_2);
            input.insert(new_1);
            input.insert(new_2);
            input.flush();
        });

        result.sort();
        assert_eq!(result, vec![101, 102]);
    }

    #[test]
    fn assert_tag_and_kind_limit_does_not_drop_later_match() {
        let filter = DataflowFilter::default()
            .kinds(vec![30392])
            .limit(1)
            .add_tags(TagLabel::DTag, vec![100].into_iter().collect());

        let mut result = build_dataflow(filter, |input| {
            input.insert(event_row!(
                id: 1,
                pubkey: 10,
                kind: 30382,
                created_at: 1000,
                edges: vec![Edge::DTag(100)]
            ));
            input.insert(event_row!(
                id: 2,
                pubkey: 20,
                kind: 30392,
                created_at: 2000,
                edges: vec![Edge::DTag(100)]
            ));
        });

        result.sort();
        assert_eq!(result, vec![2]);
    }

    #[test]
    fn assert_author_and_kind_filter() {
        let filter = DataflowFilter::default()
            .authors(vec![10])
            .kinds(vec![30392]);

        let mut result = build_dataflow(filter, |input| {
            input.insert(event_row!(
                id: 1,
                pubkey: 10,
                kind: 30382,
                created_at: 1000,
            ));
            input.insert(event_row!(
                id: 2,
                pubkey: 10,
                kind: 30392,
                created_at: 2000,
            ));
            input.insert(event_row!(
                id: 3,
                pubkey: 20,
                kind: 30392,
                created_at: 3000,
            ));
        });

        result.sort();
        assert_eq!(result, vec![2]);
    }

    #[test]
    fn assert_kind_index_multiworker() {
        let mut result = build_dataflow(DataflowFilter::default().kinds(vec![30382]), |input| {
            for id in 1..=32 {
                input.insert(event_row!(
                    id: id,
                    pubkey: 10_000 + id,
                    kind: 30382,
                    created_at: 1_000 + id,
                ));
            }
        });

        result.sort();
        result.dedup();

        assert_eq!(result.len(), 32);
    }
}
