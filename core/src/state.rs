use crate::{
    query::{
        filter::{DataflowFilter, DataflowFilterIndex},
        trace_iter::TraceIter,
    },
    types::Rank,
};
use differential_dataflow::{
    VecCollection,
    operators::arrange::TraceAgent,
    trace::{Cursor, TraceReader, implementations::ord_neu::OrdValSpine},
};
use nostr_sdk::filter::Filter;
use persist::db::PersistStore;
use std::{cell::RefCell, collections::BTreeSet, sync::Arc};
use timely::progress::Antichain;
use types::{
    edges::TagKey,
    event::EventRow,
    types::{Diff, Node, Timestamp},
};

pub type EventsByIdTrace = TraceAgent<OrdValSpine<Node, EventRow, Timestamp, Diff>>;
pub type EventsByPubkeyTrace = TraceAgent<OrdValSpine<Node, EventRow, Timestamp, Diff>>;
pub type EventsByKindTrace = TraceAgent<OrdValSpine<u16, EventRow, Timestamp, Diff>>;
pub type EventsByTagsTrace = TraceAgent<OrdValSpine<TagKey, Node, Timestamp, Diff>>;

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

    fn trace_ready<Tr>(trace: &mut Tr) -> bool
    where
        Tr: TraceReader<Time = u64> + Clone,
    {
        let mut upper = Antichain::new();
        trace.read_upper(&mut upper);
        !upper.less_equal(&0)
    }

    fn is_ready(&mut self, filter: &DataflowFilter) -> bool {
        let index = filter.get_index();

        match index {
            DataflowFilterIndex::ById => match &mut self.events_by_id {
                Some(trace) => Self::trace_ready(trace),
                None => false,
            },
            DataflowFilterIndex::ByAuthor => match &mut self.events_by_pubkey {
                Some(trace) => Self::trace_ready(trace),
                None => false,
            },
            DataflowFilterIndex::ByKind => match &mut self.events_by_kind {
                Some(trace) => Self::trace_ready(trace),
                None => false,
            },
            DataflowFilterIndex::ByTag => match &mut self.events_by_tags {
                Some(trace) => Self::trace_ready(trace),
                None => false,
            },
        }
    }

    pub fn query(&mut self, filter: &DataflowFilter) -> Result<Vec<EventRow>, QueryError> {
        if !self.is_ready(filter) {
            tracing::warn!("State not ready for query: {:?}", filter);
            return Err(QueryError::NotReady);
        }

        let index = filter.get_index();
        match index {
            DataflowFilterIndex::ById => match &mut self.events_by_id {
                Some(trace) => Ok(TraceIter::new(trace, filter.clone()).collect()),
                None => Err(QueryError::NoIndex),
            },
            DataflowFilterIndex::ByAuthor => match &mut self.events_by_pubkey {
                Some(trace) => Ok(TraceIter::new(trace, filter.clone()).collect()),
                None => Err(QueryError::NoIndex),
            },
            DataflowFilterIndex::ByKind => match &mut self.events_by_kind {
                Some(trace) => Ok(TraceIter::new(trace, filter.clone()).collect()),
                None => Err(QueryError::NoIndex),
            },
            DataflowFilterIndex::ByTag => {
                let events_by_tagkey = self.events_by_tags.as_mut().ok_or(QueryError::NoIndex)?;
                let events_by_id = self.events_by_id.as_mut().ok_or(QueryError::NoIndex)?;

                let event_ids: Vec<Node> =
                    TraceIter::new(events_by_tagkey, filter.clone()).collect();

                let id_filter = filter
                    .clone()
                    .ids(event_ids)
                    .limit(filter.limit.unwrap_or(500));

                Ok(TraceIter::new(events_by_id, id_filter).collect())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::filter::{DataflowFilter, DataflowFilterTags};
    use differential_dataflow::{
        AsCollection, input::InputSession, operators::arrange::ArrangeByKey,
    };
    use std::sync::atomic::{AtomicU64, Ordering};
    use timely::{
        dataflow::{
            ProbeHandle,
            operators::{Probe, ToStream},
        },
        execute_directly,
    };
    use types::{event_row, tags::EventTag};

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

    fn build_dataflow<FBuild>(filter: DataflowFilter, build_inputs: FBuild) -> Vec<EventRow>
    where
        FBuild: FnOnce(&mut InputSession<u64, EventRow, Diff>) + Send + Sync + 'static,
    {
        execute_directly(move |worker| {
            let mut input: InputSession<u64, EventRow, Diff> = InputSession::new();
            let mut probe = ProbeHandle::<u64>::new();
            let persist = Arc::new(PersistStore::open(make_temp_persist_path()).unwrap());
            let mut state = State::new(persist);

            let (mut events_by_id, mut events_by_pubkey, mut events_by_kind, mut events_by_tags) =
                worker.dataflow::<u64, _, _>(|scope| {
                    let events = input.to_collection(scope).probe_with(&probe);
                    (
                        events.map(|e| (e.id, e)).arrange_by_key().trace.clone(),
                        events.map(|e| (e.pubkey, e)).arrange_by_key().trace.clone(),
                        events.map(|e| (e.kind, e)).arrange_by_key().trace.clone(),
                        events
                            .flat_map(|e| e.tag_keys())
                            .arrange_by_key()
                            .trace
                            .clone(),
                    )
                });

            input.advance_to(1);
            build_inputs(&mut input);
            input.flush();

            let seal = *input.time() + 1;
            input.advance_to(seal);
            input.flush();
            while probe.less_than(&seal) {
                worker.step();
            }

            state.set_events_by_id(events_by_id);
            state.set_events_by_author(events_by_pubkey);
            state.set_events_by_kind(events_by_kind);
            state.set_events_by_tags(events_by_tags);

            state.query(&filter).unwrap()
        })
    }

    #[test]
    fn assert_author_index() {
        let filter = DataflowFilter::default().authors(vec![10, 30]);

        let mut result = build_dataflow(filter, |input| {
            input.insert(event_row!(id: 1, pubkey: 10, created_at: 1000));
            input.insert(event_row!(id: 2, pubkey: 20, created_at: 2000));
            input.insert(event_row!(id: 3, pubkey: 30, created_at: 3000));
        });
        result.sort_by_key(|e| e.id);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].id, 1);
        assert_eq!(result[1].id, 3);
    }

    #[test]
    fn assert_id_index() {
        let mut result = build_dataflow(DataflowFilter::default().ids(vec![2, 3]), |input| {
            input.insert(event_row!(id: 1, pubkey: 10, kind: 1, created_at: 1000));
            input.insert(event_row!(id: 2, pubkey: 20, kind: 1, created_at: 2000));
            input.insert(event_row!(id: 3, pubkey: 30, kind: 3, created_at: 3000));
        });

        result.sort_by_key(|e| e.id);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].id, 2);
        assert_eq!(result[1].id, 3);
    }

    #[test]
    fn assert_kind_index() {
        let mut result = build_dataflow(DataflowFilter::default().kinds(vec![1]), |input| {
            input.insert(event_row!(id: 1, pubkey: 10, kind: 1, created_at: 1000));
            input.insert(event_row!(id: 2, pubkey: 20, kind: 3, created_at: 2000));
            input.insert(event_row!(id: 3, pubkey: 30, kind: 1, created_at: 3000));
        });

        result.sort_by_key(|e| e.id);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].id, 1);
        assert_eq!(result[1].id, 3);
    }

    #[test]
    fn assert_tag_index() {
        let filter = DataflowFilter::default()
            .add_tags(DataflowFilterTags::Pubkey, vec![100].into_iter().collect());

        let mut result = build_dataflow(filter, |input| {
            input.insert(event_row!(
                id: 1,
                pubkey: 10,
                kind: 1,
                created_at: 1000,
                tags: vec![EventTag::Pubkey(100)]
            ));
            input.insert(event_row!(
                id: 2,
                pubkey: 20,
                kind: 1,
                created_at: 2000,
                tags: vec![EventTag::Pubkey(200)]
            ));
            input.insert(event_row!(
                id: 3,
                pubkey: 30,
                kind: 1,
                created_at: 3000,
                tags: vec![EventTag::Topic(999), EventTag::Pubkey(100)]
            ));
        });

        result.sort_by_key(|e| e.id);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].id, 1);
        assert_eq!(result[1].id, 3);
    }
}
