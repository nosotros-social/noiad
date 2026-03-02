use differential_dataflow::trace::{Cursor, TraceReader};
use std::{collections::HashSet, marker::PhantomData};
use types::{edges::TagKey, event::EventRow, types::Node};

use crate::query::filter::{DataflowFilter, DataflowFilterIndex, DataflowFilterTags};

pub struct TraceIter<Tr, K>
where
    Tr: TraceReader,
{
    filter: DataflowFilter,
    storage: Tr::Storage,
    cursor: Tr::Cursor,
    key_index: usize,
    count: usize,
    started: bool,
    seen: HashSet<Node>,
    phantom: PhantomData<K>,
}

impl<Tr, K> TraceIter<Tr, K>
where
    for<'a> Tr: TraceReader<KeyOwn: Ord>,
{
    pub fn new(trace: &mut Tr, filter: DataflowFilter) -> Self {
        let (mut cursor, storage) = trace.cursor();
        cursor.rewind_keys(&storage);
        cursor.rewind_vals(&storage);
        Self {
            filter,
            storage,
            cursor,
            count: 0,
            key_index: 0,
            started: false,
            seen: HashSet::new(),
            phantom: PhantomData,
        }
    }
}

trait TraceKeyIndex<Tr>
where
    Tr: TraceReader,
{
    fn seek_current_key(&mut self) -> bool;
    fn keys_len(&self) -> usize;
}

/// ID and Author index
impl<Tr> TraceKeyIndex<Tr> for TraceIter<Tr, Node>
where
    for<'a> Tr: TraceReader<Key<'a> = &'a Node, Val<'a> = &'a EventRow>,
{
    fn seek_current_key(&mut self) -> bool {
        let keys = match self.filter.get_index() {
            DataflowFilterIndex::ById => self.filter.ids.as_ref(),
            DataflowFilterIndex::ByAuthor => self.filter.authors.as_ref(),
            _ => return false,
        };
        let Some(key) = keys.and_then(|k| k.iter().nth(self.key_index)) else {
            return false;
        };
        self.cursor.seek_key(&self.storage, key);

        if self.cursor.key_valid(&self.storage) && self.cursor.key(&self.storage) == key {
            self.cursor.rewind_vals(&self.storage);
            return true;
        }
        false
    }

    fn keys_len(&self) -> usize {
        match self.filter.get_index() {
            DataflowFilterIndex::ById => self.filter.ids.as_ref().map_or(0, |k| k.len()),
            DataflowFilterIndex::ByAuthor => self.filter.authors.as_ref().map_or(0, |k| k.len()),
            _ => 0,
        }
    }
}

/// TraceyKey for Kind index
impl<Tr> TraceKeyIndex<Tr> for TraceIter<Tr, u16>
where
    for<'a> Tr: TraceReader<Key<'a> = &'a u16, Val<'a> = &'a EventRow>,
{
    fn seek_current_key(&mut self) -> bool {
        let keys = match self.filter.get_index() {
            DataflowFilterIndex::ByKind => self.filter.kinds.as_ref(),
            _ => return false,
        };
        let Some(key) = keys.and_then(|k| k.iter().nth(self.key_index)) else {
            return false;
        };
        self.cursor.seek_key(&self.storage, key);

        if self.cursor.key_valid(&self.storage) && self.cursor.key(&self.storage) == key {
            self.cursor.rewind_vals(&self.storage);
            return true;
        }
        false
    }

    fn keys_len(&self) -> usize {
        self.filter.kinds.as_ref().map_or(0, |k| k.len())
    }
}

impl<Tr> TraceKeyIndex<Tr> for TraceIter<Tr, TagKey>
where
    for<'a> Tr: TraceReader<Key<'a> = &'a TagKey, Val<'a> = &'a Node>,
{
    fn seek_current_key(&mut self) -> bool {
        let keys = match self.filter.get_index() {
            DataflowFilterIndex::ByTag => self.filter.tag_keys(),
            _ => return false,
        };
        let Some(ref keys) = keys else {
            return false;
        };
        let Some(key) = keys.iter().nth(self.key_index) else {
            return false;
        };
        self.cursor.seek_key(&self.storage, key);

        if self.cursor.key_valid(&self.storage) && self.cursor.key(&self.storage) == key {
            self.cursor.rewind_vals(&self.storage);
            return true;
        }
        false
    }

    fn keys_len(&self) -> usize {
        match self.filter.get_index() {
            DataflowFilterIndex::ByTag => self.filter.tag_keys().map_or(0, |k| k.len()),
            _ => 0,
        }
    }
}

pub trait TraceValueBehavior<Tr>
where
    Tr: TraceReader,
{
    type Item: Clone;

    fn read_item(&self) -> Self::Item;
    fn dedup_key(item: &Self::Item) -> Node;
    fn matches_filter(&self, item: &Self::Item) -> bool;
    fn needs_dedup(&self) -> bool {
        false
    }
}

impl<Tr> TraceValueBehavior<Tr> for TraceIter<Tr, Node>
where
    for<'a> Tr: TraceReader<Key<'a> = &'a Node, Val<'a> = &'a EventRow>,
{
    type Item = EventRow;

    fn read_item(&self) -> Self::Item {
        self.cursor.val(&self.storage).clone()
    }

    fn dedup_key(item: &Self::Item) -> Node {
        item.id
    }

    fn matches_filter(&self, item: &Self::Item) -> bool {
        self.filter.matches(item)
    }
}

impl<Tr> TraceValueBehavior<Tr> for TraceIter<Tr, u16>
where
    for<'a> Tr: TraceReader<Key<'a> = &'a u16, Val<'a> = &'a EventRow>,
{
    type Item = EventRow;

    fn read_item(&self) -> Self::Item {
        self.cursor.val(&self.storage).clone()
    }

    fn dedup_key(item: &Self::Item) -> Node {
        item.id
    }

    fn matches_filter(&self, item: &Self::Item) -> bool {
        self.filter.matches(item)
    }
}

impl<Tr> TraceValueBehavior<Tr> for TraceIter<Tr, TagKey>
where
    for<'a> Tr: TraceReader<Key<'a> = &'a TagKey, Val<'a> = &'a Node>,
{
    type Item = Node;

    fn read_item(&self) -> Self::Item {
        *self.cursor.val(&self.storage)
    }

    fn dedup_key(item: &Self::Item) -> Node {
        *item
    }

    fn matches_filter(&self, _item: &Self::Item) -> bool {
        true
    }

    fn needs_dedup(&self) -> bool {
        true
    }
}

trait DiffToI64: Copy {
    fn to_i64(self) -> i64;
}

impl DiffToI64 for &isize {
    fn to_i64(self) -> i64 {
        *self as i64
    }
}

impl<Tr, K> Iterator for TraceIter<Tr, K>
where
    Tr: TraceReader,
    for<'a> Tr::DiffGat<'a>: DiffToI64,
    Self: TraceKeyIndex<Tr> + TraceValueBehavior<Tr>,
{
    type Item = <Self as TraceValueBehavior<Tr>>::Item;

    fn next(&mut self) -> Option<Self::Item> {
        if self.count >= self.filter.limit.unwrap_or(500) {
            return None;
        }

        if !self.started {
            self.started = true;
            while self.key_index < self.keys_len() && !self.seek_current_key() {
                self.key_index += 1;
            }
        }

        loop {
            if self.key_index >= self.keys_len() {
                return None;
            }

            if !self.cursor.key_valid(&self.storage) {
                return None;
            }

            if !self.cursor.val_valid(&self.storage) {
                self.key_index += 1;
                while self.key_index < self.keys_len() && !self.seek_current_key() {
                    self.key_index += 1;
                }
                continue;
            }

            let item = self.read_item();

            let mut diff_sum = 0i64;
            self.cursor.map_times(&self.storage, |_time, diff| {
                diff_sum += diff.to_i64();
            });

            self.cursor.step_val(&self.storage);

            if diff_sum <= 0 {
                continue;
            }

            if self.needs_dedup() {
                let key = Self::dedup_key(&item);
                if self.seen.contains(&key) {
                    continue;
                }
                self.seen.insert(key);
            }

            if !self.matches_filter(&item) {
                continue;
            }

            self.count += 1;
            return Some(item);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use differential_dataflow::{
        AsCollection, input::InputSession, operators::arrange::ArrangeByKey,
    };
    use timely::{
        dataflow::{
            ProbeHandle,
            operators::{Probe, ToStream},
        },
        execute_directly,
    };
    use types::{event_row, tags::EventTag, types::Diff};

    pub fn build_dataflow<FBuild>(filter: DataflowFilter, build_inputs: FBuild) -> Vec<EventRow>
    where
        FBuild: FnOnce(&mut InputSession<u64, EventRow, Diff>) + Send + Sync + 'static,
    {
        execute_directly(move |worker| {
            let mut events_input: InputSession<u64, EventRow, Diff> = InputSession::new();
            let mut probe = ProbeHandle::<u64>::new();
            let index = filter.get_index();

            let (mut events_by_id, mut events_by_pubkey, mut events_by_kind, mut events_by_tags) =
                worker.dataflow::<u64, _, _>(|scope| {
                    let events = events_input.to_collection(scope).probe_with(&probe);

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

            events_input.advance_to(1);
            build_inputs(&mut events_input);
            events_input.flush();

            let seal = *events_input.time() + 1;
            events_input.advance_to(seal);
            events_input.flush();
            while probe.less_than(&seal) {
                worker.step();
            }

            match index {
                DataflowFilterIndex::ById => TraceIter::new(&mut events_by_id, filter).collect(),
                DataflowFilterIndex::ByAuthor => {
                    TraceIter::new(&mut events_by_pubkey, filter).collect()
                }
                DataflowFilterIndex::ByKind => {
                    TraceIter::new(&mut events_by_kind, filter).collect()
                }
                DataflowFilterIndex::ByTag => {
                    let event_ids: Vec<Node> =
                        TraceIter::new(&mut events_by_tags, filter.clone()).collect();
                    let id_filter = filter
                        .clone()
                        .ids(event_ids)
                        .limit(filter.limit.unwrap_or(500));
                    TraceIter::new(&mut events_by_id, id_filter).collect()
                }
            }
        })
    }

    #[test]
    fn assert_empty_iter() {
        let results = build_dataflow(DataflowFilter::default(), |_input| {});
        assert!(results.is_empty());
    }

    #[test]
    fn assert_filter_kinds() {
        let results = build_dataflow(DataflowFilter::default().kinds(vec![1]), |input| {
            input.insert(event_row!(id: 10, pubkey: 10, kind: 1, created_at: 1000));
            input.insert(event_row!(id: 20, pubkey: 20, kind: 1, created_at: 2000));
            input.insert(event_row!(id: 30, pubkey: 30, kind: 3, created_at: 3000));
        });

        assert_eq!(results.len(), 2);
    }

    #[test]
    fn assert_filter_ids() {
        let results = build_dataflow(DataflowFilter::default().ids(vec![1, 3]), |input| {
            input.insert(event_row!(id: 1, pubkey: 10, kind: 1, created_at: 1000));
            input.insert(event_row!(id: 2, pubkey: 20, kind: 1, created_at: 2000));
            input.insert(event_row!(id: 3, pubkey: 30, kind: 1, created_at: 3000));
        });

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].pubkey, 10);
        assert_eq!(results[1].pubkey, 30);
    }

    #[test]
    fn assert_filter_authors() {
        let results = build_dataflow(DataflowFilter::default().authors(vec![20, 30]), |input| {
            input.insert(event_row!(id: 1, pubkey: 10, kind: 1, created_at: 1000));
            input.insert(event_row!(id: 2, pubkey: 20, kind: 1, created_at: 2000));
            input.insert(event_row!(id: 3, pubkey: 30, kind: 1, created_at: 3000));
        });

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].pubkey, 20);
        assert_eq!(results[1].pubkey, 30);
    }

    #[test]
    fn assert_filter_authors_and_kinds() {
        let results = build_dataflow(
            DataflowFilter::default()
                .authors(vec![10, 20])
                .kinds(vec![1]),
            |input| {
                input.insert(event_row!(id: 1, pubkey: 10, kind: 1, created_at: 1000));
                input.insert(event_row!(id: 2, pubkey: 10, kind: 3, created_at: 2000));
                input.insert(event_row!(id: 3, pubkey: 20, kind: 1, created_at: 3000));
                input.insert(event_row!(id: 4, pubkey: 20, kind: 3, created_at: 4000));
            },
        );

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].id, 1);
        assert_eq!(results[1].id, 3);
    }

    #[test]
    fn assert_filter_by_tag() {
        let results = build_dataflow(
            DataflowFilter::default()
                .add_tags(DataflowFilterTags::Pubkey, vec![100].into_iter().collect()),
            |input| {
                input.insert(EventRow {
                    id: 1,
                    pubkey: 10,
                    kind: 1,
                    created_at: 1000,
                    tags: vec![EventTag::Pubkey(100)],
                });
                input.insert(EventRow {
                    id: 2,
                    pubkey: 20,
                    kind: 1,
                    created_at: 2000,
                    tags: vec![EventTag::Pubkey(200)],
                });
                input.insert(EventRow {
                    id: 3,
                    pubkey: 30,
                    kind: 1,
                    created_at: 3000,
                    tags: vec![EventTag::Pubkey(100), EventTag::Topic(999)],
                });
            },
        );

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].id, 1);
        assert_eq!(results[1].id, 3);
    }

    #[test]
    fn assert_filter_by_tag_deduplicates() {
        // Event 1 has two tags that both match the filter.
        // It should appear only once in results.
        let results = build_dataflow(
            DataflowFilter::default().add_tags(
                DataflowFilterTags::Pubkey,
                vec![100, 200].into_iter().collect(),
            ),
            |input| {
                input.insert(EventRow {
                    id: 1,
                    pubkey: 10,
                    kind: 1,
                    created_at: 1000,
                    tags: vec![EventTag::Pubkey(100), EventTag::Pubkey(200)],
                });
            },
        );

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id, 1);
    }

    #[test]
    fn assert_filter_by_tag_and_author() {
        let results = build_dataflow(
            DataflowFilter::default()
                .authors(vec![10, 30])
                .add_tags(DataflowFilterTags::Pubkey, vec![100].into_iter().collect()),
            |input| {
                input.insert(EventRow {
                    id: 1,
                    pubkey: 10,
                    kind: 1,
                    created_at: 1000,
                    tags: vec![EventTag::Pubkey(100)],
                });
                input.insert(EventRow {
                    id: 2,
                    pubkey: 20,
                    kind: 1,
                    created_at: 2000,
                    tags: vec![EventTag::Pubkey(200)],
                });
                input.insert(EventRow {
                    id: 3,
                    pubkey: 30,
                    kind: 1,
                    created_at: 3000,
                    tags: vec![EventTag::Pubkey(100), EventTag::Topic(999)],
                });
                input.insert(EventRow {
                    id: 4,
                    pubkey: 40,
                    kind: 1,
                    created_at: 4000,
                    tags: vec![EventTag::Pubkey(100)],
                });
            },
        );

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].id, 1);
        assert_eq!(results[1].id, 3);
    }

    #[test]
    fn assert_filter_by_dtag_and_author() {
        let results = build_dataflow(
            DataflowFilter::default()
                .authors(vec![10, 30])
                .add_tags(DataflowFilterTags::DTag, vec![100].into_iter().collect()),
            |input| {
                input.insert(EventRow {
                    id: 1,
                    pubkey: 10,
                    kind: 1,
                    created_at: 1000,
                    tags: vec![EventTag::DTag(100)],
                });
                input.insert(EventRow {
                    id: 2,
                    pubkey: 20,
                    kind: 1,
                    created_at: 2000,
                    tags: vec![EventTag::DTag(200)],
                });
                input.insert(EventRow {
                    id: 3,
                    pubkey: 30,
                    kind: 1,
                    created_at: 3000,
                    tags: vec![EventTag::DTag(100)],
                });
                input.insert(EventRow {
                    id: 4,
                    pubkey: 40,
                    kind: 1,
                    created_at: 4000,
                    tags: vec![EventTag::DTag(100)],
                });
            },
        );

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].id, 1);
        assert_eq!(results[1].id, 3);
    }

    #[test]
    fn assert_filter_by_dtag_and_kind() {
        let results = build_dataflow(
            DataflowFilter::default()
                .kinds(vec![30382])
                .add_tags(DataflowFilterTags::DTag, vec![100].into_iter().collect()),
            |input| {
                input.insert(EventRow {
                    id: 1,
                    pubkey: 10,
                    kind: 30383,
                    created_at: 1000,
                    tags: vec![EventTag::DTag(100)],
                });
                input.insert(EventRow {
                    id: 2,
                    pubkey: 20,
                    kind: 30382,
                    created_at: 2000,
                    tags: vec![EventTag::DTag(200)],
                });
                input.insert(EventRow {
                    id: 3,
                    pubkey: 30,
                    kind: 30382,
                    created_at: 3000,
                    tags: vec![EventTag::DTag(100)],
                });
                input.insert(EventRow {
                    id: 4,
                    pubkey: 40,
                    kind: 30382,
                    created_at: 4000,
                    tags: vec![EventTag::DTag(200)],
                });
            },
        );

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id, 3);
    }

    #[test]
    fn assert_retractions() {
        let results = build_dataflow(DataflowFilter::default().ids(vec![2]), |input| {
            let event = event_row!(id: 2, pubkey: 20, kind: 1, created_at: 2000);
            input.insert(event.clone());
            input.flush();
            input.advance_to(2);
            input.remove(event);
            input.flush();
        });
        assert!(results.is_empty());
    }

    #[test]
    fn assert_missing_key() {
        let results = build_dataflow(DataflowFilter::default().ids(vec![2]), |input| {
            input.insert(event_row!(id: 1, pubkey: 10, kind: 1, created_at: 1000));
            input.insert(event_row!(id: 3, pubkey: 30, kind: 1, created_at: 3000));
        });

        assert!(results.is_empty());
    }

    #[test]
    fn assert_limit() {
        let results = build_dataflow(
            DataflowFilter::default().authors(vec![10]).limit(2),
            |input| {
                input.insert(event_row!(id: 1, pubkey: 10, kind: 1, created_at: 1000));
                input.insert(event_row!(id: 2, pubkey: 10, kind: 1, created_at: 2000));
                input.insert(event_row!(id: 3, pubkey: 10, kind: 1, created_at: 3000));
                input.insert(event_row!(id: 4, pubkey: 10, kind: 1, created_at: 4000));
                input.insert(event_row!(id: 5, pubkey: 10, kind: 1, created_at: 5000));
            },
        );

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].id, 1);
        assert_eq!(results[1].id, 2);
    }

    #[test]
    fn assert_since_filters_out_older_events() {
        let results = build_dataflow(
            DataflowFilter::default().authors(vec![10]).since(1500),
            |input| {
                input.insert(event_row!(id: 1, pubkey: 10, kind: 1, created_at: 1000));
                input.insert(event_row!(id: 2, pubkey: 10, kind: 1, created_at: 1500));
                input.insert(event_row!(id: 3, pubkey: 10, kind: 1, created_at: 2000));
            },
        );

        assert_eq!(results.iter().map(|e| e.id).collect::<Vec<_>>(), vec![2, 3]);
    }

    #[test]
    fn assert_until_filters_out_newer_events() {
        let results = build_dataflow(
            DataflowFilter::default().authors(vec![10]).until(1500),
            |input| {
                input.insert(event_row!(id: 1, pubkey: 10, kind: 1, created_at: 1000));
                input.insert(event_row!(id: 2, pubkey: 10, kind: 1, created_at: 1500));
                input.insert(event_row!(id: 3, pubkey: 10, kind: 1, created_at: 2000));
            },
        );

        assert_eq!(results.iter().map(|e| e.id).collect::<Vec<_>>(), vec![1, 2]);
    }

    #[test]
    fn assert_since_until_are_inclusive() {
        let results = build_dataflow(
            DataflowFilter::default()
                .authors(vec![10])
                .since(1000)
                .until(2000),
            |input| {
                input.insert(event_row!(id: 1, pubkey: 10, kind: 1, created_at: 999));
                input.insert(event_row!(id: 2, pubkey: 10, kind: 1, created_at: 1000));
                input.insert(event_row!(id: 3, pubkey: 10, kind: 1, created_at: 1500));
                input.insert(event_row!(id: 4, pubkey: 10, kind: 1, created_at: 2000));
                input.insert(event_row!(id: 5, pubkey: 10, kind: 1, created_at: 2001));
            },
        );

        assert_eq!(
            results.iter().map(|e| e.id).collect::<Vec<_>>(),
            vec![2, 3, 4]
        );
    }

    #[test]
    fn assert_multiple_missing_keys_with_one_present() {
        let results = build_dataflow(DataflowFilter::default().ids(vec![2, 4, 5]), |input| {
            input.insert(event_row!(id: 1, pubkey: 10, kind: 1, created_at: 1000));
            input.insert(event_row!(id: 3, pubkey: 30, kind: 1, created_at: 3000));
            input.insert(event_row!(id: 5, pubkey: 50, kind: 1, created_at: 5000));
            input.insert(event_row!(id: 7, pubkey: 70, kind: 1, created_at: 7000));
        });

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id, 5);
    }
}
