use rkyv::{access, deserialize, rancor};
use rocksdb::{
    DBIteratorWithThreadMode, DBWithThreadMode, Direction, IteratorMode, MultiThreaded, ReadOptions,
};
use types::{edges::Edge, event::EventRow};

use crate::{
    db::{NUM_EVENT_SHARDS, PersistStore},
    event::ArchivedEventRecord,
    query::PersistQuery,
    schema::{EVENTS_CF, cf},
};

pub struct EventIterator<'a> {
    iters: Vec<DBIteratorWithThreadMode<'a, DBWithThreadMode<MultiThreaded>>>,
    shard_prefixes: Vec<u8>,
    current_iter: usize,
    query: PersistQuery,
}

impl<'a> Iterator for EventIterator<'a> {
    type Item = EventRow;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let iter = self.iters.get_mut(self.current_iter)?;
            let item = match iter.next() {
                Some(item) => item,
                None => {
                    self.current_iter += 1;
                    continue;
                }
            };

            let (key, value_bytes) = match item {
                Ok(v) => v,
                Err(err) => {
                    eprintln!("rocksdb iter err: {err}");
                    continue;
                }
            };

            let expected_shard = *self.shard_prefixes.get(self.current_iter)?;
            if key.first().copied() != Some(expected_shard) {
                self.current_iter += 1;
                continue;
            }

            let archived = match access::<ArchivedEventRecord, rancor::Error>(&value_bytes) {
                Ok(v) => v,
                Err(err) => {
                    eprintln!("rocksdb iter err: {err}");
                    continue;
                }
            };
            let kind = archived.kind.to_native();
            if !self.query.matches_kind(kind) {
                continue;
            }

            let tags: Vec<Edge> = match deserialize::<Vec<Edge>, rancor::Error>(&archived.tags) {
                Ok(v) => v,
                Err(err) => {
                    eprintln!("rkyv tags deserialize err: {err}");
                    continue;
                }
            };

            let event = EventRow {
                id: archived.id.to_native(),
                pubkey: archived.pubkey.to_native(),
                kind,
                created_at: archived.created_at.to_native(),
                edges: tags,
            };

            return Some(event);
        }
    }
}

pub trait PersistQueryIter {
    type Iter<'a>: Iterator<Item = EventRow> + 'a
    where
        Self: 'a;

    /// An iterator over rocksdb items matching the query
    fn iter<'a>(self, persist: &'a PersistStore) -> Self::Iter<'a>;

    fn matches(&self, notification: &EventRow) -> bool;
}

impl PersistQueryIter for PersistQuery {
    type Iter<'a> = EventIterator<'a>;

    fn iter<'a>(self, persist: &'a PersistStore) -> Self::Iter<'a> {
        persist.iter_events(self)
    }

    fn matches(&self, event: &EventRow) -> bool {
        self.matches_kind(event.kind)
    }
}

impl PersistStore {
    pub fn iter_events(&self, query: PersistQuery) -> EventIterator<'_> {
        self.iter_events_for_worker(query, 0, 1)
    }

    pub fn iter_events_for_worker(
        &self,
        query: PersistQuery,
        worker_id: usize,
        workers: usize,
    ) -> EventIterator<'_> {
        let cf_events = cf!(self.db, EVENTS_CF);
        let shard_prefixes = (0..NUM_EVENT_SHARDS)
            .filter(|shard_idx| shard_idx % workers == worker_id)
            .map(|shard_idx| shard_idx as u8)
            .collect::<Vec<_>>();
        let iters = shard_prefixes
            .iter()
            .map(|shard_idx| {
                let start_key = [*shard_idx];
                let mut readopts = ReadOptions::default();
                readopts.fill_cache(false);
                self.db.iterator_cf_opt(
                    &cf_events,
                    readopts,
                    IteratorMode::From(&start_key, Direction::Forward),
                )
            })
            .collect();

        EventIterator {
            iters,
            shard_prefixes,
            current_iter: 0,
            query,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use types::event_row;

    use super::*;
    use crate::{event_raw, helpers::TestStore};

    #[test]
    fn assert_iter_empty_store() {
        let store = TestStore::default();
        let events: Vec<_> = PersistQuery::default().iter(&store).collect();
        assert!(events.is_empty());
    }

    #[test]
    fn assert_iter_all_events() {
        let store = TestStore::default();

        let e1 = store
            .insert_event(&event_raw!(id: [1u8; 32], kind: 1))
            .unwrap()
            .unwrap();
        let e2 = store
            .insert_event(&event_raw!(id: [2u8; 32], kind: 2))
            .unwrap()
            .unwrap();
        let e3 = store
            .insert_event(&event_raw!(id: [3u8; 32], kind: 3))
            .unwrap()
            .unwrap();

        let events: Vec<_> = PersistQuery::default().iter(&store).collect();
        let ids: Vec<_> = events.iter().map(|e| e.id).collect();

        assert_eq!(ids, vec![e1.id, e2.id, e3.id]);
    }

    #[test]
    fn assert_iter_filter_by_multiple_kinds() {
        let store = TestStore::default();

        store
            .insert_event(&event_raw!(id: [3u8; 32], kind: 30382))
            .unwrap();
        let e1 = store
            .insert_event(&event_raw!(id: [1u8; 32], kind: 1))
            .unwrap()
            .unwrap();
        let e2 = store
            .insert_event(&event_raw!(id: [2u8; 32], kind: 3))
            .unwrap()
            .unwrap();

        let events: Vec<_> = PersistQuery::default()
            .kinds(vec![1, 3])
            .iter(&store)
            .collect();
        let ids: Vec<_> = events.iter().map(|e| e.id).collect();

        assert_eq!(ids, vec![e1.id, e2.id]);
    }

    #[test]
    fn assert_iter_events_for_worker_partitions_shard_prefixes() {
        let store = TestStore::default();
        let mut all_ids = HashSet::new();

        for idx in 1..=128u8 {
            let event = store
                .insert_event(&event_raw!(id: [idx; 32], kind: 1))
                .unwrap()
                .unwrap();
            all_ids.insert(event.id);
        }

        let mut worker_ids = HashSet::new();
        for worker_id in 0..4 {
            let ids: HashSet<_> = store
                .iter_events_for_worker(PersistQuery::default(), worker_id, 4)
                .map(|event| event.id)
                .collect();
            assert!(!ids.is_empty());
            assert!(worker_ids.is_disjoint(&ids));
            worker_ids.extend(ids);
        }

        assert_eq!(worker_ids, all_ids);
    }

    #[test]
    fn assert_query_matches_event_row() {
        let query = PersistQuery::default().kinds(vec![1, 3]);

        let matching = event_row! {
            id: 1,
            pubkey: 2,
            kind: 1,
            created_at: 1000,
        };
        let not_matching = event_row! {
            id: 3,
            pubkey: 4,
            kind: 30382,
            created_at: 2000,
        };

        assert!(query.matches(&matching));
        assert!(!query.matches(&not_matching));
    }
}
