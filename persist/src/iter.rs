use rocksdb::{DBIteratorWithThreadMode, DBWithThreadMode, MultiThreaded};
use types::event::{Edge, EventRow};

use crate::{
    db::PersistStore,
    edges::EdgeLabel,
    event::{ArchivedEventRecord, EventRecord},
    query::PersistQuery,
    schema::{EVENTS, cf},
    tag::EventTag,
};

pub struct EventIterator<'a> {
    iter: DBIteratorWithThreadMode<'a, DBWithThreadMode<MultiThreaded>>,
    query: PersistQuery<EventRow>,
}

impl<'a> Iterator for EventIterator<'a> {
    type Item = EventRow;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.by_ref().find_map(|result| {
            let (_, value_bytes) = result.ok()?;
            let archived = unsafe { rkyv::access_unchecked::<ArchivedEventRecord>(&value_bytes) };

            let kind = archived.kind.to_native();
            if let Some(kinds) = &self.query.kinds
                && !kinds.contains(&kind)
            {
                return None;
            }

            Some(EventRow {
                id: archived.id.to_native(),
                pubkey: archived.pubkey.to_native(),
                kind,
                created_at: archived.created_at.to_native(),
            })
        })
    }
}

pub struct EdgeIterator<'a> {
    pub iter: DBIteratorWithThreadMode<'a, DBWithThreadMode<MultiThreaded>>,
    pub query: PersistQuery<Edge>,
    pub row_bytes: Option<Box<[u8]>>,
    pub tag_index: usize,
}

impl<'a> Iterator for EdgeIterator<'a> {
    type Item = (u32, u32);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(bytes) = &self.row_bytes {
                let archived = unsafe { rkyv::access_unchecked::<ArchivedEventRecord>(bytes) };

                if !self.query.matches_kind(archived.kind.to_native()) {
                    self.row_bytes = None;
                    self.tag_index = 0;
                    continue;
                }

                let tags = &archived.tags;

                while self.tag_index < tags.len() {
                    let tag = &tags[self.tag_index];
                    self.tag_index += 1;

                    if !self.query.matches_label(&EdgeLabel::from(tag)) {
                        continue;
                    }

                    if let Some(target) = tag.target() {
                        let from = archived.pubkey.to_native();
                        let to = target.to_native();
                        return Some((from, to));
                    }
                }

                self.row_bytes = None;
                self.tag_index = 0;
                continue;
            }

            let item = self.iter.next()?;
            let result = item.ok()?;
            let (_, value_bytes) = result;

            self.row_bytes = Some(value_bytes);
            self.tag_index = 0;
        }
    }
}

pub trait PersistQueryIter {
    type Item: Clone;

    type Iter<'a>: Iterator<Item = Self::Item> + 'a
    where
        Self: 'a;

    type UpdateIter<'a>: Iterator<Item = Self::Item> + 'a
    where
        Self: 'a;

    /// An iterator over rocksdb items matching the query
    fn iter<'a>(self, persist: &'a PersistStore) -> Self::Iter<'a>;

    /// An iterator over updates from a single event matching the query
    fn iter_updates<'a>(&'a self, event: &'a EventRecord) -> Self::UpdateIter<'a>;

    fn matches(&self, notification: &EventRecord) -> bool;
}

impl PersistQueryIter for PersistQuery<EventRow> {
    type Item = EventRow;
    type Iter<'a> = EventIterator<'a>;
    type UpdateIter<'a> = std::option::IntoIter<EventRow>;

    fn iter<'a>(self, persist: &'a PersistStore) -> Self::Iter<'a> {
        persist.iter_events(self)
    }

    fn iter_updates<'a>(&'a self, event: &'a EventRecord) -> Self::UpdateIter<'a> {
        if !self.matches_kind(event.kind) {
            return None.into_iter();
        }
        Some(EventRow { ..event.into() }).into_iter()
    }

    fn matches(&self, event: &EventRecord) -> bool {
        self.matches_kind(event.kind)
    }
}

pub struct EdgeUpdateIter<'a> {
    pubkey: u32,
    tags: &'a [EventTag],
    query: &'a PersistQuery<Edge>,
    tag_index: usize,
}

impl<'a> Iterator for EdgeUpdateIter<'a> {
    type Item = Edge;

    fn next(&mut self) -> Option<Self::Item> {
        self.tags[self.tag_index..].iter().find_map(|tag| {
            self.tag_index += 1;

            if !self.query.matches_label(&EdgeLabel::from(tag)) {
                return None;
            }

            tag.target().map(|to| (self.pubkey, to))
        })
    }
}

impl PersistQueryIter for PersistQuery<Edge> {
    type Item = Edge;
    type Iter<'a> = EdgeIterator<'a>;
    type UpdateIter<'a> = EdgeUpdateIter<'a>;

    fn iter<'a>(self, persist: &'a PersistStore) -> Self::Iter<'a> {
        persist.iter_edges(self)
    }

    fn iter_updates<'a>(&'a self, event: &'a EventRecord) -> EdgeUpdateIter<'a> {
        EdgeUpdateIter {
            pubkey: event.pubkey,
            tags: &event.tags,
            query: self,
            tag_index: 0,
        }
    }

    fn matches(&self, event: &EventRecord) -> bool {
        if !self.matches_kind(event.kind) {
            return false;
        }

        if let Some(labels) = &self.label
            && !event
                .tags
                .iter()
                .any(|tag| labels.contains(&EdgeLabel::from(tag)))
        {
            return false;
        }

        true
    }
}

impl PersistStore {
    pub fn iter_events(&self, query: PersistQuery<EventRow>) -> EventIterator<'_> {
        let cf_events = cf!(self.db, EVENTS);
        let iter = self
            .db
            .iterator_cf(&cf_events, rocksdb::IteratorMode::Start);
        EventIterator { iter, query }
    }

    pub fn iter_edges(&self, query: PersistQuery<Edge>) -> EdgeIterator<'_> {
        let cf_events = cf!(self.db, EVENTS);
        EdgeIterator {
            iter: self
                .db
                .iterator_cf(&cf_events, rocksdb::IteratorMode::Start),
            query,
            row_bytes: None,
            tag_index: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use types::event::EventRaw;

    use crate::{
        edges::EdgeLabel, event::decode_hex32, helpers::TestStore, iter::PersistQueryIter,
        query::PersistQuery,
    };

    fn hex32(byte: u8) -> String {
        hex::encode([byte; 32])
    }

    fn get_interner(store: &TestStore, bytes: &[u8; 32]) -> u32 {
        store.interner.intern(&store.db, bytes.as_slice()).unwrap()
    }

    #[test]
    fn assert_iter_events() {
        let store = TestStore::new();

        let pubkey = [1u8; 32];
        for i in 0..3u8 {
            let event = EventRaw {
                id: [i + 10; 32],
                pubkey,
                kind: 1,
                created_at: 1000 + i as u64,
                tags_json: b"[]".to_vec(),
            };
            store.insert_event(&event).unwrap();
        }

        let events: Vec<_> = store.iter_events(PersistQuery::events()).collect();
        assert_eq!(events.len(), 3);
    }

    #[test]
    fn assert_iter_edges() {
        let store = TestStore::new();

        let pubkey = [10u8; 32];
        let pubkey2 = [20u8; 32];
        let pubkey3 = [30u8; 32];
        let e_tag_1 = hex32(1);
        let e_tag_2 = hex32(2);
        let p_tag_1 = hex32(3);
        let p_tag_2 = hex32(4);
        let event = EventRaw {
            id: [1; 32],
            pubkey,
            kind: 1,
            created_at: 1000,
            tags_json: format!(r#"[["e", "{e_tag_1}"], ["p", "{p_tag_1}"]]"#).into_bytes(),
        };
        let event2 = EventRaw {
            id: [2; 32],
            pubkey: pubkey2,
            kind: 1,
            created_at: 2000,
            tags_json: format!(r#"[["e", "{e_tag_2}"], ["p", "{p_tag_2}"]]"#).into_bytes(),
        };
        let event3 = EventRaw {
            id: [3; 32],
            pubkey: pubkey3,
            kind: 3,
            created_at: 3000,
            tags_json: format!(r#"[["p", "{p_tag_1}"], ["p", "{p_tag_2}"]]"#).into_bytes(),
        };
        store.insert_event(&event).unwrap();
        store.insert_event(&event2).unwrap();
        store.insert_event(&event3).unwrap();

        let pubkey_sym = get_interner(&store, &pubkey);
        let pubkey_sym2 = get_interner(&store, &pubkey2);
        let pubkey_sym3 = get_interner(&store, &pubkey3);

        let query = PersistQuery::edges();
        let edges: Vec<_> = store.iter_edges(query).collect();
        assert_eq!(
            edges,
            vec![
                (pubkey_sym, 1),
                (pubkey_sym, 3),
                (pubkey_sym3, 3),
                (pubkey_sym3, 6),
                (pubkey_sym2, 4),
                (pubkey_sym2, 6),
            ]
        );

        let query = PersistQuery::edges().kinds(vec![1]);
        let edges: Vec<_> = store.iter_edges(query).collect();
        assert_eq!(
            edges,
            vec![
                (pubkey_sym, 1),
                (pubkey_sym, 3),
                (pubkey_sym2, 4),
                (pubkey_sym2, 6),
            ]
        );

        let query = PersistQuery::edges().label(vec![EdgeLabel::Pubkey]);
        let edges: Vec<_> = store.iter_edges(query).collect();
        assert_eq!(
            edges,
            vec![
                (pubkey_sym, 3),
                (pubkey_sym3, 3),
                (pubkey_sym3, 6),
                (pubkey_sym2, 6),
            ]
        );
    }

    #[test]
    fn assert_iter_updates_events() {
        let store = TestStore::new();

        let pubkey = [1u8; 32];

        let raw1 = EventRaw {
            id: [10; 32],
            pubkey,
            kind: 1,
            created_at: 1000,
            tags_json: b"[]".to_vec(),
        };
        let raw2 = EventRaw {
            id: [11; 32],
            pubkey,
            kind: 3,
            created_at: 1001,
            tags_json: b"[]".to_vec(),
        };

        let event1 = store.insert_event(&raw1).unwrap().unwrap();
        let event2 = store.insert_event(&raw2).unwrap().unwrap();

        let query = PersistQuery::events();
        let rows1: Vec<_> = query.iter_updates(&event1).collect();
        let rows2: Vec<_> = query.iter_updates(&event2).collect();

        assert_eq!(rows1.len(), 1);
        assert_eq!(rows2.len(), 1);
        assert_eq!(rows1[0].kind, 1);
        assert_eq!(rows2[0].kind, 3);

        let query = PersistQuery::events().kinds(vec![3]);
        let rows1: Vec<_> = query.iter_updates(&event1).collect();
        let rows2: Vec<_> = query.iter_updates(&event2).collect();

        assert!(rows1.is_empty());
        assert_eq!(rows2.len(), 1);
        assert!(!query.matches(&event1));
        assert!(query.matches(&event2));
    }

    #[test]
    fn assert_iter_updates_edges() {
        let store = TestStore::new();

        let pubkey1 = [10u8; 32];
        let pubkey2 = [20u8; 32];

        let e_tag_1 = hex32(1);
        let p_tag_1 = hex32(3);
        let p_tag_2 = hex32(4);

        let raw1 = EventRaw {
            id: [1; 32],
            pubkey: pubkey1,
            kind: 1,
            created_at: 1000,
            tags_json: format!(r#"[["e","{e_tag_1}"],["p","{p_tag_1}"]]"#).into_bytes(),
        };
        let raw2 = EventRaw {
            id: [2; 32],
            pubkey: pubkey2,
            kind: 2,
            created_at: 1001,
            tags_json: format!(r#"[["p","{p_tag_2}"]]"#).into_bytes(),
        };

        let event1 = store.insert_event(&raw1).unwrap().unwrap();
        let event2 = store.insert_event(&raw2).unwrap().unwrap();

        let pubkey_sym1 = get_interner(&store, &pubkey1);
        let pubkey_sym2 = get_interner(&store, &pubkey2);
        let e_sym1 = get_interner(&store, &decode_hex32(e_tag_1.as_bytes()).unwrap());
        let p_sym1 = get_interner(&store, &decode_hex32(p_tag_1.as_bytes()).unwrap());
        let p_sym2 = get_interner(&store, &decode_hex32(p_tag_2.as_bytes()).unwrap());

        let query = PersistQuery::edges();
        let edges1: Vec<_> = query.iter_updates(&event1).collect();
        let edges2: Vec<_> = query.iter_updates(&event2).collect();

        assert_eq!(edges1, vec![(pubkey_sym1, e_sym1), (pubkey_sym1, p_sym1)]);
        assert_eq!(edges2, vec![(pubkey_sym2, p_sym2)]);

        let query = PersistQuery::edges().label(vec![EdgeLabel::Pubkey]);
        let edges1: Vec<_> = query.iter_updates(&event1).collect();
        let edges2: Vec<_> = query.iter_updates(&event2).collect();

        assert_eq!(edges1, vec![(pubkey_sym1, p_sym1)]);
        assert_eq!(edges2, vec![(pubkey_sym2, p_sym2)]);

        let query = PersistQuery::edges().label(vec![EdgeLabel::Pubkey]);
        let edges1: Vec<_> = query.iter_updates(&event1).collect();
        let edges2: Vec<_> = query.iter_updates(&event2).collect();

        assert_eq!(edges1, vec![(pubkey_sym1, p_sym1)]);
        assert_eq!(edges2, vec![(pubkey_sym2, p_sym2)]);
        assert!(query.matches(&event1));
        assert!(query.matches(&event2));
    }
}
