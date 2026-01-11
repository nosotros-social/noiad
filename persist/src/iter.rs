use rocksdb::{DBIteratorWithThreadMode, DBWithThreadMode, MultiThreaded};
use types::event::{Edge, EventRow};

use crate::{
    db::PersistStore,
    edges::EdgeLabel,
    event::ArchivedEventRecord,
    query::PersistQuery,
    schema::{EVENTS, cf},
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

    use crate::{edges::EdgeLabel, helpers::TestStore, query::PersistQuery};

    fn hex32(byte: u8) -> String {
        hex::encode([byte; 32])
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

        let get_interner =
            |bytes: &[u8; 32]| store.interner.intern(&store.db, bytes.as_slice()).unwrap();

        let pubkey_sym = get_interner(&pubkey);
        let pubkey_sym2 = get_interner(&pubkey2);
        let pubkey_sym3 = get_interner(&pubkey3);

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
}
