use rkyv::{access, from_bytes, rancor};
use rocksdb::{DBIteratorWithThreadMode, DBWithThreadMode, MultiThreaded};
use types::event::{Kind, Node};

use crate::{
    db::PersistStore,
    edges::EdgeLabel,
    event::{ArchivedEventRecord, EventRecord},
    query::PersistQuery,
    schema::{EVENTS_CF, cf},
    tag::EventTag,
};

pub struct EventIterator<'a> {
    iter: DBIteratorWithThreadMode<'a, DBWithThreadMode<MultiThreaded>>,
    query: PersistQuery,
}

pub type KindEdgeLabel = (Kind, Node, Node, EdgeLabel);

impl<'a> Iterator for EventIterator<'a> {
    type Item = EventRecord;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let item = self.iter.next()?;

            let (_, value_bytes) = match item {
                Ok(v) => v,
                Err(err) => {
                    eprintln!("rocksdb iter err: {err}");
                    continue;
                }
            };

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

            let decoded = match from_bytes::<EventRecord, rancor::Error>(&value_bytes) {
                Ok(v) => v,
                Err(err) => {
                    eprintln!("rocksdb iter err: {err}");
                    continue;
                }
            };

            return Some(decoded);
        }
    }
}

pub struct EventEdges {
    pub kind: Kind,
    pub from: Node,
    pub tags: std::vec::IntoIter<EventTag>,
}

impl Iterator for EventEdges {
    type Item = (Kind, Node, Node, EdgeLabel);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let tag = self.tags.next()?;

            let (to, label) = match tag {
                EventTag::RootReply(target) => (target, EdgeLabel::RootReply),
                EventTag::Reply(target) => (target, EdgeLabel::Reply),
                EventTag::Mention(target) => (target, EdgeLabel::Mention),
                EventTag::Pubkey(target) => (target, EdgeLabel::Pubkey),
                EventTag::PubkeyUpper(target) => (target, EdgeLabel::PubkeyUpper),
                EventTag::Quote(target) => (target, EdgeLabel::Quote),
                EventTag::QuoteAddress { pubkey, .. } => (pubkey, EdgeLabel::QuoteAddress),
                EventTag::Address { pubkey, .. } => (pubkey, EdgeLabel::Address),
                EventTag::EventReport(target) => (target, EdgeLabel::EventReport),
                EventTag::PubkeyReport(target) => (target, EdgeLabel::PubkeyReport),
                EventTag::Hashtag(target) => (target, EdgeLabel::Hashtag),
                EventTag::Bolt11(_) => continue,
                EventTag::DTag(_) => continue,
            };

            return Some((self.kind, self.from, to, label));
        }
    }
}

pub trait PersistQueryIter {
    type Iter<'a>: Iterator<Item = EventRecord> + 'a
    where
        Self: 'a;

    /// An iterator over rocksdb items matching the query
    fn iter<'a>(self, persist: &'a PersistStore) -> Self::Iter<'a>;

    fn matches(&self, notification: &EventRecord) -> bool;
}

impl PersistQueryIter for PersistQuery {
    type Iter<'a> = EventIterator<'a>;

    fn iter<'a>(self, persist: &'a PersistStore) -> Self::Iter<'a> {
        persist.iter_events(self)
    }

    fn matches(&self, event: &EventRecord) -> bool {
        self.matches_kind(event.kind)
    }
}

impl PersistStore {
    pub fn iter_events(&self, query: PersistQuery) -> EventIterator<'_> {
        let cf_events = cf!(self.db, EVENTS_CF);
        let iter = self
            .db
            .iterator_cf(&cf_events, rocksdb::IteratorMode::Start);
        EventIterator { iter, query }
    }
}

#[cfg(test)]
mod tests {
    use types::event::EventRaw;

    use crate::{helpers::TestStore, query::PersistQuery};

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

        let events: Vec<_> = store.iter_events(PersistQuery::default()).collect();
        assert_eq!(events.len(), 3);
    }
}
