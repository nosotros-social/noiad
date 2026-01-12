use anyhow::Result;
use rkyv::{Archive, from_bytes, to_bytes};
use rkyv::{Deserialize, Serialize, rancor};
use types::event::{EventRaw, EventRow};

use crate::db::{PersistInputUpdate, PersistStore, PersistUpdate};
use crate::interner::InternBatch;
use crate::schema::{EVENTS, EVENTS_BY_PUBKEY, cf};
use crate::tag::EventTag;

#[derive(Archive, Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EventRecord {
    pub id: u32,
    pub pubkey: u32,
    pub kind: u16,
    pub created_at: u64,
    pub tags: Vec<EventTag>,
}

impl From<&EventRecord> for EventRow {
    fn from(row: &EventRecord) -> Self {
        EventRow {
            id: row.id,
            pubkey: row.pubkey,
            kind: row.kind,
            created_at: row.created_at,
        }
    }
}

impl EventRecord {
    pub fn key(&self) -> [u8; 4] {
        self.id.to_be_bytes()
    }

    pub fn key_by_pubkey(&self) -> [u8; 10] {
        let mut key = [0u8; 10];
        key[0..4].copy_from_slice(&self.pubkey.to_be_bytes());
        key[4..6].copy_from_slice(&self.kind.to_be_bytes());
        key[6..10].copy_from_slice(&self.id.to_be_bytes());
        key
    }

    pub fn key_by_pubkey_prefix(&self) -> [u8; 6] {
        let mut key = [0u8; 6];
        key[0..4].copy_from_slice(&self.pubkey.to_be_bytes());
        key[4..6].copy_from_slice(&self.kind.to_be_bytes());
        key
    }

    pub fn is_replaceable(&self) -> bool {
        self.kind == 0 || self.kind == 3 || (self.kind >= 10000 && self.kind < 20000)
    }

    pub fn is_addressable(&self) -> bool {
        self.kind >= 30000 && self.kind < 40000
    }

    pub fn d_tag(&self) -> Option<u32> {
        self.tags.iter().find_map(|e| match e {
            EventTag::DTag(d) => Some(*d),
            _ => None,
        })
    }
}

impl PersistStore {
    pub fn get_event(&self, id: u32) -> Result<Option<EventRecord>> {
        let cf_events = cf!(self.db, EVENTS);
        self.db
            .get_cf(&cf_events, id.to_be_bytes())?
            .map(|bytes| {
                from_bytes::<EventRecord, rancor::Error>(&bytes).map_err(anyhow::Error::from)
            })
            .transpose()
    }

    pub fn insert_event(&self, raw: &EventRaw) -> Result<Option<EventRecord>> {
        let mut batch = InternBatch::new(&self.db);
        let result = self.insert_event_internal(&mut batch, raw)?;
        self.db.write(batch.write_batch)?;
        Ok(result)
    }

    fn insert_event_internal(
        &self,
        batch: &mut InternBatch,
        raw: &EventRaw,
    ) -> Result<Option<EventRecord>> {
        let cf_events = cf!(self.db, EVENTS);
        let cf_by_pubkey = cf!(self.db, EVENTS_BY_PUBKEY);

        let id = self.interner.intern_batch(batch, &raw.id)?;
        let pubkey = self.interner.intern_batch(batch, &raw.pubkey)?;

        if self.db.get_cf(&cf_events, id.to_be_bytes())?.is_some() {
            return Ok(None);
        }

        let parsed_tags: Vec<Vec<&str>> =
            serde_json::from_slice(&raw.tags_json).unwrap_or_default();

        let tags = self.parse_and_intern_tags(batch, &parsed_tags)?;
        let event = EventRecord {
            id,
            pubkey,
            kind: raw.kind,
            created_at: raw.created_at,
            tags,
        };

        if (event.is_replaceable() || event.is_addressable())
            && let Some(old_event) = self.find_replaceable_event(&event)?
        {
            if old_event.created_at >= event.created_at {
                return Ok(None);
            }

            self.delete_event(batch, &old_event);
        }

        let event_value = to_bytes::<rancor::Error>(&event)?;
        batch
            .write_batch
            .put_cf(&cf_events, event.key(), event_value.as_ref());
        batch
            .write_batch
            .put_cf(&cf_by_pubkey, event.key_by_pubkey(), []);

        Ok(Some(event))
    }

    pub fn delete_event(&self, batch: &mut InternBatch, event: &EventRecord) {
        let cf_events = cf!(self.db, EVENTS);
        let cf_by_pubkey = cf!(self.db, EVENTS_BY_PUBKEY);
        batch.write_batch.delete_cf(&cf_events, event.key());
        batch
            .write_batch
            .delete_cf(&cf_by_pubkey, event.key_by_pubkey());
    }

    pub fn apply_updates(&self, inputs: &[PersistInputUpdate]) -> Result<()> {
        let mut batch = InternBatch::new(&self.db);
        let mut notifications: Vec<PersistUpdate> = Vec::new();

        for (event, ts, diff) in inputs {
            if *diff > 0 {
                if let Some(event) = self.insert_event_internal(&mut batch, event)? {
                    notifications.push((event, *ts, 1));
                }
            } else if *diff < 0 {
                let id = self.interner.intern_batch(&mut batch, &event.id)?;
                if let Some(event) = self.get_event(id)? {
                    self.delete_event(&mut batch, &event);
                    notifications.push((event, *ts, -1));
                }
            }
        }

        self.db.write(batch.write_batch)?;

        for (event, ts, diff) in notifications.drain(..) {
            self.notify(event, ts, diff);
        }

        Ok(())
    }

    fn parse_and_intern_tags(
        &self,
        batch: &mut InternBatch,
        tags: &[Vec<&str>],
    ) -> Result<Vec<EventTag>> {
        tags.iter()
            .filter_map(|tag| {
                let tag_name = *tag.first()?;
                let value = *tag.get(1)?;
                let edge_result: Result<EventTag> = match tag_name {
                    "e" => {
                        let id_bytes = decode_hex32(value.as_bytes())?;
                        self.interner
                            .intern_batch(batch, &id_bytes)
                            .map(|interned| {
                                let marker = tag.get(3).copied();
                                match marker {
                                    Some("reply") => EventTag::Reply(interned),
                                    Some("root") => EventTag::RootReply(interned),
                                    _ => EventTag::Mention(interned),
                                }
                            })
                    }
                    "p" => {
                        let bytes = decode_hex32(value.as_bytes())?;
                        self.interner
                            .intern_batch(batch, &bytes)
                            .map(EventTag::Pubkey)
                    }
                    "P" => {
                        let bytes = decode_hex32(value.as_bytes())?;
                        self.interner
                            .intern_batch(batch, &bytes)
                            .map(EventTag::PubkeyUpper)
                    }
                    "q" => {
                        if value.contains(':') {
                            let (pubkey, address) = parse_address(value, batch, &self.interner)?;
                            Ok(EventTag::QuoteAddress { pubkey, address })
                        } else {
                            let bytes = decode_hex32(value.as_bytes())?;
                            self.interner
                                .intern_batch(batch, &bytes)
                                .map(EventTag::Quote)
                        }
                    }
                    "a" => {
                        let (pubkey, address) = parse_address(value, batch, &self.interner)?;
                        Ok(EventTag::Address { pubkey, address })
                    }
                    "t" => {
                        let lowercase = value.to_lowercase();
                        self.interner
                            .intern_batch(batch, lowercase.as_bytes())
                            .map(EventTag::Hashtag)
                    }
                    "d" => self
                        .interner
                        .intern_batch(batch, value.as_bytes())
                        .map(EventTag::DTag),
                    "bolt11" => self
                        .interner
                        .intern_batch(batch, value.as_bytes())
                        .map(EventTag::Bolt11),
                    _ => {
                        return None;
                    }
                };

                Some(edge_result)
            })
            .collect()
    }

    fn find_replaceable_event(&self, event: &EventRecord) -> Result<Option<EventRecord>> {
        let cf_by_pubkey = cf!(self.db, EVENTS_BY_PUBKEY);

        let prefix = event.key_by_pubkey_prefix();
        let iter = self.db.prefix_iterator_cf(&cf_by_pubkey, prefix);

        let new_d = event.is_addressable().then(|| event.d_tag()).flatten();

        for item in iter {
            let (key_bytes, _) = item?;
            if !key_bytes.starts_with(&prefix) {
                break;
            }
            if key_bytes.len() != 10 {
                continue;
            }

            let event_id =
                u32::from_be_bytes([key_bytes[6], key_bytes[7], key_bytes[8], key_bytes[9]]);

            let Some(found_event) = self.get_event(event_id)? else {
                continue;
            };

            if event.is_addressable() && found_event.d_tag() != new_d {
                continue;
            }

            return Ok(Some(found_event));
        }

        Ok(None)
    }
}

pub fn decode_hex32(hex_bytes: &[u8]) -> Option<[u8; 32]> {
    if hex_bytes.len() != 64 {
        return None;
    }
    let mut out = [0u8; 32];
    hex::decode_to_slice(hex_bytes, &mut out).ok()?;
    Some(out)
}

fn parse_address(
    value: &str,
    batch: &mut InternBatch,
    interner: &crate::interner::Interner,
) -> Option<(u32, u32)> {
    let mut parts = value.splitn(3, ':');
    let _kind = parts.next()?;
    let pubkey_hex = parts.next()?;
    let _identifier = parts.next()?;

    if pubkey_hex.len() != 64 {
        return None;
    }

    let pubkey_bytes = decode_hex32(pubkey_hex.as_bytes())?;
    let pubkey = interner.intern_batch(batch, &pubkey_bytes).ok()?;
    let address = interner.intern_batch(batch, value.as_bytes()).ok()?;

    Some((pubkey, address))
}

#[cfg(test)]
mod tests {
    use types::event::EventRaw;

    use crate::{helpers::TestStore, query::PersistQuery};

    #[test]
    fn assert_get_event_not_found() {
        let store = TestStore::new();
        assert!(store.get_event(999).unwrap().is_none());
    }

    #[test]
    fn assert_insert_event() {
        let store = TestStore::new();

        let raw = EventRaw {
            id: [1u8; 32],
            pubkey: [2u8; 32],
            kind: 1,
            created_at: 1000,
            tags_json: b"[]".to_vec(),
        };

        let event = store.insert_event(&raw).unwrap().unwrap();

        let stored = store.get_event(event.id).unwrap().unwrap();
        assert_eq!(stored.id, event.id);
        assert_eq!(stored.pubkey, event.pubkey);
        assert_eq!(stored.kind, 1);
        assert_eq!(stored.created_at, 1000);
    }

    #[test]
    fn assert_insert_duplicate_event_returns_none() {
        let store = TestStore::new();

        let pubkey = [2u8; 32];
        let raw = EventRaw {
            id: [1u8; 32],
            pubkey,
            kind: 1,
            created_at: 1000,
            tags_json: b"[]".to_vec(),
        };

        let event1 = store.insert_event(&raw).unwrap();
        let event2 = store.insert_event(&raw).unwrap();

        assert!(event1.is_some());
        assert!(event2.is_none());
    }

    #[test]
    fn assert_replaceable_event_newer_replaces_older() {
        let store = TestStore::new();

        let pubkey = [1u8; 32];

        let old_raw = EventRaw {
            id: [2u8; 32],
            pubkey,
            kind: 0,
            created_at: 1,
            tags_json: b"[]".to_vec(),
        };
        let new_raw = EventRaw {
            id: [3u8; 32],
            pubkey,
            kind: 0,
            created_at: 2,
            tags_json: b"[]".to_vec(),
        };

        let old_event = store.insert_event(&old_raw).unwrap().unwrap();
        let new_event = store.insert_event(&new_raw).unwrap().unwrap();

        assert!(store.get_event(old_event.id).unwrap().is_none());
        assert!(store.get_event(new_event.id).unwrap().is_some());
    }

    #[test]
    fn assert_replaceable_event_older_ignored() {
        let store = TestStore::new();

        let pubkey = [1u8; 32];

        let new_raw = EventRaw {
            id: [2u8; 32],
            pubkey,
            kind: 0,
            created_at: 2,
            tags_json: b"[]".to_vec(),
        };
        let old_raw = EventRaw {
            id: [3u8; 32],
            pubkey,
            kind: 0,
            created_at: 1,
            tags_json: b"[]".to_vec(),
        };

        let new_event = store.insert_event(&new_raw).unwrap().unwrap();
        let old_result = store.insert_event(&old_raw).unwrap();

        assert!(store.get_event(new_event.id).unwrap().is_some());
        assert!(old_result.is_none());
    }

    #[test]
    fn assert_addressable_event_update() {
        let store = TestStore::new();

        let pubkey = [1u8; 32];

        let old_raw = EventRaw {
            id: [2u8; 32],
            pubkey,
            kind: 30023,
            created_at: 1000,
            tags_json: b"[[\"d\",\"test\"]]".to_vec(),
        };
        let new_raw = EventRaw {
            id: [3u8; 32],
            pubkey,
            kind: 30023,
            created_at: 2000,
            tags_json: b"[[\"d\",\"test\"]]".to_vec(),
        };

        let old_event = store.insert_event(&old_raw).unwrap().unwrap();
        let new_event = store.insert_event(&new_raw).unwrap().unwrap();

        assert!(store.get_event(old_event.id).unwrap().is_none());
        assert!(store.get_event(new_event.id).unwrap().is_some());
    }

    #[test]
    fn assert_addressable_event_keep() {
        let store = TestStore::new();

        let pubkey = [1u8; 32];

        let raw1 = EventRaw {
            id: [2u8; 32],
            pubkey,
            kind: 30023,
            created_at: 1,
            tags_json: b"[[\"d\",\"first\"]]".to_vec(),
        };
        let raw2 = EventRaw {
            id: [3u8; 32],
            pubkey,
            kind: 30023,
            created_at: 2,
            tags_json: b"[[\"d\",\"second\"]]".to_vec(),
        };

        let event1 = store.insert_event(&raw1).unwrap().unwrap();
        let event2 = store.insert_event(&raw2).unwrap().unwrap();

        assert!(store.get_event(event1.id).unwrap().is_some());
        assert!(store.get_event(event2.id).unwrap().is_some());
    }

    #[test]
    fn assert_apply_updates() {
        let store = TestStore::new();

        let pubkey = [1u8; 32];
        let ts = 0;
        let diff = 1;

        let raw_events = vec![
            (
                EventRaw {
                    id: [2u8; 32],
                    pubkey,
                    kind: 1,
                    created_at: 1000,
                    tags_json: b"[]".to_vec(),
                },
                ts,
                diff,
            ),
            (
                EventRaw {
                    id: [3u8; 32],
                    pubkey,
                    kind: 1,
                    created_at: 1001,
                    tags_json: b"[]".to_vec(),
                },
                ts,
                diff,
            ),
            (
                EventRaw {
                    id: [4u8; 32],
                    pubkey,
                    kind: 1,
                    created_at: 1002,
                    tags_json: b"[]".to_vec(),
                },
                ts,
                diff,
            ),
        ];

        store.apply_updates(&raw_events).unwrap();

        let events: Vec<_> = store.iter_events(PersistQuery::events()).collect();
        assert_eq!(events.len(), 3);
    }
}
