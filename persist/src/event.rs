use anyhow::Result;
use lightning_invoice::Bolt11Invoice;
use nostr_sdk::Kind;
use rkyv::{Archive, from_bytes, to_bytes};
use rkyv::{Deserialize, Serialize, rancor};
use std::str::FromStr;
use types::event::{EventRaw, EventRow, Node};

use crate::db::{PersistInputUpdate, PersistStore, PersistUpdate};
use crate::interner::InternBatch;
use crate::iter::EventEdges;
use crate::schema::{ADDRESSABLE_CF, EVENTS_CF, REPLACEABLE_CF, cf};
use crate::tag::EventTag;

#[derive(Archive, Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Ord, PartialOrd)]
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

    pub fn replaceable_key(&self) -> [u8; 6] {
        let mut key = [0u8; 6];
        key[0..4].copy_from_slice(&self.pubkey.to_be_bytes());
        key[4..6].copy_from_slice(&self.kind.to_be_bytes());
        key
    }

    pub fn addressable_key(&self) -> Option<[u8; 10]> {
        let d = self.d_tag()?;
        let mut key = [0u8; 10];
        key[0..4].copy_from_slice(&self.pubkey.to_be_bytes());
        key[4..6].copy_from_slice(&self.kind.to_be_bytes());
        key[6..10].copy_from_slice(&d.to_be_bytes());
        Some(key)
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

    #[inline]
    pub fn to_edges(self) -> EventEdges {
        EventEdges {
            kind: self.kind,
            from: self.pubkey,
            tags: self.tags.into_iter(),
        }
    }

    #[inline]
    pub fn is_note(&self) -> bool {
        self.kind == Kind::TextNote.as_u16()
    }

    #[inline]
    pub fn is_reply(&self) -> bool {
        if self.kind == Kind::Comment.as_u16() {
            return true;
        }
        self.tags
            .iter()
            .any(|tag| matches!(tag, EventTag::Reply(_) | EventTag::RootReply(_)))
    }

    pub fn is_root_post(&self) -> bool {
        self.is_note() && !self.is_reply()
    }

    pub fn has_report(&self) -> bool {
        self.tags
            .iter()
            .any(|tag| matches!(tag, EventTag::EventReport(_) | EventTag::PubkeyReport(_)))
    }

    pub fn hashtags_for_author(self) -> impl Iterator<Item = (Node, Node)> {
        let author = self.pubkey;

        self.tags.into_iter().filter_map(move |tag| {
            let EventTag::Hashtag(topic) = tag else {
                return None;
            };

            Some((author, topic))
        })
    }

    pub fn reported_pubkeys(self) -> impl Iterator<Item = Node> {
        self.tags.into_iter().filter_map(|tag| {
            let EventTag::PubkeyReport(pk) = tag else {
                return None;
            };

            Some(pk)
        })
    }
}

impl PersistStore {
    pub fn get_event(&self, id: u32) -> Result<Option<EventRecord>> {
        let cf_events = cf!(self.db, EVENTS_CF);
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
        let cf_events = cf!(self.db, EVENTS_CF);

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

        if event.is_replaceable() {
            if let Some(old_event) = self.find_replaceable(&event)? {
                if old_event.created_at >= event.created_at {
                    return Ok(None);
                }
                self.delete_event(batch, &old_event);
            }
            let cf_replaceable = cf!(self.db, REPLACEABLE_CF);
            batch.write_batch.put_cf(
                &cf_replaceable,
                event.replaceable_key(),
                event.id.to_be_bytes(),
            );
        }

        if event.is_addressable() {
            if let Some(old_event) = self.find_addressable(&event)? {
                if old_event.created_at >= event.created_at {
                    return Ok(None);
                }
                self.delete_event(batch, &old_event);
            }
            if let Some(key) = event.addressable_key() {
                let cf_addressable = cf!(self.db, ADDRESSABLE_CF);
                batch
                    .write_batch
                    .put_cf(&cf_addressable, key, event.id.to_be_bytes());
            }
        }

        let event_value = to_bytes::<rancor::Error>(&event)?;
        batch
            .write_batch
            .put_cf(&cf_events, event.key(), event_value.as_ref());

        Ok(Some(event))
    }

    pub fn delete_event(&self, batch: &mut InternBatch, event: &EventRecord) {
        let cf_events = cf!(self.db, EVENTS_CF);
        batch.write_batch.delete_cf(&cf_events, event.key());

        if event.is_replaceable() {
            let cf_replaceable = cf!(self.db, REPLACEABLE_CF);
            batch
                .write_batch
                .delete_cf(&cf_replaceable, event.replaceable_key());
        }
        if event.is_addressable()
            && let Some(key) = event.addressable_key()
        {
            let cf_addressable = cf!(self.db, ADDRESSABLE_CF);
            batch.write_batch.delete_cf(&cf_addressable, key);
        }
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

                    "bolt11" => {
                        let invoice = Bolt11Invoice::from_str(value).ok()?;
                        let amount_sats = invoice
                            .amount_milli_satoshis()
                            .map(|msat| msat / 1000)
                            .unwrap_or(0);
                        Ok(EventTag::Bolt11(amount_sats))
                    }
                    _ => {
                        return None;
                    }
                };

                Some(edge_result)
            })
            .collect()
    }

    fn find_replaceable(&self, event: &EventRecord) -> Result<Option<EventRecord>> {
        let cf_replaceable = cf!(self.db, REPLACEABLE_CF);
        let key = event.replaceable_key();

        let Some(id_bytes) = self.db.get_cf(&cf_replaceable, key)? else {
            return Ok(None);
        };

        let id = u32::from_be_bytes(id_bytes.try_into().unwrap());
        self.get_event(id)
    }

    fn find_addressable(&self, event: &EventRecord) -> Result<Option<EventRecord>> {
        let Some(key) = event.addressable_key() else {
            return Ok(None);
        };

        let cf_addressable = cf!(self.db, ADDRESSABLE_CF);

        let Some(id_bytes) = self.db.get_cf(&cf_addressable, key)? else {
            return Ok(None);
        };

        let id = u32::from_be_bytes(id_bytes.try_into().unwrap());
        self.get_event(id)
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
    use crate::helpers::TestStore;
    use crate::query::PersistQuery;
    use crate::tag::EventTag;
    use lightning::bitcoin::hashes::{Hash as _, sha256};
    use lightning::bitcoin::secp256k1::{Secp256k1, SecretKey};
    use lightning::bolt11_invoice::{Currency, InvoiceBuilder};
    use lightning::types::payment::{PaymentHash, PaymentSecret};
    use std::time::Duration;
    use types::event::EventRaw;

    fn build_bolt11(sats: u64) -> String {
        let secp = Secp256k1::new();
        let node_secret_key = SecretKey::from_slice(&[42u8; 32]).unwrap();
        let payment_hash = PaymentHash([0u8; 32]);
        let payment_secret = PaymentSecret([1u8; 32]);
        let payment_hash_sha256 = sha256::Hash::from_slice(&payment_hash.0).unwrap();
        let amount_msat = sats * 1000;

        let builder = InvoiceBuilder::new(Currency::Bitcoin)
            .description("test zap".to_string())
            .payment_hash(payment_hash_sha256)
            .payment_secret(payment_secret)
            .duration_since_epoch(Duration::from_secs(1_700_000_000))
            .min_final_cltv_expiry_delta(144)
            .amount_milli_satoshis(amount_msat);

        let invoice = builder
            .build_signed(|msg| secp.sign_ecdsa_recoverable(msg, &node_secret_key))
            .unwrap();

        invoice.to_string()
    }

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

        let events: Vec<_> = store.iter_events(PersistQuery::default()).collect();
        assert_eq!(events.len(), 3);
    }

    #[test]
    fn assert_bolt11_tag_stores_sats() {
        let store = TestStore::new();

        let sats = 21_000u64;
        let invoice = build_bolt11(sats);

        let raw = EventRaw {
            id: [9u8; 32],
            pubkey: [8u8; 32],
            kind: 1,
            created_at: 1000,
            tags_json: format!("[[\"bolt11\",\"{invoice}\"]]").into_bytes(),
        };

        let event = store.insert_event(&raw).unwrap().unwrap();
        let stored = store.get_event(event.id).unwrap().unwrap();

        let mut found = None;
        for tag in stored.tags {
            if let EventTag::Bolt11(amount) = tag {
                found = Some(amount);
                break;
            }
        }

        assert_eq!(found, Some(sats));
    }

    #[test]
    fn assert_invalid_bolt11_is_ignored() {
        let store = TestStore::new();

        let raw = EventRaw {
            id: [7u8; 32],
            pubkey: [6u8; 32],
            kind: 1,
            created_at: 1000,
            tags_json: b"[[\"bolt11\",\"not-a-real-invoice\"]]".to_vec(),
        };

        let event = store.insert_event(&raw).unwrap().unwrap();
        let stored = store.get_event(event.id).unwrap().unwrap();

        for tag in stored.tags {
            if let EventTag::Bolt11(_) = tag {
                panic!("invalid bolt11 should not produce EventTag::Bolt11");
            }
        }
    }
}
