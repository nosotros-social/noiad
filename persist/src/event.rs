use anyhow::Result;
use lightning_invoice::Bolt11Invoice;
use nostr_sdk::JsonUtil;
use rkyv::{Archive, from_bytes, to_bytes};
use rkyv::{Deserialize, Serialize, rancor};
use rocksdb::WriteBatch;
use serde_json::Value;
use std::collections::HashMap;
use std::str::FromStr;
use types::edges::Edge;

use crate::db::{PersistInputUpdate, PersistStore, PersistUpdate};
use crate::interner::InternBatch;
use crate::schema::{ADDRESSABLE_CF, EVENT_RAW_CF, EVENTS_CF, REPLACEABLE_CF, cf};

#[derive(Archive, Debug, Clone)]
pub struct EventRaw {
    pub id: [u8; 32],
    pub pubkey: [u8; 32],
    pub created_at: u32,
    pub content: Vec<u8>,
    pub sig: [u8; 64],
    pub kind: u16,
    pub tags_json: Vec<u8>,
}

#[derive(Archive, Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Ord, PartialOrd)]
pub struct EventRecord {
    pub id: u32,
    pub pubkey: u32,
    pub kind: u16,
    pub created_at: u32,
    pub tags: Vec<Edge>,
}

#[derive(Archive, Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Ord, PartialOrd)]
pub struct EventRawRecord {
    pub content: Vec<u8>,
    pub sig: [u8; 64],
    pub raw_tags: Vec<u8>,
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
            Edge::DTag(d) => Some(*d),
            _ => None,
        })
    }

    pub fn to_original(
        &self,
        raw: &EventRawRecord,
        id_bytes: [u8; 32],
        pubkey_bytes: [u8; 32],
    ) -> Result<nostr_sdk::event::Event> {
        let content = String::from_utf8(raw.content.clone())
            .map_err(|e| anyhow::anyhow!("event content is not valid utf-8: {e}"))?;

        let tags_value: Value = match serde_json::from_slice(&raw.raw_tags) {
            Ok(Value::Array(arr)) => Value::Array(arr),
            _ => Value::Array(vec![]),
        };

        let json_value = serde_json::json!({
            "id": hex::encode(id_bytes),
            "pubkey": hex::encode(pubkey_bytes),
            "created_at": self.created_at,
            "kind": self.kind,
            "tags": tags_value,
            "content": Value::String(content),
            "sig": hex::encode(raw.sig),
        });

        let json = serde_json::to_string(&json_value)?;
        let event = nostr_sdk::event::Event::from_json(json)?;

        Ok(event)
    }
}

impl PersistStore {
    pub fn get_event(&self, id: u32) -> Result<Option<EventRecord>> {
        let event_db = self.event_db(id);
        let cf_events = cf!(event_db, EVENTS_CF);
        event_db
            .get_cf(&cf_events, id.to_be_bytes())?
            .map(|bytes| {
                from_bytes::<EventRecord, rancor::Error>(&bytes).map_err(anyhow::Error::from)
            })
            .transpose()
    }

    fn get_event_raw(&self, id: u32) -> Result<Option<EventRawRecord>> {
        let cf_event_raw = cf!(self.db, EVENT_RAW_CF);
        self.db
            .get_cf(&cf_event_raw, id.to_be_bytes())?
            .map(|bytes| {
                from_bytes::<EventRawRecord, rancor::Error>(&bytes).map_err(anyhow::Error::from)
            })
            .transpose()
    }

    pub fn insert_event(&self, raw: &EventRaw) -> Result<Option<EventRecord>> {
        let mut batch = InternBatch::new(&self.db, &self.interner);
        let mut shard_batches: HashMap<usize, WriteBatch> = HashMap::new();
        let result = self.insert_event_internal(&mut batch, &mut shard_batches, raw)?;
        self.db.write(std::mem::take(&mut batch.write_batch))?;
        for (shard_id, shard_batch) in shard_batches {
            self.event_shards[shard_id].write(shard_batch)?;
        }
        Ok(result.map(|(event, _)| event))
    }

    fn insert_event_internal(
        &self,
        batch: &mut InternBatch,
        shard_batches: &mut HashMap<usize, WriteBatch>,
        raw: &EventRaw,
    ) -> Result<Option<(EventRecord, Option<EventRecord>)>> {
        if let Some(existing_id) = self.interner.get(&self.db, &raw.id)? {
            let event_db = self.event_db(existing_id);
            let cf_events = cf!(event_db, EVENTS_CF);
            if event_db
                .get_cf(&cf_events, existing_id.to_be_bytes())?
                .is_some()
            {
                return Ok(None);
            }
        }

        let id = self.interner.intern_batch(batch, &raw.id)?;
        let pubkey = self.interner.intern_batch(batch, &raw.pubkey)?;

        let parsed_tags: Vec<Vec<String>> = match serde_json::from_slice(&raw.tags_json) {
            Ok(tags) => tags,
            Err(err) => {
                tracing::warn!(
                    event_id = %hex::encode(raw.id),
                    "failed to parse event tags JSON: {err}"
                );
                Vec::new()
            }
        };

        let tags = self.parse_and_intern_tags(batch, &parsed_tags)?;
        let event = EventRecord {
            id,
            pubkey,
            kind: raw.kind,
            created_at: raw.created_at,
            tags,
        };
        let raw_event = EventRawRecord {
            content: raw.content.clone(),
            sig: raw.sig,
            raw_tags: raw.tags_json.clone(),
        };

        let mut deleted_event: Option<EventRecord> = None;

        if event.is_replaceable() {
            if let Some(old_event) = self.find_replaceable(&event)? {
                if old_event.created_at >= event.created_at {
                    return Ok(None);
                }
                self.delete_event(batch, shard_batches, &old_event);
                deleted_event = Some(old_event);
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
                self.delete_event(batch, shard_batches, &old_event);
                deleted_event = Some(old_event);
            }
            if let Some(key) = event.addressable_key() {
                let cf_addressable = cf!(self.db, ADDRESSABLE_CF);
                batch
                    .write_batch
                    .put_cf(&cf_addressable, key, event.id.to_be_bytes());
            }
        }

        let event_value = to_bytes::<rancor::Error>(&event)?;
        let raw_event_value = to_bytes::<rancor::Error>(&raw_event)?;
        let shard_id = self.shard_for_event_id(event.id);
        let event_db = &self.event_shards[shard_id];
        let cf_events = cf!(event_db, EVENTS_CF);
        shard_batches.entry(shard_id).or_default().put_cf(
            &cf_events,
            event.key(),
            event_value.as_ref(),
        );
        let cf_event_raw = cf!(self.db, EVENT_RAW_CF);
        batch
            .write_batch
            .put_cf(&cf_event_raw, event.key(), raw_event_value.as_ref());

        Ok(Some((event, deleted_event)))
    }

    fn delete_event(
        &self,
        batch: &mut InternBatch,
        shard_batches: &mut HashMap<usize, WriteBatch>,
        event: &EventRecord,
    ) {
        let shard_id = self.shard_for_event_id(event.id);
        let event_db = &self.event_shards[shard_id];
        let cf_events = cf!(event_db, EVENTS_CF);
        shard_batches
            .entry(shard_id)
            .or_default()
            .delete_cf(&cf_events, event.key());
        let cf_event_raw = cf!(self.db, EVENT_RAW_CF);
        batch.write_batch.delete_cf(&cf_event_raw, event.key());

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
        if inputs.is_empty() {
            return Ok(());
        }

        let mut max_ts: u64 = 0;
        let mut batch = InternBatch::new(&self.db, &self.interner);
        let mut shard_batches: HashMap<usize, WriteBatch> = HashMap::new();
        let mut notifications: Vec<PersistUpdate> = Vec::with_capacity(inputs.len());

        for (event, ts, diff) in inputs {
            if *ts > max_ts {
                max_ts = *ts;
            }

            if *diff > 0 {
                if let Some((event, deleted)) =
                    self.insert_event_internal(&mut batch, &mut shard_batches, event)?
                {
                    if let Some(old_event) = deleted {
                        notifications.push((old_event, *ts, -1));
                    }
                    notifications.push((event, *ts, 1));
                }
            } else if *diff < 0
                && let Some(id) = self.interner.get(&self.db, &event.id)?
                && let Some(existing) = self.get_event(id)?
            {
                self.delete_event(&mut batch, &mut shard_batches, &existing);
                notifications.push((existing, *ts, -1));
            }
        }

        self.db.write(std::mem::take(&mut batch.write_batch))?;
        for (shard_id, shard_batch) in shard_batches {
            self.event_shards[shard_id].write(shard_batch)?;
        }

        let new_upper = max_ts + 1;
        self.save_checkpoint(new_upper)?;

        for (event, ts, diff) in notifications.drain(..) {
            self.notify(event, ts, diff);
        }

        Ok(())
    }

    fn parse_and_intern_tags(
        &self,
        batch: &mut InternBatch,
        tags: &[Vec<String>],
    ) -> Result<Vec<Edge>> {
        tags.iter()
            .filter_map(|tag| {
                let tag_name = tag.first()?.as_str();
                let value = tag.get(1)?.as_str();
                let edge_result: Result<Edge> = match tag_name {
                    "e" => {
                        let id_bytes = decode_hex32(value.as_bytes())?;
                        self.interner
                            .intern_batch(batch, &id_bytes)
                            .map(|interned| {
                                let marker = tag.get(3).map(String::as_str);
                                match marker {
                                    Some("reply") => Edge::Reply(interned),
                                    Some("root") => Edge::RootReply(interned),
                                    _ => Edge::Mention(interned),
                                }
                            })
                    }
                    "p" => {
                        let bytes = decode_hex32(value.as_bytes())?;
                        self.interner.intern_batch(batch, &bytes).map(Edge::Pubkey)
                    }
                    "P" => {
                        let bytes = decode_hex32(value.as_bytes())?;
                        self.interner
                            .intern_batch(batch, &bytes)
                            .map(Edge::PubkeyUpper)
                    }
                    "q" => {
                        if value.contains(':') {
                            let (pubkey, _) = parse_address(value, batch, &self.interner)?;
                            Ok(Edge::QuoteAddress(pubkey))
                        } else {
                            let bytes = decode_hex32(value.as_bytes())?;
                            self.interner.intern_batch(batch, &bytes).map(Edge::Quote)
                        }
                    }
                    "a" => {
                        let (pubkey, _) = parse_address(value, batch, &self.interner)?;
                        Ok(Edge::Address(pubkey))
                    }
                    "t" => {
                        let lowercase = value.to_lowercase();
                        self.interner
                            .intern_batch(batch, lowercase.as_bytes())
                            .map(Edge::Topic)
                    }
                    "d" => {
                        let bytes_to_intern = (value.len() == 64)
                            // assumes the DTag is a pubkey or a event id
                            .then(|| hex::decode(value).ok())
                            .flatten()
                            .unwrap_or_else(|| value.as_bytes().to_vec());

                        self.interner
                            .intern_batch(batch, &bytes_to_intern)
                            .map(Edge::DTag)
                    }
                    "bolt11" => {
                        let invoice = Bolt11Invoice::from_str(value).ok()?;
                        let amount_sats = invoice
                            .amount_milli_satoshis()
                            .map(|msat| msat / 1000)
                            .unwrap_or(0);
                        Ok(Edge::Bolt11(amount_sats))
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

    pub fn get_original_event(&self, event_id: u32) -> Result<Option<nostr_sdk::event::Event>> {
        let Some(record) = self.get_event(event_id)? else {
            return Ok(None);
        };
        let Some(raw) = self.get_event_raw(event_id)? else {
            return Ok(None);
        };

        let id_bytes = self
            .interner
            .resolve(&self.db, record.id)?
            .ok_or_else(|| anyhow::anyhow!("missing interner entry for event id {}", record.id))?
            .try_into()
            .map_err(|v: Vec<u8>| anyhow::anyhow!("event id is not 32 bytes (len={})", v.len()))?;

        let pubkey_bytes = self
            .interner
            .resolve(&self.db, record.pubkey)?
            .ok_or_else(|| anyhow::anyhow!("missing interner entry for pubkey {}", record.pubkey))?
            .try_into()
            .map_err(|v: Vec<u8>| anyhow::anyhow!("pubkey is not 32 bytes (len={})", v.len()))?;

        record.to_original(&raw, id_bytes, pubkey_bytes).map(Some)
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
    use crate::event::EventRaw;
    use crate::event_raw;
    use crate::helpers::TestStore;
    use crate::query::PersistQuery;
    use lightning::bitcoin::hashes::{Hash as _, sha256};
    use lightning::bitcoin::secp256k1::{Secp256k1, SecretKey};
    use lightning::bolt11_invoice::{Currency, InvoiceBuilder};
    use lightning::types::payment::{PaymentHash, PaymentSecret};
    use std::time::Duration;
    use types::edges::Edge;

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
        let store = TestStore::default();
        assert!(store.get_event(999).unwrap().is_none());
    }

    #[test]
    fn assert_insert_event() {
        let store = TestStore::default();

        let raw = event_raw! {
            id: [1u8; 32],
            pubkey: [2u8; 32],
            kind: 1,
            created_at: 1000,
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
        let store = TestStore::default();

        let pubkey = [2u8; 32];
        let raw = event_raw! {
            id: [1u8; 32],
            pubkey,
            kind: 1,
            created_at: 1000,
        };

        let event1 = store.insert_event(&raw).unwrap();
        let event2 = store.insert_event(&raw).unwrap();

        assert!(event1.is_some());
        assert!(event2.is_none());
    }

    #[test]
    fn assert_replaceable_event_older_ignored() {
        let store = TestStore::default();
        let pubkey = [1u8; 32];
        let new_raw = event_raw! {
            id: [2u8; 32],
            pubkey,
            kind: 0,
            created_at: 2,
        };
        let old_raw = event_raw! {
            id: [3u8; 32],
            pubkey,
            kind: 0,
            created_at: 1,
        };

        let new_event = store.insert_event(&new_raw).unwrap().unwrap();
        let old_result = store.insert_event(&old_raw).unwrap();

        assert!(store.get_event(new_event.id).unwrap().is_some());
        assert!(old_result.is_none());
    }

    #[test]
    fn assert_addressable_events() {
        let store = TestStore::default();

        let pubkey = [1u8; 32];

        let raw1 = event_raw! {
            id: [2u8; 32],
            pubkey,
            kind: 30023,
            created_at: 1,
            tags_json: b"[[\"d\",\"first\"]]".to_vec(),
        };
        let raw2 = event_raw! {
            id: [3u8; 32],
            pubkey,
            kind: 30023,
            created_at: 2,
            tags_json: b"[[\"d\",\"first\"]]".to_vec(),
        };

        let event1 = store.insert_event(&raw1).unwrap().unwrap();
        let event2 = store.insert_event(&raw2).unwrap().unwrap();

        assert!(store.get_event(event1.id).unwrap().is_none());
        assert!(store.get_event(event2.id).unwrap().is_some());
    }

    #[test]
    fn assert_bolt11_tag_stores_sats() {
        let store = TestStore::default();

        let sats = 21_000u64;
        let invoice = build_bolt11(sats);

        let raw = event_raw! (
            id: [9u8; 32],
            pubkey: [8u8; 32],
            kind: 1,
            tags_json: format!("[[\"bolt11\",\"{invoice}\"]]").into_bytes(),
        );

        let event = store.insert_event(&raw).unwrap().unwrap();
        let stored = store.get_event(event.id).unwrap().unwrap();

        let mut found = None;
        for tag in stored.tags {
            if let Edge::Bolt11(amount) = tag {
                found = Some(amount);
                break;
            }
        }

        assert_eq!(found, Some(sats));
    }

    #[test]
    fn assert_zap_receipt_tags_store_sender_recipient_and_amount() {
        let store = TestStore::default();

        let recipient =
            hex::decode("fd4169cc72134e14cb4da85a58cb7c1fbc2964c943833fc04bbd2de8f747990c")
                .unwrap();
        let sender =
            hex::decode("c6603b0f1ccfec625d9c08b753e4f774eaf7d1cf2769223125b5fd4da728019e")
                .unwrap();
        let invoice = "lnbc210n1p5taldgpp5mt438wrurccqyfr2wezvxltvq65uva52vu353pejh5hk5nafz7nqhp5ahv2jk6q8w7en4p9fyec3a95pw2pfkrgsxxu7x5l3xmet20zkreqcqzpuxqrwzqsp5svx5nt0y4485g74muzr27jf8nqlrjfetnt8ufq3u3yerqsy2dxjs9qxpqysgqm7lykvrlkkdktnylz8axr4fdal5j6u4qtv2xe26f06qzvzsxvak30q8prqxt9dwxccd33e3ezur5rcp8ua3nlrqwjlcw3s4u4se22tqq8pdgyv";
        let description = r#"{"kind":9734,"pubkey":"c6603b0f1ccfec625d9c08b753e4f774eaf7d1cf2769223125b5fd4da728019e","content":"","tags":[["p","fd4169cc72134e14cb4da85a58cb7c1fbc2964c943833fc04bbd2de8f747990c"]]}"#;

        let raw = event_raw! (
            id: [10u8; 32],
            pubkey: [11u8; 32],
            kind: 9735,
            tags_json: serde_json::json!([
                ["p", hex::encode(&recipient)],
                ["P", hex::encode(&sender)],
                ["bolt11", invoice.to_string()],
                ["description", description.to_string()]
            ])
            .to_string()
            .into_bytes(),
        );

        let event = store.insert_event(&raw).unwrap().unwrap();
        let stored = store.get_event(event.id).unwrap().unwrap();
        let recipient_node = store.interner.intern(&store.db, &recipient).unwrap();
        let sender_node = store.interner.intern(&store.db, &sender).unwrap();

        assert!(stored.tags.contains(&Edge::Pubkey(recipient_node)));
        assert!(stored.tags.contains(&Edge::PubkeyUpper(sender_node)));
        assert!(stored.tags.contains(&Edge::Bolt11(21)));
    }

    #[test]
    fn assert_invalid_bolt11_is_ignored() {
        let store = TestStore::default();

        let raw = event_raw! (
            id: [7u8; 32],
            pubkey: [6u8; 32],
            kind: 1,
            created_at: 1000,
            tags_json: b"[[\"bolt11\",\"not-a-real-invoice\"]]".to_vec(),
        );

        let event = store.insert_event(&raw).unwrap().unwrap();
        let stored = store.get_event(event.id).unwrap().unwrap();

        for tag in stored.tags {
            if let Edge::Bolt11(_) = tag {
                panic!("invalid bolt11 should not produce EventTag::Bolt11");
            }
        }
    }

    #[test]
    fn assert_dtag_with_event_pubkey() {
        let store = TestStore::default();

        let pubkey = [3u8; 32];

        let event_raw = event_raw! {
            id: [1u8; 32],
            pubkey,
            kind: 1,
            created_at: 1000,
        };
        let event_record = store.insert_event(&event_raw).unwrap().unwrap();

        let addressable_raw = event_raw! (
            id: [2u8; 32],
            pubkey: [4u8; 32],
            kind: 30000,
            created_at: 1000,
            tags_json: format!("[[\"d\",\"{}\"]]", hex::encode(pubkey)).into_bytes(),
        );
        let addressable_record = store.insert_event(&addressable_raw).unwrap().unwrap();

        assert_eq!(addressable_record.d_tag().unwrap(), event_record.pubkey);
    }

    #[test]
    fn assert_get_original_event() {
        let store = TestStore::default();

        let raw = event_raw! (
            id: [1u8; 32],
            pubkey: [2u8; 32],
            kind: 1,
            created_at: 1234567890,
        );

        let record = store.insert_event(&raw).unwrap().unwrap();
        let original = store.get_original_event(record.id).unwrap().unwrap();

        assert_eq!(original.id.as_bytes(), &raw.id);
        assert_eq!(original.pubkey.as_bytes(), &raw.pubkey);
    }

    #[test]
    fn assert_apply_updates() {
        let store = TestStore::default();
        let pubkey_1 = [1u8; 32];
        let pubkey_2 = [2u8; 32];

        // batch 1 at ts 100
        let event_1 = event_raw!(id: [10u8; 32], pubkey: pubkey_1, kind: 1, created_at: 1000);
        let event_2 = event_raw!(id: [11u8; 32], pubkey: pubkey_1, kind: 1, created_at: 1001);
        let replaceable_1 = event_raw!(id: [20u8; 32], pubkey: pubkey_1, kind: 0, created_at: 1000);
        let addressable_1 = event_raw!(
            id: [30u8; 32], pubkey: pubkey_2, kind: 30023, created_at: 1000,
            tags_json: b"[[\"d\",\"blog\"]]".to_vec()
        );

        store
            .apply_updates(&[
                (event_1.clone(), 100, 1),
                (event_2.clone(), 100, 1),
                (replaceable_1.clone(), 100, 1),
                (addressable_1.clone(), 100, 1),
            ])
            .unwrap();

        assert_eq!(store.load_checkpoint().unwrap(), Some(101));

        // batch 2 at ts 200
        let replaceable_2 = event_raw!(id: [21u8; 32], pubkey: pubkey_1, kind: 0, created_at: 2000);
        let addressable_2 = event_raw!(
            id: [31u8; 32], pubkey: pubkey_2, kind: 30023, created_at: 2000,
            tags_json: b"[[\"d\",\"blog\"]]".to_vec()
        );

        store
            .apply_updates(&[
                (replaceable_2.clone(), 200, 1),
                (addressable_2.clone(), 200, 1),
                (event_2.clone(), 200, -1),
            ])
            .unwrap();

        assert_eq!(store.load_checkpoint().unwrap(), Some(201));

        let id = |raw: &EventRaw| store.interner.get(&store.db, &raw.id).unwrap().unwrap();

        assert!(store.get_event(id(&event_1)).unwrap().is_some());
        assert!(store.get_event(id(&replaceable_2)).unwrap().is_some());
        assert!(store.get_event(id(&addressable_2)).unwrap().is_some());
        // assert negatives
        assert!(store.get_event(id(&event_2)).unwrap().is_none());
        assert!(store.get_event(id(&replaceable_1)).unwrap().is_none());
        assert!(store.get_event(id(&addressable_1)).unwrap().is_none());

        let all_events: Vec<_> = store.iter_events(PersistQuery::default()).collect();
        assert_eq!(all_events.len(), 3);
    }

    #[test]
    fn assert_notifications() {
        let store = TestStore::default();
        let mut sub = store.subscribe(0, 1);

        let pubkey = [1u8; 32];
        let old_event = event_raw!(
            id: [2u8; 32], pubkey, kind: 30023, created_at: 1000,
            tags_json: b"[[\"d\",\"test\"]]".to_vec()
        );
        let new_event = event_raw!(
            id: [3u8; 32], pubkey, kind: 30023, created_at: 2000,
            tags_json: b"[[\"d\",\"test\"]]".to_vec()
        );

        store.apply_updates(&[(old_event.clone(), 100, 1)]).unwrap();
        store.apply_updates(&[(new_event.clone(), 200, 1)]).unwrap();

        let notifications: Vec<_> = std::iter::from_fn(|| sub.try_recv().ok()).collect();
        let resolve = |id| store.interner.resolve(&store.db, id).unwrap().unwrap();

        // assert old_event
        assert_eq!(
            (&resolve(notifications[0].0.id), notifications[0].2),
            (&old_event.id.to_vec(), 1)
        );

        // assert old_event reaction and new_event
        assert_eq!(
            (&resolve(notifications[1].0.id), notifications[1].2),
            (&old_event.id.to_vec(), -1)
        );
        assert_eq!(
            (&resolve(notifications[2].0.id), notifications[2].2),
            (&new_event.id.to_vec(), 1)
        );
    }
}
