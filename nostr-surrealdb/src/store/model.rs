use anyhow::Result;
use nostr_database::nostr::event::{Event, EventId, Kind, Tags};
use nostr_database::nostr::key::PublicKey;
use nostr_database::nostr::secp256k1::schnorr::Signature;
use nostr_database::nostr::types::Timestamp;
use serde::{Deserialize, Serialize};
use std::convert::TryFrom;
use std::str::FromStr;
use surrealdb::RecordId;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventRecord {
    pub id: RecordId,
    pub kind: Kind,
    pub pubkey: String,
    pub content: String,
    pub sig: String,
    pub tags: Tags,
    pub tag_pairs: Vec<String>,
    pub created_at: Timestamp,
}

impl From<Event> for EventRecord {
    fn from(e: Event) -> Self {
        let tag_pairs = e
            .tags
            .iter()
            .filter_map(|tag| {
                if let Some(letter) = tag.single_letter_tag()
                    && let Some(content) = tag.content()
                {
                    return Some(letter.to_string() + content);
                }
                None
            })
            .collect::<Vec<String>>();
        EventRecord {
            id: ("event", e.id.to_string()).into(),
            kind: e.kind,
            pubkey: e.pubkey.to_string(),
            content: e.content,
            tags: e.tags,
            tag_pairs,
            sig: e.sig.to_string(),
            created_at: e.created_at,
        }
    }
}

impl TryFrom<EventRecord> for Event {
    type Error = anyhow::Error;

    fn try_from(e: EventRecord) -> Result<Self, Self::Error> {
        Ok(Event::new(
            EventId::from_str(&e.id.key().to_string())?,
            PublicKey::from_str(&e.pubkey)?,
            e.created_at,
            e.kind,
            e.tags,
            e.content,
            Signature::from_str(&e.sig)?,
        ))
    }
}
