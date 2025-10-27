use std::str::FromStr;

use anyhow::Result;
use serde::{Deserialize, Serialize};

use nostr_sdk::prelude::*;

use sqlx::prelude::FromRow;
use sqlx::types::Json;

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct EventRecord {
    pub id: String,
    pub kind: i32,
    pub pubkey: String,
    pub content: String,
    pub sig: String,
    pub tags: Json<Tags>,
    pub created_at: i64,
}

impl From<Event> for EventRecord {
    fn from(e: Event) -> Self {
        EventRecord {
            id: e.id.to_string(),
            kind: e.kind.as_u16() as i32,
            pubkey: e.pubkey.to_string(),
            content: e.content,
            tags: e.tags.into(),
            sig: e.sig.to_string(),
            created_at: e.created_at.as_u64() as i64,
        }
    }
}

impl TryFrom<EventRecord> for Event {
    type Error = anyhow::Error;

    fn try_from(row: EventRecord) -> Result<Self> {
        let id = EventId::from_str(&row.id)?;
        let pubkey = PublicKey::from_str(&row.pubkey)?;
        let sig = Signature::from_str(&row.sig)?;
        let tags: Tags = row.tags.0;
        let kind = Kind::from(row.kind as u16);

        Ok(Event::new(
            id,
            pubkey,
            Timestamp::from(row.created_at as u64),
            kind,
            tags,
            row.content,
            sig,
        ))
    }
}
