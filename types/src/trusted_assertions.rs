use anyhow::Result;
use nostr_sdk::{Event, Keys};

pub trait IntoNostrEvent {
    fn into_nostr_event(self, identifier: String, keys: &Keys) -> Result<Event>;
}
