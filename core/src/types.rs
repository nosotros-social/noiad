use anyhow::Result;
use nostr_sdk::{Event, Keys};
use persist::db::PersistStore;

use crate::algorithms::{
    trusted_assertions::TrustedAssertion,
    trusted_lists::{TrustedListFollowers, TrustedListRanks},
};

pub type Rank = isize;

pub trait IntoNostrEvent {
    fn into_nostr_event(
        self,
        identifier: String,
        keys: &Keys,
        persist: &PersistStore,
    ) -> Result<Event>;
}

#[derive(Clone, Debug)]
pub enum TrustedPayload {
    Assertion(TrustedAssertion),
    ListRanks(TrustedListRanks),
    ListFollowers(TrustedListFollowers),
}

impl IntoNostrEvent for TrustedPayload {
    fn into_nostr_event(
        self,
        identifier: String,
        keys: &Keys,
        persist: &PersistStore,
    ) -> Result<Event> {
        match self {
            TrustedPayload::Assertion(value) => value.into_nostr_event(identifier, keys, persist),
            TrustedPayload::ListRanks(value) => value.into_nostr_event(identifier, keys, persist),
            TrustedPayload::ListFollowers(value) => {
                value.into_nostr_event(identifier, keys, persist)
            }
        }
    }
}
