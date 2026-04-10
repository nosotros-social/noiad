use anyhow::Result;
use differential_dataflow::VecCollection;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::Reduce;
use nostr_sdk::{Event, EventBuilder, Keys, Kind, Tag, TagKind};
use persist::db::PersistStore;
use serde::{Deserialize, Serialize};
use timely::dataflow::Scope;
use types::types::{Diff, Node};

use crate::algorithms::trusted_assertions::TrustedAssertion;
use crate::operators::top_k::TopK;
use crate::types::IntoNostrEvent;

#[derive(Clone, Debug, Default, PartialEq, Eq, Ord, PartialOrd, Serialize, Deserialize)]
pub struct TrustedListRanks {
    pub pubkeys_by_rank: Vec<(Node, Diff)>,
}

impl IntoNostrEvent for TrustedListRanks {
    fn into_nostr_event(
        self,
        identifier: String,
        keys: &Keys,
        persist: &PersistStore,
    ) -> Result<Event> {
        let mut tags = Vec::with_capacity(3 + self.pubkeys_by_rank.len());

        tags.push(Tag::identifier(&identifier));
        tags.push(Tag::title("Top Pubkeys by Rank"));
        tags.push(Tag::custom(TagKind::custom("metric"), ["rank"]));

        for (pubkey_node, rank) in self.pubkeys_by_rank {
            let Some(pubkey_bytes) = persist.resolve_node(pubkey_node)? else {
                tracing::warn!("Could not resolve pubkey node {pubkey_node} to bytes");
                continue;
            };

            let pubkey_hex = hex::encode(pubkey_bytes);

            tags.push(Tag::custom(
                TagKind::p(),
                [pubkey_hex, String::new(), rank.to_string()],
            ));
        }

        Ok(EventBuilder::new(Kind::Custom(30392), "")
            .tags(tags)
            .sign_with_keys(keys)?)
    }
}

pub fn trusted_list_ranks<G>(
    trusted_assertions: &VecCollection<G, (Node, TrustedAssertion), Diff>,
    k: usize,
) -> VecCollection<G, TrustedListRanks, Diff>
where
    G: Scope,
    G::Timestamp: Lattice,
{
    let top_pairs = trusted_assertions
        .flat_map(|(pubkey, assertion)| assertion.rank.map(|rank| (rank, pubkey)))
        .top_k(k);

    top_pairs
        .map(|(rank, pubkey)| ((), (rank, pubkey)))
        .reduce(|_k, vals, out| {
            let mut pubkeys_by_rank: Vec<(Node, Diff)> = Vec::new();

            for &(&(rank, pubkey), diff) in vals.iter().rev() {
                if diff > 0 {
                    pubkeys_by_rank.push((pubkey, rank));
                }
            }

            out.push((TrustedListRanks { pubkeys_by_rank }, 1));
        })
        .map(|(_k, list)| list)
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Ord, PartialOrd, Serialize, Deserialize)]
pub struct TrustedListFollowers {
    pub pubkeys_by_followers: Vec<(Node, Diff)>,
}

impl IntoNostrEvent for TrustedListFollowers {
    fn into_nostr_event(
        self,
        identifier: String,
        keys: &Keys,
        persist: &PersistStore,
    ) -> Result<Event> {
        let mut tags = Vec::with_capacity(3 + self.pubkeys_by_followers.len());

        tags.push(Tag::identifier(&identifier));
        tags.push(Tag::title("Top Pubkeys by Followers"));
        tags.push(Tag::custom(TagKind::custom("metric"), ["follower_cnt"]));

        for (pubkey_node, follower_cnt) in self.pubkeys_by_followers {
            let Some(pubkey_bytes) = persist.resolve_node(pubkey_node)? else {
                tracing::warn!("Could not resolve pubkey node {pubkey_node} to bytes");
                continue;
            };

            let pubkey_hex = hex::encode(pubkey_bytes);

            tags.push(Tag::custom(
                TagKind::p(),
                [pubkey_hex, String::new(), follower_cnt.to_string()],
            ));
        }

        Ok(EventBuilder::new(Kind::Custom(30392), "")
            .tags(tags)
            .sign_with_keys(keys)?)
    }
}

pub fn trusted_list_followers<G>(
    trusted_assertions: &VecCollection<G, (Node, TrustedAssertion), Diff>,
    k: usize,
) -> VecCollection<G, TrustedListFollowers, Diff>
where
    G: Scope,
    G::Timestamp: Lattice,
{
    let top_pairs = trusted_assertions
        .map(|(pubkey, assertion)| (assertion.follower_cnt, pubkey))
        .top_k(k);

    top_pairs
        .map(|(follower_cnt, pubkey)| ((), (follower_cnt, pubkey)))
        .reduce(|_k, vals, out| {
            let mut pubkeys_by_followers: Vec<(Node, Diff)> = Vec::new();

            for &(&(follower_cnt, pubkey), diff) in vals.iter().rev() {
                if diff > 0 {
                    pubkeys_by_followers.push((pubkey, follower_cnt));
                }
            }

            out.push((
                TrustedListFollowers {
                    pubkeys_by_followers,
                },
                1,
            ));
        })
        .map(|(_k, list)| list)
}

#[cfg(test)]
mod tests {
    use super::*;
    use differential_dataflow::input::InputSession;
    use std::sync::mpsc::channel;
    use timely::dataflow::operators::capture::{Capture, Extract};

    fn assertion_with_rank(rank: Diff) -> TrustedAssertion {
        TrustedAssertion {
            rank: Some(rank),
            ..Default::default()
        }
    }

    #[test]
    fn assert_trusted_list_ranks_top_k_ordered() {
        let (tx, rx) = channel();

        timely::execute_directly(move |worker| {
            let mut input: InputSession<u64, (Node, TrustedAssertion), Diff> = InputSession::new();

            worker.dataflow::<u64, _, _>(|scope| {
                let assertions = input.to_collection(scope);
                trusted_list_ranks(&assertions, 3).inner.capture_into(tx);
            });

            input.update((1, assertion_with_rank(50)), 1 as Diff);
            input.update((2, assertion_with_rank(20)), 1 as Diff);
            input.update((3, assertion_with_rank(15)), 1 as Diff);
            input.update((4, assertion_with_rank(30)), 1 as Diff);
            input.update((5, assertion_with_rank(25)), 1 as Diff);

            input.advance_to(1);
            input.flush();
            drop(input);

            while worker.step() {}
        });

        let mut last: Option<TrustedListRanks> = None;
        for (_t, batch) in rx.extract() {
            for (list, _ts, diff) in batch {
                if diff > 0 {
                    last = Some(list);
                }
            }
        }

        assert_eq!(
            last.unwrap().pubkeys_by_rank,
            vec![(1, 50), (4, 30), (5, 25)]
        );
    }
}
