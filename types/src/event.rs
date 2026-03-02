use nostr_sdk::Kind;
use serde::{Deserialize, Serialize};
use std::fmt::Display;

use crate::{
    edges::{EventEdges, TagKey},
    tags::EventTag,
    types::Node,
};

#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialOrd, PartialEq, Eq, Ord, Hash)]
pub struct EventRow {
    pub id: Node,
    pub pubkey: Node,
    pub kind: u16,
    pub created_at: u64,
    pub tags: Vec<EventTag>,
}
// TODO add columnation

impl EventRow {
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
        self.kind == Kind::Reporting.as_u16()
    }

    pub fn p_tags(self) -> impl Iterator<Item = Node> {
        self.tags.into_iter().filter_map(|tag| {
            let EventTag::Pubkey(pk) = tag else {
                return None;
            };

            Some(pk)
        })
    }

    pub fn hashtags_for_author(self) -> impl Iterator<Item = (Node, Node)> {
        let author = self.pubkey;

        self.tags.into_iter().filter_map(move |tag| {
            let EventTag::Topic(topic) = tag else {
                return None;
            };

            Some((author, topic))
        })
    }

    pub fn tag_keys(self) -> Vec<(TagKey, Node)> {
        let event_id = self.id;
        self.tags
            .into_iter()
            .filter_map(move |tag| {
                let (label, node) = tag.to_edge()?;
                Some(((label, node), event_id))
            })
            .collect()
    }
}

impl Display for EventRow {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "EventRow {{ id: {}, pubkey: {}, kind: {}, created_at: {} }}",
            self.id, self.pubkey, self.kind, self.created_at
        )
    }
}
