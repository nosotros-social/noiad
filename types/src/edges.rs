use serde::{Deserialize, Serialize};

use crate::{
    tags::EventTag,
    types::{KindU16, Node},
};

pub type TagKey = (EdgeLabel, Node);
pub type KindEdgeLabel = (KindU16, Node, Node, EdgeLabel);

#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub enum EdgeLabel {
    RootReply,
    Reply,
    Mention,
    Pubkey,
    PubkeyUpper,
    Quote,
    QuoteAddress,
    Address,
    Topic,
    Bolt11,
    DTag,
}

pub struct EventEdges {
    pub kind: u16,
    pub from: Node,
    pub tags: std::vec::IntoIter<EventTag>,
}

impl Iterator for EventEdges {
    type Item = (u16, Node, Node, EdgeLabel);

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
                EventTag::Topic(target) => (target, EdgeLabel::Topic),
                EventTag::DTag(target) => (target, EdgeLabel::DTag),
                EventTag::Bolt11(_) => continue,
            };

            return Some((self.kind, self.from, to, label));
        }
    }
}
