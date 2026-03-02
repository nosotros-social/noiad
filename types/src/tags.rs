use rkyv::{Archive, Deserialize as RDeserialize, Serialize as RSerialize};
use serde::{Deserialize, Serialize};

use crate::{edges::EdgeLabel, types::Node};

#[derive(
    Archive,
    Debug,
    Clone,
    Serialize,
    Deserialize,
    RSerialize,
    RDeserialize,
    PartialEq,
    Eq,
    Ord,
    PartialOrd,
    Hash,
)]
pub enum EventTag {
    RootReply(u32),
    Reply(u32),
    Mention(u32),
    Pubkey(u32),
    PubkeyUpper(u32),
    Quote(u32),
    QuoteAddress { pubkey: u32, address: u32 },
    Address { pubkey: u32, address: u32 },
    Topic(u32),
    Bolt11(u64),
    DTag(u32),
}

impl EventTag {
    pub fn to_edge(&self) -> Option<(EdgeLabel, Node)> {
        let (label, node) = match self {
            EventTag::RootReply(n) => (EdgeLabel::RootReply, *n),
            EventTag::Reply(n) => (EdgeLabel::Reply, *n),
            EventTag::Mention(n) => (EdgeLabel::Mention, *n),
            EventTag::Pubkey(n) => (EdgeLabel::Pubkey, *n),
            EventTag::PubkeyUpper(n) => (EdgeLabel::PubkeyUpper, *n),
            EventTag::Quote(n) => (EdgeLabel::Quote, *n),
            EventTag::QuoteAddress { pubkey, .. } => (EdgeLabel::QuoteAddress, *pubkey),
            EventTag::Address { pubkey, .. } => (EdgeLabel::Address, *pubkey),
            EventTag::Topic(n) => (EdgeLabel::Topic, *n),
            EventTag::DTag(n) => (EdgeLabel::DTag, *n),
            EventTag::Bolt11(_) => return None,
        };
        Some((label, node))
    }
}
