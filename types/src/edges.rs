use rkyv::{Archive, Deserialize as RDeserialize, Serialize as RSerialize};
use serde::{Deserialize, Serialize};

use crate::types::Node;

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
pub enum Edge {
    RootReply(Node),
    Reply(Node),
    Mention(Node),
    Pubkey(Node),
    PubkeyUpper(Node),
    Quote(Node),
    QuoteAddress(Node),
    Address(Node),
    Topic(Node),
    Bolt11(u64),
    DTag(Node),
}

impl Edge {
    pub fn to_edge_key(&self) -> Option<(EdgeLabel, Node)> {
        match self {
            Edge::RootReply(n) => Some((EdgeLabel::RootReply, *n)),
            Edge::Reply(n) => Some((EdgeLabel::Reply, *n)),
            Edge::Mention(n) => Some((EdgeLabel::Mention, *n)),
            Edge::Pubkey(n) => Some((EdgeLabel::Pubkey, *n)),
            Edge::PubkeyUpper(n) => Some((EdgeLabel::PubkeyUpper, *n)),
            Edge::Quote(n) => Some((EdgeLabel::Quote, *n)),
            Edge::QuoteAddress(pubkey) => Some((EdgeLabel::QuoteAddress, *pubkey)),
            Edge::Address(pubkey) => Some((EdgeLabel::Address, *pubkey)),
            Edge::Topic(n) => Some((EdgeLabel::Topic, *n)),
            Edge::DTag(n) => Some((EdgeLabel::DTag, *n)),
            Edge::Bolt11(_) => None,
        }
    }
}
