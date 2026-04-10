use serde::{Deserialize, Serialize};

use crate::{edges::Edge, types::Node};

#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub enum TagLabel {
    Event,
    EventUpper,
    Pubkey,
    PubkeyUpper,
    Address,
    AddressUpper,
    Quote,
    Topic,
    DTag,
}

pub type TagKey = (TagLabel, Node);

impl TryFrom<Edge> for TagKey {
    type Error = ();

    fn try_from(value: Edge) -> Result<Self, Self::Error> {
        match value {
            Edge::RootReply(n) => Ok((TagLabel::Event, n)),
            Edge::Reply(n) => Ok((TagLabel::Event, n)),
            Edge::Mention(n) => Ok((TagLabel::Event, n)),
            Edge::Pubkey(n) => Ok((TagLabel::Pubkey, n)),
            Edge::PubkeyUpper(n) => Ok((TagLabel::PubkeyUpper, n)),
            Edge::Quote(n) => Ok((TagLabel::Quote, n)),
            Edge::QuoteAddress(n) => Ok((TagLabel::Quote, n)),
            Edge::Address(n) => Ok((TagLabel::Address, n)),
            Edge::Topic(n) => Ok((TagLabel::Topic, n)),
            Edge::DTag(n) => Ok((TagLabel::DTag, n)),
            Edge::Bolt11(_) => Err(()),
        }
    }
}
