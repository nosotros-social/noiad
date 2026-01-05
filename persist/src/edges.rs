use rkyv::{Archive, Deserialize, Serialize};

pub type Kind = u16;
pub type Node = u32;
pub type EdgeKind = (Kind, Node, Node);

#[derive(Archive, Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Edge {
    RootReply(u32),
    Reply(u32),
    Mention(u32),
    Pubkey(u32),
    PubkeyUpper(u32),
    Quote(u32),
    QuoteAddress { pubkey: u32, address: u32 },
    Address { pubkey: u32, address: u32 },
    EventReport(u32),
    PubkeyReport(u32),
    Hashtag(u32),
    Bolt11(u32),
    DTag(u32),
}

impl Edge {
    pub fn target(&self) -> Option<u32> {
        match self {
            Edge::RootReply(id) => Some(*id),
            Edge::Reply(id) => Some(*id),
            Edge::Mention(id) => Some(*id),
            Edge::Pubkey(id) => Some(*id),
            Edge::PubkeyUpper(id) => Some(*id),
            Edge::Quote(id) => Some(*id),
            Edge::QuoteAddress { pubkey, .. } => Some(*pubkey),
            Edge::Address { pubkey, .. } => Some(*pubkey),
            Edge::EventReport(id) => Some(*id),
            Edge::PubkeyReport(id) => Some(*id),
            Edge::Hashtag(id) => Some(*id),
            Edge::Bolt11(id) => Some(*id),
            Edge::DTag(_) => None,
        }
    }
}
