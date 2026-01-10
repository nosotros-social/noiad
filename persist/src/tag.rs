use rkyv::{Archive, Deserialize, Serialize, rend::u32_le};

#[derive(Archive, Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum EventTag {
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

impl ArchivedEventTag {
    pub fn target(&self) -> Option<u32_le> {
        match self {
            ArchivedEventTag::RootReply(id) => Some(*id),
            ArchivedEventTag::Reply(id) => Some(*id),
            ArchivedEventTag::Mention(id) => Some(*id),
            ArchivedEventTag::Pubkey(id) => Some(*id),
            ArchivedEventTag::PubkeyUpper(id) => Some(*id),
            ArchivedEventTag::Quote(id) => Some(*id),
            ArchivedEventTag::QuoteAddress { pubkey, .. } => Some(*pubkey),
            ArchivedEventTag::Address { pubkey, .. } => Some(*pubkey),
            ArchivedEventTag::EventReport(id) => Some(*id),
            ArchivedEventTag::PubkeyReport(id) => Some(*id),
            ArchivedEventTag::Hashtag(id) => Some(*id),
            ArchivedEventTag::Bolt11(id) => Some(*id),
            ArchivedEventTag::DTag(_) => None,
        }
    }
}
