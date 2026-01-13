use rkyv::{Archive, Deserialize, Serialize};

#[derive(Archive, Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Ord, PartialOrd)]
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
    Bolt11(u64),
    DTag(u32),
}
