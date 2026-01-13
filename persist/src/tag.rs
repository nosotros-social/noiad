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
    Bolt11(u64),
    DTag(u32),
}

macro_rules! impl_target {
    ($ty:ty, $out:ty) => {
        impl $ty {
            pub fn target(&self) -> Option<$out> {
                match self {
                    Self::RootReply(id) => Some(*id),
                    Self::Reply(id) => Some(*id),
                    Self::Mention(id) => Some(*id),
                    Self::Pubkey(id) => Some(*id),
                    Self::PubkeyUpper(id) => Some(*id),
                    Self::Quote(id) => Some(*id),
                    Self::QuoteAddress { pubkey, .. } => Some(*pubkey),
                    Self::Address { pubkey, .. } => Some(*pubkey),
                    Self::EventReport(id) => Some(*id),
                    Self::PubkeyReport(id) => Some(*id),
                    Self::Hashtag(id) => Some(*id),
                    Self::Bolt11(id) => Some(*id),
                    Self::DTag(_) => None,
                }
            }
        }
    };
}

impl_target!(EventTag, u32);
impl_target!(ArchivedEventTag, u32_le);
