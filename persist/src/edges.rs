use crate::tag::{ArchivedEventTag, EventTag};

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum EdgeLabel {
    RootReply,
    Reply,
    Mention,
    Pubkey,
    PubkeyUpper,
    Quote,
    QuoteAddress,
    Address,
    EventReport,
    PubkeyReport,
    Hashtag,
    Bolt11,
    DTag,
}

macro_rules! impl_edge_label_from_tag_ref {
    ($tag:ident) => {
        impl From<&$tag> for EdgeLabel {
            fn from(tag: &$tag) -> Self {
                match tag {
                    $tag::RootReply(_) => EdgeLabel::RootReply,
                    $tag::Reply(_) => EdgeLabel::Reply,
                    $tag::Mention(_) => EdgeLabel::Mention,
                    $tag::Pubkey(_) => EdgeLabel::Pubkey,
                    $tag::PubkeyUpper(_) => EdgeLabel::PubkeyUpper,
                    $tag::Quote(_) => EdgeLabel::Quote,
                    $tag::QuoteAddress { .. } => EdgeLabel::QuoteAddress,
                    $tag::Address { .. } => EdgeLabel::Address,
                    $tag::EventReport(_) => EdgeLabel::EventReport,
                    $tag::PubkeyReport(_) => EdgeLabel::PubkeyReport,
                    $tag::Hashtag(_) => EdgeLabel::Hashtag,
                    $tag::Bolt11(_) => EdgeLabel::Bolt11,
                    $tag::DTag(_) => EdgeLabel::DTag,
                }
            }
        }
    };
}

impl_edge_label_from_tag_ref!(EventTag);
impl_edge_label_from_tag_ref!(ArchivedEventTag);
