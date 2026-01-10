use rocksdb::{DBIteratorWithThreadMode, DBWithThreadMode, MultiThreaded};
use types::event::Edge;

use crate::{event::ArchivedEventRecord, query::PersistQuery, tag::ArchivedEventTag};

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

impl From<&ArchivedEventTag> for EdgeLabel {
    fn from(tag: &ArchivedEventTag) -> Self {
        match tag {
            ArchivedEventTag::RootReply(_) => EdgeLabel::RootReply,
            ArchivedEventTag::Reply(_) => EdgeLabel::Reply,
            ArchivedEventTag::Mention(_) => EdgeLabel::Mention,
            ArchivedEventTag::Pubkey(_) => EdgeLabel::Pubkey,
            ArchivedEventTag::PubkeyUpper(_) => EdgeLabel::PubkeyUpper,
            ArchivedEventTag::Quote(_) => EdgeLabel::Quote,
            ArchivedEventTag::QuoteAddress { .. } => EdgeLabel::QuoteAddress,
            ArchivedEventTag::Address { .. } => EdgeLabel::Address,
            ArchivedEventTag::EventReport(_) => EdgeLabel::EventReport,
            ArchivedEventTag::PubkeyReport(_) => EdgeLabel::PubkeyReport,
            ArchivedEventTag::Hashtag(_) => EdgeLabel::Hashtag,
            ArchivedEventTag::Bolt11(_) => EdgeLabel::Bolt11,
            ArchivedEventTag::DTag(_) => EdgeLabel::DTag,
        }
    }
}

pub struct EdgeIterator<'a> {
    pub iter: DBIteratorWithThreadMode<'a, DBWithThreadMode<MultiThreaded>>,
    pub query: PersistQuery<Edge>,
    pub row_bytes: Option<Box<[u8]>>,
    pub tag_index: usize,
}

impl<'a> Iterator for EdgeIterator<'a> {
    type Item = (u32, u32);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(bytes) = &self.row_bytes {
                let archived = unsafe { rkyv::access_unchecked::<ArchivedEventRecord>(bytes) };

                if let Some(kinds) = &self.query.kinds
                    && !kinds.contains(&archived.kind.to_native())
                {
                    self.row_bytes = None;
                    self.tag_index = 0;
                    continue;
                }

                let tags = &archived.tags;

                while self.tag_index < tags.len() {
                    let tag = &tags[self.tag_index];
                    self.tag_index += 1;

                    if let Some(label) = &self.query.label
                        && !label.contains(&EdgeLabel::from(tag))
                    {
                        continue;
                    }

                    if let Some(target) = tag.target() {
                        let from = archived.pubkey.to_native();
                        let to = target.to_native();
                        return Some((from, to));
                    }
                }

                self.row_bytes = None;
                self.tag_index = 0;
                continue;
            }

            let item = self.iter.next()?;
            let result = item.ok()?;
            let (_, value_bytes) = result;

            self.row_bytes = Some(value_bytes);
            self.tag_index = 0;
        }
    }
}
