use nostr_sdk::Alphabet;
use persist::db::PersistStore;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use types::{
    event::EventRowCompactValue,
    tags::{TagKey, TagLabel},
    types::Node,
};

pub const MAX_QUERY_LIMIT: usize = 500;

pub fn effective_query_limit(limit: Option<usize>) -> usize {
    limit.unwrap_or(MAX_QUERY_LIMIT).min(MAX_QUERY_LIMIT)
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DataflowFilter {
    pub ids: Option<BTreeSet<Node>>,
    pub kinds: Option<BTreeSet<u16>>,
    pub authors: Option<BTreeSet<Node>>,
    pub limit: Option<usize>,
    pub since: Option<u32>,
    pub until: Option<u32>,
    pub tags: Option<BTreeMap<TagLabel, BTreeSet<Node>>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataflowFilterIndex {
    ById,
    ByAuthor,
    ByKind,
    ByTag,
}

impl DataflowFilter {
    #[inline]
    pub fn ids<I>(mut self, ids: I) -> Self
    where
        I: IntoIterator<Item = Node>,
    {
        self.ids = extend_or_collect(self.ids, ids);
        self
    }

    #[inline]
    pub fn authors<I>(mut self, authors: I) -> Self
    where
        I: IntoIterator<Item = Node>,
    {
        self.authors = extend_or_collect(self.authors, authors);
        self
    }

    #[inline]
    pub fn kinds<I>(mut self, kinds: I) -> Self
    where
        I: IntoIterator<Item = u16>,
    {
        self.kinds = extend_or_collect(self.kinds, kinds);
        self
    }

    #[inline]
    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    #[inline]
    pub fn since(mut self, since: u32) -> Self {
        self.since = Some(since);
        self
    }

    #[inline]
    pub fn until(mut self, until: u32) -> Self {
        self.until = Some(until);
        self
    }

    #[inline]
    pub fn add_tags(mut self, tag_type: TagLabel, tag_values: BTreeSet<Node>) -> Self {
        let values: BTreeSet<Node> = tag_values.into_iter().collect();
        match self.tags.as_mut() {
            Some(tags) => {
                tags.entry(tag_type).or_default().extend(values);
            }
            None => {
                let mut map = BTreeMap::new();
                map.insert(tag_type, values);
                self.tags = Some(map);
            }
        }
        self
    }

    #[inline]
    pub fn has_ids(&self) -> bool {
        self.ids.as_ref().is_some_and(|s| !s.is_empty())
    }

    #[inline]
    pub fn has_authors(&self) -> bool {
        self.authors.as_ref().is_some_and(|s| !s.is_empty())
    }

    #[inline]
    pub fn has_kinds(&self) -> bool {
        self.kinds.as_ref().is_some_and(|s| !s.is_empty())
    }

    #[inline]
    pub fn has_tags(&self) -> bool {
        self.tags.as_ref().is_some_and(|t| !t.is_empty())
    }

    #[inline]
    fn match_ids(&self, event_id: Node) -> bool {
        self.ids.as_ref().is_none_or(|ids| ids.contains(&event_id))
    }

    #[inline]
    fn match_authors(&self, event: &EventRowCompactValue) -> bool {
        self.authors
            .as_ref()
            .is_none_or(|authors| authors.contains(&event.pubkey))
    }

    #[inline]
    fn match_kinds(&self, event: &EventRowCompactValue) -> bool {
        self.kinds
            .as_ref()
            .is_none_or(|kinds| kinds.contains(&event.kind))
    }

    #[inline]
    fn match_since(&self, event: &EventRowCompactValue) -> bool {
        self.since
            .as_ref()
            .is_none_or(|since| event.created_at >= *since)
    }

    #[inline]
    fn match_until(&self, event: &EventRowCompactValue) -> bool {
        self.until
            .as_ref()
            .is_none_or(|until| event.created_at <= *until)
    }

    #[inline]
    pub fn matches(&self, event_id: Node, event: &EventRowCompactValue) -> bool {
        self.match_ids(event_id)
            && self.match_authors(event)
            && self.match_kinds(event)
            && self.match_since(event)
            && self.match_until(event)
    }

    pub fn from_nostr_filter(
        filter: nostr_sdk::Filter,
        persist: &PersistStore,
    ) -> Option<DataflowFilter> {
        let kinds_opt: Option<BTreeSet<u16>> = filter
            .kinds
            .map(|kinds| kinds.iter().map(|k| k.as_u16()).collect::<BTreeSet<_>>())
            .filter(|kinds| !kinds.is_empty());

        let ids_opt: Option<BTreeSet<_>> = if let Some(ids) = filter.ids.as_ref() {
            let interned: BTreeSet<_> = ids
                .iter()
                .filter_map(|id| persist.intern_get(id.as_bytes()).ok().flatten())
                .collect();
            if interned.is_empty() {
                return None;
            }
            Some(interned)
        } else {
            None
        };

        let authors_opt: Option<BTreeSet<_>> = if let Some(authors) = filter.authors.as_ref() {
            let interned: BTreeSet<_> = authors
                .iter()
                .filter_map(|author| persist.intern_get(author.as_bytes()).ok().flatten())
                .collect();
            if interned.is_empty() {
                return None;
            }
            Some(interned)
        } else {
            None
        };

        let since_opt = filter.since;
        let until_opt = filter.until;
        let limit_opt = filter.limit.and_then(|n| (n > 0).then_some(n));

        let mut dataflow_filter = DataflowFilter::default();

        if let Some(ids) = ids_opt {
            dataflow_filter = dataflow_filter.ids(ids);
        }
        if let Some(authors) = authors_opt {
            dataflow_filter = dataflow_filter.authors(authors);
        }
        if let Some(kinds) = kinds_opt {
            dataflow_filter = dataflow_filter.kinds(kinds);
        }
        if let Some(since) = since_opt {
            dataflow_filter = dataflow_filter.since(since.as_u64() as u32);
        }
        if let Some(until) = until_opt {
            dataflow_filter = dataflow_filter.until(until.as_u64() as u32);
        }
        if let Some(limit) = limit_opt {
            dataflow_filter = dataflow_filter.limit(limit);
        }

        for (tag, values) in &filter.generic_tags {
            let tag_type = match (tag.character, tag.uppercase) {
                (Alphabet::E, false) => Some(TagLabel::Event),
                (Alphabet::E, true) => Some(TagLabel::EventUpper),
                (Alphabet::P, false) => Some(TagLabel::Pubkey),
                (Alphabet::P, true) => Some(TagLabel::PubkeyUpper),
                (Alphabet::T, _) => Some(TagLabel::Topic),
                (Alphabet::Q, _) => Some(TagLabel::Quote),
                (Alphabet::A, false) => Some(TagLabel::Address),
                (Alphabet::A, true) => Some(TagLabel::AddressUpper),
                (Alphabet::D, _) => Some(TagLabel::DTag),
                _ => None,
            };

            if let Some(tag_type) = tag_type {
                let interned_values: BTreeSet<Node> = values
                    .iter()
                    .filter_map(|v| match tag_type {
                        TagLabel::Event
                        | TagLabel::EventUpper
                        | TagLabel::Pubkey
                        | TagLabel::PubkeyUpper
                        | TagLabel::Quote => hex::decode(v)
                            .ok()
                            .and_then(|b| persist.intern_get(&b).ok().flatten()),
                        TagLabel::DTag => {
                            let as_string = persist.intern_get(v.as_bytes()).ok().flatten();
                            if as_string.is_some() {
                                return as_string;
                            }

                            if v.len() == 64
                                && let Ok(decoded) = hex::decode(v)
                            {
                                return persist.intern_get(&decoded).ok().flatten();
                            }

                            None
                        }
                        TagLabel::Topic | TagLabel::Address | TagLabel::AddressUpper => {
                            persist.intern_get(v.as_bytes()).ok().flatten()
                        }
                    })
                    .collect();

                if interned_values.is_empty() {
                    return None;
                }

                dataflow_filter = dataflow_filter.add_tags(tag_type, interned_values);
            }
        }

        Some(dataflow_filter)
    }

    pub fn tag_keys(&self) -> Option<BTreeSet<TagKey>> {
        let tags = self.tags.as_ref()?;
        let mut keys = BTreeSet::new();

        for (tag_type, values) in tags {
            for value in values {
                keys.insert((*tag_type, *value));
            }
        }

        Some(keys).filter(|k| !k.is_empty())
    }

    pub fn get_index(&self) -> DataflowFilterIndex {
        if self.ids.is_some() {
            DataflowFilterIndex::ById
        } else if self.has_tags() {
            DataflowFilterIndex::ByTag
        } else if self.authors.is_some() {
            DataflowFilterIndex::ByAuthor
        } else if self.kinds.is_some() {
            DataflowFilterIndex::ByKind
        } else {
            DataflowFilterIndex::ById
        }
    }
}

fn extend_or_collect<T, I>(mut set: Option<BTreeSet<T>>, iter: I) -> Option<BTreeSet<T>>
where
    I: IntoIterator<Item = T>,
    T: Eq + Ord,
{
    match set.as_mut() {
        Some(s) => {
            s.extend(iter);
        }
        None => set = Some(iter.into_iter().collect()),
    };
    set
}

#[macro_export]
macro_rules! event_row {
    ($($field:ident : $value:expr),* $(,)?) => {
        EventRowCompactValue {
            $($field: $value,)*
                ..Default::default()
        }
    };
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn assert_matches_authors() {
        let filter = DataflowFilter::default().authors(vec![1, 2, 3]);
        assert!(filter.matches(1, &event_row!(pubkey: 1)));
        assert!(filter.matches(2, &event_row!(pubkey: 2)));
        assert!(filter.matches(3, &event_row!(pubkey: 3)));
        assert!(!filter.matches(4, &event_row!(pubkey: 4)));
    }

    #[test]
    fn assert_matches_kinds() {
        let filter = DataflowFilter::default().kinds(vec![0, 1, 10002]);
        assert!(filter.matches(1, &event_row!(kind: 0)));
        assert!(filter.matches(2, &event_row!(kind: 1)));
        assert!(filter.matches(3, &event_row!(kind: 10002)));
        assert!(!filter.matches(4, &event_row!(kind: 25)));
    }
}
