use differential_dataflow::trace::{Cursor, TraceReader};
use serde::{Deserialize, Serialize};
use std::{collections::HashSet, marker::PhantomData};
use types::{event::EventRowCompactValue, tags::TagKey, types::Node};

use crate::query::filter::{DataflowFilter, DataflowFilterIndex};

pub struct TraceIter<Tr, K>
where
    Tr: TraceReader,
{
    filter: DataflowFilter,
    storage: Tr::Storage,
    cursor: Tr::Cursor,
    key_index: usize,
    count: usize,
    started: bool,
    seen: HashSet<Node>,
    phantom: PhantomData<K>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct PubkeyKey(pub Node);

impl<Tr, K> TraceIter<Tr, K>
where
    for<'a> Tr: TraceReader<KeyOwn: Ord>,
{
    pub fn new(trace: &mut Tr, filter: DataflowFilter) -> Self {
        let (mut cursor, storage) = trace.cursor();
        cursor.rewind_keys(&storage);
        cursor.rewind_vals(&storage);
        Self {
            filter,
            storage,
            cursor,
            count: 0,
            key_index: 0,
            started: false,
            seen: HashSet::new(),
            phantom: PhantomData,
        }
    }
}

trait TraceKeyIndex<Tr>
where
    Tr: TraceReader,
{
    fn initialize_keys(&mut self) -> bool;
    fn advance_key(&mut self) -> bool;
}

/// ID index
impl<Tr> TraceKeyIndex<Tr> for TraceIter<Tr, Node>
where
    for<'a> Tr: TraceReader<Key<'a> = &'a Node>,
{
    fn initialize_keys(&mut self) -> bool {
        if self.filter.get_index() != DataflowFilterIndex::ById {
            return false;
        }

        if self.filter.ids.is_none() {
            self.cursor.rewind_keys(&self.storage);
            if self.cursor.key_valid(&self.storage) {
                self.cursor.rewind_vals(&self.storage);
                return true;
            }
            return false;
        }

        while let Some(key) = self
            .filter
            .ids
            .as_ref()
            .and_then(|k| k.iter().nth(self.key_index))
        {
            self.cursor.seek_key(&self.storage, key);
            if self.cursor.key_valid(&self.storage) && self.cursor.key(&self.storage) == key {
                self.cursor.rewind_vals(&self.storage);
                return true;
            }
            self.key_index += 1;
        }

        false
    }

    fn advance_key(&mut self) -> bool {
        if self.filter.get_index() != DataflowFilterIndex::ById {
            return false;
        }

        if self.filter.ids.is_none() {
            self.cursor.step_key(&self.storage);
            if self.cursor.key_valid(&self.storage) {
                self.cursor.rewind_vals(&self.storage);
                return true;
            }
            return false;
        }

        self.key_index += 1;
        while let Some(key) = self
            .filter
            .ids
            .as_ref()
            .and_then(|k| k.iter().nth(self.key_index))
        {
            self.cursor.seek_key(&self.storage, key);
            if self.cursor.key_valid(&self.storage) && self.cursor.key(&self.storage) == key {
                self.cursor.rewind_vals(&self.storage);
                return true;
            }
            self.key_index += 1;
        }

        false
    }
}

// pubkey index
impl<Tr> TraceKeyIndex<Tr> for TraceIter<Tr, PubkeyKey>
where
    for<'a> Tr: TraceReader<Key<'a> = &'a PubkeyKey>,
{
    fn initialize_keys(&mut self) -> bool {
        while let Some(author) = self
            .filter
            .authors
            .as_ref()
            .and_then(|k| k.iter().nth(self.key_index))
        {
            let key = PubkeyKey(*author);
            self.cursor.seek_key(&self.storage, &key);

            if self.cursor.key_valid(&self.storage) && self.cursor.key(&self.storage) == &key {
                self.cursor.rewind_vals(&self.storage);
                return true;
            }
            self.key_index += 1;
        }
        false
    }

    fn advance_key(&mut self) -> bool {
        self.key_index += 1;
        while let Some(author) = self
            .filter
            .authors
            .as_ref()
            .and_then(|k| k.iter().nth(self.key_index))
        {
            let key = PubkeyKey(*author);
            self.cursor.seek_key(&self.storage, &key);

            if self.cursor.key_valid(&self.storage) && self.cursor.key(&self.storage) == &key {
                self.cursor.rewind_vals(&self.storage);
                return true;
            }
            self.key_index += 1;
        }
        false
    }
}

/// kind index
impl<Tr> TraceKeyIndex<Tr> for TraceIter<Tr, u16>
where
    for<'a> Tr: TraceReader<Key<'a> = &'a u16>,
{
    fn initialize_keys(&mut self) -> bool {
        while let Some(key) = self
            .filter
            .kinds
            .as_ref()
            .and_then(|k| k.iter().nth(self.key_index))
        {
            self.cursor.seek_key(&self.storage, key);
            if self.cursor.key_valid(&self.storage) && self.cursor.key(&self.storage) == key {
                self.cursor.rewind_vals(&self.storage);
                return true;
            }
            self.key_index += 1;
        }
        false
    }

    fn advance_key(&mut self) -> bool {
        self.key_index += 1;
        while let Some(key) = self
            .filter
            .kinds
            .as_ref()
            .and_then(|k| k.iter().nth(self.key_index))
        {
            self.cursor.seek_key(&self.storage, key);
            if self.cursor.key_valid(&self.storage) && self.cursor.key(&self.storage) == key {
                self.cursor.rewind_vals(&self.storage);
                return true;
            }
            self.key_index += 1;
        }
        false
    }
}

impl<Tr> TraceKeyIndex<Tr> for TraceIter<Tr, TagKey>
where
    for<'a> Tr: TraceReader<Key<'a> = &'a TagKey>,
{
    fn initialize_keys(&mut self) -> bool {
        while let Some(key) = self
            .filter
            .tag_keys()
            .and_then(|k| k.iter().nth(self.key_index).copied())
        {
            self.cursor.seek_key(&self.storage, &key);

            if self.cursor.key_valid(&self.storage) && self.cursor.key(&self.storage) == &key {
                self.cursor.rewind_vals(&self.storage);
                return true;
            }
            self.key_index += 1;
        }
        false
    }

    fn advance_key(&mut self) -> bool {
        self.key_index += 1;
        while let Some(key) = self
            .filter
            .tag_keys()
            .and_then(|k| k.iter().nth(self.key_index).copied())
        {
            self.cursor.seek_key(&self.storage, &key);

            if self.cursor.key_valid(&self.storage) && self.cursor.key(&self.storage) == &key {
                self.cursor.rewind_vals(&self.storage);
                return true;
            }
            self.key_index += 1;
        }
        false
    }
}

pub trait TraceValueBehavior<Tr>
where
    Tr: TraceReader,
{
    type Item: Clone;

    fn read_item(&self) -> Self::Item;
    fn dedup_key(item: &Self::Item) -> Node;
    fn matches_filter(&self, item: &Self::Item) -> bool;
    fn needs_dedup(&self) -> bool {
        false
    }
}

impl<Tr> TraceValueBehavior<Tr> for TraceIter<Tr, Node>
where
    for<'a> Tr: TraceReader<Key<'a> = &'a Node, Val<'a> = &'a EventRowCompactValue>,
{
    type Item = (Node, EventRowCompactValue);

    fn read_item(&self) -> Self::Item {
        let id = *self.cursor.key(&self.storage);
        (id, self.cursor.val(&self.storage).clone())
    }

    fn dedup_key(item: &Self::Item) -> Node {
        item.0
    }

    fn matches_filter(&self, item: &Self::Item) -> bool {
        self.filter.matches(item.0, &item.1)
    }
}

impl<Tr> TraceValueBehavior<Tr> for TraceIter<Tr, u16>
where
    for<'a> Tr: TraceReader<Key<'a> = &'a u16, Val<'a> = &'a Node>,
{
    type Item = Node;

    fn read_item(&self) -> Self::Item {
        *self.cursor.val(&self.storage)
    }

    fn dedup_key(item: &Self::Item) -> Node {
        *item
    }

    fn matches_filter(&self, _item: &Self::Item) -> bool {
        true
    }

    fn needs_dedup(&self) -> bool {
        false
    }
}

impl<Tr> TraceValueBehavior<Tr> for TraceIter<Tr, PubkeyKey>
where
    for<'a> Tr: TraceReader<Key<'a> = &'a PubkeyKey, Val<'a> = &'a Node>,
{
    type Item = Node;

    fn read_item(&self) -> Self::Item {
        *self.cursor.val(&self.storage)
    }

    fn dedup_key(item: &Self::Item) -> Node {
        *item
    }

    fn matches_filter(&self, _item: &Self::Item) -> bool {
        true
    }

    fn needs_dedup(&self) -> bool {
        false
    }
}

impl<Tr> TraceValueBehavior<Tr> for TraceIter<Tr, TagKey>
where
    for<'a> Tr: TraceReader<Key<'a> = &'a TagKey, Val<'a> = &'a Node>,
{
    type Item = Node;

    fn read_item(&self) -> Self::Item {
        *self.cursor.val(&self.storage)
    }

    fn dedup_key(item: &Self::Item) -> Node {
        *item
    }

    fn matches_filter(&self, _item: &Self::Item) -> bool {
        true
    }

    fn needs_dedup(&self) -> bool {
        true
    }
}

trait DiffToI64: Copy {
    fn to_i64(self) -> i64;
}

impl DiffToI64 for &isize {
    fn to_i64(self) -> i64 {
        *self as i64
    }
}

impl<Tr, K> Iterator for TraceIter<Tr, K>
where
    Tr: TraceReader,
    for<'a> Tr::DiffGat<'a>: DiffToI64,
    Self: TraceKeyIndex<Tr> + TraceValueBehavior<Tr>,
{
    type Item = <Self as TraceValueBehavior<Tr>>::Item;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.started {
            self.started = true;
            if !self.initialize_keys() {
                return None;
            }
        }

        loop {
            if !self.cursor.key_valid(&self.storage) {
                return None;
            }

            if !self.cursor.val_valid(&self.storage) {
                if !self.advance_key() {
                    return None;
                }
                continue;
            }

            let item = self.read_item();

            let mut diff_sum = 0i64;
            self.cursor.map_times(&self.storage, |_time, diff| {
                diff_sum += diff.to_i64();
            });

            self.cursor.step_val(&self.storage);

            if diff_sum <= 0 {
                continue;
            }

            if self.needs_dedup() {
                let key = Self::dedup_key(&item);
                if self.seen.contains(&key) {
                    continue;
                }
                self.seen.insert(key);
            }

            if !self.matches_filter(&item) {
                continue;
            }

            self.count += 1;
            return Some(item);
        }
    }
}
