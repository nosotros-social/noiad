use std::marker::PhantomData;

use types::event::{Edge, EventRow};

use crate::{
    db::PersistStore,
    edges::{EdgeIterator, EdgeLabel},
    event::EventIterator,
};

pub struct PersistQuery<T> {
    pub kinds: Option<Vec<u16>>,
    pub label: Option<Vec<EdgeLabel>>,
    _marker: PhantomData<T>,
}

impl PersistQuery<EventRow> {
    pub fn events() -> Self {
        Self {
            kinds: None,
            label: None,
            _marker: PhantomData,
        }
    }

    pub fn kinds<I>(mut self, kinds: I) -> Self
    where
        I: IntoIterator<Item = u16>,
    {
        self.kinds = Some(kinds.into_iter().collect());
        self
    }
}

impl PersistQuery<Edge> {
    pub fn edges() -> Self {
        Self {
            kinds: None,
            label: None,
            _marker: PhantomData,
        }
    }

    pub fn kinds<I>(mut self, kinds: I) -> Self
    where
        I: IntoIterator<Item = u16>,
    {
        self.kinds = Some(kinds.into_iter().collect());
        self
    }

    pub fn label(mut self, label: Vec<EdgeLabel>) -> Self {
        self.label = Some(label);
        self
    }
}

pub trait PersistQueryIter {
    type Item: Clone;

    type Iter<'a>: Iterator<Item = Self::Item> + 'a
    where
        Self: 'a;

    fn iter<'a>(self, persist: &'a PersistStore) -> Self::Iter<'a>;
}

impl PersistQueryIter for PersistQuery<EventRow> {
    type Item = EventRow;
    type Iter<'a> = EventIterator<'a>;

    fn iter<'a>(self, persist: &'a PersistStore) -> Self::Iter<'a> {
        persist.iter_events(self)
    }
}

impl PersistQueryIter for PersistQuery<(u32, u32)> {
    type Item = Edge;
    type Iter<'a> = EdgeIterator<'a>;

    fn iter<'a>(self, persist: &'a PersistStore) -> Self::Iter<'a> {
        persist.iter_edges(self)
    }
}
