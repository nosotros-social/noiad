use std::marker::PhantomData;

use types::event::{Edge, EventRow};

use crate::edges::EdgeLabel;

#[derive(Debug, Clone)]
pub struct PersistQuery<T> {
    pub kinds: Option<Vec<u16>>,
    pub label: Option<Vec<EdgeLabel>>,
    _marker: PhantomData<T>,
}

impl<T> PersistQuery<T> {
    pub fn kinds<I>(mut self, kinds: I) -> Self
    where
        I: IntoIterator<Item = u16>,
    {
        self.kinds = Some(kinds.into_iter().collect());
        self
    }

    pub fn matches_kind(&self, kind: u16) -> bool {
        match &self.kinds {
            Some(kinds) => kinds.contains(&kind),
            None => true,
        }
    }
}

impl PersistQuery<EventRow> {
    pub fn events() -> Self {
        Self {
            kinds: None,
            label: None,
            _marker: PhantomData,
        }
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

    pub fn label(mut self, label: Vec<EdgeLabel>) -> Self {
        self.label = Some(label);
        self
    }

    pub fn matches_label(&self, label: &EdgeLabel) -> bool {
        match &self.label {
            Some(labels) => labels.contains(label),
            None => true,
        }
    }
}
