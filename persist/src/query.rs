#[derive(Debug, Clone, Default)]
pub struct PersistQuery {
    pub kinds: Option<Vec<u16>>,
}

impl PersistQuery {
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
