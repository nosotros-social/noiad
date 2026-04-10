use serde::{Deserialize, Serialize};
use std::fmt::Display;

use crate::{edges::Edge, types::Node};

#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialOrd, PartialEq, Eq, Ord, Hash)]
pub struct EventRow {
    pub id: Node,
    pub pubkey: Node,
    pub kind: u16,
    pub created_at: u32,
    pub edges: Vec<Edge>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialOrd, PartialEq, Eq, Ord, Hash)]
pub struct EventRowCompactValue {
    pub pubkey: Node,
    pub kind: u16,
    pub created_at: u32,
}
// TODO add columnation

impl EventRow {
    #[inline]
    pub fn compact(&self) -> EventRowCompactValue {
        EventRowCompactValue {
            pubkey: self.pubkey,
            kind: self.kind,
            created_at: self.created_at as u32,
        }
    }
}

impl Display for EventRow {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "EventRow {{ id: {}, pubkey: {}, kind: {}, created_at: {} }}",
            self.id, self.pubkey, self.kind, self.created_at
        )
    }
}
