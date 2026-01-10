use std::fmt::Display;

use serde::{Deserialize, Serialize};

pub type Node = u32;
pub type Edge = (u32, u32);
pub type DiffRow = u64;

#[derive(Debug, Clone, Serialize, Deserialize, PartialOrd, PartialEq, Eq, Ord)]
pub struct EventRow {
    pub id: Node,
    pub pubkey: Node,
    pub kind: u16,
    pub created_at: u64,
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

impl EventRow {
    pub fn new(id: u32, pubkey: u32, kind: u16, created_at: u64) -> Self {
        Self {
            id,
            pubkey,
            kind,
            created_at,
        }
    }
}

// TODO add columnation

#[derive(Debug, Clone)]
pub struct EventRaw {
    pub id: [u8; 32],
    pub pubkey: [u8; 32],
    pub created_at: u64,
    pub kind: u16,
    pub tags_json: Vec<u8>,
}
