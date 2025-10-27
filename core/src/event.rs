use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    pub id: String,
    pub kind: u16,
    pub pubkey: String,
    pub content: String,
    pub created_at: u64,
    pub tags: Vec<Vec<String>>,
}
