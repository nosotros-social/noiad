use crate::db::PersistStore;
use crate::event::EventRaw;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

pub struct TestStore {
    pub store: Option<PersistStore>,
    pub path: PathBuf,
}

static COUNTER: AtomicU64 = AtomicU64::new(0);

impl Default for TestStore {
    fn default() -> Self {
        let id = COUNTER.fetch_add(1, Ordering::SeqCst);
        let path = PathBuf::from(format!("./test_db_{}", id));
        let store = PersistStore::open(&path).unwrap();

        Self {
            store: Some(store),
            path,
        }
    }
}

impl Drop for TestStore {
    fn drop(&mut self) {
        self.store.take();
        let _ = fs::remove_dir_all(&self.path);
    }
}

impl std::ops::Deref for TestStore {
    type Target = PersistStore;

    fn deref(&self) -> &Self::Target {
        self.store.as_ref().unwrap()
    }
}

impl std::ops::DerefMut for TestStore {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.store.as_mut().unwrap()
    }
}

impl Default for EventRaw {
    fn default() -> Self {
        Self {
            id: [0u8; 32],
            pubkey: [0u8; 32],
            created_at: 0,
            content: Vec::new(),
            sig: [0u8; 64],
            kind: 0,
            tags_json: Vec::new(),
        }
    }
}

#[macro_export]
macro_rules! event_raw {
    ($($field:ident $(:$value:expr)?),* $(,)?) => {
        $crate::event::EventRaw {
            $($field $(: $value)?),*,
            ..Default::default()
        }
    };
}
