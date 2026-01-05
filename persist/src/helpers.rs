use crate::db::{PersistStore, RocksDBConfig};
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

pub struct TestStore {
    pub store: Option<PersistStore>,
    pub path: PathBuf,
}

static COUNTER: AtomicU64 = AtomicU64::new(0);

impl TestStore {
    pub fn new() -> Self {
        let id = COUNTER.fetch_add(1, Ordering::SeqCst);
        let path = PathBuf::from(format!("./test_db_{}", id));
        let config = RocksDBConfig {
            max_total_wal_size: 1024 * 1024 * 10,
        };
        let store = PersistStore::open(&path, config).unwrap();

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
