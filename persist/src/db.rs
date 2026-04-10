use anyhow::Result;
use rocksdb::{ColumnFamilyDescriptor, DBWithThreadMode, MultiThreaded, Options};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Mutex;
use tokio::sync::mpsc;
use types::types::Diff;

use crate::{
    event::{EventRaw, EventRecord},
    interner::Interner,
    schema::{self, CHECKPOINTS_CF, cf},
};

pub type PersistInputUpdate = (EventRaw, u64, Diff);
pub type PersistUpdate = (EventRecord, u64, Diff);
pub const NUM_EVENT_SHARDS: usize = 64;
type RocksDb = DBWithThreadMode<MultiThreaded>;
type WorkerKey = (usize, usize);

const CHECKPOINT_KEY: &[u8] = b"checkpoint";

#[derive(Debug)]
pub struct PersistStore {
    pub interner_db: RocksDb,
    pub db: RocksDb,
    pub interner: Interner,
    pub(crate) event_write_lock: Mutex<()>,
    subscribers: Mutex<HashMap<WorkerKey, mpsc::UnboundedSender<PersistUpdate>>>,
}

fn db_options() -> Options {
    let mut options = Options::default();
    options.create_if_missing(true);
    options.create_missing_column_families(true);
    options.set_write_buffer_size(256 * 1024 * 1024);
    options.set_max_total_wal_size(1024 * 1024 * 1024);
    options.set_compression_type(rocksdb::DBCompressionType::Zstd);
    options
}

pub fn event_shard_id(event_id: u32) -> usize {
    (event_id as usize) % NUM_EVENT_SHARDS
}

pub fn event_record_key(event_id: u32) -> [u8; 5] {
    let mut key = [0u8; 5];
    key[0] = event_shard_id(event_id) as u8;
    key[1..].copy_from_slice(&event_id.to_be_bytes());
    key
}

fn open_event_db(path: impl AsRef<Path>) -> Result<RocksDb> {
    let options = db_options();
    let cfs = [
        schema::EVENTS_CF,
        schema::EVENT_RAW_CF,
        schema::REPLACEABLE_CF,
        schema::ADDRESSABLE_CF,
        schema::CHECKPOINTS_CF,
    ]
    .into_iter()
    .map(|name| ColumnFamilyDescriptor::new(name, options.clone()))
    .collect::<Vec<_>>();

    let db = RocksDb::open_cf_descriptors(&options, path, cfs)?;
    Ok(db)
}

fn open_interner_db(path: impl AsRef<Path>) -> Result<RocksDb> {
    let options = db_options();
    let cfs = [schema::INTERN_FORWARD_CF, schema::INTERN_REVERSE_CF]
        .into_iter()
        .map(|name| ColumnFamilyDescriptor::new(name, options.clone()))
        .collect::<Vec<_>>();

    let db = RocksDb::open_cf_descriptors(&options, path, cfs)?;
    Ok(db)
}

impl PersistStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let root = path.as_ref();
        std::fs::create_dir_all(root)?;

        let interner_db = open_interner_db(root.join("interner"))?;
        let db = open_event_db(root.join("events"))?;
        let interner = Interner::new(&interner_db)?;
        Ok(PersistStore {
            interner_db,
            db,
            interner,
            event_write_lock: Mutex::new(()),
            subscribers: Mutex::new(HashMap::new()),
        })
    }

    pub fn intern(&self, bytes: &[u8]) -> Result<u32> {
        self.interner.intern(&self.interner_db, bytes)
    }

    pub fn intern_get(&self, bytes: &[u8]) -> Result<Option<u32>> {
        self.interner.get(&self.interner_db, bytes)
    }

    pub fn resolve_node(&self, node: u32) -> Result<Option<Vec<u8>>> {
        self.interner.resolve(&self.interner_db, node)
    }

    pub fn shard_for_event_id(&self, event_id: u32) -> usize {
        event_shard_id(event_id)
    }

    pub fn save_checkpoint(&self, checkpoint: u64) -> Result<()> {
        let cf = cf!(self.db, CHECKPOINTS_CF);
        self.db
            .put_cf(&cf, CHECKPOINT_KEY, checkpoint.to_be_bytes())?;
        Ok(())
    }

    pub fn load_checkpoint(&self) -> Result<Option<u64>> {
        let cf = cf!(self.db, CHECKPOINTS_CF);
        if let Some(value) = self.db.get_pinned_cf(&cf, CHECKPOINT_KEY)? {
            let bytes: [u8; 8] = value
                .as_ref()
                .try_into()
                .expect("checkpoint must be 8 bytes");
            let checkpoint = u64::from_be_bytes(bytes);
            Ok(Some(checkpoint))
        } else {
            Ok(None)
        }
    }

    pub fn subscribe(
        &self,
        worker_id: usize,
        workers: usize,
    ) -> mpsc::UnboundedReceiver<PersistUpdate> {
        let (tx, rx) = mpsc::unbounded_channel();
        self.subscribers
            .lock()
            .unwrap()
            .insert((worker_id, workers), tx);
        rx
    }

    pub fn notify(&self, event: EventRecord, ts: u64, diff: Diff) {
        self.subscribers
            .lock()
            .unwrap()
            .retain(|(worker_id, workers), tx| {
                if self.shard_for_event_id(event.id) % *workers != *worker_id {
                    return true;
                }

                tx.send((event.clone(), ts, diff)).is_ok()
            });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::helpers::TestStore;

    #[test]
    fn assert_store_column_families() {
        let store = TestStore::default();

        assert!(
            store
                .interner_db
                .cf_handle(schema::INTERN_FORWARD_CF)
                .is_some()
        );
        assert!(
            store
                .interner_db
                .cf_handle(schema::INTERN_REVERSE_CF)
                .is_some()
        );
        assert!(store.db.cf_handle(schema::INTERN_FORWARD_CF).is_none());
        assert!(store.db.cf_handle(schema::INTERN_REVERSE_CF).is_none());
        assert!(store.db.cf_handle(schema::EVENT_RAW_CF).is_some());
        assert!(store.db.cf_handle(schema::EVENTS_CF).is_some());
        assert!(store.db.cf_handle(schema::REPLACEABLE_CF).is_some());
        assert!(store.db.cf_handle(schema::ADDRESSABLE_CF).is_some());
        assert!(store.db.cf_handle(schema::CHECKPOINTS_CF).is_some());
    }
}
