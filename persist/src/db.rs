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
    pub event_shards: Vec<RocksDb>,
    pub db: RocksDb,
    pub interner: Interner,
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

fn open_main_db(path: impl AsRef<Path>) -> Result<RocksDb> {
    let options = db_options();
    let cfs = [
        schema::INTERN_FORWARD_CF,
        schema::INTERN_REVERSE_CF,
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

fn open_event_shard(path: impl AsRef<Path>) -> Result<RocksDb> {
    let options = db_options();
    let cfs = [schema::EVENTS_CF]
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
        std::fs::create_dir_all(root.join("event_shards"))?;

        let db = open_main_db(root.join("main"))?;
        let event_shards = (0..NUM_EVENT_SHARDS)
            .map(|shard_id| {
                open_event_shard(root.join("event_shards").join(format!("{shard_id:02}")))
            })
            .collect::<Result<Vec<_>>>()?;
        let interner = Interner::new(&db)?;
        Ok(PersistStore {
            event_shards,
            db,
            interner,
            subscribers: Mutex::new(HashMap::new()),
        })
    }

    pub fn shard_for_event_id(&self, event_id: u32) -> usize {
        (event_id as usize) % self.event_shards.len()
    }

    pub fn event_db(&self, event_id: u32) -> &RocksDb {
        &self.event_shards[self.shard_for_event_id(event_id)]
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

        assert!(store.db.cf_handle(schema::INTERN_FORWARD_CF).is_some());
        assert!(store.db.cf_handle(schema::INTERN_REVERSE_CF).is_some());
        assert!(store.db.cf_handle(schema::EVENT_RAW_CF).is_some());
        assert!(store.db.cf_handle(schema::REPLACEABLE_CF).is_some());
        assert!(store.db.cf_handle(schema::ADDRESSABLE_CF).is_some());
        assert!(store.db.cf_handle(schema::CHECKPOINTS_CF).is_some());
        assert_eq!(store.event_shards.len(), NUM_EVENT_SHARDS);
        for shard in &store.event_shards {
            assert!(shard.cf_handle(schema::EVENTS_CF).is_some());
        }
    }
}
