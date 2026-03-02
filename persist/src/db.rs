use anyhow::Result;
use rocksdb::{ColumnFamilyDescriptor, DBWithThreadMode, MultiThreaded, Options};
use std::path::Path;
use tokio::sync::broadcast;
use types::types::Diff;

use crate::{
    event::{EventRaw, EventRecord},
    interner::Interner,
    schema::{self, CHECKPOINTS_CF, cf},
};

pub type PersistInputUpdate = (EventRaw, u64, Diff);
pub type PersistUpdate = (EventRecord, u64, Diff);

const CHECKPOINT_KEY: &[u8] = b"checkpoint";

#[derive(Debug)]
pub struct PersistStore {
    pub db: DBWithThreadMode<MultiThreaded>,
    pub interner: Interner,
    updates_tx: broadcast::Sender<PersistUpdate>,
}

impl PersistStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let mut options = Options::default();
        options.create_if_missing(true);
        options.create_missing_column_families(true);
        options.set_write_buffer_size(256 * 1024 * 1024);
        options.set_max_total_wal_size(1024 * 1024 * 1024);
        options.set_compression_type(rocksdb::DBCompressionType::Zstd);

        let cfs: Vec<_> = schema::COLUMN_FAMILIES
            .iter()
            .map(|name| ColumnFamilyDescriptor::new(*name, options.clone()))
            .collect();

        let db = DBWithThreadMode::<MultiThreaded>::open_cf_descriptors(&options, path, cfs)?;
        let interner = Interner::new(&db)?;
        let (updates_tx, _) = broadcast::channel(100_000);

        Ok(PersistStore {
            db,
            interner,
            updates_tx,
        })
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
            Ok(Some(u64::from_be_bytes(bytes)))
        } else {
            Ok(None)
        }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<PersistUpdate> {
        self.updates_tx.subscribe()
    }

    pub fn notify(&self, event: EventRecord, ts: u64, diff: Diff) {
        let _ = self.updates_tx.send((event, ts, diff));
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
        assert!(store.db.cf_handle(schema::EVENTS_CF).is_some());
        assert!(store.db.cf_handle(schema::REPLACEABLE_CF).is_some());
        assert!(store.db.cf_handle(schema::ADDRESSABLE_CF).is_some());
        assert!(store.db.cf_handle(schema::CHECKPOINTS_CF).is_some());
    }
}
