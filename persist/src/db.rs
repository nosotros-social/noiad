use anyhow::Result;
use rocksdb::{ColumnFamilyDescriptor, DBWithThreadMode, MultiThreaded, Options};
use std::path::Path;

use crate::{
    interner::Interner,
    schema::{self},
};

pub struct RocksDBConfig {
    pub max_total_wal_size: u64,
}

#[derive(Debug)]
pub struct PersistStore {
    pub db: DBWithThreadMode<MultiThreaded>,
    pub interner: Interner,
}

impl PersistStore {
    pub fn open(path: impl AsRef<Path>, config: RocksDBConfig) -> Result<Self> {
        let mut options = rocksdb::Options::default();
        options.create_if_missing(true);
        options.create_missing_column_families(true);
        options.set_write_buffer_size(256 * 1024 * 1024); // 256MB
        options.set_prefix_extractor(rocksdb::SliceTransform::create_fixed_prefix(32));
        options.set_max_total_wal_size(config.max_total_wal_size);

        let cf_opts = Options::default();
        let cfs: Vec<_> = schema::COLUMN_FAMILIES
            .iter()
            .map(|name| ColumnFamilyDescriptor::new(*name, cf_opts.clone()))
            .collect();

        let db = DBWithThreadMode::<MultiThreaded>::open_cf_descriptors(&options, path, cfs)?;
        let interner = Interner::new(&db)?;

        Ok(PersistStore { db, interner })
    }

    pub fn save_checkpoint(&self, checkpoint: u64) -> Result<()> {
        let cf = self
            .db
            .cf_handle(schema::CHECKPOINTS)
            .expect("checkpoint cf must exist");
        self.db.put_cf(&cf, b"latest", checkpoint.to_be_bytes())?;
        Ok(())
    }

    pub fn load_checkpoint(&self) -> Result<Option<u64>> {
        let cf = self
            .db
            .cf_handle(schema::CHECKPOINTS)
            .expect("checkpoint cf must exist");
        if let Some(value) = self.db.get_pinned_cf(&cf, b"latest")? {
            let bytes: [u8; 8] = value
                .as_ref()
                .try_into()
                .expect("checkpoint must be 8 bytes");
            Ok(Some(u64::from_be_bytes(bytes)))
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::helpers::TestStore;

    #[test]
    fn assert_store_column_families() {
        let store = TestStore::new();

        assert!(store.db.cf_handle(schema::INTERN_FORWARD).is_some());
        assert!(store.db.cf_handle(schema::INTERN_REVERSE).is_some());
        assert!(store.db.cf_handle(schema::EVENTS).is_some());
        assert!(store.db.cf_handle(schema::EVENTS_BY_PUBKEY).is_some());
        assert!(store.db.cf_handle(schema::CHECKPOINTS).is_some());
    }
}
