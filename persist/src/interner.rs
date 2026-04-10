use anyhow::Result;
use rocksdb::{DBWithThreadMode, MultiThreaded, WriteBatch};
use std::{
    collections::HashMap,
    hash::{DefaultHasher, Hash, Hasher},
    sync::Mutex,
    sync::atomic::{AtomicU32, Ordering},
};

use crate::schema::{INTERN_FORWARD_CF, INTERN_REVERSE_CF, cf};

const PENDING_SHARDS: usize = 64;

#[derive(Debug)]
pub struct Interner {
    next_symbol: AtomicU32,
    pending_shards: Vec<Mutex<HashMap<Vec<u8>, (u32, usize)>>>,
}

/// A batch for interning multiple hashes at once.
pub struct InternBatch<'a> {
    pub db: &'a DBWithThreadMode<MultiThreaded>,
    interner: &'a Interner,
    pub write_batch: WriteBatch,
    cache: HashMap<Vec<u8>, u32>,
    pending_keys: Vec<Vec<u8>>,
}

impl<'a> InternBatch<'a> {
    pub fn new(db: &'a DBWithThreadMode<MultiThreaded>, interner: &'a Interner) -> Self {
        Self {
            db,
            interner,
            write_batch: WriteBatch::default(),
            cache: HashMap::new(),
            pending_keys: Vec::new(),
        }
    }
}

impl Drop for InternBatch<'_> {
    fn drop(&mut self) {
        if self.pending_keys.is_empty() {
            return;
        }

        for key in self.pending_keys.drain(..) {
            let shard_idx = Interner::pending_shard_index(&key);
            let mut pending = self.interner.pending_shards[shard_idx].lock().unwrap();
            if let Some((_, refs)) = pending.get_mut(&key) {
                *refs -= 1;
                if *refs == 0 {
                    pending.remove(&key);
                }
            }
        }
    }
}

impl Interner {
    pub fn new(db: &DBWithThreadMode<MultiThreaded>) -> Result<Self> {
        let next_symbol = Self::find_max_symbol(db)? + 1;
        Ok(Self {
            next_symbol: AtomicU32::new(next_symbol),
            pending_shards: (0..PENDING_SHARDS)
                .map(|_| Mutex::new(HashMap::new()))
                .collect(),
        })
    }

    fn pending_shard_index(bytes: &[u8]) -> usize {
        let mut hasher = DefaultHasher::new();
        bytes.hash(&mut hasher);
        (hasher.finish() as usize) % PENDING_SHARDS
    }

    pub fn get(&self, db: &DBWithThreadMode<MultiThreaded>, bytes: &[u8]) -> Result<Option<u32>> {
        let cf_forward = cf!(db, INTERN_FORWARD_CF);

        let Some(pinned) = db.get_pinned_cf(&cf_forward, bytes)? else {
            return Ok(None);
        };

        let arr: [u8; 4] = pinned[..4].try_into().expect("must be 4 bytes");
        Ok(Some(u32::from_be_bytes(arr)))
    }

    pub fn find_max_symbol(db: &DBWithThreadMode<MultiThreaded>) -> Result<u32> {
        let cf = cf!(db, INTERN_REVERSE_CF);
        let mut iter = db.raw_iterator_cf(&cf);
        iter.seek_to_last();

        if iter.valid()
            && let Some(key) = iter.key()
            && key.len() == 4
        {
            let bytes: [u8; 4] = key.try_into().expect("reverse key must be 4 bytes");
            return Ok(u32::from_be_bytes(bytes));
        }

        Ok(0)
    }

    pub fn intern(&self, db: &DBWithThreadMode<MultiThreaded>, hex: &[u8]) -> Result<u32> {
        let cf_forward = cf!(db, INTERN_FORWARD_CF);
        let cf_reverse = cf!(db, INTERN_REVERSE_CF);

        if let Some(symbol) = self.get(db, hex)? {
            return Ok(symbol);
        }

        let shard_idx = Self::pending_shard_index(hex);
        let mut pending = self.pending_shards[shard_idx].lock().unwrap();
        if let Some(symbol) = self.get(db, hex)? {
            return Ok(symbol);
        }
        if let Some((symbol, _)) = pending.get(hex) {
            return Ok(*symbol);
        }

        let symbol = self.next_symbol.fetch_add(1, Ordering::Relaxed);
        let symbol_bytes = symbol.to_be_bytes();
        let mut batch = WriteBatch::default();
        batch.put_cf(&cf_forward, hex, symbol_bytes);
        batch.put_cf(&cf_reverse, symbol_bytes, hex);
        pending.insert(hex.to_vec(), (symbol, 1));
        db.write(batch)?;
        pending.remove(hex);

        Ok(symbol)
    }

    pub fn intern_batch(&self, batch: &mut InternBatch, hex: &[u8]) -> Result<u32> {
        if let Some(symbol) = batch.cache.get(hex) {
            return Ok(*symbol);
        }

        if let Some(symbol) = self.get(batch.db, hex)? {
            batch.cache.insert(hex.to_vec(), symbol);
            return Ok(symbol);
        }

        let shard_idx = Self::pending_shard_index(hex);
        let mut pending = self.pending_shards[shard_idx].lock().unwrap();
        if let Some(symbol) = self.get(batch.db, hex)? {
            batch.cache.insert(hex.to_vec(), symbol);
            return Ok(symbol);
        }
        if let Some((symbol, refs)) = pending.get_mut(hex) {
            let symbol = *symbol;
            *refs += 1;
            let symbol_bytes = symbol.to_be_bytes();
            let cf_forward = cf!(batch.db, INTERN_FORWARD_CF);
            let cf_reverse = cf!(batch.db, INTERN_REVERSE_CF);
            batch.write_batch.put_cf(&cf_forward, hex, symbol_bytes);
            batch.write_batch.put_cf(&cf_reverse, symbol_bytes, hex);
            batch.cache.insert(hex.to_vec(), symbol);
            batch.pending_keys.push(hex.to_vec());
            return Ok(symbol);
        }

        let symbol = self.next_symbol.fetch_add(1, Ordering::Relaxed);
        let symbol_bytes = symbol.to_be_bytes();
        let cf_forward = cf!(batch.db, INTERN_FORWARD_CF);
        let cf_reverse = cf!(batch.db, INTERN_REVERSE_CF);
        batch.write_batch.put_cf(&cf_forward, hex, symbol_bytes);
        batch.write_batch.put_cf(&cf_reverse, symbol_bytes, hex);

        let key = hex.to_vec();
        pending.insert(key.clone(), (symbol, 1));
        batch.cache.insert(key.clone(), symbol);
        batch.pending_keys.push(key);

        Ok(symbol)
    }

    pub fn resolve(
        &self,
        db: &DBWithThreadMode<MultiThreaded>,
        symbol: u32,
    ) -> Result<Option<Vec<u8>>> {
        let cf_reverse = cf!(db, INTERN_REVERSE_CF);
        let symbol_bytes = symbol.to_be_bytes();
        let bytes = db.get_cf(&cf_reverse, symbol_bytes)?;
        Ok(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::helpers::TestStore;
    use std::{collections::HashSet, sync::Arc, thread};

    #[test]
    fn assert_interner() {
        let store = TestStore::default();
        let db = &store.interner_db;
        let interner = &store.interner;

        let pubkey1 = [1u8; 32];
        let pubkey2 = [2u8; 32];
        let symbol1 = interner.intern(db, &pubkey1).unwrap();
        let symbol1_again = interner.intern(db, &pubkey1).unwrap();
        let symbol2 = interner.intern(db, &pubkey2).unwrap();
        let symbol3 = interner.intern(db, &[1u8]).unwrap();
        let symbol3_again = interner.intern(db, &[1u8]).unwrap();
        let symbol4 = interner.intern(db, &[2u8]).unwrap();

        assert_eq!(symbol1, 1);
        assert_eq!(symbol1_again, 1);
        assert_eq!(symbol2, 2);

        assert_eq!(symbol3, 3);
        assert_eq!(symbol4, 4);
        assert_eq!(symbol3_again, 3);
        assert_eq!(store.interner.next_symbol.load(Ordering::Relaxed), 5);
        assert_eq!(Interner::find_max_symbol(&store.interner_db).unwrap(), 4);

        assert_eq!(
            interner.resolve(db, symbol1).unwrap().unwrap(),
            pubkey1.to_vec()
        );
        assert_eq!(
            interner.resolve(db, symbol2).unwrap().unwrap(),
            pubkey2.to_vec()
        );
        assert_eq!(interner.resolve(db, symbol3).unwrap().unwrap(), vec![1u8]);
        assert_eq!(interner.resolve(db, symbol4).unwrap().unwrap(), vec![2u8]);
        assert_eq!(
            interner.resolve(db, symbol3_again).unwrap().unwrap(),
            vec![1u8]
        );
    }

    #[test]
    fn assert_multithread_intern_different_bytes() {
        let store = Arc::new(TestStore::default());
        let num_threads = 8;

        let handles: Vec<_> = (0..num_threads)
            .map(|i| {
                let store = Arc::clone(&store);
                thread::spawn(move || {
                    let mut bytes = [0u8; 32];
                    bytes[0] = i as u8;
                    store.intern(&bytes).unwrap()
                })
            })
            .collect();

        let symbols: HashSet<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();

        assert_eq!(symbols.len(), num_threads);
    }

    #[test]
    fn assert_batched_intern_same_bytes() {
        let store = TestStore::default();
        let mut batch = InternBatch::new(&store.interner_db, &store.interner);

        let bytes_a = [1u8; 32];
        let bytes_b = [2u8; 32];

        let symbol_a1 = store.interner.intern_batch(&mut batch, &bytes_a).unwrap();
        let symbol_a2 = store.interner.intern_batch(&mut batch, &bytes_a).unwrap();
        let symbol_a3 = store.interner.intern_batch(&mut batch, &bytes_a).unwrap();

        assert_eq!(symbol_a1, symbol_a2);
        assert_eq!(symbol_a2, symbol_a3);

        let symbol_b1 = store.interner.intern_batch(&mut batch, &bytes_b).unwrap();
        let symbol_b2 = store.interner.intern_batch(&mut batch, &bytes_b).unwrap();

        assert_eq!(symbol_b1, symbol_b2);
        assert_ne!(symbol_a1, symbol_b1);

        store
            .interner_db
            .write(std::mem::take(&mut batch.write_batch))
            .unwrap();

        let resolved_a = store.resolve_node(symbol_a1).unwrap().unwrap();
        let resolved_b = store.resolve_node(symbol_b1).unwrap().unwrap();

        assert_eq!(resolved_a, bytes_a.to_vec());
        assert_eq!(resolved_b, bytes_b.to_vec());
    }

    #[test]
    fn assert_batched_intern_same_bytes_across_batches() {
        let store = TestStore::default();
        let bytes = [9u8; 32];

        // Simulate two concurrent writers
        let mut batch_a = InternBatch::new(&store.interner_db, &store.interner);
        let mut batch_b = InternBatch::new(&store.interner_db, &store.interner);

        let symbol_a = store.interner.intern_batch(&mut batch_a, &bytes).unwrap();
        let symbol_b = store.interner.intern_batch(&mut batch_b, &bytes).unwrap();

        assert_eq!(symbol_a, symbol_b);

        store
            .interner_db
            .write(std::mem::take(&mut batch_a.write_batch))
            .unwrap();
        store
            .interner_db
            .write(std::mem::take(&mut batch_b.write_batch))
            .unwrap();

        let symbol = store.intern_get(&bytes).unwrap().unwrap();
        assert_eq!(symbol, symbol_a);
        assert_eq!(
            store.resolve_node(symbol_a).unwrap().unwrap(),
            bytes.to_vec()
        );
    }
}
