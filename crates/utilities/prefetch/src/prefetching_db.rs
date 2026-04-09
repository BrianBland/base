use std::{collections::HashMap as StdHashMap, sync::Arc};

use alloy_primitives::{Address, B256};
use dashmap::DashMap;
use revm::{
    Database, DatabaseCommit,
    primitives::{HashMap, StorageKey, StorageValue},
    state::{Account, AccountInfo, Bytecode},
};

/// Prefetched storage-value buffer used by [`PrefetchingDb`].
#[derive(Debug, Clone)]
pub enum PrefetchBuffer {
    /// Concurrently writable buffer for asynchronous prefetch.
    Concurrent(Arc<DashMap<(Address, StorageKey), StorageValue>>),
    /// Frozen read-only buffer for synchronous prefetch.
    Frozen(Arc<StdHashMap<(Address, StorageKey), StorageValue>>),
}

impl PrefetchBuffer {
    /// Creates a concurrent prefetch buffer sized for `capacity` entries.
    pub fn concurrent(capacity: usize) -> Self {
        Self::Concurrent(Arc::new(DashMap::with_capacity(capacity)))
    }

    /// Creates a frozen prefetch buffer from already-prefetched entries.
    pub fn frozen(entries: StdHashMap<(Address, StorageKey), StorageValue>) -> Self {
        Self::Frozen(Arc::new(entries))
    }

    /// Returns `true` when the buffer currently contains no prefetched entries.
    pub fn is_empty(&self) -> bool {
        match self {
            Self::Concurrent(buffer) => buffer.is_empty(),
            Self::Frozen(buffer) => buffer.is_empty(),
        }
    }

    /// Looks up a prefetched value for `(address, slot)`.
    pub fn get(&self, address: Address, slot: StorageKey) -> Option<StorageValue> {
        match self {
            Self::Concurrent(buffer) => buffer.get(&(address, slot)).map(|value| *value),
            Self::Frozen(buffer) => buffer.get(&(address, slot)).copied(),
        }
    }

    /// Inserts a prefetched `(address, slot) -> value` entry into a concurrent buffer.
    pub fn insert(&self, address: Address, slot: StorageKey, value: StorageValue) -> bool {
        match self {
            Self::Concurrent(buffer) => {
                buffer.insert((address, slot), value);
                true
            }
            Self::Frozen(_) => false,
        }
    }
}

/// Database wrapper that consults a prefetch buffer for storage reads.
#[derive(Debug)]
pub struct PrefetchingDb<DB>
where
    DB: DatabaseCommit + Database,
{
    db: DB,
    buffer: PrefetchBuffer,
}

impl<DB> PrefetchingDb<DB>
where
    DB: DatabaseCommit + Database,
{
    /// Creates a new prefetching database wrapper.
    pub const fn new(db: DB, buffer: PrefetchBuffer) -> Self {
        Self { db, buffer }
    }

    /// Returns a reference to the underlying database.
    pub const fn db(&self) -> &DB {
        &self.db
    }

    /// Returns a mutable reference to the underlying database.
    pub const fn db_mut(&mut self) -> &mut DB {
        &mut self.db
    }

    /// Returns a reference to the current prefetch buffer.
    pub const fn buffer(&self) -> &PrefetchBuffer {
        &self.buffer
    }
}

impl<DB> Database for PrefetchingDb<DB>
where
    DB: DatabaseCommit + Database,
{
    type Error = <DB as Database>::Error;

    fn basic(&mut self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        self.db.basic(address)
    }

    fn code_by_hash(&mut self, code_hash: B256) -> Result<Bytecode, Self::Error> {
        self.db.code_by_hash(code_hash)
    }

    fn storage(
        &mut self,
        address: Address,
        index: StorageKey,
    ) -> Result<StorageValue, Self::Error> {
        if !self.buffer.is_empty()
            && let Some(value) = self.buffer.get(address, index)
        {
            return Ok(value);
        }

        self.db.storage(address, index)
    }

    fn block_hash(&mut self, number: u64) -> Result<B256, Self::Error> {
        self.db.block_hash(number)
    }
}

impl<DB> DatabaseCommit for PrefetchingDb<DB>
where
    DB: DatabaseCommit + Database,
{
    fn commit(&mut self, changes: HashMap<Address, Account>) {
        self.db.commit(changes);
    }
}

#[cfg(test)]
mod tests {
    use alloy_primitives::Address;
    use revm::{
        Database,
        primitives::{StorageKey, StorageValue},
    };

    use super::{PrefetchBuffer, PrefetchingDb};
    use crate::{LatencyInjectingDb, LatencyInjectingDbConfig};

    #[test]
    fn frozen_buffer_short_circuits_storage_lookup() {
        let address = Address::with_last_byte(1);
        let slot = StorageKey::from(7_u64);
        let db = LatencyInjectingDb::new(LatencyInjectingDbConfig::default());
        db.insert_storage(address, slot, StorageValue::from(11_u64));

        let mut entries = std::collections::HashMap::new();
        entries.insert((address, slot), StorageValue::from(99_u64));
        let buffer = PrefetchBuffer::frozen(entries);
        let mut prefetching_db = PrefetchingDb::new(db, buffer);

        let value = prefetching_db.storage(address, slot).expect("storage read succeeds");
        assert_eq!(value, StorageValue::from(99_u64));
    }

    #[test]
    fn concurrent_buffer_short_circuits_storage_lookup() {
        let address = Address::with_last_byte(2);
        let slot = StorageKey::from(8_u64);
        let db = LatencyInjectingDb::new(LatencyInjectingDbConfig::default());
        db.insert_storage(address, slot, StorageValue::from(12_u64));

        let buffer = PrefetchBuffer::concurrent(1);
        assert!(buffer.insert(address, slot, StorageValue::from(77_u64)));
        let mut prefetching_db = PrefetchingDb::new(db, buffer);

        let value = prefetching_db.storage(address, slot).expect("storage read succeeds");
        assert_eq!(value, StorageValue::from(77_u64));
    }
}
