use core::convert::Infallible;
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
    thread::sleep,
    time::Duration,
};

use alloy_primitives::{Address, B256};
use revm::{
    Database, DatabaseCommit,
    primitives::{HashMap as RevmHashMap, StorageKey, StorageValue},
    state::{Account, AccountInfo, Bytecode},
};

/// Configuration for the synthetic latency-injecting database.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LatencyInjectingDbConfig {
    /// Cold-read latency for account lookups.
    pub account_miss_latency: Duration,
    /// Cold-read latency for storage lookups.
    pub storage_miss_latency: Duration,
    /// Cold-read latency for bytecode lookups.
    pub code_miss_latency: Duration,
    /// Cold-read latency for block hash lookups.
    pub block_hash_miss_latency: Duration,
}

impl Default for LatencyInjectingDbConfig {
    fn default() -> Self {
        Self {
            account_miss_latency: Duration::ZERO,
            storage_miss_latency: Duration::ZERO,
            code_miss_latency: Duration::ZERO,
            block_hash_miss_latency: Duration::ZERO,
        }
    }
}

/// Read statistics captured by [`LatencyInjectingDb`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LatencyDbStats {
    /// Total account reads.
    pub basic_reads: u64,
    /// Account reads that incurred miss latency.
    pub basic_misses: u64,
    /// Total storage reads.
    pub storage_reads: u64,
    /// Storage reads that incurred miss latency.
    pub storage_misses: u64,
    /// Total code reads.
    pub code_reads: u64,
    /// Code reads that incurred miss latency.
    pub code_misses: u64,
    /// Total block-hash reads.
    pub block_hash_reads: u64,
    /// Block-hash reads that incurred miss latency.
    pub block_hash_misses: u64,
}

#[derive(Debug, Default)]
struct LatencyInjectingDbState {
    accounts: HashMap<Address, AccountInfo>,
    storage: HashMap<(Address, StorageKey), StorageValue>,
    bytecode: HashMap<B256, Bytecode>,
    block_hashes: HashMap<u64, B256>,
    seen_accounts: HashSet<Address>,
    seen_storage: HashSet<(Address, StorageKey)>,
    seen_code: HashSet<B256>,
    seen_block_hashes: HashSet<u64>,
    stats: LatencyDbStats,
}

/// In-memory database that injects latency on first read of each key.
///
/// This is intentionally synthetic and is designed for benchmark `PoCs`. Clones share
/// data, cold/warm state, and stats.
#[derive(Debug, Clone)]
pub struct LatencyInjectingDb {
    config: LatencyInjectingDbConfig,
    state: Arc<Mutex<LatencyInjectingDbState>>,
}

impl LatencyInjectingDb {
    /// Creates an empty synthetic database.
    pub fn new(config: LatencyInjectingDbConfig) -> Self {
        Self { config, state: Arc::new(Mutex::new(LatencyInjectingDbState::default())) }
    }

    /// Inserts account info for `address`.
    pub fn insert_account(&self, address: Address, info: AccountInfo) {
        let mut state = self.state.lock().expect("latency db mutex poisoned");
        state.accounts.insert(address, info);
    }

    /// Inserts storage value for `(address, key)`.
    pub fn insert_storage(&self, address: Address, key: StorageKey, value: StorageValue) {
        let mut state = self.state.lock().expect("latency db mutex poisoned");
        state.storage.insert((address, key), value);
    }

    /// Inserts bytecode keyed by its hash.
    pub fn insert_bytecode(&self, code_hash: B256, code: Bytecode) {
        let mut state = self.state.lock().expect("latency db mutex poisoned");
        state.bytecode.insert(code_hash, code);
    }

    /// Inserts a block hash for block `number`.
    pub fn insert_block_hash(&self, number: u64, hash: B256) {
        let mut state = self.state.lock().expect("latency db mutex poisoned");
        state.block_hashes.insert(number, hash);
    }

    /// Marks an account key as warm for subsequent reads.
    pub fn warm_account(&self, address: Address) {
        let mut state = self.state.lock().expect("latency db mutex poisoned");
        state.seen_accounts.insert(address);
    }

    /// Marks a storage key as warm for subsequent reads.
    pub fn warm_storage(&self, address: Address, key: StorageKey) {
        let mut state = self.state.lock().expect("latency db mutex poisoned");
        state.seen_storage.insert((address, key));
    }

    /// Marks a bytecode hash as warm for subsequent reads.
    pub fn warm_code_hash(&self, code_hash: B256) {
        let mut state = self.state.lock().expect("latency db mutex poisoned");
        state.seen_code.insert(code_hash);
    }

    /// Marks a block hash lookup key as warm for subsequent reads.
    pub fn warm_block_hash(&self, number: u64) {
        let mut state = self.state.lock().expect("latency db mutex poisoned");
        state.seen_block_hashes.insert(number);
    }

    /// Clears cold/warm tracking so subsequent reads are cold again.
    pub fn reset_cold_reads(&self) {
        let mut state = self.state.lock().expect("latency db mutex poisoned");
        state.seen_accounts.clear();
        state.seen_storage.clear();
        state.seen_code.clear();
        state.seen_block_hashes.clear();
    }

    /// Resets accumulated read statistics.
    pub fn reset_stats(&self) {
        let mut state = self.state.lock().expect("latency db mutex poisoned");
        state.stats = LatencyDbStats::default();
    }

    /// Returns a snapshot of current statistics.
    pub fn stats(&self) -> LatencyDbStats {
        let state = self.state.lock().expect("latency db mutex poisoned");
        state.stats
    }

    fn basic_with_latency(&self, address: Address) -> Option<AccountInfo> {
        let (miss, value) = {
            let mut state = self.state.lock().expect("latency db mutex poisoned");
            state.stats.basic_reads = state.stats.basic_reads.saturating_add(1);
            let miss = state.seen_accounts.insert(address);
            if miss {
                state.stats.basic_misses = state.stats.basic_misses.saturating_add(1);
            }
            (miss, state.accounts.get(&address).cloned())
        };
        if miss {
            sleep(self.config.account_miss_latency);
        }
        value
    }

    fn code_with_latency(&self, code_hash: B256) -> Bytecode {
        let (miss, code) = {
            let mut state = self.state.lock().expect("latency db mutex poisoned");
            state.stats.code_reads = state.stats.code_reads.saturating_add(1);
            let miss = state.seen_code.insert(code_hash);
            if miss {
                state.stats.code_misses = state.stats.code_misses.saturating_add(1);
            }
            (miss, state.bytecode.get(&code_hash).cloned().unwrap_or_default())
        };
        if miss {
            sleep(self.config.code_miss_latency);
        }
        code
    }

    fn storage_with_latency(&self, address: Address, index: StorageKey) -> StorageValue {
        let (miss, value) = {
            let mut state = self.state.lock().expect("latency db mutex poisoned");
            state.stats.storage_reads = state.stats.storage_reads.saturating_add(1);
            let miss = state.seen_storage.insert((address, index));
            if miss {
                state.stats.storage_misses = state.stats.storage_misses.saturating_add(1);
            }
            (miss, state.storage.get(&(address, index)).copied().unwrap_or(StorageValue::ZERO))
        };
        if miss {
            sleep(self.config.storage_miss_latency);
        }
        value
    }

    fn block_hash_with_latency(&self, number: u64) -> B256 {
        let (miss, hash) = {
            let mut state = self.state.lock().expect("latency db mutex poisoned");
            state.stats.block_hash_reads = state.stats.block_hash_reads.saturating_add(1);
            let miss = state.seen_block_hashes.insert(number);
            if miss {
                state.stats.block_hash_misses = state.stats.block_hash_misses.saturating_add(1);
            }
            (miss, state.block_hashes.get(&number).copied().unwrap_or_default())
        };
        if miss {
            sleep(self.config.block_hash_miss_latency);
        }
        hash
    }
}

impl Database for LatencyInjectingDb {
    type Error = Infallible;

    fn basic(&mut self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        Ok(self.basic_with_latency(address))
    }

    fn code_by_hash(&mut self, code_hash: B256) -> Result<Bytecode, Self::Error> {
        Ok(self.code_with_latency(code_hash))
    }

    fn storage(
        &mut self,
        address: Address,
        index: StorageKey,
    ) -> Result<StorageValue, Self::Error> {
        Ok(self.storage_with_latency(address, index))
    }

    fn block_hash(&mut self, number: u64) -> Result<B256, Self::Error> {
        Ok(self.block_hash_with_latency(number))
    }
}

impl DatabaseCommit for LatencyInjectingDb {
    fn commit(&mut self, changes: RevmHashMap<Address, Account>) {
        let mut state = self.state.lock().expect("latency db mutex poisoned");
        for (address, account) in changes {
            if let Some(code) = account.info.code.clone() {
                state.bytecode.insert(account.info.code_hash, code);
            }
            for (slot, value) in account.storage {
                state.storage.insert((address, slot), value.present_value);
            }
            state.accounts.insert(address, account.info);
        }
    }
}

#[cfg(test)]
mod tests {
    use alloy_primitives::{Address, B256};
    use revm::{
        Database,
        primitives::{StorageKey, StorageValue},
    };

    use super::{AccountInfo, LatencyInjectingDb, LatencyInjectingDbConfig};

    #[test]
    fn cold_and_warm_reads_are_counted() {
        let db = LatencyInjectingDb::new(LatencyInjectingDbConfig::default());
        let address = Address::with_last_byte(1);
        db.insert_account(address, AccountInfo::default());

        db.reset_stats();
        let mut handle = db.clone();
        let _ = handle.basic(address);
        let _ = handle.basic(address);

        let stats = db.stats();
        assert_eq!(stats.basic_reads, 2);
        assert_eq!(stats.basic_misses, 1);
    }

    #[test]
    fn resetting_cold_reads_makes_next_read_a_miss_again() {
        let db = LatencyInjectingDb::new(LatencyInjectingDbConfig::default());
        let address = Address::with_last_byte(2);
        db.insert_account(address, AccountInfo::default());

        db.reset_stats();
        let mut handle = db.clone();
        let _ = handle.basic(address);
        db.reset_cold_reads();
        let _ = handle.basic(address);

        let stats = db.stats();
        assert_eq!(stats.basic_misses, 2);
    }

    #[test]
    fn explicitly_warmed_storage_reads_do_not_count_as_miss() {
        let db = LatencyInjectingDb::new(LatencyInjectingDbConfig::default());
        let address = Address::with_last_byte(3);
        let slot = StorageKey::from(1_u64);
        db.insert_storage(address, slot, StorageValue::from(7_u64));

        db.reset_stats();
        db.warm_storage(address, slot);

        let mut handle = db.clone();
        let value = handle.storage(address, slot).expect("read");

        assert_eq!(value, StorageValue::from(7_u64));
        let stats = db.stats();
        assert_eq!(stats.storage_reads, 1);
        assert_eq!(stats.storage_misses, 0);
    }

    #[test]
    fn explicitly_warmed_account_and_code_reads_do_not_count_as_miss() {
        let db = LatencyInjectingDb::new(LatencyInjectingDbConfig::default());
        let address = Address::with_last_byte(4);
        let code_hash = B256::with_last_byte(5);
        db.insert_account(address, AccountInfo { code_hash, ..Default::default() });
        db.insert_bytecode(code_hash, Default::default());

        db.reset_stats();
        db.warm_account(address);
        db.warm_code_hash(code_hash);

        let mut handle = db.clone();
        let _ = handle.basic(address).expect("basic");
        let _ = handle.code_by_hash(code_hash).expect("code");

        let stats = db.stats();
        assert_eq!(stats.basic_reads, 1);
        assert_eq!(stats.basic_misses, 0);
        assert_eq!(stats.code_reads, 1);
        assert_eq!(stats.code_misses, 0);
    }
}
