use std::{
    collections::HashMap as StdHashMap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use alloy_primitives::{Address, B256};
use dashmap::DashMap;
use revm::{
    Database, DatabaseCommit,
    primitives::{HashMap, StorageKey, StorageValue},
    state::{Account, AccountInfo, Bytecode},
};

/// Snapshot of observed prefetch activity for one execution window.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PrefetchMetricsSnapshot {
    /// Number of entries inserted by the prefetch worker.
    pub prefetched_entries: u64,
    /// Number of duplicate prefetch inserts that replaced an existing value.
    pub duplicate_prefetch_entries: u64,
    /// Number of storage reads satisfied from the prefetch buffer.
    pub storage_prefetch_hits: u64,
    /// Number of storage reads that fell through to the backing database.
    pub storage_prefetch_misses: u64,
    /// Total nanoseconds spent on storage reads satisfied from the prefetch buffer.
    pub total_prefetch_hit_latency_ns: u64,
    /// Total nanoseconds spent on storage reads that fell through to the backing database.
    pub total_storage_db_latency_ns: u64,
}

impl PrefetchMetricsSnapshot {
    /// Returns the number of unique prefetched entries currently represented by the snapshot.
    pub const fn unique_prefetched_entries(&self) -> u64 {
        self.prefetched_entries.saturating_sub(self.duplicate_prefetch_entries)
    }

    /// Returns the average latency of database-backed storage reads.
    pub const fn average_storage_db_latency(&self) -> Duration {
        if self.storage_prefetch_misses == 0 {
            return Duration::ZERO;
        }
        Duration::from_nanos(self.total_storage_db_latency_ns / self.storage_prefetch_misses)
    }

    /// Returns the average latency of prefetch-buffer hits.
    pub const fn average_prefetch_hit_latency(&self) -> Duration {
        if self.storage_prefetch_hits == 0 {
            return Duration::ZERO;
        }
        Duration::from_nanos(self.total_prefetch_hit_latency_ns / self.storage_prefetch_hits)
    }

    /// Returns the fraction of requested hints that were later used, scaled by 10,000.
    pub const fn useful_prefetch_ratio_x10000(&self, requested_hint_count: usize) -> u32 {
        if requested_hint_count == 0 {
            return 0;
        }
        ((self.storage_prefetch_hits as u128).saturating_mul(10_000) / requested_hint_count as u128)
            as u32
    }
}

#[derive(Debug, Default)]
struct PrefetchMetricsInner {
    prefetched_entries: AtomicU64,
    duplicate_prefetch_entries: AtomicU64,
    storage_prefetch_hits: AtomicU64,
    storage_prefetch_misses: AtomicU64,
    total_prefetch_hit_latency_ns: AtomicU64,
    total_storage_db_latency_ns: AtomicU64,
}

/// Shared telemetry handle for prefetch execution.
#[derive(Debug, Clone, Default)]
pub struct PrefetchMetrics {
    inner: Arc<PrefetchMetricsInner>,
}

impl PrefetchMetrics {
    /// Creates a new empty telemetry handle.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates telemetry with `prefetched_entries` pre-populated.
    pub fn with_prefetched_entries(prefetched_entries: usize) -> Self {
        let metrics = Self::new();
        metrics.inner.prefetched_entries.store(prefetched_entries as u64, Ordering::Relaxed);
        metrics
    }

    /// Returns a point-in-time snapshot of the telemetry.
    pub fn snapshot(&self) -> PrefetchMetricsSnapshot {
        PrefetchMetricsSnapshot {
            prefetched_entries: self.inner.prefetched_entries.load(Ordering::Relaxed),
            duplicate_prefetch_entries: self
                .inner
                .duplicate_prefetch_entries
                .load(Ordering::Relaxed),
            storage_prefetch_hits: self.inner.storage_prefetch_hits.load(Ordering::Relaxed),
            storage_prefetch_misses: self.inner.storage_prefetch_misses.load(Ordering::Relaxed),
            total_prefetch_hit_latency_ns: self
                .inner
                .total_prefetch_hit_latency_ns
                .load(Ordering::Relaxed),
            total_storage_db_latency_ns: self
                .inner
                .total_storage_db_latency_ns
                .load(Ordering::Relaxed),
        }
    }

    /// Records one prefetch insert and whether it replaced an existing entry.
    pub fn record_prefetch_insert(&self, duplicate: bool) {
        self.inner.prefetched_entries.fetch_add(1, Ordering::Relaxed);
        if duplicate {
            self.inner.duplicate_prefetch_entries.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Records a storage lookup satisfied from the prefetch buffer.
    pub fn record_storage_prefetch_hit(&self, latency: Duration) {
        self.inner.storage_prefetch_hits.fetch_add(1, Ordering::Relaxed);
        self.record_duration(&self.inner.total_prefetch_hit_latency_ns, latency);
    }

    /// Records a storage lookup that fell through to the backing database.
    pub fn record_storage_prefetch_miss(&self, latency: Duration) {
        self.inner.storage_prefetch_misses.fetch_add(1, Ordering::Relaxed);
        self.record_duration(&self.inner.total_storage_db_latency_ns, latency);
    }

    fn record_duration(&self, target: &AtomicU64, duration: Duration) {
        let nanos = duration.as_nanos().min(u64::MAX as u128) as u64;
        target.fetch_add(nanos, Ordering::Relaxed);
    }
}

/// Prefetched storage-value buffer used by [`PrefetchingDb`].
#[derive(Debug, Clone)]
pub enum PrefetchBuffer {
    /// Concurrently writable buffer for asynchronous prefetch.
    Concurrent {
        /// Shared concurrent entries.
        entries: Arc<DashMap<(Address, StorageKey), StorageValue>>,
        /// Shared prefetch telemetry.
        metrics: PrefetchMetrics,
    },
    /// Frozen read-only buffer for synchronous prefetch.
    Frozen {
        /// Shared read-only entries.
        entries: Arc<StdHashMap<(Address, StorageKey), StorageValue>>,
        /// Shared prefetch telemetry.
        metrics: PrefetchMetrics,
    },
}

impl PrefetchBuffer {
    /// Creates a concurrent prefetch buffer sized for `capacity` entries.
    pub fn concurrent(capacity: usize) -> Self {
        Self::Concurrent {
            entries: Arc::new(DashMap::with_capacity(capacity)),
            metrics: PrefetchMetrics::new(),
        }
    }

    /// Creates a frozen prefetch buffer from already-prefetched entries.
    pub fn frozen(entries: StdHashMap<(Address, StorageKey), StorageValue>) -> Self {
        let metrics = PrefetchMetrics::with_prefetched_entries(entries.len());
        Self::Frozen { entries: Arc::new(entries), metrics }
    }

    /// Returns `true` when the buffer currently contains no prefetched entries.
    pub fn is_empty(&self) -> bool {
        match self {
            Self::Concurrent { entries, .. } => entries.is_empty(),
            Self::Frozen { entries, .. } => entries.is_empty(),
        }
    }

    /// Looks up a prefetched value for `(address, slot)`.
    pub fn get(&self, address: Address, slot: StorageKey) -> Option<StorageValue> {
        match self {
            Self::Concurrent { entries, .. } => entries.get(&(address, slot)).map(|value| *value),
            Self::Frozen { entries, .. } => entries.get(&(address, slot)).copied(),
        }
    }

    /// Inserts a prefetched `(address, slot) -> value` entry into a concurrent buffer.
    pub fn insert(&self, address: Address, slot: StorageKey, value: StorageValue) -> bool {
        match self {
            Self::Concurrent { entries, metrics } => {
                let duplicate = entries.insert((address, slot), value).is_some();
                metrics.record_prefetch_insert(duplicate);
                true
            }
            Self::Frozen { .. } => false,
        }
    }

    /// Returns a snapshot of shared prefetch telemetry.
    pub fn metrics(&self) -> PrefetchMetricsSnapshot {
        self.metrics_handle().snapshot()
    }

    /// Returns a cloneable telemetry handle shared with the buffer.
    pub fn metrics_handle(&self) -> PrefetchMetrics {
        match self {
            Self::Concurrent { metrics, .. } | Self::Frozen { metrics, .. } => metrics.clone(),
        }
    }

    /// Records a storage lookup satisfied from the prefetch buffer.
    pub fn record_storage_prefetch_hit(&self, latency: Duration) {
        self.metrics_handle().record_storage_prefetch_hit(latency);
    }

    /// Records a storage lookup that fell through to the backing database.
    pub fn record_storage_prefetch_miss(&self, latency: Duration) {
        self.metrics_handle().record_storage_prefetch_miss(latency);
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
        let lookup_start = Instant::now();
        if !self.buffer.is_empty()
            && let Some(value) = self.buffer.get(address, index)
        {
            self.buffer.record_storage_prefetch_hit(lookup_start.elapsed());
            return Ok(value);
        }

        let value = self.db.storage(address, index)?;
        self.buffer.record_storage_prefetch_miss(lookup_start.elapsed());
        Ok(value)
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
    use std::time::Duration;

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
        let metrics = prefetching_db.buffer().metrics();
        assert_eq!(metrics.prefetched_entries, 1);
        assert_eq!(metrics.storage_prefetch_hits, 1);
        assert_eq!(metrics.storage_prefetch_misses, 0);
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
        let metrics = prefetching_db.buffer().metrics();
        assert_eq!(metrics.prefetched_entries, 1);
        assert_eq!(metrics.storage_prefetch_hits, 1);
        assert_eq!(metrics.storage_prefetch_misses, 0);
    }

    #[test]
    fn missing_buffer_entry_records_db_fallback() {
        let address = Address::with_last_byte(3);
        let slot = StorageKey::from(9_u64);
        let db = LatencyInjectingDb::new(LatencyInjectingDbConfig {
            storage_miss_latency: Duration::from_micros(1),
            ..Default::default()
        });
        db.insert_storage(address, slot, StorageValue::from(13_u64));

        let buffer = PrefetchBuffer::concurrent(1);
        let mut prefetching_db = PrefetchingDb::new(db, buffer);

        let value = prefetching_db.storage(address, slot).expect("storage read succeeds");
        assert_eq!(value, StorageValue::from(13_u64));
        let metrics = prefetching_db.buffer().metrics();
        assert_eq!(metrics.storage_prefetch_hits, 0);
        assert_eq!(metrics.storage_prefetch_misses, 1);
        assert!(metrics.average_storage_db_latency() >= Duration::from_micros(1));
    }
}
