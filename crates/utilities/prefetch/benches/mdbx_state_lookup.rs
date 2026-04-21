//! Criterion benchmark for MDBX state lookup latency calibration.
//!
//! Two benchmark groups are produced per data source:
//! - `warm`: standard run, OS page cache stays warm across iterations.
//! - `cold_disk`: each Criterion sample evicts the data file from the OS page cache via
//!   `posix_fadvise(POSIX_FADV_DONTNEED)` (Linux only) so reads have to round-trip to disk.
//!
//! The `cold_disk` group requires Linux (in-process or via Docker). On macOS native it is skipped
//! because `posix_fadvise` is not available and `purge` requires root.

use std::{collections::HashSet, env, hint::black_box, path::PathBuf, time::Duration};

use alloy_primitives::{Address, B256, U256, keccak256};
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use reth_db::{
    Database,
    cursor::{DbCursorRO, DbCursorRW, DbDupCursorRO},
    tables,
    transaction::{DbTx, DbTxMut},
};
use reth_primitives_traits::{Account, StorageEntry};
use tempfile::TempDir;

const MDBX_PATH_ENV: &str = "BASE_PREFETCH_MDBX_PATH";
const LOOKUP_KEY_COUNT_ENV: &str = "BASE_PREFETCH_LOOKUP_KEY_COUNT";
const COLD_CACHE_ENV: &str = "BASE_PREFETCH_COLD_CACHE";
const USDC_HOLDERS_ENV: &str = "BASE_PREFETCH_USDC_HOLDERS";

const TOTAL_ACCOUNTS: usize = 50_000;
const SLOTS_PER_ACCOUNT: usize = 4;
const HOT_KEYS: usize = 1_024;
const LOOKUPS_PER_ITERATION: usize = 20_000;
const COLD_LOOKUPS_PER_ITERATION: usize = 256;
const DEFAULT_SAMPLED_KEYS: usize = 64_000;

/// USDC mainnet `_balances` mapping lives at storage slot 9.
const USDC_BALANCES_SLOT: u64 = 9;
/// Synthetic USDC contract address used when seeding holders. Last byte chosen so the address is
/// distinct from any of the LCG-derived holder addresses in [`holder_address`].
const USDC_TOKEN_ADDRESS: Address = Address::new([
    0xC0, 0xC0, 0xC0, 0xC0, 0xC0, 0xC0, 0xC0, 0xC0, 0xC0, 0xC0, 0xC0, 0xC0, 0xC0, 0xC0, 0xC0, 0xC0,
    0xC0, 0xC0, 0xC0, 0xC1,
]);

/// Cross-platform helpers for running MDBX with as little caching as possible.
///
/// On Linux, [`evict`] uses the strongest eviction mechanism available:
/// 1. `drop_caches` (`/proc/sys/vm/drop_caches`) — reliable but requires `CAP_SYS_ADMIN`. Used by
///    default when writable, which is the case inside a privileged Docker container or on a Linux
///    host running the bench as root.
/// 2. `posix_fadvise(POSIX_FADV_DONTNEED)` against the MDBX data file — no privileges required,
///    but advisory only: the kernel keeps mmap-backed pages around when there is no memory
///    pressure, so cold-disk numbers may not be cold at all.
///
/// On other platforms eviction is a no-op and a warning is printed once.
mod cold_cache {
    use std::path::{Path, PathBuf};
    use std::sync::OnceLock;

    use reth_db::mdbx::DatabaseArguments;

    #[cfg(target_os = "linux")]
    const DROP_CACHES_PATH: &str = "/proc/sys/vm/drop_caches";

    /// Returns `DatabaseArguments` configured to minimize MDBX-internal caching effects.
    ///
    /// reth already hardcodes `MDBX_NORDAHEAD` so we cannot tighten read-ahead further. We do
    /// open exclusive with a single reader slot to avoid reader-table churn between samples.
    pub(super) fn cold_friendly_args() -> DatabaseArguments {
        DatabaseArguments::default()
            .with_exclusive(Some(true))
            .with_max_readers(Some(1))
    }

    /// Returns the path of the MDBX data file inside `db_dir`, if it exists.
    pub(super) fn data_file_path(db_dir: &Path) -> Option<PathBuf> {
        for candidate in ["mdbx.dat", "data.mdb"] {
            let path = db_dir.join(candidate);
            if path.exists() {
                return Some(path);
            }
        }
        None
    }

    /// Whether the user requested cold-cache benchmarks.
    pub(super) fn requested() -> bool {
        std::env::var_os(super::COLD_CACHE_ENV)
            .is_some_and(|value| !value.is_empty() && value != "0")
    }

    /// The eviction strategy that will be used for [`evict`].
    ///
    /// `DropCaches` and `Fadvise` only exist on Linux; on other platforms only `Unsupported` is
    /// constructible, which avoids dead-code warnings without resorting to `#[allow(...)]`.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) enum Strategy {
        #[cfg(target_os = "linux")]
        DropCaches,
        #[cfg(target_os = "linux")]
        Fadvise,
        Unsupported,
    }

    impl Strategy {
        pub(super) const fn label(self) -> &'static str {
            match self {
                #[cfg(target_os = "linux")]
                Self::DropCaches => "drop_caches",
                #[cfg(target_os = "linux")]
                Self::Fadvise => "posix_fadvise",
                Self::Unsupported => "unsupported",
            }
        }
    }

    /// Detects which eviction strategy is available, preferring stronger options.
    ///
    /// On Linux, attempts a probe write to `/proc/sys/vm/drop_caches` to verify CAP_SYS_ADMIN; if
    /// that fails, falls back to `posix_fadvise`. On other platforms returns `Unsupported`.
    pub(super) fn detect_strategy() -> Strategy {
        #[cfg(target_os = "linux")]
        {
            if probe_drop_caches() {
                return Strategy::DropCaches;
            }
            warn_once(
                "/proc/sys/vm/drop_caches is not writable (CAP_SYS_ADMIN missing). Falling back \
                 to posix_fadvise(POSIX_FADV_DONTNEED), which is advisory and may not actually \
                 evict mmap-backed pages. Re-run with `--cap-add SYS_ADMIN` (Docker) or as root \
                 for reliable cold-cache numbers."
                    .to_string(),
            );
            Strategy::Fadvise
        }

        #[cfg(not(target_os = "linux"))]
        {
            Strategy::Unsupported
        }
    }

    /// Drops the OS page cache for `path` using the strategy returned by [`detect_strategy`].
    /// Failures are logged once and ignored so a single hiccup does not nuke the entire run.
    pub(super) fn evict(strategy: Strategy, path: &Path) {
        match strategy {
            #[cfg(target_os = "linux")]
            Strategy::DropCaches => evict_drop_caches(),
            #[cfg(target_os = "linux")]
            Strategy::Fadvise => evict_fadvise(path),
            Strategy::Unsupported => {
                let _ = path;
                warn_once(
                    "cold-cache eviction not supported on this platform; run inside a Linux \
                     container with --cap-add SYS_ADMIN (see crates/utilities/prefetch/README.md)."
                        .to_string(),
                );
            }
        }
    }

    #[cfg(target_os = "linux")]
    fn probe_drop_caches() -> bool {
        std::fs::OpenOptions::new()
            .write(true)
            .open(DROP_CACHES_PATH)
            .and_then(|mut file| std::io::Write::write_all(&mut file, b"1\n"))
            .is_ok()
    }

    #[cfg(target_os = "linux")]
    fn evict_drop_caches() {
        if let Err(error) = std::fs::OpenOptions::new()
            .write(true)
            .open(DROP_CACHES_PATH)
            .and_then(|mut file| std::io::Write::write_all(&mut file, b"1\n"))
        {
            warn_once(format!("write to {DROP_CACHES_PATH} failed: {error}"));
        }
    }


    #[cfg(target_os = "linux")]
    fn evict_fadvise(path: &Path) {
        use std::os::fd::AsRawFd;
        match std::fs::File::open(path) {
            Ok(file) => {
                let result = unsafe {
                    libc::posix_fadvise(file.as_raw_fd(), 0, 0, libc::POSIX_FADV_DONTNEED)
                };
                if result != 0 {
                    warn_once(format!(
                        "posix_fadvise(POSIX_FADV_DONTNEED) failed for {} with errno={result}",
                        path.display()
                    ));
                }
            }
            Err(error) => warn_once(format!(
                "could not open {} for cache eviction: {error}",
                path.display()
            )),
        }
    }


    fn warn_once(message: String) {
        static WARNED: OnceLock<()> = OnceLock::new();
        WARNED.get_or_init(|| {
            eprintln!("[base-prefetch::cold_cache] {message}");
        });
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PlainStorageLookupKey {
    address: Address,
    slot: B256,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct HashedStorageLookupKey {
    hashed_address: B256,
    slot: B256,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AccessPattern {
    WarmSingleKey,
    FullRandom,
    Mixed { hot_percent: u8 },
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum DataSource {
    ExistingDb(PathBuf),
    SeededEphemeralDb,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct LcgRng {
    state: u64,
}

impl LcgRng {
    const fn with_seed(seed: u64) -> Self {
        Self { state: seed }
    }

    const fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
        self.state
    }

    fn next_index(&mut self, upper_bound: usize) -> usize {
        debug_assert!(upper_bound > 0);
        (self.next_u64() as usize) % upper_bound
    }

    fn next_hot_pick(&mut self, hot_percent: u8) -> bool {
        (self.next_u64() % 100) < u64::from(hot_percent)
    }
}

/// Owns either an existing on-disk MDBX directory or an ephemeral seeded one.
///
/// Holding the [`TempDir`] keeps the seeded database alive for the duration of the benchmark
/// while still letting us address the MDBX data file path for cold-cache eviction.
struct DbHandle<DB> {
    db: DB,
    data_file: Option<PathBuf>,
    _tempdir: Option<TempDir>,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct MdbxStateLookupBenchmark;

impl MdbxStateLookupBenchmark {
    fn run(c: &mut Criterion) {
        let sampled_keys_target = Self::lookup_key_target();
        let cold_requested = cold_cache::requested();
        let cold_strategy =
            if cold_requested { cold_cache::detect_strategy() } else { cold_cache::Strategy::Unsupported };
        let cold_enabled = cold_requested && cold_strategy != cold_cache::Strategy::Unsupported;
        if cold_requested {
            if cold_enabled {
                eprintln!(
                    "[base-prefetch::cold_cache] cold benchmarks enabled, eviction strategy = {}",
                    cold_strategy.label()
                );
            } else {
                eprintln!(
                    "[base-prefetch::cold_cache] {COLD_CACHE_ENV} requested but the current \
                     platform cannot evict the OS page cache. Run inside a Linux container with \
                     --cap-add SYS_ADMIN (see crates/utilities/prefetch/README.md). Continuing \
                     with warm benches only."
                );
            }
        }

        match Self::data_source() {
            DataSource::ExistingDb(path) => {
                let db = reth_db::open_db_read_only(&path, cold_cache::cold_friendly_args())
                    .unwrap_or_else(|error| {
                        panic!("failed to open MDBX DB at {}: {error}", path.as_path().display())
                    });
                let data_file = cold_cache::data_file_path(&path);
                let handle = DbHandle { db, data_file, _tempdir: None };

                let plain_keys = Self::sample_plain_storage_keys(&handle.db, sampled_keys_target);
                if !plain_keys.is_empty() {
                    let hot_keys = plain_keys
                        .iter()
                        .copied()
                        .take(HOT_KEYS.min(plain_keys.len()))
                        .collect::<Vec<_>>();
                    Self::run_group_plain(
                        c,
                        &handle,
                        "existing_db_plain",
                        &plain_keys,
                        &hot_keys,
                        cold_enabled,
                        cold_strategy,
                    );
                    return;
                }

                let hashed_keys = Self::sample_hashed_storage_keys(&handle.db, sampled_keys_target);
                assert!(
                    !hashed_keys.is_empty(),
                    "no PlainStorageState or HashedStorages rows found in provided MDBX database"
                );
                let hot_keys = hashed_keys
                    .iter()
                    .copied()
                    .take(HOT_KEYS.min(hashed_keys.len()))
                    .collect::<Vec<_>>();
                Self::run_group_hashed(
                    c,
                    &handle,
                    "existing_db_hashed",
                    &hashed_keys,
                    &hot_keys,
                    cold_enabled,
                    cold_strategy,
                );
            }
            DataSource::SeededEphemeralDb => {
                if let Some(holder_count) = Self::usdc_holder_count() {
                    let handle = Self::create_seeded_handle();
                    eprintln!(
                        "[base-prefetch::usdc] seeding {holder_count} USDC holders \
                         (PlainAccountState + PlainStorageState[USDC])..."
                    );
                    let seed_start = std::time::Instant::now();
                    let seeded = Self::seed_usdc_state(&handle.db, holder_count);
                    let (apparent_mb, allocated_mb) =
                        Self::db_file_size_mb(&handle).unwrap_or((0.0, 0.0));
                    eprintln!(
                        "[base-prefetch::usdc] seeded {} holders in {:.1}s; MDBX file apparent \
                         size = {:.2} MB, allocated on disk = {:.2} MB",
                        seeded,
                        seed_start.elapsed().as_secs_f64(),
                        apparent_mb,
                        allocated_mb,
                    );
                    Self::run_group_usdc(
                        c,
                        &handle,
                        seeded as u64,
                        cold_enabled,
                        cold_strategy,
                    );
                    return;
                }

                let handle = Self::create_seeded_handle();
                let plain_keys =
                    Self::seed_plain_storage_state(&handle.db, TOTAL_ACCOUNTS, SLOTS_PER_ACCOUNT);
                let hot_keys = plain_keys
                    .iter()
                    .copied()
                    .take(HOT_KEYS.min(plain_keys.len()))
                    .collect::<Vec<_>>();
                Self::run_group_plain(
                    c,
                    &handle,
                    "seeded_ephemeral",
                    &plain_keys,
                    &hot_keys,
                    cold_enabled,
                    cold_strategy,
                );
            }
        }
    }

    fn usdc_holder_count() -> Option<usize> {
        env::var(USDC_HOLDERS_ENV).ok().and_then(|raw| {
            let trimmed = raw.trim();
            if trimmed.is_empty() || trimmed == "0" {
                return None;
            }
            trimmed.replace('_', "").parse::<usize>().ok().filter(|value| *value > 0)
        })
    }

    /// Reports `(apparent_mb, allocated_mb)` for the MDBX data file. `apparent_mb` is
    /// `stat.st_size` (the file size as `ls -l` would show); `allocated_mb` is
    /// `stat.st_blocks * 512` (the bytes actually pinned on disk, sparse-aware). MDBX maps the
    /// data file to its geometry max, which often produces a sparse file much larger than the
    /// populated B-tree pages, so the two numbers can differ by orders of magnitude.
    #[cfg(unix)]
    fn db_file_size_mb<DB>(handle: &DbHandle<DB>) -> Option<(f64, f64)> {
        use std::os::unix::fs::MetadataExt;
        handle
            .data_file
            .as_ref()
            .and_then(|path| std::fs::metadata(path).ok())
            .map(|meta| {
                let apparent = meta.len() as f64 / (1024.0 * 1024.0);
                let allocated = (meta.blocks() * 512) as f64 / (1024.0 * 1024.0);
                (apparent, allocated)
            })
    }

    #[cfg(not(unix))]
    fn db_file_size_mb<DB>(handle: &DbHandle<DB>) -> Option<(f64, f64)> {
        handle.data_file.as_ref().and_then(|path| std::fs::metadata(path).ok()).map(|meta| {
            let mb = meta.len() as f64 / (1024.0 * 1024.0);
            (mb, mb)
        })
    }

    fn create_seeded_handle() -> DbHandle<reth_db::DatabaseEnv> {
        let tempdir = tempfile::tempdir().expect("tempdir for seeded mdbx");
        let db = reth_db::init_db(tempdir.path(), cold_cache::cold_friendly_args())
            .expect("seed mdbx env");
        let data_file = cold_cache::data_file_path(tempdir.path());
        DbHandle { db, data_file, _tempdir: Some(tempdir) }
    }

    fn run_group_plain<DB>(
        c: &mut Criterion,
        handle: &DbHandle<DB>,
        source_label: &str,
        keys: &[PlainStorageLookupKey],
        hot_keys: &[PlainStorageLookupKey],
        include_cold: bool,
        cold_strategy: cold_cache::Strategy,
    ) where
        DB: Database,
    {
        let mut group = c.benchmark_group(format!("mdbx_storage_lookup/{source_label}/warm"));
        group.warm_up_time(Duration::from_secs(2));
        group.measurement_time(Duration::from_secs(8));
        group.sample_size(100);
        group.throughput(Throughput::Elements(LOOKUPS_PER_ITERATION as u64));

        group.bench_function("warm_single_key", |b| {
            b.iter(|| {
                Self::run_plain_lookup_batch(
                    &handle.db,
                    keys,
                    hot_keys,
                    AccessPattern::WarmSingleKey,
                    LOOKUPS_PER_ITERATION,
                );
            });
        });
        group.bench_function("full_random", |b| {
            b.iter(|| {
                Self::run_plain_lookup_batch(
                    &handle.db,
                    keys,
                    hot_keys,
                    AccessPattern::FullRandom,
                    LOOKUPS_PER_ITERATION,
                );
            });
        });
        group.bench_function("mixed_hot90", |b| {
            b.iter(|| {
                Self::run_plain_lookup_batch(
                    &handle.db,
                    keys,
                    hot_keys,
                    AccessPattern::Mixed { hot_percent: 90 },
                    LOOKUPS_PER_ITERATION,
                );
            });
        });
        group.bench_function("mixed_hot70", |b| {
            b.iter(|| {
                Self::run_plain_lookup_batch(
                    &handle.db,
                    keys,
                    hot_keys,
                    AccessPattern::Mixed { hot_percent: 70 },
                    LOOKUPS_PER_ITERATION,
                );
            });
        });
        group.finish();

        if !include_cold {
            return;
        }

        let Some(data_file) = handle.data_file.as_ref() else {
            eprintln!(
                "[base-prefetch::cold_cache] cold benchmarks skipped for {source_label}: no MDBX \
                 data file located"
            );
            return;
        };

        let mut group = c.benchmark_group(format!("mdbx_storage_lookup/{source_label}/cold_disk"));
        group.warm_up_time(Duration::from_millis(500));
        group.measurement_time(Duration::from_secs(20));
        group.sample_size(20);
        group.throughput(Throughput::Elements(COLD_LOOKUPS_PER_ITERATION as u64));

        group.bench_function("full_random", |b| {
            b.iter_custom(|iters| {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    cold_cache::evict(cold_strategy, data_file);
                    let start = std::time::Instant::now();
                    Self::run_plain_lookup_batch(
                        &handle.db,
                        keys,
                        hot_keys,
                        AccessPattern::FullRandom,
                        COLD_LOOKUPS_PER_ITERATION,
                    );
                    total += start.elapsed();
                }
                total
            });
        });
        group.bench_function("mixed_hot70", |b| {
            b.iter_custom(|iters| {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    cold_cache::evict(cold_strategy, data_file);
                    let start = std::time::Instant::now();
                    Self::run_plain_lookup_batch(
                        &handle.db,
                        keys,
                        hot_keys,
                        AccessPattern::Mixed { hot_percent: 70 },
                        COLD_LOOKUPS_PER_ITERATION,
                    );
                    total += start.elapsed();
                }
                total
            });
        });
        group.finish();
    }

    fn run_group_hashed<DB>(
        c: &mut Criterion,
        handle: &DbHandle<DB>,
        source_label: &str,
        keys: &[HashedStorageLookupKey],
        hot_keys: &[HashedStorageLookupKey],
        include_cold: bool,
        cold_strategy: cold_cache::Strategy,
    ) where
        DB: Database,
    {
        let mut group = c.benchmark_group(format!("mdbx_storage_lookup/{source_label}/warm"));
        group.warm_up_time(Duration::from_secs(2));
        group.measurement_time(Duration::from_secs(8));
        group.sample_size(100);
        group.throughput(Throughput::Elements(LOOKUPS_PER_ITERATION as u64));

        group.bench_function("warm_single_key", |b| {
            b.iter(|| {
                Self::run_hashed_lookup_batch(
                    &handle.db,
                    keys,
                    hot_keys,
                    AccessPattern::WarmSingleKey,
                    LOOKUPS_PER_ITERATION,
                );
            });
        });
        group.bench_function("full_random", |b| {
            b.iter(|| {
                Self::run_hashed_lookup_batch(
                    &handle.db,
                    keys,
                    hot_keys,
                    AccessPattern::FullRandom,
                    LOOKUPS_PER_ITERATION,
                );
            });
        });
        group.bench_function("mixed_hot90", |b| {
            b.iter(|| {
                Self::run_hashed_lookup_batch(
                    &handle.db,
                    keys,
                    hot_keys,
                    AccessPattern::Mixed { hot_percent: 90 },
                    LOOKUPS_PER_ITERATION,
                );
            });
        });
        group.bench_function("mixed_hot70", |b| {
            b.iter(|| {
                Self::run_hashed_lookup_batch(
                    &handle.db,
                    keys,
                    hot_keys,
                    AccessPattern::Mixed { hot_percent: 70 },
                    LOOKUPS_PER_ITERATION,
                );
            });
        });
        group.finish();

        if !include_cold {
            return;
        }

        let Some(data_file) = handle.data_file.as_ref() else {
            eprintln!(
                "[base-prefetch::cold_cache] cold benchmarks skipped for {source_label}: no MDBX \
                 data file located"
            );
            return;
        };

        let mut group = c.benchmark_group(format!("mdbx_storage_lookup/{source_label}/cold_disk"));
        group.warm_up_time(Duration::from_millis(500));
        group.measurement_time(Duration::from_secs(20));
        group.sample_size(20);
        group.throughput(Throughput::Elements(COLD_LOOKUPS_PER_ITERATION as u64));

        group.bench_function("full_random", |b| {
            b.iter_custom(|iters| {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    cold_cache::evict(cold_strategy, data_file);
                    let start = std::time::Instant::now();
                    Self::run_hashed_lookup_batch(
                        &handle.db,
                        keys,
                        hot_keys,
                        AccessPattern::FullRandom,
                        COLD_LOOKUPS_PER_ITERATION,
                    );
                    total += start.elapsed();
                }
                total
            });
        });
        group.bench_function("mixed_hot70", |b| {
            b.iter_custom(|iters| {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    cold_cache::evict(cold_strategy, data_file);
                    let start = std::time::Instant::now();
                    Self::run_hashed_lookup_batch(
                        &handle.db,
                        keys,
                        hot_keys,
                        AccessPattern::Mixed { hot_percent: 70 },
                        COLD_LOOKUPS_PER_ITERATION,
                    );
                    total += start.elapsed();
                }
                total
            });
        });
        group.finish();
    }

    fn seed_plain_storage_state<DB>(
        db: &DB,
        total_accounts: usize,
        slots_per_account: usize,
    ) -> Vec<PlainStorageLookupKey>
    where
        DB: Database,
    {
        let mut keys = Vec::with_capacity(total_accounts.saturating_mul(slots_per_account));
        let tx = db.tx_mut().expect("mdbx tx should open");
        let mut cursor = tx
            .cursor_dup_write::<tables::PlainStorageState>()
            .expect("plain storage cursor should open");

        for account_idx in 0..total_accounts {
            let address = Self::address_from_index(account_idx);
            for slot_idx in 0..slots_per_account {
                let slot_number = (account_idx * slots_per_account + slot_idx) as u64;
                let slot = Self::b256_from_u64(slot_number);
                let value = U256::from(slot_number.saturating_add(1));
                cursor
                    .upsert(address, &StorageEntry { key: slot, value })
                    .expect("seed upsert should succeed");
                keys.push(PlainStorageLookupKey { address, slot });
            }
        }

        drop(cursor);
        tx.commit().expect("seed commit should succeed");
        keys
    }

    /// Derives a holder address from a deterministic index by hashing the index. Resulting
    /// addresses are uniformly distributed across the 20-byte address space, mirroring real
    /// users whose addresses are random hashes.
    fn holder_address(index: u64) -> Address {
        let mut buf = [0_u8; 32];
        buf[24..32].copy_from_slice(&index.to_be_bytes());
        let hash = keccak256(buf);
        Address::from_slice(&hash[12..32])
    }

    /// Solidity mapping slot for `_balances[holder]` when `_balances` lives at slot 9 (USDC).
    fn usdc_balance_slot(holder: Address) -> B256 {
        let mut buf = [0_u8; 64];
        buf[12..32].copy_from_slice(holder.as_slice());
        buf[56..64].copy_from_slice(&USDC_BALANCES_SLOT.to_be_bytes());
        keccak256(buf)
    }

    /// Seeds `holder_count` holder accounts (PlainAccountState) plus their USDC balance entries
    /// (PlainStorageState dupsort under `USDC_TOKEN_ADDRESS`). The USDC contract account itself
    /// is also written. Returns the seeded holder count so callers can derive holder addresses on
    /// the fly via [`holder_address`] without materializing a billions-long Vec.
    ///
    /// Insert order matters a lot at scale: MDBX is dramatically faster when keys arrive in
    /// monotonically-increasing order (no page splits, near-bulk-load throughput). We do two
    /// global-sort passes, one per table, so every insert hits the rightmost B-tree page.
    ///
    /// To fit billion-holder runs in a 16 GB Docker VM we sort by a *single packed `u64`* per
    /// entry: the top 32 bits hold the address/slot prefix, the low 32 bits hold the source
    /// index. That is 8 bytes per entry, so 1B holders' sort vector fits in ~8 GB and 4B holders
    /// fits in ~32 GB. Sort is by the packed `u64` directly, which gives prefix-major /
    /// index-minor ordering — collisions on the 32-bit prefix sub-sort by index, which is fine
    /// since a colliding-prefix group at 1B holders averages only `~ceil(1B / 2^32) = 1` entry.
    /// Recomputing the full key during insert costs one keccak256 per row (~100 ns), negligible
    /// next to MDBX cursor + page-write work.
    fn seed_usdc_state<DB>(db: &DB, holder_count: usize) -> usize
    where
        DB: Database,
    {
        assert!(
            holder_count <= u32::MAX as usize,
            "holder_count {holder_count} exceeds u32 source-index range; widen the sort-vec \
             index type to u64 to support more holders"
        );

        let usdc_account = Account {
            nonce: 1,
            balance: U256::ZERO,
            bytecode_hash: Some(B256::with_last_byte(0xC1)),
        };

        // Phase 1: PlainAccountState. Sort by packed (32-bit address prefix, 32-bit index),
        // stream-insert.
        {
            let mut sorted: Vec<u64> = (0..holder_count as u32)
                .map(|index| {
                    let address = Self::holder_address(u64::from(index));
                    Self::pack_sort_key(Self::address_prefix(address), index)
                })
                .collect();
            sorted.sort_unstable();

            let tx = db.tx_mut().expect("mdbx tx_mut should open");
            let mut cursor = tx
                .cursor_write::<tables::PlainAccountState>()
                .expect("plain account cursor should open");
            cursor
                .upsert(USDC_TOKEN_ADDRESS, &usdc_account)
                .expect("USDC contract upsert should succeed");
            for packed in &sorted {
                let index = Self::unpack_sort_index(*packed);
                let address = Self::holder_address(u64::from(index));
                let account = Account {
                    nonce: 1,
                    balance: U256::from(1_000_000_000_000_000_000_u128),
                    bytecode_hash: None,
                };
                cursor.upsert(address, &account).expect("account upsert should succeed");
            }
            drop(cursor);
            tx.commit().expect("account-phase commit should succeed");
        }

        // Phase 2: PlainStorageState (USDC balances). Sort by packed (32-bit slot prefix,
        // 32-bit index). All entries land under one dup-tree (USDC_TOKEN_ADDRESS), so
        // prefix-sorted slot order keeps insertion close to the rightmost dup-leaf.
        {
            let mut sorted: Vec<u64> = (0..holder_count as u32)
                .map(|index| {
                    let slot = Self::usdc_balance_slot(Self::holder_address(u64::from(index)));
                    Self::pack_sort_key(Self::slot_prefix(slot), index)
                })
                .collect();
            sorted.sort_unstable();

            let tx = db.tx_mut().expect("mdbx tx_mut should open");
            let mut cursor = tx
                .cursor_dup_write::<tables::PlainStorageState>()
                .expect("plain storage cursor should open");
            for packed in &sorted {
                let index = Self::unpack_sort_index(*packed);
                let slot = Self::usdc_balance_slot(Self::holder_address(u64::from(index)));
                cursor
                    .upsert(
                        USDC_TOKEN_ADDRESS,
                        &StorageEntry {
                            key: slot,
                            value: U256::from(1_000_000_u64.saturating_add(u64::from(index))),
                        },
                    )
                    .expect("balance upsert should succeed");
            }
            drop(cursor);
            tx.commit().expect("storage-phase commit should succeed");
        }

        holder_count
    }

    fn pack_sort_key(prefix32: u32, index: u32) -> u64 {
        (u64::from(prefix32) << 32) | u64::from(index)
    }

    fn unpack_sort_index(packed: u64) -> u32 {
        packed as u32
    }

    fn address_prefix(address: Address) -> u32 {
        u32::from_be_bytes(address.as_slice()[0..4].try_into().expect("address has 20 bytes"))
    }

    fn slot_prefix(slot: B256) -> u32 {
        u32::from_be_bytes(slot.as_slice()[0..4].try_into().expect("B256 has 32 bytes"))
    }

    /// Reads `holder.account` for the next `count` random holders.
    fn run_usdc_account_lookup_batch<DB>(
        db: &DB,
        holder_count: u64,
        count: usize,
        rng: &mut LcgRng,
    ) where
        DB: Database,
    {
        let tx = db.tx().expect("mdbx tx should open");
        let mut cursor = tx
            .cursor_read::<tables::PlainAccountState>()
            .expect("plain account cursor should open");
        for _ in 0..count {
            let address = Self::holder_address(rng.next_u64() % holder_count);
            let entry = cursor.seek_exact(address).expect("account seek").expect("account row");
            black_box(entry);
        }
    }

    /// Reads `holder.balance` (one PlainStorageState dup-cursor lookup) for the next `count`
    /// random holders.
    fn run_usdc_balance_lookup_batch<DB>(
        db: &DB,
        holder_count: u64,
        count: usize,
        rng: &mut LcgRng,
    ) where
        DB: Database,
    {
        let tx = db.tx().expect("mdbx tx should open");
        let mut cursor = tx
            .cursor_dup_read::<tables::PlainStorageState>()
            .expect("plain storage cursor should open");
        for _ in 0..count {
            let address = Self::holder_address(rng.next_u64() % holder_count);
            let slot = Self::usdc_balance_slot(address);
            let entry = cursor
                .seek_by_key_subkey(USDC_TOKEN_ADDRESS, slot)
                .expect("balance seek")
                .expect("balance row");
            black_box(entry);
        }
    }

    /// Reads BOTH the holder's account row AND their USDC balance row for `count` random holders.
    /// This is the closest approximation to what `transferFrom` does for one party.
    fn run_usdc_account_plus_balance_batch<DB>(
        db: &DB,
        holder_count: u64,
        count: usize,
        rng: &mut LcgRng,
    ) where
        DB: Database,
    {
        let tx = db.tx().expect("mdbx tx should open");
        let mut account_cursor = tx
            .cursor_read::<tables::PlainAccountState>()
            .expect("plain account cursor should open");
        let mut storage_cursor = tx
            .cursor_dup_read::<tables::PlainStorageState>()
            .expect("plain storage cursor should open");
        for _ in 0..count {
            let address = Self::holder_address(rng.next_u64() % holder_count);
            let account = account_cursor.seek_exact(address).expect("account seek").expect("row");
            let balance = storage_cursor
                .seek_by_key_subkey(USDC_TOKEN_ADDRESS, Self::usdc_balance_slot(address))
                .expect("balance seek")
                .expect("row");
            black_box((account, balance));
        }
    }

    fn run_group_usdc<DB>(
        c: &mut Criterion,
        handle: &DbHandle<DB>,
        holder_count: u64,
        include_cold: bool,
        cold_strategy: cold_cache::Strategy,
    ) where
        DB: Database,
    {
        let label = "usdc_holders";
        let cold_lookups = COLD_LOOKUPS_PER_ITERATION as u64;
        let warm_lookups = LOOKUPS_PER_ITERATION as u64;

        let mut warm = c.benchmark_group(format!("{label}/warm"));
        warm.warm_up_time(Duration::from_secs(1));
        warm.measurement_time(Duration::from_secs(6));
        warm.sample_size(50);
        warm.throughput(Throughput::Elements(warm_lookups));
        warm.bench_function("account_only", |b| {
            b.iter(|| {
                let mut rng = LcgRng::with_seed(0xACC0_ACC0_ACC0_ACC0);
                Self::run_usdc_account_lookup_batch(
                    &handle.db,
                    holder_count,
                    LOOKUPS_PER_ITERATION,
                    &mut rng,
                );
            });
        });
        warm.bench_function("balance_only", |b| {
            b.iter(|| {
                let mut rng = LcgRng::with_seed(0xBA1A_BA1A_FACE_FACE);
                Self::run_usdc_balance_lookup_batch(
                    &handle.db,
                    holder_count,
                    LOOKUPS_PER_ITERATION,
                    &mut rng,
                );
            });
        });
        warm.bench_function("account_plus_balance", |b| {
            b.iter(|| {
                let mut rng = LcgRng::with_seed(0xFA17_BA1A_FA17_BA1A);
                Self::run_usdc_account_plus_balance_batch(
                    &handle.db,
                    holder_count,
                    LOOKUPS_PER_ITERATION,
                    &mut rng,
                );
            });
        });
        warm.finish();

        if !include_cold {
            return;
        }

        let Some(data_file) = handle.data_file.as_ref() else {
            eprintln!("[base-prefetch::usdc] no MDBX data file located, skipping cold benches");
            return;
        };

        let mut cold = c.benchmark_group(format!("{label}/cold_disk"));
        cold.warm_up_time(Duration::from_millis(500));
        cold.measurement_time(Duration::from_secs(20));
        cold.sample_size(20);
        cold.throughput(Throughput::Elements(cold_lookups));

        cold.bench_function("account_only", |b| {
            b.iter_custom(|iters| {
                let mut total = Duration::ZERO;
                let mut rng = LcgRng::with_seed(0xACC0_ACC0_ACC0_ACC0);
                for _ in 0..iters {
                    cold_cache::evict(cold_strategy, data_file);
                    let start = std::time::Instant::now();
                    Self::run_usdc_account_lookup_batch(
                        &handle.db,
                        holder_count,
                        COLD_LOOKUPS_PER_ITERATION,
                        &mut rng,
                    );
                    total += start.elapsed();
                }
                total
            });
        });

        cold.bench_function("balance_only", |b| {
            b.iter_custom(|iters| {
                let mut total = Duration::ZERO;
                let mut rng = LcgRng::with_seed(0xBA1A_BA1A_FACE_FACE);
                for _ in 0..iters {
                    cold_cache::evict(cold_strategy, data_file);
                    let start = std::time::Instant::now();
                    Self::run_usdc_balance_lookup_batch(
                        &handle.db,
                        holder_count,
                        COLD_LOOKUPS_PER_ITERATION,
                        &mut rng,
                    );
                    total += start.elapsed();
                }
                total
            });
        });

        cold.bench_function("account_plus_balance", |b| {
            b.iter_custom(|iters| {
                let mut total = Duration::ZERO;
                let mut rng = LcgRng::with_seed(0xFA17_BA1A_FA17_BA1A);
                for _ in 0..iters {
                    cold_cache::evict(cold_strategy, data_file);
                    let start = std::time::Instant::now();
                    Self::run_usdc_account_plus_balance_batch(
                        &handle.db,
                        holder_count,
                        COLD_LOOKUPS_PER_ITERATION,
                        &mut rng,
                    );
                    total += start.elapsed();
                }
                total
            });
        });

        cold.finish();
    }

    fn sample_plain_storage_keys<DB>(db: &DB, target: usize) -> Vec<PlainStorageLookupKey>
    where
        DB: Database,
    {
        let tx = db.tx().expect("mdbx tx should open");
        let mut cursor = tx
            .cursor_dup_read::<tables::PlainStorageState>()
            .expect("plain storage cursor should open");
        let mut keys = Vec::with_capacity(target);
        let mut seen = HashSet::with_capacity(target);
        let mut rng = LcgRng::with_seed(0xABCDEF0123456789);

        for _ in 0..target.saturating_mul(8) {
            if keys.len() >= target {
                break;
            }
            let probe_address = Self::address_from_index(rng.next_u64() as usize);
            if let Some((address, entry)) = cursor.seek(probe_address).expect("seek should succeed")
                && seen.insert((address, entry.key))
            {
                keys.push(PlainStorageLookupKey { address, slot: entry.key });
            }
        }

        if keys.len() < target {
            let mut walker = cursor.walk(None).expect("walk should succeed");
            while keys.len() < target {
                let Some((address, entry)) = walker.next().transpose().expect("walk step") else {
                    break;
                };
                if seen.insert((address, entry.key)) {
                    keys.push(PlainStorageLookupKey { address, slot: entry.key });
                }
            }
        }

        keys
    }

    fn sample_hashed_storage_keys<DB>(db: &DB, target: usize) -> Vec<HashedStorageLookupKey>
    where
        DB: Database,
    {
        let tx = db.tx().expect("mdbx tx should open");
        let mut cursor = tx
            .cursor_dup_read::<tables::HashedStorages>()
            .expect("hashed storage cursor should open");
        let mut keys = Vec::with_capacity(target);
        let mut seen = HashSet::with_capacity(target);
        let mut rng = LcgRng::with_seed(0xBCADFEA123456789);

        for _ in 0..target.saturating_mul(8) {
            if keys.len() >= target {
                break;
            }
            let probe_hashed_address = Self::b256_from_u64(rng.next_u64());
            if let Some((hashed_address, entry)) =
                cursor.seek(probe_hashed_address).expect("seek should succeed")
                && seen.insert((hashed_address, entry.key))
            {
                keys.push(HashedStorageLookupKey { hashed_address, slot: entry.key });
            }
        }

        if keys.len() < target {
            let mut walker = cursor.walk(None).expect("walk should succeed");
            while keys.len() < target {
                let Some((hashed_address, entry)) = walker.next().transpose().expect("walk step")
                else {
                    break;
                };
                if seen.insert((hashed_address, entry.key)) {
                    keys.push(HashedStorageLookupKey { hashed_address, slot: entry.key });
                }
            }
        }

        keys
    }

    fn run_plain_lookup_batch<DB>(
        db: &DB,
        all_keys: &[PlainStorageLookupKey],
        hot_keys: &[PlainStorageLookupKey],
        access_pattern: AccessPattern,
        lookups: usize,
    ) where
        DB: Database,
    {
        let tx = db.tx().expect("mdbx tx should open");
        let mut cursor = tx
            .cursor_dup_read::<tables::PlainStorageState>()
            .expect("plain storage cursor should open");
        let mut rng = LcgRng::with_seed(0xDEADBEEF12345678);

        for _ in 0..lookups {
            let key = Self::pick_plain_key(&mut rng, all_keys, hot_keys, access_pattern);
            let entry = cursor
                .seek_by_key_subkey(key.address, key.slot)
                .expect("lookup should not fail")
                .expect("lookup key should exist");
            black_box(entry);
        }
    }

    fn run_hashed_lookup_batch<DB>(
        db: &DB,
        all_keys: &[HashedStorageLookupKey],
        hot_keys: &[HashedStorageLookupKey],
        access_pattern: AccessPattern,
        lookups: usize,
    ) where
        DB: Database,
    {
        let tx = db.tx().expect("mdbx tx should open");
        let mut cursor = tx
            .cursor_dup_read::<tables::HashedStorages>()
            .expect("hashed storage cursor should open");
        let mut rng = LcgRng::with_seed(0xDEADBEEF12345678);

        for _ in 0..lookups {
            let key = Self::pick_hashed_key(&mut rng, all_keys, hot_keys, access_pattern);
            let entry = cursor
                .seek_by_key_subkey(key.hashed_address, key.slot)
                .expect("lookup should not fail")
                .expect("lookup key should exist");
            black_box(entry);
        }
    }

    fn pick_plain_key(
        rng: &mut LcgRng,
        all_keys: &[PlainStorageLookupKey],
        hot_keys: &[PlainStorageLookupKey],
        access_pattern: AccessPattern,
    ) -> PlainStorageLookupKey {
        match access_pattern {
            AccessPattern::WarmSingleKey => all_keys[0],
            AccessPattern::FullRandom => all_keys[rng.next_index(all_keys.len())],
            AccessPattern::Mixed { hot_percent } => {
                if rng.next_hot_pick(hot_percent) {
                    hot_keys[rng.next_index(hot_keys.len())]
                } else {
                    all_keys[rng.next_index(all_keys.len())]
                }
            }
        }
    }

    fn pick_hashed_key(
        rng: &mut LcgRng,
        all_keys: &[HashedStorageLookupKey],
        hot_keys: &[HashedStorageLookupKey],
        access_pattern: AccessPattern,
    ) -> HashedStorageLookupKey {
        match access_pattern {
            AccessPattern::WarmSingleKey => all_keys[0],
            AccessPattern::FullRandom => all_keys[rng.next_index(all_keys.len())],
            AccessPattern::Mixed { hot_percent } => {
                if rng.next_hot_pick(hot_percent) {
                    hot_keys[rng.next_index(hot_keys.len())]
                } else {
                    all_keys[rng.next_index(all_keys.len())]
                }
            }
        }
    }

    fn address_from_index(index: usize) -> Address {
        let mut bytes = [0_u8; 20];
        bytes[12..20].copy_from_slice(&(index as u64).to_be_bytes());
        Address::from(bytes)
    }

    fn b256_from_u64(value: u64) -> B256 {
        let mut bytes = [0_u8; 32];
        bytes[24..32].copy_from_slice(&value.to_be_bytes());
        B256::from(bytes)
    }

    fn data_source() -> DataSource {
        env::var_os(MDBX_PATH_ENV).map_or(DataSource::SeededEphemeralDb, |path| {
            DataSource::ExistingDb(PathBuf::from(path))
        })
    }

    fn lookup_key_target() -> usize {
        env::var(LOOKUP_KEY_COUNT_ENV).map_or(DEFAULT_SAMPLED_KEYS, |raw| {
            raw.parse::<usize>().ok().filter(|value| *value > 0).unwrap_or(DEFAULT_SAMPLED_KEYS)
        })
    }
}

fn mdbx_state_lookup(c: &mut Criterion) {
    MdbxStateLookupBenchmark::run(c);
}

criterion_group!(benches, mdbx_state_lookup);
criterion_main!(benches);
