//! Criterion benchmark for MDBX state lookup latency calibration.

use std::{collections::HashSet, env, hint::black_box, path::PathBuf, time::Duration};

use alloy_primitives::{Address, B256, U256};
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use reth_db::{
    Database,
    cursor::{DbCursorRO, DbCursorRW, DbDupCursorRO},
    mdbx::DatabaseArguments,
    tables,
    test_utils::create_test_rw_db,
    transaction::{DbTx, DbTxMut},
};
use reth_primitives_traits::StorageEntry;

const MDBX_PATH_ENV: &str = "BASE_PREFETCH_MDBX_PATH";
const LOOKUP_KEY_COUNT_ENV: &str = "BASE_PREFETCH_LOOKUP_KEY_COUNT";

const TOTAL_ACCOUNTS: usize = 50_000;
const SLOTS_PER_ACCOUNT: usize = 4;
const HOT_KEYS: usize = 1_024;
const LOOKUPS_PER_ITERATION: usize = 20_000;
const DEFAULT_SAMPLED_KEYS: usize = 64_000;

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

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct MdbxStateLookupBenchmark;

impl MdbxStateLookupBenchmark {
    fn run(c: &mut Criterion) {
        let sampled_keys_target = Self::lookup_key_target();
        match Self::data_source() {
            DataSource::ExistingDb(path) => {
                let db = reth_db::open_db_read_only(&path, DatabaseArguments::default())
                    .unwrap_or_else(|error| {
                        panic!("failed to open MDBX DB at {}: {error}", path.as_path().display())
                    });

                let plain_keys = Self::sample_plain_storage_keys(&db, sampled_keys_target);
                if !plain_keys.is_empty() {
                    let hot_keys = plain_keys
                        .iter()
                        .copied()
                        .take(HOT_KEYS.min(plain_keys.len()))
                        .collect::<Vec<_>>();
                    Self::run_group_plain(c, &db, "existing_db_plain", &plain_keys, &hot_keys);
                    return;
                }

                let hashed_keys = Self::sample_hashed_storage_keys(&db, sampled_keys_target);
                assert!(
                    !hashed_keys.is_empty(),
                    "no PlainStorageState or HashedStorages rows found in provided MDBX database"
                );
                let hot_keys = hashed_keys
                    .iter()
                    .copied()
                    .take(HOT_KEYS.min(hashed_keys.len()))
                    .collect::<Vec<_>>();
                Self::run_group_hashed(c, &db, "existing_db_hashed", &hashed_keys, &hot_keys);
            }
            DataSource::SeededEphemeralDb => {
                let db = create_test_rw_db();
                let plain_keys =
                    Self::seed_plain_storage_state(&db, TOTAL_ACCOUNTS, SLOTS_PER_ACCOUNT);
                let hot_keys = plain_keys
                    .iter()
                    .copied()
                    .take(HOT_KEYS.min(plain_keys.len()))
                    .collect::<Vec<_>>();
                Self::run_group_plain(c, &db, "seeded_ephemeral", &plain_keys, &hot_keys);
            }
        }
    }

    fn run_group_plain<DB>(
        c: &mut Criterion,
        db: &DB,
        source_label: &str,
        keys: &[PlainStorageLookupKey],
        hot_keys: &[PlainStorageLookupKey],
    ) where
        DB: Database,
    {
        let mut group = c.benchmark_group(format!("mdbx_storage_lookup/{source_label}"));
        group.warm_up_time(Duration::from_secs(2));
        group.measurement_time(Duration::from_secs(8));
        group.sample_size(100);
        group.throughput(Throughput::Elements(LOOKUPS_PER_ITERATION as u64));

        group.bench_function("warm_single_key", |b| {
            b.iter(|| {
                Self::run_plain_lookup_batch(db, keys, hot_keys, AccessPattern::WarmSingleKey);
            });
        });
        group.bench_function("full_random", |b| {
            b.iter(|| {
                Self::run_plain_lookup_batch(db, keys, hot_keys, AccessPattern::FullRandom);
            });
        });
        group.bench_function("mixed_hot90", |b| {
            b.iter(|| {
                Self::run_plain_lookup_batch(
                    db,
                    keys,
                    hot_keys,
                    AccessPattern::Mixed { hot_percent: 90 },
                );
            });
        });
        group.bench_function("mixed_hot70", |b| {
            b.iter(|| {
                Self::run_plain_lookup_batch(
                    db,
                    keys,
                    hot_keys,
                    AccessPattern::Mixed { hot_percent: 70 },
                );
            });
        });
        group.finish();
    }

    fn run_group_hashed<DB>(
        c: &mut Criterion,
        db: &DB,
        source_label: &str,
        keys: &[HashedStorageLookupKey],
        hot_keys: &[HashedStorageLookupKey],
    ) where
        DB: Database,
    {
        let mut group = c.benchmark_group(format!("mdbx_storage_lookup/{source_label}"));
        group.warm_up_time(Duration::from_secs(2));
        group.measurement_time(Duration::from_secs(8));
        group.sample_size(100);
        group.throughput(Throughput::Elements(LOOKUPS_PER_ITERATION as u64));

        group.bench_function("warm_single_key", |b| {
            b.iter(|| {
                Self::run_hashed_lookup_batch(db, keys, hot_keys, AccessPattern::WarmSingleKey);
            });
        });
        group.bench_function("full_random", |b| {
            b.iter(|| {
                Self::run_hashed_lookup_batch(db, keys, hot_keys, AccessPattern::FullRandom);
            });
        });
        group.bench_function("mixed_hot90", |b| {
            b.iter(|| {
                Self::run_hashed_lookup_batch(
                    db,
                    keys,
                    hot_keys,
                    AccessPattern::Mixed { hot_percent: 90 },
                );
            });
        });
        group.bench_function("mixed_hot70", |b| {
            b.iter(|| {
                Self::run_hashed_lookup_batch(
                    db,
                    keys,
                    hot_keys,
                    AccessPattern::Mixed { hot_percent: 70 },
                );
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
    ) where
        DB: Database,
    {
        let tx = db.tx().expect("mdbx tx should open");
        let mut cursor = tx
            .cursor_dup_read::<tables::PlainStorageState>()
            .expect("plain storage cursor should open");
        let mut rng = LcgRng::with_seed(0xDEADBEEF12345678);

        for _ in 0..LOOKUPS_PER_ITERATION {
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
    ) where
        DB: Database,
    {
        let tx = db.tx().expect("mdbx tx should open");
        let mut cursor = tx
            .cursor_dup_read::<tables::HashedStorages>()
            .expect("hashed storage cursor should open");
        let mut rng = LcgRng::with_seed(0xDEADBEEF12345678);

        for _ in 0..LOOKUPS_PER_ITERATION {
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
