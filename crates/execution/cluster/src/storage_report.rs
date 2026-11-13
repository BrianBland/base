//! Which contracts hold the most storage rows: the input for choosing `split_storage`.

use std::{
    borrow::Cow,
    cmp::Reverse,
    collections::{BinaryHeap, HashMap},
    fmt,
    panic::{self, AssertUnwindSafe},
    path::Path,
    sync::atomic::{AtomicU64, Ordering},
    thread,
};

use alloy_primitives::{B256, Bytes};
use parking_lot::Mutex;
use reth_db::{
    DatabaseEnv, DatabaseEnvKind,
    mdbx::{Cursor, DatabaseArguments, MaxReadTransactionDuration, RO},
    table::Table,
    tables,
};
use tracing::info;

use crate::{
    Bootstrap, BootstrapError, ReadParallelism, TablePartitions, TablePosition, TableRange,
    WorkQueue,
};

/// One contract's number of `HashedStorages` rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ContractRows {
    /// The contract's hashed address, the table's key.
    pub hashed_address: B256,
    /// Its storage rows (duplicates of the key).
    pub rows: u64,
}

/// The contracts with the most `HashedStorages` rows, most first (ties by ascending hashed
/// address), and the table's total.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageReport {
    /// Rows of the whole table.
    pub total_rows: u64,
    /// The contracts with the most rows, most first.
    pub top: Vec<ContractRows>,
}

impl StorageReport {
    /// Contracts listed when the operator does not ask for a number.
    pub const DEFAULT_TOP: usize = 20;
    /// Smallest share of the table, in percent, at which a contract is suggested for
    /// `split_storage`.
    pub const SPLIT_SHARE_PERCENT: u64 = 1;

    /// Rows of one contract at which a reader hands the contract's remaining values to the other
    /// readers, so that no single thread walks a giant contract.
    pub const HAND_OFF_ROWS: u64 = 1_000_000;

    /// Walks `HashedStorages` of the database at `db` once, read-only, with
    /// [`ReadParallelism::DEFAULT`], and keeps the `top` contracts with the most rows. Takes no
    /// lock: on a running node the read transactions hold back MDBX page reuse until the walk
    /// ends.
    pub fn scan(db: &Path, top: usize) -> Result<Self, BootstrapError> {
        Self::scan_with(db, top, ReadParallelism::DEFAULT, Self::HAND_OFF_ROWS)
    }

    /// [`Self::scan`] with `read` and a hand-off at `hand_off_rows` rows of one contract. The
    /// table's partitions are counted by a pool of threads, each with its own read transaction;
    /// a thread counting a contract that reaches `hand_off_rows` rows hands the contract's values
    /// with a greater first byte to the pool, one range per byte, and the pieces are summed. The
    /// report is the same whatever `read` and `hand_off_rows`.
    pub fn scan_with(
        db: &Path,
        top: usize,
        read: ReadParallelism,
        hand_off_rows: u64,
    ) -> Result<Self, BootstrapError> {
        let arguments = DatabaseArguments::default()
            .with_max_read_transaction_duration(Some(MaxReadTransactionDuration::Unbounded));
        let env = DatabaseEnv::open(db, DatabaseEnvKind::RO, arguments)?;
        let partitions = {
            let txn = env.begin_ro_txn()?;
            let dbi = txn.open_db(Some(tables::HashedStorages::NAME))?.dbi();
            TablePartitions::plan(&txn, dbi, &[], read.partition_bytes)?
        };
        let queue = WorkQueue::new(
            (0..partitions.len()).map(|index| CountRange::Keys(partitions.range(index))),
        );
        let handed_off = Mutex::new(HashMap::new());
        let rows = AtomicU64::new(0);
        let scan = StorageScan {
            env: &env,
            top,
            hand_off_rows,
            queue: &queue,
            handed_off: &handed_off,
            rows: &rows,
        };
        let counters = thread::scope(|scope| {
            let threads: Vec<_> = (0..read.threads.get())
                .map(|_| {
                    scope.spawn(|| {
                        let counted = panic::catch_unwind(AssertUnwindSafe(|| scan.count()));
                        if !matches!(counted, Ok(Ok(_))) {
                            queue.stop();
                        }
                        counted.unwrap_or_else(|panic| panic::resume_unwind(panic))
                    })
                })
                .collect();
            threads
                .into_iter()
                .map(|thread| thread.join().unwrap_or_else(|panic| panic::resume_unwind(panic)))
                .collect::<Result<Vec<_>, _>>()
        })?;
        let mut report = StorageCounter::new(top);
        for counter in counters {
            report.merge(counter.finish());
        }
        for (hashed_address, rows) in handed_off.into_inner() {
            report.add(ContractRows { hashed_address, rows });
        }
        Ok(report.finish())
    }

    /// Share of the table held by `entry`, in percent.
    pub fn share_percent(&self, entry: &ContractRows) -> f64 {
        if self.total_rows == 0 {
            return 0.0;
        }
        entry.rows as f64 * 100.0 / self.total_rows as f64
    }

    /// The listed contracts holding at least [`Self::SPLIT_SHARE_PERCENT`] of the rows.
    pub fn split_candidates(&self) -> Vec<B256> {
        self.top
            .iter()
            .filter(|entry| entry.rows * 100 >= self.total_rows * Self::SPLIT_SHARE_PERCENT)
            .map(|entry| entry.hashed_address)
            .collect()
    }
}

/// One line per listed contract, the total, and last a `split_storage` line ready to paste into
/// the cluster config.
impl fmt::Display for StorageReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "contracts with the most HashedStorages rows (hashed address, rows, share):")?;
        for entry in &self.top {
            writeln!(
                f,
                "{} {:>14} {:>7.3}%",
                entry.hashed_address,
                entry.rows,
                self.share_percent(entry)
            )?;
        }
        writeln!(f, "HashedStorages total rows: {}", self.total_rows)?;
        let candidates: Vec<_> =
            self.split_candidates().iter().map(|address| format!("\"{address}\"")).collect();
        write!(f, "split_storage = [{}]", candidates.join(", "))
    }
}

/// Counts the rows of each key of an ordered walk of `HashedStorages`, keeping only the `top`
/// keys with the most rows in memory.
#[derive(Debug)]
pub struct StorageCounter {
    top: usize,
    total_rows: u64,
    current: Option<ContractRows>,
    /// The kept contracts, least rows (then greatest address) at the top of the heap.
    kept: BinaryHeap<Reverse<(u64, Reverse<B256>)>>,
}

impl StorageCounter {
    /// A counter keeping the `top` contracts with the most rows.
    pub fn new(top: usize) -> Self {
        Self { top, total_rows: 0, current: None, kept: BinaryHeap::with_capacity(top + 1) }
    }

    /// The hashed address an encoded `HashedStorages` key holds; an error when the key is not
    /// 32 bytes.
    pub fn address(key: &[u8]) -> Result<B256, BootstrapError> {
        B256::try_from(key).map_err(|_| BootstrapError::InvalidRow {
            table: tables::HashedStorages::NAME,
            key: Bytes::copy_from_slice(key),
        })
    }

    /// Counts one row of `address` and returns the rows of `address` counted so far. Rows must
    /// arrive in table order, so that each key's rows are consecutive.
    pub fn count(&mut self, address: B256) -> u64 {
        self.total_rows += 1;
        match &mut self.current {
            Some(current) if current.hashed_address == address => current.rows += 1,
            _ => {
                self.keep_current();
                self.current = Some(ContractRows { hashed_address: address, rows: 1 });
            }
        }
        self.current.map_or(0, |current| current.rows)
    }

    /// Removes the contract being counted, and its rows, from the counts, and returns it.
    pub fn withdraw_current(&mut self) -> Option<ContractRows> {
        let current = self.current.take()?;
        self.total_rows -= current.rows;
        Some(current)
    }

    /// Counts `entry`, a contract counted elsewhere, with all its rows.
    pub fn add(&mut self, entry: ContractRows) {
        self.total_rows += entry.rows;
        self.keep(entry);
    }

    /// Counts the rows and contracts of `report`, made from rows not counted here.
    pub fn merge(&mut self, report: StorageReport) {
        self.total_rows += report.total_rows;
        for entry in report.top {
            self.keep(entry);
        }
    }

    fn keep_current(&mut self) {
        if let Some(entry) = self.current.take() {
            self.keep(entry);
        }
    }

    fn keep(&mut self, entry: ContractRows) {
        self.kept.push(Reverse((entry.rows, Reverse(entry.hashed_address))));
        if self.kept.len() > self.top {
            self.kept.pop();
        }
    }

    /// The report of every row counted.
    pub fn finish(mut self) -> StorageReport {
        self.keep_current();
        let top = self
            .kept
            .into_sorted_vec()
            .into_iter()
            .map(|Reverse((rows, Reverse(hashed_address)))| ContractRows { hashed_address, rows })
            .collect();
        StorageReport { total_rows: self.total_rows, top }
    }
}

/// A range of `HashedStorages` for a storage scan's thread to count.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CountRange {
    /// Whole contracts, counted one by one; a contract reaching the hand-off threshold is handed
    /// off.
    Keys(TableRange),
    /// Part of the values of a handed-off contract.
    Values {
        /// The contract.
        hashed_address: B256,
        /// Its values to count.
        range: TableRange,
    },
}

/// What the threads of one storage scan share.
#[derive(Debug, Clone, Copy)]
pub struct StorageScan<'a> {
    env: &'a DatabaseEnv,
    top: usize,
    hand_off_rows: u64,
    queue: &'a WorkQueue<CountRange>,
    /// The rows of each handed-off contract counted so far.
    handed_off: &'a Mutex<HashMap<B256, u64>>,
    /// Rows counted so far by every thread, for progress logs.
    rows: &'a AtomicU64,
}

impl StorageScan<'_> {
    /// Counts ranges from the queue, from one read transaction, until none is left or the queue
    /// stops; returns the counts of the whole contracts counted.
    fn count(&self) -> Result<StorageCounter, BootstrapError> {
        let txn = self.env.begin_ro_txn()?;
        let mut cursor = txn.cursor(txn.open_db(Some(tables::HashedStorages::NAME))?.dbi())?;
        let mut counter = StorageCounter::new(self.top);
        while let Some(work) = self.queue.next_item() {
            let rows = match &work.item {
                CountRange::Keys(range) => self.count_keys(&mut cursor, range, &mut counter)?,
                CountRange::Values { hashed_address, range } => {
                    let rows = self.count_values(&mut cursor, range)?;
                    self.record_handed_off(ContractRows { hashed_address: *hashed_address, rows });
                    rows
                }
            };
            let before = self.rows.fetch_add(rows, Ordering::Relaxed);
            if before / Bootstrap::PROGRESS_ROWS != (before + rows) / Bootstrap::PROGRESS_ROWS {
                info!(rows = before + rows, "counting storage rows");
            }
        }
        Ok(counter)
    }

    /// Counts the contracts of `range` into `counter` and returns the rows walked. A contract
    /// reaching the hand-off threshold at a value with first byte `b` is withdrawn from
    /// `counter`: its values with first bytes after `b` are handed to the queue, and this thread
    /// counts the rest of byte `b` before moving to the next contract.
    fn count_keys(
        &self,
        cursor: &mut Cursor<RO>,
        range: &TableRange,
        counter: &mut StorageCounter,
    ) -> Result<u64, BootstrapError> {
        let mut walked = 0;
        let mut handed_off: Option<(ContractRows, TablePosition)> = None;
        let mut row = range.start.seek(cursor)?;
        while let Some((key, value)) = row {
            if range.is_passed_by(&key, &value) || self.queue.is_stopped() {
                break;
            }
            if let Some((contract, own_end)) = &mut handed_off {
                if !own_end.is_reached_by(&key, &value) {
                    contract.rows += 1;
                    walked += 1;
                    row = cursor.next()?;
                    continue;
                }
                let past = TablePosition::after_key(contract.hashed_address.as_slice());
                self.record_handed_off(*contract);
                handed_off = None;
                if !past.is_reached_by(&key, &value) {
                    row = past.seek(cursor)?;
                    continue;
                }
            }
            let address = StorageCounter::address(&key)?;
            walked += 1;
            if counter.count(address) == self.hand_off_rows
                && let Some(contract) = counter.withdraw_current()
            {
                handed_off = Some((contract, self.hand_off(address, &value, range)));
            }
            row = cursor.next()?;
        }
        if let Some((contract, _)) = handed_off {
            self.record_handed_off(contract);
        }
        Ok(walked)
    }

    /// Hands the values of contract `address` whose first byte is greater than `value`'s to the
    /// queue, one range per first byte, within `range`; returns where the values left to this
    /// thread end.
    fn hand_off(&self, address: B256, value: &[u8], range: &TableRange) -> TablePosition {
        let key = address.as_slice();
        let next_byte = value.first().map_or(0, |byte| usize::from(*byte) + 1);
        let bounds: Vec<_> = (next_byte..=usize::from(u8::MAX))
            .map(|byte| TablePosition::duplicate(key, vec![byte as u8]))
            .chain(std::iter::once(TablePosition::after_key(key)))
            .collect();
        let pieces = bounds
            .windows(2)
            .map(|pair| {
                let end = match &range.end {
                    Some(limit) if *limit < pair[1] => limit.clone(),
                    _ => pair[1].clone(),
                };
                (pair[0].clone(), end)
            })
            .filter(|(start, end)| start < end)
            .map(|(start, end)| CountRange::Values {
                hashed_address: address,
                range: TableRange { start, end: Some(end) },
            })
            .collect();
        self.queue.push_front(pieces);
        bounds[0].clone()
    }

    fn record_handed_off(&self, contract: ContractRows) {
        *self.handed_off.lock().entry(contract.hashed_address).or_default() += contract.rows;
    }

    /// Counts the rows of `range`.
    fn count_values(
        &self,
        cursor: &mut Cursor<RO>,
        range: &TableRange,
    ) -> Result<u64, BootstrapError> {
        let mut rows = 0;
        let mut row = range.start.seek(cursor)?;
        while let Some((key, value)) = row {
            if range.is_passed_by(&key, &value) || self.queue.is_stopped() {
                break;
            }
            rows += 1;
            row = cursor.next::<Cow<'_, [u8]>, Cow<'_, [u8]>>()?;
        }
        Ok(rows)
    }
}
