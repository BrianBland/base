//! Which contracts hold the most storage rows: the input for choosing `split_storage`.

use std::{borrow::Cow, cmp::Reverse, collections::BinaryHeap, fmt, path::Path};

use alloy_primitives::{B256, Bytes};
use reth_db::{
    DatabaseEnv, DatabaseEnvKind,
    mdbx::{DatabaseArguments, MaxReadTransactionDuration},
    table::Table,
    tables,
};
use tracing::info;

use crate::{Bootstrap, BootstrapError};

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

    /// Walks `HashedStorages` of the database at `db` once, read-only, and keeps the `top`
    /// contracts with the most rows. Takes no lock: on a running node the read transaction holds
    /// back MDBX page reuse until the walk ends.
    pub fn scan(db: &Path, top: usize) -> Result<Self, BootstrapError> {
        let arguments = DatabaseArguments::default()
            .with_max_read_transaction_duration(Some(MaxReadTransactionDuration::Unbounded));
        let env = DatabaseEnv::open(db, DatabaseEnvKind::RO, arguments)?;
        let txn = env.begin_ro_txn()?;
        let mut cursor = txn.cursor(txn.open_db(Some(tables::HashedStorages::NAME))?.dbi())?;
        let mut counter = StorageCounter::new(top);
        let mut row = cursor.first::<Cow<'_, [u8]>, ()>()?;
        while let Some((key, ())) = row {
            counter.count(StorageCounter::address(&key)?);
            if counter.total_rows.is_multiple_of(Bootstrap::PROGRESS_ROWS) {
                info!(rows = counter.total_rows, "counting storage rows");
            }
            row = cursor.next()?;
        }
        Ok(counter.finish())
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

    /// Counts one row of `address`. Rows must arrive in table order, so that each key's rows
    /// are consecutive.
    pub fn count(&mut self, address: B256) {
        self.total_rows += 1;
        match &mut self.current {
            Some(current) if current.hashed_address == address => current.rows += 1,
            _ => {
                self.keep_current();
                self.current = Some(ContractRows { hashed_address: address, rows: 1 });
            }
        }
    }

    fn keep_current(&mut self) {
        let Some(entry) = self.current.take() else { return };
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
