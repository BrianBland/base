//! Counts of this node's remote reads, owner answers and apply-mode outcomes, and their periodic
//! log lines.

use std::{
    io,
    sync::{
        Arc, Weak,
        atomic::{AtomicU64, Ordering},
    },
    thread::JoinHandle,
    time::Duration,
};

use metrics::Counter;
use tracing::info;

use crate::ServableRange;

/// The query a remote read sent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RemoteReadKind {
    /// `AccountValue`.
    Account,
    /// `StorageValue`.
    Storage,
    /// `HasStorage`.
    StorageProbe,
    /// `Get` of a bytecode.
    Bytecode,
}

/// Remote reads counted since the node started.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RemoteReadCounts {
    /// `AccountValue` reads.
    pub accounts: u64,
    /// `StorageValue` reads.
    pub storages: u64,
    /// `HasStorage` reads.
    pub storage_probes: u64,
    /// Bytecode reads.
    pub bytecodes: u64,
    /// Reads of any kind that failed after every owner was tried.
    pub failures: u64,
    /// Reads that waited longer than [`ClusterClient::SLOW_WAIT`](crate::ClusterClient::SLOW_WAIT)
    /// on owners behind the requested block, whether or not they then succeeded.
    pub waits: u64,
    /// Total time spent in remote reads, retries included.
    pub elapsed: Duration,
}

impl RemoteReadCounts {
    /// Reads of every kind.
    pub const fn total(&self) -> u64 {
        self.accounts + self.storages + self.storage_probes + self.bytecodes
    }

    /// The reads counted since `earlier`.
    pub const fn since(&self, earlier: &Self) -> Self {
        Self {
            accounts: self.accounts - earlier.accounts,
            storages: self.storages - earlier.storages,
            storage_probes: self.storage_probes - earlier.storage_probes,
            bytecodes: self.bytecodes - earlier.bytecodes,
            failures: self.failures - earlier.failures,
            waits: self.waits - earlier.waits,
            elapsed: self.elapsed.saturating_sub(earlier.elapsed),
        }
    }
}

/// Lock-free counters of this node's remote reads, also exported as the metrics
/// `base_cluster_remote_reads` and `base_cluster_remote_read_failures`.
#[derive(Debug)]
pub struct RemoteReadStats {
    accounts: AtomicU64,
    storages: AtomicU64,
    storage_probes: AtomicU64,
    bytecodes: AtomicU64,
    failures: AtomicU64,
    waits: AtomicU64,
    elapsed_nanos: AtomicU64,
    reads_metric: Counter,
    failures_metric: Counter,
}

impl Default for RemoteReadStats {
    fn default() -> Self {
        Self {
            accounts: AtomicU64::new(0),
            storages: AtomicU64::new(0),
            storage_probes: AtomicU64::new(0),
            bytecodes: AtomicU64::new(0),
            failures: AtomicU64::new(0),
            waits: AtomicU64::new(0),
            elapsed_nanos: AtomicU64::new(0),
            reads_metric: metrics::counter!("base_cluster_remote_reads"),
            failures_metric: metrics::counter!("base_cluster_remote_read_failures"),
        }
    }
}

impl RemoteReadStats {
    /// Interval of the remote-read log line.
    pub const LOG_INTERVAL: Duration = Duration::from_secs(10);

    /// Counts one remote read of `kind` that took `elapsed`, and a failure when it `failed`.
    pub fn record(&self, kind: RemoteReadKind, elapsed: Duration, failed: bool) {
        let counter = match kind {
            RemoteReadKind::Account => &self.accounts,
            RemoteReadKind::Storage => &self.storages,
            RemoteReadKind::StorageProbe => &self.storage_probes,
            RemoteReadKind::Bytecode => &self.bytecodes,
        };
        counter.fetch_add(1, Ordering::Relaxed);
        let nanos = u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX);
        self.elapsed_nanos.fetch_add(nanos, Ordering::Relaxed);
        self.reads_metric.increment(1);
        if failed {
            self.failures.fetch_add(1, Ordering::Relaxed);
            self.failures_metric.increment(1);
        }
    }

    /// Counts one read that waited on owners behind the requested block.
    pub fn record_wait(&self) {
        self.waits.fetch_add(1, Ordering::Relaxed);
    }

    /// The reads counted so far.
    pub fn counts(&self) -> RemoteReadCounts {
        RemoteReadCounts {
            accounts: self.accounts.load(Ordering::Relaxed),
            storages: self.storages.load(Ordering::Relaxed),
            storage_probes: self.storage_probes.load(Ordering::Relaxed),
            bytecodes: self.bytecodes.load(Ordering::Relaxed),
            failures: self.failures.load(Ordering::Relaxed),
            waits: self.waits.load(Ordering::Relaxed),
            elapsed: Duration::from_nanos(self.elapsed_nanos.load(Ordering::Relaxed)),
        }
    }

    /// Logs the reads of each `interval` in which any happened, on a background thread that
    /// exits at the first interval after the stats are dropped.
    pub fn spawn_logger(self: &Arc<Self>, interval: Duration) -> io::Result<JoinHandle<()>> {
        let stats = Arc::downgrade(self);
        std::thread::Builder::new()
            .name("cluster-read-stats".into())
            .spawn(move || Self::log_every(&stats, interval))
    }

    fn log_every(stats: &Weak<Self>, interval: Duration) {
        let mut logged = RemoteReadCounts::default();
        loop {
            std::thread::sleep(interval);
            let Some(stats) = stats.upgrade() else { return };
            let counts = stats.counts();
            let new = counts.since(&logged);
            if new.total() > 0 {
                info!(
                    accounts = new.accounts,
                    storages = new.storages,
                    storage_probes = new.storage_probes,
                    bytecodes = new.bytecodes,
                    failures = new.failures,
                    waits = new.waits,
                    elapsed = ?new.elapsed,
                    "cluster remote reads"
                );
            }
            logged = counts;
        }
    }
}

/// How an owner answered one state read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OwnerAnswer {
    /// Answered with a value.
    Served,
    /// Refused because the owner has not reached the requested block.
    Behind,
    /// Refused for any other reason.
    Refused,
}

/// An owner's answers counted since the node started, and its history at the last change.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct OwnerCounts {
    /// Reads answered with a value.
    pub served: u64,
    /// Reads refused because the owner was behind.
    pub behind: u64,
    /// Reads refused for any other reason.
    pub refused: u64,
    /// Entries of the history index.
    pub entries: u64,
    /// Lowest servable block, or `None` when nothing is servable.
    pub lowest: Option<u64>,
    /// Highest servable block, or `None` when nothing is servable.
    pub highest: Option<u64>,
}

/// Lock-free counters of an owner's answers and a snapshot of its history, for the periodic
/// `cluster owner` log line.
#[derive(Debug, Default)]
pub struct OwnerStats {
    served: AtomicU64,
    behind: AtomicU64,
    refused: AtomicU64,
    entries: AtomicU64,
    /// Lowest and highest servable block plus one; 0 means none.
    lowest: AtomicU64,
    highest: AtomicU64,
}

impl OwnerStats {
    /// Counts one answer.
    pub fn record(&self, answer: OwnerAnswer) {
        let counter = match answer {
            OwnerAnswer::Served => &self.served,
            OwnerAnswer::Behind => &self.behind,
            OwnerAnswer::Refused => &self.refused,
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }

    /// Records the history's entry count and servable range.
    pub fn record_history(&self, entries: usize, servable: Option<ServableRange>) {
        let plus_one = |block: Option<u64>| block.map_or(0, |block| block.saturating_add(1));
        self.entries.store(entries as u64, Ordering::Relaxed);
        self.lowest.store(plus_one(servable.map(|range| range.lowest)), Ordering::Relaxed);
        self.highest.store(plus_one(servable.map(|range| range.highest)), Ordering::Relaxed);
    }

    /// The answers counted so far and the history last recorded.
    pub fn counts(&self) -> OwnerCounts {
        let block = |plus_one: u64| plus_one.checked_sub(1);
        OwnerCounts {
            served: self.served.load(Ordering::Relaxed),
            behind: self.behind.load(Ordering::Relaxed),
            refused: self.refused.load(Ordering::Relaxed),
            entries: self.entries.load(Ordering::Relaxed),
            lowest: block(self.lowest.load(Ordering::Relaxed)),
            highest: block(self.highest.load(Ordering::Relaxed)),
        }
    }

    /// Logs the answers of each `interval` and the history, on a background thread that exits
    /// at the first interval after the stats are dropped.
    pub fn spawn_logger(self: &Arc<Self>, interval: Duration) -> io::Result<JoinHandle<()>> {
        let stats = Arc::downgrade(self);
        std::thread::Builder::new()
            .name("cluster-owner-stats".into())
            .spawn(move || Self::log_every(&stats, interval))
    }

    fn log_every(stats: &Weak<Self>, interval: Duration) {
        let mut logged = OwnerCounts::default();
        loop {
            std::thread::sleep(interval);
            let Some(stats) = stats.upgrade() else { return };
            let counts = stats.counts();
            info!(
                served = counts.served - logged.served,
                behind = counts.behind - logged.behind,
                refused = counts.refused - logged.refused,
                entries = counts.entries,
                lowest = ?counts.lowest,
                highest = ?counts.highest,
                "cluster owner"
            );
            logged = counts;
        }
    }
}

/// Why an apply node executed a new payload instead of applying a peer's result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApplyFallback {
    /// No peer had the result within the fetch budget.
    Unavailable,
    /// No peer could be reached.
    Unreachable,
    /// A peer's result did not decode or failed a post-execution check.
    Invalid,
    /// The payload could not be applied: it failed a pre-execution check, carries a block access
    /// list, or its parent's header or state is not available.
    Ineligible,
}

impl ApplyFallback {
    /// Every reason, in the order [`ApplyCounts::fallbacks`] counts them.
    pub const ALL: [Self; 4] =
        [Self::Unavailable, Self::Unreachable, Self::Invalid, Self::Ineligible];

    /// The reason's metric label.
    pub const fn label(self) -> &'static str {
        match self {
            Self::Unavailable => "unavailable",
            Self::Unreachable => "unreachable",
            Self::Invalid => "invalid",
            Self::Ineligible => "ineligible",
        }
    }

    const fn index(self) -> usize {
        self as usize
    }
}

/// Apply-mode outcomes counted since the node started.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ApplyCounts {
    /// Blocks inserted from a peer's result.
    pub applied: u64,
    /// Blocks executed instead, indexed like [`ApplyFallback::ALL`].
    pub fallbacks: [u64; 4],
    /// Total time spent fetching results, whether or not one arrived.
    pub fetch_elapsed: Duration,
}

impl ApplyCounts {
    /// Fallbacks for `reason`.
    pub const fn fallbacks(&self, reason: ApplyFallback) -> u64 {
        self.fallbacks[reason.index()]
    }

    /// Payloads counted, applied or not.
    pub fn total(&self) -> u64 {
        self.applied + self.fallbacks.iter().sum::<u64>()
    }

    /// The outcomes counted since `earlier`.
    pub fn since(&self, earlier: &Self) -> Self {
        Self {
            applied: self.applied - earlier.applied,
            fallbacks: std::array::from_fn(|index| {
                self.fallbacks[index] - earlier.fallbacks[index]
            }),
            fetch_elapsed: self.fetch_elapsed.saturating_sub(earlier.fetch_elapsed),
        }
    }
}

/// Lock-free counters of this node's apply-mode outcomes, also exported as the metrics
/// `base_cluster_applied_blocks` and `base_cluster_apply_fallbacks{reason}`.
#[derive(Debug)]
pub struct ApplyStats {
    applied: AtomicU64,
    fallbacks: [AtomicU64; 4],
    fetch_nanos: AtomicU64,
    applied_metric: Counter,
    fallback_metrics: [Counter; 4],
}

impl Default for ApplyStats {
    fn default() -> Self {
        Self {
            applied: AtomicU64::new(0),
            fallbacks: Default::default(),
            fetch_nanos: AtomicU64::new(0),
            applied_metric: metrics::counter!("base_cluster_applied_blocks"),
            fallback_metrics: ApplyFallback::ALL.map(|reason| {
                metrics::counter!("base_cluster_apply_fallbacks", "reason" => reason.label())
            }),
        }
    }
}

impl ApplyStats {
    /// Counts time spent fetching a result.
    pub fn record_fetch(&self, elapsed: Duration) {
        let nanos = u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX);
        self.fetch_nanos.fetch_add(nanos, Ordering::Relaxed);
    }

    /// Counts a block inserted from a peer's result.
    pub fn record_applied(&self) {
        self.applied.fetch_add(1, Ordering::Relaxed);
        self.applied_metric.increment(1);
    }

    /// Counts a block executed instead of applied, for `reason`.
    pub fn record_fallback(&self, reason: ApplyFallback) {
        self.fallbacks[reason.index()].fetch_add(1, Ordering::Relaxed);
        self.fallback_metrics[reason.index()].increment(1);
    }

    /// The outcomes counted so far.
    pub fn counts(&self) -> ApplyCounts {
        ApplyCounts {
            applied: self.applied.load(Ordering::Relaxed),
            fallbacks: std::array::from_fn(|index| self.fallbacks[index].load(Ordering::Relaxed)),
            fetch_elapsed: Duration::from_nanos(self.fetch_nanos.load(Ordering::Relaxed)),
        }
    }

    /// Logs the outcomes of each `interval` in which any payload was counted, on a background
    /// thread that exits at the first interval after the stats are dropped.
    pub fn spawn_logger(self: &Arc<Self>, interval: Duration) -> io::Result<JoinHandle<()>> {
        let stats = Arc::downgrade(self);
        std::thread::Builder::new()
            .name("cluster-apply-stats".into())
            .spawn(move || Self::log_every(&stats, interval))
    }

    fn log_every(stats: &Weak<Self>, interval: Duration) {
        let mut logged = ApplyCounts::default();
        loop {
            std::thread::sleep(interval);
            let Some(stats) = stats.upgrade() else { return };
            let counts = stats.counts();
            let new = counts.since(&logged);
            if new.total() > 0 {
                info!(
                    applied = new.applied,
                    unavailable = new.fallbacks(ApplyFallback::Unavailable),
                    unreachable = new.fallbacks(ApplyFallback::Unreachable),
                    invalid = new.fallbacks(ApplyFallback::Invalid),
                    ineligible = new.fallbacks(ApplyFallback::Ineligible),
                    fetch_elapsed = ?new.fetch_elapsed,
                    "cluster apply"
                );
            }
            logged = counts;
        }
    }
}
