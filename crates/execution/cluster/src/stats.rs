//! Counts of this node's remote reads and apply-mode outcomes, and their periodic log lines.

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

/// The query a remote read sent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RemoteReadKind {
    /// `Get`.
    Get,
    /// `SeekGe`.
    Seek,
    /// `SeekDupGe`.
    DuplicateSeek,
}

/// Remote reads counted since the node started.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RemoteReadCounts {
    /// `Get` reads.
    pub gets: u64,
    /// `SeekGe` reads.
    pub seeks: u64,
    /// `SeekDupGe` reads.
    pub duplicate_seeks: u64,
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
        self.gets + self.seeks + self.duplicate_seeks
    }

    /// The reads counted since `earlier`.
    pub const fn since(&self, earlier: &Self) -> Self {
        Self {
            gets: self.gets - earlier.gets,
            seeks: self.seeks - earlier.seeks,
            duplicate_seeks: self.duplicate_seeks - earlier.duplicate_seeks,
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
    gets: AtomicU64,
    seeks: AtomicU64,
    duplicate_seeks: AtomicU64,
    failures: AtomicU64,
    waits: AtomicU64,
    elapsed_nanos: AtomicU64,
    reads_metric: Counter,
    failures_metric: Counter,
}

impl Default for RemoteReadStats {
    fn default() -> Self {
        Self {
            gets: AtomicU64::new(0),
            seeks: AtomicU64::new(0),
            duplicate_seeks: AtomicU64::new(0),
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
            RemoteReadKind::Get => &self.gets,
            RemoteReadKind::Seek => &self.seeks,
            RemoteReadKind::DuplicateSeek => &self.duplicate_seeks,
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
            gets: self.gets.load(Ordering::Relaxed),
            seeks: self.seeks.load(Ordering::Relaxed),
            duplicate_seeks: self.duplicate_seeks.load(Ordering::Relaxed),
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
                    gets = new.gets,
                    seeks = new.seeks,
                    duplicate_seeks = new.duplicate_seeks,
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
