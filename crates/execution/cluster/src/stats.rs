//! Counts of this node's remote reads, and their periodic log line.

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

    /// The reads counted so far.
    pub fn counts(&self) -> RemoteReadCounts {
        RemoteReadCounts {
            gets: self.gets.load(Ordering::Relaxed),
            seeks: self.seeks.load(Ordering::Relaxed),
            duplicate_seeks: self.duplicate_seeks.load(Ordering::Relaxed),
            failures: self.failures.load(Ordering::Relaxed),
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
                    elapsed = ?new.elapsed,
                    "cluster remote reads"
                );
            }
            logged = counts;
        }
    }
}
