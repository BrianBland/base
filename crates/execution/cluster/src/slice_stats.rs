//! Counts of this node's trie-slice computations and skipped message-passer checks, and their
//! periodic log line.

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

/// Trie-slice work counted since the node started.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TrieSliceCounts {
    /// Blocks whose trie slice was computed (blocks with held changes).
    pub blocks: u64,
    /// Side computations of split contracts whose account shard this node does not hold.
    pub side_calls: u64,
    /// Total time spent computing trie slices.
    pub elapsed: Duration,
    /// Total time spent capturing subtree hashes for the state-root check, outside `elapsed`.
    pub capture_elapsed: Duration,
    /// Message-passer storage-root comparisons skipped because this node does not hold that
    /// contract's whole storage trie.
    pub message_passer_skips: u64,
}

impl TrieSliceCounts {
    /// The work counted since `earlier`.
    pub const fn since(&self, earlier: &Self) -> Self {
        Self {
            blocks: self.blocks - earlier.blocks,
            side_calls: self.side_calls - earlier.side_calls,
            elapsed: self.elapsed.saturating_sub(earlier.elapsed),
            capture_elapsed: self.capture_elapsed.saturating_sub(earlier.capture_elapsed),
            message_passer_skips: self.message_passer_skips - earlier.message_passer_skips,
        }
    }

    /// Whether anything was counted.
    pub const fn is_empty(&self) -> bool {
        self.blocks == 0 && self.message_passer_skips == 0 && self.capture_elapsed.is_zero()
    }
}

/// Lock-free counters of this node's trie-slice work, also exported as the metrics
/// `base_cluster_trie_slice_blocks`, `base_cluster_trie_slice_side_calls` and
/// `base_cluster_message_passer_root_skips`.
#[derive(Debug)]
pub struct TrieSliceStats {
    blocks: AtomicU64,
    side_calls: AtomicU64,
    elapsed_nanos: AtomicU64,
    capture_elapsed_nanos: AtomicU64,
    message_passer_skips: AtomicU64,
    blocks_metric: Counter,
    side_calls_metric: Counter,
    message_passer_skips_metric: Counter,
}

impl Default for TrieSliceStats {
    fn default() -> Self {
        Self {
            blocks: AtomicU64::new(0),
            side_calls: AtomicU64::new(0),
            elapsed_nanos: AtomicU64::new(0),
            capture_elapsed_nanos: AtomicU64::new(0),
            message_passer_skips: AtomicU64::new(0),
            blocks_metric: metrics::counter!("base_cluster_trie_slice_blocks"),
            side_calls_metric: metrics::counter!("base_cluster_trie_slice_side_calls"),
            message_passer_skips_metric: metrics::counter!(
                "base_cluster_message_passer_root_skips"
            ),
        }
    }
}

impl TrieSliceStats {
    /// Interval of the trie-slice log line.
    pub const LOG_INTERVAL: Duration = Duration::from_secs(10);

    /// Counts one block's trie slice, computed with `side_calls` side computations in `elapsed`.
    pub fn record_block(&self, side_calls: u64, elapsed: Duration) {
        self.blocks.fetch_add(1, Ordering::Relaxed);
        self.side_calls.fetch_add(side_calls, Ordering::Relaxed);
        self.elapsed_nanos.fetch_add(Self::nanos(elapsed), Ordering::Relaxed);
        self.blocks_metric.increment(1);
        self.side_calls_metric.increment(side_calls);
    }

    /// Counts one block's subtree-hash capture, which took `elapsed`.
    pub fn record_capture(&self, elapsed: Duration) {
        self.capture_elapsed_nanos.fetch_add(Self::nanos(elapsed), Ordering::Relaxed);
    }

    fn nanos(elapsed: Duration) -> u64 {
        u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX)
    }

    /// Counts one skipped message-passer storage-root comparison.
    pub fn record_message_passer_skip(&self) {
        self.message_passer_skips.fetch_add(1, Ordering::Relaxed);
        self.message_passer_skips_metric.increment(1);
    }

    /// The work counted so far.
    pub fn counts(&self) -> TrieSliceCounts {
        TrieSliceCounts {
            blocks: self.blocks.load(Ordering::Relaxed),
            side_calls: self.side_calls.load(Ordering::Relaxed),
            elapsed: Duration::from_nanos(self.elapsed_nanos.load(Ordering::Relaxed)),
            capture_elapsed: Duration::from_nanos(
                self.capture_elapsed_nanos.load(Ordering::Relaxed),
            ),
            message_passer_skips: self.message_passer_skips.load(Ordering::Relaxed),
        }
    }

    /// Logs the work of each `interval` in which any was counted, on a background thread that
    /// exits at the first interval after the stats are dropped.
    pub fn spawn_logger(self: &Arc<Self>, interval: Duration) -> io::Result<JoinHandle<()>> {
        let stats = Arc::downgrade(self);
        std::thread::Builder::new()
            .name("cluster-trie-stats".into())
            .spawn(move || Self::log_every(&stats, interval))
    }

    fn log_every(stats: &Weak<Self>, interval: Duration) {
        let mut logged = TrieSliceCounts::default();
        loop {
            std::thread::sleep(interval);
            let Some(stats) = stats.upgrade() else { return };
            let counts = stats.counts();
            let new = counts.since(&logged);
            if !new.is_empty() {
                info!(
                    blocks = new.blocks,
                    side_calls = new.side_calls,
                    elapsed = ?new.elapsed,
                    capture_elapsed = ?new.capture_elapsed,
                    message_passer_skips = new.message_passer_skips,
                    "cluster trie slices"
                );
            }
            logged = counts;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counts_accumulate_and_subtract() {
        let stats = TrieSliceStats::default();
        stats.record_block(2, Duration::from_millis(5));
        let earlier = stats.counts();
        stats.record_block(1, Duration::from_millis(3));
        stats.record_capture(Duration::from_millis(2));
        stats.record_message_passer_skip();

        let counts = stats.counts();
        assert_eq!(
            counts,
            TrieSliceCounts {
                blocks: 2,
                side_calls: 3,
                elapsed: Duration::from_millis(8),
                capture_elapsed: Duration::from_millis(2),
                message_passer_skips: 1,
            }
        );
        assert_eq!(
            counts.since(&earlier),
            TrieSliceCounts {
                blocks: 1,
                side_calls: 1,
                elapsed: Duration::from_millis(3),
                capture_elapsed: Duration::from_millis(2),
                message_passer_skips: 1,
            }
        );
        assert!(TrieSliceCounts::default().is_empty());
    }
}
