//! Scheduling of the state-root check over recent blocks, its counters, and their periodic log
//! line.

use std::{
    io,
    sync::{
        Arc, Weak,
        atomic::{AtomicU64, Ordering},
    },
    thread::JoinHandle,
    time::{Duration, Instant},
};

use metrics::{Counter, Gauge};
use tracing::{info, warn};

use crate::{CheckOutcome, CheckedBlock, RootMismatch, SkipReason};

/// State-root checks counted since the node started.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RootCheckCounts {
    /// Blocks whose assembled root equals the header's.
    pub verified: u64,
    /// Blocks skipped because an owner of a needed subtree had no capture of them.
    pub skipped_not_available: u64,
    /// Blocks skipped because the trie is not two full levels of hashed branch nodes.
    pub skipped_unverifiable: u64,
    /// Blocks skipped because no owner of a needed subtree could be reached.
    pub skipped_unreachable: u64,
    /// Blocks whose assembled root differs from the header's.
    pub mismatched: u64,
}

impl RootCheckCounts {
    /// The checks counted since `earlier`.
    pub const fn since(&self, earlier: &Self) -> Self {
        Self {
            verified: self.verified - earlier.verified,
            skipped_not_available: self.skipped_not_available - earlier.skipped_not_available,
            skipped_unverifiable: self.skipped_unverifiable - earlier.skipped_unverifiable,
            skipped_unreachable: self.skipped_unreachable - earlier.skipped_unreachable,
            mismatched: self.mismatched - earlier.mismatched,
        }
    }

    /// Blocks skipped for any reason.
    pub const fn skipped(&self) -> u64 {
        self.skipped_not_available + self.skipped_unverifiable + self.skipped_unreachable
    }

    /// Whether anything was counted.
    pub const fn is_empty(&self) -> bool {
        self.verified == 0 && self.skipped() == 0 && self.mismatched == 0
    }
}

/// Lock-free counters of the state-root check, also exported as the metrics
/// `base_cluster_root_check_{verified,skipped_not_available,skipped_unverifiable,
/// skipped_unreachable,mismatched}` and the gauge `base_cluster_root_check_last_verified_block`.
#[derive(Debug)]
pub struct RootCheckStats {
    verified: AtomicU64,
    skipped_not_available: AtomicU64,
    skipped_unverifiable: AtomicU64,
    skipped_unreachable: AtomicU64,
    mismatched: AtomicU64,
    /// Last verified block number; 0 when none.
    last_verified: AtomicU64,
    /// Canonical head seen by the last step.
    head: AtomicU64,
    verified_metric: Counter,
    not_available_metric: Counter,
    unverifiable_metric: Counter,
    unreachable_metric: Counter,
    mismatched_metric: Counter,
    last_verified_metric: Gauge,
}

impl Default for RootCheckStats {
    fn default() -> Self {
        Self {
            verified: AtomicU64::new(0),
            skipped_not_available: AtomicU64::new(0),
            skipped_unverifiable: AtomicU64::new(0),
            skipped_unreachable: AtomicU64::new(0),
            mismatched: AtomicU64::new(0),
            last_verified: AtomicU64::new(0),
            head: AtomicU64::new(0),
            verified_metric: metrics::counter!("base_cluster_root_check_verified"),
            not_available_metric: metrics::counter!(
                "base_cluster_root_check_skipped_not_available"
            ),
            unverifiable_metric: metrics::counter!("base_cluster_root_check_skipped_unverifiable"),
            unreachable_metric: metrics::counter!("base_cluster_root_check_skipped_unreachable"),
            mismatched_metric: metrics::counter!("base_cluster_root_check_mismatched"),
            last_verified_metric: metrics::gauge!("base_cluster_root_check_last_verified_block"),
        }
    }
}

impl RootCheckStats {
    /// Interval of the root-check log line.
    pub const LOG_INTERVAL: Duration = Duration::from_secs(10);

    fn record_verified(&self, number: u64) {
        self.verified.fetch_add(1, Ordering::Relaxed);
        self.last_verified.fetch_max(number, Ordering::Relaxed);
        self.verified_metric.increment(1);
        self.last_verified_metric.set(self.last_verified.load(Ordering::Relaxed) as f64);
    }

    fn record_skip(&self, reason: SkipReason) {
        let (count, metric) = match reason {
            SkipReason::NotAvailable => (&self.skipped_not_available, &self.not_available_metric),
            SkipReason::Unverifiable => (&self.skipped_unverifiable, &self.unverifiable_metric),
            SkipReason::Unreachable => (&self.skipped_unreachable, &self.unreachable_metric),
        };
        count.fetch_add(1, Ordering::Relaxed);
        metric.increment(1);
    }

    fn record_mismatch(&self) {
        self.mismatched.fetch_add(1, Ordering::Relaxed);
        self.mismatched_metric.increment(1);
    }

    /// The checks counted so far.
    pub fn counts(&self) -> RootCheckCounts {
        RootCheckCounts {
            verified: self.verified.load(Ordering::Relaxed),
            skipped_not_available: self.skipped_not_available.load(Ordering::Relaxed),
            skipped_unverifiable: self.skipped_unverifiable.load(Ordering::Relaxed),
            skipped_unreachable: self.skipped_unreachable.load(Ordering::Relaxed),
            mismatched: self.mismatched.load(Ordering::Relaxed),
        }
    }

    /// The last verified block number, if any.
    pub fn last_verified(&self) -> Option<u64> {
        Some(self.last_verified.load(Ordering::Relaxed)).filter(|number| *number > 0)
    }

    /// Blocks between the canonical head and the last verified block (the head itself when none
    /// was verified).
    pub fn unverified_blocks(&self) -> u64 {
        let head = self.head.load(Ordering::Relaxed);
        head.saturating_sub(self.last_verified.load(Ordering::Relaxed))
    }

    /// Logs the checks of each `interval` in which any was counted, on a background thread that
    /// exits at the first interval after the stats are dropped.
    pub fn spawn_logger(self: &Arc<Self>, interval: Duration) -> io::Result<JoinHandle<()>> {
        let stats = Arc::downgrade(self);
        std::thread::Builder::new()
            .name("cluster-root-stats".into())
            .spawn(move || Self::log_every(&stats, interval))
    }

    fn log_every(stats: &Weak<Self>, interval: Duration) {
        let mut logged = RootCheckCounts::default();
        loop {
            std::thread::sleep(interval);
            let Some(stats) = stats.upgrade() else { return };
            let counts = stats.counts();
            let new = counts.since(&logged);
            if !new.is_empty() {
                info!(
                    verified = new.verified,
                    skipped_not_available = new.skipped_not_available,
                    skipped_unverifiable = new.skipped_unverifiable,
                    skipped_unreachable = new.skipped_unreachable,
                    mismatched = new.mismatched,
                    last_verified = ?stats.last_verified(),
                    unverified_blocks = stats.unverified_blocks(),
                    "cluster state root checks"
                );
            }
            logged = counts;
        }
    }
}

/// What one [`RootCheckSchedule::step`] did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StepOutcome {
    /// No block is ready to check.
    Idle,
    /// The block verified.
    Verified(u64),
    /// The block could not be assembled yet; it is tried again next step.
    Retrying(u64, SkipReason),
    /// The block was given up on.
    Skipped(u64, SkipReason),
    /// The block's assembled root differs from its header's.
    Mismatch(Box<RootMismatch>),
}

/// Which recent block to check next: one at a time, [`Self::LAG_BLOCKS`] behind the canonical
/// head, never more than [`Self::MAX_BEHIND_BLOCKS`] further behind; a block whose owners cannot
/// answer is tried again until [`Self::RETRY_FOR`] has passed, then skipped.
///
/// Checking starts above the head the node started at. Captures are kept in memory, so owners
/// that restarted with this node have none of the blocks it inserted before; each such block
/// would only be retried for [`Self::RETRY_FOR`] and skipped.
#[derive(Debug)]
pub struct RootCheckSchedule {
    stats: Arc<RootCheckStats>,
    warn_blocks: u64,
    /// Lowest block number not yet finished.
    next: u64,
    /// The block being retried and when it was first tried.
    pending: Option<(u64, Instant)>,
    /// The last verified block or, before the first, the head the node started at.
    verified_or_start: u64,
    last_warning: Option<Instant>,
}

impl RootCheckSchedule {
    /// Blocks behind the head a block is first checked: its owners insert it at about the same
    /// time as this node, so a short lag avoids needless retries.
    pub const LAG_BLOCKS: u64 = 2;
    /// Furthest a checked block may fall behind the lagged head before blocks are passed over:
    /// well inside [`RootCaptures::WINDOW_BLOCKS`](crate::RootCaptures::WINDOW_BLOCKS).
    pub const MAX_BEHIND_BLOCKS: u64 = 32;
    /// How long one block is retried while an owner has not captured it or cannot be reached.
    pub const RETRY_FOR: Duration = Duration::from_secs(20);
    /// Least time between two "not verified" warnings.
    pub const WARN_INTERVAL: Duration = Duration::from_secs(60);

    /// A schedule counting into `stats` that warns once `warn_blocks` blocks pass unverified, for
    /// a node that started at head `started_at`.
    pub const fn new(stats: Arc<RootCheckStats>, warn_blocks: u64, started_at: u64) -> Self {
        Self {
            stats,
            warn_blocks,
            next: started_at + 1,
            pending: None,
            verified_or_start: started_at,
            last_warning: None,
        }
    }

    /// The counters.
    pub const fn stats(&self) -> &Arc<RootCheckStats> {
        &self.stats
    }

    /// Checks at most one block at time `now`, given canonical head `head`, the canonical block
    /// at a number (`header`), and one check attempt (`check`).
    pub fn step(
        &mut self,
        head: u64,
        now: Instant,
        header: impl Fn(u64) -> Option<CheckedBlock>,
        check: impl FnOnce(CheckedBlock) -> CheckOutcome,
    ) -> StepOutcome {
        self.stats.head.store(head, Ordering::Relaxed);
        self.warn_if_unverified(head, self.verified_or_start, now);

        let (number, since) = match self.pending {
            Some(pending) => pending,
            None => {
                let target = head.saturating_sub(Self::LAG_BLOCKS);
                let number = self.next.max(target.saturating_sub(Self::MAX_BEHIND_BLOCKS));
                if number == 0 || number > target {
                    return StepOutcome::Idle;
                }
                (number, now)
            }
        };
        let Some(block) = header(number) else {
            self.pending = None;
            return StepOutcome::Idle;
        };
        let outcome = match check(block) {
            CheckOutcome::Verified => {
                self.stats.record_verified(number);
                self.verified_or_start = number;
                StepOutcome::Verified(number)
            }
            CheckOutcome::Mismatch(mismatch) => {
                self.stats.record_mismatch();
                StepOutcome::Mismatch(mismatch)
            }
            CheckOutcome::Skipped(reason)
                if reason != SkipReason::Unverifiable
                    && now.duration_since(since) < Self::RETRY_FOR =>
            {
                self.pending = Some((number, since));
                return StepOutcome::Retrying(number, reason);
            }
            CheckOutcome::Skipped(reason) => {
                self.stats.record_skip(reason);
                StepOutcome::Skipped(number, reason)
            }
        };
        self.pending = None;
        self.next = number + 1;
        outcome
    }

    fn warn_if_unverified(&mut self, head: u64, start: u64, now: Instant) {
        let unverified = head.saturating_sub(start);
        let quiet =
            self.last_warning.is_some_and(|last| now.duration_since(last) < Self::WARN_INTERVAL);
        if unverified < self.warn_blocks || quiet {
            return;
        }
        self.last_warning = Some(now);
        let counts = self.stats.counts();
        warn!(
            unverified_blocks = unverified,
            last_verified = ?self.stats.last_verified(),
            skipped_not_available = counts.skipped_not_available,
            skipped_unverifiable = counts.skipped_unverifiable,
            skipped_unreachable = counts.skipped_unreachable,
            "cluster state root not verified for many blocks"
        );
    }
}

#[cfg(test)]
mod tests {
    use alloy_primitives::B256;

    use super::*;

    fn block(number: u64) -> Option<CheckedBlock> {
        Some(CheckedBlock {
            number,
            hash: B256::with_last_byte(number as u8),
            state_root: B256::ZERO,
        })
    }

    #[test]
    fn checks_each_block_behind_the_head_once() {
        let mut schedule = RootCheckSchedule::new(Arc::default(), 100, 0);
        let now = Instant::now();
        assert_eq!(schedule.step(2, now, block, |_| unreachable!()), StepOutcome::Idle);

        assert_eq!(
            schedule.step(4, now, block, |_| CheckOutcome::Verified),
            StepOutcome::Verified(1)
        );
        assert_eq!(
            schedule.step(4, now, block, |_| CheckOutcome::Verified),
            StepOutcome::Verified(2)
        );
        assert_eq!(schedule.step(4, now, block, |_| unreachable!()), StepOutcome::Idle);

        assert_eq!(schedule.stats().counts().verified, 2);
        assert_eq!(schedule.stats().last_verified(), Some(2));
        assert_eq!(schedule.stats().unverified_blocks(), 2);
    }

    #[test]
    fn an_unavailable_block_is_retried_then_skipped_and_counted_never_mismatched() {
        let mut schedule = RootCheckSchedule::new(Arc::default(), 100, 0);
        let start = Instant::now();
        let unavailable = |_| CheckOutcome::Skipped(SkipReason::NotAvailable);

        assert_eq!(
            schedule.step(3, start, block, unavailable),
            StepOutcome::Retrying(1, SkipReason::NotAvailable)
        );
        let later = start + RootCheckSchedule::RETRY_FOR / 2;
        assert_eq!(
            schedule.step(5, later, block, unavailable),
            StepOutcome::Retrying(1, SkipReason::NotAvailable)
        );
        let expired = start + RootCheckSchedule::RETRY_FOR;
        assert_eq!(
            schedule.step(5, expired, block, unavailable),
            StepOutcome::Skipped(1, SkipReason::NotAvailable)
        );

        let counts = schedule.stats().counts();
        assert_eq!((counts.skipped_not_available, counts.mismatched, counts.verified), (1, 0, 0));
        assert_eq!(
            schedule.step(5, expired, block, |_| CheckOutcome::Verified),
            StepOutcome::Verified(2)
        );
    }

    #[test]
    fn an_unverifiable_block_is_skipped_at_once() {
        let mut schedule = RootCheckSchedule::new(Arc::default(), 100, 0);
        let outcome = schedule
            .step(3, Instant::now(), block, |_| CheckOutcome::Skipped(SkipReason::Unverifiable));

        assert_eq!(outcome, StepOutcome::Skipped(1, SkipReason::Unverifiable));
        assert_eq!(schedule.stats().counts().skipped_unverifiable, 1);
    }

    #[test]
    fn a_far_behind_schedule_jumps_to_recent_blocks() {
        let mut schedule = RootCheckSchedule::new(Arc::default(), 100, 0);
        let head = 1_000;
        let expected = head - RootCheckSchedule::LAG_BLOCKS - RootCheckSchedule::MAX_BEHIND_BLOCKS;

        let outcome = schedule.step(head, Instant::now(), block, |_| CheckOutcome::Verified);

        assert_eq!(outcome, StepOutcome::Verified(expected));
    }

    #[test]
    fn blocks_inserted_before_the_node_started_are_not_checked() {
        let mut schedule = RootCheckSchedule::new(Arc::default(), 100, 16);
        let now = Instant::now();
        assert_eq!(schedule.step(18, now, block, |_| unreachable!()), StepOutcome::Idle);

        assert_eq!(
            schedule.step(19, now, block, |_| CheckOutcome::Verified),
            StepOutcome::Verified(17)
        );
    }
}
