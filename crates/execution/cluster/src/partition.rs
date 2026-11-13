//! Reading a table of the source database with several threads: splitting it into consecutive
//! partitions, handing them out in order or as a shared work list, and reading each with its own
//! cursor.

use std::{
    borrow::Cow,
    collections::VecDeque,
    num::{NonZeroU64, NonZeroUsize},
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc::{self, Receiver, SyncSender},
    },
};

use alloy_primitives::B256;
use parking_lot::{Condvar, Mutex};
use reth_db::mdbx::{Cursor, RO, Transaction};

use crate::RowBatch;

/// A row's raw key and value, borrowed from a read transaction.
pub type RawRow<'txn> = (Cow<'txn, [u8]>, Cow<'txn, [u8]>);

/// How the source database is read: by how many threads, how far ahead of the writers, and in
/// partitions of what size.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadParallelism {
    /// Threads reading the source, each with its own read transaction and cursor.
    pub threads: NonZeroUsize,
    /// Partitions a bootstrap reads at once, counted from the one being written: with
    /// `queued_batches`, bounds the rows in memory.
    pub window: NonZeroUsize,
    /// Row batches queued for each partition being read.
    pub queued_batches: usize,
    /// Size of a partition, estimated from the table's page count.
    pub partition_bytes: NonZeroU64,
}

impl ReadParallelism {
    /// Reader threads when the operator does not ask for a number.
    pub const DEFAULT_THREADS: NonZeroUsize = NonZeroUsize::new(16).unwrap();
    /// Row batches queued per partition: with [`RowBatch::MAX_BYTES`], about 8 mebibytes, enough
    /// that a partition of [`Self::PARTITION_BYTES`] is read whole without waiting for the
    /// writers.
    pub const QUEUED_BATCHES: usize = 8;
    /// 4 mebibytes of table pages per partition: small enough to fit its queue, large enough that
    /// seeking to it costs little against reading it.
    pub const PARTITION_BYTES: NonZeroU64 = NonZeroU64::new(4 << 20).unwrap();
    /// [`Self::DEFAULT_THREADS`] threads.
    pub const DEFAULT: Self = Self::with_threads(Self::DEFAULT_THREADS);

    /// `threads` threads, each partition being read at once: rows in memory are bounded by
    /// `threads * (QUEUED_BATCHES + 1)` batches.
    pub const fn with_threads(threads: NonZeroUsize) -> Self {
        Self {
            threads,
            window: threads,
            queued_batches: Self::QUEUED_BATCHES,
            partition_bytes: Self::PARTITION_BYTES,
        }
    }
}

/// A position in a table's order: the first row whose key is at least `key` and, when
/// `duplicate` is not empty, whose value is at least `duplicate` among that key's values.
/// Positions order as the rows they start at.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct TablePosition {
    key: Vec<u8>,
    duplicate: Vec<u8>,
}

impl TablePosition {
    /// The first row of the table.
    pub const START: Self = Self { key: Vec::new(), duplicate: Vec::new() };

    /// The first row whose key is at least `key`.
    pub const fn key(key: Vec<u8>) -> Self {
        Self { key, duplicate: Vec::new() }
    }

    /// The first row whose key is greater than `key`.
    pub fn after_key(key: &[u8]) -> Self {
        let mut after = Vec::with_capacity(key.len() + 1);
        after.extend_from_slice(key);
        after.push(0);
        Self::key(after)
    }

    /// The first row of a duplicate-key table whose key is `key` and whose value is at least
    /// `duplicate`, else the first row with a greater key.
    pub fn duplicate(key: &[u8], duplicate: Vec<u8>) -> Self {
        Self { key: key.to_vec(), duplicate }
    }

    /// Whether the row `key`, `value` is at or after this position.
    pub fn is_reached_by(&self, key: &[u8], value: &[u8]) -> bool {
        (key, value) >= (self.key.as_slice(), self.duplicate.as_slice())
    }

    /// Positions `cursor` at the first row at or after this position and returns it.
    pub fn seek<'txn>(
        &self,
        cursor: &mut Cursor<RO>,
    ) -> Result<Option<RawRow<'txn>>, reth_db::mdbx::Error> {
        if self.duplicate.is_empty() {
            return if self.key.is_empty() { cursor.first() } else { cursor.set_range(&self.key) };
        }
        if cursor.get_both_range::<()>(&self.key, &self.duplicate)?.is_some() {
            cursor.get_current()
        } else {
            Self::after_key(&self.key).seek(cursor)
        }
    }

    /// The `index`-th of `count` ascending 4-byte big-endian prefixes spaced evenly over all byte
    /// strings, for `0 < index < count <= 2^32`.
    fn prefix(index: u64, count: u64) -> Vec<u8> {
        (((index << Self::PREFIX_BITS) / count) as u32).to_be_bytes().to_vec()
    }

    /// Bits of a partition boundary's prefix.
    const PREFIX_BITS: u32 = 32;
}

/// A table's consecutive partitions: the first starts at the table's first row, each ends where
/// the next starts, and the last ends with the table. Together they hold every row once, in
/// order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TablePartitions {
    starts: Vec<TablePosition>,
}

impl TablePartitions {
    /// Partitions of table `dbi` of about `partition_bytes` of its pages each, at key prefixes
    /// spaced evenly over the key space; and, for each key of `split_keys` (duplicate-key tables
    /// only), as many partitions of that key's values at value prefixes spaced evenly, the first
    /// starting at the key's first value.
    pub fn plan(
        txn: &Transaction<RO>,
        dbi: u32,
        split_keys: &[B256],
        partition_bytes: NonZeroU64,
    ) -> Result<Self, reth_db::mdbx::Error> {
        let stat = txn.db_stat(dbi)?;
        let bytes =
            (stat.leaf_pages() + stat.overflow_pages()) as u64 * u64::from(stat.page_size());
        let count = bytes.div_ceil(partition_bytes.get()).clamp(1, 1 << TablePosition::PREFIX_BITS);
        let prefixes = || (1..count).map(move |index| TablePosition::prefix(index, count));
        let mut starts: Vec<_> = std::iter::once(TablePosition::START)
            .chain(prefixes().map(TablePosition::key))
            .collect();
        for key in split_keys {
            starts.push(TablePosition::key(key.to_vec()));
            starts
                .extend(prefixes().map(|prefix| TablePosition::duplicate(key.as_slice(), prefix)));
            starts.push(TablePosition::after_key(key.as_slice()));
        }
        starts.sort_unstable();
        starts.dedup();
        Ok(Self { starts })
    }

    /// Number of partitions.
    pub const fn len(&self) -> usize {
        self.starts.len()
    }

    /// Whether there is no partition; never true of a planned table.
    pub const fn is_empty(&self) -> bool {
        self.starts.is_empty()
    }

    /// Where partition `index` starts and, unless it is the last, where it ends.
    pub fn range(&self, index: usize) -> TableRange {
        TableRange { start: self.starts[index].clone(), end: self.starts.get(index + 1).cloned() }
    }
}

/// Consecutive rows of a table: from `start` up to, not including, `end`, or to the table's end.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableRange {
    /// The range's first position.
    pub start: TablePosition,
    /// The first position after the range; `None` at the table's end.
    pub end: Option<TablePosition>,
}

impl TableRange {
    /// Whether the row `key`, `value`, at or after `start`, lies past the range.
    pub fn is_passed_by(&self, key: &[u8], value: &[u8]) -> bool {
        self.end.as_ref().is_some_and(|end| end.is_reached_by(key, value))
    }
}

/// What a reader sends about the partition it reads: its rows in batches, then its end.
#[derive(Debug)]
pub enum PartitionRows {
    /// The partition's next rows.
    Batch(RowBatch),
    /// Every row of the partition has been sent.
    End,
}

/// A partition claimed by a reader: its rows and where to send them.
#[derive(Debug)]
pub struct ClaimedPartition {
    /// The partition's rows.
    pub range: TableRange,
    /// The partition's queue; dropping it before sending [`PartitionRows::End`] tells the
    /// consumer that the partition was not read to its end.
    pub rows: SyncSender<PartitionRows>,
}

/// The partitions of one table, read by a pool of threads and consumed in order by one thread.
/// Readers claim partitions in order, at most `window` from the oldest one not yet consumed, so
/// the rows in memory stay bounded by `window * (queued_batches + 1)` batches; each partition's
/// queue is created when it is claimed.
#[derive(Debug)]
pub struct OrderedPartitions {
    partitions: TablePartitions,
    window: usize,
    queued_batches: usize,
    claims: Mutex<PartitionClaims>,
    changed: Condvar,
    stopped: AtomicBool,
}

/// The claiming state of an [`OrderedPartitions`].
#[derive(Debug)]
pub struct PartitionClaims {
    /// Partitions claimed by readers.
    claimed: usize,
    /// Partitions the consumer has finished.
    consumed: usize,
    /// Queues of the claimed partitions the consumer has not taken yet, in partition order.
    queued: VecDeque<Receiver<PartitionRows>>,
}

impl OrderedPartitions {
    /// `partitions` to be read at most `window` at a time, with `queued_batches` batches queued
    /// for each.
    pub const fn new(
        partitions: TablePartitions,
        window: NonZeroUsize,
        queued_batches: usize,
    ) -> Self {
        Self {
            partitions,
            window: window.get(),
            queued_batches,
            claims: Mutex::new(PartitionClaims {
                claimed: 0,
                consumed: 0,
                queued: VecDeque::new(),
            }),
            changed: Condvar::new(),
            stopped: AtomicBool::new(false),
        }
    }

    /// Number of partitions.
    pub const fn len(&self) -> usize {
        self.partitions.len()
    }

    /// Whether there is no partition.
    pub const fn is_empty(&self) -> bool {
        self.partitions.is_empty()
    }

    /// Claims the next partition, waiting until it is inside the window; `None` once every
    /// partition is claimed or the reading has stopped.
    pub fn claim(&self) -> Option<ClaimedPartition> {
        let mut claims = self.claims.lock();
        loop {
            if self.is_stopped() || claims.claimed == self.len() {
                return None;
            }
            if claims.claimed < claims.consumed + self.window {
                let (rows, queue) = mpsc::sync_channel(self.queued_batches);
                let range = self.partitions.range(claims.claimed);
                claims.claimed += 1;
                claims.queued.push_back(queue);
                self.changed.notify_all();
                return Some(ClaimedPartition { range, rows });
            }
            self.changed.wait(&mut claims);
        }
    }

    /// The single consumer of the partitions; dropping it stops the reading.
    pub const fn consumer(&self) -> PartitionConsumer<'_> {
        PartitionConsumer { partitions: self, taken: 0 }
    }

    /// Stops the reading: no partition is claimed any more, and the queues the consumer has not
    /// taken are closed, so readers blocked on them return.
    pub fn stop(&self) {
        let mut claims = self.claims.lock();
        self.stopped.store(true, Ordering::Relaxed);
        claims.queued.clear();
        self.changed.notify_all();
    }

    /// Whether the reading has stopped.
    pub fn is_stopped(&self) -> bool {
        self.stopped.load(Ordering::Relaxed)
    }
}

/// The consumer of [`OrderedPartitions`]: takes each partition's queue in order. Dropping it,
/// on success, error, or panic, stops the reading, so no reader waits for it forever.
#[derive(Debug)]
pub struct PartitionConsumer<'a> {
    partitions: &'a OrderedPartitions,
    taken: usize,
}

impl PartitionConsumer<'_> {
    /// Marks every partition taken so far as consumed and waits for the next one's queue; `None`
    /// once every partition is consumed or the reading has stopped.
    pub fn next_partition(&mut self) -> Option<Receiver<PartitionRows>> {
        let partitions = self.partitions;
        let mut claims = partitions.claims.lock();
        claims.consumed = self.taken;
        partitions.changed.notify_all();
        loop {
            if let Some(queue) = claims.queued.pop_front() {
                self.taken += 1;
                return Some(queue);
            }
            if partitions.is_stopped() || claims.consumed == partitions.len() {
                return None;
            }
            partitions.changed.wait(&mut claims);
        }
    }
}

impl Drop for PartitionConsumer<'_> {
    fn drop(&mut self) {
        self.partitions.stop();
    }
}

/// A shared list of work items for a pool of threads, to which items may add more. A thread
/// waits for an item while any item is still being worked on, since that one may add more.
#[derive(Debug)]
pub struct WorkQueue<T> {
    work: Mutex<PendingWork<T>>,
    changed: Condvar,
    stopped: AtomicBool,
}

/// The items of a [`WorkQueue`] not yet taken, and the number not yet finished.
#[derive(Debug)]
pub struct PendingWork<T> {
    items: VecDeque<T>,
    unfinished: usize,
}

impl<T> WorkQueue<T> {
    /// A queue of `items`.
    pub fn new(items: impl IntoIterator<Item = T>) -> Self {
        let items: VecDeque<_> = items.into_iter().collect();
        Self {
            work: Mutex::new(PendingWork { unfinished: items.len(), items }),
            changed: Condvar::new(),
            stopped: AtomicBool::new(false),
        }
    }

    /// Takes the next item, waiting while the queue is empty but some item is unfinished; `None`
    /// once every item is finished or the queue has stopped. The item is finished when the
    /// returned guard drops.
    pub fn next_item(&self) -> Option<WorkItem<'_, T>> {
        let mut work = self.work.lock();
        loop {
            if self.is_stopped() || work.unfinished == 0 {
                return None;
            }
            if let Some(item) = work.items.pop_front() {
                return Some(WorkItem { queue: self, item });
            }
            self.changed.wait(&mut work);
        }
    }

    /// Adds `items` at the front of the queue, to be taken before the others.
    pub fn push_front(&self, items: Vec<T>) {
        let mut work = self.work.lock();
        work.unfinished += items.len();
        for item in items.into_iter().rev() {
            work.items.push_front(item);
        }
        self.changed.notify_all();
    }

    /// Stops the queue: no item is handed out any more.
    pub fn stop(&self) {
        let _work = self.work.lock();
        self.stopped.store(true, Ordering::Relaxed);
        self.changed.notify_all();
    }

    /// Whether the queue has stopped.
    pub fn is_stopped(&self) -> bool {
        self.stopped.load(Ordering::Relaxed)
    }

    fn finish_item(&self) {
        let mut work = self.work.lock();
        work.unfinished -= 1;
        if work.unfinished == 0 {
            self.changed.notify_all();
        }
    }
}

/// An item taken from a [`WorkQueue`]; it is finished when this drops, also on panic, so the
/// other threads never wait for it forever.
#[derive(Debug)]
pub struct WorkItem<'a, T> {
    queue: &'a WorkQueue<T>,
    /// The item.
    pub item: T,
}

impl<T> Drop for WorkItem<'_, T> {
    fn drop(&mut self) {
        self.queue.finish_item();
    }
}

#[cfg(test)]
mod tests {
    use std::thread;

    use super::*;

    fn partitions(count: u8) -> TablePartitions {
        let starts = std::iter::once(TablePosition::START)
            .chain((1..count).map(|byte| TablePosition::key(vec![byte])))
            .collect();
        TablePartitions { starts }
    }

    #[test]
    fn readers_claim_in_order_within_the_window_and_the_consumer_takes_in_order() {
        let ordered = OrderedPartitions::new(partitions(4), NonZeroUsize::new(2).unwrap(), 1);
        let first = ordered.claim().unwrap();
        let second = ordered.claim().unwrap();
        assert_eq!(first.range, partitions(4).range(0));
        assert_eq!(second.range, partitions(4).range(1));
        let mut consumer = ordered.consumer();
        thread::scope(|scope| {
            let third = scope.spawn(|| ordered.claim().map(|claimed| claimed.range));
            second.rows.send(PartitionRows::End).unwrap();
            first.rows.send(PartitionRows::End).unwrap();
            let queue = consumer.next_partition().unwrap();
            assert!(matches!(queue.recv(), Ok(PartitionRows::End)), "partition 0 comes first");
            let queue = consumer.next_partition().unwrap();
            assert!(matches!(queue.recv(), Ok(PartitionRows::End)));
            assert_eq!(third.join().unwrap(), Some(partitions(4).range(2)), "inside the window");
        });
        assert!(consumer.next_partition().unwrap().recv().is_err(), "partition 2 was dropped");
        assert_eq!(ordered.claim().map(|claimed| claimed.range), Some(partitions(4).range(3)));
        assert!(ordered.claim().is_none(), "every partition is claimed");
    }

    #[test]
    fn dropping_the_consumer_releases_readers_waiting_for_the_window_or_their_queue() {
        let ordered = OrderedPartitions::new(partitions(3), NonZeroUsize::MIN, 0);
        let consumer = ordered.consumer();
        let claimed = ordered.claim().unwrap();
        thread::scope(|scope| {
            let waiting = scope.spawn(|| ordered.claim().is_none());
            let sending = scope.spawn(move || claimed.rows.send(PartitionRows::End).is_err());
            drop(consumer);
            assert!(waiting.join().unwrap(), "no partition is claimed after the stop");
            assert!(sending.join().unwrap(), "the untaken queue is closed");
        });
    }

    #[test]
    fn a_reader_waiting_for_the_window_is_released_by_a_failing_reader() {
        let ordered = OrderedPartitions::new(partitions(3), NonZeroUsize::MIN, 1);
        let _consumer = ordered.consumer();
        let claimed = ordered.claim().unwrap();
        thread::scope(|scope| {
            let waiting = scope.spawn(|| ordered.claim().is_none());
            drop(claimed);
            ordered.stop();
            assert!(waiting.join().unwrap(), "no partition is claimed after the stop");
        });
    }

    #[test]
    fn a_partition_dropped_before_its_end_disconnects_the_consumer() {
        let ordered = OrderedPartitions::new(partitions(2), NonZeroUsize::MIN, 1);
        let claimed = ordered.claim().unwrap();
        claimed.rows.send(PartitionRows::Batch(RowBatch::default())).unwrap();
        drop(claimed);
        let mut consumer = ordered.consumer();
        let queue = consumer.next_partition().unwrap();
        assert!(matches!(queue.recv(), Ok(PartitionRows::Batch(_))));
        assert!(queue.recv().is_err(), "no end: the partition was not read whole");
    }

    #[test]
    fn work_added_by_an_item_is_taken_and_a_panicking_item_still_finishes() {
        let queue = WorkQueue::new([1u32, 2]);
        let mut seen = Vec::new();
        while let Some(work) = queue.next_item() {
            if work.item == 1 {
                queue.push_front(vec![10, 11]);
            }
            seen.push(work.item);
        }
        assert_eq!(seen, [1, 10, 11, 2]);

        let queue = WorkQueue::new([1u32, 2]);
        thread::scope(|scope| {
            let panicking = scope.spawn(|| {
                let _work = queue.next_item();
                panic!("worker panics holding an item");
            });
            assert!(panicking.join().is_err());
            let rest: Vec<_> =
                std::iter::from_fn(|| queue.next_item().map(|work| work.item)).collect();
            assert_eq!(rest, [2], "the panicked item is finished, so the queue drains");
        });
    }

    #[test]
    fn positions_order_as_the_rows_they_start_at() {
        let key = [7u8; 32];
        let first = TablePosition::key(key.to_vec());
        let middle = TablePosition::duplicate(&key, vec![0x80]);
        let after = TablePosition::after_key(&key);
        assert!(TablePosition::START < first && first < middle && middle < after);
        assert!(first.is_reached_by(&key, &[]));
        assert!(!middle.is_reached_by(&key, &[0x7f, 0xff]));
        assert!(middle.is_reached_by(&key, &[0x80]));
        assert!(!after.is_reached_by(&key, &[0xff; 40]));
        let mut next = key;
        next[31] += 1;
        assert!(after.is_reached_by(&next, &[]));
        assert!(!TablePosition::key(vec![8]).is_reached_by(&next, &[]));
        assert!(TablePosition::key(vec![8]).is_reached_by(&[8], &[]));
    }

    #[test]
    fn prefixes_ascend_and_spread_over_the_key_space() {
        let prefixes: Vec<_> = (1..256).map(|index| TablePosition::prefix(index, 256)).collect();
        assert!(prefixes.windows(2).all(|pair| pair[0] < pair[1]));
        assert_eq!(prefixes[0], vec![1, 0, 0, 0]);
        assert_eq!(prefixes[254], vec![0xff, 0, 0, 0]);
    }
}
