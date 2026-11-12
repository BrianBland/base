use std::{
    ops::{Bound, RangeBounds},
    sync::Arc,
};

use reth_db::{
    DatabaseError,
    common::{PairResult, ValueOnlyResult},
    cursor::{
        DbCursorRO, DbCursorRW, DbDupCursorRO, DbDupCursorRW, DupWalker, RangeWalker,
        ReverseWalker, Walker,
    },
    mdbx::{Error as MdbxError, RW, TransactionKind, cursor::Cursor},
    table::{DupSort, Encode, IntoVec, Table},
};

use crate::{ClusterNode, ShardedReads, ShardedTable};

/// What a [`ClusteredCursor`] does beyond forwarding to MDBX.
#[derive(Debug)]
pub enum CursorSharding {
    /// Forward everything: the table is not sharded or the database is a pass-through.
    PassThrough,
    /// Route reads: a read-only cursor of a clustered database over a sharded table.
    Reads(ShardedReads),
    /// Drop writes to keys this node does not hold: a read-write cursor of a clustered database
    /// over a sharded table. Reads stay local.
    Writes {
        /// This node.
        node: Arc<ClusterNode>,
        /// The table written.
        table: ShardedTable,
    },
}

impl CursorSharding {
    /// Whether a write of `key` reaches MDBX.
    pub fn accepts_write<T: Table>(&self, key: &T::Key) -> Result<bool, DatabaseError> {
        match self {
            Self::Writes { node, table } => node.holds(*table, &key.clone().encode().into_vec()),
            Self::PassThrough | Self::Reads(_) => Ok(true),
        }
    }
}

/// A [`ClusteredTx`](crate::ClusteredTx) cursor over table `T`: read-only, read-write, and for
/// duplicate-key tables also dup read-only and dup read-write, matching MDBX's single cursor type.
///
/// The table is fixed when the cursor is created. Walkers are built from this cursor's own
/// positioning methods, so every read, including iteration, passes through the methods below.
#[derive(Debug)]
pub struct ClusteredCursor<K: TransactionKind, T: Table> {
    inner: Cursor<K, T>,
    sharding: CursorSharding,
}

impl<K: TransactionKind, T: Table> ClusteredCursor<K, T> {
    /// Wraps an MDBX cursor.
    pub const fn new(inner: Cursor<K, T>, sharding: CursorSharding) -> Self {
        Self { inner, sharding }
    }
}

impl<K: TransactionKind, T: Table> DbCursorRO<T> for ClusteredCursor<K, T> {
    fn first(&mut self) -> PairResult<T> {
        match &mut self.sharding {
            CursorSharding::Reads(reads) => reads.first(&mut self.inner),
            _ => self.inner.first(),
        }
    }

    fn seek_exact(&mut self, key: T::Key) -> PairResult<T> {
        match &mut self.sharding {
            CursorSharding::Reads(reads) => reads.seek_exact(&mut self.inner, key),
            _ => self.inner.seek_exact(key),
        }
    }

    fn seek(&mut self, key: T::Key) -> PairResult<T> {
        match &mut self.sharding {
            CursorSharding::Reads(reads) => reads.seek(&mut self.inner, key),
            _ => self.inner.seek(key),
        }
    }

    fn next(&mut self) -> PairResult<T> {
        match &mut self.sharding {
            CursorSharding::Reads(reads) => reads.next(&mut self.inner),
            _ => self.inner.next(),
        }
    }

    fn prev(&mut self) -> PairResult<T> {
        match &self.sharding {
            CursorSharding::Reads(reads) => Err(reads.unsupported("prev")),
            _ => self.inner.prev(),
        }
    }

    fn last(&mut self) -> PairResult<T> {
        match &self.sharding {
            CursorSharding::Reads(reads) => Err(reads.unsupported("last")),
            _ => self.inner.last(),
        }
    }

    fn current(&mut self) -> PairResult<T> {
        match &mut self.sharding {
            CursorSharding::Reads(reads) => reads.current(&mut self.inner),
            _ => self.inner.current(),
        }
    }

    fn walk(&mut self, start_key: Option<T::Key>) -> Result<Walker<'_, T, Self>, DatabaseError> {
        let start = match start_key {
            Some(key) => self.seek(key),
            None => self.first(),
        };
        Ok(Walker::new(self, start.transpose()))
    }

    fn walk_range(
        &mut self,
        range: impl RangeBounds<T::Key>,
    ) -> Result<RangeWalker<'_, T, Self>, DatabaseError> {
        let start = match range.start_bound().cloned() {
            Bound::Included(key) => self.seek(key),
            Bound::Excluded(_) => unreachable!("MDBX cursors reject an excluded start bound"),
            Bound::Unbounded => self.first(),
        };
        Ok(RangeWalker::new(self, start.transpose(), range.end_bound().cloned()))
    }

    fn walk_back(
        &mut self,
        start_key: Option<T::Key>,
    ) -> Result<ReverseWalker<'_, T, Self>, DatabaseError> {
        let start = match start_key {
            Some(key) => self.seek(key),
            None => self.last(),
        };
        Ok(ReverseWalker::new(self, start.transpose()))
    }

    fn prefetch(&mut self, enabled: bool) -> &mut Self {
        self.inner.prefetch(enabled);
        self
    }
}

impl<K: TransactionKind, T: DupSort> DbDupCursorRO<T> for ClusteredCursor<K, T> {
    fn prev_dup(&mut self) -> PairResult<T> {
        match &self.sharding {
            CursorSharding::Reads(reads) => Err(reads.unsupported("prev_dup")),
            _ => self.inner.prev_dup(),
        }
    }

    fn next_dup(&mut self) -> PairResult<T> {
        match &mut self.sharding {
            CursorSharding::Reads(reads) => reads.next_dup(&mut self.inner),
            _ => self.inner.next_dup(),
        }
    }

    fn last_dup(&mut self) -> ValueOnlyResult<T> {
        match &self.sharding {
            CursorSharding::Reads(reads) => Err(reads.unsupported("last_dup")),
            _ => self.inner.last_dup(),
        }
    }

    fn next_no_dup(&mut self) -> PairResult<T> {
        match &self.sharding {
            CursorSharding::Reads(reads) => Err(reads.unsupported("next_no_dup")),
            _ => self.inner.next_no_dup(),
        }
    }

    fn next_dup_val(&mut self) -> ValueOnlyResult<T> {
        match &mut self.sharding {
            CursorSharding::Reads(reads) => {
                reads.next_dup(&mut self.inner).map(|row| row.map(|(_, value)| value))
            }
            _ => self.inner.next_dup_val(),
        }
    }

    fn seek_by_key_subkey(&mut self, key: T::Key, subkey: T::SubKey) -> ValueOnlyResult<T> {
        match &mut self.sharding {
            CursorSharding::Reads(reads) => reads.seek_by_key_subkey(&mut self.inner, key, subkey),
            _ => self.inner.seek_by_key_subkey(key, subkey),
        }
    }

    /// Starts where MDBX's own `walk_dup` does, including its `NotFound` error when only a subkey
    /// is given and the table is empty.
    fn walk_dup(
        &mut self,
        key: Option<T::Key>,
        subkey: Option<T::SubKey>,
    ) -> Result<DupWalker<'_, T, Self>, DatabaseError> {
        let start = match (key, subkey) {
            (Some(key), Some(subkey)) => {
                self.seek_by_key_subkey(key.clone(), subkey)?.map(|value| Ok((key, value)))
            }
            (Some(key), None) => self.seek_exact(key)?.map(Ok),
            (None, Some(subkey)) => match self.first()? {
                Some((key, _)) => {
                    self.seek_by_key_subkey(key.clone(), subkey)?.map(|value| Ok((key, value)))
                }
                None => Some(Err(DatabaseError::Read(MdbxError::NotFound.into()))),
            },
            (None, None) => self.first().transpose(),
        };
        Ok(DupWalker { cursor: self, start })
    }
}

impl<T: Table> DbCursorRW<T> for ClusteredCursor<RW, T> {
    fn upsert(&mut self, key: T::Key, value: &T::Value) -> Result<(), DatabaseError> {
        if !self.sharding.accepts_write::<T>(&key)? {
            return Ok(());
        }
        self.inner.upsert(key, value)
    }

    fn insert(&mut self, key: T::Key, value: &T::Value) -> Result<(), DatabaseError> {
        if !self.sharding.accepts_write::<T>(&key)? {
            return Ok(());
        }
        self.inner.insert(key, value)
    }

    fn append(&mut self, key: T::Key, value: &T::Value) -> Result<(), DatabaseError> {
        if !self.sharding.accepts_write::<T>(&key)? {
            return Ok(());
        }
        self.inner.append(key, value)
    }

    fn delete_current(&mut self) -> Result<(), DatabaseError> {
        self.inner.delete_current()
    }
}

impl<T: DupSort> DbDupCursorRW<T> for ClusteredCursor<RW, T> {
    fn delete_current_duplicates(&mut self) -> Result<(), DatabaseError> {
        self.inner.delete_current_duplicates()
    }

    fn append_dup(&mut self, key: T::Key, value: T::Value) -> Result<(), DatabaseError> {
        if !self.sharding.accepts_write::<T>(&key)? {
            return Ok(());
        }
        self.inner.append_dup(key, value)
    }
}
