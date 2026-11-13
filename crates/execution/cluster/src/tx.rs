use std::sync::Arc;

use reth_db::{
    DatabaseError,
    cursor::DbCursorRO,
    mdbx::{RW, TransactionKind, tx::Tx},
    table::{Decode, Decompress, DupSort, Encode, IntoVec, Table, TableImporter},
    transaction::{DbTx, DbTxMut},
};

use crate::{
    ClusterNode, ClusteredCursor, CursorSharding, PointRead, ReadSnapshot, ShardedReads,
    ShardedTable, SplitCursor,
};

/// What a [`ClusteredTx`] does beyond forwarding to MDBX.
#[derive(Debug, Clone)]
pub enum TxSharding {
    /// Forward everything: the database is a pass-through.
    PassThrough,
    /// A read-only transaction of a clustered database, reading at a snapshot.
    Reads(Arc<ReadSnapshot>),
    /// A read-write transaction of a clustered database.
    Writes(Arc<ClusterNode>),
}

/// A [`ClusteredDatabase`](crate::ClusteredDatabase) transaction, read-only (`RO`) or read-write
/// (`RW`).
///
/// Every point read and write passes through this type with its table and key known; cursors are
/// handed out as [`ClusteredCursor`]s for the requested table.
#[derive(Debug)]
pub struct ClusteredTx<K: TransactionKind> {
    inner: Tx<K>,
    sharding: TxSharding,
}

impl<K: TransactionKind> ClusteredTx<K> {
    /// Wraps an MDBX transaction.
    pub const fn new(inner: Tx<K>, sharding: TxSharding) -> Self {
        Self { inner, sharding }
    }

    /// How a cursor over `T` is sharded. A cursor of a duplicate-key table gets a split cursor of
    /// its own, opened on first use, when any contract's storage is split.
    fn cursor_sharding<T: Table>(&self) -> Result<CursorSharding<K>, DatabaseError> {
        let Some(table) = ShardedTable::from_table_name(T::NAME) else {
            return Ok(CursorSharding::PassThrough);
        };
        let (node, snapshot) = match &self.sharding {
            TxSharding::PassThrough => return Ok(CursorSharding::PassThrough),
            TxSharding::Reads(snapshot) => {
                (Arc::clone(snapshot.node()), Some(Arc::clone(snapshot)))
            }
            TxSharding::Writes(node) => (Arc::clone(node), None),
        };
        let split = if node.cluster().splits_storage() {
            SplitCursor::new(&self.inner, table)?
        } else {
            None
        };
        Ok(CursorSharding::Sharded(ShardedReads::new(node, table, snapshot, split)))
    }

    /// Reads the value of `key` of a sharded table where the read rule says, or `None` to read
    /// local MDBX.
    fn sharded_get<T: Table>(
        &self,
        key: impl FnOnce() -> Result<T::Key, DatabaseError>,
        encoded: &[u8],
    ) -> Result<Option<Option<T::Value>>, DatabaseError> {
        let CursorSharding::Sharded(reads) = self.cursor_sharding::<T>()? else { return Ok(None) };
        if reads.table().is_duplicate() {
            let first = self.cursor_read::<T>()?.seek_exact(key()?)?;
            return Ok(Some(first.map(|(_, value)| value)));
        }
        match reads.point(encoded)? {
            PointRead::Local => Ok(None),
            PointRead::Invisible => Ok(Some(None)),
            PointRead::Remote(value) => {
                Ok(Some(value.map(|value| T::Value::decompress(&value)).transpose()?))
            }
        }
    }

    fn accepts_write<T: Table>(
        &self,
        key: &T::Key,
        value: &T::Value,
    ) -> Result<bool, DatabaseError> {
        self.cursor_sharding::<T>()?.accepts_write::<T>(key, value)
    }
}

impl<K: TransactionKind> DbTx for ClusteredTx<K> {
    type Cursor<T: Table> = ClusteredCursor<K, T>;
    type DupCursor<T: DupSort> = ClusteredCursor<K, T>;

    fn get<T: Table>(&self, key: T::Key) -> Result<Option<T::Value>, DatabaseError> {
        if !matches!(self.sharding, TxSharding::PassThrough) {
            let encoded = key.clone().encode().into_vec();
            if let Some(value) = self.sharded_get::<T>(|| Ok(key.clone()), &encoded)? {
                return Ok(value);
            }
        }
        self.inner.get::<T>(key)
    }

    fn get_by_encoded_key<T: Table>(
        &self,
        key: &<T::Key as Encode>::Encoded,
    ) -> Result<Option<T::Value>, DatabaseError> {
        if !matches!(self.sharding, TxSharding::PassThrough) {
            let decoded = || T::Key::decode(key.as_ref());
            if let Some(value) = self.sharded_get::<T>(decoded, key.as_ref())? {
                return Ok(value);
            }
        }
        self.inner.get_by_encoded_key::<T>(key)
    }

    fn commit(self) -> Result<(), DatabaseError> {
        match self.sharding {
            TxSharding::Writes(node) => node.commit(self.inner),
            TxSharding::PassThrough | TxSharding::Reads(_) => self.inner.commit(),
        }
    }

    fn abort(self) {
        self.inner.abort()
    }

    fn cursor_read<T: Table>(&self) -> Result<Self::Cursor<T>, DatabaseError> {
        let sharding = self.cursor_sharding::<T>()?;
        self.inner.cursor_read::<T>().map(|cursor| ClusteredCursor::new(cursor, sharding))
    }

    fn cursor_dup_read<T: DupSort>(&self) -> Result<Self::DupCursor<T>, DatabaseError> {
        let sharding = self.cursor_sharding::<T>()?;
        self.inner.cursor_dup_read::<T>().map(|cursor| ClusteredCursor::new(cursor, sharding))
    }

    fn entries<T: Table>(&self) -> Result<usize, DatabaseError> {
        self.inner.entries::<T>()
    }

    fn disable_long_read_transaction_safety(&mut self) {
        self.inner.disable_long_read_transaction_safety()
    }

    fn prefetch(&self, enabled: bool) -> &Self {
        self.inner.prefetch(enabled);
        self
    }
}

impl DbTxMut for ClusteredTx<RW> {
    type CursorMut<T: Table> = ClusteredCursor<RW, T>;
    type DupCursorMut<T: DupSort> = ClusteredCursor<RW, T>;

    fn put<T: Table>(&self, key: T::Key, value: T::Value) -> Result<(), DatabaseError> {
        if !self.accepts_write::<T>(&key, &value)? {
            return Ok(());
        }
        DbTxMut::put::<T>(&self.inner, key, value)
    }

    fn append<T: Table>(&self, key: T::Key, value: T::Value) -> Result<(), DatabaseError> {
        if !self.accepts_write::<T>(&key, &value)? {
            return Ok(());
        }
        DbTxMut::append::<T>(&self.inner, key, value)
    }

    fn delete<T: Table>(
        &self,
        key: T::Key,
        value: Option<T::Value>,
    ) -> Result<bool, DatabaseError> {
        self.inner.delete::<T>(key, value)
    }

    fn clear<T: Table>(&self) -> Result<(), DatabaseError> {
        self.inner.clear::<T>()
    }

    fn cursor_write<T: Table>(&self) -> Result<Self::CursorMut<T>, DatabaseError> {
        let sharding = self.cursor_sharding::<T>()?;
        self.inner.cursor_write::<T>().map(|cursor| ClusteredCursor::new(cursor, sharding))
    }

    fn cursor_dup_write<T: DupSort>(&self) -> Result<Self::DupCursorMut<T>, DatabaseError> {
        let sharding = self.cursor_sharding::<T>()?;
        self.inner.cursor_dup_write::<T>().map(|cursor| ClusteredCursor::new(cursor, sharding))
    }
}

impl TableImporter for ClusteredTx<RW> {}
