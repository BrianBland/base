use std::sync::Arc;

use reth_db::{
    DatabaseError,
    mdbx::{RW, TransactionKind, tx::Tx},
    table::{Decompress, DupSort, Encode, IntoVec, Table, TableImporter},
    transaction::{DbTx, DbTxMut},
};

use crate::{
    ClusterNode, ClusteredCursor, CursorSharding, ReadSnapshot, ShardedReads, ShardedTable,
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

    fn cursor_sharding<T: Table>(&self) -> CursorSharding {
        let Some(table) = ShardedTable::from_table_name(T::NAME) else {
            return CursorSharding::PassThrough;
        };
        match &self.sharding {
            TxSharding::PassThrough => CursorSharding::PassThrough,
            TxSharding::Reads(snapshot) => {
                CursorSharding::Reads(ShardedReads::new(Arc::clone(snapshot), table))
            }
            TxSharding::Writes(node) => CursorSharding::Writes { node: Arc::clone(node), table },
        }
    }

    /// Reads the encoded `key` of a sharded table from an owner when this transaction reads at a
    /// snapshot and this node does not hold the key; `None` means "read local MDBX".
    fn remote_value<T: Table>(
        &self,
        key: &[u8],
    ) -> Result<Option<Option<T::Value>>, DatabaseError> {
        let TxSharding::Reads(snapshot) = &self.sharding else { return Ok(None) };
        let Some(table) = ShardedTable::from_table_name(T::NAME) else { return Ok(None) };
        if snapshot.node().holds(table, key)? {
            return Ok(None);
        }
        let value = snapshot.value(table, key)?;
        Ok(Some(value.map(|value| T::Value::decompress(&value)).transpose()?))
    }

    fn accepts_write<T: Table>(&self, key: &T::Key) -> Result<bool, DatabaseError> {
        self.cursor_sharding::<T>().accepts_write::<T>(key)
    }
}

impl<K: TransactionKind> DbTx for ClusteredTx<K> {
    type Cursor<T: Table> = ClusteredCursor<K, T>;
    type DupCursor<T: DupSort> = ClusteredCursor<K, T>;

    fn get<T: Table>(&self, key: T::Key) -> Result<Option<T::Value>, DatabaseError> {
        if matches!(self.sharding, TxSharding::Reads(_))
            && let Some(value) = self.remote_value::<T>(&key.clone().encode().into_vec())?
        {
            return Ok(value);
        }
        self.inner.get::<T>(key)
    }

    fn get_by_encoded_key<T: Table>(
        &self,
        key: &<T::Key as Encode>::Encoded,
    ) -> Result<Option<T::Value>, DatabaseError> {
        if let Some(value) = self.remote_value::<T>(key.as_ref())? {
            return Ok(value);
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
        let sharding = self.cursor_sharding::<T>();
        self.inner.cursor_read::<T>().map(|cursor| ClusteredCursor::new(cursor, sharding))
    }

    fn cursor_dup_read<T: DupSort>(&self) -> Result<Self::DupCursor<T>, DatabaseError> {
        let sharding = self.cursor_sharding::<T>();
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
        if !self.accepts_write::<T>(&key)? {
            return Ok(());
        }
        DbTxMut::put::<T>(&self.inner, key, value)
    }

    fn append<T: Table>(&self, key: T::Key, value: T::Value) -> Result<(), DatabaseError> {
        if !self.accepts_write::<T>(&key)? {
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
        let sharding = self.cursor_sharding::<T>();
        self.inner.cursor_write::<T>().map(|cursor| ClusteredCursor::new(cursor, sharding))
    }

    fn cursor_dup_write<T: DupSort>(&self) -> Result<Self::DupCursorMut<T>, DatabaseError> {
        let sharding = self.cursor_sharding::<T>();
        self.inner.cursor_dup_write::<T>().map(|cursor| ClusteredCursor::new(cursor, sharding))
    }
}

impl TableImporter for ClusteredTx<RW> {}
