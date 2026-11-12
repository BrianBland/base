use reth_db::{
    DatabaseError,
    mdbx::{RW, TransactionKind, tx::Tx},
    table::{DupSort, Encode, Table, TableImporter},
    transaction::{DbTx, DbTxMut},
};

use crate::ClusteredCursor;

/// A [`ClusteredDatabase`](crate::ClusteredDatabase) transaction, read-only (`RO`) or read-write
/// (`RW`).
///
/// Every point read and write passes through this type with its table and key known; cursors are
/// handed out as [`ClusteredCursor`]s for the requested table.
#[derive(Debug)]
pub struct ClusteredTx<K: TransactionKind> {
    inner: Tx<K>,
}

impl<K: TransactionKind> ClusteredTx<K> {
    /// Wraps an MDBX transaction.
    pub const fn new(inner: Tx<K>) -> Self {
        Self { inner }
    }
}

impl<K: TransactionKind> DbTx for ClusteredTx<K> {
    type Cursor<T: Table> = ClusteredCursor<K, T>;
    type DupCursor<T: DupSort> = ClusteredCursor<K, T>;

    fn get<T: Table>(&self, key: T::Key) -> Result<Option<T::Value>, DatabaseError> {
        self.inner.get::<T>(key)
    }

    fn get_by_encoded_key<T: Table>(
        &self,
        key: &<T::Key as Encode>::Encoded,
    ) -> Result<Option<T::Value>, DatabaseError> {
        self.inner.get_by_encoded_key::<T>(key)
    }

    fn commit(self) -> Result<(), DatabaseError> {
        self.inner.commit()
    }

    fn abort(self) {
        self.inner.abort()
    }

    fn cursor_read<T: Table>(&self) -> Result<Self::Cursor<T>, DatabaseError> {
        self.inner.cursor_read::<T>().map(ClusteredCursor::new)
    }

    fn cursor_dup_read<T: DupSort>(&self) -> Result<Self::DupCursor<T>, DatabaseError> {
        self.inner.cursor_dup_read::<T>().map(ClusteredCursor::new)
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
        DbTxMut::put::<T>(&self.inner, key, value)
    }

    fn append<T: Table>(&self, key: T::Key, value: T::Value) -> Result<(), DatabaseError> {
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
        self.inner.cursor_write::<T>().map(ClusteredCursor::new)
    }

    fn cursor_dup_write<T: DupSort>(&self) -> Result<Self::DupCursorMut<T>, DatabaseError> {
        self.inner.cursor_dup_write::<T>().map(ClusteredCursor::new)
    }
}

impl TableImporter for ClusteredTx<RW> {}
