//! A raw duplicate cursor over the storage tables, reading the ranges of split contracts on
//! encoded keys and subkeys.

use reth_db::{
    DatabaseError,
    cursor::DbDupCursorRO,
    mdbx::{TransactionKind, cursor::Cursor, tx::Tx},
    table::DupSort,
    tables::{self, RawDupSort, RawKey, RawValue},
    transaction::DbTx,
};

use crate::ShardedTable;

/// A second, raw MDBX cursor of a duplicate-key sharded table, opened next to a read-only
/// cursor so that duplicates of split contracts can be sought by subkey whatever the typed table
/// view of that cursor is.
#[derive(Debug)]
pub enum SplitCursor<K: TransactionKind> {
    /// Over `HashedStorages`.
    HashedStorages(Cursor<K, RawDupSort<tables::HashedStorages>>),
    /// Over `PackedStoragesTrie`.
    StoragesTrie(Cursor<K, RawDupSort<tables::PackedStoragesTrie>>),
}

impl<K: TransactionKind> SplitCursor<K> {
    /// Opens a cursor over `table` in `tx`, or `None` when the table has no duplicates.
    pub fn open(tx: &Tx<K>, table: ShardedTable) -> Result<Option<Self>, DatabaseError> {
        Ok(match table {
            ShardedTable::HashedStorages => Some(Self::HashedStorages(tx.cursor_dup_read()?)),
            ShardedTable::StoragesTrie => Some(Self::StoragesTrie(tx.cursor_dup_read()?)),
            ShardedTable::HashedAccounts | ShardedTable::AccountsTrie | ShardedTable::Bytecodes => {
                None
            }
        })
    }

    /// The encoded value of the first duplicate of `key` with subkey `>= subkey`.
    pub fn seek_by_key_subkey(
        &mut self,
        key: &[u8],
        subkey: &[u8],
    ) -> Result<Option<Vec<u8>>, DatabaseError> {
        match self {
            Self::HashedStorages(cursor) => Self::seek(cursor, key, subkey),
            Self::StoragesTrie(cursor) => Self::seek(cursor, key, subkey),
        }
    }

    /// The encoded value of the next duplicate of the current key.
    pub fn next_dup(&mut self) -> Result<Option<Vec<u8>>, DatabaseError> {
        match self {
            Self::HashedStorages(cursor) => Self::next(cursor),
            Self::StoragesTrie(cursor) => Self::next(cursor),
        }
    }

    fn seek<T: DupSort>(
        cursor: &mut Cursor<K, RawDupSort<T>>,
        key: &[u8],
        subkey: &[u8],
    ) -> Result<Option<Vec<u8>>, DatabaseError> {
        let found = cursor.seek_by_key_subkey(
            RawKey::from_vec(key.to_vec()),
            RawKey::from_vec(subkey.to_vec()),
        )?;
        Ok(found.map(RawValue::into_value))
    }

    fn next<T: DupSort>(
        cursor: &mut Cursor<K, RawDupSort<T>>,
    ) -> Result<Option<Vec<u8>>, DatabaseError> {
        Ok(cursor.next_dup()?.map(|(_, value)| value.into_value()))
    }
}
