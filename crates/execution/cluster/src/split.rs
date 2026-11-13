//! A raw duplicate cursor over the storage tables, reading the ranges of split contracts on
//! encoded keys and subkeys.

use reth_db::{
    DatabaseError,
    mdbx::{Cursor, Transaction, TransactionKind, ffi::MDBX_dbi, tx::Tx},
    table::Table,
    tables,
};

use crate::ShardedTable;

/// A second, raw MDBX cursor of a duplicate-key sharded table, kept next to a read-only cursor so
/// that duplicates of split contracts can be sought by subkey whatever the typed table view of
/// that cursor is. The MDBX cursor is opened when a split contract is first read, so a cursor that
/// reads only other contracts never opens it.
#[derive(Debug)]
pub struct SplitCursor<K: TransactionKind> {
    tx: Transaction<K>,
    table: MDBX_dbi,
    cursor: Option<Cursor<K>>,
}

impl<K: TransactionKind> SplitCursor<K> {
    /// A split cursor over `table` in `tx`, or `None` when the table has no duplicates.
    pub fn new(tx: &Tx<K>, table: ShardedTable) -> Result<Option<Self>, DatabaseError> {
        let name = match table {
            ShardedTable::HashedStorages => tables::HashedStorages::NAME,
            ShardedTable::StoragesTrie => tables::PackedStoragesTrie::NAME,
            ShardedTable::HashedAccounts | ShardedTable::AccountsTrie | ShardedTable::Bytecodes => {
                return Ok(None);
            }
        };
        Ok(Some(Self { tx: tx.inner().clone(), table: tx.get_dbi_raw(name)?, cursor: None }))
    }

    /// The encoded value of the first duplicate of `key` with subkey `>= subkey`.
    pub fn seek_by_key_subkey(
        &mut self,
        key: &[u8],
        subkey: &[u8],
    ) -> Result<Option<Vec<u8>>, DatabaseError> {
        self.cursor()?
            .get_both_range(key, subkey)
            .map_err(|error| DatabaseError::Read(error.into()))
    }

    /// The encoded value of the next duplicate of the current key.
    pub fn next_dup(&mut self) -> Result<Option<Vec<u8>>, DatabaseError> {
        let next = self.cursor()?.next_dup::<(), Vec<u8>>();
        Ok(next.map_err(|error| DatabaseError::Read(error.into()))?.map(|(_, value)| value))
    }

    fn cursor(&mut self) -> Result<&mut Cursor<K>, DatabaseError> {
        let cursor = match self.cursor.take() {
            Some(cursor) => cursor,
            None => self
                .tx
                .cursor(self.table)
                .map_err(|error| DatabaseError::InitCursor(error.into()))?,
        };
        Ok(self.cursor.insert(cursor))
    }
}
