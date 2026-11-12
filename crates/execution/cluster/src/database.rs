use std::{net::TcpListener, path::PathBuf, sync::Arc};

use metrics::Label;
use reth_db::{
    Database, DatabaseEnv, DatabaseError,
    database_metrics::DatabaseMetrics,
    mdbx::{RO, RW},
};

use crate::{
    ClientOptions, Cluster, ClusterNode, ClusterStartError, ClusteredTx, ReadSnapshot, TxSharding,
};

/// The node's state database: the MDBX [`DatabaseEnv`] behind the sharding layer.
///
/// Every transaction it opens is a [`ClusteredTx`], whose cursors are [`ClusteredCursor`]s, so
/// each read and write passes through this crate before reaching MDBX. A pass-through database
/// forwards every operation unchanged; a clustered one follows the crate's read and write rules.
///
/// [`ClusteredCursor`]: crate::ClusteredCursor
#[derive(Debug, Clone)]
pub struct ClusteredDatabase {
    inner: DatabaseEnv,
    cluster: Option<Arc<ClusterNode>>,
}

impl ClusteredDatabase {
    /// Wraps an opened MDBX environment as a pass-through database.
    pub const fn new(inner: DatabaseEnv) -> Self {
        Self { inner, cluster: None }
    }

    /// Wraps an opened MDBX environment as a node of `cluster`, serving this node's shards to
    /// peers on `listener`.
    pub fn clustered(
        inner: DatabaseEnv,
        cluster: Arc<Cluster>,
        listener: TcpListener,
        options: ClientOptions,
    ) -> Result<Self, ClusterStartError> {
        let node = ClusterNode::start(inner.clone(), cluster, listener, options)?;
        Ok(Self { inner, cluster: Some(Arc::new(node)) })
    }

    /// This node's side of the cluster, or `None` for a pass-through database.
    pub const fn cluster(&self) -> Option<&Arc<ClusterNode>> {
        self.cluster.as_ref()
    }
}

impl Database for ClusteredDatabase {
    type TX = ClusteredTx<RO>;
    type TXMut = ClusteredTx<RW>;

    fn tx(&self) -> Result<Self::TX, DatabaseError> {
        let tx = self.inner.tx()?;
        let sharding = match &self.cluster {
            None => TxSharding::PassThrough,
            Some(node) => {
                let tip = ClusterNode::finish_checkpoint(&tx)?;
                TxSharding::Reads(Arc::new(ReadSnapshot::new(Arc::clone(node), tip)))
            }
        };
        Ok(ClusteredTx::new(tx, sharding))
    }

    fn tx_mut(&self) -> Result<Self::TXMut, DatabaseError> {
        let sharding = self
            .cluster
            .as_ref()
            .map_or(TxSharding::PassThrough, |node| TxSharding::Writes(Arc::clone(node)));
        Ok(ClusteredTx::new(self.inner.tx_mut()?, sharding))
    }

    fn path(&self) -> PathBuf {
        self.inner.path()
    }

    fn oldest_reader_txnid(&self) -> Option<u64> {
        self.inner.oldest_reader_txnid()
    }

    fn last_txnid(&self) -> Option<u64> {
        self.inner.last_txnid()
    }
}

impl DatabaseMetrics for ClusteredDatabase {
    fn report_metrics(&self) {
        self.inner.report_metrics()
    }

    fn gauge_metrics(&self) -> Vec<(&'static str, f64, Vec<Label>)> {
        self.inner.gauge_metrics()
    }

    fn counter_metrics(&self) -> Vec<(&'static str, u64, Vec<Label>)> {
        self.inner.counter_metrics()
    }

    fn histogram_metrics(&self) -> Vec<(&'static str, f64, Vec<Label>)> {
        self.inner.histogram_metrics()
    }
}

#[cfg(test)]
mod tests {
    use alloy_primitives::{B256, U256};
    use reth_db::{
        ClientVersion,
        cursor::{DbCursorRO, DbCursorRW, DbDupCursorRO},
        init_db,
        mdbx::DatabaseArguments,
        tables::{HashedAccounts, HashedStorages},
        transaction::{DbTx, DbTxMut},
    };
    use reth_primitives_traits::{Account, StorageEntry};
    use tempfile::TempDir;

    use super::*;

    /// What a caller sees after running [`exercise`] against a database.
    #[derive(Debug, PartialEq)]
    struct Observed {
        account_one: Option<Account>,
        deleted_two: bool,
        account_two_after_delete: Option<Account>,
        accounts_walked: Vec<(B256, Account)>,
        storage_seeked: Option<StorageEntry>,
        storage_next_dup: Option<(B256, StorageEntry)>,
    }

    fn hashed(byte: u8) -> B256 {
        B256::with_last_byte(byte)
    }

    fn account(nonce: u64) -> Account {
        Account { nonce, balance: U256::from(nonce), bytecode_hash: None }
    }

    fn slot(byte: u8) -> StorageEntry {
        StorageEntry { key: hashed(byte), value: U256::from(byte) }
    }

    fn open_mdbx() -> (DatabaseEnv, TempDir) {
        let dir = TempDir::new().expect("temp dir");
        let env = init_db(dir.path(), DatabaseArguments::new(ClientVersion::default()))
            .expect("open mdbx");
        (env, dir)
    }

    fn exercise(db: &impl Database) -> Result<Observed, DatabaseError> {
        let tx = db.tx_mut()?;
        for byte in 1..=3 {
            tx.put::<HashedAccounts>(hashed(byte), account(byte.into()))?;
        }
        let mut storage = tx.cursor_dup_write::<HashedStorages>()?;
        for byte in [1, 3, 5] {
            storage.upsert(hashed(1), &slot(byte))?;
        }
        drop(storage);
        tx.commit()?;

        let tx = db.tx_mut()?;
        let deleted_two = tx.delete::<HashedAccounts>(hashed(2), None)?;
        tx.commit()?;

        let tx = db.tx()?;
        let accounts_walked =
            tx.cursor_read::<HashedAccounts>()?.walk(None)?.collect::<Result<Vec<_>, _>>()?;
        let mut storage = tx.cursor_dup_read::<HashedStorages>()?;
        let storage_seeked = storage.seek_by_key_subkey(hashed(1), hashed(2))?;
        let storage_next_dup = storage.next_dup()?;
        Ok(Observed {
            account_one: tx.get::<HashedAccounts>(hashed(1))?,
            deleted_two,
            account_two_after_delete: tx.get::<HashedAccounts>(hashed(2))?,
            accounts_walked,
            storage_seeked,
            storage_next_dup,
        })
    }

    #[test]
    fn reads_and_writes_match_raw_mdbx() {
        let (raw, _raw_dir) = open_mdbx();
        let (wrapped, _wrapped_dir) = open_mdbx();
        let clustered = ClusteredDatabase::new(wrapped);

        let through_mdbx = exercise(&raw).expect("raw mdbx");
        let through_cluster = exercise(&clustered).expect("clustered database");

        assert_eq!(through_cluster, through_mdbx);
        assert_eq!(
            through_cluster,
            Observed {
                account_one: Some(account(1)),
                deleted_two: true,
                account_two_after_delete: None,
                accounts_walked: vec![(hashed(1), account(1)), (hashed(3), account(3))],
                storage_seeked: Some(slot(3)),
                storage_next_dup: Some((hashed(1), slot(5))),
            }
        );
    }
}
