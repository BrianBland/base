//! The snapshot a read-only transaction of a clustered database reads at, and its remote reads.

use std::{
    sync::{Arc, OnceLock},
    time::Instant,
};

use alloy_primitives::B256;
use reth_db::DatabaseError;

use crate::{ClientError, ClusterNode, RemoteReadKind, SnapshotId};

/// The persisted tip `P` of one read-only transaction, and exact reads of other nodes' rows as of
/// it. Shared by the transaction and its cursors.
#[derive(Debug)]
pub struct ReadSnapshot {
    node: Arc<ClusterNode>,
    number: u64,
    hash: OnceLock<Option<B256>>,
}

impl ReadSnapshot {
    /// A snapshot at block `number`, the `Finish` checkpoint of the transaction.
    pub const fn new(node: Arc<ClusterNode>, number: u64) -> Self {
        Self { node, number, hash: OnceLock::new() }
    }

    /// Block number of `P`.
    pub const fn number(&self) -> u64 {
        self.number
    }

    /// This node.
    pub const fn node(&self) -> &Arc<ClusterNode> {
        &self.node
    }

    /// `P` with its hash, looked up once per transaction when the node knows it.
    fn id(&self) -> SnapshotId {
        let hash = *self.hash.get_or_init(|| self.node.owner_store().block_hash(self.number));
        SnapshotId { number: self.number, hash }
    }

    fn remote<R>(
        &self,
        kind: RemoteReadKind,
        read: impl FnOnce(SnapshotId) -> Result<R, ClientError>,
    ) -> Result<R, DatabaseError> {
        let snapshot = self.id();
        let started = Instant::now();
        let result = read(snapshot);
        self.node.client().remote_read_stats().record(kind, started.elapsed(), result.is_err());
        result.map_err(|error| {
            DatabaseError::Other(format!(
                "cluster {kind:?} read at block {} failed: {error}",
                self.number
            ))
        })
    }

    /// The encoded account at hashed `address` at `P`, read from an owner.
    pub fn account(&self, address: B256) -> Result<Option<Vec<u8>>, DatabaseError> {
        self.remote(RemoteReadKind::Account, |snapshot| {
            self.node.client().account(snapshot, address)
        })
    }

    /// The encoded storage entry of exactly hashed `slot` of hashed `address` at `P`, read from
    /// an owner of the row.
    pub fn storage(&self, address: B256, slot: B256) -> Result<Option<Vec<u8>>, DatabaseError> {
        self.remote(RemoteReadKind::Storage, |snapshot| {
            self.node.client().storage(snapshot, address, slot)
        })
    }

    /// Whether hashed `address` has any storage at `P`, read from an owner.
    pub fn has_storage(&self, address: B256) -> Result<bool, DatabaseError> {
        self.remote(RemoteReadKind::StorageProbe, |snapshot| {
            self.node.client().has_storage(snapshot, address)
        })
    }

    /// The encoded bytecode with hash `key`, read from an owner.
    pub fn bytecode(&self, key: &[u8]) -> Result<Option<Vec<u8>>, DatabaseError> {
        self.remote(RemoteReadKind::Bytecode, |snapshot| self.node.client().bytecode(snapshot, key))
    }
}
