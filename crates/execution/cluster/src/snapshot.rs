//! The snapshot a read-only transaction of a clustered database reads at, and its remote reads.

use std::{
    sync::{Arc, OnceLock},
    time::Instant,
};

use alloy_primitives::B256;
use reth_db::DatabaseError;

use crate::{
    ClientError, ClusterNode, DuplicateValues, RemoteReadKind, ShardEntries, ShardedTable,
    SnapshotId,
};

/// The persisted tip `P` of one read-only transaction, and reads of other nodes' shards as of
/// it. Shared by the transaction and its cursors.
#[derive(Debug)]
pub struct ReadSnapshot {
    node: Arc<ClusterNode>,
    number: u64,
    hash: OnceLock<Option<B256>>,
}

impl ReadSnapshot {
    /// Entries asked for per remote `SeekGe` or `SeekDupGe`.
    pub const PAGE_LEN: u32 = 64;

    /// A snapshot at block `number`, the `Finish` checkpoint of the transaction.
    pub const fn new(node: Arc<ClusterNode>, number: u64) -> Self {
        Self { node, number, hash: OnceLock::new() }
    }

    /// Block number of `P`.
    pub const fn number(&self) -> u64 {
        self.number
    }

    /// This node.
    pub fn node(&self) -> &ClusterNode {
        &self.node
    }

    /// `P` with its hash, looked up once per transaction when the node knows it.
    fn id(&self) -> SnapshotId {
        let hash = *self.hash.get_or_init(|| self.node.block_hash(self.number));
        SnapshotId { number: self.number, hash }
    }

    fn remote<R>(
        &self,
        table: ShardedTable,
        kind: RemoteReadKind,
        read: impl FnOnce(SnapshotId) -> Result<R, ClientError>,
    ) -> Result<R, DatabaseError> {
        let snapshot = self.id();
        let started = Instant::now();
        let result = read(snapshot);
        self.node.remote_read_stats().record(kind, started.elapsed(), result.is_err());
        result.map_err(|error| {
            DatabaseError::Other(format!(
                "cluster read of {table:?} at block {} failed: {error}",
                self.number
            ))
        })
    }

    /// The encoded value of `key` in `table` at `P`, read from an owner.
    pub fn get(&self, table: ShardedTable, key: &[u8]) -> Result<Option<Vec<u8>>, DatabaseError> {
        self.remote(table, RemoteReadKind::Get, |snapshot| {
            self.node.client().get(snapshot, table, key)
        })
    }

    /// A page of `key`'s shard with keys `>= key` at `P`, read from an owner.
    pub fn seek_ge(&self, table: ShardedTable, key: &[u8]) -> Result<ShardEntries, DatabaseError> {
        self.remote(table, RemoteReadKind::Seek, |snapshot| {
            self.node.client().seek_ge(snapshot, table, key, Self::PAGE_LEN)
        })
    }

    /// The encoded value of `key` in `table` at `P`, read from an owner; for a duplicate-key
    /// table, the key's first duplicate.
    pub fn value(&self, table: ShardedTable, key: &[u8]) -> Result<Option<Vec<u8>>, DatabaseError> {
        if table.is_duplicate() {
            let first = self.seek_dup_ge(table, key, &table.first_subkey(), 1)?;
            return Ok(first.values.into_iter().next());
        }
        self.get(table, key)
    }

    /// Up to `limit` duplicates of `key` with subkeys `>= subkey` at `P`, read from an owner.
    pub fn seek_dup_ge(
        &self,
        table: ShardedTable,
        key: &[u8],
        subkey: &[u8],
        limit: u32,
    ) -> Result<DuplicateValues, DatabaseError> {
        self.remote(table, RemoteReadKind::DuplicateSeek, |snapshot| {
            self.node.client().seek_dup_ge(snapshot, table, key, subkey, limit)
        })
    }
}
