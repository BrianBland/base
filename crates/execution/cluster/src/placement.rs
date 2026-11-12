//! Shards, sharded tables, and rendezvous placement of shards on storage nodes.

use alloy_primitives::keccak256;
use serde::{Deserialize, Serialize};

/// Identifier of a cluster node, unique within the cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct NodeId(pub u64);

/// One of the 256 shards: the value of the first byte of a hash.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Shard(pub u8);

impl Shard {
    /// Number of shards.
    pub const COUNT: usize = 256;

    /// Every shard, in ascending order.
    pub fn all() -> impl Iterator<Item = Self> {
        (0..=u8::MAX).map(Self)
    }

    /// Rendezvous score of `node` for this shard: `keccak(shard || node_id as 8 big-endian bytes)`.
    pub fn score(self, node: NodeId) -> [u8; 32] {
        let mut preimage = [0u8; 9];
        preimage[0] = self.0;
        preimage[1..].copy_from_slice(&node.0.to_be_bytes());
        keccak256(preimage).0
    }
}

/// Where a key of a sharded table lives.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyPlacement {
    /// Stored only by the owners of this shard.
    Shard(Shard),
    /// Stored and written on every node: account-trie paths shorter than two nibbles.
    Replicated,
}

/// The MDBX tables whose contents are sharded. The trie tables are their packed (storage v2)
/// views, `PackedAccountsTrie` and `PackedStoragesTrie`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ShardedTable {
    /// `HashedAccounts`: hashed address to account.
    HashedAccounts,
    /// `HashedStorages`: hashed address to duplicate entries keyed by hashed slot.
    HashedStorages,
    /// `PackedAccountsTrie`: packed trie path to branch node.
    AccountsTrie,
    /// `PackedStoragesTrie`: hashed address to duplicate entries keyed by packed trie path.
    StoragesTrie,
    /// `Bytecodes`: code hash to bytecode.
    Bytecodes,
}

impl ShardedTable {
    /// Length of an encoded hash key (`B256`).
    pub const HASH_KEY_LEN: usize = 32;
    /// Length of an encoded packed trie path: 32 bytes of packed nibbles and a nibble count.
    pub const PACKED_PATH_LEN: usize = 33;
    /// Largest nibble count of a trie path.
    pub const MAX_NIBBLES: u8 = 64;
    /// Paths with fewer nibbles than this are replicated rather than sharded.
    pub const SHARDED_PATH_MIN_NIBBLES: u8 = 2;

    /// Returns where the encoded `key` of this table lives, or `None` when the key is not a valid
    /// encoded key of this table.
    pub fn placement(self, key: &[u8]) -> Option<KeyPlacement> {
        match self {
            Self::HashedAccounts | Self::HashedStorages | Self::StoragesTrie | Self::Bytecodes => {
                (key.len() == Self::HASH_KEY_LEN).then(|| KeyPlacement::Shard(Shard(key[0])))
            }
            Self::AccountsTrie => {
                let nibbles = Self::packed_path_nibbles(key)?;
                Some(if nibbles < Self::SHARDED_PATH_MIN_NIBBLES {
                    KeyPlacement::Replicated
                } else {
                    KeyPlacement::Shard(Shard(key[0]))
                })
            }
        }
    }

    /// Returns the nibble count of an encoded packed trie path, or `None` when `path` is not the
    /// canonical encoding of a path: wrong length, too many nibbles, or nonzero padding.
    pub fn packed_path_nibbles(path: &[u8]) -> Option<u8> {
        if path.len() != Self::PACKED_PATH_LEN {
            return None;
        }
        let (packed, nibbles) = path.split_at(Self::PACKED_PATH_LEN - 1);
        let nibbles = nibbles[0];
        if nibbles > Self::MAX_NIBBLES {
            return None;
        }
        let used = usize::from(nibbles).div_ceil(2);
        let odd_padding = if nibbles % 2 == 1 { packed[used - 1] & 0x0f } else { 0 };
        (odd_padding == 0 && packed[used..].iter().all(|byte| *byte == 0)).then_some(nibbles)
    }
}

/// The owners of every shard: for each shard, the `R` storage nodes with the highest rendezvous
/// score, highest first.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardMap {
    owners: Vec<Vec<NodeId>>,
}

impl ShardMap {
    /// Places every shard on the `replication` highest-scoring of `storage_nodes`.
    pub fn new(storage_nodes: &[NodeId], replication: usize) -> Self {
        let owners = Shard::all()
            .map(|shard| {
                let mut ranked = storage_nodes.to_vec();
                ranked.sort_by_cached_key(|node| std::cmp::Reverse(shard.score(*node)));
                ranked.truncate(replication);
                ranked
            })
            .collect();
        Self { owners }
    }

    /// Owners of `shard`, highest rendezvous score first.
    pub fn owners(&self, shard: Shard) -> &[NodeId] {
        &self.owners[usize::from(shard.0)]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn packed_path(first_byte: u8, nibbles: u8) -> Vec<u8> {
        let mut path = vec![0u8; ShardedTable::PACKED_PATH_LEN];
        path[0] = first_byte;
        path[32] = nibbles;
        path
    }

    #[test]
    fn hash_keyed_tables_shard_by_first_byte() {
        let mut key = [0u8; 32];
        key[0] = 0xab;
        for table in [
            ShardedTable::HashedAccounts,
            ShardedTable::HashedStorages,
            ShardedTable::StoragesTrie,
            ShardedTable::Bytecodes,
        ] {
            assert_eq!(table.placement(&key), Some(KeyPlacement::Shard(Shard(0xab))));
            assert_eq!(table.placement(&key[..31]), None);
        }
    }

    #[test]
    fn account_trie_paths_shorter_than_two_nibbles_are_replicated() {
        let table = ShardedTable::AccountsTrie;
        assert_eq!(table.placement(&packed_path(0, 0)), Some(KeyPlacement::Replicated));
        assert_eq!(table.placement(&packed_path(0xa0, 1)), Some(KeyPlacement::Replicated));
        assert_eq!(table.placement(&packed_path(0xa3, 2)), Some(KeyPlacement::Shard(Shard(0xa3))));
        assert_eq!(table.placement(&packed_path(0xa3, 64)), Some(KeyPlacement::Shard(Shard(0xa3))));
        assert_eq!(table.placement(&packed_path(0xa3, 65)), None);
        assert_eq!(table.placement(&packed_path(0xa3, 1)), None, "padding nibble must be zero");
        assert_eq!(table.placement(&packed_path(0xa0, 0)), None, "padding byte must be zero");
        assert_eq!(table.placement(&[0xa3; 32]), None);
    }

    #[test]
    fn every_shard_has_r_distinct_owners() {
        let nodes: Vec<_> = (1..=5).map(NodeId).collect();
        let map = ShardMap::new(&nodes, 2);
        for shard in Shard::all() {
            let owners = map.owners(shard);
            assert_eq!(owners.len(), 2);
            assert_ne!(owners[0], owners[1]);
        }
    }

    #[test]
    fn removing_a_node_moves_only_its_shards() {
        let nodes: Vec<_> = (1..=6).map(NodeId).collect();
        let before = ShardMap::new(&nodes, 3);
        let removed = NodeId(4);
        let remaining: Vec<_> = nodes.iter().copied().filter(|node| *node != removed).collect();
        let after = ShardMap::new(&remaining, 3);

        let mut moved = 0;
        for shard in Shard::all() {
            if before.owners(shard).contains(&removed) {
                moved += 1;
                let kept: Vec<_> =
                    before.owners(shard).iter().copied().filter(|n| *n != removed).collect();
                assert!(kept.iter().all(|node| after.owners(shard).contains(node)));
            } else {
                assert_eq!(before.owners(shard), after.owners(shard), "shard {shard:?}");
            }
        }
        assert!(moved > 0);
    }
}
