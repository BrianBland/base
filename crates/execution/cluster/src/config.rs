//! Cluster configuration file and the validated cluster view built from it.

use std::{collections::HashSet, net::SocketAddr, path::Path};

use alloy_primitives::{B256, Bytes, keccak256, map::B256Set};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{KeyPlacement, NodeId, Shard, ShardMap, ShardedTable};

/// One node of the cluster as listed in the configuration file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeConfig {
    /// Node identifier.
    pub id: NodeId,
    /// Address the node's owner server listens on.
    pub address: SocketAddr,
    /// Whether the node stores shards. A node with `storage = false` owns nothing.
    #[serde(default = "NodeConfig::default_storage")]
    pub storage: bool,
}

impl NodeConfig {
    const fn default_storage() -> bool {
        true
    }
}

/// The cluster configuration file, in TOML.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterConfig {
    /// The node this configuration is loaded on.
    pub node_id: NodeId,
    /// Number of storage nodes that own each shard (`R`).
    #[serde(default = "ClusterConfig::default_replication")]
    pub replication: usize,
    /// Number of most recent blocks an owner can serve reads as of (`W`).
    #[serde(default = "ClusterConfig::default_retention_blocks")]
    pub retention_blocks: u64,
    /// Every node of the cluster, including this one.
    pub nodes: Vec<NodeConfig>,
    /// The nodes this node asks, in order, for the execution results of new payloads instead of
    /// executing them (apply mode). Empty: this node executes.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub apply_from: Vec<NodeId>,
    /// Large contracts whose storage is split over all shards by subkey: 20-byte addresses,
    /// hashed at load, or 32-byte hashed addresses. Must be the same on every node.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub split_storage: Vec<Bytes>,
}

impl ClusterConfig {
    /// Length of a contract address in `split_storage`, hashed at load.
    pub const ADDRESS_LEN: usize = 20;

    /// Default replication factor: one owner of each shard may be down.
    pub const DEFAULT_REPLICATION: usize = 2;

    /// Smallest and default retention window `W`: Base's persistence backpressure threshold.
    pub const MIN_RETENTION_BLOCKS: u64 = 160;

    const fn default_replication() -> usize {
        Self::DEFAULT_REPLICATION
    }

    const fn default_retention_blocks() -> u64 {
        Self::MIN_RETENTION_BLOCKS
    }
}

/// Why a cluster configuration was rejected.
#[derive(Debug, Error)]
pub enum ConfigError {
    /// The file could not be read.
    #[error("failed to read cluster config: {0}")]
    Read(#[from] std::io::Error),
    /// The file is not a valid configuration document.
    #[error("failed to parse cluster config: {0}")]
    Parse(#[from] toml::de::Error),
    /// Two nodes share an id.
    #[error("duplicate node id {0:?}")]
    DuplicateNode(NodeId),
    /// `node_id` is not among the listed nodes.
    #[error("this node {0:?} is not listed in the cluster")]
    UnknownSelf(NodeId),
    /// The replication factor is 0 or larger than the number of storage nodes.
    #[error("replication {replication} must be between 1 and the {storage_nodes} storage nodes")]
    InvalidReplication {
        /// Configured replication factor.
        replication: usize,
        /// Number of storage nodes.
        storage_nodes: usize,
    },
    /// The retention window is below [`ClusterConfig::MIN_RETENTION_BLOCKS`].
    #[error("retention_blocks {0} is below the minimum {min}", min = ClusterConfig::MIN_RETENTION_BLOCKS)]
    RetentionTooShort(u64),
    /// `apply_from` lists a node that is not in the cluster.
    #[error("apply_from lists {0:?}, which is not a cluster node")]
    UnknownApplySource(NodeId),
    /// `apply_from` lists this node.
    #[error("apply_from lists this node {0:?}")]
    ApplyFromSelf(NodeId),
    /// A `split_storage` entry is neither a 20-byte address nor a 32-byte hashed address.
    #[error("split_storage entry {0} is neither a 20-byte address nor a 32-byte hashed address")]
    InvalidSplitStorage(Bytes),
}

/// A validated cluster configuration with the shard owners and split contracts precomputed.
#[derive(Debug, Clone)]
pub struct Cluster {
    config: ClusterConfig,
    shard_map: ShardMap,
    split_storage: B256Set,
}

impl Cluster {
    /// Loads and validates the configuration file at `path`.
    pub fn load(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let contents = std::fs::read_to_string(path)?;
        Self::new(toml::from_str(&contents)?)
    }

    /// Validates `config` and places every shard.
    pub fn new(config: ClusterConfig) -> Result<Self, ConfigError> {
        let mut seen = HashSet::new();
        if let Some(duplicate) = config.nodes.iter().find(|node| !seen.insert(node.id)) {
            return Err(ConfigError::DuplicateNode(duplicate.id));
        }
        if !seen.contains(&config.node_id) {
            return Err(ConfigError::UnknownSelf(config.node_id));
        }
        let storage_nodes: Vec<_> =
            config.nodes.iter().filter(|node| node.storage).map(|node| node.id).collect();
        if config.replication == 0 || config.replication > storage_nodes.len() {
            return Err(ConfigError::InvalidReplication {
                replication: config.replication,
                storage_nodes: storage_nodes.len(),
            });
        }
        if config.retention_blocks < ClusterConfig::MIN_RETENTION_BLOCKS {
            return Err(ConfigError::RetentionTooShort(config.retention_blocks));
        }
        if let Some(source) = config.apply_from.iter().find(|id| !seen.contains(id)) {
            return Err(ConfigError::UnknownApplySource(*source));
        }
        if config.apply_from.contains(&config.node_id) {
            return Err(ConfigError::ApplyFromSelf(config.node_id));
        }
        let split_storage = config
            .split_storage
            .iter()
            .map(|entry| match entry.len() {
                ClusterConfig::ADDRESS_LEN => Ok(keccak256(entry)),
                ShardedTable::HASH_KEY_LEN => Ok(B256::from_slice(entry)),
                _ => Err(ConfigError::InvalidSplitStorage(entry.clone())),
            })
            .collect::<Result<_, _>>()?;
        let shard_map = ShardMap::new(&storage_nodes, config.replication);
        Ok(Self { config, shard_map, split_storage })
    }

    /// The node this cluster view belongs to.
    pub const fn node_id(&self) -> NodeId {
        self.config.node_id
    }

    /// The configuration this view was built from.
    pub const fn config(&self) -> &ClusterConfig {
        &self.config
    }

    /// The nodes this node asks for execution results, in order; empty when it executes.
    pub fn apply_from(&self) -> &[NodeId] {
        &self.config.apply_from
    }

    /// Owners of `shard`, highest rendezvous score first.
    pub fn owners(&self, shard: Shard) -> &[NodeId] {
        self.shard_map.owners(shard)
    }

    /// Whether this node owns `shard`.
    pub fn owns(&self, shard: Shard) -> bool {
        self.owners(shard).contains(&self.config.node_id)
    }

    /// The hashed addresses of the split contracts.
    pub const fn split_storage(&self) -> &B256Set {
        &self.split_storage
    }

    /// Whether any contract's storage is split.
    pub fn splits_storage(&self) -> bool {
        !self.split_storage.is_empty()
    }

    /// Whether the encoded `key` of `table` belongs to a split contract, whose duplicates are
    /// placed by subkey.
    pub fn is_split(&self, table: ShardedTable, key: &[u8]) -> bool {
        !self.split_storage.is_empty()
            && table.is_duplicate()
            && key.len() == ShardedTable::HASH_KEY_LEN
            && self.split_storage.contains(&B256::from_slice(key))
    }

    /// Where the row with encoded `key` and `subkey` of `table` lives. `subkey` (the start of an
    /// encoded duplicate value) is read only for a split contract; other rows are placed by key.
    /// `None` when the key, or the subkey of a split contract, is not valid for the table.
    pub fn placement(
        &self,
        table: ShardedTable,
        key: &[u8],
        subkey: &[u8],
    ) -> Option<KeyPlacement> {
        match table.subkey_layout() {
            Some(layout) if self.is_split(table, key) => layout.placement(subkey),
            _ => table.placement(key),
        }
    }

    /// Whether this node holds the row with encoded `key` and `subkey` of `table` (see
    /// [`Self::placement`]): the row is replicated or in a shard this node owns.
    pub fn holds(&self, table: ShardedTable, key: &[u8], subkey: &[u8]) -> Option<bool> {
        self.placement(table, key, subkey).map(|placement| self.holds_placement(placement))
    }

    /// Whether this node holds the rows at `placement`: they are replicated or in a shard this
    /// node owns.
    pub fn holds_placement(&self, placement: KeyPlacement) -> bool {
        match placement {
            KeyPlacement::Replicated => true,
            KeyPlacement::Shard(shard) => self.owns(shard),
        }
    }

    /// Address of `node`'s owner server, if the node is listed.
    pub fn address(&self, node: NodeId) -> Option<SocketAddr> {
        self.config.nodes.iter().find(|candidate| candidate.id == node).map(|n| n.address)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const THREE_NODES: &str = r#"
        node_id = 3

        [[nodes]]
        id = 1
        address = "127.0.0.1:9001"

        [[nodes]]
        id = 2
        address = "127.0.0.1:9002"

        [[nodes]]
        id = 3
        address = "127.0.0.1:9003"
        storage = false
    "#;

    fn parse(toml: &str) -> Result<Cluster, ConfigError> {
        Cluster::new(toml::from_str(toml).expect("valid toml"))
    }

    #[test]
    fn loads_from_file_with_defaults() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cluster.toml");
        std::fs::write(&path, THREE_NODES).unwrap();

        let cluster = Cluster::load(&path).unwrap();
        assert_eq!(cluster.node_id(), NodeId(3));
        assert_eq!(cluster.config().replication, ClusterConfig::DEFAULT_REPLICATION);
        assert_eq!(cluster.config().retention_blocks, ClusterConfig::MIN_RETENTION_BLOCKS);
        assert_eq!(cluster.address(NodeId(2)), Some("127.0.0.1:9002".parse().unwrap()));
        assert!(cluster.config().nodes[0].storage);
    }

    #[test]
    fn non_storing_node_owns_nothing_but_holds_replicated_keys() {
        let cluster = parse(THREE_NODES).unwrap();
        for shard in Shard::all() {
            assert!(!cluster.owns(shard));
            assert!(!cluster.owners(shard).contains(&NodeId(3)));
        }

        let mut root_child = [0u8; 33];
        root_child[0] = 0x70;
        root_child[32] = 1;
        assert_eq!(
            ShardedTable::AccountsTrie.placement(&root_child),
            Some(KeyPlacement::Replicated)
        );
        assert_eq!(cluster.holds(ShardedTable::AccountsTrie, &root_child, &[]), Some(true));
        assert_eq!(cluster.holds(ShardedTable::HashedAccounts, &[0x70; 32], &[]), Some(false));
    }

    #[test]
    fn rejects_invalid_configs() {
        let duplicate = r#"
            node_id = 1
            [[nodes]]
            id = 1
            address = "127.0.0.1:9001"
            [[nodes]]
            id = 1
            address = "127.0.0.1:9002"
        "#;
        assert!(matches!(parse(duplicate), Err(ConfigError::DuplicateNode(NodeId(1)))));

        let unknown_self = THREE_NODES.replace("node_id = 3", "node_id = 9");
        assert!(matches!(parse(&unknown_self), Err(ConfigError::UnknownSelf(NodeId(9)))));

        let too_many_owners = THREE_NODES.replace("node_id = 3", "node_id = 3\nreplication = 3");
        assert!(matches!(
            parse(&too_many_owners),
            Err(ConfigError::InvalidReplication { replication: 3, storage_nodes: 2 })
        ));

        let short_window =
            THREE_NODES.replace("node_id = 3", "node_id = 3\nretention_blocks = 159");
        assert!(matches!(parse(&short_window), Err(ConfigError::RetentionTooShort(159))));

        let unknown_source = THREE_NODES.replace("node_id = 3", "node_id = 3\napply_from = [1, 9]");
        assert!(matches!(parse(&unknown_source), Err(ConfigError::UnknownApplySource(NodeId(9)))));

        let from_self = THREE_NODES.replace("node_id = 3", "node_id = 3\napply_from = [3]");
        assert!(matches!(parse(&from_self), Err(ConfigError::ApplyFromSelf(NodeId(3)))));

        let short_entry = THREE_NODES.replace(
            "node_id = 3",
            "node_id = 3\nsplit_storage = [\"0x42000000000000000000000000000000000016\"]",
        );
        assert!(matches!(parse(&short_entry), Err(ConfigError::InvalidSplitStorage(_))));

        let not_hex = THREE_NODES.replace("node_id = 3", "node_id = 3\nsplit_storage = [\"0xzz\"]");
        assert!(toml::from_str::<ClusterConfig>(&not_hex).is_err());
    }

    #[test]
    fn apply_from_lists_sources_in_order_and_defaults_to_executing() {
        assert!(parse(THREE_NODES).unwrap().apply_from().is_empty());

        let applying = THREE_NODES.replace("node_id = 3", "node_id = 3\napply_from = [2, 1]");
        assert_eq!(parse(&applying).unwrap().apply_from(), [NodeId(2), NodeId(1)]);
    }

    #[test]
    fn split_storage_places_listed_contracts_by_subkey() {
        const ADDRESS: &str = "0x4200000000000000000000000000000000000016";
        const HASHED: &str = "0x4c06361fae302f0881e3f88f50ae396fedd00dad86367c25d0336250c9ce554d";
        let listed = THREE_NODES.replace(
            "node_id = 3",
            &format!("node_id = 1\nsplit_storage = [\"{ADDRESS}\", \"{HASHED}\"]"),
        );
        let cluster = parse(&listed).unwrap();
        let from_address = keccak256(ADDRESS.parse::<Bytes>().unwrap());
        let hashed: B256 = HASHED.parse().unwrap();
        let unlisted = B256::repeat_byte(0x4c);

        let mut slot = [0u8; 32];
        slot[0] = 0x9a;
        for contract in [from_address, hashed] {
            assert!(cluster.is_split(ShardedTable::HashedStorages, contract.as_slice()));
            assert_eq!(
                cluster.placement(ShardedTable::HashedStorages, contract.as_slice(), &slot),
                Some(KeyPlacement::Shard(Shard(0x9a)))
            );
        }
        assert!(!cluster.is_split(ShardedTable::HashedStorages, unlisted.as_slice()));
        assert!(!cluster.is_split(ShardedTable::HashedAccounts, hashed.as_slice()));
        assert_eq!(
            cluster.placement(ShardedTable::HashedStorages, unlisted.as_slice(), &slot),
            Some(KeyPlacement::Shard(Shard(0x4c)))
        );

        let path = |first_byte: u8, nibbles: u8| {
            let mut path = [0u8; 33];
            path[0] = first_byte;
            path[32] = nibbles;
            path
        };
        let trie = ShardedTable::StoragesTrie;
        assert_eq!(
            cluster.placement(trie, hashed.as_slice(), &path(0, 0)),
            Some(KeyPlacement::Replicated)
        );
        assert_eq!(
            cluster.placement(trie, hashed.as_slice(), &path(0xa0, 1)),
            Some(KeyPlacement::Replicated)
        );
        assert_eq!(
            cluster.placement(trie, hashed.as_slice(), &path(0xa3, 2)),
            Some(KeyPlacement::Shard(Shard(0xa3)))
        );
        assert_eq!(cluster.placement(trie, hashed.as_slice(), &path(0xa3, 1)), None);
        assert_eq!(
            cluster.placement(trie, unlisted.as_slice(), &path(0xa3, 2)),
            Some(KeyPlacement::Shard(Shard(0x4c)))
        );
    }
}
