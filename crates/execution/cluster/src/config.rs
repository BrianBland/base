//! Cluster configuration file and the validated cluster view built from it.

use std::{collections::HashSet, net::SocketAddr, path::Path};

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
    /// Every node of the cluster, including this one.
    pub nodes: Vec<NodeConfig>,
}

impl ClusterConfig {
    /// Default replication factor: one owner of each shard may be down.
    pub const DEFAULT_REPLICATION: usize = 2;

    const fn default_replication() -> usize {
        Self::DEFAULT_REPLICATION
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
}

/// A validated cluster configuration with the shard owners precomputed.
#[derive(Debug, Clone)]
pub struct Cluster {
    config: ClusterConfig,
    shard_map: ShardMap,
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
        let shard_map = ShardMap::new(&storage_nodes, config.replication);
        Ok(Self { config, shard_map })
    }

    /// The node this cluster view belongs to.
    pub const fn node_id(&self) -> NodeId {
        self.config.node_id
    }

    /// The configuration this view was built from.
    pub const fn config(&self) -> &ClusterConfig {
        &self.config
    }

    /// Owners of `shard`, highest rendezvous score first.
    pub fn owners(&self, shard: Shard) -> &[NodeId] {
        self.shard_map.owners(shard)
    }

    /// Whether this node owns `shard`.
    pub fn owns(&self, shard: Shard) -> bool {
        self.owners(shard).contains(&self.config.node_id)
    }

    /// Whether this node holds the encoded `key` of `table`: the key is replicated or in a shard
    /// this node owns. `None` when the key is not a valid encoded key of the table.
    pub fn holds(&self, table: ShardedTable, key: &[u8]) -> Option<bool> {
        Some(match table.placement(key)? {
            KeyPlacement::Replicated => true,
            KeyPlacement::Shard(shard) => self.owns(shard),
        })
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
        assert_eq!(cluster.holds(ShardedTable::AccountsTrie, &root_child), Some(true));
        assert_eq!(cluster.holds(ShardedTable::HashedAccounts, &[0x70; 32]), Some(false));
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
    }
}
