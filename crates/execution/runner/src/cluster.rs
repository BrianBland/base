//! Connects a clustered database to the running node: block hashes for persistence points, and
//! canonical blocks for the owner store.

use std::{future::Future, sync::Arc};

use base_execution_cluster::{BlockUpdates, CanonicalBlock, ClusterNode};
use reth_primitives_traits::{AlloyBlockHeader, NodePrimitives};
use reth_provider::{BlockHashReader, CanonStateNotification, CanonStateSubscriptions, Chain};
use tokio::sync::broadcast::error::RecvError;
use tracing::{info, warn};

/// Feeds a [`ClusterNode`] from the node's provider.
#[derive(Debug, Clone, Copy)]
pub struct ClusterFeed;

impl ClusterFeed {
    /// Installs `provider`'s block hashes on `node`, registers the initial persistence point,
    /// and returns the task that feeds every canonical block to `node`'s owner store. Subscribes
    /// before returning, so no block notified after this call is missed.
    ///
    /// The provider owns the clustered database, which owns `node`, so the installed lookup
    /// references the provider only weakly and the task keeps it alive: dropping the task and the
    /// node's other handles releases the node, its owner server and its held snapshots.
    pub fn start<P>(
        node: Arc<ClusterNode>,
        provider: P,
    ) -> eyre::Result<impl Future<Output = ()> + Send + 'static>
    where
        P: BlockHashReader + CanonStateSubscriptions + 'static,
    {
        let notifications = provider.subscribe_to_canonical_state();
        let provider = Arc::new(provider);
        let lookup = Arc::downgrade(&provider);
        node.install_block_hashes(Box::new(move |number| {
            lookup.upgrade()?.block_hash(number).ok().flatten()
        }));
        let point = node.register_persistence_point()?;
        info!(
            persisted_tip = ?point.map(|block| block.number),
            "cluster owner store following canonical blocks"
        );
        Ok(Self::follow(node, notifications, provider))
    }

    /// Feeds `node` until the notifications close, holding `_provider` for the node's lookup.
    async fn follow<N: NodePrimitives, P>(
        node: Arc<ClusterNode>,
        mut notifications: tokio::sync::broadcast::Receiver<CanonStateNotification<N>>,
        _provider: Arc<P>,
    ) {
        loop {
            match notifications.recv().await {
                Ok(notification) => {
                    for block in Self::canonical_blocks(&notification.committed()) {
                        node.owner_store().insert_block(block);
                    }
                }
                Err(RecvError::Lagged(skipped)) => warn!(
                    skipped,
                    "cluster owner store missed canonical blocks; it serves past them only from \
                     its next persistence point"
                ),
                Err(RecvError::Closed) => return,
            }
        }
    }

    /// The blocks of `chain` as the owner store retains them; a block whose notification lacks
    /// trie data has updates `None`.
    pub fn canonical_blocks<N: NodePrimitives>(chain: &Chain<N>) -> Vec<CanonicalBlock> {
        chain
            .blocks()
            .values()
            .map(|block| CanonicalBlock {
                block: block.num_hash(),
                parent_hash: block.parent_hash(),
                updates: chain.trie_data_at(block.number()).map(|data| BlockUpdates {
                    hashed_state: data.hashed_state(),
                    trie_updates: data.trie_updates(),
                }),
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use std::net::TcpListener;

    use alloy_primitives::{B256, BlockNumber};
    use base_common_consensus::BasePrimitives;
    use base_execution_cluster::{
        ClientOptions, Cluster, ClusterConfig, ClusteredDatabase, NodeConfig, NodeId,
    };
    use reth_db::test_utils::create_test_rw_db;
    use reth_provider::{CanonStateNotifications, ProviderResult};
    use tokio::sync::broadcast;

    use super::*;

    /// A provider that, like the node's real one, owns the clustered database.
    #[derive(Clone)]
    struct DatabaseOwningProvider {
        _database: ClusteredDatabase,
        canonical: broadcast::Sender<CanonStateNotification<BasePrimitives>>,
    }

    impl BlockHashReader for DatabaseOwningProvider {
        fn block_hash(&self, _number: BlockNumber) -> ProviderResult<Option<B256>> {
            Ok(Some(B256::ZERO))
        }

        fn canonical_hashes_range(
            &self,
            _start: BlockNumber,
            _end: BlockNumber,
        ) -> ProviderResult<Vec<B256>> {
            Ok(Vec::new())
        }
    }

    impl CanonStateSubscriptions for DatabaseOwningProvider {
        type Primitives = BasePrimitives;

        fn subscribe_to_canonical_state(&self) -> CanonStateNotifications<BasePrimitives> {
            self.canonical.subscribe()
        }
    }

    #[test]
    fn dropping_the_node_and_its_feed_releases_the_owner_address() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let cluster = Cluster::new(ClusterConfig {
            node_id: NodeId(1),
            replication: 1,
            retention_blocks: ClusterConfig::MIN_RETENTION_BLOCKS,
            nodes: vec![NodeConfig { id: NodeId(1), address, storage: true }],
        })
        .unwrap();
        let env = Arc::try_unwrap(create_test_rw_db()).unwrap().into_inner_db();
        let database = ClusteredDatabase::clustered(
            env,
            Arc::new(cluster),
            listener,
            ClientOptions::default(),
        )
        .unwrap();
        let node = Arc::clone(database.cluster().unwrap());
        let provider =
            DatabaseOwningProvider { _database: database, canonical: broadcast::channel(1).0 };

        let feed = ClusterFeed::start(Arc::clone(&node), provider).unwrap();
        assert_eq!(node.block_hash(0), Some(B256::ZERO));

        drop(feed);
        drop(node);
        assert!(TcpListener::bind(address).is_ok(), "the owner server still holds its address");
    }
}
