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
    pub fn start<P>(
        node: Arc<ClusterNode>,
        provider: P,
    ) -> eyre::Result<impl Future<Output = ()> + Send + 'static>
    where
        P: BlockHashReader + CanonStateSubscriptions + Clone + 'static,
    {
        let notifications = provider.subscribe_to_canonical_state();
        node.install_block_hashes(Box::new(move |number| {
            provider.block_hash(number).ok().flatten()
        }));
        let point = node.register_persistence_point()?;
        info!(
            persisted_tip = ?point.map(|block| block.number),
            "cluster owner store following canonical blocks"
        );
        Ok(Self::follow(node, notifications))
    }

    async fn follow<N: NodePrimitives>(
        node: Arc<ClusterNode>,
        mut notifications: tokio::sync::broadcast::Receiver<CanonStateNotification<N>>,
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
