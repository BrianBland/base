//! Connects a clustered database to the running node: block hashes and persisted changesets for
//! the owner store's history, canonical blocks above the persisted tip, and inserted blocks'
//! execution results for apply nodes.

use std::{future::Future, sync::Arc};

use alloy_primitives::Bytes;
use base_execution_cluster::{
    CanonicalBlock, ClusterNode, ExecutedBlockResult, JoinPoint, ProviderChangesets, ResultCapture,
};
use futures::{Stream, StreamExt};
use reth_chain_state::ExecutedBlock;
use reth_node_api::ConsensusEngineEvent;
use reth_primitives_traits::{AlloyBlockHeader, Block, NodePrimitives};
use reth_provider::{
    BlockHashReader, BlockReader, CanonStateNotification, CanonStateSubscriptions, Chain,
    ChangeSetReader, DatabaseProviderFactory, HeaderProvider, StaticFileProviderFactory,
    StorageChangeSetReader,
};
use tokio::sync::broadcast::error::RecvError;
use tracing::{info, warn};

/// Feeds a [`ClusterNode`] from the node's provider.
#[derive(Debug, Clone, Copy)]
pub struct ClusterFeed;

impl ClusterFeed {
    /// Installs `provider`'s block hashes, persisted changesets (from which `node` builds its
    /// owner's history index) and join points (for nodes joining at its persisted tip) on `node`,
    /// and returns the task that feeds every canonical block to `node`'s
    /// owner store. Subscribes before building, so no block notified after this call is missed.
    ///
    /// The provider owns the clustered database, which owns `node`, so the installed lookup and
    /// reader reference the provider only weakly and the task keeps it alive: dropping the task
    /// and the node's other handles releases the node and its owner server.
    pub fn start<P>(
        node: Arc<ClusterNode>,
        provider: P,
    ) -> eyre::Result<impl Future<Output = ()> + Send + 'static>
    where
        P: BlockHashReader + CanonStateSubscriptions + DatabaseProviderFactory + 'static,
        P::Provider:
            BlockReader + ChangeSetReader + StorageChangeSetReader + StaticFileProviderFactory,
    {
        let notifications = provider.subscribe_to_canonical_state();
        let provider = Arc::new(provider);
        let lookup = Arc::downgrade(&provider);
        node.install_block_hashes(Box::new(move |number| {
            lookup.upgrade()?.block_hash(number).ok().flatten()
        }));
        let join_points = Arc::downgrade(&provider);
        node.owner_store().install_join_points(Box::new(move |range| {
            // One read transaction, so the headers and the body are of the same chain.
            let provider = join_points.upgrade()?.database_provider_ro().ok()?;
            let body = provider.block_by_number(*range.end()).ok()??.into_body();
            let headers = provider.headers_range(range).ok()?;
            Some(JoinPoint::encode(&headers, &body))
        }));
        node.install_changesets(Box::new(ProviderChangesets::new(&provider)))?;
        info!("cluster owner store following canonical blocks");
        Ok(Self::follow(node, notifications, provider))
    }

    /// Feeds `node` until the notifications close, holding `_provider` for the node's lookup.
    /// Each notification also marks a block boundary of the node's remote-read stats.
    async fn follow<N: NodePrimitives, P>(
        node: Arc<ClusterNode>,
        mut notifications: tokio::sync::broadcast::Receiver<CanonStateNotification<N>>,
        _provider: Arc<P>,
    ) {
        loop {
            match notifications.recv().await {
                Ok(CanonStateNotification::Commit { new }) => {
                    node.client().remote_read_stats().record_block();
                    node.owner_store().commit_blocks(Self::canonical_blocks(&new));
                }
                Ok(CanonStateNotification::Reorg { old, new }) => {
                    node.client().remote_read_stats().record_block();
                    let reverted_from = old.first().number();
                    node.owner_store().reorg_blocks(reverted_from, Self::canonical_blocks(&new));
                }
                Err(RecvError::Lagged(skipped)) => warn!(
                    skipped,
                    "cluster owner store missed canonical blocks; it serves past them only once \
                     they are persisted"
                ),
                Err(RecvError::Closed) => return,
            }
        }
    }

    /// Keeps the execution result of every block the engine inserts, as `events` report them,
    /// in `node`'s executed-block store until the events close. Encoding waits for the block's
    /// trie data, so it runs on the bounded [`ResultCapture`] worker, which drops blocks captured
    /// while it is backed up and stops with the events.
    pub async fn serve_executed_blocks<N: NodePrimitives>(
        node: Arc<ClusterNode>,
        events: impl Stream<Item = ConsensusEngineEvent<N>>,
    ) {
        let capture = match ResultCapture::spawn(
            Arc::clone(node.executed_blocks()),
            |block: ExecutedBlock<N>| Self::encode_executed_block(&block),
        ) {
            Ok(capture) => capture,
            Err(error) => {
                warn!(error = %error, "failed to start keeping execution results for apply nodes");
                return;
            }
        };
        let mut events = std::pin::pin!(events);
        while let Some(event) = events.next().await {
            let (ConsensusEngineEvent::CanonicalBlockAdded(block, _)
            | ConsensusEngineEvent::ForkBlockAdded(block, _)) = event
            else {
                continue;
            };
            let (hash, number) = (block.recovered_block.hash(), block.recovered_block.number());
            if !capture.submit(hash, number, block) {
                warn!(block = %hash, "execution result capture backed up; dropped a block's result");
            }
        }
        if let Ok(Err(_)) = tokio::task::spawn_blocking(move || capture.stop()).await {
            warn!("execution result capture worker panicked");
        }
    }

    /// `block`'s encoded execution result, or `None` when it does not encode.
    pub fn encode_executed_block<N: NodePrimitives>(block: &ExecutedBlock<N>) -> Option<Bytes> {
        let trie = block.trie_data();
        let senders = block.recovered_block.senders();
        match ExecutedBlockResult::new(&block.execution_output, senders, &trie).encode() {
            Ok(encoded) => Some(encoded),
            Err(error) => {
                let hash = block.recovered_block.hash();
                warn!(block = %hash, error = %error, "failed to encode an execution result for apply nodes");
                None
            }
        }
    }

    /// The blocks of `chain` as the owner store keeps them; a block whose notification lacks
    /// trie data has hashed state `None`.
    pub fn canonical_blocks<N: NodePrimitives>(chain: &Chain<N>) -> Vec<CanonicalBlock> {
        chain
            .blocks()
            .values()
            .map(|block| CanonicalBlock {
                block: block.num_hash(),
                parent_hash: block.parent_hash(),
                hashed_state: chain.trie_data_at(block.number()).map(|data| data.hashed_state()),
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
    use reth_provider::{
        CanonStateNotifications, ProviderFactory, ProviderResult,
        test_utils::{MockNodeTypesWithDB, create_test_provider_factory},
    };
    use tokio::sync::broadcast;

    use super::*;

    type TestFactory = ProviderFactory<MockNodeTypesWithDB>;

    /// A provider that, like the node's real one, owns the clustered database. Changesets come
    /// from a separate, empty test database.
    #[derive(Clone)]
    struct DatabaseOwningProvider {
        _database: ClusteredDatabase,
        changesets: TestFactory,
        canonical: broadcast::Sender<CanonStateNotification<BasePrimitives>>,
    }

    impl DatabaseProviderFactory for DatabaseOwningProvider {
        type DB = <TestFactory as DatabaseProviderFactory>::DB;
        type Provider = <TestFactory as DatabaseProviderFactory>::Provider;
        type ProviderRW = <TestFactory as DatabaseProviderFactory>::ProviderRW;

        fn database_provider_ro(&self) -> ProviderResult<Self::Provider> {
            self.changesets.database_provider_ro()
        }

        fn database_provider_rw(&self) -> ProviderResult<Self::ProviderRW> {
            self.changesets.database_provider_rw()
        }
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
            apply_from: Vec::new(),
            split_storage: Vec::new(),
            read_retry_secs: ClusterConfig::DEFAULT_READ_RETRY_SECS,
            simulated_round_trip_micros: 0,
            root_check: Default::default(),
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
        let provider = DatabaseOwningProvider {
            _database: database,
            changesets: create_test_provider_factory(),
            canonical: broadcast::channel(1).0,
        };

        let feed = ClusterFeed::start(Arc::clone(&node), provider).unwrap();
        assert_eq!(node.owner_store().block_hash(0), Some(B256::ZERO));

        drop(feed);
        drop(node);
        assert!(TcpListener::bind(address).is_ok(), "the owner server still holds its address");
    }
}
