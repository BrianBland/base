//! What a clustered node refuses: building blocks, and the proofs-history extension.

use std::sync::Arc;

use alloy_primitives::B256;
use base_common_network::BaseEngineApi;
use base_execution_cluster::{Cluster, ClusterConfig, ClusteredDatabase, NodeConfig, NodeId};
use base_node_core::args::RollupArgs;
use base_node_runner::{BaseNodeRunner, test_utils::L1_BLOCK_INFO_DEPOSIT_TX};
use eyre::Result;
use reth_node_builder::NodeBuilder;
use reth_tasks::Runtime;

use crate::{Datadir, TestNode, chain_spec_with, cluster_config, free_address};

fn one_node() -> Result<ClusterConfig> {
    let node = NodeConfig { id: NodeId(1), address: free_address()?, storage: true };
    Ok(ClusterConfig { replication: 1, ..cluster_config(1, &[node]) })
}

/// Payload attributes sent to a clustered node are refused, since the block's state root would
/// be computed over rows it does not hold; the forkchoice state is still applied.
#[tokio::test(flavor = "multi_thread")]
async fn a_clustered_node_refuses_to_build_blocks() -> Result<()> {
    base_node_runner::test_utils::init_silenced_tracing();
    let chain_spec = chain_spec_with(16, 0, [])?;
    let node = TestNode::launch(&chain_spec, Some(one_node()?)).await?;
    let (state, attributes) = node.next_block(vec![L1_BLOCK_INFO_DEPOSIT_TX]).await?;
    let refused = node.engine.fork_choice_updated_v3(state, Some(attributes)).await;
    let message = format!("{refused:?}");
    assert!(refused.is_err(), "{message}");
    assert!(message.to_lowercase().contains("attributes"), "{message}");
    assert_ne!(node.head().await?.0, B256::ZERO);
    node.remove().await
}

/// A clustered node does not launch with the proofs-history extension.
#[tokio::test(flavor = "multi_thread")]
async fn a_clustered_node_refuses_the_proofs_history_extension() -> Result<()> {
    base_node_runner::test_utils::init_silenced_tracing();
    let chain_spec = chain_spec_with(16, 0, [])?;
    let datadir = Datadir::fresh(&chain_spec);
    let env = datadir.open().await?;
    let cluster = Arc::new(Cluster::new(one_node()?)?);
    let address = cluster.address(cluster.node_id()).expect("listed");
    let db = ClusteredDatabase::clustered(
        env,
        cluster,
        std::net::TcpListener::bind(address)?,
        Default::default(),
    )?;
    let runtime = std::thread::spawn(Runtime::test).join().expect("a runtime");
    let builder = NodeBuilder::new(datadir.node_config.clone())
        .with_database(db)
        .with_launch_context(runtime.clone());
    let args = RollupArgs { proofs_history: true, ..RollupArgs::default() };
    let launched = BaseNodeRunner::new(args).launch(builder).await;
    let message = format!("{:?}", launched.err());
    assert!(message.contains("the proofs-history extension reads trie rows"), "{message}");
    tokio::task::spawn_blocking(move || {
        runtime.shutdown_timeout(std::time::Duration::from_secs(5))
    })
    .await?;
    let _ = std::fs::remove_dir_all(datadir.path);
    Ok(())
}
