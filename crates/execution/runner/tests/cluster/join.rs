//! Joining: a node that stores nothing starts on an empty datadir at its executors' persisted
//! block, then follows the chain like any apply node.

use alloy_eips::BlockNumberOrTag;
use alloy_primitives::{Address, B256};
use alloy_provider::Provider;
use base_execution_cluster::{
    ClusterConfig, JoinMarker, NodeConfig as ClusterNodeConfig, NodeId, RootCheckSchedule,
};
use eyre::Result;

use crate::{
    Datadir, STORAGE_CONTRACT, TestNode,
    apply::{block_transactions, deposit_seed},
    chain_spec_with, cluster_config, free_address,
};

/// Accounts funded at genesis beyond the test genesis's own: enough that every one-nibble
/// account-trie child is a stored branch, so the state-root check can verify blocks.
const GENESIS_ACCOUNTS: u64 = 4_096;

/// Blocks produced before the new node joins.
const BEFORE_JOIN: u64 = 6;

/// Blocks produced after it joins, before it restarts.
const AFTER_JOIN: u64 = 6;

/// Storing executors 1 and 2 (each holding every shard, `R = 2`) and node 3, which stores
/// nothing and applies from them.
fn nodes() -> Result<Vec<ClusterNodeConfig>> {
    let node = |id, storage| -> Result<_> {
        Ok(ClusterNodeConfig { id: NodeId(id), address: free_address()?, storage })
    };
    Ok(vec![node(1, true)?, node(2, true)?, node(3, false)?])
}

/// The accounts and slots blocks `1..=blocks` of [`block_transactions`] touched.
fn touched(blocks: u64) -> (Vec<Address>, Vec<(Address, B256)>) {
    let mut accounts = vec![STORAGE_CONTRACT, crate::DEPOSITOR];
    let mut slots = Vec::new();
    for number in 1..=blocks {
        for index in 0..2 {
            accounts.push(Address::from_word(deposit_seed(number, 1, index)));
            slots.push((STORAGE_CONTRACT, deposit_seed(number, 3, index)));
        }
    }
    (accounts, slots)
}

/// A non-storing apply node started on an empty datadir joins at its executors' persisted block,
/// reaches the head by applying their results, verifies state roots, reads the executors' state,
/// keeps blocks before the join point unavailable, and survives a restart.
#[tokio::test(flavor = "multi_thread")]
async fn a_non_storing_node_joins_on_an_empty_datadir() -> Result<()> {
    base_node_runner::test_utils::init_silenced_tracing();
    let chain_spec = chain_spec_with(GENESIS_ACCOUNTS, 0, [])?;
    let nodes = nodes()?;
    let joiner_config =
        ClusterConfig { apply_from: vec![NodeId(1), NodeId(2)], ..cluster_config(3, &nodes) };
    let producer = TestNode::launch(&chain_spec, None).await?;
    let a = TestNode::launch(&chain_spec, Some(cluster_config(1, &nodes))).await?;
    let b = TestNode::launch(&chain_spec, Some(cluster_config(2, &nodes))).await?;
    for number in 1..=BEFORE_JOIN {
        let envelope = producer.produce(block_transactions(number)).await?;
        a.import(&envelope).await?;
        b.import(&envelope).await?;
    }
    a.wait_persisted(BEFORE_JOIN).await?;
    b.wait_persisted(BEFORE_JOIN).await?;

    let c = TestNode::launch(&chain_spec, Some(joiner_config.clone())).await?;
    assert_eq!(c.persisted()?, BEFORE_JOIN);
    let before_join = c.reads.get_block_by_number(BlockNumberOrTag::Number(1)).await;
    assert!(!matches!(before_join, Ok(Some(_))), "block 1 is served: {before_join:?}");

    let head = BEFORE_JOIN + AFTER_JOIN;
    for number in BEFORE_JOIN + 1..=head {
        let envelope = producer.produce(block_transactions(number)).await?;
        let hash = a.import(&envelope).await?;
        b.import(&envelope).await?;
        assert_eq!(c.import(&envelope).await?, hash);
    }
    assert_eq!(c.head().await?, a.head().await?);
    assert_eq!(c.cluster().applied_blocks().applied, AFTER_JOIN);
    let counts = c.wait_root_verified(head - RootCheckSchedule::LAG_BLOCKS).await?;
    assert_eq!(counts.mismatched, 0, "{counts:?}");
    let (accounts, slots) = touched(head);
    let state = a.state_of(&accounts, &slots)?;
    assert!(state.iter().any(|read| read.ends_with(&format!(": Some({head})"))), "no slot read");
    assert_eq!(c.state_of(&accounts, &slots)?, state);
    c.wait_persisted(head).await?;

    let c = c.stop().await?.launch(Some(joiner_config)).await?;
    assert_eq!(c.persisted()?, head);
    let envelope = producer.produce(block_transactions(head + 1)).await?;
    let hash = a.import(&envelope).await?;
    b.import(&envelope).await?;
    assert_eq!(c.import(&envelope).await?, hash);
    assert_eq!(c.head().await?, a.head().await?);
    assert_eq!(c.state_of(&accounts, &slots)?, a.state_of(&accounts, &slots)?);

    for node in [producer, a, b, c] {
        node.remove().await?;
    }
    Ok(())
}

/// A datadir whose join did not finish is refused, with a message saying to delete it.
#[tokio::test(flavor = "multi_thread")]
async fn a_half_joined_datadir_is_refused() -> Result<()> {
    base_node_runner::test_utils::init_silenced_tracing();
    let chain_spec = chain_spec_with(16, 0, [])?;
    let nodes = nodes()?;
    let config = ClusterConfig { apply_from: vec![NodeId(1)], ..cluster_config(3, &nodes) };
    let datadir = Datadir::fresh(&chain_spec);
    let path = datadir.path.clone();
    JoinMarker::create(datadir.node_config.datadir().data_dir())?;
    let refused = datadir.launch(Some(config)).await.err().map(|error| format!("{error:?}"));
    assert!(
        refused.as_ref().is_some_and(|message| message.contains("delete the datadir")),
        "{refused:?}"
    );
    let _ = std::fs::remove_dir_all(path);
    Ok(())
}
