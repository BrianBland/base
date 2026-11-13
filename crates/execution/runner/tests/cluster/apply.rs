//! Apply mode: a non-storing node inserts blocks from a storing executor's results, or executes
//! them when no valid result arrives. Blocks are built by an unclustered producer, since a
//! clustered node cannot build them.

use std::{sync::Arc, time::Duration};

use alloy_eips::eip7685::Requests;
use alloy_primitives::{Address, B256, Bytes, TxKind, keccak256};
use base_common_consensus::BaseReceipt;
use base_execution_cluster::{
    ApplyCounts, ApplyFallback, Cluster, ClusterConfig, ExecutedBlockResult, ExecutedBlocks,
    NodeConfig as ClusterNodeConfig, NodeId, OwnerServer, OwnerStore,
};
use eyre::{Result, eyre};
use reth_db::test_utils::create_test_rw_db;
use reth_provider::{ChangeSetReader, StorageChangeSetReader};

use crate::{
    STORAGE_CONTRACT, TableDump, TestNode, chain_spec_with, cluster_config, deposit, free_address,
    store_call,
};

/// Blocks each test produces.
const BLOCKS: u64 = 3;

/// Accounts funded at genesis beyond the test genesis's own, so the account trie has stored
/// branch nodes for blocks to change.
const GENESIS_ACCOUNTS: u64 = 512;

/// User deposits per block of each kind: funding a fresh account, and writing a fresh slot of
/// [`STORAGE_CONTRACT`].
const DEPOSITS_PER_KIND: u64 = 24;

/// The seed of the `index`th deposit value of `kind` in block `number`: kind 1 is a recipient
/// account, 3 a written slot of [`STORAGE_CONTRACT`], 2 and 4 their deposits' source hashes.
pub(crate) fn deposit_seed(number: u64, kind: u8, index: u64) -> B256 {
    keccak256([[kind].as_slice(), &number.to_be_bytes(), &index.to_be_bytes()].concat())
}

/// The user deposits of block `number`: they create accounts and write the block number into
/// fresh slots of [`STORAGE_CONTRACT`], so blocks change the account and storage tries.
pub(crate) fn block_transactions(number: u64) -> Vec<Bytes> {
    let seed = |kind, index| deposit_seed(number, kind, index);
    let mut transactions = Vec::new();
    for index in 0..DEPOSITS_PER_KIND {
        let account = Address::from_word(seed(1, index));
        transactions.push(deposit(
            seed(2, index),
            TxKind::Call(account),
            1_000_000_000,
            Bytes::new(),
        ));
        let call = store_call(seed(3, index), alloy_primitives::U256::from(number));
        transactions.push(deposit(seed(4, index), TxKind::Call(STORAGE_CONTRACT), 0, call));
    }
    transactions
}

/// Every account and slot blocks 1 to `BLOCKS` touched, and a few untouched genesis accounts.
fn touched() -> (Vec<Address>, Vec<(Address, B256)>) {
    let mut accounts = vec![STORAGE_CONTRACT, crate::DEPOSITOR];
    accounts.extend((0..4u64).map(|index| Address::from_word(keccak256(index.to_be_bytes()))));
    let mut slots = Vec::new();
    for number in 1..=BLOCKS {
        for index in 0..DEPOSITS_PER_KIND {
            accounts.push(Address::from_word(deposit_seed(number, 1, index)));
            slots.push((STORAGE_CONTRACT, deposit_seed(number, 3, index)));
        }
    }
    (accounts, slots)
}

impl TestNode {
    /// The account and storage changesets (the reverts) of blocks 1 to `BLOCKS`, debug-printed.
    fn changesets(&self) -> Result<Vec<String>> {
        let provider = self.launched.handle.node.provider();
        let mut changesets = Vec::new();
        for number in 1..=BLOCKS {
            changesets.push(format!("{:?}", provider.account_block_changeset(number)?));
            changesets.push(format!("{:?}", provider.storage_changeset(number)?));
        }
        Ok(changesets)
    }

    /// The state of every row blocks 1 to `BLOCKS` touched.
    fn touched_state(&self) -> Result<Vec<String>> {
        let (accounts, slots) = touched();
        self.state_of(&accounts, &slots)
    }
}

/// Storage node 1 (which with the absent storage node 2 owns every shard, `R = 2`), a
/// non-storing apply node 3 and a non-storing node 4, all at fresh addresses.
fn four_nodes() -> Result<Vec<ClusterNodeConfig>> {
    let node = |id, storage| -> Result<_> {
        Ok(ClusterNodeConfig { id: NodeId(id), address: free_address()?, storage })
    };
    Ok(vec![node(1, true)?, node(2, true)?, node(3, false)?, node(4, false)?])
}

/// `tables` as a non-storing node holds them: only the replicated account-trie paths, and the
/// replicated storage-trie paths of `split` (a split contract's hashed address); every other
/// table as it is.
fn replicated_only(tables: Vec<TableDump>, split: Option<B256>) -> Vec<TableDump> {
    // A packed trie path is 32 bytes of nibbles and then the nibble count.
    let short_path = |path: &[u8]| path.get(32).is_some_and(|len| *len < 2);
    tables
        .into_iter()
        .map(|(name, rows)| match name {
            "AccountsTrie" => (name, rows.into_iter().filter(|(key, _)| short_path(key)).collect()),
            "StoragesTrie" => {
                let replicated = |(key, value): &(Vec<u8>, Vec<u8>)| {
                    split.is_some_and(|split| key.as_slice() == split.as_slice())
                        && short_path(value)
                };
                (name, rows.into_iter().filter(replicated).collect())
            }
            "HashedAccounts" | "HashedStorages" | "Bytecodes" => (name, Vec::new()),
            _ => (name, rows),
        })
        .collect()
}

/// A non-storing apply node, with or without a split contract, ends with exactly the storing
/// executor's head, receipts, changesets and state as its provider reads it, without executing,
/// while its own sharded tables hold only the replicated rows (the executor's, which holds every
/// shard). The executor, whose config lists no `apply_from`, never applies.
async fn apply_node_matches_the_executor(split: bool) -> Result<()> {
    base_node_runner::test_utils::init_silenced_tracing();
    let chain_spec = chain_spec_with(GENESIS_ACCOUNTS, 0, [])?;
    let nodes = four_nodes()?;
    let split_storage =
        if split { vec![Bytes::copy_from_slice(STORAGE_CONTRACT.as_slice())] } else { vec![] };
    let config = |id, apply_from| ClusterConfig {
        apply_from,
        split_storage: split_storage.clone(),
        ..cluster_config(id, &nodes)
    };
    let producer = TestNode::launch(&chain_spec, None).await?;
    let executor = TestNode::launch(&chain_spec, Some(config(1, vec![]))).await?;
    let applier = TestNode::launch(&chain_spec, Some(config(3, vec![NodeId(1)]))).await?;

    for number in 1..=BLOCKS {
        let envelope = producer.produce(block_transactions(number)).await?;
        let hash = executor.import(&envelope).await?;
        assert_eq!(applier.import(&envelope).await?, hash);
    }
    executor.wait_persisted(BLOCKS).await?;
    applier.wait_persisted(BLOCKS).await?;

    assert_eq!(applier.cluster().applied_blocks().applied, BLOCKS);
    assert_eq!(applier.cluster().applied_blocks().fallbacks, [0; ApplyFallback::ALL.len()]);
    assert_eq!(executor.cluster().applied_blocks(), ApplyCounts::default());
    assert_eq!(applier.head().await?, executor.head().await?);
    assert_eq!(applier.head().await?.1, BLOCKS);
    assert_eq!(applier.receipts(BLOCKS).await?, executor.receipts(BLOCKS).await?);
    let changesets = executor.changesets()?;
    assert!(changesets.iter().any(|changeset| changeset != "[]"), "no block has reverts");
    assert_eq!(applier.changesets()?, changesets);
    let state = executor.touched_state()?;
    assert!(state.iter().any(|read| read.ends_with(": Some(1)")), "no written slot read");
    assert_eq!(applier.touched_state()?, state);
    assert_eq!(state, producer.touched_state()?);

    let split_contract = split.then(|| keccak256(STORAGE_CONTRACT));
    let expected = replicated_only(executor.state_tables()?, split_contract);
    assert!(
        expected.iter().any(|(name, rows)| *name == "AccountsTrie" && !rows.is_empty()),
        "no replicated account-trie path to compare"
    );
    if split {
        assert!(
            expected.iter().any(|(name, rows)| *name == "StoragesTrie" && !rows.is_empty()),
            "no replicated storage-trie path of the split contract to compare"
        );
    }
    assert_eq!(applier.state_tables()?, expected);

    for node in [producer, executor, applier] {
        node.remove().await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn applied_chain_matches_the_executed_chain() -> Result<()> {
    apply_node_matches_the_executor(false).await
}

#[tokio::test(flavor = "multi_thread")]
async fn apply_node_matches_the_executor_with_a_split_contract() -> Result<()> {
    apply_node_matches_the_executor(true).await
}

/// An apply node executes a payload whose result fails the receipt checks or carries execution
/// requests, or when its executor is unreachable, and counts why.
#[tokio::test(flavor = "multi_thread")]
async fn apply_node_executes_when_no_valid_result_arrives() -> Result<()> {
    base_node_runner::test_utils::init_silenced_tracing();
    let chain_spec = chain_spec_with(GENESIS_ACCOUNTS, 0, [])?;
    let nodes = four_nodes()?;
    let producer = TestNode::launch(&chain_spec, None).await?;
    let executor = TestNode::launch(&chain_spec, Some(cluster_config(1, &nodes))).await?;
    let applier_config = ClusterConfig { apply_from: vec![NodeId(4)], ..cluster_config(3, &nodes) };

    // Node 4 serves results whose first receipt reports one more gas than was used. It is up
    // before the applier starts on its empty datadir, which asks it for a join point.
    let corrupt_results = Arc::new(ExecutedBlocks::default());
    let corrupt_view = Cluster::new(cluster_config(4, &nodes))?;
    let corrupt_store =
        Arc::new(OwnerStore::new(Arc::new(corrupt_view), create_test_rw_db(), 160)?);
    let corrupt_server = OwnerServer::spawn(
        std::net::TcpListener::bind(nodes[3].address)?,
        corrupt_store,
        Arc::clone(&corrupt_results),
    )?;
    let applier = TestNode::launch(&chain_spec, Some(applier_config)).await?;

    let envelope = producer.produce(block_transactions(1)).await?;
    let hash = executor.import(&envelope).await?;
    let result = executor
        .cluster()
        .executed_blocks()
        .wait_for(hash, Duration::from_secs(5))
        .ok_or_else(|| eyre!("executor kept no result for block 1"))?;
    let mut parts = ExecutedBlockResult::decode(&result)?.into_parts::<BaseReceipt>()?;
    let BaseReceipt::Deposit(receipt) = &mut parts.output.result.receipts[0] else {
        return Err(eyre!("first receipt is not the L1 info deposit"));
    };
    receipt.inner.cumulative_gas_used += 1;
    let corrupt = ExecutedBlockResult::new(&parts.output, &parts.senders, &parts.trie).encode()?;
    corrupt_results.insert(hash, 1, corrupt);
    assert_eq!(applier.import(&envelope).await?, hash);
    assert_eq!(applier.cluster().applied_blocks().fallbacks(ApplyFallback::Invalid), 1);

    // Block 2's result differs only in carrying an execution request.
    let envelope = producer.produce(block_transactions(2)).await?;
    let hash = executor.import(&envelope).await?;
    let result = executor
        .cluster()
        .executed_blocks()
        .wait_for(hash, Duration::from_secs(5))
        .ok_or_else(|| eyre!("executor kept no result for block 2"))?;
    let mut parts = ExecutedBlockResult::decode(&result)?.into_parts::<BaseReceipt>()?;
    parts.output.result.requests = Requests::new(vec![Bytes::from_static(&[0x01, 0xaa])]);
    let corrupt = ExecutedBlockResult::new(&parts.output, &parts.senders, &parts.trie).encode()?;
    corrupt_results.insert(hash, 2, corrupt);
    assert_eq!(applier.import(&envelope).await?, hash);
    assert_eq!(applier.cluster().applied_blocks().fallbacks(ApplyFallback::Invalid), 2);

    drop(corrupt_server);
    for number in 3..=BLOCKS {
        let envelope = producer.produce(block_transactions(number)).await?;
        let hash = executor.import(&envelope).await?;
        assert_eq!(applier.import(&envelope).await?, hash);
    }
    let counts = applier.cluster().applied_blocks();
    assert_eq!(counts.applied, 0);
    assert_eq!(counts.fallbacks(ApplyFallback::Unreachable), BLOCKS - 2);
    assert_eq!(applier.head().await?, executor.head().await?);
    assert_eq!(applier.receipts(BLOCKS).await?, executor.receipts(BLOCKS).await?);
    assert_eq!(applier.touched_state()?, producer.touched_state()?);

    for node in [producer, executor, applier] {
        node.remove().await?;
    }
    Ok(())
}
