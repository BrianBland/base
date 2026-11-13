//! The cluster end to end: an unclustered producer (also the reference), three storing nodes
//! (`R = 2`, a split contract) started from full datadirs, and a non-storing apply node follow a
//! chain that creates and empties accounts, sets, zeroes and re-sets slots in many ranges of the
//! split contract, deploys and calls a contract, and destroys contracts with storage; storing
//! nodes stop, fall behind and restart, alone and all together, and every node catches up.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use alloy_genesis::GenesisAccount;
use alloy_primitives::{Address, B256, Bytes, TxKind, U256, keccak256};
use alloy_rpc_types_engine::PayloadStatusEnum;
use base_common_rpc_types_engine::BaseExecutionPayloadEnvelopeV4;
use base_execution_cluster::{
    CheckOutcome, CheckedBlock, Cluster, ClusterConfig, ClusterHashSource,
    NodeConfig as ClusterNodeConfig, NodeId, OwnerCounts, RootCheckConfig, RootCheckMode,
    RootCheckSchedule, RootChecker, RootMismatchMarker, Shard, ShardHash,
};
use eyre::{Result, eyre};
use jsonrpsee::{core::client::ClientT, http_client::HttpClient, rpc_params};
use reth_db::{
    Database, DatabaseEnv,
    cursor::DbCursorRO,
    tables,
    transaction::{DbTx, DbTxMut},
};
use reth_provider::{AccountReader, StateProviderFactory};
use reth_trie::PackedStoredNibbles;
use reth_trie_common::{BranchNodeCompact, Nibbles};

use crate::{
    DEPOSITOR, Datadir, STORAGE_CONTRACT, TestNode, chain_spec_with, cluster_config, deposit,
    deposit_from, free_address, slices::SliceRows, store_call,
};

/// Genesis density: enough accounts and split-contract slots that the two-nibble trie nodes are
/// stored branches.
const GENESIS_ACCOUNTS: u64 = 4_096;
const CONTRACT_SLOTS: u64 = 4_096;

/// Empty genesis accounts (no balance, nonce or code); touching one deletes it.
const EMPTY_ACCOUNTS: u64 = 64;

/// A genesis contract that deploys its calldata as init code with `CREATE2`, salt zero, passing
/// on the call's value: `CALLDATASIZE PUSH0 PUSH0 CALLDATACOPY PUSH0 CALLDATASIZE PUSH0
/// CALLVALUE CREATE2 STOP`.
const FACTORY: Address = Address::repeat_byte(0xfa);
const FACTORY_CODE: [u8; 10] = [0x36, 0x5f, 0x5f, 0x37, 0x5f, 0x36, 0x5f, 0x34, 0xf5, 0x00];

/// Init code deployed by [`FACTORY`]: without value it stores 1 at slot 0 and self-destructs
/// (so the contract is created and destroyed in one transaction, storage included); with value
/// it stores 2 at slot 1 and keeps no code: `CALLVALUE PUSH1 10 JUMPI PUSH1 1 PUSH0 SSTORE CALLER
/// SELFDESTRUCT JUMPDEST PUSH1 2 PUSH1 1 SSTORE STOP`.
const CHILD_INIT: [u8; 17] = [
    0x34, 0x60, 0x0a, 0x57, 0x60, 0x01, 0x5f, 0x55, 0x33, 0xff, 0x5b, 0x60, 0x02, 0x60, 0x01, 0x55,
    0x00,
];

/// Init code that returns [`crate::STORAGE_CONTRACT_CODE`] as the deployed code: `PUSH7 <code>
/// PUSH0 MSTORE PUSH1 7 PUSH1 25 RETURN`.
const DEPLOY_INIT: [u8; 15] =
    [0x66, 0x60, 0x20, 0x35, 0x5f, 0x35, 0x55, 0x00, 0x5f, 0x52, 0x60, 0x07, 0x60, 0x19, 0xf3];

/// Init code that stores 1 at slot 0 and self-destructs: `PUSH1 1 PUSH0 SSTORE CALLER
/// SELFDESTRUCT`.
const DESTROYED_INIT: [u8; 6] = [0x60, 0x01, 0x5f, 0x55, 0x33, 0xff];

/// Senders whose only transaction is one contract creation, so the address is `create(0)`.
const DEPLOYER: Address = Address::repeat_byte(0xd1);
const DESTROYER: Address = Address::repeat_byte(0xd2);

/// Blocks in which the contracts are deployed or created.
const DEPLOY_BLOCK: u64 = 2;
const DESTROY_BLOCK: u64 = 3;
const RECREATE_BLOCK: u64 = 5;

/// Split-contract slots each block sets.
const SLOTS_PER_BLOCK: u64 = 24;
/// Accounts each block creates.
const ACCOUNTS_PER_BLOCK: u64 = 8;

fn seed(number: u64, kind: u8, index: u64) -> B256 {
    keccak256([[kind].as_slice(), &number.to_be_bytes(), &index.to_be_bytes()].concat())
}

fn empty_account(index: u64) -> Address {
    Address::from_word(keccak256([b"empty".as_slice(), &index.to_be_bytes()].concat()))
}

fn deployed() -> Address {
    DEPLOYER.create(0)
}

fn destroyed() -> Address {
    DESTROYER.create(0)
}

fn child() -> Address {
    FACTORY.create2_from_code(B256::ZERO, CHILD_INIT)
}

/// The genesis of the workload: [`STORAGE_CONTRACT`] dense, the factory, empty accounts.
fn chain_spec() -> Result<Arc<base_execution_chainspec::BaseChainSpec>> {
    let factory =
        (FACTORY, GenesisAccount::default().with_code(Some(Bytes::from_static(&FACTORY_CODE))));
    let empties =
        (0..EMPTY_ACCOUNTS).map(|index| (empty_account(index), GenesisAccount::default()));
    chain_spec_with(GENESIS_ACCOUNTS, CONTRACT_SLOTS, std::iter::once(factory).chain(empties))
}

/// The user deposits of each block, and every account and slot they touch.
#[derive(Debug, Default)]
struct Workload {
    accounts: BTreeSet<Address>,
    slots: BTreeSet<(Address, B256)>,
}

impl Workload {
    /// Block `number`'s deposits: it creates accounts, deletes two empty genesis accounts, sets
    /// fresh split-contract slots to `number`, zeroes half of block `number - 1`'s and re-sets
    /// the half zeroed in block `number - 1`; the deployed contract gets the same treatment on
    /// one slot per block; and the contracts are created or destroyed in their blocks.
    fn block(&mut self, number: u64) -> Vec<Bytes> {
        let mut transactions = Vec::new();
        let mut call = |kind: u8, index: u64, to: Address, value: u128, input: Bytes| {
            transactions.push(deposit(seed(number, kind, index), TxKind::Call(to), value, input));
        };
        for index in 0..ACCOUNTS_PER_BLOCK {
            let account = Address::from_word(seed(number, 1, index));
            self.accounts.insert(account);
            call(2, index, account, 1_000_000_000, Bytes::new());
        }
        for index in [2 * number, 2 * number + 1].into_iter().filter(|i| *i < EMPTY_ACCOUNTS) {
            self.accounts.insert(empty_account(index));
            call(3, index, empty_account(index), 0, Bytes::new());
        }
        let value = U256::from(number);
        for index in 0..SLOTS_PER_BLOCK {
            let slot = seed(number, 4, index);
            self.slots.insert((STORAGE_CONTRACT, slot));
            call(5, index, STORAGE_CONTRACT, 0, store_call(slot, value));
        }
        for index in 0..SLOTS_PER_BLOCK / 2 {
            let zeroed = seed(number.saturating_sub(1), 4, index);
            call(6, index, STORAGE_CONTRACT, 0, store_call(zeroed, U256::ZERO));
            let reset = seed(number.saturating_sub(2), 4, index);
            call(7, index, STORAGE_CONTRACT, 0, store_call(reset, value));
        }
        if number > DEPLOY_BLOCK {
            for (index, (slot_block, value)) in
                [(number, value), (number - 1, U256::ZERO), (number - 2, value)]
                    .into_iter()
                    .enumerate()
            {
                let slot = seed(slot_block, 8, 0);
                self.slots.insert((deployed(), slot));
                call(9, index as u64, deployed(), 0, store_call(slot, value));
            }
        }
        let mut create = |from: Address, init: &[u8]| {
            let source = seed(number, 10, u64::from(from.0[0]));
            transactions.push(deposit_from(
                from,
                source,
                TxKind::Create,
                0,
                Bytes::copy_from_slice(init),
            ));
        };
        if number == DEPLOY_BLOCK {
            create(DEPLOYER, &DEPLOY_INIT);
        }
        if number == DESTROY_BLOCK {
            create(DESTROYER, &DESTROYED_INIT);
        }
        let factory_call = |value| {
            deposit(
                seed(number, 11, 0),
                TxKind::Call(FACTORY),
                value,
                Bytes::from_static(&CHILD_INIT),
            )
        };
        if number == DESTROY_BLOCK {
            transactions.push(factory_call(0));
        }
        if number == RECREATE_BLOCK {
            transactions.push(factory_call(1));
        }
        transactions
    }

    /// Every touched account (and the contracts and senders) and every touched slot.
    fn keys(&self) -> (Vec<Address>, Vec<(Address, B256)>) {
        let mut accounts = self.accounts.clone();
        accounts.extend([
            STORAGE_CONTRACT,
            FACTORY,
            DEPOSITOR,
            DEPLOYER,
            DESTROYER,
            deployed(),
            destroyed(),
            child(),
        ]);
        let mut slots = self.slots.clone();
        slots.extend([
            (destroyed(), B256::ZERO),
            (child(), B256::ZERO),
            (child(), B256::with_last_byte(1)),
        ]);
        (accounts.into_iter().collect(), slots.into_iter().collect())
    }
}

/// The producer, the chain it built (block `n` at index `n - 1`) and the workload.
struct Chain {
    producer: TestNode,
    blocks: Vec<BaseExecutionPayloadEnvelopeV4>,
    workload: Workload,
}

impl Chain {
    /// Produces the next block and imports it into `nodes`.
    async fn advance(&mut self, nodes: &[&TestNode]) -> Result<()> {
        let number = self.blocks.len() as u64 + 1;
        let transactions = self.workload.block(number);
        self.advance_with(transactions, nodes).await
    }

    /// Produces the next block with `transactions` instead of the workload's, and imports it into
    /// `nodes`.
    async fn advance_with(&mut self, transactions: Vec<Bytes>, nodes: &[&TestNode]) -> Result<()> {
        let envelope = self.producer.produce(transactions).await?;
        for node in nodes {
            node.import(&envelope).await?;
        }
        self.blocks.push(envelope);
        Ok(())
    }

    /// Produces blocks up to `head`, importing each into `nodes`.
    async fn advance_to(&mut self, head: u64, nodes: &[&TestNode]) -> Result<()> {
        while (self.blocks.len() as u64) < head {
            self.advance(nodes).await?;
        }
        Ok(())
    }

    /// Imports the chain into `nodes` one block at a time, every node getting a block before
    /// any gets the next, as nodes following one consensus client do.
    async fn catch_up(&self, nodes: &[&TestNode]) -> Result<()> {
        for (index, envelope) in self.blocks.iter().enumerate() {
            for node in nodes {
                if node.head().await?.1 <= index as u64 {
                    node.import(envelope).await?;
                }
            }
        }
        Ok(())
    }
}

/// Three storing nodes and a non-storing apply node (id 4, applying from 1 and 2), all
/// splitting [`STORAGE_CONTRACT`].
fn configs() -> Result<Vec<ClusterConfig>> {
    let nodes = (1..=4)
        .map(|id| {
            Ok(ClusterNodeConfig { id: NodeId(id), address: free_address()?, storage: id < 4 })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok((1..=4)
        .map(|id| ClusterConfig {
            split_storage: vec![Bytes::copy_from_slice(STORAGE_CONTRACT.as_slice())],
            apply_from: if id == 4 { vec![NodeId(1), NodeId(2)] } else { vec![] },
            ..cluster_config(id, &nodes)
        })
        .collect())
}

#[tokio::test(flavor = "multi_thread")]
async fn cluster_follows_the_chain_through_restarts() -> Result<()> {
    base_node_runner::test_utils::init_silenced_tracing();
    let chain_spec = chain_spec()?;
    let configs = configs()?;
    let producer = TestNode::launch(&chain_spec, None).await?;
    let mut storing = Vec::new();
    for config in &configs[..3] {
        storing.push(TestNode::launch_from_full_datadir(&chain_spec, config.clone()).await?);
    }
    let [a, b, c]: [TestNode; 3] = storing.try_into().map_err(|_| eyre!("three storing nodes"))?;
    let d = TestNode::launch(&chain_spec, Some(configs[3].clone())).await?;
    let mut chain = Chain { producer, blocks: Vec::new(), workload: Workload::default() };

    // Everyone follows, and every node's background check verifies blocks.
    chain.advance_to(6, &[&a, &b, &c, &d]).await?;
    for node in [&a, &b, &c, &d] {
        node.wait_root_verified(6 - RootCheckSchedule::LAG_BLOCKS).await?;
    }

    // A stops; the others go well past its persisted block; A restarts and catches up.
    a.wait_persisted(6).await?;
    let a = a.stop().await?;
    chain.advance_to(12, &[&b, &c, &d]).await?;
    let a = a.launch(Some(configs[0].clone())).await?;
    assert_eq!(a.persisted()?, 6);
    chain.catch_up(&[&a]).await?;

    // A serves: with B down, A is the only owner of the shards it shares with B.
    let b = b.stop().await?;
    let served_before = a.cluster().owner_store().stats().counts().served;
    chain.advance_to(14, &[&a, &c, &d]).await?;
    assert!(a.cluster().owner_store().stats().counts().served > served_before, "A served no read");

    // Every cluster node stops, at blocks 12 (B), 14 (D) and 16 (A and C).
    d.wait_persisted(14).await?;
    let d = d.stop().await?;
    chain.advance_to(16, &[&a, &c]).await?;
    a.wait_persisted(16).await?;
    c.wait_persisted(16).await?;
    let (a, c) = (a.stop().await?, c.stop().await?);

    // They restart in an arbitrary order and all continue.
    let d = d.launch(Some(configs[3].clone())).await?;
    let c = c.launch(Some(configs[2].clone())).await?;
    let b = b.launch(Some(configs[1].clone())).await?;
    let a = a.launch(Some(configs[0].clone())).await?;
    assert_eq!([b.persisted()?, d.persisted()?, a.persisted()?, c.persisted()?], [12, 14, 16, 16]);
    // D catches up first: its executors have not re-executed those blocks, so it executes.
    chain.catch_up(&[&d, &c, &b, &a]).await?;
    let nodes = [&a, &b, &c, &d];
    chain.advance_to(24, &nodes).await?;
    let head = chain.producer.head().await?;
    for node in [&chain.producer, &a, &b, &c, &d] {
        node.wait_persisted(head.1).await?;
    }

    // The workload did what it claims, on the reference.
    {
        let reference = chain.producer.launched.handle.node.provider();
        let genesis = reference.history_by_block_number(0)?;
        let latest = reference.latest()?;
        assert!(genesis.basic_account(&empty_account(2))?.is_some(), "no empty genesis account");
        assert_eq!(
            latest.basic_account(&empty_account(2))?,
            None,
            "a touched empty account survived"
        );
        assert_eq!(latest.basic_account(&destroyed())?, None, "a destroyed contract survived");
        assert_eq!(latest.basic_account(&child())?.map(|account| account.nonce), Some(1));
        assert!(latest.account_code(&deployed())?.is_some(), "the contract was not deployed");
    }

    // The restarted nodes' checks verify blocks after every restart, the non-storing D's
    // included, and find no mismatch. Every restarted storing node seeded its captures over
    // replicated rows that hold stale hashes of shards it does not hold.
    for (name, node) in [("A", &a), ("B", &b), ("C", &c), ("D", &d)] {
        let counts = node.wait_root_verified(head.1 - RootCheckSchedule::LAG_BLOCKS).await?;
        assert!(counts.verified > 0, "node {name}: {counts:?}");
        assert_eq!(counts.mismatched, 0, "node {name}: {counts:?}");
        println!(
            "ROOTCHECK node {name}: {counts:?} last {:?}",
            node.cluster().root_check_stats().last_verified()
        );
    }
    for (name, node) in [("A", &a), ("B", &b), ("C", &c)] {
        let cluster = node.cluster().cluster();
        let held = foreign_child_hashes(&node.env, cluster)?;
        let truth = foreign_child_hashes(&chain.producer.env, cluster)?;
        let stale = held.iter().filter(|(child, hash)| truth.get(*child) != Some(*hash)).count();
        assert!(stale > 0, "node {name}: no stale hash of a shard it does not hold");
    }

    // a. Every node is at the head and reads the reference's state.
    let (accounts, slots) = chain.workload.keys();
    let expected = chain.producer.state_of(&accounts, &slots)?;
    for (name, node) in [("A", &a), ("B", &b), ("C", &c), ("D", &d)] {
        assert_eq!(node.head().await?, head, "node {name}");
        assert_eq!(node.state_of(&accounts, &slots)?, expected, "node {name}");
    }
    let applied = d.cluster().applied_blocks();
    assert!(applied.applied > 0, "the apply node applied no block");
    assert!(applied.fallbacks.iter().sum::<u64>() > 0, "the apply node never executed");

    // b. Every storing node's held trie rows equal the reference's.
    for (name, node) in [("A", &a), ("B", &b), ("C", &c)] {
        let cluster = node.cluster().cluster();
        let rows = SliceRows::read(&node.env, cluster)?;
        assert_eq!(rows, SliceRows::read(&chain.producer.env, cluster)?, "node {name}");
        assert!(rows.accounts_trie.len() > 50, "node {name}: few account-trie rows compared");
    }
    let side_calls: u64 =
        [&a, &b, &c].iter().map(|node| node.cluster().trie_slices().side_calls).sum();
    assert!(side_calls > 0, "no node maintained a split range of a foreign account");

    // c. Only value reads went to owners, none failed, and no owner refused one: an ordered or
    // trie request would be refused as unsupported.
    let mut value_reads = 0;
    for (name, node) in nodes.iter().enumerate() {
        let reads = node.cluster().remote_reads();
        assert_eq!(reads.failures, 0, "node {name}: {reads:?}");
        value_reads += reads.accounts + reads.storages;
        let OwnerCounts { refused, .. } = node.cluster().owner_store().stats().counts();
        assert_eq!(refused, 0, "node {name} refused a read");
    }
    assert!(value_reads > 0, "no remote value read happened");

    for node in [chain.producer, a, b, c, d] {
        node.remove().await?;
    }
    Ok(())
}

/// The hashes of the shards `cluster`'s node does not hold, as the replicated one-nibble
/// account-trie rows in `env` store them: the node never maintains them.
fn foreign_child_hashes(env: &DatabaseEnv, cluster: &Cluster) -> Result<BTreeMap<Shard, B256>> {
    let mut hashes = BTreeMap::new();
    for entry in env.tx()?.cursor_read::<tables::PackedAccountsTrie>()?.walk(None)? {
        let (path, node) = entry?;
        if path.0.len() != 1 {
            continue;
        }
        for child in (0..16u8).filter(|child| node.hash_mask.is_bit_set(*child)) {
            let shard = Shard(path.0.get_unchecked(0) << 4 | child);
            if !cluster.owns(shard) {
                hashes.insert(shard, node.hash_for_nibble(child));
            }
        }
    }
    Ok(hashes)
}

/// Blocks the owners go past a stopped node: more than the minimum history window.
const PAST_WINDOW: u64 = ClusterConfig::MIN_RETENTION_BLOCKS + 15;

/// A storing node restarted below every owner's history window does not execute: each new
/// payload fails with an error naming the owners' servable ranges, and the block is not recorded
/// as invalid.
#[tokio::test(flavor = "multi_thread")]
async fn a_node_below_the_owners_history_is_refused_with_their_range() -> Result<()> {
    base_node_runner::test_utils::init_silenced_tracing();
    let chain_spec = chain_spec_with(256, 0, [])?;
    let configs = configs()?;
    let producer = TestNode::launch(&chain_spec, None).await?;
    let a = TestNode::launch(&chain_spec, Some(configs[0].clone())).await?;
    let b = TestNode::launch(&chain_spec, Some(configs[1].clone())).await?;
    let c = TestNode::launch(&chain_spec, Some(configs[2].clone())).await?;
    let mut chain = Chain { producer, blocks: Vec::new(), workload: Workload::default() };
    for _ in 0..2 {
        let envelope = chain.producer.produce(vec![]).await?;
        for node in [&a, &b, &c] {
            node.import(&envelope).await?;
        }
        chain.blocks.push(envelope);
    }
    a.wait_persisted(2).await?;
    let a = a.stop().await?;
    for _ in 2..PAST_WINDOW {
        let envelope = chain.producer.produce(vec![]).await?;
        b.import(&envelope).await?;
        c.import(&envelope).await?;
        chain.blocks.push(envelope);
    }
    b.wait_persisted(PAST_WINDOW).await?;
    c.wait_persisted(PAST_WINDOW).await?;
    let a = a.launch(Some(configs[0].clone())).await?;

    let lowest = PAST_WINDOW - ClusterConfig::MIN_RETENTION_BLOCKS;
    for _ in 0..2 {
        let refusal = a.submit(&chain.blocks[2]).await;
        let message = format!("{refusal:?}");
        assert!(message.contains("persisted block 2 is below the history"), "{message}");
        assert!(message.contains(&format!("{lowest}..={PAST_WINDOW}")), "{message}");
    }
    assert_eq!(a.head().await?.1, 2);

    for node in [chain.producer, a, b, c] {
        node.remove().await?;
    }
    Ok(())
}

/// A storing node whose owners are behind or down for longer than its read budget does not
/// execute the block: the payload fails with a retryable error, is not recorded as invalid, and
/// is accepted once the owners catch up or come back.
#[tokio::test(flavor = "multi_thread")]
async fn a_node_waits_for_its_owners_and_never_marks_the_block_invalid() -> Result<()> {
    base_node_runner::test_utils::init_silenced_tracing();
    let chain_spec = chain_spec_with(256, 0, [])?;
    let configs = configs()?;
    let producer = TestNode::launch(&chain_spec, None).await?;
    let impatient = ClusterConfig { read_retry_secs: 1, ..configs[0].clone() };
    let x = TestNode::launch(&chain_spec, Some(impatient)).await?;
    let b = TestNode::launch(&chain_spec, Some(configs[1].clone())).await?;
    let c = TestNode::launch(&chain_spec, Some(configs[2].clone())).await?;
    let mut chain = Chain { producer, blocks: Vec::new(), workload: Workload::default() };

    // X persists block 1 before its owners have it, then gets block 2.
    chain.advance(&[&x]).await?;
    x.wait_persisted(1).await?;
    chain.advance(&[]).await?;
    let behind = x.submit(&chain.blocks[1]).await;
    let message = format!("{behind:?}");
    assert!(message.contains("could serve its persisted block 1"), "{message}");
    chain.catch_up(&[&b, &c]).await?;
    assert_eq!(x.submit(&chain.blocks[1]).await?, PayloadStatusEnum::Valid);
    x.import(&chain.blocks[1]).await?;

    // Both owners of every shard X does not hold go down, and come back.
    chain.advance(&[&b, &c]).await?;
    let (b, c) = (b.stop().await?, c.stop().await?);
    let down = x.submit(&chain.blocks[2]).await;
    assert!(format!("{down:?}").contains("unreachable"), "{down:?}");
    let c = c.launch(Some(configs[2].clone())).await?;
    let b = b.launch(Some(configs[1].clone())).await?;
    x.import(&chain.blocks[2]).await?;
    chain.advance(&[&x, &b, &c]).await?;
    assert_eq!(x.head().await?, chain.producer.head().await?);

    for node in [chain.producer, x, b, c] {
        node.remove().await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_node_with_the_root_check_off_captures_nothing() -> Result<()> {
    base_node_runner::test_utils::init_silenced_tracing();
    let chain_spec = chain_spec_with(256, 0, [])?;
    let nodes = [ClusterNodeConfig { id: NodeId(1), address: free_address()?, storage: true }];
    let off = ClusterConfig {
        replication: 1,
        root_check: RootCheckConfig { mode: RootCheckMode::Off, ..RootCheckConfig::default() },
        ..cluster_config(1, &nodes)
    };
    let producer = TestNode::launch(&chain_spec, None).await?;
    let node = TestNode::launch(&chain_spec, Some(off)).await?;
    let mut chain = Chain { producer, blocks: Vec::new(), workload: Workload::default() };

    chain.advance_to(3, &[&node]).await?;

    let (head, number) = node.head().await?;
    assert_eq!(number, 3);
    assert!(node.cluster().root_captures().by_hash(head).is_none(), "block {number} captured");
    assert!(node.cluster().trie_slices().capture_elapsed.is_zero());
    for node in [chain.producer, node] {
        node.remove().await?;
    }
    Ok(())
}

/// Launches a node with its HTTP RPC server on, and returns it with a client of that server.
async fn launch_with_http(
    chain_spec: &Arc<base_execution_chainspec::BaseChainSpec>,
    cluster: Option<ClusterConfig>,
) -> Result<(TestNode, HttpClient)> {
    let mut datadir = Datadir::fresh(chain_spec);
    datadir.node_config.rpc.http = true;
    let node = datadir.launch(cluster).await?;
    let client = node
        .launched
        .handle
        .node
        .rpc_server_handle()
        .http_client()
        .ok_or_else(|| eyre!("the HTTP RPC server is off"))?;
    Ok((node, client))
}

#[tokio::test(flavor = "multi_thread")]
async fn a_clustered_node_refuses_proof_rpcs_and_says_why() -> Result<()> {
    base_node_runner::test_utils::init_silenced_tracing();
    let chain_spec = chain_spec_with(256, 0, [])?;
    let nodes = [ClusterNodeConfig { id: NodeId(1), address: free_address()?, storage: true }];
    let config = ClusterConfig { replication: 1, ..cluster_config(1, &nodes) };
    let (unclustered, full) = launch_with_http(&chain_spec, None).await?;
    let (clustered, sliced) = launch_with_http(&chain_spec, Some(config)).await?;
    let proof = || rpc_params![DEPOSITOR, Vec::<B256>::new(), "latest"];

    let answered: Result<serde_json::Value, _> = full.request("eth_getProof", proof()).await;
    assert!(answered.is_ok(), "an unclustered node answers: {answered:?}");
    for (method, params) in
        [("eth_getProof", proof()), ("eth_getAccount", rpc_params![DEPOSITOR, "latest"])]
    {
        let refused: Result<serde_json::Value, _> = sliced.request(method, params).await;
        let message = format!("{refused:?}");
        assert!(
            message.contains(&format!("{method} is not supported on a clustered node")),
            "{message}"
        );
    }
    for node in [unclustered, clustered] {
        node.remove().await?;
    }
    Ok(())
}

/// The account-trie rows of two nibbles in `env`, by path.
fn two_nibble_rows(env: &DatabaseEnv) -> Result<BTreeMap<Nibbles, BranchNodeCompact>> {
    let mut rows = BTreeMap::new();
    for entry in env.tx()?.cursor_read::<tables::PackedAccountsTrie>()?.walk(None)? {
        let (path, node) = entry?;
        if path.0.len() == 2 {
            rows.insert(path.0, node);
        }
    }
    Ok(rows)
}

/// Every shard's hash, as the one-nibble account-trie rows of a full node's `env` store them.
fn shard_hashes(env: &DatabaseEnv) -> Result<BTreeMap<Shard, ShardHash>> {
    let mut hashes = BTreeMap::new();
    for entry in env.tx()?.cursor_read::<tables::PackedAccountsTrie>()?.walk(None)? {
        let (path, node) = entry?;
        if path.0.len() == 1 {
            for child in (0..16u8).filter(|child| node.hash_mask.is_bit_set(*child)) {
                let shard = Shard(path.0.get_unchecked(0) << 4 | child);
                hashes.insert(shard, ShardHash::Hash(node.hash_for_nibble(child)));
            }
        }
    }
    Ok(hashes)
}

/// A storing node's held trie row is corrupted on disk, and the next block touching that shard
/// gives the node a wrong hash of it. In log mode, every node that asks that owner for the shard
/// counts a mismatch, the other owner of the shard asks itself and verifies the block, and the
/// mismatch names the shard and the owner. In exit mode the mismatch writes the marker, and the
/// node then refuses to start.
#[tokio::test(flavor = "multi_thread")]
async fn a_corrupted_trie_row_is_reported_by_the_nodes_that_ask_its_owner() -> Result<()> {
    base_node_runner::test_utils::init_silenced_tracing();
    let chain_spec = chain_spec()?;
    let configs = configs()?;
    let producer = TestNode::launch(&chain_spec, None).await?;
    let mut storing = Vec::new();
    for config in &configs[..3] {
        storing.push(TestNode::launch_from_full_datadir(&chain_spec, config.clone()).await?);
    }
    let [a, b, c]: [TestNode; 3] = storing.try_into().map_err(|_| eyre!("three storing nodes"))?;
    let d = TestNode::launch(&chain_spec, Some(configs[3].clone())).await?;
    let mut chain = Chain { producer, blocks: Vec::new(), workload: Workload::default() };
    let at_genesis = two_nibble_rows(&chain.producer.env)?;
    for _ in 0..3 {
        chain.advance_with(Vec::new(), &[&a, &b, &c, &d]).await?;
    }
    for node in [&a, &b, &c, &d] {
        node.wait_root_verified(1).await?;
    }

    // A shard whose first owner is A, that blocks without user deposits leave alone, and whose
    // two-nibble row has a stored child hash.
    let cluster = Arc::clone(a.cluster().cluster());
    let corrupt = NodeId(1);
    let excluded = [keccak256(STORAGE_CONTRACT)[0], keccak256(DEPOSITOR)[0]];
    chain.producer.wait_persisted(3).await?;
    let unchanged = two_nibble_rows(&chain.producer.env)?;
    let (path, child) = unchanged
        .iter()
        .filter(|(path, node)| at_genesis.get(*path) == Some(*node))
        .filter_map(|(path, node)| {
            let shard = path.get_unchecked(0) << 4 | path.get_unchecked(1);
            let child = (0..16u8).find(|child| node.hash_mask.is_bit_set(*child))?;
            (cluster.owners(Shard(shard))[0] == corrupt && !excluded.contains(&shard))
                .then_some((*path, child))
        })
        .next()
        .ok_or_else(|| eyre!("no shard to corrupt"))?;
    let shard = Shard(path.get_unchecked(0) << 4 | path.get_unchecked(1));

    // A stops; one stored child hash of the shard's row is overwritten; A restarts.
    a.wait_persisted(3).await?;
    let stopped = a.stop().await?;
    {
        let env = stopped.open().await?;
        let tx = env.tx_mut()?;
        let mut node = tx
            .get::<tables::PackedAccountsTrie>(PackedStoredNibbles(path))?
            .ok_or_else(|| eyre!("A does not hold the row"))?;
        let mut hashes = (*node.hashes).clone();
        hashes[0] = B256::repeat_byte(0xbd);
        node.hashes = hashes.into();
        tx.put::<tables::PackedAccountsTrie>(PackedStoredNibbles(path), node)?;
        tx.commit()?;
    }
    let a = stopped.launch(Some(configs[0].clone())).await?;

    // Block 4 creates an account in the shard under another child than the corrupted one.
    let target = (0u64..)
        .map(|index| Address::from_word(seed(4, 20, index)))
        .find(|address| {
            let hashed = keccak256(address);
            hashed[0] == shard.0 && hashed[1] >> 4 != child
        })
        .ok_or_else(|| eyre!("no address in the shard"))?;
    let deposit = deposit(seed(4, 21, 0), TxKind::Call(target), 1_000_000_000, Bytes::new());
    let nodes = [&a, &b, &c, &d];
    chain.advance_with(vec![deposit], &nodes).await?;
    chain.producer.wait_persisted(4).await?;
    let reference_row = two_nibble_rows(&chain.producer.env)?.remove(&path);
    assert_eq!(
        reference_row.map(|node| node.hash_for_nibble(child)),
        Some(at_genesis[&path].hash_for_nibble(child)),
        "block 4 recomputed the corrupted child"
    );
    let truth = shard_hashes(&chain.producer.env)?;

    // Checked from D, which holds nothing: the mismatch names the shard and its owner A, and
    // every other account shard has the reference's hash.
    let header =
        chain.blocks[3].execution_payload.payload_inner.payload_inner.payload_inner.clone();
    let block = CheckedBlock { number: 4, hash: header.block_hash, state_root: header.state_root };
    let source = ClusterHashSource { node: d.cluster() };
    let outcome = tokio::task::block_in_place(|| {
        RootChecker::new(d.cluster().cluster(), &source).check(block)
    });
    let CheckOutcome::Mismatch(mismatch) = outcome else {
        return Err(eyre!("expected a mismatch, got {outcome:?}"));
    };
    let wrong: Vec<_> = mismatch
        .sources
        .iter()
        .filter(|source| {
            source.contract.is_none() && truth.get(&source.shard) != Some(&source.value)
        })
        .map(|source| (source.shard, source.owner))
        .collect();
    assert_eq!(wrong, vec![(shard, corrupt)]);

    // In the background, the nodes that ask A for the shard count the mismatch; the other owner
    // of the shard asks itself and verifies the block.
    for _ in 0..RootCheckSchedule::LAG_BLOCKS {
        chain.advance_with(Vec::new(), &nodes).await?;
    }
    for node in nodes {
        let view = node.cluster().cluster();
        let me = view.node_id();
        let asked = if view.owns(shard) { me } else { view.owners(shard)[0] };
        if asked == corrupt {
            node.wait_root_checks("a mismatch", |counts, _| counts.mismatched > 0).await?;
        } else {
            let counts = node.wait_root_verified(4).await?;
            assert_eq!(counts.mismatched, 0, "node {me:?} asked node {asked:?}");
        }
    }

    // Exit mode writes the marker naming the shard and owner, and D then refuses to start.
    let datadir = d.datadir.node_config.datadir().data_dir().to_path_buf();
    assert!(RootCheckMode::Exit.on_mismatch(&mismatch, &datadir));
    let marker = std::fs::read_to_string(RootMismatchMarker::path(&datadir))?;
    assert!(marker.contains(&format!("{:02x}={}:", shard.0, corrupt.0)), "{marker}");
    let d_path = d.datadir.path.clone();
    let refused = d.stop().await?.launch(Some(configs[3].clone())).await;
    let message = format!("{:?}", refused.err());
    assert!(message.contains("cluster state-root mismatch was recorded"), "{message}");
    let _ = std::fs::remove_dir_all(d_path);

    for node in [chain.producer, a, b, c] {
        node.remove().await?;
    }
    Ok(())
}
