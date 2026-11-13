//! Apply mode end to end: clustered nodes driven through the in-memory Engine API, one executing
//! payloads and others inserting them from its execution results, or executing them when no valid
//! result arrives.

use std::{
    net::{SocketAddr, TcpListener},
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};

use alloy_eips::{BlockNumberOrTag, Encodable2718, eip7685::Requests};
use alloy_genesis::GenesisAccount;
use alloy_primitives::{Address, B64, B256, Bytes, TxKind, U256, keccak256};
use alloy_provider::{Provider, RootProvider};
use alloy_rpc_types_engine::{ForkchoiceState, JwtSecret, PayloadAttributes};
use base_common_consensus::{BaseReceipt, JovianExtraData, TxDeposit};
use base_common_genesis::RollupConfig;
use base_common_network::{Base, BaseEngineApi};
use base_common_rpc_types_engine::{BaseExecutionPayloadEnvelopeV4, BasePayloadAttributes};
use base_consensus_engine::{
    BaseEngineClient, EngineClient, EngineClientBuilder, EngineEndpoint, InMemoryTransport,
};
use base_consensus_providers::L1_RPC_TIMEOUT;
use base_execution_chainspec::BaseChainSpec;
use base_execution_cluster::{
    ApplyCounts, ApplyFallback, Cluster, ClusterConfig, ClusterNode, ClusteredDatabase,
    ExecutedBlockResult, ExecutedBlocks, NodeConfig as ClusterNodeConfig, NodeId, OwnerServer,
    OwnerStore,
};
use base_node_core::args::RollupArgs;
use base_node_runner::{
    BaseNodeRunner, LaunchedBaseNode,
    test_utils::{BLOCK_TIME_SECONDS, L1_BLOCK_INFO_DEPOSIT_TX},
};
use base_test_utils::build_test_genesis;
use eyre::{Result, eyre};
use reth_db::{
    ClientVersion, Database, DatabaseEnv,
    cursor::DbCursorRO,
    init_db,
    mdbx::{DatabaseArguments, RO, tx::Tx},
    table::Table,
    tables::{self, RawTable},
    test_utils::tempdir_path,
    transaction::DbTx,
};
use reth_node_builder::{NodeBuilder, NodeConfig};
use reth_node_core::{
    args::{DatadirArgs, DiscoveryArgs, NetworkArgs, RpcServerArgs},
    dirs::{DataDirPath, MaybePlatformPath},
};
use reth_provider::{
    AccountReader, ChangeSetReader, StateProvider, StateProviderFactory, StorageChangeSetReader,
};
use reth_tasks::Runtime;

/// Blocks each test produces.
const BLOCKS: u64 = 3;

/// Accounts funded at genesis beyond the test genesis's own, so the account trie has stored
/// branch nodes for blocks to change.
const GENESIS_ACCOUNTS: u64 = 512;

/// User deposits per block of each kind: funding a fresh account, and writing a fresh slot of
/// [`STORAGE_CONTRACT`].
const DEPOSITS_PER_KIND: u64 = 24;

/// A genesis contract that stores the block number at the slot given by its first calldata word:
/// `PUSH0 CALLDATALOAD NUMBER SWAP1 SSTORE STOP`.
const STORAGE_CONTRACT: Address = Address::repeat_byte(0xc0);
const STORAGE_CONTRACT_CODE: [u8; 6] = [0x5f, 0x35, 0x43, 0x90, 0x55, 0x00];

/// The transactions of block `number`: the L1 info deposit, then user deposits that create
/// accounts and storage slots, so blocks change the account and storage tries.
fn block_transactions(number: u64) -> Vec<Bytes> {
    let seed = |kind, index| deposit_seed(number, kind, index);
    let deposit = |source_hash, to, mint, input| {
        TxDeposit {
            source_hash,
            from: Address::repeat_byte(0xde),
            to: TxKind::Call(to),
            mint,
            value: U256::ZERO,
            gas_limit: 100_000,
            is_system_transaction: false,
            input,
        }
        .encoded_2718()
        .into()
    };
    let mut transactions = vec![L1_BLOCK_INFO_DEPOSIT_TX];
    for index in 0..DEPOSITS_PER_KIND {
        let account = Address::from_word(seed(1, index));
        transactions.push(deposit(seed(2, index), account, 1_000_000_000, Bytes::new()));
        let slot = Bytes::from(seed(3, index).to_vec());
        transactions.push(deposit(seed(4, index), STORAGE_CONTRACT, 0, slot));
    }
    transactions
}

/// The seed of the `index`th deposit value of `kind` in block `number`: kind 1 is a recipient
/// account, 3 a written slot of [`STORAGE_CONTRACT`], 2 and 4 their deposits' source hashes.
fn deposit_seed(number: u64, kind: u8, index: u64) -> B256 {
    keccak256([[kind].as_slice(), &number.to_be_bytes(), &index.to_be_bytes()].concat())
}

/// A table's name and its raw `(key, value)` rows, in key order.
type TableDump = (&'static str, Vec<(Vec<u8>, Vec<u8>)>);

/// A launched clustered node and the handles a test drives and inspects it through.
struct TestNode {
    launched: LaunchedBaseNode,
    env: DatabaseEnv,
    cluster: Arc<ClusterNode>,
    engine: BaseEngineClient<RootProvider, RootProvider<Base>>,
    reads: RootProvider<Base>,
    data_path: PathBuf,
}

impl TestNode {
    /// Launches node `node_id` of `nodes`, serving peers on `listener`, applying from
    /// `apply_from` and splitting the storage of the `split_storage` contracts.
    async fn launch(
        runtime: &Runtime,
        chain_spec: &Arc<BaseChainSpec>,
        node_id: u64,
        nodes: &[ClusterNodeConfig],
        listener: TcpListener,
        apply_from: Vec<NodeId>,
        split_storage: Vec<Bytes>,
    ) -> Result<Self> {
        let network = NetworkArgs {
            discovery: DiscoveryArgs { disable_discovery: true, ..DiscoveryArgs::default() },
            ..NetworkArgs::default()
        };
        let mut node_config = NodeConfig::new(Arc::clone(chain_spec))
            .with_network(network)
            .with_rpc(RpcServerArgs::default().with_unused_ports())
            .with_unused_ports();
        node_config.engine.persistence_threshold = 0;
        let data_path = tempdir_path();
        node_config = node_config.with_datadir_args(DatadirArgs {
            datadir: MaybePlatformPath::<DataDirPath>::from(data_path.clone()),
            ..Default::default()
        });
        let env = init_db(
            node_config.datadir().db(),
            DatabaseArguments::new(ClientVersion::default())
                .with_geometry_max_size(Some(100 * 1024 * 1024)),
        )?;
        let cluster = Cluster::new(ClusterConfig {
            node_id: NodeId(node_id),
            replication: 2,
            retention_blocks: ClusterConfig::MIN_RETENTION_BLOCKS,
            nodes: nodes.to_vec(),
            apply_from,
            split_storage,
            read_retry_secs: ClusterConfig::DEFAULT_READ_RETRY_SECS,
        })?;
        let db = ClusteredDatabase::clustered(
            env.clone(),
            Arc::new(cluster),
            listener,
            Default::default(),
        )?;
        let cluster = Arc::clone(db.cluster().ok_or_else(|| eyre!("database is not clustered"))?);
        let launched = BaseNodeRunner::new(RollupArgs::default())
            .launch(
                NodeBuilder::new(node_config)
                    .with_database(db)
                    .with_launch_context(runtime.clone()),
            )
            .await?;
        let endpoint = EngineEndpoint::InMemory {
            transport: InMemoryTransport { module: launched.auth_module.clone() },
            engine_api: launched.engine_api.clone(),
        };
        let reads: RootProvider<Base> = endpoint.clone().connect(JwtSecret::random()).await?;
        let engine = EngineClientBuilder {
            l2: endpoint,
            l2_jwt: JwtSecret::random(),
            l1_rpc: "http://127.0.0.1:8545".parse()?,
            l1_rpc_timeout: L1_RPC_TIMEOUT,
            cfg: Arc::new(RollupConfig::default()),
        }
        .build()
        .await?;
        Ok(Self { launched, env, cluster, engine, reads, data_path })
    }

    /// Builds the next block on this node's head and returns its payload, without inserting it.
    async fn build_next(&self) -> Result<BaseExecutionPayloadEnvelopeV4> {
        let parent = self
            .engine
            .l2_block_by_label(BlockNumberOrTag::Latest)
            .await?
            .ok_or_else(|| eyre!("missing head block"))?;
        let base_fee_params = self
            .launched
            .handle
            .node
            .chain_spec()
            .base_fee_params_at_timestamp(parent.header.timestamp);
        let transactions = block_transactions(parent.header.number + 1);
        let attributes = BasePayloadAttributes {
            payload_attributes: PayloadAttributes {
                timestamp: parent.header.timestamp + BLOCK_TIME_SECONDS,
                prev_randao: B256::ZERO,
                suggested_fee_recipient: parent.header.beneficiary,
                withdrawals: Some(vec![]),
                parent_beacon_block_root: Some(
                    parent.header.parent_beacon_block_root.unwrap_or(B256::ZERO),
                ),
                slot_number: None,
                target_gas_limit: None,
            },
            transactions: Some(transactions.clone()),
            no_tx_pool: Some(true),
            gas_limit: Some(parent.header.gas_limit),
            eip_1559_params: Some(B64::from(
                ((base_fee_params.max_change_denominator as u64) << 32)
                    | base_fee_params.elasticity_multiplier as u64,
            )),
            min_base_fee: parent.header.base_fee_per_gas,
        };
        let head = parent.header.hash;
        let state = ForkchoiceState {
            head_block_hash: head,
            safe_block_hash: head,
            finalized_block_hash: head,
        };
        let started = self.engine.fork_choice_updated_v3(state, Some(attributes)).await?;
        let payload_id =
            started.payload_id.ok_or_else(|| eyre!("forkchoice did not start a payload"))?;
        tokio::time::sleep(Duration::from_millis(200)).await;
        let envelope = self.engine.get_payload_v4(payload_id).await?;
        let built = &envelope.execution_payload.payload_inner.payload_inner.payload_inner;
        if built.transactions != transactions {
            return Err(eyre!("payload does not carry the block's transactions"));
        }
        Ok(envelope)
    }

    /// Inserts `envelope` and makes it the head, as a consensus client does.
    async fn import(&self, envelope: &BaseExecutionPayloadEnvelopeV4) -> Result<B256> {
        let status = self
            .engine
            .new_payload_v4(envelope.execution_payload.clone(), envelope.parent_beacon_block_root)
            .await?;
        if !status.status.is_valid() {
            return Err(eyre!("payload rejected: {status:?}"));
        }
        let hash = status.latest_valid_hash.ok_or_else(|| eyre!("payload status missing hash"))?;
        let state = ForkchoiceState {
            head_block_hash: hash,
            safe_block_hash: hash,
            finalized_block_hash: hash,
        };
        self.engine.fork_choice_updated_v3(state, None).await?;
        Ok(hash)
    }

    /// Waits until block `number` is persisted: the `Finish` checkpoint reaches it.
    async fn wait_persisted(&self, number: u64) -> Result<()> {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let tx = self.env.tx()?;
            if ClusterNode::finish_checkpoint(&tx)? >= number {
                return Ok(());
            }
            drop(tx);
            if Instant::now() >= deadline {
                return Err(eyre!("block {number} was not persisted"));
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// The head block's hash and number.
    async fn head(&self) -> Result<(B256, u64)> {
        let head = self
            .reads
            .get_block_by_number(BlockNumberOrTag::Latest)
            .await?
            .ok_or_else(|| eyre!("missing head block"))?;
        Ok((head.header.hash, head.header.number))
    }

    /// The receipts of blocks 1 to `BLOCKS`, as JSON.
    async fn receipts(&self) -> Result<Vec<serde_json::Value>> {
        let mut receipts = Vec::new();
        for number in 1..=BLOCKS {
            let block = self.reads.get_block_receipts(number.into()).await?;
            receipts.push(serde_json::to_value(block.ok_or_else(|| eyre!("no receipts"))?)?);
        }
        Ok(receipts)
    }

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

    /// The latest account and storage of every account and slot blocks 1 to `BLOCKS` touched,
    /// and of a few untouched genesis accounts, read through the node's provider.
    fn touched_state(&self) -> Result<Vec<String>> {
        let state = self.launched.handle.node.provider().latest()?;
        let mut accounts = vec![STORAGE_CONTRACT, Address::repeat_byte(0xde)];
        accounts.extend((0..4u64).map(|index| Address::from_word(keccak256(index.to_be_bytes()))));
        let mut slots = Vec::new();
        for number in 1..=BLOCKS {
            for index in 0..DEPOSITS_PER_KIND {
                accounts.push(Address::from_word(deposit_seed(number, 1, index)));
                slots.push(deposit_seed(number, 3, index));
            }
        }
        let mut read = Vec::new();
        for account in accounts {
            read.push(format!("{account}: {:?}", state.basic_account(&account)?));
        }
        for slot in slots {
            read.push(format!("{slot}: {:?}", state.storage(STORAGE_CONTRACT, slot)?));
        }
        Ok(read)
    }

    /// Raw dumps of the state, trie and stage tables in this node's MDBX.
    fn state_tables(&self) -> Result<Vec<TableDump>> {
        let tx = self.env.tx()?;
        Ok(vec![
            dump::<tables::HashedAccounts>(&tx)?,
            dump::<tables::HashedStorages>(&tx)?,
            dump::<tables::PackedAccountsTrie>(&tx)?,
            dump::<tables::PackedStoragesTrie>(&tx)?,
            dump::<tables::Bytecodes>(&tx)?,
            dump::<tables::PlainAccountState>(&tx)?,
            dump::<tables::PlainStorageState>(&tx)?,
            dump::<tables::StageCheckpoints>(&tx)?,
        ])
    }
}

fn dump<T: Table>(tx: &Tx<RO>) -> Result<TableDump> {
    let entries = tx
        .cursor_read::<RawTable<T>>()?
        .walk(None)?
        .map(|entry| entry.map(|(key, value)| (key.into_key(), value.into_value())))
        .collect::<Result<_, _>>()?;
    Ok((T::NAME, entries))
}

fn chain_spec() -> Result<Arc<BaseChainSpec>> {
    let mut genesis = build_test_genesis();
    for index in 0..GENESIS_ACCOUNTS {
        let account = GenesisAccount::default().with_balance(U256::from(index + 1));
        genesis.alloc.insert(Address::from_word(keccak256(index.to_be_bytes())), account);
    }
    genesis.alloc.insert(
        STORAGE_CONTRACT,
        GenesisAccount::default().with_code(Some(Bytes::from_static(&STORAGE_CONTRACT_CODE))),
    );
    let base_fee_params = BaseChainSpec::from_genesis(genesis.clone())
        .base_fee_params_at_timestamp(genesis.timestamp);
    genesis.extra_data = JovianExtraData::encode(B64::ZERO, base_fee_params, 0)?;
    Ok(Arc::new(BaseChainSpec::from_genesis(genesis)))
}

fn bound() -> Result<(TcpListener, SocketAddr)> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let address = listener.local_addr()?;
    Ok((listener, address))
}

/// Storage nodes 1 and 2 (each owning every shard, `R = 2`) and a stateless node 3.
fn three_nodes(addresses: [SocketAddr; 3]) -> Vec<ClusterNodeConfig> {
    let node = |id, address, storage| ClusterNodeConfig { id: NodeId(id), address, storage };
    vec![node(1, addresses[0], true), node(2, addresses[1], true), node(3, addresses[2], false)]
}

/// An apply node ends with exactly the executing node's state, receipts and head, without
/// executing; the executing node, whose config lists no `apply_from`, never applies.
#[tokio::test(flavor = "multi_thread")]
async fn applied_chain_matches_the_executed_chain() -> Result<()> {
    base_node_runner::test_utils::init_silenced_tracing();
    let chain_spec = chain_spec()?;
    let (executor_listener, executor_address) = bound()?;
    let (apply_listener, apply_address) = bound()?;
    let nodes = three_nodes([executor_address, apply_address, bound()?.1]);
    let runtime = Runtime::test();
    let executor =
        TestNode::launch(&runtime, &chain_spec, 1, &nodes, executor_listener, vec![], vec![])
            .await?;
    let applier =
        TestNode::launch(&runtime, &chain_spec, 2, &nodes, apply_listener, vec![NodeId(1)], vec![])
            .await?;

    for _ in 0..BLOCKS {
        let envelope = executor.build_next().await?;
        let hash = executor.import(&envelope).await?;
        assert_eq!(applier.import(&envelope).await?, hash);
    }
    executor.wait_persisted(BLOCKS).await?;
    applier.wait_persisted(BLOCKS).await?;

    assert_eq!(applier.cluster.applied_blocks().applied, BLOCKS);
    assert_eq!(applier.cluster.applied_blocks().fallbacks, [0; 4]);
    assert_eq!(executor.cluster.applied_blocks(), ApplyCounts::default());
    assert_eq!(applier.head().await?, executor.head().await?);
    assert_eq!(applier.head().await?.1, BLOCKS);
    assert_eq!(applier.receipts().await?, executor.receipts().await?);
    let executed = executor.state_tables()?;
    for table in ["HashedAccounts", "HashedStorages", "AccountsTrie", "StoragesTrie"] {
        assert!(
            executed.iter().any(|(name, rows)| *name == table && !rows.is_empty()),
            "{table} is empty, so the comparison does not cover it"
        );
    }
    assert_eq!(applier.state_tables()?, executed);
    let changesets = executor.changesets()?;
    assert!(changesets.iter().any(|changeset| changeset != "[]"), "no block has reverts");
    assert_eq!(applier.changesets()?, changesets);
    assert_eq!(applier.touched_state()?, executor.touched_state()?);

    let paths = [executor.data_path.clone(), applier.data_path.clone()];
    drop((executor, applier, runtime));
    for path in paths {
        let _ = std::fs::remove_dir_all(path);
    }
    Ok(())
}

/// A stateless apply node (`storage = false`, owning no shard) ends with the executing node's
/// head, receipts, changesets and state as its provider reads it, while its own sharded tables
/// hold only the replicated account-trie paths and the replicated storage-trie paths of
/// [`STORAGE_CONTRACT`], whose storage is split on every node.
#[tokio::test(flavor = "multi_thread")]
async fn stateless_apply_node_matches_the_executed_chain() -> Result<()> {
    base_node_runner::test_utils::init_silenced_tracing();
    let chain_spec = chain_spec()?;
    let (executor_listener, executor_address) = bound()?;
    let (apply_listener, apply_address) = bound()?;
    let nodes = three_nodes([executor_address, bound()?.1, apply_address]);
    let runtime = Runtime::test();
    let split = || vec![Bytes::copy_from_slice(STORAGE_CONTRACT.as_slice())];
    let executor =
        TestNode::launch(&runtime, &chain_spec, 1, &nodes, executor_listener, vec![], split())
            .await?;
    let applier = TestNode::launch(
        &runtime,
        &chain_spec,
        3,
        &nodes,
        apply_listener,
        vec![NodeId(1)],
        split(),
    )
    .await?;

    for _ in 0..BLOCKS {
        let envelope = executor.build_next().await?;
        let hash = executor.import(&envelope).await?;
        assert_eq!(applier.import(&envelope).await?, hash);
    }
    executor.wait_persisted(BLOCKS).await?;
    applier.wait_persisted(BLOCKS).await?;

    assert_eq!(applier.cluster.applied_blocks().applied, BLOCKS);
    assert_eq!(applier.cluster.applied_blocks().fallbacks, [0; 4]);
    assert_eq!(applier.head().await?, executor.head().await?);
    assert_eq!(applier.receipts().await?, executor.receipts().await?);
    let changesets = executor.changesets()?;
    assert!(changesets.iter().any(|changeset| changeset != "[]"), "no block has reverts");
    assert_eq!(applier.changesets()?, changesets);
    let state = executor.touched_state()?;
    assert!(state.iter().any(|read| read.ends_with(": Some(1)")), "no written slot read");
    assert_eq!(applier.touched_state()?, state);

    let split_contract = keccak256(STORAGE_CONTRACT);
    // A packed trie path is 32 bytes of nibbles and then the nibble count.
    let short_path = |path: &[u8]| path.get(32).is_some_and(|len| *len < 2);
    let replicated_only = |(name, rows): TableDump| -> TableDump {
        if name == "AccountsTrie" {
            (name, rows.into_iter().filter(|(key, _)| short_path(key)).collect())
        } else if name == "StoragesTrie" {
            let replicated = |(key, value): &(Vec<u8>, Vec<u8>)| {
                key.as_slice() == split_contract.as_slice() && short_path(value)
            };
            (name, rows.into_iter().filter(replicated).collect())
        } else if ["HashedAccounts", "HashedStorages", "Bytecodes"].contains(&name) {
            (name, Vec::new())
        } else {
            (name, rows)
        }
    };
    let expected: Vec<_> = executor.state_tables()?.into_iter().map(replicated_only).collect();
    assert!(
        expected.iter().any(|(name, rows)| *name == "AccountsTrie" && !rows.is_empty()),
        "no replicated account-trie path to compare"
    );
    assert!(
        expected.iter().any(|(name, rows)| *name == "StoragesTrie" && !rows.is_empty()),
        "no replicated storage-trie path of the split contract to compare"
    );
    assert_eq!(applier.state_tables()?, expected);

    let paths = [executor.data_path.clone(), applier.data_path.clone()];
    drop((executor, applier, runtime));
    for path in paths {
        let _ = std::fs::remove_dir_all(path);
    }
    Ok(())
}

/// An apply node executes a payload whose result fails the receipt checks or carries execution
/// requests, or when its executor is unreachable, and counts why.
#[tokio::test(flavor = "multi_thread")]
async fn apply_node_executes_when_no_valid_result_arrives() -> Result<()> {
    base_node_runner::test_utils::init_silenced_tracing();
    let chain_spec = chain_spec()?;
    let (executor_listener, executor_address) = bound()?;
    let (apply_listener, apply_address) = bound()?;
    let (corrupt_listener, corrupt_address) = bound()?;
    let nodes = three_nodes([executor_address, apply_address, corrupt_address]);
    let runtime = Runtime::test();
    let executor =
        TestNode::launch(&runtime, &chain_spec, 1, &nodes, executor_listener, vec![], vec![])
            .await?;
    let applier =
        TestNode::launch(&runtime, &chain_spec, 2, &nodes, apply_listener, vec![NodeId(3)], vec![])
            .await?;

    // Node 3 serves results whose first receipt reports one more gas than was used.
    let corrupt_results = Arc::new(ExecutedBlocks::default());
    let corrupt_view = Cluster::new(ClusterConfig {
        node_id: NodeId(3),
        apply_from: vec![],
        ..applier.cluster.cluster().config().clone()
    })?;
    let corrupt_store: Arc<OwnerStore<Tx<RO>>> =
        Arc::new(OwnerStore::new(Arc::new(corrupt_view), 160));
    let corrupt_server =
        OwnerServer::spawn(corrupt_listener, corrupt_store, Arc::clone(&corrupt_results))?;

    let envelope = executor.build_next().await?;
    let hash = executor.import(&envelope).await?;
    let result = executor
        .cluster
        .executed_blocks()
        .wait_for(hash, Duration::from_secs(5))
        .ok_or_else(|| eyre!("executor kept no result for block 1"))?;
    let mut parts = ExecutedBlockResult::decode(&result)?.into_parts::<BaseReceipt>()?;
    let BaseReceipt::Deposit(receipt) = &mut parts.output.result.receipts[0] else {
        return Err(eyre!("first receipt is not the L1 info deposit"));
    };
    receipt.inner.cumulative_gas_used += 1;
    let corrupt = ExecutedBlockResult::new(&parts.output, &parts.senders, &parts.trie).encode()?;
    corrupt_results.insert(hash, corrupt);
    assert_eq!(applier.import(&envelope).await?, hash);
    assert_eq!(applier.cluster.applied_blocks().fallbacks(ApplyFallback::Invalid), 1);

    // Block 2's result differs only in carrying an execution request.
    let envelope = executor.build_next().await?;
    let hash = executor.import(&envelope).await?;
    let result = executor
        .cluster
        .executed_blocks()
        .wait_for(hash, Duration::from_secs(5))
        .ok_or_else(|| eyre!("executor kept no result for block 2"))?;
    let mut parts = ExecutedBlockResult::decode(&result)?.into_parts::<BaseReceipt>()?;
    parts.output.result.requests = Requests::new(vec![Bytes::from_static(&[0x01, 0xaa])]);
    let corrupt = ExecutedBlockResult::new(&parts.output, &parts.senders, &parts.trie).encode()?;
    corrupt_results.insert(hash, corrupt);
    assert_eq!(applier.import(&envelope).await?, hash);
    assert_eq!(applier.cluster.applied_blocks().fallbacks(ApplyFallback::Invalid), 2);

    drop(corrupt_server);
    for _ in 2..BLOCKS {
        let envelope = executor.build_next().await?;
        let hash = executor.import(&envelope).await?;
        assert_eq!(applier.import(&envelope).await?, hash);
    }
    let counts = applier.cluster.applied_blocks();
    assert_eq!(counts.applied, 0);
    assert_eq!(counts.fallbacks(ApplyFallback::Unreachable), BLOCKS - 2);
    assert_eq!(applier.head().await?, executor.head().await?);
    assert_eq!(applier.receipts().await?, executor.receipts().await?);

    let paths = [executor.data_path.clone(), applier.data_path.clone()];
    drop((executor, applier, runtime));
    for path in paths {
        let _ = std::fs::remove_dir_all(path);
    }
    Ok(())
}
