//! Apply mode end to end: clustered nodes driven through the in-memory Engine API, one executing
//! payloads and others inserting them from its execution results, or executing them when no valid
//! result arrives.

use std::{
    collections::{BTreeMap, BTreeSet},
    net::{SocketAddr, TcpListener},
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};

use alloy_eips::{
    BlockNumberOrTag, Encodable2718,
    eip7685::{EMPTY_REQUESTS_HASH, Requests},
};
use alloy_genesis::GenesisAccount;
use alloy_primitives::{Address, B64, B256, Bytes, TxKind, U256, keccak256};
use alloy_provider::{Provider, RootProvider};
use alloy_rpc_types_engine::{ForkchoiceState, JwtSecret, PayloadAttributes, PayloadStatusEnum};
use base_common_consensus::{BaseReceipt, JovianExtraData, Predeploys, TxDeposit};
use base_common_genesis::RollupConfig;
use base_common_network::{Base, BaseEngineApi};
use base_common_rpc_types_engine::{
    BaseExecutionPayloadEnvelopeV4, BaseExecutionPayloadV4, BasePayloadAttributes,
};
use base_consensus_engine::{
    BaseEngineClient, EngineClient, EngineClientBuilder, EngineEndpoint, InMemoryTransport,
};
use base_consensus_providers::L1_RPC_TIMEOUT;
use base_execution_chainspec::BaseChainSpec;
use base_execution_cluster::{
    ApplyCounts, ApplyFallback, Cluster, ClusterConfig, ClusterNode, ClusteredDatabase,
    ExecutedBlockResult, ExecutedBlocks, NodeConfig as ClusterNodeConfig, NodeId, OwnerServer,
    OwnerStore, Shard, ShardMap,
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
    cursor::{DbCursorRO, DbDupCursorRO},
    init_db,
    mdbx::{DatabaseArguments, RO, tx::Tx},
    table::Table,
    tables::{self, RawTable},
    test_utils::{create_test_rw_db, tempdir_path},
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
use reth_trie_common::{BranchNodeCompact, Nibbles};

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
/// accounts (each deposit sends what it mints) and storage slots, so blocks change the account
/// and storage tries.
fn block_transactions(number: u64) -> Vec<Bytes> {
    let seed = |kind, index| deposit_seed(number, kind, index);
    let deposit = |source_hash, to, mint, input| {
        TxDeposit {
            source_hash,
            from: Address::repeat_byte(0xde),
            to: TxKind::Call(to),
            mint,
            value: U256::from(mint),
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
    cluster: Option<Arc<ClusterNode>>,
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
        let (node_config, data_path, env) = Self::datadir(chain_spec)?;
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
        Self::start(runtime, node_config, data_path, env, db).await
    }

    /// Launches a node without a cluster config: its database is a pass-through to MDBX.
    async fn launch_unclustered(
        runtime: &Runtime,
        chain_spec: &Arc<BaseChainSpec>,
    ) -> Result<Self> {
        let (node_config, data_path, env) = Self::datadir(chain_spec)?;
        let db = ClusteredDatabase::new(env.clone());
        Self::start(runtime, node_config, data_path, env, db).await
    }

    /// A node config over a fresh datadir, and that datadir's MDBX.
    fn datadir(
        chain_spec: &Arc<BaseChainSpec>,
    ) -> Result<(NodeConfig<BaseChainSpec>, PathBuf, DatabaseEnv)> {
        let network = NetworkArgs {
            discovery: DiscoveryArgs { disable_discovery: true, ..DiscoveryArgs::default() },
            ..NetworkArgs::default()
        };
        let mut node_config = NodeConfig::new(Arc::clone(chain_spec))
            .with_network(network)
            .with_rpc(RpcServerArgs::default().with_unused_ports())
            .with_unused_ports();
        node_config.engine.persistence_threshold = 0;
        // As a clustered node requires: the state tables are at the `Finish` checkpoint.
        node_config.engine.num_state_masking_blocks = 0;
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
        Ok((node_config, data_path, env))
    }

    /// Launches the node of `node_config` on `db`, whose MDBX is `env`.
    async fn start(
        runtime: &Runtime,
        node_config: NodeConfig<BaseChainSpec>,
        data_path: PathBuf,
        env: DatabaseEnv,
        db: ClusteredDatabase,
    ) -> Result<Self> {
        let cluster = db.cluster().cloned();
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

    /// This node's side of its cluster.
    const fn cluster(&self) -> &Arc<ClusterNode> {
        self.cluster.as_ref().expect("a clustered node")
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
    chain_spec_with(GENESIS_ACCOUNTS, 0)
}

/// A genesis with `accounts` funded accounts and [`STORAGE_CONTRACT`] holding `contract_slots`
/// slots.
fn chain_spec_with(accounts: u64, contract_slots: u64) -> Result<Arc<BaseChainSpec>> {
    let mut genesis = build_test_genesis();
    for index in 0..accounts {
        let account = GenesisAccount::default().with_balance(U256::from(index + 1));
        genesis.alloc.insert(Address::from_word(keccak256(index.to_be_bytes())), account);
    }
    let storage = (0..contract_slots)
        .map(|index| {
            (
                keccak256([b"slot".as_slice(), &index.to_be_bytes()].concat()),
                B256::with_last_byte(1),
            )
        })
        .collect();
    genesis.alloc.insert(
        STORAGE_CONTRACT,
        GenesisAccount::default()
            .with_code(Some(Bytes::from_static(&STORAGE_CONTRACT_CODE)))
            .with_storage(Some(storage)),
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

    assert_eq!(applier.cluster().applied_blocks().applied, BLOCKS);
    assert_eq!(applier.cluster().applied_blocks().fallbacks, [0; 4]);
    assert_eq!(executor.cluster().applied_blocks(), ApplyCounts::default());
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

    assert_eq!(applier.cluster().applied_blocks().applied, BLOCKS);
    assert_eq!(applier.cluster().applied_blocks().fallbacks, [0; 4]);
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
        ..applier.cluster().cluster().config().clone()
    })?;
    let corrupt_store =
        Arc::new(OwnerStore::new(Arc::new(corrupt_view), create_test_rw_db(), 160)?);
    let corrupt_server =
        OwnerServer::spawn(corrupt_listener, corrupt_store, Arc::clone(&corrupt_results))?;

    let envelope = executor.build_next().await?;
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
    corrupt_results.insert(hash, corrupt);
    assert_eq!(applier.import(&envelope).await?, hash);
    assert_eq!(applier.cluster().applied_blocks().fallbacks(ApplyFallback::Invalid), 1);

    // Block 2's result differs only in carrying an execution request.
    let envelope = executor.build_next().await?;
    let hash = executor.import(&envelope).await?;
    let result = executor
        .cluster()
        .executed_blocks()
        .wait_for(hash, Duration::from_secs(5))
        .ok_or_else(|| eyre!("executor kept no result for block 2"))?;
    let mut parts = ExecutedBlockResult::decode(&result)?.into_parts::<BaseReceipt>()?;
    parts.output.result.requests = Requests::new(vec![Bytes::from_static(&[0x01, 0xaa])]);
    let corrupt = ExecutedBlockResult::new(&parts.output, &parts.senders, &parts.trie).encode()?;
    corrupt_results.insert(hash, corrupt);
    assert_eq!(applier.import(&envelope).await?, hash);
    assert_eq!(applier.cluster().applied_blocks().fallbacks(ApplyFallback::Invalid), 2);

    drop(corrupt_server);
    for _ in 2..BLOCKS {
        let envelope = executor.build_next().await?;
        let hash = executor.import(&envelope).await?;
        assert_eq!(applier.import(&envelope).await?, hash);
    }
    let counts = applier.cluster().applied_blocks();
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

/// Blocks of the trie-slice test, and its genesis: dense enough that the two-nibble account-trie
/// nodes and [`STORAGE_CONTRACT`]'s two-nibble storage-trie nodes are stored branches.
const SLICE_BLOCKS: u64 = 4;
const SLICE_GENESIS_ACCOUNTS: u64 = 4_096;
const SLICE_CONTRACT_SLOTS: u64 = 4_096;

/// Ids of three storage nodes (`R = 2`) of which the first two are launched: exactly one of them
/// owns the message passer's account shard, and exactly one owns [`STORAGE_CONTRACT`]'s.
fn slice_node_ids() -> [u64; 3] {
    let message_passer = keccak256(Predeploys::L2_TO_L1_MESSAGE_PASSER)[0];
    let contract = keccak256(STORAGE_CONTRACT)[0];
    (3..)
        .map(|third| [1, 2, third])
        .find(|ids| {
            let shards = ShardMap::new(&ids.map(NodeId), 2);
            let one_launched_owner = |shard| {
                shards.owners(Shard(shard)).iter().filter(|id| ids[..2].contains(&id.0)).count()
                    == 1
            };
            one_launched_owner(message_passer) && one_launched_owner(contract)
        })
        .expect("some third node id splits both shards")
}

/// A child of a replicated one-nibble node: the storage trie's contract (`None` for the account
/// trie), the node's path, and the child nibble.
type ChildPosition = (Option<B256>, Nibbles, u8);

/// A child's state and tree mask bits and, when its hash bit is set, its hash.
#[derive(Debug, PartialEq, Eq)]
struct ChildFlags {
    present: bool,
    stored: bool,
    hash: Option<B256>,
}

/// The trie rows of a node's slice that must equal a full node's, read from raw MDBX.
#[derive(Debug, PartialEq, Eq)]
struct SliceRows {
    /// Account-trie rows of two or more nibbles in held shards, except the path to
    /// [`STORAGE_CONTRACT`]'s account leaf.
    accounts_trie: BTreeMap<Vec<u8>, Vec<u8>>,
    /// Storage-trie rows of held unsplit accounts, and of held ranges of the split contract.
    storages_trie: BTreeMap<Vec<u8>, BTreeSet<Vec<u8>>>,
    /// State, tree and hash bits and hash of each held child of a replicated one-nibble node of
    /// the account trie (except the split contract's account shard) and of the split contract's
    /// storage trie.
    held_children: BTreeMap<ChildPosition, ChildFlags>,
}

impl SliceRows {
    /// The rows `cluster`'s node holds, as `env` stores them.
    fn read(env: &DatabaseEnv, cluster: &Cluster) -> Result<Self> {
        let split = keccak256(STORAGE_CONTRACT);
        let split_leaf = Nibbles::unpack(split);
        let owns = |path: &Nibbles| {
            cluster.owns(Shard(path.get_unchecked(0) << 4 | path.get_unchecked(1)))
        };
        // A packed trie path is 32 bytes of nibbles and then the nibble count.
        let path_of =
            |encoded: &[u8]| Nibbles::unpack(&encoded[..32]).slice(..usize::from(encoded[32]));
        let tx = env.tx()?;
        let (_, accounts) = dump::<tables::PackedAccountsTrie>(&tx)?;
        let accounts_trie = accounts
            .into_iter()
            .filter(|(key, _)| {
                let path = path_of(key);
                path.len() >= 2 && owns(&path) && !split_leaf.starts_with(&path)
            })
            .collect();
        let mut storages_trie: BTreeMap<_, BTreeSet<_>> = BTreeMap::new();
        for (address, value) in dump::<tables::PackedStoragesTrie>(&tx)?.1 {
            let held = if address.as_slice() == split.as_slice() {
                let path = path_of(&value[..33]);
                path.len() >= 2 && owns(&path)
            } else {
                cluster.owns(Shard(address[0]))
            };
            if held {
                storages_trie.entry(address).or_default().insert(value);
            }
        }

        let mut held_children = BTreeMap::new();
        let mut record = |owner: Option<B256>, path: Nibbles, node: &BranchNodeCompact| {
            for child in 0..16u8 {
                let mut child_path = path;
                child_path.push(child);
                let on_split_path = owner.is_none() && split_leaf.starts_with(&child_path);
                if owns(&child_path) && !on_split_path {
                    let hashed = node.hash_mask.is_bit_set(child);
                    held_children.insert(
                        (owner, path, child),
                        ChildFlags {
                            present: node.state_mask.is_bit_set(child),
                            stored: node.tree_mask.is_bit_set(child),
                            hash: hashed.then(|| node.hash_for_nibble(child)),
                        },
                    );
                }
            }
        };
        for entry in tx.cursor_read::<tables::PackedAccountsTrie>()?.walk(None)? {
            let (path, node) = entry?;
            if path.0.len() == 1 {
                record(None, path.0, &node);
            }
        }
        for entry in
            tx.cursor_dup_read::<tables::PackedStoragesTrie>()?.walk_dup(Some(split), None)?
        {
            let entry = entry?.1;
            if entry.nibbles.0.len() == 1 {
                record(Some(split), entry.nibbles.0, &entry.node);
            }
        }
        Ok(Self { accounts_trie, storages_trie, held_children })
    }
}

impl TestNode {
    /// Submits `payload` built on this node's head without making it the head.
    async fn submit(
        &self,
        payload: BaseExecutionPayloadV4,
        parent_beacon_block_root: B256,
    ) -> Result<PayloadStatusEnum> {
        Ok(self.engine.new_payload_v4(payload, parent_beacon_block_root).await?.status)
    }
}

/// `envelope`'s payload with the given header roots replaced, resealed under its new hash.
fn resealed(
    envelope: &BaseExecutionPayloadEnvelopeV4,
    state_root: Option<B256>,
    withdrawals_root: Option<B256>,
) -> Result<BaseExecutionPayloadV4> {
    let mut payload = envelope.execution_payload.clone();
    let mut block = payload.clone().into_block_raw()?;
    // Header fields an Engine API V4 payload carries beside the payload.
    block.header.parent_beacon_block_root = Some(envelope.parent_beacon_block_root);
    block.header.requests_hash = Some(EMPTY_REQUESTS_HASH);
    let built = &payload.payload_inner.payload_inner.payload_inner;
    if block.header.hash_slow() != built.block_hash {
        return Err(eyre!("the payload's header does not reseal to its hash"));
    }
    if let Some(root) = state_root {
        block.header.state_root = root;
        payload.payload_inner.payload_inner.payload_inner.state_root = root;
    }
    if let Some(root) = withdrawals_root {
        block.header.withdrawals_root = Some(root);
        payload.withdrawals_root = root;
    }
    payload.payload_inner.payload_inner.payload_inner.block_hash = block.header.hash_slow();
    Ok(payload)
}

/// Executing clustered nodes keep only their trie slice: after the blocks persist, every
/// meaningful row each node holds equals an unclustered full node's. A clustered node accepts a
/// block whose header state root is wrong, which the unclustered node rejects; the
/// message-passer storage root is compared only by the node holding that storage trie and
/// skipped and counted by the other.
#[tokio::test(flavor = "multi_thread")]
async fn clustered_nodes_keep_trie_slices_equal_to_a_full_node() -> Result<()> {
    base_node_runner::test_utils::init_silenced_tracing();
    let chain_spec = chain_spec_with(SLICE_GENESIS_ACCOUNTS, SLICE_CONTRACT_SLOTS)?;
    let ids = slice_node_ids();
    let (first_listener, first_address) = bound()?;
    let (second_listener, second_address) = bound()?;
    let addresses = [first_address, second_address, bound()?.1];
    let nodes: Vec<_> = ids
        .into_iter()
        .zip(addresses)
        .map(|(id, address)| ClusterNodeConfig { id: NodeId(id), address, storage: true })
        .collect();
    let split = || vec![Bytes::copy_from_slice(STORAGE_CONTRACT.as_slice())];
    let runtime = Runtime::test();
    let reference = TestNode::launch_unclustered(&runtime, &chain_spec).await?;
    let first =
        TestNode::launch(&runtime, &chain_spec, ids[0], &nodes, first_listener, vec![], split())
            .await?;
    let second =
        TestNode::launch(&runtime, &chain_spec, ids[1], &nodes, second_listener, vec![], split())
            .await?;
    let clustered = [&first, &second];
    let at_genesis = clustered
        .iter()
        .map(|node| SliceRows::read(&node.env, node.cluster().cluster()))
        .collect::<Result<Vec<_>>>()?;

    for _ in 0..SLICE_BLOCKS {
        let envelope = reference.build_next().await?;
        let hash = reference.import(&envelope).await?;
        for node in clustered {
            assert_eq!(node.import(&envelope).await?, hash);
        }
    }
    for node in [&reference, &first, &second] {
        node.wait_persisted(SLICE_BLOCKS).await?;
    }

    let message_passer = keccak256(Predeploys::L2_TO_L1_MESSAGE_PASSER);
    let contract = keccak256(STORAGE_CONTRACT);
    for (node, at_genesis) in clustered.into_iter().zip(at_genesis) {
        let cluster = node.cluster().cluster();
        let expected = SliceRows::read(&reference.env, cluster)?;
        assert_eq!(SliceRows::read(&node.env, cluster)?, expected, "node {:?}", cluster.node_id());
        assert!(expected.accounts_trie.len() > 50, "few account-trie rows compared");
        assert!(expected.held_children.len() > 50, "few replicated children compared");
        assert_ne!(expected.accounts_trie, at_genesis.accounts_trie, "no held row changed");
        let counts = node.cluster().trie_slices();
        assert_eq!(counts.blocks, SLICE_BLOCKS);
        if cluster.owns(Shard(contract[0])) {
            assert_eq!(counts.side_calls, 0, "the contract's account holder needs no side call");
        } else {
            assert_eq!(counts.side_calls, SLICE_BLOCKS);
            let key = contract.to_vec();
            assert!(
                expected.storages_trie.get(&key).is_some_and(|rows| !rows.is_empty())
                    && expected.storages_trie.get(&key) != at_genesis.storages_trie.get(&key),
                "held ranges of the split contract did not change"
            );
        }
        let skips = if cluster.owns(Shard(message_passer[0])) { 0 } else { SLICE_BLOCKS };
        assert_eq!(counts.message_passer_skips, skips);
    }

    let envelope = reference.build_next().await?;
    let root = envelope.parent_beacon_block_root;
    let wrong_state_root = resealed(&envelope, Some(B256::repeat_byte(0x5a)), None)?;
    let wrong_withdrawals_root = resealed(&envelope, None, Some(B256::repeat_byte(0x5b)))?;
    assert!(reference.submit(wrong_state_root.clone(), root).await?.is_invalid());
    assert!(reference.submit(wrong_withdrawals_root.clone(), root).await?.is_invalid());
    for node in clustered {
        let status = node.submit(wrong_state_root.clone(), root).await?;
        assert!(status.is_valid(), "{status:?}");
        let holds_message_passer = node.cluster().cluster().owns(Shard(message_passer[0]));
        let status = node.submit(wrong_withdrawals_root.clone(), root).await?;
        assert_eq!(status.is_valid(), !holds_message_passer, "{status:?}");
    }

    let paths = [reference.data_path.clone(), first.data_path.clone(), second.data_path.clone()];
    drop((reference, first, second, runtime));
    for path in paths {
        let _ = std::fs::remove_dir_all(path);
    }
    Ok(())
}
