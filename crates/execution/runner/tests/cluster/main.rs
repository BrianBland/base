//! Clustered nodes end to end: nodes launched in process, driven through the in-memory Engine
//! API by an unclustered node that builds every block, stopped and relaunched on their datadirs.

mod apply;
mod e2e;
mod guards;
mod join;
mod slices;

use std::{
    net::{SocketAddr, TcpListener},
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};

use alloy_eips::{BlockNumberOrTag, Encodable2718};
use alloy_genesis::GenesisAccount;
use alloy_primitives::{Address, B64, B256, Bytes, TxKind, U256, keccak256};
use alloy_provider::{Provider, RootProvider};
use alloy_rpc_types_engine::{ForkchoiceState, JwtSecret, PayloadAttributes, PayloadStatusEnum};
use base_common_consensus::{JovianExtraData, TxDeposit};
use base_common_genesis::RollupConfig;
use base_common_network::{Base, BaseEngineApi};
use base_common_rpc_types_engine::{BaseExecutionPayloadEnvelopeV4, BasePayloadAttributes};
use base_consensus_engine::{
    BaseEngineClient, EngineClient, EngineClientBuilder, EngineEndpoint, InMemoryTransport,
};
use base_consensus_providers::L1_RPC_TIMEOUT;
use base_execution_chainspec::BaseChainSpec;
use base_execution_cluster::{
    Cluster, ClusterConfig, ClusterNode, ClusteredDatabase, NodeConfig as ClusterNodeConfig,
    NodeId, RootCheckConfig, RootCheckCounts, RootCheckMode,
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
use reth_provider::{AccountReader, StateProvider, StateProviderFactory};
use reth_tasks::Runtime;

/// Sender of the test's user deposits: deposits mint what they send, so it needs no funds.
const DEPOSITOR: Address = Address::repeat_byte(0xde);

/// A genesis contract that stores its second calldata word at the slot given by its first:
/// `PUSH1 0x20 CALLDATALOAD PUSH0 CALLDATALOAD SSTORE STOP`. Storing zero deletes the slot.
const STORAGE_CONTRACT: Address = Address::repeat_byte(0xc0);
const STORAGE_CONTRACT_CODE: [u8; 7] = [0x60, 0x20, 0x35, 0x5f, 0x35, 0x55, 0x00];

/// Longest wait for a launched node's datadir and owner address to be released after it stops.
const RELEASE_TIMEOUT: Duration = Duration::from_secs(30);

/// A user deposit from [`DEPOSITOR`] identified by `source_hash`, sending `value` (which it
/// mints) to `to` with `input`.
fn deposit(source_hash: B256, to: TxKind, value: u128, input: Bytes) -> Bytes {
    deposit_from(DEPOSITOR, source_hash, to, value, input)
}

/// A user deposit from `from`; see [`deposit`].
fn deposit_from(from: Address, source_hash: B256, to: TxKind, value: u128, input: Bytes) -> Bytes {
    TxDeposit {
        source_hash,
        from,
        to,
        mint: value,
        value: U256::from(value),
        gas_limit: 200_000,
        is_system_transaction: false,
        input,
    }
    .encoded_2718()
    .into()
}

/// Calldata of [`STORAGE_CONTRACT_CODE`]: store `value` at `slot`.
fn store_call(slot: B256, value: U256) -> Bytes {
    [slot.as_slice(), &value.to_be_bytes::<32>()].concat().into()
}

/// A table's name and its raw `(key, value)` rows, in key order.
type TableDump = (&'static str, Vec<(Vec<u8>, Vec<u8>)>);

fn dump<T: Table>(tx: &Tx<RO>) -> Result<TableDump> {
    let entries = tx
        .cursor_read::<RawTable<T>>()?
        .walk(None)?
        .map(|entry| entry.map(|(key, value)| (key.into_key(), value.into_value())))
        .collect::<Result<_, _>>()?;
    Ok((T::NAME, entries))
}

/// A genesis with `accounts` funded accounts, [`STORAGE_CONTRACT`] holding `contract_slots`
/// slots, and `extra` accounts.
fn chain_spec_with(
    accounts: u64,
    contract_slots: u64,
    extra: impl IntoIterator<Item = (Address, GenesisAccount)>,
) -> Result<Arc<BaseChainSpec>> {
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
    genesis.alloc.extend(extra);
    let base_fee_params = BaseChainSpec::from_genesis(genesis.clone())
        .base_fee_params_at_timestamp(genesis.timestamp);
    genesis.extra_data = JovianExtraData::encode(B64::ZERO, base_fee_params, 0)?;
    Ok(Arc::new(BaseChainSpec::from_genesis(genesis)))
}

/// A free localhost address, for a node's owner server.
fn free_address() -> Result<SocketAddr> {
    Ok(TcpListener::bind("127.0.0.1:0")?.local_addr()?)
}

/// Longest wait for a node's background state-root check to reach a block: a block an owner
/// cannot answer yet is retried for [`RootCheckSchedule::RETRY_FOR`] before it is skipped.
///
/// [`RootCheckSchedule::RETRY_FOR`]: base_execution_cluster::RootCheckSchedule::RETRY_FOR
const ROOT_CHECK_TIMEOUT: Duration = Duration::from_secs(120);

/// A cluster config of `nodes` seen from `node_id`, `R = 2`, with the state-root check in log
/// mode so that a mismatch fails an assertion instead of exiting the test process.
fn cluster_config(node_id: u64, nodes: &[ClusterNodeConfig]) -> ClusterConfig {
    ClusterConfig {
        node_id: NodeId(node_id),
        replication: 2,
        retention_blocks: ClusterConfig::MIN_RETENTION_BLOCKS,
        nodes: nodes.to_vec(),
        apply_from: Vec::new(),
        split_storage: Vec::new(),
        read_retry_secs: ClusterConfig::DEFAULT_READ_RETRY_SECS,
        simulated_round_trip_micros: 0,
        root_check: RootCheckConfig { mode: RootCheckMode::Log, ..RootCheckConfig::default() },
    }
}

/// A launched node and the handles a test drives and inspects it through. Each node runs on its
/// own task runtime, so stopping it releases its datadir and owner address.
struct TestNode {
    launched: LaunchedBaseNode,
    env: DatabaseEnv,
    cluster: Option<Arc<ClusterNode>>,
    engine: BaseEngineClient<RootProvider, RootProvider<Base>>,
    reads: RootProvider<Base>,
    runtime: Runtime,
    datadir: Datadir,
}

/// A node's datadir and node config, kept across restarts.
struct Datadir {
    node_config: NodeConfig<BaseChainSpec>,
    path: PathBuf,
}

impl Datadir {
    /// A node config over a fresh datadir.
    fn fresh(chain_spec: &Arc<BaseChainSpec>) -> Self {
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
        let path = tempdir_path();
        node_config = node_config.with_datadir_args(DatadirArgs {
            datadir: MaybePlatformPath::<DataDirPath>::from(path.clone()),
            ..Default::default()
        });
        Self { node_config, path }
    }

    /// Opens this datadir's MDBX, waiting for a stopped node to release it.
    async fn open(&self) -> Result<DatabaseEnv> {
        let deadline = Instant::now() + RELEASE_TIMEOUT;
        loop {
            let opened = init_db(
                self.node_config.datadir().db(),
                DatabaseArguments::new(ClientVersion::default())
                    .with_geometry_max_size(Some(100 * 1024 * 1024)),
            );
            match opened {
                Ok(env) => return Ok(env),
                Err(error) if Instant::now() >= deadline => return Err(error),
                Err(_) => tokio::time::sleep(Duration::from_millis(50)).await,
            }
        }
    }

    /// Launches a node on this datadir: clustered under `cluster`, otherwise a pass-through to
    /// MDBX. A fresh datadir gets its genesis from the node. The launch future is boxed: it is
    /// large, and tests await many of them.
    async fn launch(self, cluster: Option<ClusterConfig>) -> Result<TestNode> {
        Box::pin(self.launch_unboxed(cluster)).await
    }

    async fn launch_unboxed(self, cluster: Option<ClusterConfig>) -> Result<TestNode> {
        let env = self.open().await?;
        let db = match cluster {
            None => ClusteredDatabase::new(env.clone()),
            Some(config) => {
                let cluster = Arc::new(Cluster::new(config)?);
                let address =
                    cluster.address(cluster.node_id()).ok_or_else(|| eyre!("unlisted"))?;
                let listener = Self::bind(address).await?;
                let options = cluster.config().client_options();
                ClusteredDatabase::clustered(env.clone(), cluster, listener, options)?
            }
        };
        let cluster = db.cluster().cloned();
        // A runtime of its own, built off the test's runtime so that it owns its threads.
        let runtime = std::thread::spawn(Runtime::test)
            .join()
            .map_err(|_| eyre!("building a node runtime panicked"))?;
        let builder = NodeBuilder::new(self.node_config.clone())
            .with_database(db)
            .with_launch_context(runtime.clone());
        // Launched inside the node's runtime, so tasks it spawns directly stop with it.
        let handle = runtime.handle().clone();
        let launched = tokio::task::spawn_blocking(move || {
            handle.block_on(BaseNodeRunner::new(RollupArgs::default()).launch(builder))
        })
        .await?;
        let launched = match launched {
            Ok(launched) => launched,
            Err(error) => {
                // A runtime cannot be dropped on the test's runtime.
                tokio::task::spawn_blocking(move || {
                    runtime.shutdown_timeout(Duration::from_secs(10))
                })
                .await?;
                return Err(error);
            }
        };
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
        Ok(TestNode { launched, env, cluster, engine, reads, runtime, datadir: self })
    }

    /// Binds `address`, waiting for a stopped node to release it.
    async fn bind(address: SocketAddr) -> Result<TcpListener> {
        let deadline = Instant::now() + RELEASE_TIMEOUT;
        loop {
            match TcpListener::bind(address) {
                Ok(listener) => return Ok(listener),
                Err(error) if Instant::now() >= deadline => return Err(error.into()),
                Err(_) => tokio::time::sleep(Duration::from_millis(50)).await,
            }
        }
    }
}

impl TestNode {
    /// Launches a node on a fresh datadir whose genesis it writes itself (through the clustered
    /// database when `cluster` is set).
    async fn launch(
        chain_spec: &Arc<BaseChainSpec>,
        cluster: Option<ClusterConfig>,
    ) -> Result<Self> {
        Datadir::fresh(chain_spec).launch(cluster).await
    }

    /// Launches a clustered node as an operator starts one: from a full datadir (genesis written
    /// by an unclustered node) whose rows of shards it does not hold are never shed.
    async fn launch_from_full_datadir(
        chain_spec: &Arc<BaseChainSpec>,
        cluster: ClusterConfig,
    ) -> Result<Self> {
        let full = Self::launch(chain_spec, None).await?;
        full.stop().await?.launch(Some(cluster)).await
    }

    /// Stops the node as an operator does (reth's graceful shutdown, which persists the blocks
    /// it still holds in memory) and returns its datadir once its database and owner address are
    /// released.
    async fn stop(self) -> Result<Datadir> {
        let Self { launched, env, cluster, engine, reads, runtime, datadir } = self;
        let address = cluster.as_ref().map(|node| node.server().local_addr());
        drop((engine, reads, env, cluster, launched));
        tokio::task::spawn_blocking(move || {
            if !runtime.graceful_shutdown_with_timeout(Duration::from_secs(10)) {
                return Err(eyre!("the node did not shut down gracefully"));
            }
            runtime.shutdown_timeout(Duration::from_secs(10));
            Ok(())
        })
        .await??;
        drop(datadir.open().await?);
        if let Some(address) = address {
            drop(Datadir::bind(address).await?);
        }
        Ok(datadir)
    }

    /// Stops the node and deletes its datadir.
    async fn remove(self) -> Result<()> {
        let datadir = self.stop().await?;
        let _ = std::fs::remove_dir_all(datadir.path);
        Ok(())
    }

    /// This node's side of its cluster.
    const fn cluster(&self) -> &Arc<ClusterNode> {
        self.cluster.as_ref().expect("a clustered node")
    }

    /// The forkchoice state of this node's head and the attributes of a block on it carrying
    /// `transactions`.
    async fn next_block(
        &self,
        transactions: Vec<Bytes>,
    ) -> Result<(ForkchoiceState, BasePayloadAttributes)> {
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
            transactions: Some(transactions),
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
        Ok((state, attributes))
    }

    /// Builds the next block with `transactions` after the L1 info deposit on this node's head,
    /// inserts it and makes it the head, and returns its payload.
    async fn produce(&self, transactions: Vec<Bytes>) -> Result<BaseExecutionPayloadEnvelopeV4> {
        let transactions: Vec<_> =
            std::iter::once(L1_BLOCK_INFO_DEPOSIT_TX).chain(transactions).collect();
        let (state, attributes) = self.next_block(transactions.clone()).await?;
        let started = self.engine.fork_choice_updated_v3(state, Some(attributes)).await?;
        let payload_id =
            started.payload_id.ok_or_else(|| eyre!("forkchoice did not start a payload"))?;
        tokio::time::sleep(Duration::from_millis(200)).await;
        let envelope = self.engine.get_payload_v4(payload_id).await?;
        let built = &envelope.execution_payload.payload_inner.payload_inner.payload_inner;
        if built.transactions != transactions {
            return Err(eyre!("payload does not carry the block's transactions"));
        }
        self.import(&envelope).await?;
        Ok(envelope)
    }

    /// Submits `envelope` as a new payload and returns its status.
    async fn submit(&self, envelope: &BaseExecutionPayloadEnvelopeV4) -> Result<PayloadStatusEnum> {
        Ok(self
            .engine
            .new_payload_v4(envelope.execution_payload.clone(), envelope.parent_beacon_block_root)
            .await?
            .status)
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

    /// The persisted block: the `Finish` checkpoint.
    fn persisted(&self) -> Result<u64> {
        Ok(ClusterNode::finish_checkpoint(&self.env.tx()?)?)
    }

    /// Waits until block `number` is persisted.
    async fn wait_persisted(&self, number: u64) -> Result<()> {
        let deadline = Instant::now() + Duration::from_secs(10);
        while self.persisted()? < number {
            if Instant::now() >= deadline {
                return Err(eyre!("block {number} was not persisted"));
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        Ok(())
    }

    /// Waits until this node's background state-root check satisfies `done` (given its counts
    /// and last verified block), and returns the counts.
    async fn wait_root_checks(
        &self,
        what: &str,
        done: impl Fn(&RootCheckCounts, Option<u64>) -> bool,
    ) -> Result<RootCheckCounts> {
        let stats = self.cluster().root_check_stats();
        let deadline = Instant::now() + ROOT_CHECK_TIMEOUT;
        loop {
            let (counts, last) = (stats.counts(), stats.last_verified());
            if done(&counts, last) {
                return Ok(counts);
            }
            if Instant::now() >= deadline {
                return Err(eyre!("root check: {what}: {counts:?}, last verified {last:?}"));
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// Waits until this node's background state-root check has verified block `number`.
    async fn wait_root_verified(&self, number: u64) -> Result<RootCheckCounts> {
        self.wait_root_checks(&format!("block {number} verified"), |_, last| {
            last.is_some_and(|last| last >= number)
        })
        .await
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

    /// The receipts of blocks `1..=blocks`, as JSON.
    async fn receipts(&self, blocks: u64) -> Result<Vec<serde_json::Value>> {
        let mut receipts = Vec::new();
        for number in 1..=blocks {
            let block = self.reads.get_block_receipts(number.into()).await?;
            receipts.push(serde_json::to_value(block.ok_or_else(|| eyre!("no receipts"))?)?);
        }
        Ok(receipts)
    }

    /// The latest account, code and storage of `accounts` and `slots`, read through the node's
    /// provider (remotely for rows a clustered node does not hold), one line each.
    fn state_of(&self, accounts: &[Address], slots: &[(Address, B256)]) -> Result<Vec<String>> {
        let state = self.launched.handle.node.provider().latest()?;
        let mut read = Vec::new();
        for account in accounts {
            let code = state.account_code(account)?.map(|code| code.original_bytes());
            read.push(format!("{account}: {:?} code {code:?}", state.basic_account(account)?));
        }
        for (account, slot) in slots {
            read.push(format!("{account} {slot}: {:?}", state.storage(*account, *slot)?));
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
