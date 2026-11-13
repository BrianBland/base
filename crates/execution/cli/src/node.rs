//! Chainless execution-node arguments and launch helpers.

use std::{
    net::{Ipv4Addr, TcpListener},
    path::{Path, PathBuf},
    sync::Arc,
};

use base_execution_chainspec::BaseChainSpec;
use base_execution_cluster::{Cluster, ClusteredDatabase};
use base_node_runner::{BaseNodeBuilder, LaunchedBaseNode};
use base_upgrade_signal::UpgradeSignalStartupMode;
use clap::{Args, value_parser};
use reth_cli_commands::node::NodeCommand;
use reth_cli_runner::CliContext;
use reth_db::{DatabaseEnv, init_db};
use reth_db_api::{Database, tables, transaction::DbTx};
use reth_node_builder::NodeBuilder;
use reth_node_core::{
    args::{
        DatabaseArgs, DatadirArgs, DebugArgs, DevArgs, EngineArgs, EraArgs, MetricArgs,
        NetworkArgs, PruningArgs, RpcServerArgs, StaticFilesArgs, StorageArgs, TxPoolArgs,
    },
    node_config::NodeConfig,
    version,
};
use reth_rpc_server_types::{LenientRpcModuleValidator, RpcModuleValidator};
use reth_stages_types::StageId;
use tracing::info;

use crate::{
    MeteringArgs, RpcStandardNodeArgs, ShadowIndexerArgs, StandardNodeArgs,
    chainspec::BaseChainSpecParser,
};

const DEFAULT_BASE_MAX_INBOUND_EL_PEERS: usize = 80;
const DEFAULT_BASE_MAX_OUTBOUND_EL_PEERS: usize = 80;

/// Clustered Base arguments: whether this node keeps only its shards of the state.
#[derive(Debug, Clone, Default, PartialEq, Eq, Args)]
pub struct ClusterArgs {
    /// Cluster configuration file (TOML). When set, this node stores only its shards of the
    /// state (clustered Base, experimental) and reads the rest from its peers. Requires an
    /// initialised datadir and `--engine.num-state-masking-blocks 0`.
    #[arg(long = "cluster.config", value_name = "FILE")]
    pub cluster_config: Option<PathBuf>,
}

/// Reth's `node` command with Base's own node arguments.
#[derive(Debug, Args)]
pub struct BaseNodeCommand<Ext: clap::Args + std::fmt::Debug> {
    /// Reth's node arguments.
    #[command(flatten)]
    pub node: NodeCommand<BaseChainSpecParser, Ext>,

    /// Clustered Base arguments.
    #[command(flatten)]
    pub cluster: ClusterArgs,
}

/// Chainless execution-node arguments shared by embedded Base commands.
#[derive(Debug, Clone, Args)]
pub struct ExecutionNodeConfigArgs {
    /// The path to the configuration file to use.
    #[arg(long, value_name = "FILE", verbatim_doc_comment)]
    pub config: Option<PathBuf>,

    /// Prometheus metrics configuration.
    #[command(flatten)]
    pub metrics: MetricArgs,

    /// Add a new instance of a node.
    ///
    /// Configures the ports of the node to avoid conflicts with the defaults.
    ///
    /// Max number of instances is 200.
    #[arg(long, value_name = "INSTANCE", global = true, value_parser = value_parser!(u16).range(1..=200))]
    pub instance: Option<u16>,

    /// Sets all ports to unused, allowing the OS to choose random unused ports when sockets are
    /// bound.
    #[arg(long, conflicts_with = "instance", global = true)]
    pub with_unused_ports: bool,

    /// All datadir related arguments.
    #[command(flatten)]
    pub datadir: DatadirArgs,

    /// All networking related arguments.
    #[command(flatten)]
    pub network: NetworkArgs,

    /// All rpc related arguments.
    #[command(flatten)]
    pub rpc: RpcServerArgs,

    /// All txpool related arguments with --txpool prefix.
    #[command(flatten)]
    pub txpool: TxPoolArgs,

    /// All debug related arguments with --debug prefix.
    #[command(flatten)]
    pub debug: DebugArgs,

    /// All database related arguments.
    #[command(flatten)]
    pub db: DatabaseArgs,

    /// All dev related arguments with --dev prefix.
    #[command(flatten)]
    pub dev: DevArgs,

    /// All pruning related arguments.
    #[command(flatten)]
    pub pruning: PruningArgs,

    /// Engine cli arguments.
    #[command(flatten, next_help_heading = "Engine")]
    pub engine: EngineArgs,

    /// All ERA related arguments with --era prefix.
    #[command(flatten, next_help_heading = "ERA")]
    pub era: EraArgs,

    /// All static files related arguments with --static-files prefix.
    #[command(flatten, next_help_heading = "Static Files")]
    pub static_files: StaticFilesArgs,

    /// All storage related arguments with --storage prefix.
    #[command(flatten, next_help_heading = "Storage")]
    pub storage: StorageArgs,

    /// Clustered Base arguments.
    #[command(flatten, next_help_heading = "Cluster")]
    pub cluster: ClusterArgs,
}

impl ExecutionNodeConfigArgs {
    /// Converts parsed args into a chain-injected execution runtime config.
    pub fn into_runtime_config(self, chain: Arc<BaseChainSpec>) -> ExecutionNodeRuntimeConfig {
        let Self {
            config,
            metrics,
            instance,
            with_unused_ports,
            datadir,
            network,
            rpc,
            txpool,
            debug,
            db,
            dev,
            pruning,
            engine,
            era,
            static_files,
            storage,
            cluster,
        } = self;

        let mut node_config = NodeConfig {
            datadir,
            config,
            chain,
            metrics,
            instance,
            network,
            rpc,
            txpool,
            builder: Default::default(),
            debug,
            db,
            dev,
            pruning,
            engine,
            era,
            static_files,
            storage,
            jit: Default::default(),
        };

        node_config.network.max_inbound_peers.get_or_insert(DEFAULT_BASE_MAX_INBOUND_EL_PEERS);
        node_config.network.max_outbound_peers.get_or_insert(DEFAULT_BASE_MAX_OUTBOUND_EL_PEERS);

        ExecutionNodeRuntimeConfig {
            node_config,
            with_unused_ports,
            upgrade_signal_startup: UpgradeSignalStartupMode::ReadAndApply,
            cluster,
        }
    }
}

/// Execution node arguments shared by RPC-style binaries that provide chain selection themselves.
#[derive(Debug, Clone, Args)]
pub struct ExecutionNodeArgs {
    /// Shared execution node arguments.
    #[command(flatten)]
    pub node: ExecutionNodeConfigArgs,

    /// Standard Base execution-node extension arguments.
    #[command(flatten)]
    pub standard: RpcStandardNodeArgs,

    /// Metering RPC and priority-fee resource budget arguments.
    #[command(flatten)]
    pub metering: MeteringArgs,

    /// Shadow indexer `ExEx` arguments.
    #[command(flatten)]
    pub shadow_indexer: ShadowIndexerArgs,
}

impl ExecutionNodeArgs {
    /// Converts parsed args into a launchable standard execution node configuration.
    pub fn into_launch_config(self, chain: Arc<BaseChainSpec>) -> ExecutionNodeLaunchConfig {
        let runtime = self.node.into_runtime_config(chain);
        ExecutionNodeLaunchConfig {
            node_config: runtime.node_config,
            standard: StandardNodeArgs::from(self.standard)
                .with_metering(self.metering)
                .with_shadow_indexer(self.shadow_indexer),
            with_unused_ports: runtime.with_unused_ports,
            upgrade_signal_startup: runtime.upgrade_signal_startup,
            cluster: runtime.cluster,
        }
    }
}

/// A chain-injected execution-node runtime configuration.
#[derive(Debug, Clone)]
pub struct ExecutionNodeRuntimeConfig {
    /// Reth node configuration.
    pub node_config: NodeConfig<BaseChainSpec>,
    /// Whether all ports should be assigned by the OS.
    pub with_unused_ports: bool,
    /// Whether this launch should perform its own upgrade-signal startup read.
    pub upgrade_signal_startup: UpgradeSignalStartupMode,
    /// Clustered Base arguments.
    pub cluster: ClusterArgs,
}

impl ExecutionNodeRuntimeConfig {
    /// Converts reth's `node` command into a runtime config plus its extension arguments.
    ///
    /// Base launches the `node` command itself rather than through
    /// [`NodeCommand::execute`], which opens the database as a bare MDBX environment.
    pub fn from_node_command<Ext>(command: BaseNodeCommand<Ext>) -> eyre::Result<(Self, Ext)>
    where
        Ext: clap::Args + std::fmt::Debug,
    {
        let BaseNodeCommand { node, cluster } = command;
        let NodeCommand {
            datadir,
            config,
            chain,
            metrics,
            instance,
            with_unused_ports,
            network,
            rpc,
            txpool,
            builder,
            debug,
            db,
            dev,
            pruning,
            engine,
            era,
            static_files,
            storage,
            jit,
            ext,
        } = node;
        engine.validate()?;

        let node_config = NodeConfig {
            datadir,
            config,
            chain,
            metrics,
            instance,
            network,
            rpc,
            txpool,
            builder,
            debug,
            db,
            dev,
            pruning,
            engine,
            era,
            static_files,
            storage,
            jit,
        };
        let runtime = Self {
            node_config,
            with_unused_ports,
            upgrade_signal_startup: UpgradeSignalStartupMode::ReadAndApply,
            cluster,
        };
        Ok((runtime, ext))
    }

    /// Enables authenticated Engine API over IPC on the supplied node config.
    pub const fn enable_auth_ipc(node_config: &mut NodeConfig<BaseChainSpec>) {
        node_config.rpc.auth_ipc = true;
    }

    /// Configures the embedded execution node auth endpoint used by unified Base binaries.
    ///
    /// Embedded consensus talks to the auth module in memory, so the auth server is only bound to
    /// an OS-assigned loopback port and auth IPC is left to the `--auth-ipc` flag.
    pub fn configure_unified_auth_endpoint(node_config: &mut NodeConfig<BaseChainSpec>) {
        node_config.rpc.auth_port = 0;
        node_config.rpc.auth_addr = Ipv4Addr::LOCALHOST.into();
    }

    /// Enables authenticated Engine API over IPC.
    pub const fn with_auth_ipc(mut self) -> Self {
        Self::enable_auth_ipc(&mut self.node_config);
        self
    }

    /// Marks the upgrade-signal startup schedule as already applied by the caller.
    pub const fn with_upgrade_signal_startup_already_applied(mut self) -> Self {
        self.upgrade_signal_startup = UpgradeSignalStartupMode::AlreadyApplied;
        self
    }

    /// Configures authenticated Engine API access for unified Base binaries.
    pub fn with_unified_auth_endpoint(mut self) -> Self {
        Self::configure_unified_auth_endpoint(&mut self.node_config);
        self
    }

    /// Converts the runtime config into a reth node builder.
    pub fn into_node_builder<Rpc>(mut self, ctx: CliContext) -> eyre::Result<BaseNodeBuilder>
    where
        Rpc: RpcModuleValidator,
    {
        if let Some(http_api) = &self.node_config.rpc.http_api {
            Rpc::validate_selection(http_api, "http.api").map_err(|e| eyre::eyre!("{e}"))?;
        }
        if let Some(ws_api) = &self.node_config.rpc.ws_api {
            Rpc::validate_selection(ws_api, "ws.api").map_err(|e| eyre::eyre!("{e}"))?;
        }

        info!(
            target: "reth::cli",
            version = ?version::version_metadata().short_version,
            client = %version::version_metadata().name_client,
            "Starting client"
        );

        if self.with_unused_ports {
            self.node_config = self.node_config.with_unused_ports();
        }

        let database = self.open_database()?;
        let builder = NodeBuilder::new(self.node_config)
            .with_database(database)
            .with_launch_context(ctx.task_executor);

        Ok(builder)
    }

    /// Opens the node's database: clustered when a cluster config is set, otherwise a
    /// pass-through to MDBX.
    pub fn open_database(&self) -> eyre::Result<ClusteredDatabase> {
        let db_path = self.node_config.datadir().db();
        info!(target: "reth::cli", path = ?db_path, "Opening database");
        let env = init_db(db_path, self.node_config.db.database_args())?
            .with_metrics_if(self.node_config.db.metrics_enabled());
        match &self.cluster.cluster_config {
            None => Ok(ClusteredDatabase::new(env)),
            Some(config) => self.open_clustered(env, config),
        }
    }

    /// Checks the clustered node's operating requirements, starts its owner server, and sheds
    /// in the background.
    fn open_clustered(&self, env: DatabaseEnv, config: &Path) -> eyre::Result<ClusteredDatabase> {
        let masking = self.node_config.engine.num_state_masking_blocks;
        eyre::ensure!(
            masking == 0,
            "a clustered node requires --engine.num-state-masking-blocks 0, got {masking}"
        );
        eyre::ensure!(
            !self.node_config.debug.skip_state_root,
            "a clustered node computes every state root; remove --debug.skip-state-root"
        );
        let finish = env.tx()?.get::<tables::StageCheckpoints>(StageId::Finish.to_string())?;
        eyre::ensure!(
            finish.is_some(),
            "a clustered node requires an initialised datadir; run `init` without a cluster config"
        );
        let cluster = Arc::new(Cluster::load(config)?);
        let address = cluster
            .address(cluster.node_id())
            .ok_or_else(|| eyre::eyre!("cluster config does not list this node"))?;
        let listener = TcpListener::bind(address)?;
        let options = cluster.config().client_options();
        let database = ClusteredDatabase::clustered(env, cluster, listener, options)?;
        if let Some(node) = database.cluster() {
            node.spawn_shed()?;
        }
        Ok(database)
    }

    /// Converts the runtime config into a reth node builder with the default RPC validator.
    pub fn into_default_node_builder(self, ctx: CliContext) -> eyre::Result<BaseNodeBuilder> {
        self.into_node_builder::<LenientRpcModuleValidator>(ctx)
    }
}

/// A chain-injected standard execution node configuration ready to launch.
#[derive(Debug, Clone)]
pub struct ExecutionNodeLaunchConfig {
    /// Reth node configuration.
    pub node_config: NodeConfig<BaseChainSpec>,
    /// Standard Base execution-node extension arguments.
    pub standard: StandardNodeArgs,
    /// Whether all ports should be assigned by the OS.
    pub with_unused_ports: bool,
    /// Whether this launch should perform its own upgrade-signal startup read.
    pub upgrade_signal_startup: UpgradeSignalStartupMode,
    /// Clustered Base arguments.
    pub cluster: ClusterArgs,
}

impl ExecutionNodeLaunchConfig {
    /// Converts this standard launch config into the shared runtime config plus standard args.
    pub fn into_runtime_config(self) -> (ExecutionNodeRuntimeConfig, StandardNodeArgs) {
        let Self { node_config, standard, with_unused_ports, upgrade_signal_startup, cluster } =
            self;
        (
            ExecutionNodeRuntimeConfig {
                node_config,
                with_unused_ports,
                upgrade_signal_startup,
                cluster,
            },
            standard,
        )
    }

    /// Enables authenticated Engine API over IPC.
    pub const fn with_auth_ipc(mut self) -> Self {
        ExecutionNodeRuntimeConfig::enable_auth_ipc(&mut self.node_config);
        self
    }

    /// Marks the upgrade-signal startup schedule as already applied by the caller.
    pub const fn with_upgrade_signal_startup_already_applied(mut self) -> Self {
        self.upgrade_signal_startup = UpgradeSignalStartupMode::AlreadyApplied;
        self
    }

    /// Configures authenticated Engine API access for unified Base binaries.
    pub fn with_unified_auth_endpoint(mut self) -> Self {
        ExecutionNodeRuntimeConfig::configure_unified_auth_endpoint(&mut self.node_config);
        self
    }

    /// Launches the execution node and returns its handle.
    pub async fn launch<Rpc>(self, ctx: CliContext) -> eyre::Result<LaunchedBaseNode>
    where
        Rpc: RpcModuleValidator,
    {
        let (execution, standard) = self.into_runtime_config();
        let upgrade_signal_startup = execution.upgrade_signal_startup;
        let builder = execution.into_node_builder::<Rpc>(ctx)?;
        crate::StandardBaseRethNode::launch_with_upgrade_signal_startup(
            builder,
            standard,
            upgrade_signal_startup,
        )
        .await
    }

    /// Launches the execution node with the default RPC module validator.
    pub async fn launch_default(self, ctx: CliContext) -> eyre::Result<LaunchedBaseNode> {
        self.launch::<LenientRpcModuleValidator>(ctx).await
    }
}

#[cfg(test)]
mod tests {
    use clap::Parser;
    use reth_cli_commands::node::NoArgs;
    use reth_db_api::transaction::DbTxMut;

    use super::*;

    #[derive(Debug, Parser)]
    struct CommandParser<T: Args> {
        #[command(flatten)]
        args: T,
    }

    #[test]
    fn shared_execution_args_parse_without_standard_node_args() {
        let args = CommandParser::<ExecutionNodeConfigArgs>::parse_from([
            "reth",
            "--port",
            "30333",
            "--auth-ipc.path=/tmp/engine.ipc",
        ])
        .args;

        assert_eq!(args.network.port, 30333);

        let runtime = args.into_runtime_config(Arc::new(BaseChainSpec::devnet()));
        assert_eq!(runtime.node_config.rpc.auth_ipc_path, "/tmp/engine.ipc");
        assert!(!runtime.node_config.rpc.auth_ipc);
    }

    #[test]
    fn standard_execution_args_keep_base_extension_args_separate() {
        let args = CommandParser::<ExecutionNodeArgs>::parse_from([
            "reth",
            "--port",
            "30333",
            "--rollup.sequencer",
            "http://localhost:8545",
        ])
        .args;

        assert_eq!(args.node.network.port, 30333);
        assert_eq!(args.standard.rollup_args.sequencer.as_deref(), Some("http://localhost:8545"));
        assert!(!args.metering.enable_metering);
    }

    #[test]
    fn standard_execution_args_parse_metering_separately() {
        let args = CommandParser::<ExecutionNodeArgs>::parse_from([
            "reth",
            "--enable-metering",
            "--metering.metered-opcodes",
            "SSTORE",
        ])
        .args;

        assert!(args.metering.enable_metering);
        assert_eq!(args.metering.metering_metered_opcodes, vec!["SSTORE".to_string()]);

        let launch_config = args.into_launch_config(Arc::new(BaseChainSpec::devnet()));
        assert!(launch_config.standard.metering.enable_metering);
        assert_eq!(
            launch_config.standard.metering.metering_metered_opcodes,
            vec!["SSTORE".to_string()]
        );
    }

    #[test]
    fn runtime_config_can_enable_auth_ipc_without_standard_node_args() {
        let args = CommandParser::<ExecutionNodeConfigArgs>::parse_from([
            "reth",
            "--auth-ipc.path=/tmp/engine.ipc",
        ])
        .args;

        let runtime = args.into_runtime_config(Arc::new(BaseChainSpec::devnet())).with_auth_ipc();

        assert!(runtime.node_config.rpc.auth_ipc);
        assert_eq!(runtime.node_config.rpc.auth_ipc_path, "/tmp/engine.ipc");
    }

    #[test]
    fn unified_auth_endpoint_binds_loopback_without_enabling_ipc() {
        let args = CommandParser::<ExecutionNodeConfigArgs>::parse_from([
            "reth",
            "--authrpc.addr=0.0.0.0",
            "--authrpc.port=8551",
        ])
        .args;

        let runtime = args
            .into_runtime_config(Arc::new(BaseChainSpec::devnet()))
            .with_unified_auth_endpoint();

        assert!(!runtime.node_config.rpc.auth_ipc);
        assert_eq!(runtime.node_config.rpc.auth_port, 0);
        assert_eq!(runtime.node_config.rpc.auth_addr, Ipv4Addr::LOCALHOST);
    }

    #[test]
    fn runtime_config_sets_base_default_el_peer_limits() {
        let args = CommandParser::<ExecutionNodeConfigArgs>::parse_from(["reth"]).args;

        let runtime = args.into_runtime_config(Arc::new(BaseChainSpec::devnet()));

        assert_eq!(runtime.node_config.network.max_inbound_peers, Some(80));
        assert_eq!(runtime.node_config.network.max_outbound_peers, Some(80));
    }

    #[test]
    fn runtime_config_preserves_explicit_el_peer_limits() {
        let args = CommandParser::<ExecutionNodeConfigArgs>::parse_from([
            "reth",
            "--max-inbound-peers",
            "12",
            "--max-outbound-peers",
            "34",
        ])
        .args;

        let runtime = args.into_runtime_config(Arc::new(BaseChainSpec::devnet()));

        assert_eq!(runtime.node_config.network.max_inbound_peers, Some(12));
        assert_eq!(runtime.node_config.network.max_outbound_peers, Some(34));
    }

    /// A runtime config over a fresh datadir in `dir` with a one-node cluster config.
    fn clustered_runtime(dir: &Path, masking_blocks: &str) -> ExecutionNodeRuntimeConfig {
        let config = dir.join("cluster.toml");
        std::fs::write(
            &config,
            "node_id = 1\nreplication = 1\n[[nodes]]\nid = 1\naddress = \"127.0.0.1:0\"\n",
        )
        .unwrap();
        let datadir = dir.join("data");
        let args = CommandParser::<ExecutionNodeArgs>::parse_from([
            "reth",
            "--datadir",
            datadir.to_str().unwrap(),
            "--engine.num-state-masking-blocks",
            masking_blocks,
            "--cluster.config",
            config.to_str().unwrap(),
        ])
        .args;
        args.into_launch_config(Arc::new(BaseChainSpec::devnet())).into_runtime_config().0
    }

    #[test]
    fn node_command_takes_a_cluster_config() {
        let command = CommandParser::<BaseNodeCommand<NoArgs>>::parse_from([
            "reth",
            "--chain",
            "base",
            "--cluster.config",
            "/etc/base/cluster.toml",
        ])
        .args;
        let (runtime, _) = ExecutionNodeRuntimeConfig::from_node_command(command).unwrap();
        assert_eq!(runtime.cluster.cluster_config, Some(PathBuf::from("/etc/base/cluster.toml")));
    }

    #[test]
    fn clustered_database_requires_its_operating_requirements() {
        let dir = tempfile::tempdir().unwrap();

        let masked = clustered_runtime(dir.path(), "1").open_database().unwrap_err();
        assert!(masked.to_string().contains("num-state-masking-blocks"), "{masked}");

        let runtime = clustered_runtime(dir.path(), "0");
        let uninitialised = runtime.open_database().unwrap_err();
        assert!(uninitialised.to_string().contains("initialised datadir"), "{uninitialised}");

        let pass_through =
            ExecutionNodeRuntimeConfig { cluster: ClusterArgs::default(), ..runtime.clone() }
                .open_database()
                .unwrap();
        assert!(pass_through.cluster().is_none());
        let tx = pass_through.tx_mut().unwrap();
        tx.put::<tables::StageCheckpoints>(StageId::Finish.to_string(), Default::default())
            .unwrap();
        tx.commit().unwrap();
        drop(pass_through);

        let mut skipping_state_roots = runtime.clone();
        skipping_state_roots.node_config.debug.skip_state_root = true;
        let skipped = skipping_state_roots.open_database().unwrap_err();
        assert!(skipped.to_string().contains("skip-state-root"), "{skipped}");

        let clustered = runtime.open_database().unwrap();
        assert!(clustered.cluster().is_some());
    }

    #[test]
    fn launch_config_delegates_auth_ipc_to_runtime_config() {
        let args = CommandParser::<ExecutionNodeArgs>::parse_from([
            "reth",
            "--auth-ipc.path=/tmp/engine.ipc",
        ])
        .args;

        let launch = args.into_launch_config(Arc::new(BaseChainSpec::devnet())).with_auth_ipc();

        assert!(launch.node_config.rpc.auth_ipc);
        assert_eq!(launch.node_config.rpc.auth_ipc_path, "/tmp/engine.ipc");

        let (runtime, _standard) = launch.into_runtime_config();
        assert!(runtime.node_config.rpc.auth_ipc);
        assert_eq!(runtime.node_config.rpc.auth_ipc_path, "/tmp/engine.ipc");
    }
}
