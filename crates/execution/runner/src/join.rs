//! Starting a node that stores no state on an empty datadir: it joins its cluster at a peer's
//! persisted block before reth opens the datadir.

use std::{sync::Arc, time::Instant};

use base_common_consensus::BasePrimitives;
use base_execution_chainspec::BaseChainSpec;
use base_execution_cluster::{ClusterJoin, ClusterNode, ClusteredDatabase, JoinMarker};
use eyre::Result;
use reth_db_api::{Database, tables, transaction::DbTx};
use reth_db_common::init::init_genesis_with_settings;
use reth_node_builder::{NodeConfig, NodeTypesWithDBAdapter};
use reth_primitives_traits::{AlloyBlockHeader, NodePrimitives};
use reth_provider::{
    DBProvider, DatabaseProviderFactory, ProviderFactory,
    providers::{RocksDBProvider, StaticFileProvider},
};
use reth_stages_types::StageId;
use reth_tasks::Runtime;
use tracing::info;

use crate::node::BaseNode;

type JoinFactory = ProviderFactory<NodeTypesWithDBAdapter<BaseNode, ClusteredDatabase>>;
type Header = <BasePrimitives as NodePrimitives>::BlockHeader;

/// Joins a node that stores no state to its cluster when its datadir is empty.
#[derive(Debug, Clone, Copy)]
pub struct ClusterJoinStart;

impl ClusterJoinStart {
    /// Whether `node`, over `db`, must join before launching: it applies peers' results
    /// (`apply_from`, which only a node that stores nothing may list) and its datadir has no
    /// `Finish` checkpoint. Every other clustered node on an empty datadir gets its genesis from
    /// reth's launch as before.
    pub fn is_needed(node: &ClusterNode, db: &ClusteredDatabase) -> Result<bool> {
        if node.cluster().apply_from().is_empty() {
            return Ok(false);
        }
        Ok(db.tx()?.get::<tables::StageCheckpoints>(StageId::Finish.to_string())?.is_none())
    }

    /// Joins: marks the datadir, writes genesis as reth's launch does, fetches a confirmed join
    /// point from the `apply_from` peers (asking for up to the config's `read_retry_secs`),
    /// writes it (nothing when the peer is still at genesis), and removes the mark. A datadir the process left marked is refused at the next
    /// start ([`JoinMarker::ensure_absent`]). Blocks on the network; run it off async threads.
    pub fn run(
        node: &Arc<ClusterNode>,
        db: &ClusteredDatabase,
        config: &NodeConfig<BaseChainSpec>,
        runtime: &Runtime,
    ) -> Result<()> {
        let started = Instant::now();
        let datadir = config.datadir();
        JoinMarker::create(datadir.data_dir())?;
        let genesis_hash = init_genesis_with_settings(
            &Self::factory(db, config, runtime)?,
            config.storage_settings(),
        )?;
        let budget = node.cluster().config().client_options().retry_for;
        let (peer, headers) =
            ClusterJoin::fetch::<Header>(node.client(), node.cluster(), genesis_hash, budget)?;
        let block = headers.as_ref().map_or(0, |headers| headers.tip().number());
        info!(peer = peer.0, block, "cluster node joining at a peer's persisted block");
        if let Some(headers) = headers {
            // A factory of its own: the one that wrote genesis keeps its header writer on
            // genesis's static file.
            let provider = Self::factory(db, config, runtime)?.database_provider_rw()?;
            headers.write(&provider)?;
            provider.commit()?;
        }
        JoinMarker::remove(datadir.data_dir())?;
        info!(block, elapsed = ?started.elapsed(), "cluster node joined");
        Ok(())
    }

    /// A provider factory over the node's datadir, as reth's launch opens it.
    fn factory(
        db: &ClusteredDatabase,
        config: &NodeConfig<BaseChainSpec>,
        runtime: &Runtime,
    ) -> Result<JoinFactory> {
        let datadir = config.datadir();
        let static_files = StaticFileProvider::read_write(datadir.static_files())?;
        let rocksdb = RocksDBProvider::builder(datadir.rocksdb()).with_default_tables().build()?;
        Ok(ProviderFactory::new(
            db.clone(),
            Arc::clone(&config.chain),
            static_files,
            rocksdb,
            runtime.clone(),
        )?)
    }
}
