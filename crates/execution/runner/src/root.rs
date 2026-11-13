//! The state-root strategy of a clustered node: maintain the node's trie slice instead of
//! computing the full state root.

use std::{fmt, sync::Arc, time::Instant};

use base_common_consensus::BasePrimitives;
use base_execution_cluster::{ClusterNode, LocalReads};
use reth_engine_tree::tree::state_root_strategy::{
    LazyHashedPostState, PreparedStateRootJob, StateRootJob, StateRootJobContext,
    StateRootJobOutcome, StateRootStrategy,
};
use reth_evm::ConfigureEvm;
use reth_node_api::NodePrimitives;
use reth_primitives_traits::{AlloyBlockHeader, RecoveredBlock};
use reth_provider::{
    BlockExecutionOutput, BlockNumReader, ChangeSetReader, DatabaseProviderFactory,
    DatabaseProviderROFactory, ProviderError, ProviderResult, PruneCheckpointReader,
    StageCheckpointReader, StateRootProvider, StorageChangeSetReader, StorageSettingsCache,
};
use reth_storage_overlay::{OverlayManager, OverlayStateProviderFactory};

type BaseBlock = <BasePrimitives as NodePrimitives>::Block;
type BaseReceipt = <BasePrimitives as NodePrimitives>::Receipt;

/// Engine state-root strategy of a clustered node. Each block's state root is taken from its
/// header, not computed, and the block carries the trie updates of this node's slice: the rows
/// of the shards it holds, computed by reth's incremental root code over this node's database and
/// in-memory ancestors with [`TrieSlice`](base_execution_cluster::TrieSlice) prefix sets, inside a
/// [`LocalReads`] scope. A node that stores nothing computes nothing.
pub struct ClusterStateRoot<P> {
    node: Arc<ClusterNode>,
    provider: P,
    overlay_manager: OverlayManager<BasePrimitives>,
}

impl<P> fmt::Debug for ClusterStateRoot<P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClusterStateRoot").field("node", &self.node).finish_non_exhaustive()
    }
}

impl<P> ClusterStateRoot<P> {
    /// The strategy of `node`, over `provider`'s database and the engine's `overlay_manager`.
    pub const fn new(
        node: Arc<ClusterNode>,
        provider: P,
        overlay_manager: OverlayManager<BasePrimitives>,
    ) -> Self {
        Self { node, provider, overlay_manager }
    }
}

impl<P, Evm> StateRootStrategy<BasePrimitives, P, Evm> for ClusterStateRoot<P>
where
    P: DatabaseProviderFactory + Clone + Send + Sync + 'static,
    P::Provider: BlockNumReader
        + PruneCheckpointReader
        + StageCheckpointReader
        + ChangeSetReader
        + StorageChangeSetReader
        + StorageSettingsCache
        + 'static,
    Evm: ConfigureEvm<Primitives = BasePrimitives> + 'static,
{
    fn prepare(
        &self,
        ctx: StateRootJobContext<'_, BasePrimitives, P, Evm>,
    ) -> ProviderResult<PreparedStateRootJob<BasePrimitives>> {
        let state_provider_factory = OverlayStateProviderFactory::new(
            self.provider.clone(),
            self.overlay_manager.overlay_builder(ctx.env().parent_hash),
        );
        let job = TrieSliceJob { node: Arc::clone(&self.node), state_provider_factory };
        Ok(PreparedStateRootJob::new(Box::new(job), None))
    }
}

/// One block's trie-slice computation, run when the block has executed.
struct TrieSliceJob<P> {
    node: Arc<ClusterNode>,
    state_provider_factory: OverlayStateProviderFactory<P, BasePrimitives>,
}

impl<P> StateRootJob<BasePrimitives> for TrieSliceJob<P>
where
    P: DatabaseProviderFactory + Clone + Send + 'static,
    P::Provider: BlockNumReader
        + PruneCheckpointReader
        + StageCheckpointReader
        + ChangeSetReader
        + StorageChangeSetReader
        + StorageSettingsCache
        + 'static,
{
    fn name(&self) -> &'static str {
        "cluster-trie-slice"
    }

    fn finish(
        &mut self,
        block: &RecoveredBlock<BaseBlock>,
        _output: Arc<BlockExecutionOutput<BaseReceipt>>,
        hashed_state: &LazyHashedPostState,
    ) -> ProviderResult<StateRootJobOutcome> {
        let state_root = block.header().state_root();
        let plan = self.node.trie_slice().plan(hashed_state.get()).map_err(ProviderError::other)?;
        if plan.is_empty() {
            return Ok(StateRootJobOutcome::new(state_root, Default::default()));
        }
        let started = Instant::now();
        let side_calls = plan.side_calls.len() as u64;
        let _local = LocalReads::enter();
        let provider = self.state_provider_factory.database_provider_ro()?;
        let updates = plan.compute(|input| {
            provider.state_root_from_nodes_with_updates(input).map(|(_, updates)| updates)
        })?;
        self.node.trie_slice_stats().record_block(side_calls, started.elapsed());
        Ok(StateRootJobOutcome::new(state_root, Arc::new(updates)))
    }
}
