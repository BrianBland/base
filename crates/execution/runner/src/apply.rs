//! Apply mode: an engine-tree validator that inserts a new payload from a peer's execution result
//! instead of executing it, and executes it as before whenever that fails.

use std::{
    cell::OnceCell,
    fmt,
    sync::{Arc, OnceLock},
    time::{Duration, Instant},
};

use alloy_primitives::B256;
use base_common_consensus::BasePrimitives;
use base_common_rpc_types_engine::ExecutionData;
use base_execution_cluster::{ApplyFallback, ClusterNode, ExecutedBlockResult};
use base_node_core::{BaseEngineTypes, BaseNodeTypes, BasePayloadValidatorBuilder};
use reth_chain_state::ExecutedBlock;
use reth_consensus::{ConsensusError, FullConsensus};
use reth_engine_tree::tree::{
    CacheWaitDurations, EngineApiTreeState, EngineValidator, TreeConfig, ValidationOutcome,
    ValidationOutput, WaitForCaches, payload_validator::TreeCtx,
};
use reth_network_p2p::full_block::SealedBlockWithAccessList;
use reth_node_api::{
    AddOnsContext, BuiltPayloadExecutedBlock, InsertBlockErrorKind, InvalidPayloadAttributesError,
    NewPayloadError, NodePrimitives, PayloadTypes, PayloadValidator,
};
use reth_node_builder::{
    FullNodeComponents,
    rpc::{BasicEngineValidatorBuilder, EngineValidatorBuilder, PayloadValidatorBuilder},
};
use reth_payload_builder::PayloadBuilderResources;
use reth_primitives_traits::{RecoveredBlock, SealedBlock, SealedHeader};
use reth_provider::{HeaderProvider, ProviderError, StateProviderFactory};
use reth_storage_overlay::OverlayManager;
use reth_trie_common::HashedPostState;
use thiserror::Error;
use tracing::warn;

type BaseBlock = <BasePrimitives as NodePrimitives>::Block;
type BaseHeader = <BasePrimitives as NodePrimitives>::BlockHeader;
type PayloadAttributes = <BaseEngineTypes as PayloadTypes>::PayloadAttributes;
type BasePayloadValidator<Node> =
    <BasePayloadValidatorBuilder as PayloadValidatorBuilder<Node>>::Validator;

/// Builds the node's engine-tree validator: reth's basic validator, wrapped by an
/// [`ApplyEngineValidator`] that applies peers' results when the installed cluster node lists
/// nodes to apply from. Clones share the installed node.
#[derive(Debug, Clone, Default)]
pub struct ApplyEngineValidatorBuilder {
    cluster: Arc<OnceLock<Arc<ClusterNode>>>,
}

impl ApplyEngineValidatorBuilder {
    /// Makes validators built afterwards apply from `node`'s `apply_from` nodes. The node is known
    /// only once the node builder holds the database, after the add-ons are chosen. Only the first
    /// node installed is kept.
    pub fn install_cluster(&self, node: Arc<ClusterNode>) {
        let _ = self.cluster.set(node);
    }
}

impl<Node> EngineValidatorBuilder<Node> for ApplyEngineValidatorBuilder
where
    Node: FullNodeComponents<Types: BaseNodeTypes>,
    BasicEngineValidatorBuilder<BasePayloadValidatorBuilder>: EngineValidatorBuilder<Node>,
    BasePayloadValidatorBuilder: PayloadValidatorBuilder<Node>,
    BasePayloadValidator<Node>: PayloadValidator<BaseEngineTypes, Block = BaseBlock>,
{
    type EngineValidator = ApplyEngineValidator<
        <BasicEngineValidatorBuilder<BasePayloadValidatorBuilder> as EngineValidatorBuilder<
            Node,
        >>::EngineValidator,
        Node::Provider,
        BasePayloadValidator<Node>,
    >;

    async fn build_tree_validator(
        self,
        ctx: &AddOnsContext<'_, Node>,
        tree_config: TreeConfig,
        overlay_manager: OverlayManager<BasePrimitives>,
    ) -> eyre::Result<Self::EngineValidator> {
        let inner = BasicEngineValidatorBuilder::new(BasePayloadValidatorBuilder::default())
            .build_tree_validator(ctx, tree_config, overlay_manager)
            .await?;
        let applier = match self.cluster.get() {
            Some(node) if !node.cluster().apply_from().is_empty() => Some(BlockApplier {
                node: Arc::clone(node),
                provider: ctx.node.provider().clone(),
                consensus: Arc::new(ctx.node.consensus().clone()),
                payload_validator: BasePayloadValidatorBuilder::default().build(ctx).await?,
            }),
            _ => None,
        };
        Ok(ApplyEngineValidator { inner, applier })
    }
}

/// An engine-tree validator that, with an applier, inserts new payloads from peers' execution
/// results and executes through `inner` whenever applying fails; without one it is `inner`.
#[derive(Debug)]
pub struct ApplyEngineValidator<V, P, PV> {
    inner: V,
    applier: Option<BlockApplier<P, PV>>,
}

impl<V, P, PV> EngineValidator<BaseEngineTypes, BasePrimitives> for ApplyEngineValidator<V, P, PV>
where
    V: EngineValidator<BaseEngineTypes, BasePrimitives>,
    P: HeaderProvider<Header = BaseHeader> + StateProviderFactory + Send + Sync + 'static,
    PV: PayloadValidator<BaseEngineTypes, Block = BaseBlock> + Send + Sync + 'static,
{
    fn validate_payload_attributes_against_header(
        &self,
        attr: &PayloadAttributes,
        header: &BaseHeader,
    ) -> Result<(), InvalidPayloadAttributesError> {
        self.inner.validate_payload_attributes_against_header(attr, header)
    }

    fn convert_payload_to_block(
        &self,
        payload: ExecutionData,
    ) -> Result<SealedBlock<BaseBlock>, NewPayloadError> {
        self.inner.convert_payload_to_block(payload)
    }

    fn validate_payload(
        &mut self,
        payload: ExecutionData,
        ctx: TreeCtx<'_, BasePrimitives>,
    ) -> ValidationOutcome<BasePrimitives> {
        let Some(applier) = &self.applier else {
            return self.inner.validate_payload(payload, ctx);
        };
        let applied = applier
            .apply(&payload, ctx.state(), |payload| self.inner.convert_payload_to_block(payload));
        match applied {
            Ok(executed_block) => {
                applier.node.apply_stats().record_applied();
                Ok(ValidationOutput::new(executed_block, None))
            }
            Err(error) => {
                let reason = error.reason();
                applier.node.apply_stats().record_fallback(reason);
                warn!(
                    block = %payload.payload.block_hash(),
                    number = payload.payload.block_number(),
                    reason = reason.label(),
                    error = %error,
                    "executing a payload instead of applying a peer's result"
                );
                self.inner.validate_payload(payload, ctx)
            }
        }
    }

    fn validate_block(
        &mut self,
        block: SealedBlockWithAccessList<BaseBlock>,
        ctx: TreeCtx<'_, BasePrimitives>,
    ) -> ValidationOutcome<BasePrimitives> {
        self.inner.validate_block(block, ctx)
    }

    fn on_inserted_executed_block(
        &self,
        block: BuiltPayloadExecutedBlock<BasePrimitives>,
    ) -> reth_provider::ProviderResult<ExecutedBlock<BasePrimitives>> {
        self.inner.on_inserted_executed_block(block)
    }

    fn on_canonical_head_changed(&self, hash: B256, state: &EngineApiTreeState<BasePrimitives>) {
        self.inner.on_canonical_head_changed(hash, state);
    }

    fn payload_builder_resources(
        &self,
        parent_hash: B256,
        parent_header: &BaseHeader,
        timestamp: u64,
        state: &mut EngineApiTreeState<BasePrimitives>,
    ) -> PayloadBuilderResources {
        self.inner.payload_builder_resources(parent_hash, parent_header, timestamp, state)
    }
}

impl<V: WaitForCaches, P, PV> WaitForCaches for ApplyEngineValidator<V, P, PV> {
    fn wait_for_caches(&self) -> CacheWaitDurations {
        self.inner.wait_for_caches()
    }
}

/// Why a payload could not be applied from a peer's result.
#[derive(Debug, Error)]
pub enum ApplyError {
    /// The payload carries a block access list, which apply mode does not carry over.
    #[error("payload carries a block access list")]
    AccessList,
    /// The parent's header is neither in the tree nor in the database.
    #[error("parent {0} is unknown")]
    UnknownParent(B256),
    /// The parent's header or state could not be read.
    #[error("parent unavailable: {0}")]
    Parent(#[from] ProviderError),
    /// The payload does not convert to a block.
    #[error("payload conversion failed: {0}")]
    Conversion(#[from] NewPayloadError),
    /// The block fails a header or pre-execution check.
    #[error("pre-execution check failed: {0}")]
    PreExecution(ConsensusError),
    /// No result was fetched.
    #[error("no result fetched")]
    Fetch(ApplyFallback),
    /// The fetched result does not decode.
    #[error("result does not decode: {0}")]
    Decode(String),
    /// The result's senders do not match the block's transactions.
    #[error("result has {senders} senders for {transactions} transactions")]
    Senders {
        /// Senders in the result.
        senders: usize,
        /// Transactions in the block.
        transactions: usize,
    },
    /// The result carries execution requests, which Base never produces.
    #[error("result carries {0} execution requests")]
    Requests(usize),
    /// The result fails a post-execution check against the block.
    #[error("post-execution check failed: {0}")]
    PostExecution(ConsensusError),
    /// The result fails a Base post-execution check against the block and its parent's state.
    #[error("Base post-execution check failed: {0}")]
    BasePostExecution(InsertBlockErrorKind),
}

impl ApplyError {
    /// The fallback reason this error is counted under.
    pub const fn reason(&self) -> ApplyFallback {
        match self {
            Self::Fetch(reason) => *reason,
            Self::Decode(_) | Self::Senders { .. } | Self::Requests(_) | Self::PostExecution(_) => {
                ApplyFallback::Invalid
            }
            Self::BasePostExecution(error) if error.is_validation_error() => ApplyFallback::Invalid,
            Self::AccessList
            | Self::UnknownParent(_)
            | Self::Parent(_)
            | Self::Conversion(_)
            | Self::PreExecution(_)
            | Self::BasePostExecution(_) => ApplyFallback::Ineligible,
        }
    }
}

/// Inserts new payloads from the execution results of a cluster's `apply_from` nodes, running the
/// checks execution runs before and after the EVM.
pub struct BlockApplier<P, PV> {
    node: Arc<ClusterNode>,
    provider: P,
    consensus: Arc<dyn FullConsensus<BasePrimitives>>,
    payload_validator: PV,
}

impl<P, PV> fmt::Debug for BlockApplier<P, PV> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BlockApplier")
            .field("apply_from", &self.node.cluster().apply_from())
            .finish_non_exhaustive()
    }
}

impl<P, PV> BlockApplier<P, PV>
where
    P: HeaderProvider<Header = BaseHeader> + StateProviderFactory,
    PV: PayloadValidator<BaseEngineTypes, Block = BaseBlock>,
{
    /// Longest total time spent fetching one block's result before executing it instead.
    pub const FETCH_BUDGET: Duration = Duration::from_millis(400);

    /// The executed block of `payload` built from a peer's result. `convert` is the node's own
    /// payload-to-block conversion.
    pub fn apply(
        &self,
        payload: &ExecutionData,
        tree: &EngineApiTreeState<BasePrimitives>,
        convert: impl FnOnce(ExecutionData) -> Result<SealedBlock<BaseBlock>, NewPayloadError>,
    ) -> Result<ExecutedBlock<BasePrimitives>, ApplyError> {
        if payload.block_access_list.is_some() {
            return Err(ApplyError::AccessList);
        }
        let parent_hash = payload.payload.parent_hash();
        let parent = match tree.tree_state().sealed_header_by_hash(&parent_hash) {
            Some(parent) => parent,
            None => self
                .provider
                .sealed_header_by_hash(parent_hash)?
                .ok_or(ApplyError::UnknownParent(parent_hash))?,
        };
        let block = convert(payload.clone())?;
        self.validate_pre_execution(&block, &parent).map_err(ApplyError::PreExecution)?;

        let started = Instant::now();
        let fetched = self.node.client().executed_block(block.hash(), Self::FETCH_BUDGET);
        self.node.apply_stats().record_fetch(started.elapsed());
        let parts = ExecutedBlockResult::decode(&fetched.map_err(ApplyError::Fetch)?)
            .map_err(|error| ApplyError::Decode(error.to_string()))?
            .into_parts()
            .map_err(|error| ApplyError::Decode(error.to_string()))?;

        let transactions = block.body().transactions.len();
        if parts.senders.len() != transactions {
            return Err(ApplyError::Senders { senders: parts.senders.len(), transactions });
        }
        // Base's post-execution validation does not read the requests.
        let requests = parts.output.result.requests.len();
        if requests != 0 {
            return Err(ApplyError::Requests(requests));
        }
        let block = RecoveredBlock::new_sealed(block, parts.senders);
        self.consensus
            .validate_block_post_execution(&block, &parts.output.result, None, None)
            .map_err(ApplyError::PostExecution)?;
        let hashed_state = OnceCell::new();
        self.payload_validator
            .validate_block_post_execution_with_hashed_state(
                || {
                    hashed_state.get_or_init(|| {
                        HashedPostState::from(parts.trie.hashed_state.as_ref().clone())
                    })
                },
                &block,
                &parent,
                || self.provider.state_by_block_hash(parent_hash),
            )
            .map_err(ApplyError::BasePostExecution)?;
        Ok(ExecutedBlock::new(Arc::new(block), Arc::new(parts.output), parts.trie))
    }

    /// The header and body checks execution runs on a block before the EVM.
    fn validate_pre_execution(
        &self,
        block: &SealedBlock<BaseBlock>,
        parent: &SealedHeader<BaseHeader>,
    ) -> Result<(), ConsensusError> {
        self.consensus.validate_header(block.sealed_header())?;
        self.consensus.validate_header_against_parent(block.sealed_header(), parent)?;
        self.consensus.validate_block_pre_execution_with_tx_root(block, None)
    }
}
