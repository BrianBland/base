//! Off-by-default ahead-of-builder speculation for the basic payload builder.
//!
//! The contract lives in `crates/common/evm/SPECULATOR.md` ("Phase 2").

use std::{
    fmt,
    panic::resume_unwind,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

use alloy_consensus::transaction::Recovered;
use alloy_evm::EvmEnv;
use alloy_primitives::{Address, B256, U256};
use base_common_chains::Upgrades;
use base_common_consensus::{BasePrimitives, BaseTxEnvelope};
use base_common_evm::{
    BaseBlockExecutionCtx, BaseBlockExecutorFactory, BaseEvmFactory, BaseSpecId, ParallelDatabase,
    SpeculationParent, Speculator,
};
use base_execution_evm::{BaseEvmConfig, BaseRethReceiptBuilder};
use reth_evm::{ConfigureEvm, EvmEnvFor, ExecutionCtxFor};
use reth_primitives_traits::TxTy;
use reth_revm::database::StateProviderDatabase;
use reth_storage_api::{StateProviderBox, StateProviderFactory, errors::ProviderError};
use reth_transaction_pool::BestTransactionsAttributes;
use revm::{
    DatabaseRef,
    primitives::hardfork::SpecId,
    state::{AccountInfo, Bytecode},
};
use tracing::{debug, warn};

use crate::BuilderMetrics;

/// Creates a worker-local parent-state reader; called on the worker thread that uses it.
pub type SpeculativeDatabaseFactory = Arc<dyn Fn() -> Box<dyn ParallelDatabase> + Send + Sync>;

/// EVM configurations whose block executor can consume speculative results.
pub trait SpeculativeEvmConfig: ConfigureEvm {
    /// Returns the epoch for a block built on `hash`, or `None` when `env` is unsupported.
    fn speculation_parent(
        &self,
        hash: B256,
        env: &EvmEnvFor<Self>,
        database: SpeculativeDatabaseFactory,
    ) -> Option<SpeculationParent>;

    /// Lets the block executor consume validated predictions from `speculator`.
    fn install_speculator(ctx: &mut ExecutionCtxFor<'_, Self>, speculator: Arc<Speculator>);

    /// Converts a pool transaction into the speculator's envelope type.
    fn speculation_candidate(
        transaction: Recovered<TxTy<Self::Primitives>>,
    ) -> Recovered<BaseTxEnvelope>;
}

impl<ChainSpec> SpeculativeEvmConfig for BaseEvmConfig<ChainSpec>
where
    ChainSpec: Upgrades,
    Self: ConfigureEvm<
            Primitives = BasePrimitives,
            BlockExecutorFactory = BaseBlockExecutorFactory<
                BaseRethReceiptBuilder,
                Arc<ChainSpec>,
                BaseEvmFactory,
            >,
        >,
{
    fn speculation_parent(
        &self,
        hash: B256,
        env: &EvmEnv<BaseSpecId>,
        database: SpeculativeDatabaseFactory,
    ) -> Option<SpeculationParent> {
        if env.cfg_env.spec.into_eth_spec().is_enabled_in(SpecId::AMSTERDAM) {
            return None;
        }
        Some(SpeculationParent::new(
            hash,
            env.clone(),
            *self.executor_factory.evm_factory(),
            database,
        ))
    }

    fn install_speculator(ctx: &mut BaseBlockExecutionCtx, speculator: Arc<Speculator>) {
        ctx.speculator = Some(speculator);
    }

    fn speculation_candidate(transaction: Recovered<BaseTxEnvelope>) -> Recovered<BaseTxEnvelope> {
        transaction
    }
}

/// Persistent speculative workers shared by every payload job of one builder.
///
/// At most one build owns the workers at a time; a concurrent build runs sequentially.
#[derive(Debug)]
pub struct BuilderSpeculation {
    speculator: Arc<Speculator>,
    busy: AtomicBool,
}

impl BuilderSpeculation {
    /// Wraps persistent workers.
    pub fn new(speculator: Speculator) -> Self {
        Self { speculator: Arc::new(speculator), busy: AtomicBool::new(false) }
    }

    /// Starts `workers` forwarding workers; zero disables speculation.
    pub fn with_workers(workers: usize) -> std::io::Result<Option<Arc<Self>>> {
        if workers == 0 {
            return Ok(None);
        }
        Speculator::new(workers, true).map(|speculator| Some(Arc::new(Self::new(speculator))))
    }

    /// The workers, including counters of the latest epoch.
    pub const fn speculator(&self) -> &Arc<Speculator> {
        &self.speculator
    }

    /// Installs `parent` as the workers' epoch, unless another build owns them.
    pub fn start(self: &Arc<Self>, parent: SpeculationParent) -> Option<SpeculationSession> {
        if self.busy.swap(true, Ordering::AcqRel) {
            debug!(target: "payload_builder", "speculative workers busy; building sequentially");
            return None;
        }
        self.speculator.reset(parent);
        Some(SpeculationSession { speculation: Arc::clone(self) })
    }
}

/// One build's ownership of the workers. Dropping it cancels the epoch and records metrics.
#[derive(Debug)]
pub struct SpeculationSession {
    speculation: Arc<BuilderSpeculation>,
}

impl SpeculationSession {
    /// The workers to install into the block executor.
    pub fn speculator(&self) -> Arc<Speculator> {
        Arc::clone(&self.speculation.speculator)
    }

    /// Feeds pool candidates in builder order; miss counting starts here, after the sequencer
    /// prefix.
    pub fn submit(&self, candidates: &[Recovered<BaseTxEnvelope>]) {
        self.speculation.speculator.reset_owner_timing();
        self.speculation.speculator.submit(candidates);
    }
}

impl Drop for SpeculationSession {
    fn drop(&mut self) {
        let speculator = &self.speculation.speculator;
        speculator.cancel();
        let stats = speculator.stats();
        BuilderMetrics::speculative_hits_total().increment(stats.consumed as u64);
        BuilderMetrics::speculative_misses_total().increment(stats.inline_executions as u64);
        BuilderMetrics::speculative_rejected_total().increment(stats.validation_failures as u64);
        debug!(
            target: "payload_builder",
            hits = stats.consumed,
            misses = stats.inline_executions,
            rejected = stats.validation_failures,
            "speculative execution session ended"
        );
        self.speculation.busy.store(false, Ordering::Release);
    }
}

/// Speculation inputs for one pool build.
pub struct SpeculationFeed<'a> {
    /// Workers shared across builds.
    pub speculation: Arc<BuilderSpeculation>,
    /// Worker-local readers of the parent state.
    pub database: SpeculativeDatabaseFactory,
    /// Independent pool snapshot in the order the builder will see it.
    pub candidates:
        Box<dyn FnOnce(BestTransactionsAttributes) -> Vec<Recovered<BaseTxEnvelope>> + 'a>,
}

impl fmt::Debug for SpeculationFeed<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SpeculationFeed").field("speculation", &self.speculation).finish()
    }
}

/// Worker-local parent-state reader over a reth state provider.
///
/// The speculator's read interface is infallible, and a provider error must never be mistaken
/// for state. A failure is logged and unwinds with the [`ProviderError`] as payload
/// (`resume_unwind` skips the panic hook); the speculator contains the unwind, stops that
/// worker's generation and the affected transactions execute sequentially.
pub struct SpeculativeStateReader {
    // reth providers are `Send` but not `Sync`; the lock is only ever taken by the owning worker.
    provider: Mutex<StateProviderDatabase<StateProviderBox>>,
}

impl fmt::Debug for SpeculativeStateReader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SpeculativeStateReader").finish_non_exhaustive()
    }
}

impl SpeculativeStateReader {
    /// Wraps an already opened provider.
    pub fn new(provider: StateProviderBox) -> Self {
        Self { provider: Mutex::new(StateProviderDatabase::new(provider)) }
    }

    /// Opens the state after block `hash`; a failure aborts the calling worker's generation.
    pub fn open(factory: &impl StateProviderFactory, hash: B256) -> Self {
        Self::new(factory.state_by_block_hash(hash).unwrap_or_else(|error| Self::abort(error)))
    }

    fn read<T>(
        &self,
        read: impl FnOnce(&StateProviderDatabase<StateProviderBox>) -> Result<T, ProviderError>,
    ) -> Result<T, std::convert::Infallible> {
        let provider = self.provider.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        Ok(read(&provider).unwrap_or_else(|error| Self::abort(error)))
    }

    fn abort(error: ProviderError) -> ! {
        warn!(
            target: "payload_builder",
            error = %error,
            "speculative parent-state read failed; aborting speculation for this build"
        );
        resume_unwind(Box::new(error))
    }
}

impl DatabaseRef for SpeculativeStateReader {
    type Error = std::convert::Infallible;

    fn basic_ref(&self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        self.read(|provider| provider.basic_ref(address))
    }

    fn code_by_hash_ref(&self, hash: B256) -> Result<Bytecode, Self::Error> {
        self.read(|provider| provider.code_by_hash_ref(hash))
    }

    fn storage_ref(&self, address: Address, slot: U256) -> Result<U256, Self::Error> {
        self.read(|provider| provider.storage_ref(address, slot))
    }

    fn block_hash_ref(&self, number: u64) -> Result<B256, Self::Error> {
        self.read(|provider| provider.block_hash_ref(number))
    }
}
