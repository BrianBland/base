//! Fixture validation through the production block executor and state hook.

use std::{
    path::Path,
    sync::{Arc, Mutex},
};

use alloy_consensus::Header;
use alloy_evm::EvmEnv;
use base_common_consensus::{BaseBlock, BaseReceipt};
use base_common_evm::{BaseBlockExecutionCtx, BaseSpecId, ParallelPayload, Workers};
use base_execution_evm::{BaseEvmConfig, BaseNextBlockEnvAttributes};
use eyre::{Result, ensure};
use reth_evm::{ConfigureEvm, execute::Executor};
use reth_primitives_traits::{RecoveredBlock, SealedBlock, SealedHeader};
use revm::{
    DatabaseCommit,
    database::{BundleState, CacheDB, State},
    state::EvmState,
};

use crate::{BlockFixture, PreDb};

/// Bench-only context injection. Production block-based contexts remain sequential.
#[derive(Debug, Clone)]
pub struct ExecutorBench {
    /// Unmodified production configuration.
    pub config: BaseEvmConfig,
    /// Optional payload context used only by this fixture adapter.
    pub payload: Option<ParallelPayload>,
}

impl ConfigureEvm for ExecutorBench {
    type Primitives = <BaseEvmConfig as ConfigureEvm>::Primitives;
    type Error = <BaseEvmConfig as ConfigureEvm>::Error;
    type NextBlockEnvCtx = BaseNextBlockEnvAttributes;
    type BlockExecutorFactory = <BaseEvmConfig as ConfigureEvm>::BlockExecutorFactory;
    type BlockAssembler = <BaseEvmConfig as ConfigureEvm>::BlockAssembler;

    fn block_executor_factory(&self) -> &Self::BlockExecutorFactory {
        self.config.block_executor_factory()
    }
    fn block_assembler(&self) -> &Self::BlockAssembler {
        self.config.block_assembler()
    }
    fn evm_env(&self, header: &Header) -> Result<EvmEnv<BaseSpecId>, Self::Error> {
        self.config.evm_env(header)
    }
    fn next_evm_env(
        &self,
        parent: &Header,
        attributes: &Self::NextBlockEnvCtx,
    ) -> Result<EvmEnv<BaseSpecId>, Self::Error> {
        self.config.next_evm_env(parent, attributes)
    }
    fn context_for_block(
        &self,
        block: &SealedBlock<BaseBlock>,
    ) -> Result<BaseBlockExecutionCtx, Self::Error> {
        let mut context = self.config.context_for_block(block)?;
        context.parallel = self.payload.clone();
        Ok(context)
    }
    fn context_for_next_block(
        &self,
        parent: &SealedHeader<Header>,
        attributes: Self::NextBlockEnvCtx,
    ) -> Result<BaseBlockExecutionCtx, Self::Error> {
        self.config.context_for_next_block(parent, attributes)
    }
}

/// The complete observable output of the production execution path.
#[derive(Debug)]
pub struct ExecutorObservation {
    /// Full receipts, including deposit fields.
    pub receipts: Vec<BaseReceipt>,
    /// Accounts, storage, contracts, and reverts.
    pub bundle: BundleState,
    /// Every state-hook commit, in order (including system calls).
    pub hooks: Vec<EvmState>,
}

impl ExecutorBench {
    /// Executes a fixture with the normal executor, transition tracking, and a recording hook.
    pub fn observe(
        &self,
        pre: &PreDb,
        block: &RecoveredBlock<BaseBlock>,
    ) -> Result<ExecutorObservation> {
        let hooks = Arc::new(Mutex::new(Vec::new()));
        let recorded = Arc::clone(&hooks);
        let state = State::builder().with_database_ref(pre).with_bundle_update().build();
        let mut executor = self.executor(state);
        let result = executor.execute_one_with_state_hook(block, move |state| {
            recorded.lock().unwrap().push(state)
        })?;
        let bundle = executor.into_state().take_bundle();
        let hooks = Arc::try_unwrap(hooks).unwrap().into_inner().unwrap();
        Ok(ExecutorObservation { receipts: result.receipts, bundle, hooks })
    }

    /// Checks receipts, full bundles, and the committed values after every state-hook call.
    pub fn validate(config: BaseEvmConfig, data: &Path, threads: &[usize]) -> Result<()> {
        let mut files = std::fs::read_dir(data)?
            .map(|entry| entry.map(|e| e.path()))
            .collect::<Result<Vec<_>, _>>()?;
        files.retain(|path| path.extension().is_some_and(|extension| extension == "json"));
        files.sort();
        let workers = threads
            .iter()
            .map(|threads| Workers::new(threads + 1).map(Arc::new))
            .collect::<Result<Vec<_>>>()?;
        for file in files {
            let fixture: BlockFixture = serde_json::from_slice(&std::fs::read(&file)?)?;
            let pre = PreDb::new(&fixture.prestate);
            let block = fixture.block()?;
            let baseline = Self { config: config.clone(), payload: None }.observe(&pre, &block)?;
            for (threads, workers) in threads.iter().zip(&workers) {
                let bench = Self {
                    config: config.clone(),
                    payload: Some(ParallelPayload {
                        transactions: fixture.txs.clone().into(),
                        workers: Arc::clone(workers),
                        factory: *config.executor_factory.evm_factory(),
                    }),
                };
                let before = ParallelPayload::completed_runs();
                let actual = bench.observe(&pre, &block)?;
                ensure!(
                    ParallelPayload::completed_runs() > before,
                    "{}: parallel path did not run",
                    file.display()
                );
                ensure!(
                    actual.receipts == baseline.receipts,
                    "{}: executor receipts differ at {threads}",
                    file.display()
                );
                ensure!(
                    actual.bundle == baseline.bundle,
                    "{}: executor bundle differs at {threads}: accounts={} contracts={} reverts={} sizes={:?}/{:?}",
                    file.display(),
                    actual.bundle.state == baseline.bundle.state,
                    actual.bundle.contracts == baseline.bundle.contracts,
                    actual.bundle.reverts == baseline.bundle.reverts,
                    (actual.bundle.state_size, actual.bundle.reverts_size),
                    (baseline.bundle.state_size, baseline.bundle.reverts_size)
                );
                ensure!(
                    actual.hooks.len() == baseline.hooks.len(),
                    "state-hook commit counts differ"
                );
                ensure!(
                    actual.hooks.len() >= block.body().transactions.len(),
                    "recording hook missed transaction commits"
                );
                let mut expected_state = CacheDB::new(&pre);
                let mut actual_state = CacheDB::new(&pre);
                for (index, (expected, actual)) in
                    baseline.hooks.iter().zip(actual.hooks).enumerate()
                {
                    expected_state.commit(expected.clone());
                    actual_state.commit(actual);
                    let equal = actual_state.cache.accounts.len()
                        == expected_state.cache.accounts.len()
                        && expected_state.cache.accounts.iter().all(|(address, expected)| {
                            actual_state.cache.accounts.get(address).is_some_and(|actual| {
                                actual.info == expected.info
                                    && actual.storage == expected.storage
                                    && actual.account_state == expected.account_state
                            })
                        });
                    ensure!(
                        equal,
                        "{}: state-hook values differ at commit {index}, {threads} threads",
                        file.display()
                    );
                }
                println!(
                    "{}: via-executor {threads} threads: receipts, bundle, reverts, {} hook commits equal",
                    file.display(),
                    baseline.hooks.len()
                );
            }
        }
        Ok(())
    }
}
