//! Fixture preparation for the reusable parallel executor.

use std::time::Instant;

use alloy_consensus::TxReceipt;
use alloy_eips::Typed2718;
use base_common_consensus::BaseBlock;
use base_common_evm::{
    DEPOSIT_TRANSACTION_TYPE, ParallelOutcome, Schedule, Store, TxOutcome, Workers,
};
use base_execution_evm::BaseEvmConfig;
use eyre::Result;
use reth_evm::{
    ConfigureEvm,
    execute::{BlockExecutor, Executor},
};
use reth_primitives_traits::RecoveredBlock;
use reth_revm::{State, db::states::bundle_state::BundleRetention};
use revm::{DatabaseRef, database::BundleState};

/// Executes a fixture's system calls and leading deposits before scheduling its suffix.
#[derive(Debug)]
pub struct BenchExecution;

impl BenchExecution {
    /// Canonical sequential reference, with exactly the legacy bench timing boundary.
    pub fn sequential<DB: DatabaseRef<Error: Send + Sync + 'static> + std::fmt::Debug>(
        config: &BaseEvmConfig,
        db: DB,
        block: &RecoveredBlock<BaseBlock>,
    ) -> Result<(Vec<TxOutcome>, BundleState, u64)> {
        let start = Instant::now();
        let mut executor =
            config.executor(State::builder().with_database_ref(db).with_bundle_update().build());
        let result = executor.execute_one(block)?;
        let nanos = start.elapsed().as_nanos() as u64;
        let bundle = executor.into_state().take_bundle();
        let txs = result
            .receipts
            .iter()
            .map(|r| TxOutcome {
                success: r.status(),
                cumulative_gas: r.cumulative_gas_used(),
                logs: r.logs().to_vec(),
            })
            .collect();
        Ok((txs, bundle, nanos))
    }

    /// Runs the shared engine over the non-deposit suffix of a block.
    pub fn execute(
        config: &BaseEvmConfig,
        block: &RecoveredBlock<BaseBlock>,
        store: &Store<'_>,
        workers: &Workers,
        schedule: Schedule,
        trace: bool,
    ) -> Result<ParallelOutcome> {
        let started = Instant::now();
        let env = config.evm_env(block.header())?;
        let recovered: Vec<_> = block.transactions_recovered().collect();
        let deposits =
            recovered.iter().take_while(|tx| tx.ty() == DEPOSIT_TRANSACTION_TYPE).count();
        let mut txs = Vec::with_capacity(recovered.len());
        {
            let mut state = State::builder().with_database_ref(store).with_bundle_update().build();
            let mut executor = config.executor_for_block(&mut state, block.sealed_block())?;
            executor.apply_pre_execution_changes()?;
            for tx in &recovered[..deposits] {
                executor.execute_transaction(*tx)?;
            }
            txs.extend(executor.receipts().iter().map(|receipt| TxOutcome {
                success: alloy_consensus::TxReceipt::status(receipt),
                cumulative_gas: alloy_consensus::TxReceipt::cumulative_gas_used(receipt),
                logs: alloy_consensus::TxReceipt::logs(receipt).to_vec(),
            }));
            drop(executor);
            state.merge_transitions(BundleRetention::PlainState);
            store.apply_bundle(&state.take_bundle());
        }
        let prefix_nanos = started.elapsed().as_nanos() as u64;
        let mut outcome = ParallelOutcome::execute(
            config.executor_factory.evm_factory(),
            env,
            &block.body().transactions[deposits..],
            store,
            workers,
            schedule,
            trace,
        )?;
        let prefix_gas = txs.last().map_or(0, |tx| tx.cumulative_gas);
        outcome.stats.setup_nanos += prefix_nanos;
        outcome.stats.fixed_nanos += prefix_nanos;
        for tx in &mut outcome.txs {
            tx.cumulative_gas += prefix_gas;
        }
        txs.append(&mut outcome.txs);
        outcome.txs = txs;
        Ok(outcome)
    }
}
