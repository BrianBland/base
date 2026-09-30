//! Offline basic-builder choices: canonical order, optional deterministic invalid nonce injection.
//!
//! The real executor enforces cumulative declared gas and Jovian DA-footprint limits. Optional
//! byte limits model `ExecutionInfo::is_tx_over_limits` using Fjord estimated DA bytes. Pool ordering,
//! predicates, wall-clock deadlines and resource-metering policy are deliberately not simulated.
//! Both arms receive identical choices and skip invalid transactions and their nonce descendants.

use std::{
    collections::HashSet,
    path::Path,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use alloy_consensus::{SignableTransaction, Transaction, TxLegacy, transaction::Recovered};
use alloy_eips::{Encodable2718, Typed2718};
use alloy_evm::{
    Evm,
    block::{BlockExecutionError, BlockExecutor, BlockValidationError},
};
use alloy_primitives::{B256, Signature, U256};
use base_common_consensus::{BaseReceipt, BaseTxEnvelope};
use base_common_evm::{DEPOSIT_TRANSACTION_TYPE, SpeculationParent, Speculator, SpeculatorStats};
use base_execution_evm::BaseEvmConfig;
use eyre::{Result, ensure};
use reth_evm::ConfigureEvm;
use revm::{
    database::{BundleState, State, states::bundle_state::BundleRetention},
    state::EvmState,
};

use crate::{BenchExecution, BlockFixture, PreDb, Schedule, Store, Workers};

/// One simulation arm; workers are additional to the builder's owner thread.
#[derive(Debug, Clone)]
pub struct BuilderSim {
    /// Predicted lookahead length.
    pub window: usize,
    /// Persistent workers.
    pub threads: usize,
    /// Forward predicted writes.
    pub forwarding: bool,
    /// Untimed pre-execution before entering the choice loop.
    pub idle_prewarm_ms: u64,
    /// Maximum bounded frontier wait; zero benchmarks inline fallback.
    pub frontier_wait_ms: u64,
    /// Deterministic invalid nonce fraction in [0, 1].
    pub inject_invalid: f64,
    /// Optional compressed transaction byte ceiling.
    pub tx_da_limit: Option<u64>,
    /// Optional compressed block byte ceiling.
    pub block_da_limit: Option<u64>,
}

/// Observable simulation output; equality gates run outside the timed loop.
#[derive(Debug)]
pub struct BuilderObservation {
    /// Included hashes in builder order, including the deposit prefix.
    pub included: Vec<B256>,
    /// Full receipts.
    pub receipts: Vec<BaseReceipt>,
    /// Full state bundle and reverts.
    pub bundle: BundleState,
    /// Time spent in the candidate loop, excluding prewarm and worker shutdown.
    pub nanos: u64,
    /// Settled speculative counters.
    pub stats: SpeculatorStats,
    /// Number of non-deposit choices considered.
    pub considered: usize,
}

impl BuilderSim {
    /// Same seeded fault stream for both arms; intentionally invalid signatures are not recovered.
    pub fn candidates(&self, fixture: &BlockFixture) -> Result<Vec<Recovered<BaseTxEnvelope>>> {
        Ok(fixture
            .block()?
            .transactions_recovered()
            .enumerate()
            .map(|(index, tx)| {
                let mut tx = tx.cloned();
                let random =
                    u64::from_le_bytes(tx.inner().tx_hash().as_slice()[..8].try_into().unwrap())
                        ^ fixture.header.number.wrapping_mul(0x9e3779b97f4a7c15)
                        ^ (index as u64).wrapping_mul(0xbf58476d1ce4e5b9);
                if tx.ty() != DEPOSIT_TRANSACTION_TYPE
                    && (random as f64 / u64::MAX as f64) < self.inject_invalid
                {
                    let bad = TxLegacy {
                        chain_id: tx.chain_id(),
                        nonce: u64::MAX - 1,
                        gas_price: tx.max_fee_per_gas(),
                        gas_limit: tx.gas_limit(),
                        to: tx.kind(),
                        value: tx.value(),
                        input: tx.input().clone(),
                    };
                    tx = Recovered::new_unchecked(
                        BaseTxEnvelope::Legacy(bad.into_signed(Signature::new(
                            U256::from(1),
                            U256::from(2),
                            false,
                        ))),
                        tx.signer(),
                    );
                }
                tx
            })
            .collect())
    }

    /// Runs one arm with the production executor; None is the pure sequential reference.
    pub fn run(
        &self,
        config: &BaseEvmConfig,
        fixture: &BlockFixture,
        workers: Option<&Arc<Speculator>>,
    ) -> Result<BuilderObservation> {
        let block = fixture.block()?;
        let candidates = self.candidates(fixture)?;
        let pre = Arc::new(PreDb::new(&fixture.prestate));
        let mut state =
            State::builder().with_database_ref(Arc::clone(&pre)).with_bundle_update().build();
        let system_changes = Arc::new(Mutex::new(Vec::<EvmState>::new()));
        if workers.is_some() {
            let changes = Arc::clone(&system_changes);
            state.set_state_hook(Some(Box::new(move |state| changes.lock().unwrap().push(state))));
        }
        let mut executor = config.executor_for_block(&mut state, block.sealed_block())?;
        executor.apply_pre_execution_changes()?;
        if let Some(workers) = workers {
            workers.reset(SpeculationParent::new(
                fixture.header.parent_hash,
                config.evm_env(&fixture.header)?,
                *config.executor_factory.evm_factory(),
                Arc::new(move || Box::new(Arc::clone(&pre))),
            ));
            for state in system_changes.lock().unwrap().drain(..) {
                workers.on_commit(B256::ZERO, &state);
            }
            executor.evm.db_mut().set_state_hook(None);
            executor.ctx.speculator = Some(Arc::clone(workers));
        }
        let prefix = candidates.iter().take_while(|tx| tx.ty() == DEPOSIT_TRANSACTION_TYPE).count();
        let mut included = Vec::new();
        for tx in &candidates[..prefix] {
            executor.execute_transaction(tx)?;
            included.push(tx.inner().tx_hash());
        }
        if let Some(workers) = workers {
            workers.submit(
                &candidates[prefix..candidates.len().min(prefix.saturating_add(self.window))],
            );
            std::thread::sleep(Duration::from_millis(self.idle_prewarm_ms));
        }
        let mut rejected = HashSet::new();
        let mut da_bytes = 0u64;
        if let Some(workers) = workers {
            workers.reset_owner_timing();
        }
        let started = Instant::now();
        for (index, tx) in candidates.iter().enumerate().skip(prefix) {
            if let Some(workers) = workers {
                let window =
                    &candidates[index..candidates.len().min(index.saturating_add(self.window))];
                if rejected.is_empty() {
                    workers.submit(window);
                } else {
                    let visible: Vec<_> = window
                        .iter()
                        .filter(|tx| !rejected.contains(&tx.signer()))
                        .cloned()
                        .collect();
                    workers.submit(&visible);
                }
            }
            if rejected.contains(&tx.signer()) {
                continue;
            }
            let size = base_common_flz::tx_estimated_size_fjord(&tx.encoded_2718()) / 1_000_000;
            if self.tx_da_limit.is_some_and(|limit| size > limit)
                || self.block_da_limit.is_some_and(|limit| da_bytes.saturating_add(size) > limit)
            {
                rejected.insert(tx.signer());
                continue;
            }
            match executor.execute_transaction_without_commit(tx) {
                Ok(output) => {
                    executor.commit_transaction(output);
                    da_bytes += size;
                    included.push(tx.inner().tx_hash());
                }
                Err(BlockExecutionError::Validation(BlockValidationError::InvalidTx {
                    error,
                    ..
                })) => {
                    if !error.is_nonce_too_low() {
                        rejected.insert(tx.signer());
                    }
                }
                Err(BlockExecutionError::Validation(error))
                    if matches!(
                        error,
                        BlockValidationError::TransactionGasLimitMoreThanAvailableBlockGas { .. }
                    ) || matches!(&error, BlockValidationError::Other(inner)
                            if matches!(inner.downcast_ref::<base_common_evm::BaseBlockExecutionError>(),
                                Some(base_common_evm::BaseBlockExecutionError::TransactionDaFootprintAboveGasLimit { .. }))) =>
                {
                    rejected.insert(tx.signer());
                }
                Err(error) => return Err(error.into()),
            }
        }
        let nanos = started.elapsed().as_nanos() as u64;
        let (_, result) = executor.finish()?;
        state.merge_transitions(BundleRetention::Reverts);
        let bundle = state.take_bundle();
        let stats = if let Some(workers) = workers {
            workers.cancel();
            ensure!(workers.wait_idle(Duration::from_secs(30)), "speculation failed to settle");
            workers.stats()
        } else {
            SpeculatorStats::default()
        };
        Ok(BuilderObservation {
            included,
            receipts: result.receipts,
            bundle,
            nanos,
            stats,
            considered: candidates.len() - prefix,
        })
    }

    /// Interleaved min-of-N arms. Every iteration checks included order, receipts and full bundle.
    pub fn bench(
        &self,
        config: &BaseEvmConfig,
        data: &Path,
        threads: &[usize],
        windows: &[usize],
        iters: usize,
    ) -> Result<()> {
        ensure!(
            iters > 0 && !threads.is_empty() && threads.iter().all(|t| *t > 0),
            "positive iterations/workers required"
        );
        ensure!(
            !windows.is_empty() && windows.iter().all(|k| *k > 0),
            "positive lookahead required"
        );
        ensure!((0.0..=1.0).contains(&self.inject_invalid), "inject-invalid must be in [0,1]");
        let mut files = std::fs::read_dir(data)?
            .map(|entry| entry.map(|entry| entry.path()))
            .collect::<Result<Vec<_>, _>>()?;
        files.retain(|path| path.extension().is_some_and(|ext| ext == "json"));
        files.sort();
        ensure!(!files.is_empty(), "no fixtures");
        let mut arms = Vec::new();
        for &window in windows {
            for &threads in threads {
                let arm = Self { window, threads, ..self.clone() };
                let mut workers = Speculator::new(threads, self.forwarding)?;
                workers.frontier_wait = Duration::from_millis(self.frontier_wait_ms);
                arms.push((arm, Arc::new(workers)));
            }
        }
        let reference_workers =
            threads.iter().map(|&t| Workers::new(t)).collect::<Result<Vec<_>>>()?;
        let mut reference_totals = vec![0u64; threads.len()];
        let mut reference_seq_total = 0u64;
        let mut sequential = Vec::new();
        let mut timings = vec![Vec::new(); arms.len()];
        let mut totals = vec![(0usize, 0usize, 0usize, 0usize); arms.len()];
        println!(
            "block,k,threads,forwarding,prewarm_ms,invalid,seq_ms,sim_ms,consumed,total,validation_failures,waste,included,not_ready,absent,invalidations,blocked,frontier_retries,timeouts,validation_ns,wait_ms,take_ns,submit_ns,inline_ns,inline_executions,commit_ns,owner_queue_wait_ns,worker_queue_wait_ns,worker_busy_ns,worker_idle_ns,read_validation_ns,rebase_ns,validation_lookups,store_validation_ns,store_validation_reads,repair_ns,submit_clone_ns,invalid_outcomes,invalid_consumed,removal_invalidations,retired_writers,terminal_misses,invalid_repairs,frontier_wait_ns"
        );
        for file in files {
            let fixture: BlockFixture = serde_json::from_slice(&std::fs::read(file)?)?;
            let block = fixture.block()?;
            let pre = PreDb::new(&fixture.prestate);
            let mut reference_seq = u64::MAX;
            let mut reference_best = vec![u64::MAX; threads.len()];
            let mut seq = u64::MAX;
            let mut best: Vec<Option<BuilderObservation>> = (0..arms.len()).map(|_| None).collect();
            for iteration in 0..iters {
                let load = std::process::Command::new("uptime").output()?;
                println!(
                    "# load block={} iteration={} {}",
                    fixture.header.number,
                    iteration,
                    String::from_utf8_lossy(&load.stdout).trim()
                );
                let (expected, bundle, canonical_nanos) =
                    BenchExecution::sequential(config, &pre, &block)?;
                reference_seq = reference_seq.min(canonical_nanos);
                let baseline = self.run(config, &fixture, None)?;
                seq = seq.min(baseline.nanos);
                println!(
                    "# sample block={} iteration={} kind=sequential canonical_ns={} builder_ns={}",
                    fixture.header.number, iteration, canonical_nanos, baseline.nanos
                );
                for offset in 0..arms.len() + threads.len() {
                    let index = (offset + iteration) % (arms.len() + threads.len());
                    if index >= arms.len() {
                        let reference = index - arms.len();
                        let store = Store::new(&pre);
                        let started = Instant::now();
                        let outcome = BenchExecution::execute(
                            config,
                            &block,
                            &store,
                            &reference_workers[reference],
                            Schedule::default(),
                            false,
                        )?;
                        let nanos = started.elapsed().as_nanos() as u64;
                        ensure!(outcome.txs == expected, "ordered reference receipts differ");
                        ensure!(store.diff(&bundle).is_empty(), "ordered reference state differs");
                        reference_best[reference] = reference_best[reference].min(nanos);
                        println!(
                            "# sample block={} iteration={} kind=ordered threads={} ns={} seq_ns={}",
                            fixture.header.number,
                            iteration,
                            threads[reference],
                            nanos,
                            canonical_nanos
                        );
                        continue;
                    }
                    let (arm, workers) = &arms[index];
                    let actual = arm.run(config, &fixture, Some(workers))?;
                    ensure!(
                        actual.included == baseline.included,
                        "{}: included order differs",
                        fixture.header.number
                    );
                    ensure!(
                        actual.receipts == baseline.receipts,
                        "{}: receipts differ",
                        fixture.header.number
                    );
                    ensure!(
                        actual.bundle == baseline.bundle,
                        "{}: bundle differs: accounts={} contracts={} reverts={}",
                        fixture.header.number,
                        actual.bundle.state == baseline.bundle.state,
                        actual.bundle.contracts == baseline.bundle.contracts,
                        actual.bundle.reverts == baseline.bundle.reverts
                    );
                    println!(
                        "# sample block={} iteration={} kind=builder k={} threads={} ns={} seq_ns={}",
                        fixture.header.number,
                        iteration,
                        arm.window,
                        arm.threads,
                        actual.nanos,
                        baseline.nanos
                    );
                    if best[index].as_ref().is_none_or(|best| actual.nanos < best.nanos) {
                        best[index] = Some(actual);
                    }
                }
            }
            reference_seq_total += reference_seq;
            for (index, &nanos) in reference_best.iter().enumerate() {
                reference_totals[index] += nanos;
                println!(
                    "# reference block={} threads={} seq_ms={:.3} ordered_ms={:.3} speedup={:.3}",
                    fixture.header.number,
                    threads[index],
                    reference_seq as f64 / 1e6,
                    nanos as f64 / 1e6,
                    reference_seq as f64 / nanos as f64
                );
            }
            sequential.push(seq);
            for (index, best) in best.into_iter().enumerate() {
                let best = best.unwrap();
                let arm = &arms[index].0;
                let stats = best.stats;
                println!(
                    "# plan block={} threads={} k={} generations={} replans={}",
                    fixture.header.number,
                    arm.threads,
                    arm.window,
                    stats.generations,
                    stats.replans
                );
                let waste = stats.executions.saturating_sub(stats.consumed);
                timings[index].push(best.nanos);
                totals[index].0 += stats.consumed;
                totals[index].1 += best.considered;
                totals[index].2 += stats.validation_failures;
                totals[index].3 += waste;
                println!(
                    "{},{},{},{},{},{},{:.3},{:.3},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
                    fixture.header.number,
                    arm.window,
                    arm.threads,
                    arm.forwarding,
                    arm.idle_prewarm_ms,
                    arm.inject_invalid,
                    seq as f64 / 1e6,
                    best.nanos as f64 / 1e6,
                    stats.consumed,
                    best.considered,
                    stats.validation_failures,
                    waste,
                    best.included.len(),
                    stats.not_ready,
                    stats.absent,
                    stats.invalidations,
                    stats.blocked,
                    stats.frontier_retries,
                    stats.timeouts,
                    stats.validation_nanos,
                    arm.frontier_wait_ms,
                    stats.take_nanos,
                    stats.submit_nanos,
                    stats.inline_nanos,
                    stats.inline_executions,
                    stats.commit_nanos,
                    stats.owner_queue_wait_nanos,
                    stats.worker_queue_wait_nanos,
                    stats.worker_busy_nanos,
                    stats.worker_idle_nanos,
                    stats.read_validation_nanos,
                    stats.rebase_nanos,
                    stats.validation_lookups,
                    stats.store_validation_nanos,
                    stats.store_validation_reads,
                    stats.repair_nanos,
                    stats.submit_clone_nanos,
                    stats.invalid_outcomes,
                    stats.invalid_consumed,
                    stats.removal_invalidations,
                    stats.retired_writers,
                    stats.terminal_misses,
                    stats.invalid_repairs,
                    stats.frontier_wait_nanos
                );
            }
        }
        for (index, &nanos) in reference_totals.iter().enumerate() {
            println!(
                "# reference-total threads={} seq_ms={:.3} ordered_ms={:.3} speedup={:.3}",
                threads[index],
                reference_seq_total as f64 / 1e6,
                nanos as f64 / 1e6,
                reference_seq_total as f64 / nanos as f64
            );
        }
        for (index, (arm, _)) in arms.iter().enumerate() {
            let (hits, considered, failures, waste) = totals[index];
            println!(
                "# k={} threads={} forward={} prewarm={} invalid={} seq_ms={:.3} sim_ms={:.3} seq_p50={:.3} seq_p99={:.3} sim_p50={:.3} sim_p99={:.3} hits={}/{} failures={} waste={}",
                arm.window,
                arm.threads,
                arm.forwarding,
                arm.idle_prewarm_ms,
                arm.inject_invalid,
                sequential.iter().sum::<u64>() as f64 / 1e6,
                timings[index].iter().sum::<u64>() as f64 / 1e6,
                Self::percentile(&sequential, 50),
                Self::percentile(&sequential, 99),
                Self::percentile(&timings[index], 50),
                Self::percentile(&timings[index], 99),
                hits,
                considered,
                failures,
                waste
            );
        }
        Ok(())
    }

    /// Nearest-rank percentile of per-block minimum timings, in milliseconds.
    pub fn percentile(nanos: &[u64], percentile: usize) -> f64 {
        let mut sorted = nanos.to_vec();
        sorted.sort_unstable();
        sorted[(sorted.len() * percentile).div_ceil(100).saturating_sub(1)] as f64 / 1e6
    }
}
