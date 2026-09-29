//! CLI for the parallel EVM experiment.

use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::Instant,
};

use alloy_consensus::{BlockHeader, TxReceipt};
use alloy_primitives::logs_bloom;
use base_common_consensus::BaseBlock;
use base_execution_chainspec::BaseChainSpecBuilder;
use base_execution_evm::BaseEvmConfig;
use base_parallel_evm_bench::{
    BenchExecution, BlockFixture, CriticalPath, PreDb, RpcRecorder, Schedule, Stats, Store,
    TxOutcome, Workers,
};
use clap::{Parser, Subcommand};
use eyre::{Result, ensure};
use reth_evm::{ConfigureEvm, execute::Executor};
use reth_primitives_traits::RecoveredBlock;
use reth_revm::{State, db::BundleState};
use revm::DatabaseRef;

#[derive(Parser)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Offline basic-builder loop with ahead-of-choice speculative execution.
    BuilderSim {
        #[arg(long)]
        data: PathBuf,
        #[arg(long, value_delimiter = ',', default_value = "4,8")]
        threads: Vec<usize>,
        #[arg(long, value_delimiter = ',', default_value = "32,128")]
        k: Vec<usize>,
        #[arg(long, default_value_t = 3)]
        iters: usize,
        #[arg(long, default_value_t = false, action = clap::ArgAction::Set)]
        forwarding: bool,
        #[arg(long, default_value_t = 0)]
        idle_prewarm_ms: u64,
        #[arg(long, default_value_t = 0.0)]
        inject_invalid: f64,
        #[arg(long)]
        tx_da_limit: Option<u64>,
        #[arg(long)]
        block_da_limit: Option<u64>,
    },
    /// Fetch block fixtures (block + touched parent state) from an archive RPC.
    Fetch {
        #[arg(long)]
        rpc: String,
        /// Fast (possibly inconsistent) RPC used only to discover which keys a block touches;
        /// those keys are then batch-fetched from `--rpc` via `eth_getProof`.
        #[arg(long)]
        hint_rpc: Option<String>,
        #[arg(long)]
        from: u64,
        #[arg(long, default_value_t = 1)]
        count: u64,
        #[arg(long, default_value_t = 1)]
        step: u64,
        #[arg(long, default_value_t = 8)]
        jobs: u64,
        #[arg(long)]
        out: PathBuf,
    },
    /// Print per-transaction cumulative gas and status of a fixture's sequential execution.
    Receipts {
        #[arg(long)]
        file: PathBuf,
    },
    /// Benchmark sequential vs parallel execution over fixtures.
    Bench {
        /// Validate receipts, bundles, reverts and state hooks through the production executor.
        #[arg(long)]
        via_executor: bool,
        #[arg(long)]
        data: PathBuf,
        #[arg(long, value_delimiter = ',', default_value = "1,2,4,8,12")]
        threads: Vec<usize>,
        #[arg(long, default_value_t = 5)]
        iters: usize,
        /// Defer speculation until the sender's previous transaction has executed.
        #[arg(long)]
        sender_gate: bool,
        /// Speculate at most this many transactions above the commit frontier (default unbounded).
        #[arg(long)]
        window: Option<usize>,
    },
}

fn config() -> BaseEvmConfig {
    BaseEvmConfig::base(Arc::new(BaseChainSpecBuilder::base_mainnet().build()))
}

fn sequential<DB: DatabaseRef<Error: Send + Sync + 'static> + std::fmt::Debug>(
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

fn fetch(rpc: &str, hint_rpc: Option<&str>, number: u64, out: &Path) -> Result<()> {
    let path = out.join(format!("{number}.json"));
    if path.exists() {
        return Ok(());
    }
    let recorder = RpcRecorder::new(rpc, number - 1)?;
    let (header, txs, senders) = recorder.fetch_block(number)?;
    let mut fixture = BlockFixture { header, txs, senders, prestate: Default::default() };
    let block = fixture.block()?;
    if let Some(hint_rpc) = hint_rpc {
        let hint = RpcRecorder::new(hint_rpc, number - 1)?;
        // Divergent values may fail execution; the keys touched so far are still useful.
        let _ = sequential(&config(), &hint, &block);
        let hint = hint.pre.into_inner().unwrap();
        let started = Instant::now();
        eprintln!(
            "block {number}: hint {} accounts, {} slot owners",
            hint.accounts.len(),
            hint.storage.len()
        );
        recorder.prefetch(&hint)?;
        eprintln!("block {number}: prefetched in {:?}", started.elapsed());
    }
    let (outcomes, _, _) = sequential(&config(), &recorder, &block)?;
    let gas = outcomes.last().map(|o| o.cumulative_gas).unwrap_or_default();
    let bloom = logs_bloom(outcomes.iter().flat_map(|o| o.logs.iter()));
    fixture.prestate = recorder.pre.into_inner().unwrap();
    let ok = gas == block.header().gas_used() && bloom == block.header().logs_bloom();
    let path = if ok { path } else { out.join(format!("{number}.mismatch")) };
    std::fs::write(&path, serde_json::to_vec(&fixture)?)?;
    ensure!(
        ok,
        "block {number}: gas {gas} vs header {}, bloom ok {}",
        block.header().gas_used(),
        bloom == block.header().logs_bloom()
    );
    println!("fetched {number}: {} txs, {gas} gas", fixture.txs.len());
    Ok(())
}

fn bench(data: &Path, threads: &[usize], iters: usize, schedule: Schedule) -> Result<()> {
    let config = config();
    let mut files: Vec<_> =
        std::fs::read_dir(data)?.map(|e| e.map(|e| e.path())).collect::<Result<_, _>>()?;
    files.retain(|p| p.extension().is_some_and(|e| e == "json"));
    files.sort();
    let mut seq_total = 0u64;
    let mut par_total = vec![0u64; threads.len()];
    // Critical-path bounds: [unbounded, 8 cores, unbounded with per-account conflicts].
    let mut path_total = [0u64; 3];
    let mut exec_total = 0u64;
    let mut stats_total = vec![Stats::default(); threads.len()];
    let mut execs_per_tx = vec![Vec::new(); threads.len()];
    let mut scheduled_txs = 0usize;
    let single = Workers::new(1)?;
    let workers = threads.iter().map(|&t| Workers::new(t)).collect::<Result<Vec<_>>>()?;
    println!(
        "block,canonical,txs,gas_m,dependent_txs,chain_txs,seq_us,ideal_speedup,ideal8_speedup,ideal_peraccount_speedup,{}",
        threads
            .iter()
            .map(|t| format!(
                "par{t}_us,par{t}_speedup,par{t}_execs:blocked:invalidated:commit_fails:rebased"
            ))
            .collect::<Vec<_>>()
            .join(",")
    );
    for file in files {
        let fixture: BlockFixture = serde_json::from_slice(&std::fs::read(&file)?)?;
        let block = fixture.block()?;
        let pre = PreDb::new(&fixture.prestate);
        let (expected, bundle, _) = sequential(&config, &pre, &block)?;

        // Correctness and dependency trace at every thread count before timing.
        let store = Store::new(&pre);
        let traced = BenchExecution::execute(&config, &block, &store, &single, schedule, true)?;
        let critical = CriticalPath::new(&traced.traces, None, false);
        let bounds = [
            critical.path_nanos,
            CriticalPath::new(&traced.traces, Some(8), false).path_nanos,
            CriticalPath::new(&traced.traces, None, true).path_nanos,
        ];
        for (&t, workers) in threads.iter().zip(&workers) {
            let store = Store::new(&pre);
            let out = BenchExecution::execute(&config, &block, &store, workers, schedule, false)?;
            ensure!(out.txs == expected, "{}: receipts differ at {t} threads", file.display());
            let diffs = store.diff(&bundle);
            ensure!(
                diffs.is_empty(),
                "{}: state differs at {t} threads: {:?}",
                file.display(),
                &diffs[..diffs.len().min(5)]
            );
        }

        // Arms are interleaved per iteration and the minimum is kept (with its counters), so
        // background load on the host skews every arm alike and mostly drops out.
        let mut seq = u64::MAX;
        let mut par = vec![u64::MAX; threads.len()];
        let mut stats = vec![Stats::default(); threads.len()];
        for _ in 0..iters {
            seq = seq.min(sequential(&config, &pre, &block)?.2);
            for (k, workers) in workers.iter().enumerate() {
                let t = threads[k];
                let store = Store::new(&pre);
                let start = Instant::now();
                let out =
                    BenchExecution::execute(&config, &block, &store, workers, schedule, false)?;
                let nanos = start.elapsed().as_nanos() as u64;
                ensure!(
                    out.txs == expected,
                    "{}: timed receipts differ at {t} threads",
                    file.display()
                );
                let diffs = store.diff(&bundle);
                ensure!(
                    diffs.is_empty(),
                    "{}: timed state differs at {t} threads: {:?}",
                    file.display(),
                    &diffs[..diffs.len().min(5)]
                );
                if nanos < par[k] {
                    par[k] = nanos;
                    stats[k] = out.stats;
                }
            }
        }
        seq_total += seq;
        let scheduled = traced.traces.len();
        scheduled_txs += scheduled;
        for (total, bound) in path_total.iter_mut().zip(bounds) {
            *total += bound;
        }
        exec_total += critical.total_nanos;
        let mut row = format!(
            "{},{},{},{:.1},{},{},{},{:.2},{:.2},{:.2}",
            block.header().number(),
            expected.last().is_some_and(|o| o.cumulative_gas == block.header().gas_used())
                && logs_bloom(expected.iter().flat_map(|o| o.logs.iter()))
                    == block.header().logs_bloom(),
            block.body().transactions.len(),
            block.header().gas_used() as f64 / 1e6,
            critical.dependent_txs,
            critical.path_txs,
            seq / 1000,
            critical.total_nanos as f64 / bounds[0] as f64,
            critical.total_nanos as f64 / bounds[1] as f64,
            critical.total_nanos as f64 / bounds[2] as f64,
        );
        for (k, par) in par.into_iter().enumerate() {
            par_total[k] += par;
            let s = &stats[k];
            row += &format!(
                ",{},{:.2},{}:{}:{}:{}:{}",
                par / 1000,
                seq as f64 / par as f64,
                s.executions,
                s.blocked,
                s.invalidated,
                s.commit_fails,
                s.rebased
            );
            stats_total[k].accumulate(s);
            execs_per_tx[k].push(s.executions as f64 / scheduled.max(1) as f64);
        }
        println!("{row}");
        println!("#   hot: {:?}", critical.hot);
    }
    let ideal = path_total.map(|p| exec_total as f64 / p as f64);
    println!(
        "# total seq {} ms; ideal speedup: unbounded {:.2}, 8 cores {:.2}, unbounded with per-account conflicts {:.2}",
        seq_total / 1_000_000,
        ideal[0],
        ideal[1],
        ideal[2]
    );
    let ms = |nanos: u64| nanos / 1_000_000;
    let blocks = execs_per_tx.first().map_or(0, Vec::len).max(1) as u64;
    for (k, t) in threads.iter().enumerate() {
        let s = &stats_total[k];
        println!("# {t} threads blocking idle waits: {}", s.idle_waits);
        let per_tx = |count: usize| count as f64 / scheduled_txs.max(1) as f64;
        execs_per_tx[k].sort_by(f64::total_cmp);
        println!(
            "# {t} threads: {} ms, speedup {:.2}; execs/tx {:.3} (block p50 {:.2}); wasted/tx: blocked {:.3} invalidated {:.3} commit_fail {:.3}; rebased/tx {:.3}; ms: commit {} idle {}; exec ms: committed {} wasted blocked {} other {}",
            par_total[k] / 1_000_000,
            seq_total as f64 / par_total[k] as f64,
            per_tx(s.executions),
            execs_per_tx[k].get(execs_per_tx[k].len() / 2).copied().unwrap_or_default(),
            per_tx(s.blocked),
            per_tx(s.invalidated),
            per_tx(s.commit_fails),
            per_tx(s.rebased),
            ms(s.commit_nanos),
            ms(s.idle_nanos),
            ms(s.committed_execution_nanos),
            ms(s.blocked_nanos),
            ms(s.execution_nanos - s.committed_execution_nanos - s.blocked_nanos),
        );
        println!(
            "# {t} threads engine: reads/exec {:.2}, validated_reads {}, reader_locs {}; ms: setup {} commit exec {} validate {} reexec {} apply {}; us/block: fixed {} thread time outside executions and idling {}",
            s.reads as f64 / s.executions.max(1) as f64,
            s.validated_reads,
            s.reader_locs,
            ms(s.setup_nanos),
            ms(s.commit_exec_nanos),
            ms(s.validate_nanos),
            ms(s.reexec_nanos),
            ms(s.apply_nanos),
            s.fixed_nanos / blocks / 1000,
            (par_total[k] * *t as u64).saturating_sub(s.execution_nanos + s.idle_nanos)
                / blocks
                / 1000,
        );
    }
    Ok(())
}

fn main() -> Result<()> {
    match Cli::parse().cmd {
        Cmd::BuilderSim {
            data,
            threads,
            k,
            iters,
            forwarding,
            idle_prewarm_ms,
            inject_invalid,
            tx_da_limit,
            block_da_limit,
        } => base_parallel_evm_bench::BuilderSim {
            window: 1,
            threads: 1,
            forwarding,
            idle_prewarm_ms,
            inject_invalid,
            tx_da_limit,
            block_da_limit,
        }
        .bench(&config(), &data, &threads, &k, iters),
        Cmd::Fetch { rpc, hint_rpc, from, count, step, jobs, out } => {
            std::fs::create_dir_all(&out)?;
            let blocks: Vec<u64> = (0..count).map(|i| from + i * step).collect();
            std::thread::scope(|scope| {
                for chunk in blocks.chunks(blocks.len().div_ceil(jobs as usize).max(1)) {
                    let (rpc, hint_rpc, out) = (&rpc, hint_rpc.as_deref(), &out);
                    scope.spawn(move || {
                        for &number in chunk {
                            if let Err(err) = fetch(rpc, hint_rpc, number, out) {
                                eprintln!("block {number}: {err:#}");
                            }
                        }
                    });
                }
            });
            Ok(())
        }
        Cmd::Receipts { file } => {
            let fixture: BlockFixture = serde_json::from_slice(&std::fs::read(&file)?)?;
            let pre = PreDb::new(&fixture.prestate);
            let (txs, _, _) = sequential(&config(), &pre, &fixture.block()?)?;
            for (i, tx) in txs.iter().enumerate() {
                println!("{i} {} {}", tx.cumulative_gas, u8::from(tx.success));
            }
            Ok(())
        }
        Cmd::Bench { data, threads, iters, sender_gate, window, via_executor } => {
            if via_executor {
                return base_parallel_evm_bench::ExecutorBench::validate(config(), &data, &threads);
            }
            bench(&data, &threads, iters, Schedule { sender_gate, window })
        }
    }
}
