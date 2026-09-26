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
use clap::{Parser, Subcommand};
use base_parallel_evm_bench::{
    BlockFixture, CriticalPath, ParallelOutcome, PreDb, RpcRecorder, Store, TxOutcome,
};
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
    /// Fetch block fixtures (block + touched parent state) from an archive RPC.
    Fetch {
        #[arg(long)]
        rpc: String,
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
        #[arg(long)]
        data: PathBuf,
        #[arg(long, value_delimiter = ',', default_value = "1,2,4,8,12")]
        threads: Vec<usize>,
        #[arg(long, default_value_t = 5)]
        iters: usize,
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

fn fetch(rpc: &str, number: u64, out: &Path) -> Result<()> {
    let path = out.join(format!("{number}.json"));
    if path.exists() {
        return Ok(());
    }
    let recorder = RpcRecorder::new(rpc, number - 1)?;
    let (header, txs, senders) = recorder.fetch_block(number)?;
    let mut fixture = BlockFixture { header, txs, senders, prestate: Default::default() };
    let block = fixture.block()?;
    let (outcomes, _, _) = sequential(&config(), &recorder, &block)?;
    let gas = outcomes.last().map(|o| o.cumulative_gas).unwrap_or_default();
    let bloom = logs_bloom(outcomes.iter().flat_map(|o| o.logs.iter()));
    fixture.prestate = recorder.pre.into_inner().unwrap();
    let ok = gas == block.header().gas_used() && bloom == block.header().logs_bloom();
    let path = if ok { path } else { out.join(format!("{number}.mismatch")) };
    std::fs::write(&path, serde_json::to_vec(&fixture)?)?;
    ensure!(ok, "block {number}: gas {gas} vs header {}, bloom ok {}", block.header().gas_used(), bloom == block.header().logs_bloom());
    println!("fetched {number}: {} txs, {gas} gas", fixture.txs.len());
    Ok(())
}

fn median(mut xs: Vec<u64>) -> u64 {
    xs.sort_unstable();
    xs[xs.len() / 2]
}

fn bench(data: &Path, threads: &[usize], iters: usize) -> Result<()> {
    let config = config();
    let mut files: Vec<_> = std::fs::read_dir(data)?.map(|e| e.map(|e| e.path())).collect::<Result<_, _>>()?;
    files.retain(|p| p.extension().is_some_and(|e| e == "json"));
    files.sort();
    let mut seq_total = 0u64;
    let mut par_total = vec![0u64; threads.len()];
    let (mut path_total, mut exec_total) = (0u64, 0u64);
    println!("block,txs,gas_m,dependent_txs,seq_us,ideal_speedup,{}", threads.iter().map(|t| format!("par{t}_us,par{t}_speedup,par{t}_execs:commit_fails")).collect::<Vec<_>>().join(","));
    for file in files {
        let fixture: BlockFixture = serde_json::from_slice(&std::fs::read(&file)?)?;
        let block = fixture.block()?;
        let pre = PreDb::new(&fixture.prestate);
        let (expected, bundle, _) = sequential(&config, &pre, &block)?;

        // Correctness and dependency trace at every thread count before timing.
        let store = Store::new(&pre);
        let traced = ParallelOutcome::execute(&config, &block, &store, 1, true)?;
        let critical = CriticalPath::new(&traced.traces);
        for &t in threads {
            let store = Store::new(&pre);
            let out = ParallelOutcome::execute(&config, &block, &store, t, false)?;
            ensure!(out.txs == expected, "{}: receipts differ at {t} threads", file.display());
            let diffs = store.diff(&bundle);
            ensure!(diffs.is_empty(), "{}: state differs at {t} threads: {:?}", file.display(), &diffs[..diffs.len().min(5)]);
        }

        let seq = median((0..iters).map(|_| sequential(&config, &pre, &block).map(|r| r.2)).collect::<Result<_>>()?);
        seq_total += seq;
        path_total += critical.path_nanos;
        exec_total += critical.total_nanos;
        let mut row = format!(
            "{},{},{:.1},{},{},{:.2}",
            block.header().number(),
            block.body().transactions.len(),
            block.header().gas_used() as f64 / 1e6,
            critical.dependent_txs,
            seq / 1000,
            critical.total_nanos as f64 / critical.path_nanos as f64,
        );
        for (k, &t) in threads.iter().enumerate() {
            let mut reexec = (0, 0);
            let par = median(
                (0..iters)
                    .map(|_| {
                        let store = Store::new(&pre);
                        let start = Instant::now();
                        let out = ParallelOutcome::execute(&config, &block, &store, t, false)?;
                        reexec = (out.executions, out.reexecuted);
                        Ok(start.elapsed().as_nanos() as u64)
                    })
                    .collect::<Result<_>>()?,
            );
            par_total[k] += par;
            row += &format!(",{},{:.2},{}:{}", par / 1000, seq as f64 / par as f64, reexec.0, reexec.1);
        }
        println!("{row}");
        println!("#   hot: {:?}", critical.hot);
    }
    println!("# total seq {} ms; ideal (critical path, infinite cores) speedup {:.2}", seq_total / 1_000_000, exec_total as f64 / path_total as f64);
    for (k, t) in threads.iter().enumerate() {
        println!("# {t} threads: {} ms, speedup {:.2}", par_total[k] / 1_000_000, seq_total as f64 / par_total[k] as f64);
    }
    Ok(())
}

fn main() -> Result<()> {
    match Cli::parse().cmd {
        Cmd::Fetch { rpc, from, count, step, jobs, out } => {
            std::fs::create_dir_all(&out)?;
            let blocks: Vec<u64> = (0..count).map(|i| from + i * step).collect();
            std::thread::scope(|scope| {
                for chunk in blocks.chunks(blocks.len().div_ceil(jobs as usize).max(1)) {
                    let (rpc, out) = (&rpc, &out);
                    scope.spawn(move || {
                        for &number in chunk {
                            if let Err(err) = fetch(rpc, number, out) {
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
        Cmd::Bench { data, threads, iters } => bench(&data, &threads, iters),
    }
}
