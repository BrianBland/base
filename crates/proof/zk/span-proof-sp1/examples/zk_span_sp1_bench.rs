//! Benchmark harness for the experimental SP1 zk-span prover.

use std::{env, error::Error, str::FromStr, time::Instant};

use alloy_consensus::{SignableTransaction, TxEip1559};
use alloy_eips::eip2718::Encodable2718;
use alloy_primitives::{Address, TxKind, address};
use alloy_signer::SignerSync;
use alloy_signer_local::PrivateKeySigner;
use base_alloy_consensus::OpTxEnvelope;
use base_protocol::ZkSpanBatchTransactions;
use base_zk_span_proof::{
    ZkSpanSignatureProofInput, ZkSpanSignatureProofJournal, ZkSpanSignatureProofStatement,
};
use base_zk_span_proof_sp1::{ZkSpanSignatureProofSp1Mode, ZkSpanSignatureProofSp1Prover};

const TEST_CHAIN_ID: u64 = 8_453;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BenchBackend {
    Cpu,
    Cuda,
    Mock,
    Light,
}

impl BenchBackend {
    fn parse(value: &str) -> Result<Self, String> {
        match value {
            "cpu" => Ok(Self::Cpu),
            "cuda" => Ok(Self::Cuda),
            "mock" => Ok(Self::Mock),
            "light" => Ok(Self::Light),
            _ => Err(format!("unsupported --backend value: {value}")),
        }
    }

    const fn as_env(self) -> &'static str {
        match self {
            Self::Cpu => "cpu",
            Self::Cuda => "cuda",
            Self::Mock => "mock",
            Self::Light => "light",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct BenchConfig {
    tx_count: usize,
    block_count: usize,
    mode: ZkSpanSignatureProofSp1Mode,
    backend: BenchBackend,
}

impl Default for BenchConfig {
    fn default() -> Self {
        Self {
            tx_count: 1,
            block_count: 1,
            mode: ZkSpanSignatureProofSp1Mode::Execute,
            backend: BenchBackend::Cpu,
        }
    }
}

impl BenchConfig {
    fn parse() -> Result<Self, String> {
        let mut config = Self::default();
        let mut args = env::args().skip(1);

        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--tx-count" => {
                    let Some(value) = args.next() else {
                        return Err("missing value for --tx-count".to_string());
                    };
                    config.tx_count = value
                        .parse::<usize>()
                        .map_err(|error| format!("invalid --tx-count value {value}: {error}"))?;
                }
                "--block-count" => {
                    let Some(value) = args.next() else {
                        return Err("missing value for --block-count".to_string());
                    };
                    config.block_count = value
                        .parse::<usize>()
                        .map_err(|error| format!("invalid --block-count value {value}: {error}"))?;
                }
                "--mode" => {
                    let Some(value) = args.next() else {
                        return Err("missing value for --mode".to_string());
                    };
                    config.mode = match value.as_str() {
                        "execute" => ZkSpanSignatureProofSp1Mode::Execute,
                        "core" => ZkSpanSignatureProofSp1Mode::Core,
                        "compressed" => ZkSpanSignatureProofSp1Mode::Compressed,
                        "plonk" => ZkSpanSignatureProofSp1Mode::Plonk,
                        "groth16" => ZkSpanSignatureProofSp1Mode::Groth16,
                        _ => return Err(format!("unsupported --mode value: {value}")),
                    };
                }
                "--backend" => {
                    let Some(value) = args.next() else {
                        return Err("missing value for --backend".to_string());
                    };
                    config.backend = BenchBackend::parse(&value)?;
                }
                "--help" | "-h" => {
                    Self::print_usage();
                    std::process::exit(0);
                }
                _ => return Err(format!("unrecognized argument: {arg}")),
            }
        }

        if config.tx_count == 0 {
            return Err("--tx-count must be greater than zero".to_string());
        }

        if config.block_count == 0 {
            return Err("--block-count must be greater than zero".to_string());
        }

        if config.block_count > config.tx_count {
            return Err("--block-count cannot exceed --tx-count".to_string());
        }

        Ok(config)
    }

    fn print_usage() {
        eprintln!(
            "Usage: cargo run -p base-zk-span-proof-sp1 --example zk_span_sp1_bench --release -- [--tx-count N] [--block-count N] [--mode execute|core|compressed|plonk|groth16] [--backend cpu|cuda|mock|light]"
        );
    }
}

fn sample_signer() -> PrivateKeySigner {
    PrivateKeySigner::from_str("0x59c6995e998f97a5a0044976f7dcb9f1f6d0d5f7d28f5b6f8d64f5b5f5b0f001")
        .expect("valid test private key")
}

fn signed_transaction(nonce: u64, to: Address) -> Vec<u8> {
    let signer = sample_signer();
    let transaction = TxEip1559 {
        chain_id: TEST_CHAIN_ID,
        nonce,
        gas_limit: 21_000,
        max_fee_per_gas: 2,
        max_priority_fee_per_gas: 1,
        to: TxKind::Call(to),
        ..Default::default()
    };
    let signature = signer.sign_hash_sync(&transaction.signature_hash()).expect("signable tx");

    let mut encoded = Vec::new();
    OpTxEnvelope::from(transaction.into_signed(signature)).encode_2718(&mut encoded);
    encoded
}

fn sample_signed_transactions(tx_count: usize) -> Vec<Vec<u8>> {
    (0..tx_count)
        .map(|index| {
            signed_transaction(
                (index + 1) as u64,
                address!("3333333333333333333333333333333333333333"),
            )
        })
        .collect()
}

fn block_tx_counts(tx_count: usize, block_count: usize) -> Vec<u64> {
    let base_block_tx_count = tx_count / block_count;
    let remainder = tx_count % block_count;

    (0..block_count)
        .map(|index| {
            let extra_tx = usize::from(index < remainder);
            (base_block_tx_count + extra_tx) as u64
        })
        .collect()
}

fn expected_journal(
    signed_transactions: &[Vec<u8>],
    block_tx_counts: Vec<u64>,
) -> Result<ZkSpanSignatureProofJournal, Box<dyn Error>> {
    let input = ZkSpanSignatureProofInput::from_signed_transactions_with_blocks(
        signed_transactions,
        block_tx_counts,
    )?;
    Ok(ZkSpanSignatureProofStatement::journal_for_input(&input)?)
}

fn verify_batch_journal(
    signed_transactions: &[Vec<u8>],
    block_tx_counts: Vec<u64>,
    actual: &ZkSpanSignatureProofJournal,
) -> Result<(), Box<dyn Error>> {
    use base_protocol::ZkSpanBatch;

    let canonical_transactions =
        ZkSpanSignatureProofStatement::canonical_bytes_from_signed_bytes(signed_transactions)?;
    let tx_roots = ZkSpanSignatureProofStatement::tx_roots_from_signed_bytes(
        signed_transactions,
        &block_tx_counts,
    )?;
    let mut batch_transactions = ZkSpanBatchTransactions::default();
    batch_transactions.add_txs(canonical_transactions, TEST_CHAIN_ID)?;

    let expected = ZkSpanSignatureProofStatement::journal_for_batch(&ZkSpanBatch {
        chain_id: TEST_CHAIN_ID,
        block_tx_counts,
        tx_roots,
        txs: batch_transactions,
        ..Default::default()
    })?;
    if &expected != actual {
        return Err("SP1 journal did not match expected batch journal".into());
    }

    Ok(())
}

fn run(config: BenchConfig) -> Result<(), Box<dyn Error>> {
    // This CLI is single-threaded and sets the backend before any prover initialization.
    unsafe {
        std::env::set_var("SP1_PROVER", config.backend.as_env());
    }

    let signed_transactions = sample_signed_transactions(config.tx_count);
    let block_tx_counts = block_tx_counts(config.tx_count, config.block_count);
    let signed_bytes = signed_transactions.iter().map(Vec::len).sum::<usize>();

    let input_started_at = Instant::now();
    let input = ZkSpanSignatureProofInput::from_signed_transactions_with_blocks(
        &signed_transactions,
        block_tx_counts.clone(),
    )?;
    let input_elapsed = input_started_at.elapsed();
    let input_bytes = input.encode()?;
    let expected = expected_journal(&signed_transactions, block_tx_counts.clone())?;

    println!("benchmark.backend={}", config.backend.as_env());
    println!("benchmark.mode={:?}", config.mode);
    println!("benchmark.tx_count={}", config.tx_count);
    println!("benchmark.block_count={}", config.block_count);
    println!("benchmark.signed_bytes={signed_bytes}");
    println!("benchmark.witness_bytes={}", input_bytes.len());
    println!("benchmark.input_prep_ms={:.3}", input_elapsed.as_secs_f64() * 1_000.0);

    let prover_started_at = Instant::now();
    let prover = ZkSpanSignatureProofSp1Prover::from_env()?;
    let setup_elapsed = prover_started_at.elapsed();
    println!("setup.elapsed_ms={:.3}", setup_elapsed.as_secs_f64() * 1_000.0);

    let execute_started_at = Instant::now();
    let execution = prover.execute_input(&input)?;
    let execute_elapsed = execute_started_at.elapsed();
    println!("execute.elapsed_ms={:.3}", execute_elapsed.as_secs_f64() * 1_000.0);
    println!("execute.total_instruction_count={}", execution.report.total_instruction_count());
    println!("execute.total_syscall_count={}", execution.report.total_syscall_count());
    println!(
        "execute.total_cycles={}",
        execution.report.total_instruction_count() + execution.report.total_syscall_count()
    );
    println!("execute.touched_memory_addresses={}", execution.report.touched_memory_addresses);
    println!("execute.exit_code={}", execution.report.exit_code);
    for (label, cycles) in &execution.report.cycle_tracker {
        println!("execute.cycle_tracker.{label}={cycles}");
    }
    for (label, invocations) in &execution.report.invocation_tracker {
        println!("execute.invocations.{label}={invocations}");
    }
    if let Some(gas) = execution.report.gas() {
        println!("execute.gas={gas}");
    }
    if execution.journal != expected {
        return Err("SP1 execute journal did not match expected input journal".into());
    }
    verify_batch_journal(&signed_transactions, block_tx_counts.clone(), &execution.journal)?;

    if config.mode.is_execute_only() {
        return Ok(());
    }

    let prove_started_at = Instant::now();
    let proof = prover.prove_input(&input, config.mode)?;
    let prove_elapsed = prove_started_at.elapsed();
    println!("prove.elapsed_ms={:.3}", prove_elapsed.as_secs_f64() * 1_000.0);
    println!("prove.mode={}", proof.proof.proof);
    println!("prove.public_values_bytes={}", proof.proof.public_values.as_slice().len());

    let verify_started_at = Instant::now();
    let verified_journal = prover.verify_proof(&proof)?;
    let verify_elapsed = verify_started_at.elapsed();
    println!("verify.elapsed_ms={:.3}", verify_elapsed.as_secs_f64() * 1_000.0);

    if verified_journal != expected {
        return Err("SP1 proof journal did not match expected input journal".into());
    }
    verify_batch_journal(&signed_transactions, block_tx_counts, &verified_journal)?;

    Ok(())
}

fn main() -> Result<(), Box<dyn Error>> {
    let config = BenchConfig::parse()?;
    run(config)
}
