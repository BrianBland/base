//! Benchmark harness for zk span proof execution and proving.

use std::{env, error::Error, str::FromStr, time::Instant};

use alloy_consensus::{SignableTransaction, TxEip1559};
use alloy_eips::eip2718::Encodable2718;
use alloy_primitives::{Address, TxKind, address};
use alloy_signer::SignerSync;
use alloy_signer_local::PrivateKeySigner;
use base_alloy_consensus::OpTxEnvelope;
use base_protocol::ZkSpanBatchTransactions;
use base_zk_span_proof::{
    ZK_SPAN_SIGNATURE_PROOF_ELF, ZkSpanSignatureProof, ZkSpanSignatureProofInput,
    ZkSpanSignatureProofProver, ZkSpanSignatureProofStatement,
};
use risc0_zkvm::{ExecutorEnv, Prover, ProverOpts, default_executor, default_prover};

const TEST_CHAIN_ID: u64 = 8_453;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BenchMode {
    Composite,
    Succinct,
    Groth16,
}

impl BenchMode {
    fn parse(value: &str) -> Result<Self, String> {
        match value {
            "composite" | "fast" => Ok(Self::Composite),
            "succinct" => Ok(Self::Succinct),
            "groth16" => Ok(Self::Groth16),
            _ => Err(format!("unsupported --mode value: {value}")),
        }
    }

    fn prover_opts(self) -> ProverOpts {
        match self {
            Self::Composite => ProverOpts::fast(),
            Self::Succinct => ProverOpts::succinct(),
            Self::Groth16 => ProverOpts::groth16(),
        }
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::Composite => "composite",
            Self::Succinct => "succinct",
            Self::Groth16 => "groth16",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct BenchConfig {
    tx_count: usize,
    block_count: usize,
    mode: BenchMode,
    execute_only: bool,
    skip_execute: bool,
}

impl Default for BenchConfig {
    fn default() -> Self {
        Self {
            tx_count: 1,
            block_count: 1,
            mode: BenchMode::Composite,
            execute_only: false,
            skip_execute: false,
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
                    config.mode = BenchMode::parse(&value)?;
                }
                "--skip-execute" => {
                    config.skip_execute = true;
                }
                "--execute-only" => {
                    config.execute_only = true;
                }
                "--help" | "-h" => {
                    Self::print_usage();
                    std::process::exit(0);
                }
                _ => {
                    return Err(format!("unrecognized argument: {arg}"));
                }
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

        if config.execute_only && config.skip_execute {
            return Err("--execute-only cannot be combined with --skip-execute".to_string());
        }

        Ok(config)
    }

    fn print_usage() {
        eprintln!(
            "Usage: cargo run -p base-zk-span-proof --example zk_span_bench --features prove --release -- [--tx-count N] [--block-count N] [--mode composite|succinct|groth16] [--execute-only] [--skip-execute]"
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

fn sample_batch_from_signed_transactions(
    signed_transactions: &[Vec<u8>],
    block_tx_counts: Vec<u64>,
) -> Result<base_protocol::ZkSpanBatch, Box<dyn Error>> {
    use base_protocol::ZkSpanBatch;

    let canonical_transactions =
        ZkSpanSignatureProofStatement::canonical_bytes_from_signed_bytes(signed_transactions)?;
    let tx_roots = ZkSpanSignatureProofStatement::tx_roots_from_signed_bytes(
        signed_transactions,
        &block_tx_counts,
    )?;
    let mut batch_transactions = ZkSpanBatchTransactions::default();
    batch_transactions.add_txs(canonical_transactions, TEST_CHAIN_ID)?;

    Ok(ZkSpanBatch {
        chain_id: TEST_CHAIN_ID,
        block_tx_counts,
        tx_roots,
        txs: batch_transactions,
        ..Default::default()
    })
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

fn run(config: BenchConfig) -> Result<(), Box<dyn Error>> {
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

    println!("benchmark.mode={}", config.mode.as_str());
    println!("benchmark.tx_count={}", config.tx_count);
    println!("benchmark.block_count={}", config.block_count);
    println!("benchmark.signed_bytes={signed_bytes}");
    println!("benchmark.witness_bytes={}", input_bytes.len());
    println!("benchmark.input_prep_ms={:.3}", input_elapsed.as_secs_f64() * 1_000.0);

    if !config.skip_execute {
        let execute_env = ExecutorEnv::builder().write_slice(&input_bytes).build()?;
        let execute_started_at = Instant::now();
        let session = default_executor().execute(execute_env, ZK_SPAN_SIGNATURE_PROOF_ELF)?;
        let execute_elapsed = execute_started_at.elapsed();

        println!("execute.elapsed_ms={:.3}", execute_elapsed.as_secs_f64() * 1_000.0);
        println!("execute.segments={}", session.segments.len());
        println!("execute.user_cycles={}", session.cycles());
    }

    if config.execute_only {
        return Ok(());
    }

    let prove_env = ExecutorEnv::builder().write_slice(&input_bytes).build()?;
    let prove_started_at = Instant::now();
    let prove_info = default_prover().prove_with_opts(
        prove_env,
        ZK_SPAN_SIGNATURE_PROOF_ELF,
        &config.mode.prover_opts(),
    )?;
    let prove_elapsed = prove_started_at.elapsed();

    let proof =
        ZkSpanSignatureProof::new(ZkSpanSignatureProofProver::encode_receipt(&prove_info.receipt)?);
    let proof_bytes = proof.encode()?;

    println!("prove.elapsed_ms={:.3}", prove_elapsed.as_secs_f64() * 1_000.0);
    println!("prove.segments={}", prove_info.stats.segments);
    println!("prove.total_cycles={}", prove_info.stats.total_cycles);
    println!("prove.user_cycles={}", prove_info.stats.user_cycles);
    println!("prove.paging_cycles={}", prove_info.stats.paging_cycles);
    println!("prove.reserved_cycles={}", prove_info.stats.reserved_cycles);
    println!("prove.receipt_bytes={}", proof.receipt.len());
    println!("prove.envelope_bytes={}", proof_bytes.len());

    let prover = ZkSpanSignatureProofProver::embedded()?;
    let mut batch = sample_batch_from_signed_transactions(&signed_transactions, block_tx_counts)?;
    batch.proof = proof_bytes;

    let verify_started_at = Instant::now();
    prover.verifier().verify_batch(&batch)?;
    let verify_elapsed = verify_started_at.elapsed();

    println!("verify.elapsed_ms={:.3}", verify_elapsed.as_secs_f64() * 1_000.0);

    Ok(())
}

fn main() -> Result<(), Box<dyn Error>> {
    let config = BenchConfig::parse().map_err(|error| {
        BenchConfig::print_usage();
        error
    })?;
    run(config)
}
