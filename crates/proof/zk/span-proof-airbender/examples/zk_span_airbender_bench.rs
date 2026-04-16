//! Benchmark harness for the experimental Airbender zk-span prover.

use std::{env, error::Error, path::PathBuf, str::FromStr, time::Instant};

use airbender_host::{Prover, ProverLevel, Runner, VerificationRequest, Verifier};
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
use base_zk_span_proof_airbender::{
    ZkSpanSignatureProofAirbender, ZkSpanSignatureProofAirbenderMode,
};
#[cfg(feature = "gpu-prover")]
use execution_utils::unrolled_gpu::{UnrolledProver, UnrolledProverLevel};
#[cfg(feature = "gpu-prover")]
use gpu_prover::execution::prover::ExecutionProverConfiguration;
#[cfg(feature = "gpu-prover")]
use riscv_transpiler::abstractions::non_determinism::QuasiUARTSource;

const TEST_CHAIN_ID: u64 = 8_453;
#[cfg(feature = "gpu-prover")]
const GPU_PROVER_LEVEL: ProverLevel = ProverLevel::RecursionUnified;
#[cfg(feature = "gpu-prover")]
const GPU_ALLOCATOR_BLOCK_LOG_SIZE: u32 = 20;

#[derive(Debug, Clone, PartialEq, Eq)]
struct BenchConfig {
    tx_count: usize,
    block_count: usize,
    mode: ZkSpanSignatureProofAirbenderMode,
    guest_dist_dir: PathBuf,
    worker_threads: Option<usize>,
}

impl Default for BenchConfig {
    fn default() -> Self {
        Self {
            tx_count: 1,
            block_count: 1,
            mode: ZkSpanSignatureProofAirbenderMode::Execute,
            guest_dist_dir: ZkSpanSignatureProofAirbender::default_dist_dir(),
            worker_threads: None,
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
                        "execute" => ZkSpanSignatureProofAirbenderMode::Execute,
                        "dev" => ZkSpanSignatureProofAirbenderMode::Dev,
                        "cpu" => ZkSpanSignatureProofAirbenderMode::Cpu,
                        "gpu" => ZkSpanSignatureProofAirbenderMode::Gpu,
                        _ => return Err(format!("unsupported --mode value: {value}")),
                    };
                }
                "--guest-dist-dir" => {
                    let Some(value) = args.next() else {
                        return Err("missing value for --guest-dist-dir".to_string());
                    };
                    config.guest_dist_dir = PathBuf::from(value);
                }
                "--worker-threads" => {
                    let Some(value) = args.next() else {
                        return Err("missing value for --worker-threads".to_string());
                    };
                    config.worker_threads = Some(value.parse::<usize>().map_err(|error| {
                        format!("invalid --worker-threads value {value}: {error}")
                    })?);
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
            "Usage: cargo run --release --example zk_span_airbender_bench -- [--tx-count N] [--block-count N] [--mode execute|dev|cpu|gpu] [--guest-dist-dir PATH] [--worker-threads N]"
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
        return Err("Airbender journal did not match expected batch journal".into());
    }

    Ok(())
}

#[cfg(feature = "gpu-prover")]
fn env_flag(name: &str) -> Result<bool, Box<dyn Error>> {
    match env::var(name) {
        Ok(value) => match value.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => Ok(true),
            "0" | "false" | "no" | "off" => Ok(false),
            other => Err(format!("invalid boolean {name} value: {other}").into()),
        },
        Err(env::VarError::NotPresent) => Ok(false),
        Err(error) => Err(format!("failed to read {name}: {error}").into()),
    }
}

#[cfg(feature = "gpu-prover")]
fn env_usize(name: &str) -> Result<Option<usize>, Box<dyn Error>> {
    match env::var(name) {
        Ok(value) => Ok(Some(
            value
                .parse::<usize>()
                .map_err(|error| format!("invalid {name} value {value}: {error}"))?,
        )),
        Err(env::VarError::NotPresent) => Ok(None),
        Err(error) => Err(format!("failed to read {name}: {error}").into()),
    }
}

#[cfg(feature = "gpu-prover")]
fn gpu_prover_level() -> UnrolledProverLevel {
    match GPU_PROVER_LEVEL {
        ProverLevel::Base => UnrolledProverLevel::Base,
        ProverLevel::RecursionUnrolled => UnrolledProverLevel::RecursionUnrolled,
        ProverLevel::RecursionUnified => UnrolledProverLevel::RecursionUnified,
    }
}

#[cfg(feature = "gpu-prover")]
fn gpu_base_path(app_bin_path: &std::path::Path) -> Result<String, Box<dyn Error>> {
    let path = app_bin_path.to_str().ok_or("Airbender app.bin path is not valid UTF-8")?;
    Ok(path.strip_suffix(".bin").unwrap_or(path).to_string())
}

#[cfg(feature = "gpu-prover")]
fn gpu_configuration(
    worker_threads: Option<usize>,
) -> Result<ExecutionProverConfiguration, Box<dyn Error>> {
    let mut configuration = ExecutionProverConfiguration::default();
    if let Some(threads) = worker_threads {
        configuration.max_thread_pool_threads = Some(threads);
        configuration.replay_worker_threads_count = threads;
    }

    if env_flag("ZK_AIRBENDER_GPU_LOW_VRAM")? {
        configuration.prover_context_config.max_device_allocation_blocks_count = Some(4_096);
        configuration.prover_context_config.host_allocator_blocks_count = 256;
        configuration.host_allocator_backing_allocation_size = 1 << 24;
        configuration.host_allocators_per_job_count = 16;
        configuration.host_allocators_per_device_count = 8;
        configuration.min_free_host_allocators_per_job = 4;
        configuration.expected_concurrent_jobs = 1;
    }

    if let Some(max_device_mb) = env_usize("ZK_AIRBENDER_GPU_MAX_DEVICE_MB")? {
        configuration.prover_context_config.max_device_allocation_blocks_count =
            Some(max_device_mb >> (GPU_ALLOCATOR_BLOCK_LOG_SIZE - 20));
    }
    if let Some(context_host_mb) = env_usize("ZK_AIRBENDER_GPU_CONTEXT_HOST_MB")? {
        configuration.prover_context_config.host_allocator_blocks_count =
            context_host_mb >> (GPU_ALLOCATOR_BLOCK_LOG_SIZE - 20);
    }
    if let Some(pinned_buffer_mb) = env_usize("ZK_AIRBENDER_GPU_PINNED_BUFFER_MB")? {
        configuration.host_allocator_backing_allocation_size = pinned_buffer_mb << 20;
    }
    if let Some(host_allocators_per_job) = env_usize("ZK_AIRBENDER_GPU_HOST_ALLOCATORS_PER_JOB")? {
        configuration.host_allocators_per_job_count = host_allocators_per_job;
    }
    if let Some(host_allocators_per_device) =
        env_usize("ZK_AIRBENDER_GPU_HOST_ALLOCATORS_PER_DEVICE")?
    {
        configuration.host_allocators_per_device_count = host_allocators_per_device;
    }
    if let Some(expected_jobs) = env_usize("ZK_AIRBENDER_GPU_EXPECTED_JOBS")? {
        configuration.expected_concurrent_jobs = expected_jobs;
    }
    if let Some(min_free_per_job) = env_usize("ZK_AIRBENDER_GPU_MIN_FREE_ALLOCATORS_PER_JOB")? {
        configuration.min_free_host_allocators_per_job = min_free_per_job;
    }

    Ok(configuration)
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
    let expected = expected_journal(&signed_transactions, block_tx_counts.clone())?;
    let expected_commitment = ZkSpanSignatureProofAirbender::commitment_for_journal(&expected);
    let inputs = ZkSpanSignatureProofAirbender::inputs_for_input(&input)?;

    println!("benchmark.mode={:?}", config.mode);
    println!("benchmark.tx_count={}", config.tx_count);
    println!("benchmark.block_count={}", config.block_count);
    println!("benchmark.signed_bytes={signed_bytes}");
    println!("benchmark.input_words={}", inputs.words().len());
    println!("benchmark.guest_dist_dir={}", config.guest_dist_dir.display());
    println!("benchmark.gpu_prover_feature={}", cfg!(feature = "gpu-prover"));
    println!("benchmark.input_prep_ms={:.3}", input_elapsed.as_secs_f64() * 1_000.0);

    let program_started_at = Instant::now();
    let program = ZkSpanSignatureProofAirbender::load_program(&config.guest_dist_dir)?;
    let program_elapsed = program_started_at.elapsed();
    println!("setup.program_load_ms={:.3}", program_elapsed.as_secs_f64() * 1_000.0);

    let runner_started_at = Instant::now();
    let runner = program.transpiler_runner().build()?;
    let runner_elapsed = runner_started_at.elapsed();
    println!("setup.runner_build_ms={:.3}", runner_elapsed.as_secs_f64() * 1_000.0);

    let execute_started_at = Instant::now();
    let execution = runner.run(inputs.words())?;
    let execute_elapsed = execute_started_at.elapsed();
    let execute_commitment =
        ZkSpanSignatureProofAirbender::commitment_for_output_words(execution.receipt.output);
    println!("execute.elapsed_ms={:.3}", execute_elapsed.as_secs_f64() * 1_000.0);
    println!("execute.cycles={}", execution.cycles_executed);
    println!("execute.reached_end={}", execution.reached_end);
    println!("execute.output_words={:?}", execution.receipt.output);
    if execute_commitment != expected_commitment {
        return Err("Airbender execute commitment did not match expected zk-span journal".into());
    }
    verify_batch_journal(&signed_transactions, block_tx_counts.clone(), &expected)?;

    if config.mode.is_execute_only() {
        return Ok(());
    }

    match config.mode {
        ZkSpanSignatureProofAirbenderMode::Execute => {}
        ZkSpanSignatureProofAirbenderMode::Dev => {
            let prover_started_at = Instant::now();
            let prover = program.dev_prover().build()?;
            let prover_setup_elapsed = prover_started_at.elapsed();
            println!("setup.prover_build_ms={:.3}", prover_setup_elapsed.as_secs_f64() * 1_000.0);

            let prove_started_at = Instant::now();
            let proof = prover.prove(inputs.words())?;
            let prove_elapsed = prove_started_at.elapsed();
            println!("prove.elapsed_ms={:.3}", prove_elapsed.as_secs_f64() * 1_000.0);
            println!("prove.cycles={}", proof.cycles);
            println!("prove.output_words={:?}", proof.receipt.output);

            let proof_commitment =
                ZkSpanSignatureProofAirbender::commitment_for_output_words(proof.receipt.output);
            if proof_commitment != expected_commitment {
                return Err(
                    "Airbender dev proof commitment did not match expected zk-span journal".into(),
                );
            }

            let verifier_started_at = Instant::now();
            let verifier = program.dev_verifier().build()?;
            let vk = verifier.generate_vk()?;
            verifier.verify(
                &proof.proof,
                &vk,
                VerificationRequest::dev(inputs.words(), &expected_commitment),
            )?;
            let verify_elapsed = verifier_started_at.elapsed();
            println!("verify.elapsed_ms={:.3}", verify_elapsed.as_secs_f64() * 1_000.0);
        }
        ZkSpanSignatureProofAirbenderMode::Cpu => {
            let prover_started_at = Instant::now();
            let mut builder = program.cpu_prover();
            if let Some(worker_threads) = config.worker_threads {
                builder = builder.with_worker_threads(worker_threads);
            }
            let prover = builder.build()?;
            let prover_setup_elapsed = prover_started_at.elapsed();
            println!("setup.prover_build_ms={:.3}", prover_setup_elapsed.as_secs_f64() * 1_000.0);
            println!("prove.level={:?}", ProverLevel::Base);

            let prove_started_at = Instant::now();
            let proof = prover.prove(inputs.words())?;
            let prove_elapsed = prove_started_at.elapsed();
            println!("prove.elapsed_ms={:.3}", prove_elapsed.as_secs_f64() * 1_000.0);
            println!("prove.cycles={}", proof.cycles);
            println!("prove.output_words={:?}", proof.receipt.output);

            let proof_commitment =
                ZkSpanSignatureProofAirbender::commitment_for_output_words(proof.receipt.output);
            if proof_commitment != expected_commitment {
                return Err(
                    "Airbender CPU proof commitment did not match expected zk-span journal".into(),
                );
            }

            let verifier_started_at = Instant::now();
            let verifier = program.real_verifier(ProverLevel::Base).build()?;
            let vk = verifier.generate_vk()?;
            verifier.verify(&proof.proof, &vk, VerificationRequest::real(&expected_commitment))?;
            let verify_elapsed = verifier_started_at.elapsed();
            println!("verify.elapsed_ms={:.3}", verify_elapsed.as_secs_f64() * 1_000.0);
        }
        ZkSpanSignatureProofAirbenderMode::Gpu => {
            #[cfg(feature = "gpu-prover")]
            {
                let configuration = gpu_configuration(config.worker_threads)?;
                let max_device_allocation_blocks_count =
                    configuration.prover_context_config.max_device_allocation_blocks_count;
                let context_host_allocator_blocks_count =
                    configuration.prover_context_config.host_allocator_blocks_count;
                let host_allocator_backing_allocation_size =
                    configuration.host_allocator_backing_allocation_size;
                let host_allocators_per_job_count = configuration.host_allocators_per_job_count;
                let host_allocators_per_device_count =
                    configuration.host_allocators_per_device_count;
                let base_path = gpu_base_path(program.app_bin())?;
                let prover_started_at = Instant::now();
                let prover = UnrolledProver::new(&base_path, configuration, gpu_prover_level());
                let prover_setup_elapsed = prover_started_at.elapsed();
                println!(
                    "setup.prover_build_ms={:.3}",
                    prover_setup_elapsed.as_secs_f64() * 1_000.0
                );
                println!("prove.level={GPU_PROVER_LEVEL:?}");
                println!(
                    "gpu.max_device_allocation_blocks_count={}",
                    max_device_allocation_blocks_count
                        .map_or_else(|| "auto".to_string(), |value| value.to_string())
                );
                println!(
                    "gpu.context_host_allocator_blocks_count={}",
                    context_host_allocator_blocks_count
                );
                println!(
                    "gpu.host_allocator_backing_allocation_size={}",
                    host_allocator_backing_allocation_size
                );
                println!("gpu.host_allocators_per_job_count={}", host_allocators_per_job_count);
                println!(
                    "gpu.host_allocators_per_device_count={}",
                    host_allocators_per_device_count
                );
                println!(
                    "gpu.low_vram_mode={}",
                    env::var("ZKSYNC_AIRBENDER_LOW_VRAM_MODE")
                        .map(|value| value == "1" || value.eq_ignore_ascii_case("true"))
                        .unwrap_or(false)
                );

                let prove_started_at = Instant::now();
                let (proof, cycles) =
                    prover.prove(0, QuasiUARTSource::new_with_reads(inputs.words().to_vec()));
                let prove_elapsed = prove_started_at.elapsed();
                println!("prove.elapsed_ms={:.3}", prove_elapsed.as_secs_f64() * 1_000.0);
                println!("prove.cycles={cycles}");

                let mut proof_output_words = [0u32; 8];
                for (index, register) in
                    proof.register_final_values.iter().take(proof_output_words.len()).enumerate()
                {
                    proof_output_words[index] = register.value;
                }
                println!("prove.output_words={proof_output_words:?}");
                let proof_commitment =
                    ZkSpanSignatureProofAirbender::commitment_for_output_words(proof_output_words);
                if proof_commitment != expected_commitment {
                    return Err(
                        "Airbender GPU proof commitment did not match expected zk-span journal"
                            .into(),
                    );
                }
            }

            #[cfg(not(feature = "gpu-prover"))]
            {
                return Err(
                    "Airbender GPU mode requires building this example with --features gpu-prover"
                        .into(),
                );
            }
        }
    }

    Ok(())
}

fn main() -> Result<(), Box<dyn Error>> {
    let config = BenchConfig::parse()?;
    run(config)
}
