//! Host driver for the ZK signature removal OpenVM prototype.
//!
//! Loads real Base blocks from a fixture (see `zk_sig_removal_analysis`), builds the guest,
//! checks the guest statement against an independent alloy implementation, and measures
//! execution cost and proving time.

use std::{collections::HashMap, path::PathBuf, time::Instant};

use alloy_consensus::{TxEnvelope, transaction::SignerRecoverable};
use alloy_eips::eip2718::Decodable2718;
use alloy_primitives::keccak256;
use alloy_trie::root::ordered_trie_root_encoded;
use clap::{Parser, ValueEnum};
use openvm_build::GuestOptions;
use openvm_circuit::arch::instructions::exe::VmExe;
use openvm_sdk::{
    DefaultStarkEngine, Sdk, StdIn,
    config::{AggregationSystemParams, AppConfig},
    prover::verify_app_proof,
};
use openvm_sdk_config::{SdkVmConfig, TranspilerConfig};
use openvm_stark_sdk::{
    bench::run_with_metric_collection,
    config::{
        app_params_with_100_bits_security, internal_params_with_100_bits_security,
        leaf_params_with_100_bits_security,
    },
};
use openvm_transpiler::FromElf;
use openvm_verify_stark_host::{verify_vm_stark_proof_decoded, vk::VmStarkVerifyingKey};

#[derive(Clone, Copy, Debug, ValueEnum)]
enum Mode {
    /// Execute only: cycles, trace cells, segments.
    Exec,
    /// Prove all app segments (no aggregation) and verify.
    App,
    /// Prove app segments and aggregate into one STARK proof, then verify.
    Stark,
}

#[derive(Parser, Debug)]
struct Args {
    /// Fixture written by `zk_sig_removal_analysis`.
    #[arg(long, env = "ZKSIG_FIXTURE")]
    fixture: PathBuf,
    #[arg(long, default_value = "../guest")]
    guest: PathBuf,
    #[arg(long, default_value_t = 0)]
    start_block: usize,
    #[arg(long, default_value_t = 1)]
    blocks: usize,
    /// Truncate to at most this many transactions in total (last block's root is recomputed).
    #[arg(long)]
    max_txs: Option<usize>,
    /// Guest cost-attribution flags: bit 0 skips signature checks, bit 1 skips trie hashing,
    /// bit 2 uses per-signature ecrecover instead of the batch MSM.
    #[arg(long, default_value_t = 0)]
    flags: u32,
    #[arg(long, value_enum, default_value = "exec")]
    mode: Mode,
    /// log2 of the max stacked trace height per app segment.
    #[arg(long, default_value_t = 21)]
    log_stacked_height: usize,
    /// Metered memory budget per app segment (OpenVM default 15 GiB, sized for GPUs).
    #[arg(long, default_value_t = 15.0)]
    seg_mem_gib: f64,
    /// Write the guest input framed for ZisK (`u64` length prefix, 8-byte padded) to this path,
    /// print the expected statement, and exit without running OpenVM.
    #[arg(long)]
    zisk_input: Option<PathBuf>,
}

struct Block {
    root: [u8; 32],
    txs: Vec<Vec<u8>>,
}

fn load_fixture(path: &PathBuf) -> eyre::Result<Vec<Block>> {
    let buf = std::fs::read(path)?;
    let mut pos = 0;
    let mut take = |n: usize| {
        let s = &buf[pos..pos + n];
        pos += n;
        s.to_vec()
    };
    let u32le = |b: Vec<u8>| u32::from_le_bytes(b.try_into().unwrap()) as usize;
    let n = u32le(take(4));
    let mut blocks = Vec::with_capacity(n);
    for _ in 0..n {
        let root = take(32).try_into().unwrap();
        let count = u32le(take(4));
        let txs = (0..count).map(|_| {
            let len = u32le(take(4));
            take(len)
        });
        blocks.push(Block { root, txs: txs.collect() });
    }
    Ok(blocks)
}

fn is_ecdsa(tx: &[u8]) -> bool {
    tx[0] >= 0xc0 || matches!(tx[0], 1 | 2 | 4)
}

/// Independent (alloy) computation of the statement the guest reveals.
fn expected_statement(blocks: &[Block]) -> eyre::Result<[u8; 32]> {
    let mut pre = b"base.zksig.v1".to_vec();
    pre.extend((blocks.len() as u32).to_be_bytes());
    for b in blocks {
        pre.extend((b.txs.len() as u32).to_be_bytes());
        for tx in &b.txs {
            if is_ecdsa(tx) {
                let env = TxEnvelope::decode_2718(&mut tx.as_slice())?;
                pre.push(0);
                pre.extend(env.signature_hash());
                pre.extend(env.recover_signer()?);
            } else {
                pre.push(1);
                pre.extend(keccak256(tx));
            }
        }
        pre.extend(b.root);
    }
    Ok(keccak256(pre).0)
}

/// Serializes the guest input, including the untrusted public-key table and per-tx key indices.
fn guest_input(flags: u32, blocks: &[Block]) -> eyre::Result<(Vec<u8>, usize)> {
    let mut keys: HashMap<[u8; 64], u32> = HashMap::new();
    let mut key_list: Vec<[u8; 64]> = Vec::new();
    let mut body = Vec::new();
    let mut tampered = false;
    for b in blocks {
        body.extend(b.root);
        body.extend((b.txs.len() as u32).to_le_bytes());
        for tx in &b.txs {
            body.extend((tx.len() as u32).to_le_bytes());
            body.extend(tx);
            if is_ecdsa(tx) {
                // Undo the TAMPER_RLP forgery so the host can still derive the key hint.
                let canonical =
                    if tx[1] == 0xfc { [&[2, 0xf8][..], &tx[6..]].concat() } else { tx.clone() };
                let env = TxEnvelope::decode_2718(&mut canonical.as_slice())?;
                let sig = match &env {
                    TxEnvelope::Legacy(t) => *t.signature(),
                    TxEnvelope::Eip2930(t) => *t.signature(),
                    TxEnvelope::Eip1559(t) => *t.signature(),
                    TxEnvelope::Eip7702(t) => *t.signature(),
                    TxEnvelope::Eip4844(t) => *t.signature(),
                };
                let vk = sig.recover_from_prehash(&env.signature_hash())?;
                let point = vk.to_encoded_point(false);
                let xy: [u8; 64] = point.as_bytes()[1..].try_into()?;
                let idx = *keys.entry(xy).or_insert_with(|| {
                    key_list.push(xy);
                    key_list.len() as u32 - 1
                });
                // Soundness check: point one tx at a wrong (but valid) key; the guest must panic.
                let idx = if idx != 0 && !tampered && std::env::var("TAMPER_KEY").is_ok() {
                    tampered = true;
                    0
                } else {
                    idx
                };
                body.extend(idx.to_le_bytes());
            }
        }
    }
    let mut v = Vec::new();
    v.extend(flags.to_le_bytes());
    v.extend((blocks.len() as u32).to_le_bytes());
    v.extend((key_list.len() as u32).to_le_bytes());
    for k in &key_list {
        v.extend(k);
    }
    v.extend(body);
    Ok((v, key_list.len()))
}

fn main() -> eyre::Result<()> {
    let args = Args::parse();
    let mut blocks: Vec<Block> =
        load_fixture(&args.fixture)?.into_iter().skip(args.start_block).take(args.blocks).collect();
    if let Some(max) = args.max_txs {
        let mut left = max;
        blocks.retain_mut(|b| {
            if left == 0 {
                return false;
            }
            if b.txs.len() > left {
                b.txs.truncate(left);
                b.root = ordered_trie_root_encoded(&b.txs).0;
            }
            left -= b.txs.len();
            true
        });
    }
    // Soundness check: give one typed tx a non-canonical 5-byte length (2^32 + L) that a 32-bit
    // guest could wrap to L; the guest must panic.
    if std::env::var("TAMPER_RLP").is_ok() {
        let b = &mut blocks[0];
        let tx = b.txs.iter_mut().find(|t| t[0] == 2 && t[1] == 0xf8).expect("no candidate");
        let mut forged = vec![2, 0xfc, 1, 0, 0, 0];
        forged.extend_from_slice(&tx[2..]);
        *tx = forged;
        b.root = ordered_trie_root_encoded(&b.txs).0;
    }
    let txs: usize = blocks.iter().map(|b| b.txs.len()).sum();
    let ecdsa = blocks.iter().flat_map(|b| &b.txs).filter(|t| is_ecdsa(t)).count();
    let bytes: usize = blocks.iter().flat_map(|b| &b.txs).map(Vec::len).sum();

    if let Some(path) = &args.zisk_input {
        let (input, keys) = guest_input(args.flags, &blocks)?;
        let mut framed = (input.len() as u64).to_le_bytes().to_vec();
        framed.extend(&input);
        framed.resize(framed.len().next_multiple_of(8), 0);
        std::fs::write(path, framed)?;
        let expected = expected_statement(&blocks)?;
        println!(
            "blocks={} txs={txs} ecdsa={ecdsa} keys={keys} bytes={bytes} statement={}",
            blocks.len(),
            expected.iter().map(|b| format!("{b:02x}")).collect::<String>()
        );
        return Ok(());
    }

    let vm_config =
        SdkVmConfig::from_toml(&std::fs::read_to_string(args.guest.join("openvm.toml"))?)?;
    let mut vm_config = vm_config;
    vm_config
        .system
        .config
        .set_segmentation_max_memory((args.seg_mem_gib * (1u64 << 30) as f64) as usize);
    let app_params = app_params_with_100_bits_security(args.log_stacked_height);
    let agg_params = AggregationSystemParams {
        leaf: leaf_params_with_100_bits_security(),
        internal: internal_params_with_100_bits_security(),
    };
    let sdk = Sdk::new(AppConfig::new(vm_config.clone(), app_params), agg_params)?;
    let elf = sdk.build(GuestOptions::default(), &args.guest, &None, None)?;
    let exe = VmExe::from_elf(elf, vm_config.transpiler())?;

    let mut stdin = StdIn::default();
    let (input, unique_keys) = guest_input(args.flags, &blocks)?;
    stdin.write_bytes(&input);

    let t = Instant::now();
    let public = sdk.compile_and_execute(exe.clone(), stdin.clone())?;
    let exec_ms = t.elapsed().as_millis();
    if args.flags & 3 == 0 {
        eyre::ensure!(public[..32] == expected_statement(&blocks)?, "statement mismatch");
    }
    let (_, (cells, instret)) = sdk.compile_and_execute_metered_cost(exe.clone(), stdin.clone())?;
    let (_, segments) = sdk.compile_and_execute_metered(exe.clone(), stdin.clone())?;
    {
        let pk = &sdk.app_pk().app_vm_pk.vm_pk.per_air;
        let mut per_air: Vec<(u64, &str)> = pk
            .iter()
            .enumerate()
            .map(|(i, p)| {
                let h: u64 = segments.iter().map(|s| s.trace_heights[i] as u64).sum();
                (h * p.vk.params.width.total_width() as u64, p.air_name.as_str())
            })
            .collect();
        per_air.sort_unstable_by(|a, b| b.cmp(a));
        let total: u64 = per_air.iter().map(|x| x.0).sum();
        println!("trace_cells={total}");
        for (c, name) in
            per_air.iter().take(if std::env::var("BREAKDOWN").is_ok() { 14 } else { 0 })
        {
            println!("  {:5.1}% {:>13} {name}", 100.0 * *c as f64 / total as f64, c);
        }
    }
    println!(
        "blocks={} txs={txs} ecdsa={ecdsa} keys={unique_keys} bytes={bytes} flags={} \
         instret={instret} \
         cells={cells} segments={} exec_ms={exec_ms}",
        blocks.len(),
        args.flags,
        segments.len()
    );

    run_with_metric_collection("OUTPUT_PATH", || -> eyre::Result<()> {
        match args.mode {
            Mode::Exec => {}
            Mode::App => {
                let (_, app_vk) = sdk.app_keygen();
                let mut prover = sdk.app_prover(exe.clone())?;
                let t = Instant::now();
                let proof = prover.prove(stdin.clone())?;
                println!("app_prove_ms={}", t.elapsed().as_millis());
                let _ = verify_app_proof::<DefaultStarkEngine>(&app_vk, &proof)?;
            }
            Mode::Stark => {
                let _ = sdk.agg_pk();
                let t = Instant::now();
                let (proof, baseline) = sdk.prove(exe.clone(), stdin.clone(), &[])?;
                println!("stark_prove_ms={}", t.elapsed().as_millis());
                let vk = VmStarkVerifyingKey { mvk: (*sdk.agg_vk()).clone(), baseline };
                verify_vm_stark_proof_decoded(&vk, &proof)?;
            }
        }
        Ok(())
    })
}
