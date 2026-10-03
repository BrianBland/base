//! ZK signature removal DA analysis over real Base blocks.
//!
//! Input: JSONL of `eth_getBlockByNumber(n, true)` responses (one per line).
//! Output: a binary fixture of raw per-block transactions (for the `OpenVM` prover) and a
//! comparison of brotli-10 compressed span-batch transaction columns with signatures versus
//! with sender addresses + per-block transaction roots.
//!
//! Fixture format (little endian): `u32 block_count`, then per block
//! `[u8; 32] transactions_root, u32 tx_count, (u32 len, [u8; len] eip2718_bytes)*`.
//!
//! ```text
//! cargo run -p base-protocol --example zk_sig_removal_analysis --release -- blocks.jsonl fixture.bin
//! ```

use std::{collections::HashMap, io::Write};

use alloy_consensus::proofs::ordered_trie_root_encoded;
use alloy_eips::eip2718::Encodable2718;
use alloy_primitives::{Address, B256, Bytes};
use base_common_consensus::BaseTxEnvelope;
use base_common_rpc_types::Transaction;
use base_protocol::SpanBatchTransactions;
use serde_json::Value;

const CHAIN_ID: u64 = 8453;
/// Usable bytes per blob with the OP blob encoding.
const BLOB_CAPACITY: usize = 130_044;
const TARGET_BLOBS: usize = 6;
/// Blob budget per 12 s L1 block used for the throughput projection.
const BLOBS_PER_L1_BLOCK: usize = 14;
const L1_BLOCK_SECS: f64 = 12.0;
/// Uniswap Universal Router, Uniswap V2/V3 router and Aerodrome router swap selectors.
const SWAP_SELECTORS: [&str; 10] = [
    "0x3593564c",
    "0x24856bc3",
    "0x04e45aaf",
    "0x414bf389",
    "0xb858183f",
    "0x38ed1739",
    "0x7ff36ab5",
    "0x18cbafe5",
    "0xcac88ea9",
    "0x5c11d795",
];

struct Block {
    root: B256,
    /// All transactions (including deposits), EIP-2718 encoded.
    raw: Vec<Vec<u8>>,
    /// Non-deposit transactions and their senders.
    user: Vec<(Bytes, Address)>,
    /// Calldata of each non-deposit transaction, parallel to `user`.
    inputs: Vec<String>,
}

fn brotli10(data: &[u8]) -> usize {
    let mut out = Vec::new();
    {
        let mut w = brotli::CompressorWriter::new(&mut out, 1 << 16, 10, 22);
        w.write_all(data).unwrap();
    }
    out.len()
}

/// Returns (current encoding, zk encoding, signature column bytes, sender column bytes).
fn encodings(blocks: &[Block]) -> (Vec<u8>, Vec<u8>, usize, usize) {
    let mut txs = SpanBatchTransactions::default();
    let mut senders = Vec::new();
    for b in blocks {
        let (raw, from): (Vec<Bytes>, Vec<Address>) = b.user.iter().cloned().unzip();
        txs.add_txs(raw, CHAIN_ID).unwrap();
        senders.extend(from);
    }
    let mut current = Vec::new();
    txs.encode(&mut current).unwrap();

    let mut sigs = Vec::new();
    txs.encode_tx_sigs(&mut sigs).unwrap();
    let mut zk = Vec::new();
    txs.encode_contract_creation_bits(&mut zk).unwrap();
    for s in &senders {
        zk.extend_from_slice(s.as_slice());
    }
    txs.encode_tx_tos(&mut zk).unwrap();
    txs.encode_tx_data(&mut zk).unwrap();
    txs.encode_tx_nonces(&mut zk).unwrap();
    txs.encode_tx_gases(&mut zk).unwrap();
    txs.encode_protected_bits(&mut zk).unwrap();
    txs.encode_eip8130_auth_data(&mut zk).unwrap();
    for b in blocks {
        zk.extend_from_slice(b.root.as_slice());
    }
    (current, zk, sigs.len(), senders.len() * 20)
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let input = std::fs::read_to_string(&args[1]).unwrap();

    let mut blocks = Vec::new();
    let mut type_counts: HashMap<u8, usize> = HashMap::new();
    for line in input.lines().filter(|l| !l.is_empty()) {
        let v: Value = serde_json::from_str(line).unwrap();
        let blk = &v["result"];
        let root: B256 = serde_json::from_value(blk["transactionsRoot"].clone()).unwrap();
        let mut raw = Vec::new();
        let mut user = Vec::new();
        let mut inputs = Vec::new();
        for t in blk["transactions"].as_array().unwrap() {
            let tx: Transaction = serde_json::from_value(t.clone()).unwrap();
            let from = tx.inner.inner.signer();
            let env: &BaseTxEnvelope = tx.inner.inner.inner();
            let bytes = env.encoded_2718();
            *type_counts.entry(bytes[0].min(0x7f)).or_default() += 1;
            if !matches!(env, BaseTxEnvelope::Deposit(_)) {
                user.push((Bytes::from(bytes.clone()), from));
                inputs.push(t["input"].as_str().unwrap().to_owned());
            }
            raw.push(bytes);
        }
        assert_eq!(ordered_trie_root_encoded(&raw), root, "tx root mismatch");
        blocks.push(Block { root, raw, user, inputs });
    }

    let n_tx: usize = blocks.iter().map(|b| b.raw.len()).sum();
    let n_user: usize = blocks.iter().map(|b| b.user.len()).sum();
    let raw_bytes: usize = blocks.iter().flat_map(|b| &b.raw).map(Vec::len).sum();
    let user_bytes: usize = blocks.iter().flat_map(|b| &b.user).map(|u| u.0.len()).sum();
    println!(
        "blocks={} txs={} user_txs={} deposits={} raw_bytes={} user_raw_bytes={}",
        blocks.len(),
        n_tx,
        n_user,
        n_tx - n_user,
        raw_bytes,
        user_bytes
    );
    println!("first-byte counts (126 = deposit, 127 = legacy): {type_counts:?}");
    let mut sizes: Vec<usize> = blocks.iter().flat_map(|b| &b.user).map(|u| u.0.len()).collect();
    sizes.sort_unstable();
    let pct = |p: usize| sizes[(sizes.len() - 1) * p / 100];
    println!(
        "user tx size bytes: mean={} p50={} p90={} p99={} max={}",
        user_bytes / n_user,
        pct(50),
        pct(90),
        pct(99),
        sizes[sizes.len() - 1]
    );

    if let Some(out) = args.get(2) {
        let mut f = Vec::new();
        f.extend_from_slice(&(blocks.len() as u32).to_le_bytes());
        for b in &blocks {
            f.extend_from_slice(b.root.as_slice());
            f.extend_from_slice(&(b.raw.len() as u32).to_le_bytes());
            for t in &b.raw {
                f.extend_from_slice(&(t.len() as u32).to_le_bytes());
                f.extend_from_slice(t);
            }
        }
        std::fs::write(out, f).unwrap();
        println!("wrote fixture {out}");
    }

    println!(
        "\nblocks,user_txs,current_raw,current_brotli,zk_raw,zk_brotli,sig_col,sender_col,saving_pct"
    );
    let target = BLOB_CAPACITY * TARGET_BLOBS;
    let (mut cur_hit, mut zk_hit) = (None, None);
    let mut k = 5;
    while k <= blocks.len() {
        let (cur, zk, sig_col, sender_col) = encodings(&blocks[..k]);
        let (cb, zb) = (brotli10(&cur), brotli10(&zk));
        let txs: usize = blocks[..k].iter().map(|b| b.user.len()).sum();
        println!(
            "{k},{txs},{},{cb},{},{zb},{sig_col},{sender_col},{:.1}",
            cur.len(),
            zk.len(),
            100.0 * (cb - zb) as f64 / cb as f64
        );
        if cur_hit.is_none() && cb >= target {
            cur_hit = Some((k, txs));
        }
        if zk_hit.is_none() && zb >= target {
            zk_hit = Some((k, txs));
        }
        if zk_hit.is_some() {
            break;
        }
        k += 5;
    }
    println!("\n{TARGET_BLOBS} blobs ({target} B): current fills at {cur_hit:?}, zk at {zk_hit:?}");

    // Homogeneous batches of one transaction class, to project DA-bound throughput if the
    // chain were full of that class.
    let capacity = (BLOBS_PER_L1_BLOCK * BLOB_CAPACITY) as f64 / L1_BLOCK_SECS;
    let classes: [(&str, fn(&str) -> bool); 3] = [
        ("eth+erc20 transfer", |i| i == "0x" || i.starts_with("0xa9059cbb")),
        ("dex swap", |i| SWAP_SELECTORS.iter().any(|s| i.starts_with(s))),
        ("all user txs", |_| true),
    ];
    println!(
        "\nclass,txs,current_B_per_tx,zk_B_per_tx,current_tps,zk_tps ({BLOBS_PER_L1_BLOCK} blobs / 12 s)"
    );
    for (name, pred) in classes {
        let user: Vec<_> = blocks
            .iter()
            .flat_map(|b| b.user.iter().zip(&b.inputs))
            .filter(|(_, i)| pred(i))
            .map(|(u, _)| u.clone())
            .take(4_000)
            .collect();
        let n = user.len();
        let (cur, zk, _, _) =
            encodings(&[Block { root: B256::ZERO, raw: vec![], user, inputs: vec![] }]);
        let (cb, zb) = (brotli10(&cur) as f64 / n as f64, brotli10(&zk) as f64 / n as f64);
        println!("{name},{n},{cb:.1},{zb:.1},{:.0},{:.0}", capacity / cb, capacity / zb);
    }
}
