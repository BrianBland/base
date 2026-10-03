# ZK signature removal — OpenVM + ZisK prototype and feasibility study

Prototype for replacing per-transaction ECDSA signatures in Base span batches with sender
addresses, per-block `transactionsRoot`s, and one ZK proof per batch
([design doc](https://coinbase.ghe.com/cb-pages/bbland-docs/blob/master/src/pages/da/zk-sig-removal.mdx)).
Everything here was measured on real Base mainnet blocks `51770854..=51771153` (300 blocks,
81,096 txs).

## TL;DR

- **DA win is larger than the design doc estimates.** On current Base traffic, signatures are
  ~59% of the brotli-10 compressed batch. Removing them (adding senders + roots) shrinks
  compressed batches by **53–58%**. Six blobs hold **~25 blocks / 7.6k txs today** and
  **~57–60 blocks / ~17k txs** in the ZK format (2.2–2.3x capacity).
- **Proving a full 6-blob batch is cheap.** 60 blocks / 17,683 signatures / 6.57 MB of
  transactions is **270M RISC-V instructions, 19.2G trace cells, 66 app segments** in OpenVM 2.0.2.
  The app proof took **39 minutes on an M4 Max MacBook CPU** (verified). Calibrated against
  OpenVM's published GPU benchmarks, that is **~65–100 GPU-seconds** (app proof + aggregation +
  EVM wrap), i.e. **~$0.02–0.08 per 6-blob batch** at $1–3/GPU-hour.
- **Latency fits a 30 s cadence with a small cluster.** Segments prove in parallel; with ~8 GPUs a
  6-blob batch proves in ~20–25 s, with ~64 GPUs ~15 s (estimates, bounded by the ~8 s EVM wrap).
- **ECDSA no longer dominates.** Batch-verifying all signatures with one random-linear-combination
  MSM (plus a leaner Pippenger) cut proving time 5.4x vs per-signature `ecrecover` (953 s → 177 s
  for 5 blocks on the Mac). What remains is mostly keccak (signing
  hashes + transaction trie) and zkVM memory overhead, both linear in batch bytes.
- **Two independent proofs of the same statement are affordable.** The statement logic lives in
  one zkVM-independent crate (`core/`) with an OpenVM 2.x backend and a ZisK backend. For a full
  6-blob batch, OpenVM 2.x needs **157M instructions** and ZisK **94M steps**. Each is roughly one
  Ethereum L1 block of proving: an estimated **~15–50 GPU-s (OpenVM) + ~10–35 GPU-s (ZisK)**,
  i.e. **~$0.01–0.07 per batch for both** on 5090-class GPUs. Requiring both proofs (AND) means a
  forgery needs soundness bugs in two unrelated proof systems. The ZK-sig proofs are separate from
  Base's SP1 withdrawal proofs.
- **Biggest open issue is not proving:** nodes that derive only from L1 lose signatures, so they
  cannot compute transaction hashes. See [open questions](#open-questions-and-risks).

## Real Base data

`crates/consensus/protocol/examples/zk_sig_removal_analysis.rs` rebuilds EIP-2718 bytes from RPC
JSON, checks every block's `transactionsRoot`, writes a binary fixture for the prover, and
compares brotli-10 (window 22, as the batcher uses) compressed span-batch transaction columns:
current (with `tx_sigs`) vs ZK (senders placed before `tx_tos`, plus 32-byte roots per block).

| blocks | user txs | current compressed | ZK compressed | saving |
|---:|---:|---:|---:|---:|
| 5 | 1,577 | 186,919 B | 97,528 B | 47.8% |
| 15 | 4,621 | 504,551 B | 234,638 B | 53.5% |
| 25 | 7,606 | 810,541 B | 356,883 B | 56.0% |
| 55 | 16,280 | 1,700,725 B | 712,300 B | 58.1% |
| 60 | 17,683 | 1,925,372 B | 852,518 B | 55.7% |

Six blobs = 780,264 B. Traffic mix: ~270 txs/block, 95% EIP-1559, 4.3% legacy, 0.2% EIP-7702,
1 deposit/block; mean tx 375 B (p50 172 B). Senders repeat heavily: 17,683 signatures in 60 blocks
come from only **2,705 keys**. Columns only; span-batch prefix and frame overhead are ignored.

## Revised design

Changes relative to the design doc, all implemented in the prototype:

1. **Commit to signing hashes, not decoded fields.** The public statement is

   ```text
   keccak256("base.zksig.v1" || u32be(block_count) ||
             per block: u32be(tx_count) || entry* || transactions_root)
   entry = 0x00 || signing_hash || sender      (legacy / 2930 / 1559 / 7702)
         | 0x01 || keccak256(tx)               (deposits, EIP-8130, anything else)
   ```

   Derivation already reconstructs unsigned transactions from the batch, so it recomputes each
   `signing_hash` natively and checks the statement against the proof's public value. The
   separate "data consistency" property disappears: the guest never decodes fields, it only strips
   the trailing `(v, r, s)` RLP items from the signed bytes and hashes the rest. Because the
   verifier hashes a canonical re-encoding, a keccak match forces the witness bytes to be canonical.
2. **Deposits and non-ECDSA types are in the root.** `transactionsRoot` covers deposits (from L1)
   and EIP-8130 transactions (which keep their own auth data in the batch), so they enter the
   statement by full hash. The doc's EIP-7702 / ERC-4337 concern is a non-issue: 7702
   transactions have an ordinary outer ECDSA signature (authorization tuples stay in the body), and
   4337 bundles are plain EOA transactions.
3. **Batch signature verification.** The prover hints a deduplicated public-key table and a key
   index per transaction. The guest checks every key is on the curve, derives senders as
   `keccak(pubkey)[12..]`, decompresses each `R_i` from `r_i` and the y-parity, and checks

   ```text
   sum_i a_i*s_i*R_i  -  sum_k (sum_{i: key(i)=k} a_i*r_i) * Q_k  -  (sum_i a_i*z_i) * G  ==  O
   ```

   with `a_i = rho^i` and `rho` derived by Fiat-Shamir from the statement and all `(r, s, parity)`.
   Each relation `s*R = z*G + r*Q` holds iff `Q` is exactly what `ecrecover` returns; a false
   relation survives with probability at most `n_tx / n` (~`n_tx / 2^256`, Schwartz-Zippel, cofactor
   1) per Fiat-Shamir attempt; `rho = 0` is rejected. Low-s (EIP-2) and
   `r, s in [1, n)` are enforced. One MSM over `n_sigs + n_keys + 1` points replaces `n`
   independent ~400-EC-op scalar multiplications.
4. **Proof format.** OpenVM app STARK segments aggregate into one STARK, which OpenVM wraps in a
   Halo2/KZG proof of **1,760 bytes** (12 accumulator + 43 proof field elements), 0.2% of a
   6-blob batch. Halo2-KZG uses a universal SRS, avoiding Groth16's per-circuit ceremony.

## Guest cost breakdown

OpenVM 2.0.2 with the keccak, modular, and secp256k1 extensions. Costs are OpenVM's metered
trace cells for 60 blocks (17,683 signatures, 6.57 MB), obtained by toggling benchmark-only guest
flags:

| component | trace cells | share |
|---|---:|---:|
| Parse, signing-hash keccak, statement, key table, input memory | 8.2G | 43% |
| Transaction trie (keccak over every signed tx + branch nodes) | 6.3G | 33% |
| Signature batch check: R decompression + scalars | 1.2G | 6% |
| Signature batch check: MSM | 3.5G | 18% |
| **Total** | **19.2G** | |

Scaling is linear with a small economy of scale from sender dedup and MSM amortization:

| blocks | ECDSA txs | unique keys | bytes | instructions | trace cells | cells/tx | segments (15 GiB) |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 352 | 204 | 120,760 | 7.7M | 0.48G | 1.37M | 2 |
| 5 | 1,577 | 601 | 583,394 | 28.9M | 1.94G | 1.23M | 6 |
| 15 | 4,621 | 1,235 | 1,651,095 | 76.7M | 5.26G | 1.14M | 16 |
| 30 | 9,130 | 1,871 | 3,215,661 | 143.8M | 9.98G | 1.09M | 29 |
| 60 | 17,683 | 2,705 | 6,574,699 | 269.6M | 19.19G | 1.09M | 66 |

## Proving measurements (Apple M4 Max CPU, 14 cores)

CPU proving with 4 GiB segments (the 15 GiB GPU default needs >36 GB RAM); peak RSS ~16 GB.
App proofs are verified after every run.
The 60-block run's higher per-tx time likely reflects ~40 minutes of sustained laptop load and
concurrent exec runs; its trace cells per tx match the 15-block run within 5%.

| guest | blocks | ECDSA txs | app proof time | per tx |
|---|---:|---:|---:|---:|
| v1: per-signature `ecrecover` | 5 | 1,577 | 953 s | 604 ms |
| v2: batch MSM (library Pippenger) | 5 | 1,577 | 478 s | 303 ms |
| v2 | 15 | 4,621 | 1,033 s | 224 ms |
| v3: batch MSM, tuned Pippenger | 5 | 1,577 | 177 s | 112 ms |
| v3 | 15 | 4,621 | 520 s | 112 ms |
| v3 | 60 | 17,683 | **2,365 s** | **134 ms** |

STARK aggregation (v3, 1 block, 6 segments): 82.6 s total, of which app 51.5 s, leaf 18.7 s,
internal layers 12.0 s. Leaf aggregation scales with segment count (~3 s per segment on this CPU);
internal layers are a near-constant tail.

Rejected experiments: OpenVM's freeing heap allocator (`heap-embedded-alloc`) cut memory
Merkle hashing but added instructions: 566 s vs 478 s for 5 blocks. An allocation-free
ordered-trie implementation matched alloy's `HashBuilder` within 0.3% — the trie cost is keccak,
not bookkeeping — so the prototype keeps alloy.

## Double proof: OpenVM 2.x and ZisK

Layout: `core/` holds input parsing, RLP handling, the statement, the Fiat-Shamir challenge, and
all range checks behind a small `Backend` trait (keccak, key loading, `ecrecover`, batch MSM).
`openvm/guest` (OpenVM `v2.x.0-preview.2`, RV64) and `zisk/guest` (ZisK `v1.3.0-alpha`, RV64IMA)
implement it with each zkVM's accelerators. Both reveal the identical 32-byte statement; the
OpenVM host checks it against an independent alloy implementation, and `--zisk-input` writes the
same witness for ZisK and prints the expected statement. `TAMPER_KEY` and `TAMPER_RLP` inputs make
both guests panic.

Derivation rule: a ZK batch is valid iff **both** proofs verify for the statement it recomputes.
Soundness needs only one honest system; liveness needs both provers, and a missing proof falls back
to today's signed format. Proofs run in parallel, so latency is the slower of the two and cost is
their sum. Two wrapped proofs add ~3 KB to a batch (OpenVM Halo2 1,760 B; ZisK PLONK not measured).
The shared `core` crate is the remaining common-mode risk and should get the most audit attention.

Execution cost (60 blocks, 17,683 signatures, 6.57 MB; same fixture as above):

| zkVM | ISA | work | accelerator share | secp256k1 adds |
|---|---|---|---|---:|
| OpenVM 2.0.2 (earlier) | RV32 | 269.6M insns, 19.2G metered cells | keccak 31% of cells | 490,809 |
| OpenVM 2.x preview.2 | RV64 | 157.1M insns, 13.7G metered cells, 36 segments | — | same |
| ZisK 1.3.0-alpha | RV64IMA | 94.3M steps, 15.8G cost units | keccak 32%, secp add 5%, modular 1.4% | 554,916 |

ZisK steps scale linearly: 2.6M (1 block), 9.8M (5), 26.4M (15), 94.3M (60). Its top cost is the
transaction trie (34%, alloy `HashBuilder` + keccak) and one-shot `keccak256` calls (29%).

OpenVM 2.x Mac CPU app proofs (4 GiB segments): 5 blocks 259 s, 15 blocks 551 s, 60 blocks
**2,018 s** (vs 2,365 s on 2.0.2). The 5-block run overlapped a ZisK build, so its number is high.
ZisK needed a 34 GB proving key that grew past 59 GB during a Mac proof attempt and filled the
disk, so I deleted it: **ZisK numbers are emulator-only; no ZisK proof was generated locally.**

GPU estimates for one 6-blob batch (single-GPU seconds, app + aggregation, no final wrap):

| zkVM | method A | method B | estimate |
|---|---|---|---:|
| OpenVM 2.x | Mac time / Mac-to-GPU ratio on OpenVM's rv64 CI benchmarks (keccak 45x, regex 67x), +16–35% aggregation: 35–61 GPU-s | ethproofs: 5.9M insns per GPU-s on 16x5090 (26.5 GPU-s for 157M insns); ~2x better on 4 GPUs (~13) | **~15–50** |
| ZisK | ethproofs 2x5090: 4.66M steps per GPU-s → 20 GPU-s | ethproofs 8x5090: 2.67M steps per GPU-s → 35 GPU-s | **~10–35** |

Ethproofs numbers are per-proof `proving_cycles` and `proving_time` for current Ethereum L1 blocks
(OpenVM 2.1 preview averages 245M insns; ZisK 60–95M steps), so both estimates assume our
keccak-heavy mix costs about the same per cycle as an L1 block. Treat each as ±2x until a GPU run.

## GPU cost and latency forecast

Calibration: OpenVM's published CI benchmarks run on an AWS `g7.4xlarge` (one Blackwell RTX PRO
GPU). The same benchmarks run on this Mac with 4 GiB segments:

| benchmark | Mac app proof | GPU app proof (CI) | ratio |
|---|---:|---:|---:|
| keccak (18.7M insns) | 333.5 s | 9.52 s | 35x |
| regex (4.1M insns) | 32.7 s | 0.70 s | 47x |

Forecast for one 6-blob batch (60 blocks) using the Mac v3 app-proof time `T`:

- App proof: `T / 47 .. T / 35` GPU-seconds.
- Aggregation: +16% (OpenVM CI keccak: leaf 1.54 s per 9.52 s of app) to +35% (Mac ratio).
- EVM wrap: ~8 s on one RTX 5090 (OpenVM 2.0 release notes; not measured here).

With `T = 2,365 s` (60 blocks, v3): app 50–68 GPU-s, plus aggregation 58–91 GPU-s, plus the wrap:
**~65–100 GPU-seconds per 6-blob batch**. This transfers a ratio measured on OpenVM's own
benchmarks; it is not a GPU run of this guest, so treat it as ±50%.

Cost at $1–3 per GPU-hour: **~$0.02–0.08 per 6-blob batch**. At today's traffic a 6-blob ZK batch
covers ~2 minutes (720/day): **~$13–60/day**. At a full 6 blobs every 30 s (2,880/day):
**~$50–240/day**. Each 6-blob ZK batch replaces ~14.8 blobs of today's format, saving ~8.2 blobs;
proving breaks even once blobs cost more than ~$0.002–0.010 each (roughly 0.006–0.025 gwei blob
base fee at $3k ETH). Below that, the win is capacity, not dollars.

Latency: execution is serial but fast (2–4 s for 60 blocks on this CPU, metered). The 66 app
segments prove in parallel (~0.8–1.0 s each on one GPU by the ratio above), then a log-depth
aggregation tree and the Halo2 wrap. Estimates: ~8 GPUs → ~20–25 s; ~64 GPUs → ~15 s. Total
GPU-seconds do not change.

## Open questions and risks

- **Signature and tx-hash availability.** Transaction hashes are `keccak(signed tx)`. Nodes that
  derive only from L1 never see signatures, so they cannot compute tx hashes for RPC, indexing, or
  their stored bodies. Headers and state stay correct. Options: serve signatures over P2P or an
  archive (verifiable against the proven roots), or accept signature-less bodies on L1-only nodes.
  This needs a product decision before anything else.
- **Fault / validity proofs must verify the proof.** Derivation runs inside Base's proof programs,
  so they must verify the posted proofs per batch (OpenVM's Halo2/KZG and ZisK's PLONK, both BN254
  pairing checks). Their in-program cost is not measured here.
- **Soundness surface.** Derivation now trusts the zkVM circuits (OpenVM's keccak and RV32IM are
  formally verified in Lean; its ECC extension and aggregation are audited, not formally
  verified) and the Fiat-Shamir batch check. A soundness bug would let the batcher assert false
  senders with no on-chain detection. An independent review of the guest found one such bug —
  32-bit wrap in RLP long-length decoding let a malformed leaf share a valid signing preimage —
  now fixed (lengths capped at 3 bytes and bounds-checked) with a `TAMPER_RLP` regression. It
  also noted legacy `v` values above `u64` (chain IDs above ~2^63) are rejected; irrelevant for Base.
- **Fallback.** Batches without a proof must fall back to today's format without a hard fork, as
  the design doc says.
- **Remaining speedups (not done):** fewer segments per batch on 15+ GiB GPUs (less per-segment
  memory commitment overhead); a leaner MSM inner loop
  (each bucket add still costs a few thousand cells of RISC-V overhead around one EC-add chip row). Keccak
  over signing preimages and trie leaves (~2 passes over batch bytes) is a floor set by the
  Ethereum formats.

## Reproduce

```bash
# 1. Fetch blocks (any Base RPC with eth_getBlockByNumber full txs) as JSONL, then:
cargo run -p base-protocol --example zk_sig_removal_analysis --release -- blocks.jsonl fixture.bin
export ZKSIG_FIXTURE=$PWD/fixture.bin

# 2. OpenVM (standalone workspace). Guest toolchain: bash ci/install-openvm-toolchain.sh from the
#    openvm repo at v2.x.0-preview.2 (installs `openvm-1.94.1`).
cd prototypes/zk-sig-removal/openvm/host
cargo build --release
./target/release/zk-sig-removal-host --blocks 60                    # execute + cost metrics
./target/release/zk-sig-removal-host --blocks 5 --mode app --seg-mem-gib 4
TAMPER_KEY=1 ./target/release/zk-sig-removal-host --blocks 1        # wrong key: must panic
TAMPER_RLP=1 ./target/release/zk-sig-removal-host --blocks 1        # forged length: must panic
BREAKDOWN=1 ./target/release/zk-sig-removal-host --blocks 5         # per-AIR trace cells
../../scripts/prove_matrix.sh <label> --blocks 15                   # append to results/matrix.txt

# 3. ZisK (install with ziskup --version 1.3.0-alpha --nokey).
./target/release/zk-sig-removal-host --blocks 60 --zisk-input /tmp/in60.bin   # prints statement
cd ../../zisk/guest && cargo-zisk build --release
ziskemu -e target/elf/riscv64ima-zisk-zkvm-elf/release/zk-sig-removal-zisk -i /tmp/in60.bin -X -c
```

Guest flags (benchmark attribution only): `1` skip signature checks, `2` skip trie, `4`
per-signature `ecrecover`, `8` skip the MSM. `results/matrix.txt` holds the raw proving runs.
