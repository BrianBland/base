//! zkVM-independent statement for ZK signature removal.
//!
//! Proves that per-block transaction roots commit to validly signed transactions with the
//! declared senders, without revealing the signatures. Each zkVM guest supplies a [`Backend`]
//! (keccak and secp256k1 accelerators); everything else — input parsing, RLP handling, the
//! public statement, and the Fiat-Shamir challenge — lives here so every proof system proves the
//! identical statement.
//!
//! Input (little endian):
//! `u32 flags, u32 block_count, u32 key_count, key_count * ([u8; 32] x_be, [u8; 32] y_be),
//! per block: [u8; 32] tx_root_hint, u32 tx_count, (u32 len, [u8; len] eip2718_tx,
//! [u32 key_index if the tx is ECDSA-signed])*`.
//!
//! The key table and key indices are untrusted prover hints: every key is checked to be a
//! canonical non-identity curve point and the sender is `keccak256(x || y)[12..]` of the indexed
//! key.
//!
//! Public output: `statement = keccak256("base.zksig.v1" || u32be(block_count) || per block:
//! u32be(tx_count) || entry* || tx_root)` where each entry is either
//! `0x00 || signing_hash || sender` (ECDSA-signed legacy/2930/1559/7702 transactions) or
//! `0x01 || keccak256(tx)` (deposits and any other type, which the batch carries in full).
//!
//! Signatures are checked in one batch: for every ECDSA tx `i` with key `Q_k(i)`, the ECDSA
//! relation `s_i * R_i = z_i * G + r_i * Q_k(i)` (with `R_i` decompressed from `r_i` and the
//! y-parity) holds iff `Q_k(i)` is exactly what `ecrecover` returns. All relations are combined
//! with random powers `a_i = rho^i`, `rho` derived by Fiat-Shamir from the statement and all
//! signature values, into a single multi-scalar multiplication that must equal the identity.
//! By Schwartz-Zippel over `Z_n`, a false relation survives with probability at most `n_tx / n`
//! (~`n_tx / 2^256`) per Fiat-Shamir attempt.
//!
//! `flags` exist only for cost attribution in benchmarks: bit 0 skips signature checks, bit 1
//! skips trie hashing (trusting the hint), bit 2 checks signatures one at a time with
//! `ecrecover` instead of the batch MSM, bit 3 skips only the MSM. Production must reject bits
//! 0, 1 and 3.

#![no_std]

extern crate alloc;

use alloc::vec::Vec;

use alloy_trie::root::ordered_trie_root_encoded;

/// secp256k1 group order `n`, big endian.
pub const ORDER: [u8; 32] = [
    0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfe,
    0xba, 0xae, 0xdc, 0xe6, 0xaf, 0x48, 0xa0, 0x3b, 0xbf, 0xd2, 0x5e, 0x8c, 0xd0, 0x36, 0x41, 0x41,
];

/// `(n - 1) / 2` for secp256k1, big endian: the largest `s` allowed by EIP-2.
pub const HALF_ORDER: [u8; 32] = [
    0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
    0x5d, 0x57, 0x6e, 0x73, 0x57, 0xa4, 0x50, 0x1d, 0xdf, 0xe9, 0x2f, 0x46, 0x68, 0x1b, 0x20, 0xa0,
];

/// Streaming keccak-256.
pub trait Hasher {
    fn new() -> Self;
    fn update(&mut self, data: &[u8]);
    fn finalize(self) -> [u8; 32];
}

/// zkVM-specific cryptography.
pub trait Backend {
    type Hasher: Hasher;
    type Key;

    fn keccak256(data: &[u8]) -> [u8; 32] {
        let mut h = Self::Hasher::new();
        h.update(data);
        h.finalize()
    }

    /// Parses a big-endian `x || y` public key. Must panic unless it is a canonical
    /// (`x, y < p`), non-identity curve point.
    fn load_key(xy: &[u8]) -> Self::Key;

    /// Reference path: recovers the sender of one signature with `ecrecover`.
    fn ecrecover(sig: &PendingSig) -> [u8; 20];

    /// Must panic unless `sum_i a_i*(s_i*R_i - z_i*G - r_i*Q_key(i)) == O` for `a_i = rho^i`
    /// (`i` from 1), with `R_i` decompressed from `r_i` and `parity`. Callers guarantee
    /// `r_i in [1, n)`, `s_i in [1, n/2]` and `rho in [1, n)`.
    fn batch_verify(sigs: &[PendingSig], keys: &[Self::Key], rho: [u8; 32], skip_msm: bool);
}

/// Signature values of one ECDSA transaction, pending the batch check (all big endian).
pub struct PendingSig {
    pub z: [u8; 32],
    pub r: [u8; 32],
    pub s: [u8; 32],
    pub parity: u8,
    pub key: u32,
}

pub struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    pub fn bytes(&mut self, n: usize) -> &'a [u8] {
        let out = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        out
    }

    pub fn u32(&mut self) -> u32 {
        u32::from_le_bytes(self.bytes(4).try_into().unwrap())
    }
}

/// Canonical RLP header: returns `(header_len, payload_len, is_list)`. The item is guaranteed to
/// fit in `b`.
pub fn rlp_header(b: &[u8]) -> (usize, usize, bool) {
    let (h, len, list) = rlp_header_unbounded(b);
    // `h + len` cannot overflow: long lengths are capped at 3 bytes below.
    assert!(h + len <= b.len(), "rlp item exceeds buffer");
    (h, len, list)
}

fn rlp_header_unbounded(b: &[u8]) -> (usize, usize, bool) {
    let long = |ll: usize| {
        // Caps lengths below 2^24 so 32-bit `usize` arithmetic cannot wrap (a wrapped length
        // would let a malformed leaf share its signing preimage with a valid transaction).
        assert!(ll <= 3, "rlp length too large");
        assert!(b[1] != 0, "non-canonical rlp length");
        let len = b[1..1 + ll].iter().fold(0usize, |acc, x| (acc << 8) | *x as usize);
        assert!(len > 55, "non-canonical rlp length");
        (1 + ll, len)
    };
    match b[0] {
        0x00..=0x7f => (0, 1, false),
        p @ 0x80..=0xb7 => {
            let len = (p - 0x80) as usize;
            assert!(len != 1 || b[1] >= 0x80, "non-canonical rlp byte");
            (1, len, false)
        }
        p @ 0xb8..=0xbf => {
            let (h, l) = long((p - 0xb7) as usize);
            (h, l, false)
        }
        p @ 0xc0..=0xf7 => (1, (p - 0xc0) as usize, true),
        p => {
            let (h, l) = long((p - 0xf7) as usize);
            (h, l, true)
        }
    }
}

/// Decodes a canonical RLP unsigned integer of at most 32 bytes into big-endian bytes.
fn rlp_uint(item: &[u8]) -> ([u8; 32], usize) {
    let (h, len, list) = rlp_header(item);
    assert!(!list && h + len == item.len() && len <= 32, "bad rlp integer");
    let digits = &item[h..];
    assert!(digits.first() != Some(&0), "rlp integer has leading zero");
    let mut out = [0u8; 32];
    out[32 - len..].copy_from_slice(digits);
    (out, len)
}

fn rlp_u64(item: &[u8]) -> u64 {
    let (be, len) = rlp_uint(item);
    assert!(len <= 8, "integer overflow");
    u64::from_be_bytes(be[24..].try_into().unwrap())
}

/// Minimal RLP list header for a payload of `len` bytes.
fn list_header(len: usize, out: &mut [u8; 9]) -> &[u8] {
    if len <= 55 {
        out[0] = 0xc0 + len as u8;
        return &out[..1];
    }
    let be = (len as u64).to_be_bytes();
    let skip = be.iter().take_while(|b| **b == 0).count();
    out[0] = 0xf7 + (8 - skip) as u8;
    out[1..1 + 8 - skip].copy_from_slice(&be[skip..]);
    &out[..1 + 8 - skip]
}

/// Minimal RLP encoding of a u64 integer.
fn rlp_encode_u64(v: u64, out: &mut [u8; 9]) -> &[u8] {
    if v != 0 && v < 0x80 {
        out[0] = v as u8;
        return &out[..1];
    }
    let be = v.to_be_bytes();
    let skip = be.iter().take_while(|b| **b == 0).count();
    out[0] = 0x80 + (8 - skip) as u8;
    out[1..1 + 8 - skip].copy_from_slice(&be[skip..]);
    &out[..1 + 8 - skip]
}

/// Parses a signed ECDSA transaction into its signing hash and `(r, s, y_parity)`.
///
/// The unsigned encoding is derived from the signed bytes by dropping the trailing
/// `(v | y_parity, r, s)` items, so the signing preimage is hashed without re-encoding fields.
/// Because the verifier recomputes the signing hash from canonically encoded unsigned fields,
/// equality forces the witness bytes to be canonical too.
pub fn parse_ecdsa<H: Hasher>(tx: &[u8]) -> ([u8; 32], [u8; 32], [u8; 32], u8) {
    let (tx_type, body) = if tx[0] < 0x80 { (Some(tx[0]), &tx[1..]) } else { (None, tx) };
    let (h, len, list) = rlp_header(body);
    assert!(list && h + len == body.len(), "bad tx envelope");
    let payload = &body[h..];

    // Walk the top-level items, remembering the start of the last three.
    let (mut pos, mut n, mut starts) = (0usize, 0usize, [0usize; 3]);
    while pos < payload.len() {
        starts[n % 3] = pos;
        let (ih, il, _) = rlp_header(&payload[pos..]);
        pos += ih + il;
        n += 1;
    }
    assert!(pos == payload.len() && n >= 3, "bad tx payload");
    let (sv, sr, ss) = (starts[(n - 3) % 3], starts[(n - 2) % 3], starts[(n - 1) % 3]);
    let v = rlp_u64(&payload[sv..sr]);
    let fields = &payload[..sv];

    let mut hdr = [0u8; 9];
    let mut hasher = H::new();
    let parity = match tx_type {
        Some(t) => {
            assert!(matches!(t, 1 | 2 | 4) && v <= 1, "bad typed tx");
            hasher.update(&[t]);
            hasher.update(list_header(fields.len(), &mut hdr));
            hasher.update(fields);
            v
        }
        None => {
            assert!(n == 9, "bad legacy tx");
            if v == 27 || v == 28 {
                hasher.update(list_header(fields.len(), &mut hdr));
                hasher.update(fields);
                v - 27
            } else {
                assert!(v >= 35, "bad legacy v");
                let mut cid = [0u8; 9];
                let cid = rlp_encode_u64((v - 35) / 2, &mut cid);
                hasher.update(list_header(fields.len() + cid.len() + 2, &mut hdr));
                hasher.update(fields);
                hasher.update(cid);
                hasher.update(&[0x80, 0x80]);
                (v - 35) % 2
            }
        }
    };
    (hasher.finalize(), rlp_uint(&payload[sr..ss]).0, rlp_uint(&payload[ss..]).0, parity as u8)
}

/// Runs the whole statement over `input` and returns the public statement hash.
pub fn statement<B: Backend>(input: &[u8]) -> [u8; 32] {
    let mut r = Reader::new(input);
    let flags = r.u32();
    let (check_sigs, trie, per_sig) = (flags & 1 == 0, flags & 2 == 0, flags & 4 != 0);
    let block_count = r.u32();

    let key_count = r.u32() as usize;
    let mut keys = Vec::with_capacity(key_count);
    let mut addrs = Vec::with_capacity(key_count);
    for _ in 0..key_count {
        let xy = r.bytes(64);
        keys.push(B::load_key(xy));
        addrs.push(<[u8; 20]>::try_from(&B::keccak256(xy)[12..]).unwrap());
    }

    let mut statement = B::Hasher::new();
    statement.update(b"base.zksig.v1");
    statement.update(&block_count.to_be_bytes());
    let mut sig_transcript = B::Hasher::new();
    let mut sigs = Vec::new();
    let mut txs: Vec<&[u8]> = Vec::new();
    for _ in 0..block_count {
        let root_hint = r.bytes(32);
        let tx_count = r.u32();
        statement.update(&tx_count.to_be_bytes());
        txs.clear();
        for _ in 0..tx_count {
            let len = r.u32() as usize;
            let tx = r.bytes(len);
            txs.push(tx);
            if tx[0] >= 0xc0 || matches!(tx[0], 1 | 2 | 4) {
                let (z, sr, ss, parity) = parse_ecdsa::<B::Hasher>(tx);
                // r in [1, n) and s in [1, n/2] (EIP-2); checked here so backends may use
                // unchecked conversions.
                assert!(sr != [0; 32] && sr < ORDER, "invalid r");
                assert!(ss != [0; 32] && ss <= HALF_ORDER, "invalid s");
                let sig = PendingSig { z, r: sr, s: ss, parity, key: r.u32() };
                let sender = if per_sig && check_sigs {
                    B::ecrecover(&sig)
                } else {
                    addrs[sig.key as usize]
                };
                statement.update(&[0]);
                statement.update(&z);
                statement.update(&sender);
                sig_transcript.update(&sig.r);
                sig_transcript.update(&sig.s);
                sig_transcript.update(&[sig.parity]);
                sigs.push(sig);
            } else {
                statement.update(&[1]);
                statement.update(&B::keccak256(tx));
            }
        }
        if trie {
            let root = ordered_trie_root_encoded(&txs);
            assert!(root.as_slice() == root_hint, "tx root mismatch");
        }
        statement.update(root_hint);
    }
    let out = statement.finalize();

    if check_sigs && !per_sig {
        // The statement binds every z_i and sender (hence each used key); the transcript binds
        // r_i, s_i and parity. Unused table keys get a zero coefficient.
        let mut seed_pre = [0u8; 64];
        seed_pre[..32].copy_from_slice(&out);
        seed_pre[32..].copy_from_slice(&sig_transcript.finalize());
        let rho = B::keccak256(&seed_pre);
        // Fails with probability ~2^-128; a uniform challenge in [1, n) keeps backends simple.
        assert!(rho != [0; 32] && rho < ORDER, "degenerate challenge");
        B::batch_verify(&sigs, &keys, rho, flags & 8 != 0);
    }
    out
}
