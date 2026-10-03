//! ZisK guest for the ZK signature removal statement (see `zk-sig-removal-core`).
//!
//! Uses ZisK's keccak-f, secp256k1 add/double, and 256-bit modular arithmetic precompiles.
//! Field elements are little-endian `u64` limbs.

#![no_main]
ziskos::entrypoint!(main);

use ziskos::{
    syscalls::{SyscallArith256ModParams, syscall_arith256_mod, syscall_keccak_f},
    zisklib::{
        ecdsa_recover_secp256k1, is_on_curve_secp256k1, lift_x_secp256k1, msm_secp256k1,
        neg_fn_secp256k1,
    },
};
use zk_sig_removal_core::{Backend, Hasher, PendingSig, statement};

/// secp256k1 base field modulus `p`.
const P: [u64; 4] = [0xFFFFFFFEFFFFFC2F, u64::MAX, u64::MAX, u64::MAX];
/// secp256k1 group order `n`.
const N: [u64; 4] = [0xBFD25E8CD0364141, 0xBAAEDCE6AF48A03B, 0xFFFFFFFFFFFFFFFE, u64::MAX];
const G: [u64; 8] = [
    0x59F2815B16F81798,
    0x029BFCDB2DCE28D9,
    0x55A06295CE870B07,
    0x79BE667EF9DCBBAC,
    0x9C47D08FFB10D4B8,
    0xFD17B448A6855419,
    0x5DA4FBFC0E1108A8,
    0x483ADA7726A3C465,
];
const RATE: usize = 136;

fn limbs(be: &[u8]) -> [u64; 4] {
    core::array::from_fn(|i| u64::from_be_bytes(be[24 - 8 * i..32 - 8 * i].try_into().unwrap()))
}

fn be_bytes(l: &[u64]) -> [u8; 32] {
    let mut out = [0u8; 32];
    for i in 0..4 {
        out[24 - 8 * i..32 - 8 * i].copy_from_slice(&l[i].to_be_bytes());
    }
    out
}

/// `(a * b + c) mod n` in one precompile call.
fn mul_add_n(a: &[u64; 4], b: &[u64; 4], c: &[u64; 4]) -> [u64; 4] {
    let mut d = [0u64; 4];
    syscall_arith256_mod(&mut SyscallArith256ModParams { a, b, c, module: &N, d: &mut d });
    d
}

fn lt(a: &[u64; 4], b: &[u64; 4]) -> bool {
    a.iter().rev().cmp(b.iter().rev()).is_lt()
}

struct Keccak {
    state: [u64; 25],
    buf: [u8; RATE],
    len: usize,
}

impl Keccak {
    fn absorb_block(&mut self) {
        for (i, w) in self.buf.chunks_exact(8).enumerate() {
            self.state[i] ^= u64::from_le_bytes(w.try_into().unwrap());
        }
        unsafe { syscall_keccak_f(&mut self.state) };
        self.len = 0;
    }
}

impl Hasher for Keccak {
    fn new() -> Self {
        Self { state: [0; 25], buf: [0; RATE], len: 0 }
    }

    fn update(&mut self, mut data: &[u8]) {
        while !data.is_empty() {
            let n = (RATE - self.len).min(data.len());
            self.buf[self.len..self.len + n].copy_from_slice(&data[..n]);
            self.len += n;
            data = &data[n..];
            if self.len == RATE {
                self.absorb_block();
            }
        }
    }

    fn finalize(mut self) -> [u8; 32] {
        self.buf[self.len..].fill(0);
        self.buf[self.len] ^= 0x01;
        self.buf[RATE - 1] ^= 0x80;
        self.absorb_block();
        let mut out = [0u8; 32];
        for i in 0..4 {
            out[8 * i..8 * i + 8].copy_from_slice(&self.state[i].to_le_bytes());
        }
        out
    }
}

struct Zisk;

impl Backend for Zisk {
    type Hasher = Keccak;
    type Key = [u64; 8];

    fn load_key(xy: &[u8]) -> [u64; 8] {
        let (x, y) = (limbs(&xy[..32]), limbs(&xy[32..]));
        // ZisK's EC helpers require canonical, non-identity, on-curve points.
        assert!(lt(&x, &P) && lt(&y, &P), "non-canonical key");
        let key = [x[0], x[1], x[2], x[3], y[0], y[1], y[2], y[3]];
        assert!(key != [0; 8] && is_on_curve_secp256k1(&key), "key not on curve");
        key
    }

    fn ecrecover(sig: &PendingSig) -> [u8; 20] {
        let q = ecdsa_recover_secp256k1(&limbs(&sig.r), &limbs(&sig.s), &limbs(&sig.z), sig.parity)
            .expect("signature recovery failed");
        let mut xy = [0u8; 64];
        xy[..32].copy_from_slice(&be_bytes(&q[..4]));
        xy[32..].copy_from_slice(&be_bytes(&q[4..]));
        Self::keccak256(&xy)[12..].try_into().unwrap()
    }

    fn batch_verify(sigs: &[PendingSig], keys: &[[u64; 8]], rho: [u8; 32], skip_msm: bool) {
        let rho = limbs(&rho);
        let mut a = rho;
        let mut g_coeff = [0u64; 4];
        let mut key_coeffs = vec![[0u64; 4]; keys.len()];
        let mut scalars = Vec::with_capacity(sigs.len() + keys.len() + 1);
        let mut points = Vec::with_capacity(sigs.len() + keys.len() + 1);
        let zero = [0u64; 4];
        for sig in sigs {
            let r = limbs(&sig.r);
            // r < n < p, so r is a valid x coordinate candidate; the sqrt hint is verified.
            let big_r = lift_x_secp256k1(&r, sig.parity == 1).expect("r is not an x coordinate");
            scalars.push(mul_add_n(&a, &limbs(&sig.s), &zero));
            points.push(big_r);
            let k = sig.key as usize;
            key_coeffs[k] = mul_add_n(&a, &r, &key_coeffs[k]);
            g_coeff = mul_add_n(&a, &limbs(&sig.z), &g_coeff);
            a = mul_add_n(&a, &rho, &zero);
        }
        for (k, c) in keys.iter().zip(&key_coeffs) {
            scalars.push(neg_fn_secp256k1(c));
            points.push(*k);
        }
        scalars.push(neg_fn_secp256k1(&g_coeff));
        points.push(G);
        if skip_msm {
            return;
        }
        assert!(msm_secp256k1(&scalars, &points).is_none(), "batch signature check failed");
    }
}

fn main() {
    let out = statement::<Zisk>(ziskos::io::read_slice().as_ref());
    ziskos::io::commit_slice(&out);
}
