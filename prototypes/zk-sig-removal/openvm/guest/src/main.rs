//! OpenVM guest for the ZK signature removal statement (see `zk-sig-removal-core`).
//!
//! Uses OpenVM's keccak, modular-arithmetic, and secp256k1 extensions.

mod msm;

use k256::{
    Secp256k1Coord, Secp256k1Point, Secp256k1Scalar,
    ecdsa::{RecoveryId, Signature, VerifyingKey},
};
use openvm_algebra_guest::IntMod;
use openvm_ecc_guest::{
    CyclicGroup, Group,
    weierstrass::{FromCompressed, WeierstrassPoint},
};
use zk_sig_removal_core::{Backend, Hasher, PendingSig, statement};

openvm::init!();

struct Keccak(openvm_keccak256::Keccak256);

impl Hasher for Keccak {
    fn new() -> Self {
        Self(openvm_keccak256::Keccak256::new())
    }

    fn update(&mut self, data: &[u8]) {
        self.0.update(data);
    }

    fn finalize(self) -> [u8; 32] {
        let mut out = [0u8; 32];
        self.0.finalize(&mut out);
        out
    }
}

struct OpenVm;

impl Backend for OpenVm {
    type Hasher = Keccak;
    type Key = Secp256k1Point;

    fn keccak256(data: &[u8]) -> [u8; 32] {
        openvm_keccak256::keccak256(data)
    }

    fn load_key(xy: &[u8]) -> Secp256k1Point {
        let x = Secp256k1Coord::from_be_bytes(&xy[..32]).expect("x >= p");
        let y = Secp256k1Coord::from_be_bytes(&xy[32..]).expect("y >= p");
        // SAFETY: secp256k1 has cofactor 1, so every curve point is in the prime-order group.
        unsafe { Secp256k1Point::from_xy_nonidentity(x, y) }.expect("key not on curve")
    }

    fn ecrecover(sig: &PendingSig) -> [u8; 20] {
        let mut rs = [0u8; 64];
        rs[..32].copy_from_slice(&sig.r);
        rs[32..].copy_from_slice(&sig.s);
        let signature = Signature::from_slice(&rs).expect("invalid signature scalars");
        let vk = VerifyingKey::recover_from_prehash(
            &sig.z,
            &signature,
            RecoveryId::from_byte(sig.parity).unwrap(),
        )
        .expect("signature recovery failed");
        let pk = vk.to_sec1_bytes(false);
        Self::keccak256(&pk[1..])[12..].try_into().unwrap()
    }

    fn batch_verify(sigs: &[PendingSig], keys: &[Secp256k1Point], rho: [u8; 32], skip_msm: bool) {
        let rho = Secp256k1Scalar::from_be_bytes_unchecked(&rho);
        let mut a = rho.clone();
        let mut g_coeff = Secp256k1Scalar::ZERO;
        let mut key_coeffs = vec![Secp256k1Scalar::ZERO; keys.len()];
        let mut coeffs = Vec::with_capacity(sigs.len() + keys.len() + 1);
        let mut bases = Vec::with_capacity(sigs.len() + keys.len() + 1);
        for sig in sigs {
            let r = Secp256k1Scalar::from_be_bytes_unchecked(&sig.r);
            let s = Secp256k1Scalar::from_be_bytes_unchecked(&sig.s);
            let z = Secp256k1Scalar::from_be_bytes_unchecked(&sig.z);
            // r < n < p, so r is a valid x coordinate candidate; the sqrt hint is verified.
            let x = Secp256k1Coord::from_be_bytes_unchecked(&sig.r);
            let big_r =
                Secp256k1Point::decompress(x, &sig.parity).expect("r is not an x coordinate");
            coeffs.push(&a * &s);
            bases.push(big_r);
            key_coeffs[sig.key as usize] += &a * &r;
            g_coeff += &a * &z;
            a *= &rho;
        }
        for (k, c) in keys.iter().zip(key_coeffs) {
            coeffs.push(-c);
            bases.push(k.clone());
        }
        coeffs.push(-g_coeff);
        bases.push(Secp256k1Point::GENERATOR);
        if skip_msm {
            return;
        }
        let window = (coeffs.len().ilog2() as usize).saturating_sub(2).max(1);
        let sum = msm::msm(&coeffs, &bases, window);
        assert!(sum.is_identity(), "batch signature check failed");
    }
}

fn main() {
    let out = statement::<OpenVm>(&openvm::io::read_vec());
    openvm::io::reveal_bytes32(out);
}
