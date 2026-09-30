//! Pippenger MSM tuned for the batch signature check (copy of `openvm_ecc_guest::msm` with a
//! smaller window and plain identity-initialized buckets: bucket reduction costs `2^c` adds per
//! window, so `c = log2(n) - 2` beats the library's `c = log2(n)`).

use core::ops::Neg;

use k256::{Secp256k1Point, Secp256k1Scalar};
use openvm_algebra_guest::IntMod;
use openvm_ecc_guest::Group;

pub fn msm(coeffs: &[Secp256k1Scalar], bases: &[Secp256k1Point], c: usize) -> Secp256k1Point {
    let windows = 256 / c + 1;
    let mut acc = Secp256k1Point::IDENTITY;
    let mut buckets = vec![Secp256k1Point::IDENTITY; 1 << (c - 1)];
    for w in (0..windows).rev() {
        for _ in 0..c {
            acc.double_assign();
        }
        buckets.fill(Secp256k1Point::IDENTITY);
        for (coeff, base) in coeffs.iter().zip(bases) {
            let d = booth_index(w, c, coeff.as_le_bytes());
            if d > 0 {
                buckets[d as usize - 1] += base;
            } else if d < 0 {
                buckets[(-d) as usize - 1] -= base;
            }
        }
        let mut running = Secp256k1Point::IDENTITY;
        for b in buckets.iter().rev() {
            running += b;
            acc += &running;
        }
    }
    acc
}

fn booth_index(window_index: usize, window_size: usize, el: &[u8]) -> i32 {
    let skip_bits = (window_index * window_size).saturating_sub(1);
    let skip_bytes = skip_bits / 8;
    let mut v = [0u8; 4];
    for (dst, src) in v.iter_mut().zip(el.iter().skip(skip_bytes)) {
        *dst = *src
    }
    let mut tmp = u32::from_le_bytes(v);
    if window_index == 0 {
        tmp <<= 1;
    }
    tmp >>= skip_bits - (skip_bytes * 8);
    tmp &= (1 << (window_size + 1)) - 1;
    let sign = tmp & (1 << window_size) == 0;
    tmp = (tmp + 1) >> 1;
    if sign { tmp as i32 } else { ((!(tmp - 1) & ((1 << window_size) - 1)) as i32).neg() }
}
