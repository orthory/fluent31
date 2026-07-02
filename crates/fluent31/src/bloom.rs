//! Per-run bloom filter over user keys, double-hashing scheme.
//!
//! Serialized form: `[k u8][bitmap bytes...]`. The bit count is implied by
//! the bitmap length; `k` is the probe count.

/// 64-bit hash for filter probes: FNV-1a folded through the murmur3 finalizer
/// so short keys still spread across the whole word.
pub fn hash64(key: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for &b in key {
        h ^= u64::from(b);
        h = h.wrapping_mul(0x100000001b3);
    }
    // murmur3 fmix64
    h ^= h >> 33;
    h = h.wrapping_mul(0xff51afd7ed558ccd);
    h ^= h >> 33;
    h = h.wrapping_mul(0xc4ceb9fe1a85ec53);
    h ^= h >> 33;
    h
}

pub fn build(hashes: &[u64], bits_per_key: usize) -> Vec<u8> {
    let n = hashes.len().max(1);
    let nbits = (n * bits_per_key).max(64);
    let nbytes = nbits.div_ceil(8);
    let nbits = nbytes * 8;
    let k = (((bits_per_key as f64) * 0.69) as usize).clamp(1, 30) as u8;

    let mut out = vec![0u8; 1 + nbytes];
    out[0] = k;
    for &h in hashes {
        let delta = (h >> 33) | 1;
        let mut g = h;
        for _ in 0..k {
            let bit = (g % nbits as u64) as usize;
            out[1 + bit / 8] |= 1 << (bit % 8);
            g = g.wrapping_add(delta);
        }
    }
    out
}

pub fn may_contain(filter: &[u8], h: u64) -> bool {
    if filter.len() < 2 {
        return true; // degenerate/corrupt filter: fail open
    }
    let k = filter[0];
    if k > 30 {
        return true;
    }
    let bitmap = &filter[1..];
    let nbits = (bitmap.len() * 8) as u64;
    let delta = (h >> 33) | 1;
    let mut g = h;
    for _ in 0..k {
        let bit = (g % nbits) as usize;
        if bitmap[bit / 8] & (1 << (bit % 8)) == 0 {
            return false;
        }
        g = g.wrapping_add(delta);
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_false_negatives() {
        let keys: Vec<Vec<u8>> = (0..10_000u32).map(|i| i.to_le_bytes().to_vec()).collect();
        let hashes: Vec<u64> = keys.iter().map(|k| hash64(k)).collect();
        let f = build(&hashes, 10);
        for h in &hashes {
            assert!(may_contain(&f, *h));
        }
    }

    #[test]
    fn false_positive_rate_reasonable() {
        let hashes: Vec<u64> = (0..10_000u32)
            .map(|i| hash64(&i.to_le_bytes()))
            .collect();
        let f = build(&hashes, 10);
        let fp = (10_000..30_000u32)
            .filter(|i| may_contain(&f, hash64(&i.to_le_bytes())))
            .count();
        let rate = fp as f64 / 20_000.0;
        assert!(rate < 0.03, "fp rate {rate}");
    }

    #[test]
    fn empty_filter_ok() {
        let f = build(&[], 10);
        // must not panic; arbitrary membership answers are fine
        let _ = may_contain(&f, hash64(b"x"));
    }
}
