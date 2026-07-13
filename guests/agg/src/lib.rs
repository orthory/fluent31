//! Aggregation query — the "SELECT count, sum, min, max WHERE prefix"
//! replacement.
//!
//! Input: the key prefix to aggregate over (raw bytes).
//! Values are interpreted as u64 little-endian when they are at least 8
//! bytes (first 8 bytes), otherwise ignored for sum/min/max.
//! Output: `[count u64][summed u64][sum u64][min u64][max u64]` (LE), where
//! `count` is all matching keys and `summed` is how many contributed to the
//! numeric aggregates.

use fluent_guest::Fail;

fn le_u64(v: &[u8]) -> Option<u64> {
    v.get(..8).map(|b| u64::from_le_bytes(b.try_into().unwrap()))
}

#[fluent_guest::query]
fn agg(prefix: Vec<u8>) -> Result<Vec<u8>, Fail> {
    if prefix.is_empty() {
        return Err(Fail::new(2, "empty prefix not allowed"));
    }
    let scan = fluent_guest::scan_prefix(&prefix).map_err(|_| Fail::new(3, "scan failed"))?;
    let (mut count, mut summed, mut sum, mut min, mut max) = (0u64, 0u64, 0u64, u64::MAX, 0u64);
    for (_key, value) in scan {
        count += 1;
        if let Some(n) = le_u64(&value) {
            summed += 1;
            sum = sum.wrapping_add(n);
            min = min.min(n);
            max = max.max(n);
        }
    }
    let mut out = Vec::with_capacity(40);
    for word in [count, summed, sum, min, max] {
        out.extend_from_slice(&word.to_le_bytes());
    }
    Ok(out)
}
