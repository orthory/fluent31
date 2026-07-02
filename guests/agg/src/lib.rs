//! Aggregation query — the "SELECT count, sum, min, max WHERE prefix"
//! replacement.
//!
//! Input: the key prefix to aggregate over (raw bytes).
//! Values are interpreted as u64 little-endian when they are at least 8
//! bytes (first 8 bytes), otherwise ignored for sum/min/max.
//! Output: `[count u64][summed u64][sum u64][min u64][max u64]` (LE), where
//! `count` is all matching keys and `summed` is how many contributed to the
//! numeric aggregates.

fn le_u64(v: &[u8]) -> Option<u64> {
    v.get(..8).map(|b| u64::from_le_bytes(b.try_into().unwrap()))
}

fn agg_main() -> i32 {
    let prefix = fluent_guest::input();
    if prefix.is_empty() {
        fluent_guest::log("agg: empty prefix not allowed");
        return 2;
    }
    let Ok(scan) = fluent_guest::scan_prefix(&prefix) else {
        return 3;
    };
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
    fluent_guest::output(&out);
    0
}

fluent_guest::fluent_main!(agg_main);
