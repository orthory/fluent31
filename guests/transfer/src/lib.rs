//! Balance transfer executor — the "stored procedure" replacement.
//!
//! Input: `[from_len u8][from][to_len u8][to][amount u64 LE]`.
//! Balances are u64 LE values. Uses `get_for_update` so a concurrent write
//! to either account aborts the commit and the host re-runs the module
//! against a fresh snapshot (OCC retry).
//!
//! Exit codes: 0 = transferred (output: both new balances, u64 LE),
//! 1 = insufficient funds, 2 = bad input, 3 = unknown account,
//! 4 = write failed.

use fluent_guest::Fail;

#[fluent_guest::execute]
fn transfer(input: Vec<u8>) -> Result<Vec<u8>, Fail> {
    let mut pos = 0usize;
    let mut take = |n: usize| -> Option<Vec<u8>> {
        let out = input.get(pos..pos + n)?.to_vec();
        pos += n;
        Some(out)
    };
    let bad = || Fail::new(2, "input is not [flen][from][tlen][to][amount u64]");
    let flen = take(1).map(|b| b[0] as usize).ok_or_else(bad)?;
    let from = take(flen).ok_or_else(bad)?;
    let tlen = take(1).map(|b| b[0] as usize).ok_or_else(bad)?;
    let to = take(tlen).ok_or_else(bad)?;
    let amount = take(8)
        .map(|b| u64::from_le_bytes(b.try_into().unwrap()))
        .ok_or_else(bad)?;

    let unknown = || Fail::new(3, "unknown account");
    let Ok(Some(from_bal)) = fluent_guest::get_for_update(&from) else {
        return Err(unknown());
    };
    let Ok(Some(to_bal)) = fluent_guest::get_for_update(&to) else {
        return Err(unknown());
    };
    let (Some(from_bal), Some(to_bal)) = (le_u64(&from_bal), le_u64(&to_bal)) else {
        return Err(unknown());
    };

    if from_bal < amount {
        return Err(Fail::new(1, "insufficient funds"));
    }
    let new_from = from_bal - amount;
    let new_to = to_bal.wrapping_add(amount);
    let write_failed = |_| Fail::new(4, "balance write failed");
    fluent_guest::put(&from, &new_from.to_le_bytes()).map_err(write_failed)?;
    fluent_guest::put(&to, &new_to.to_le_bytes()).map_err(write_failed)?;

    let mut out = Vec::with_capacity(16);
    out.extend_from_slice(&new_from.to_le_bytes());
    out.extend_from_slice(&new_to.to_le_bytes());
    Ok(out)
}

fn le_u64(v: &[u8]) -> Option<u64> {
    v.get(..8).map(|b| u64::from_le_bytes(b.try_into().unwrap()))
}
