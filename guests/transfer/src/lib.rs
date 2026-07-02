//! Balance transfer executor — the "stored procedure" replacement.
//!
//! Input: `[from_len u8][from][to_len u8][to][amount u64 LE]`.
//! Balances are u64 LE values. Uses `get_for_update` so a concurrent write
//! to either account aborts the commit and the host re-runs the module
//! against a fresh snapshot (OCC retry).
//!
//! Exit codes: 0 = transferred, 1 = insufficient funds, 2 = bad input,
//! 3 = unknown account.

fn transfer_main() -> i32 {
    let input = fluent_guest::input();
    let mut pos = 0usize;
    let mut take = |n: usize| -> Option<Vec<u8>> {
        let out = input.get(pos..pos + n)?.to_vec();
        pos += n;
        Some(out)
    };
    let Some(flen) = take(1).map(|b| b[0] as usize) else {
        return 2;
    };
    let Some(from) = take(flen) else { return 2 };
    let Some(tlen) = take(1).map(|b| b[0] as usize) else {
        return 2;
    };
    let Some(to) = take(tlen) else { return 2 };
    let Some(amount) = take(8).map(|b| u64::from_le_bytes(b.try_into().unwrap())) else {
        return 2;
    };

    let Ok(Some(from_bal)) = fluent_guest::get_for_update(&from) else {
        return 3;
    };
    let Ok(Some(to_bal)) = fluent_guest::get_for_update(&to) else {
        return 3;
    };
    let (Some(from_bal), Some(to_bal)) = (
        from_bal.get(..8).map(|b| u64::from_le_bytes(b.try_into().unwrap())),
        to_bal.get(..8).map(|b| u64::from_le_bytes(b.try_into().unwrap())),
    ) else {
        return 3;
    };

    if from_bal < amount {
        return 1;
    }
    let new_from = from_bal - amount;
    let new_to = to_bal.wrapping_add(amount);
    if fluent_guest::put(&from, &new_from.to_le_bytes()).is_err() {
        return 4;
    }
    if fluent_guest::put(&to, &new_to.to_le_bytes()).is_err() {
        return 4;
    }
    fluent_guest::output(&new_from.to_le_bytes());
    fluent_guest::output(&new_to.to_le_bytes());
    0
}

fluent_guest::fluent_main!(transfer_main);
