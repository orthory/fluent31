//! `claim` — atomic unique-claim executor: the schema-free UNIQUE
//! constraint. Concurrent claimers of the same name race through the
//! engine's OCC loop and exactly one wins; everyone else gets a clean,
//! attributable failure. Demo: `cargo run -p fluent31 --example claim`.
//!
//! Input:  {"username": "...", "owner": "..."}
//! Keys:   uname/<username> -> owner
//! Output: {"username": "...", "owner": "...", "already": bool}
//!
//! What this demonstrates about executors:
//! - `get_for_update` puts the claim key in the transaction's conflict
//!   set: two concurrent claims of one name cannot both commit — the
//!   loser's attempt re-runs against a fresh snapshot, sees the winner,
//!   and fails with code 1 (surfaced as GUEST_FAILED + message).
//! - Re-execution safety: the module is a pure function of (input,
//!   snapshot) — a re-claim by the current owner is idempotent success
//!   (`"already": true`), not an error, so client retries are harmless.
//! - Distinct `Fail` codes per class: 1 = taken, 2 = bad input.

use fluent_guest::Fail;
use serde::Deserialize;
use serde_json::json;

#[derive(Deserialize)]
struct Input {
    username: String,
    owner: String,
}

#[fluent_guest::execute]
fn claim(raw: Vec<u8>) -> Result<String, Fail> {
    let input: Input = serde_json::from_slice(&raw)
        .map_err(|_| Fail::new(2, "input is not {\"username\", \"owner\"} JSON"))?;
    let ok = |s: &str| {
        !s.is_empty() && s.len() <= 64 && !s.contains('/') && s.is_ascii()
    };
    if !ok(&input.username) || !ok(&input.owner) {
        return Err(Fail::new(2, "username/owner must be ascii, no '/', 1..=64"));
    }

    let key = format!("uname/{}", input.username);
    let already = match fluent_guest::get_for_update(key.as_bytes()) {
        Err(_) => return Err(Fail::new(3, "claim read failed")),
        Ok(Some(holder)) if holder == input.owner.as_bytes() => true, // idempotent re-claim
        Ok(Some(holder)) => {
            return Err(Fail::new(
                1,
                format!("{:?} is taken by {}", input.username, String::from_utf8_lossy(&holder)),
            ))
        }
        Ok(None) => {
            fluent_guest::put(key.as_bytes(), input.owner.as_bytes())
                .map_err(|_| Fail::new(3, "claim write failed"))?;
            false
        }
    };
    Ok(json!({"username": input.username, "owner": input.owner, "already": already}).to_string())
}
