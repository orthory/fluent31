<!-- Generated from docs/index.html by scripts/build-agent-docs.py. Do not edit. -->

<!-- Section: Recipes · Human version: https://orthory.github.io/fluent31/#ex-oneshot -->

# Migrations & one-shots

> The category: changes you make once — format migrations, backfills, data repair, ad-hoc admin jobs. An ordinary executor, invoked without being installed.

One-shot invocation (`execonce` in the shell, `wasmExecuteOnce` in GraphQL, `db.execute_wasm` in Rust) runs module bytes that are never stored: nothing is listed, cached or replicated, and the committed writes are the only trace. Same ABI, same SDK, same limits, same OCC retry loop. Triggers still fire on its writes, so trigger-maintained indexes and feeds stay correct straight through a migration.

## The recipe

Walk a prefix, skip already-migrated records, rewrite the rest:

<!-- user_v2.rs (a one-shot migration) -->
```
#[fluent_guest::execute]
fn user_v2(_input: Vec<u8>) -> Result<String, Fail> {
    let scan = fluent_guest::scan_prefix(b"user/").map_err(|_| Fail::new(2, "scan"))?;
    let mut migrated = 0u64;
    for (key, value) in scan {
        let old: serde_json::Value = serde_json::from_slice(&value)
            .map_err(|_| Fail::new(3, "corrupt record"))?;   // fail loudly, never default
        if old.get("v").is_some() {
            continue;              // already v2: idempotent re-run
        }
        let new = serde_json::json!({ "v": 2, "name": old });
        fluent_guest::put(&key, new.to_string().as_bytes())
            .map_err(|_| Fail::new(4, "put"))?;
        migrated += 1;
    }
    Ok(format!("migrated {migrated}"))
}
```

```
$ fluent-cli ./db
> execonce guests/target/wasm32-unknown-unknown/release/user_v2.wasm
migrated 41283
```

The whole migration is **one transaction**: atomic even across conflict retries, invisible until commit, serialized against concurrent writers by OCC. That is also its bound — the write set must fit `max_txn_write_bytes` (256 MiB) and the work must fit `wasm_fuel`. For a bigger keyspace, shard by cursor: take a start key as input, migrate up to N records, return the next start key, and drive the loop from the caller. Each chunk is then its own atomic, retry-tolerant transaction.

## The category's rules

1. **Idempotent by inspection.** Detect an already-migrated record in the data itself and skip it. Never track "did it run" anywhere else — a retried attempt or a re-run must always be safe.
2. **Fail loudly on the unexpected.** A record that parses wrong is corruption, with its own exit code. A non-zero exit aborts the whole transaction: nothing half-migrated ever survives.
3. **Rehearse on a fork.** Fork the instance, run the one-shot against the fork (its own `/graphql/<instanceId>` endpoint under the server), inspect the result, then run it on the primary. See [Forks in practice](ex-forks.md).
4. **The repo is the audit trail.** A one-shot leaves no record in the store, so the script and its git history are the record of what ran. Install the big audited migrations; one-shot the rest.

---

fluent31 docs · [index](../llms.txt) · [all pages](https://orthory.github.io/fluent31/llms.txt) · this page is `Migrations & one-shots` in *Recipes*
