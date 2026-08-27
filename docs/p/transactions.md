<!-- Generated from docs/index.html by scripts/build-agent-docs.py. Do not edit. -->

<!-- Section: Concepts · Human version: https://orthory.github.io/fluent31/#transactions -->

# Transactions

> Optimistic, snapshot isolation, first committer wins.

`commit()` checks every key read with `get_for_update` and every key in the write set against the transaction's snapshot; a committed delete conflicts too. The loser gets `Error::Conflict` with nothing written, and the fix is to run the whole read-modify-write again. A plain `get` inside a transaction is a consistent read but not a conflict check, so use `get_for_update` for every key you base a write on.

## The retry loop

```
loop {
    let mut txn = db.begin();
    let n = txn.get_for_update(b"seq")?.map(decode).unwrap_or(0);
    txn.put("seq", encode(n + 1))?;
    match txn.commit() {
        Ok(()) => break,
        Err(fluent31::Error::Conflict) => continue,
        Err(e) => return Err(e),
    }
}
```

Under `SyncMode::Always`, concurrent commits share fsyncs with plain writers through group commit. Validation and application happen as one atomic step against every other writer, including plain `db.put`.

> **Executors too.** A WASM [executor](wasm-roles.md) runs inside exactly this kind of transaction, and the engine drives the retry loop for you — up to `execute_retries` attempts (3), after which `Conflict` reaches the caller and the loop above becomes the caller's job again. That is why an executor must be a pure function of its input and the database state.

---

fluent31 docs · [index](../llms.txt) · [all pages](https://orthory.github.io/fluent31/llms.txt) · this page is `Transactions` in *Concepts*
