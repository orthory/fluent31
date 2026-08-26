<!-- Generated from docs/index.html by scripts/build-agent-docs.py. Do not edit. -->

<!-- Section: Recipes · Human version: https://orthory.github.io/fluent31/#ex-executors -->

# Invariants & procedures

> The category: writes whose correctness depends on what was read — transfers, unique claims, id allocation, multi-record updates that must land together.

The shape is the guarded write, and it is an `execute` module: each call runs in a fresh optimistic transaction, exit 0 commits, and a conflicting concurrent write re-runs the whole attempt against a fresh snapshot. The engine's OCC loop is what turns "read, decide, write" into an invariant that holds under concurrency.

## When to reach for it

- More than one key must change together, or not at all.
- A constraint must survive concurrent writers: uniqueness, non-negative balances, monotonic ids.
- Every surface must go through the same logic — an installed executor cannot be bypassed by a caller doing raw puts *if* callers write through it.

**When not:** independent blind writes — a `WriteBatch` is already atomic. And if your Rust process is the only writer, an embedded `Txn` with `get_for_update` is the same machinery without the module.

## The shape

`guests/claim` is a uniqueness invariant in one match statement — concurrent claimers race through OCC and exactly one wins:

<!-- guests/claim/src/lib.rs (trimmed) -->
```
let key = format!("uname/{}", input.username);
let already = match fluent_guest::get_for_update(key.as_bytes()) {
    Ok(Some(holder)) if holder == input.owner.as_bytes() => true,  // idempotent re-claim
    Ok(Some(holder)) => return Err(Fail::new(1, format!("taken by {..}"))),
    Ok(None) => {
        fluent_guest::put(key.as_bytes(), input.owner.as_bytes())
            .map_err(|_| Fail::new(3, "claim write failed"))?;
        false
    }
    Err(_) => return Err(Fail::new(3, "claim read failed")),
};
```

Everything the category demands is in those lines:

- **`get_for_update` on every key a write depends on.** That puts the key in the conflict set: two concurrent claims of one name cannot both commit — the loser re-runs, sees the winner, and fails cleanly.
- **Idempotent under re-execution.** The module is a pure function of (input, snapshot). A re-claim by the current owner is success (`"already": true`), not an error, so client retries and OCC re-runs are harmless.
- **Distinct exit codes per failure class**, message in the output — the caller can tell "taken" from "bad input" from "engine trouble".

## Scaling the shape up

`guests/transfer` is the two-account balance move: both balances locked with `get_for_update`, insufficient funds as its own clean exit code, checked arithmetic throughout — an executor that overflows corrupts durable state.

`guests/place_order` is the full multi-write procedure: allocate a monotonic id from a counter key, write the order record, fold the amount into the customer's running stats — three coordinated writes in one transaction, which is exactly the point of an executor over plain `put`. Its strictest rule is the one to internalize: **present-but-malformed state is corruption, not a default.** An unparseable counter fails loudly with its own code — "reset to 1" would silently overwrite existing orders.

## The caller's side

`execute_retries` bounds the engine's loop — 3 attempts by default. Under real contention (twenty writers on one key) those attempts get spent and `Error::Conflict` reaches the caller with nothing written. That is not a broken invariant; it is the caller's turn. Either raise `execute_retries` or retry from outside:

```
let out: Vec<u8> = loop {
    match db.execute("claim", &input) {
        Ok(out) => break out,
        Err(fluent31::Error::Conflict) => continue,   // retries spent; the whole call is safe to re-run
        Err(e) => return Err(e.into()),
    }
};
```

Over GraphQL the same outcome is `CONFLICT` in `errors[].extensions.code`, and the client retries the mutation. Because the executor is a pure function of its input and the data, re-running the whole call is always safe — which is why that rule matters.

## Run it

```
cargo run -p fluent31 --example claim    # N concurrent claimers, exactly one winner, asserted
db.execute("claim", br#"{"username":"ada","owner":"a-1"}"#)?     // a typed module takes the same JSON here
mutation { placeOrder(customer: "you", amountCents: "4200") { id } }   # typed field: the module is installed as "placeOrder"
```

---

fluent31 docs · [index](../llms.txt) · [all pages](https://orthory.github.io/fluent31/llms.txt) · this page is `Invariants & procedures` in *Recipes*
