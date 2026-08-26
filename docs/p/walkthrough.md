<!-- Generated from docs/index.html by scripts/build-agent-docs.py. Do not edit. -->

<!-- Section: Tutorial · Human version: https://orthory.github.io/fluent31/#walkthrough -->

# The whole thing at once

> One program that installs modules, binds a trigger, drives an executor under contention, waits for derived state and rehearses a destructive change on a fork — asserting every step.

The previous steps each showed one surface. This is the shape a real driver has, and it is a file in the repository rather than prose: `crates/fluent31/examples/walkthrough.rs`. The test suite compiles it and it asserts its own results, so it cannot quietly stop being true.

## Run it

```
cargo run -p fluent31 --example walkthrough
```

Modules are ordinary crates compiled to `wasm32-unknown-unknown`, and the example builds them on first run. In a project of your own that step is yours: `cargo build --manifest-path guests/Cargo.toml --target wasm32-unknown-unknown --release`, then install the `.wasm` bytes it produces.

## What it does

It uses two modules the repository already ships. `place_order` is an executor: one transaction allocates an order id from a counter key, writes the record and folds the amount into the customer's running total. `customer_index` is a trigger consumer that keeps a secondary index over those records. Neither knows about the other.

```
db.install_module("place_order", &guest_wasm("place_order"))?;
db.install_module("customer_index", &guest_wasm("customer_index"))?;

let mode = db.create_trigger("customerIndex", "customer_index",
                            Some(b"orders/"), Some(b"orders0"))?;
```

The mode is not an argument. `customer_index` exports `on_touch`, so the engine registers it in keys mode; a module exporting `on_apply` would get changes mode instead. The call returns what was detected.

## The two loops you have to write

Everything else in the file is ordinary Rust. These two are the ones that are easy to leave out and painful to leave out.

**Waiting for derived state.** A trigger runs after the write that caused it has already committed, so a read taken the moment a write returns sees the state before the trigger ran. There is no synchronous "run the triggers now":

```
let deadline = Instant::now() + Duration::from_secs(30);
loop {
    let all = db.list_triggers()?;
    let mine: Vec<_> = all.iter().filter(|t| names.contains(&t.name.as_str())).collect();
    if let Some(t) = mine.iter().find(|t| t.last_error.is_some()) {
        panic!("trigger {} is stuck: {:?}", t.name, t.last_error);   // never transient
    }
    if mine.iter().all(|t| t.pending == 0) { break; }
    assert!(Instant::now() < deadline, "triggers did not drain in 30s");
    std::thread::sleep(Duration::from_millis(5));
}
```

`last_error` is fatal rather than transient: a module that fails holds its batch instead of dropping it, so a queue that stops moving does not start again on its own.

**Retrying a conflict.** `execute` runs the module inside a transaction and re-runs it on a commit conflict — but only `execute_retries` times, three by default. Under real contention those get spent and `Conflict` reaches the caller, having written nothing:

```
loop {
    match db.execute("place_order", input.as_bytes()) {
        Ok(out) => return serde_json::from_slice(&out)?,
        Err(Error::Conflict) => continue,        // nothing was written; run it again
        Err(e) => return Err(e),
    }
}
```

Four threads placing twenty-four orders against a single counter key spend the engine's three attempts routinely — a typical run reports around half the calls arriving here. Without this loop that is not a slow path, it is lost work.

## What it proves

Every line of output is an assertion that held:

```
== install the modules
   modules: customer_index, place_order
   place_order describes itself as kind="execute" output="PlacedOrder!"

== bind the trigger over [orders/, orders0)
   mode detected from the module's exports: keys

== one order, through the executor
   {"amountCents":1250,"customer":"acme","customerOrders":1,"customerTotalCents":1250,"id":1}
   index entry: idx/customer/acme/00000001

== 4 threads x 6 orders, all retrying on conflict
   24 orders placed, 24 distinct ids, 14 reached the caller as Conflict

== the derived state agrees with the records
   acme: 9 orders, 3850 cents
   globex: 8 orders, 2800 cents
   initech: 8 orders, 3000 cents

== rehearse a destructive change on a fork
   forked at seqno 228 -> /tmp/.tmpAOxum2/archive/rehearsal
   fork: 1 keys left, parent: 26 — untouched

done: installed, bound, drained, contended and rehearsed.
```

Two of those are worth naming. Every order gets a distinct id even though every order increments the same counter, because the id is allocated inside the transaction — a losing attempt is discarded whole rather than leaving a gap or a duplicate. And the executor's own per-customer count, written by the module, matches the number of index entries written by an unrelated trigger.

## Rehearsing on a fork

The last section deletes every order — on a copy. `fork` publishes a complete database directory by hard-linking what is already immutable, so the copy costs file count rather than data size, and opening it gives a writable clone:

```
let fork = db.fork("rehearsal")?;
let rehearsal = Db::open(&fork.path, Options::default())?;
// ... run the change here, against real data, with nothing at stake
```

This is what makes a migration checkable before it is real: run it on the fork, look at the result, and throw the fork away if it is wrong. [Forks in practice](ex-forks.md) covers the rest of the shape.

## Where to go next

- **Concepts** explains why the engine behaves the way these steps showed — snapshots and the watermark, the consistency contract, what triggers guarantee, what a fork is.
- **Extending with WASM** is the whole of the module story: what each role means, the catalogue of what to build, the guest SDK and the host ABI, and how a module becomes a typed GraphQL field.
- **Reference** is the exact surface of each entry point: every `Options` field, the GraphQL schema, the shell commands, server configuration, replication, operations.
- **Recipes** are the worked shapes — aggregates, invariants, indexes, feeds, migrations, forks — each with the reference module that implements it.

The repository carries four more walkthroughs of the same kind, each asserting its own results:

```
cargo run -p fluent31 --example live_stats       # a live aggregate, checked against a full recount
cargo run -p fluent31 --example dynamic_index    # indexes created at runtime from a spec key
cargo run -p fluent31 --example cascade_delete
cargo run -p fluent31 --example claim            # N concurrent claimers, exactly one winner
scripts/demo-orders.sh                           # typed modules against a running server
```

---

fluent31 docs · [index](../llms.txt) · [all pages](https://orthory.github.io/fluent31/llms.txt) · this page is `The whole thing at once` in *Tutorial*
