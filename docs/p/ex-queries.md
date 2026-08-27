<!-- Generated from docs/index.html by scripts/build-agent-docs.py. Do not edit. -->

<!-- Section: Recipes · Human version: https://orthory.github.io/fluent31/#ex-queries -->

# Queries in the database

> The category: report-shaped reads — count, sum, rank, filter, limit — where the answer is small and the data it summarizes is not.

Doing this from the client means scanning the whole range over the API to compute a few numbers. A `query` module moves the loop into the database: it runs at one pinned snapshot (so the report is internally consistent), it can only read (writes return `EROFS`), and it is callable from every surface.

## When to reach for it

- The result is much smaller than the data scanned.
- The whole answer must come from one consistent snapshot.
- You want the same report callable from Rust, the shell and GraphQL.

**When not:** point reads and plain range reads. `get` and `scan` are already optimal — wrapping them in a module adds a WASM invocation and removes nothing.

## The shape

`guests/agg` is the minimal reference — count, sum, min and max over a prefix, as one fold over a scan:

<!-- guests/agg/src/lib.rs (trimmed) -->
```
#[fluent_guest::query]
fn agg(prefix: Vec<u8>) -> Result<Vec<u8>, Fail> {
    if prefix.is_empty() {
        return Err(Fail::new(2, "empty prefix not allowed"));
    }
    let scan = fluent_guest::scan_prefix(&prefix).map_err(|_| Fail::new(3, "scan failed"))?;
    let (mut count, mut sum, ..) = ..;
    for (_key, value) in scan {
        count += 1;
        // fold the value into the aggregates
    }
    Ok(out)                          // 40 bytes out, however many keys in
}
```

The category's contract, visible even in the trimmed loop: validate the input (distinct `Fail` code per failure class), scan once, fold, return the answer — never the rows.

## The typed variant

`guests/top_customers` is the same category with a `describe` descriptor, so installing it (as `topCustomers` — the install name is the field name) creates a real GraphQL field — customers ranked by lifetime spend, floored and limited, computed inside the database at the operation's snapshot:

```
query { topCustomers(limit: 3, minTotalCents: "1000") { customer orders totalCents avgCents } }
```

Two category-level details worth copying from it: **clamp caller-supplied limits** in the module (it caps `limit` at 100 — the module is the last line of defense, whatever the surface), and **decide the corrupt-record policy per role**: a read-only report may skip a malformed record and `log` it, because it damages nothing; a writer never may (see [Invariants & procedures](ex-executors.md)).

## Run it

```
db.query("agg", b"accounts/")?          // Rust
query agg accounts/                      # shell
{ wasm(module: "agg", input: {text: "accounts/"}) { hex } }   # GraphQL, untyped
```

---

fluent31 docs · [index](../llms.txt) · [all pages](https://orthory.github.io/fluent31/llms.txt) · this page is `Queries in the database` in *Recipes*
