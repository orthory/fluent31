<!-- Generated from docs/index.html by scripts/build-agent-docs.py. Do not edit. -->

<!-- Section: Extending with WASM · Human version: https://orthory.github.io/fluent31/#wasm-overview -->

# Code in the database

> The query surface is WebAssembly. You install modules into the database and call them by name — as reads, as transactions, or as the consumers behind a trigger.

A module is a WASM binary stored in the database like any other value. Its exports are its roles — a read-only `query`, a transactional `execute`, an `on_touch` or `on_apply` trigger consumer, and an optional `describe` that turns the module into API — and one binary may carry several of them at once.

## Why code lives here

- **The computation runs next to the data.** A report over a million keys returns its five numbers, not a million values, and it runs at one pinned snapshot, so the answer is internally consistent.
- **The invariant belongs to the database, not to a client.** An installed executor is the same logic on every surface — Rust, the shell, GraphQL — so a rule like "stock never goes negative" holds wherever the write came from.
- **A module that describes itself becomes API.** Export `describe` and installing the module adds its own typed field to the GraphQL schema, hot-swapped at install time.
- **Module bytes are data.** They live in the engine's own keyspace as ordinary versioned keys, so they are recovered with the store, copied into forks, and time-travelled by `query_at` — the store can answer "what code ran here".

## The bargain

Modules are sandboxed. Fuel-metered and memory-capped, with no WASI, no clock and no randomness, they can import only the `fluent` host functions: `get`, `get_for_update`, `put`, `delete`, batched scans, output and log. Entropy and time are inputs, never ambient. That is the price of letting arbitrary code run in the write path, and it is what keeps the engine's own guarantees independent of any module. The limits protect reliability and integrity; authentication and authorization are a layer you put in front.

> **This section** [Roles and lifecycles](wasm-roles.md) is what each export means and when the engine calls it. [What to build with it](wasm-uses.md) is the catalogue of shapes a module takes, and the honest list of what one cannot do. [The guest SDK](wasm-sdk.md) and [The host ABI](wasm-abi.md) are the two API surfaces — the Rust one you will write against, and the raw one underneath it. [Typed GraphQL fields](wasm-typed.md) turns a module into API, and [Invoking and debugging](wasm-invoking.md) covers calling, managing and diagnosing them.

---

fluent31 docs · [index](../llms.txt) · [all pages](https://orthory.github.io/fluent31/llms.txt) · this page is `Code in the database` in *Extending with WASM*
