<!-- Generated from docs/index.html by scripts/build-agent-docs.py. Do not edit. -->

<!-- Section: Extending with WASM · Human version: https://orthory.github.io/fluent31/#wasm-invoking -->

# Invoking and debugging

> Every surface reaches the same modules, installed or not, with the same limits and the same failures.

## Installed modules

| Surface | Query | Execute |
|---|---|---|
| Rust | `db.query(name, input)`, `db.query_at(name, input, &snap)` | `db.execute(name, input)` |
| Shell | `query NAME [INPUT]` | `exec NAME [INPUT]` |
| GraphQL, generic | `wasm(module:, input:)` | `wasmExecute(module:, input:)` |
| GraphQL, typed | `<module>(args)` on `Query` | `<module>(args)` on `Mutation` |

Both return the guest's output bytes. A trigger consumer is never invoked directly — it is bound to a range and the engine calls it.

## Managing them

| Operation | Rust | Shell | GraphQL |
|---|---|---|---|
| install | `install_module(name, wasm)` | `install NAME FILE.wasm` | `installModule(name, wasm)` |
| uninstall | `uninstall_module(name)` | `uninstall NAME` | `uninstallModule(name)` |
| list | `list_modules()` | `modules` | `modules { name typed schemaError }` |
| inspect exports | `module_entries(name)`, `wasm_entries(wasm)` | — | — |

`list_modules` returns a `ModuleInfo` per module: its name, its size in bytes, and a content fingerprint that lets a caller skip re-processing bytes it has already seen. `module_entries` and `wasm_entries` return the role exports the engine found, which is how you check what a binary actually is before or after installing it. GraphQL's `installModule` also accepts WAT text (`wasm: {text: "(module ...)"}`).

Installing over an existing name replaces the bytes. Invocations already running finish on the bytes they started with. Module bytes are ordinary versioned keys in the engine's own keyspace, so they recover with the store, copy into forks, and are visible to `query_at` at an old sequence number — the store can answer "what code ran here".

## Running bytes that are never installed

| Surface | Query | Execute |
|---|---|---|
| Rust | `db.query_wasm(wasm, input)`, `query_wasm_at(.., &snap)` | `db.execute_wasm(wasm, input)` |
| Shell | `queryonce FILE.wasm [INPUT]` | `execonce FILE.wasm [INPUT]` |
| GraphQL | `wasmOnce(wasm:, input:)` | `wasmExecuteOnce(wasm:, input:)` |

Same ABI, SDK, limits and retry loop, except that the code is pinned across all attempts. Nothing is listed, cached or replicated; an executor's committed writes are the only trace. Triggers still fire on those writes. `describe` is ignored, so there is no typed field, and a trigger can only ever bind to an installed module.

## What failure looks like

| Error | Cause | What to do |
|---|---|---|
| `GuestFailed { code, output }` | the entry exited non-zero; an executor's transaction was aborted, so nothing was written | read the code and the message — this is the module's own verdict, not a fault |
| `Conflict` | an executor exhausted `execute_retries` against concurrent writers | re-run the call; the retry loop is now the caller's |
| `Error::Wasm` | a trap: out-of-bounds memory, fuel exhausted, an unreachable instruction | a bug or a runaway loop in the module; fuel bounds it, it does not fix it |
| `InvalidArgument` | an input above `max_wasm_input`, or a module with no usable exports | checked before the module runs |
| `OUTPUT_SCHEMA_VIOLATION` | the output did not match the descriptor; `committed: true` on executors | fix the module — and never blind-retry a committed one |

Over GraphQL the guest's own failures arrive as `guestExitCode` and `guestOutputText` rather than as transport errors, so a client can branch on the code. In the shell a guest failure prints `guest exited with code N, output …`, and a conflict prints `CONFLICT (first committer wins)`.

A trigger consumer's failure is not returned to anyone — the write that caused it has already committed. It surfaces as a stalled queue: `triggers` shows a growing `pending` and a `lastError`, and the runner keeps retrying with backoff until the module is fixed or replaced. [Triggers](triggers.md) covers the drain loop.

## Seeing inside a module

Set `FLUENT31_WASM_LOG` and the host prints `fluent_guest::log` output to stderr; leave it unset and the calls are cheap and silent. Logs are capped at `max_wasm_log` per invocation, so they are a debugging channel and never a results channel. Beyond that the tools are the ordinary ones: exercise the module against a [fork](forks.md) of real data, and pin the invariant with a test that runs the executor concurrently — [Testing](testing.md) has the harness.

---

fluent31 docs · [index](../llms.txt) · [all pages](https://orthory.github.io/fluent31/llms.txt) · this page is `Invoking and debugging` in *Extending with WASM*
