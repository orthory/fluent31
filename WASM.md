# fluent31 WASM modules — authoring manual & ABI spec

Everything needed to write, describe, install, and invoke an in-database
WASM module ("fluentabi v1"). Self-contained: an agent with only this file
and a Rust (or WAT) toolchain can produce a working, typed module.

Source of truth if this file ever drifts: `crates/fluent31/src/wasm/abi.rs`
(host ABI), `crates/fluent-graphql/src/descriptor.rs` (describe spec),
`crates/fluent-guest/src/lib.rs` (Rust SDK).

---

## 1. What a module is

A WASM binary (`wasm32-unknown-unknown`) stored *inside* the database
(versioned and recovered like any key) and executed by the engine in one of
two modes:

| Mode | Entry | Access | Invoked via |
|---|---|---|---|
| **query** | `run` | read-only at a pinned MVCC snapshot; `put`/`delete` return `EROFS` | `Db::query`, GraphQL `wasm(module:, input:)`, or its own typed Query field |
| **executor** | `run` | a fresh optimistic transaction; guest exit `0` commits, anything else aborts | `Db::execute`, GraphQL `wasmExecute(module:, input:)`, or its own typed Mutation field |

**Executor retry semantics (critical):** on commit conflict the WHOLE
attempt is discarded and re-run against a fresh snapshot — fresh Store,
fresh memory, fresh fuel, fresh output — up to `execute_retries` times
(default 3). Your `run` may execute several times per logical call, so it
must be a pure function of (input bytes, database state): no side channels,
no assuming a previous attempt's effects. Writes are buffered in the
transaction and only become visible on commit, so re-runs are safe as long
as you don't smuggle state out through `log` or panic-once logic.

Module bytes are resolved at the invocation's snapshot: `query_at`
time-travels code together with data, and each execute attempt sees a
consistent module version.

Determinism: the runtime canonicalizes NaNs, forces deterministic
relaxed-simd, and compiles without threads. Don't import anything beyond
the `fluent` module — there is no WASI, no clock, no randomness. If you
need entropy or time, take it as input.

## 2. Required exports

```
memory   : (export "memory" (memory ...))       — REQUIRED
run      : (export "run" (func (result i32)))   — REQUIRED, no params
describe : (export "describe" (func (result i32))) — OPTIONAL, same shape as run
```

Install is rejected unless `run` + `memory` exist. `describe`, when
present, is executed by the GraphQL server (read-only, empty input) at
install / schema-build time; its output bytes must be a descriptor
(section 5).

**Exit codes:** `0` = success (and, for executors, commit). Any non-zero
exit aborts the transaction and surfaces to callers as
`Error::GuestFailed { code, output }` — GraphQL clients see
`extensions.code = "GUEST_FAILED"` with `guestExitCode`,
`guestOutputText` / `guestOutputBase64`. Convention: use the output buffer
for a human-readable failure message and pick distinct exit codes per
failure class (the demo guests use 2..=7).

## 3. The host ABI (`fluent` import module)

Conventions:

- All pointers/lengths are u32 passed as wasm `i32`. Out-of-range memory
  access **traps** (invocation fails with `Error::Wasm`); *semantic* misuse
  returns an errno instead.
- Errnos (negative return values, i32 or i64):
  `NOT_FOUND -1`, `EROFS -2`, `EINVAL -3`, `ENOSPC -4`, `EBADF -5`,
  `ELIMIT -6`, `EIO -8`. (`EIO` means the ENGINE failed — the invocation
  will fail host-side even if you swallow the errno and exit 0.)
- Keys starting with byte `0x00` are the engine's reserved keyspace:
  writes return `EINVAL`, reads return `NOT_FOUND`, scans are silently
  clamped to the user keyspace. Empty keys are `EINVAL`.

### Imports

```
input_len  : () -> i32
input_read : (dst: i32, cap: i32, off: i32) -> i32
```
The invocation's input blob. `input_read` copies up to `cap` bytes starting
at input offset `off` into guest memory at `dst`, returns bytes copied.

```
output_write : (ptr: i32, len: i32) -> i32
```
APPENDS `len` bytes to the invocation's output. Returns `0`, or `ENOSPC`
once total output would exceed `max_wasm_output` (default 32 MiB). Check
the return value if truncated output would be a correctness bug.

```
log : (level: i32, ptr: i32, len: i32) -> i32
```
Debug logging, rate-capped at `max_wasm_log` total bytes (default 1 MiB,
then `ENOSPC`). Host prints to stderr only when the `FLUENT31_WASM_LOG`
env var is set. Never use logs to communicate results.

```
get            : (kptr, klen, off, vbuf, vcap: i32) -> i64
get_for_update : (kptr, klen, off, vbuf, vcap: i32) -> i64
```
Point lookup at this invocation's snapshot (executors see their own
buffered writes overlaid). Returns the FULL value length (i64 ≥ 0) and
copies `min(vcap, len - off)` bytes from value offset `off` into `vbuf` —
call again with a larger buffer or advancing `off` to chunk-read values
bigger than guest memory. `NOT_FOUND` if absent. `get_for_update`
additionally adds the key to the transaction's read/lock set (first
committer wins — use it for read-modify-write like counters); in a
read-only query it returns `EROFS`.

```
put    : (kptr, klen, vptr, vlen: i32) -> i32
delete : (kptr, klen: i32) -> i32
```
Buffer a write in the transaction. `EROFS` in query mode. `EINVAL` for
reserved/empty/oversized keys (`max_key_size` 16 KiB) or oversized values
(`max_value_size` 256 MiB). `ENOSPC` when the transaction's write set
exceeds `max_txn_write_bytes` (256 MiB). `delete` of an absent key
succeeds.

```
scan_open  : (lo_ptr, lo_len, hi_ptr, hi_len, flags: i32) -> i32
```
Open an iterator over `[lo, hi)` at the snapshot. Zero-length `lo`/`hi`
mean unbounded. `flags`: bit 0 = reverse; all other bits `EINVAL`.
Returns a handle (≥ 0), or `ELIMIT` after `max_wasm_scans` (default 64)
concurrently open handles. Handles are per-invocation and never survive
`run` returning.

```
scan_next : (h: i32, buf: i32, cap: i32) -> i32
```
Fills `buf` with as many whole entries as fit in `cap` (host-side batch
ceiling 16 MiB), each packed as
`[klen uvarint][vlen uvarint][key bytes][value bytes]`. Returns bytes
written; `0` = end of range; `ENOSPC` = the NEXT single entry doesn't fit
in `cap` (grow the buffer or use `scan_entry_hint`); `EBADF` bad handle;
`EIO` engine error.

```
scan_entry_hint : (h: i32) -> i64   — packed size of the next entry (0 at end)
scan_skip       : (h: i32) -> i32   — drop the next entry; 1 if skipped, 0 at end
scan_close      : (h: i32) -> i32   — free the handle
```

### Resource limits (per invocation, engine `Options` defaults)

| Limit | Default | On breach |
|---|---|---|
| `wasm_fuel` | 1_000_000_000 | trap (`Error::Wasm`) — no infinite loops |
| `wasm_memory_limit` | 64 MiB | memory.grow fails |
| `max_wasm_input` | 64 MiB | `InvalidArgument` before execution |
| `max_wasm_output` | 32 MiB | `output_write` → `ENOSPC` |
| `max_wasm_log` | 1 MiB | `log` → `ENOSPC` |
| `max_wasm_scans` | 64 open handles | `scan_open` → `ELIMIT` |

## 4. Writing a module in Rust (the `fluent-guest` SDK)

Crate setup (see `guests/place_order` for a complete example):

```toml
# guests/<name>/Cargo.toml — inside the guests/ workspace
[package]
name = "my_module"
version = "0.1.0"
edition = "2021"

[lib]
crate-type = ["cdylib"]

[dependencies]
fluent-guest = { path = "../../crates/fluent-guest" }
serde_json = "1"          # optional, works fine on wasm32-unknown-unknown
```

Add the crate to `guests/Cargo.toml` `members`. The SDK wraps the raw ABI:

```rust
fluent_guest::input() -> Vec<u8>                  // the input blob
fluent_guest::output(&[u8])                       // append to output
fluent_guest::log(&str)                           // debug log
fluent_guest::get(&[u8]) -> Option<Vec<u8>>
fluent_guest::get_for_update(&[u8]) -> Result<Option<Vec<u8>>, i32>  // Err = errno
fluent_guest::put(&[u8], &[u8]) -> Result<(), i32>
fluent_guest::delete(&[u8]) -> Result<(), i32>
fluent_guest::scan(lo: Option<&[u8]>, hi: Option<&[u8]>) -> Result<Scan, i32>
fluent_guest::scan_rev(lo, hi) -> Result<Scan, i32>
fluent_guest::scan_prefix(&[u8]) -> Result<Scan, i32>
// Scan: Iterator<Item = (Vec<u8>, Vec<u8>)>, plus .skip_pending()
```

Skeleton:

```rust
fn my_main() -> i32 {
    let input = fluent_guest::input();
    // ... validate, read, write ...
    fluent_guest::output(b"result bytes");
    0 // commit (executor) / success (query)
}
fluent_guest::fluent_main!(my_main);       // exports `run`
fluent_guest::fluent_describe!(r#"{...}"#); // optional: exports `describe` (section 5)
```

NOTE: `fluent_main!(f)` generates `pub extern "C" fn run()` — your inner
function must NOT itself be named `run` (duplicate definition).

Build:

```sh
cargo build --manifest-path guests/Cargo.toml \
  --target wasm32-unknown-unknown --release --target-dir guests/target
# artifact: guests/target/wasm32-unknown-unknown/release/my_module.wasm
```

(If cargo isn't rustup's, set `RUSTC="$(rustup which rustc)"` so the
wasm32 std is found — only when rustup exists.)

Install & invoke (GraphQL; equivalents exist on `Db` and the CLI):

```graphql
mutation Install($w: BytesInput!) {
  installModule(name: "myModule", wasm: $w) { name size typed schemaError }
}
# variables: {"w": {"base64": "<base64 of my_module.wasm>"}}

query    { wasm(module: "myModule", input: {text: "..."}) { text base64 hex len } }
mutation { wasmExecute(module: "myModule", input: {base64: "..."}) { base64 } }
```

WAT text is also accepted by `installModule` (`wasm: {text: "(module ...)"}`)
— handy for tests; see the WAT fixtures in
`crates/fluent-graphql/tests/graphql.rs`.

## 5. Typed GraphQL surface — "fluentabi v1 describe"

A module that exports `describe` becomes **its own typed root field**:
`kind: "query"` modules appear on `Query`, `kind: "execute"` on `Mutation`.
The GraphQL schema is rebuilt and hot-swapped on `installModule` /
`uninstallModule`, at server startup for already-installed modules, and on
demand via `mutation { reloadSchema }` (the resync path after installing
through the CLI or engine API directly).

`describe` runs read-only with EMPTY input and must write the descriptor
JSON to its output and exit 0. It should be static — just emit a constant
string (`fluent_describe!` does exactly this). Descriptor max size:
**64 KiB**.

### Descriptor shape

```json
{
  "kind": "query" | "execute",                     // REQUIRED
  "description": "docs for the root field",        // optional
  "args": [                                        // optional (see below)
    {"name": "customer", "type": "String!", "description": "..."}
  ],
  "types": [                                       // optional object types
    {"name": "PlacedOrder", "fields": [
      {"name": "id", "type": "U64!"},
      {"name": "note", "type": "String", "description": "..."}
    ]}
  ],
  "output": "PlacedOrder!"                         // REQUIRED
}
```

**Type grammar.** Scalars: `String`, `Int` (32-bit, range-enforced),
`Float`, `Boolean`, `U64` (64-bit unsigned; travels as a decimal string,
also accepts JSON numbers on input), `Json` (opaque passthrough). At most
ONE list level: `T`, `T!`, `[T]`, `[T!]`, `[T]!`, `[T!]!` — no nested
lists. `types` entries may reference scalars and each other; `args` may
reference scalars only. `output` may reference a declared type.

**Naming rules (enforced at install — violations REJECT the install):**

- module name must be a valid GraphQL name `[_A-Za-z][_0-9A-Za-z]*`, not
  starting with `__` (only relevant for described modules; undescribed
  ones keep the engine's looser `[A-Za-z0-9._-]` rule);
- module name must not shadow a built-in root field (`get`, `scan`,
  `wasm`, `modules`, `stats`, `checkpoints`, `snapshotSeqno`, `put`,
  `delete`, `writeBatch`, `wasmExecute`, `installModule`,
  `uninstallModule`, `checkpoint`, `deleteCheckpoint`, `flush`,
  `compactAll`, `gcVlog`, `reloadSchema`, `syncWal`);
- declared type names must not be reserved (`Query`, `Mutation`,
  `Subscription`, `Bytes`, `BytesInput`, `U64`, `Json`, `Pair`,
  `ScanPage`, `Module`, `Checkpoint`, `GcResult`, `LevelStats`, `Stats`,
  `WriteOp`, `PutOp`, `String`, `Int`, `Float`, `Boolean`, `ID`) and must
  not collide with a type another installed module already declares —
  prefix yours (`PlacedOrder`, not `Order`... think `MyModX`);
- limits: ≤ 32 types, ≤ 64 fields per type, ≤ 16 args.

A module whose descriptor fails these rules *after* it is already on disk
(installed out-of-band) degrades gracefully: it stays callable through
generic `wasm`/`wasmExecute`, and `modules { name typed schemaError }`
reports why it has no typed field.

### Input mapping (what your `run` receives)

- **With `args`:** ONE JSON object containing EVERY declared arg — omitted
  optional args are `null`. Example: field call
  `placeOrder(customer: "acme", amountCents: "5000")` with a declared but
  omitted `note` delivers `{"customer":"acme","amountCents":5000,"note":null}`.
  `U64` args arrive as JSON numbers. Non-null (`!`) args are enforced by
  GraphQL before your code runs.
- **Without `args`:** the field gets a single optional
  `input: BytesInput` (oneof `text`/`base64`/`hex`) and your `run`
  receives the raw decoded bytes (empty if omitted) — identical to the
  generic `wasm`/`wasmExecute` path.

### Output mapping (what your `run` must produce)

Output bytes are parsed as JSON and validated against `output`:

- declared object fields are type-checked recursively; `U64` accepts a
  JSON number or decimal string and is re-emitted as a string; `Int` must
  fit 32 bits; nulls violate `!` types;
- object keys you emit that are NOT declared are silently dropped;
  declared fields you omit become `null` (an error if declared `!`);
- `Json` passes through untouched.

Violations surface as a field error (`extensions.code =
"OUTPUT_SCHEMA_VIOLATION"`). For executors the error also carries
`committed: true` — output validation happens AFTER commit, so a client
must NOT blind-retry such an error. Emit output that matches your
declaration.

### GraphQL-side semantics you inherit

- Typed **query** fields run at the operation's single pinned MVCC
  snapshot — consistent with `get`/`scan`/`snapshotSeqno` in the same
  request. Typed **executor** fields run serially in document order, each
  in its own transaction.
- Root fields are always OUTER-nullable regardless of the declared
  `output` nullability, so a failure yields `field: null` + an `errors`
  entry instead of a spec-invalid response.
- Hot-swap caveat: a request in flight across an `installModule`
  *replacement* of the same name can run the NEW bytes with OLD-shaped
  args (one-request window). Replace modules under quiesced writes if
  that matters.

## 6. Reference modules

| Module | Kind | Shows |
|---|---|---|
| `guests/place_order` | execute | typed args, multi-key transaction (counter + record + stats fold), `get_for_update` locking, input validation with distinct exit codes, corruption checks that fail loudly |
| `guests/top_customers` | query | typed output list, `scan_prefix` aggregation at a snapshot, limit clamping |
| `guests/agg` | query (untyped) | raw-bytes input/output, chunked scan aggregation |
| `guests/transfer` | execute (untyped) | OCC transfer with conflict retries |
| `crates/fluent-graphql/tests/graphql.rs` | — | minimal WAT modules incl. `describe` exports (`wat_typed`) |

Demo end-to-end (server running): `scripts/demo-orders.sh` builds,
installs, seeds, and queries the typed pair.

## 7. Authoring checklist

1. Decide **query** (pure read) vs **execute** (writes; must tolerate
   re-execution).
2. Define your keyspace layout; validate any user input that becomes a key
   segment (reject `/`, empty, oversized — see `place_order`).
3. Use `get_for_update` for every read-modify-write key.
4. Distinct non-zero exit codes + message in output for each failure
   class; treat present-but-malformed state as corruption (fail), never as
   default-zero.
5. Checked arithmetic — an executor that overflows silently corrupts
   durable state.
6. Write the descriptor; prefix your type names; keep it a static string.
7. Build for `wasm32-unknown-unknown --release`, install via
   `installModule`, and confirm `typed: true, schemaError: null`.
8. Test: happy path, each failure exit, concurrency if executor
   (`tokio::spawn` N parallel calls; assert no lost updates), and restart
   (typed field must reappear — the server re-describes at startup).
