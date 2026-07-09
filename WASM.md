# fluent31 WASM modules — authoring manual & ABI spec

Everything needed to write, describe, install, and invoke an in-database
WASM module ("fluentabi v1"). Self-contained: an agent with only this file
and a Rust (or WAT) toolchain can produce a working, typed module.

Source of truth if this file ever drifts: `crates/fluent31/src/wasm/abi.rs`
(host ABI), `crates/fluent-graphql/src/descriptor.rs` (describe spec),
`crates/fluent31/src/trigger.rs` (trigger event/input encodings),
`crates/fluent-guest/src/lib.rs` (Rust SDK).

---

## 1. What a module is

A WASM binary (`wasm32-unknown-unknown`) stored *inside* the database
(versioned and recovered like any key) and executed by the engine in one of
two modes:

| Mode | Entry | Access | Invoked via |
|---|---|---|---|
| **query** | `run` | read-only at a pinned MVCC snapshot; `put`/`delete` return `EROFS` | `Db::query`, GraphQL `wasm(module:, input:)`, or its own typed Query field |
| **executor** | `run` | a fresh optimistic transaction; guest exit `0` commits, anything else aborts | `Db::execute`, GraphQL `wasmExecute(module:, input:)`, its own typed Mutation field — or by the engine itself as a **keys-mode write-range trigger** (section 8) |
| **change consumer** | `on_apply` | same as executor | ONLY by the engine, as a **changes-mode write-range trigger** (section 8): the input is the ordered list of committed changes |

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
run      : (export "run" (func (result i32)))   — no params
on_apply : (export "on_apply" (func (result i32))) — same shape as run
describe : (export "describe" (func (result i32))) — OPTIONAL, same shape as run
```

Install is rejected unless `memory` plus at least one of `run` /
`on_apply` exist. `run` serves queries, executors, and keys-mode
triggers; `on_apply` is the changes-mode trigger entry (section 8) — a
module may export either or both. `describe`, when present, is executed
by the GraphQL server (read-only, empty input) at install / schema-build
time; its output bytes must be a descriptor (section 5).

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

### Typed entry points (the primary interface)

Annotate a plain function taking one [`FromInput`] value and returning
`Result<impl IntoOutput, Fail>`; the attribute macro exports the entry
point and maps the result — `Ok` → exit 0 with the encoded output,
`Err(Fail { code, message })` → non-zero exit with the message in the
output buffer (a `Fail` code of 0 is coerced to 1: exit 0 must always
mean success).

```rust
use fluent_guest::Fail;

#[fluent_guest::main]                       // exports `run`
fn my_module(input: Vec<u8>) -> Result<String, Fail> {
    // ... validate, read, write ...
    Err(Fail::new(2, "distinct code per failure class"))
}

#[fluent_guest::on_apply]                   // exports `on_apply` (section 8)
fn my_feed(changes: Vec<fluent_guest::Change>) -> Result<(), Fail> {
    for c in changes { /* filter, then index/materialize */ }
    Ok(())
}

fluent_guest::fluent_describe!(r#"{...}"#); // optional: exports `describe` (section 5)
```

`FromInput` is implemented for `Vec<u8>` (raw bytes), `String` (UTF-8,
code-3 `Fail` on invalid), and `Vec<Change>` (the changes-mode trigger
input; code-3 `Fail` on anything else). `IntoOutput` covers `Vec<u8>`,
`String`, and `()`.

The declarative raw layer remains for modules that want to speak exit
codes directly: `fluent_main!(f)` / `fluent_on_apply!(f)` export an
`fn() -> i32` unchanged (pair the latter with `fluent_guest::changes()`).

NOTE: both macro styles generate the exported symbol themselves — the
annotated/inner function must NOT itself be named `run` / `on_apply`
(duplicate definition).

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
  `wasm`, `modules`, `stats`, `forks`, `triggers`, `snapshotSeqno`,
  `put`, `delete`, `writeBatch`, `wasmExecute`, `installModule`,
  `uninstallModule`, `fork`, `deleteFork`, `createTrigger`,
  `deleteTrigger`, `flush`, `compactAll`, `gcVlog`, `reloadSchema`,
  `syncWal`);
- declared type names must not be reserved (`Query`, `Mutation`,
  `Subscription`, `Bytes`, `BytesInput`, `U64`, `Json`, `Pair`,
  `ScanPage`, `Module`, `Fork`, `Trigger`, `GcResult`, `LevelStats`, `Stats`,
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
| `guests/place_order` | execute | typed args, multi-key transaction (counter + record + stats fold), `get_for_update` locking, input validation with distinct `Fail` codes, corruption checks that fail loudly |
| `guests/top_customers` | query | typed output list, `scan_prefix` aggregation at a snapshot, limit clamping |
| `guests/agg` | query (untyped) | raw-bytes input/output, chunked scan aggregation |
| `guests/transfer` | execute (untyped) | OCC transfer with conflict retries |
| `guests/customer_index` | execute (keys-mode trigger) | trigger-maintained secondary index: `parse_trigger_keys()`, reconcile-against-current-state, the back-pointer pattern for updates/deletes |
| `guests/order_feed` | change consumer (changes-mode trigger) | ordered changefeed materialization: `#[fluent_guest::on_apply]`, per-op kinds/values/seqnos, in-code filtering, elided-value handling |
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

## 8. Write-range triggers

A trigger binds an installed module to a user-key range: the engine
invokes it asynchronously after every committed write (put OR delete,
from any writer — plain puts, batches, transactions, other executors)
that touches `[lo, hi)`. This is the schema-free way to build custom
indexes, materialized views, and changefeeds: no declared columns, just a
key range and code.

A trigger consumes writes in one of two **modes**, detected from the
module's exports at registration (and fixed for the trigger's lifetime):

| Mode | Module exports | Entry | Input | Coalescing |
|---|---|---|---|---|
| **keys** | `run` only | `run` | touched keys, no values/kinds/order | re-touches coalesce to one pending event |
| **changes** | `on_apply` | `on_apply` | the ordered list of committed changes: seqno + kind + key + value | none — one event per committed op |

```
Db::create_trigger(name, module, lo, hi)   # None/omitted bound = open end;
                                           # returns the detected mode
Db::delete_trigger(name)                   # discards pending events
Db::list_triggers()                        # mode + pending count + lastError

CLI:      mktrig NAME MODULE [LO|-] [HI|-] | deltrig NAME | triggers
GraphQL:  createTrigger(name:, module:, lo:, hi:)  deleteTrigger(name:)
          triggers { name module lo{..} hi{..} mode pending lastError }
```

Trigger names follow module-name rules (`[A-Za-z0-9._-]`, max 64). The
module must exist at registration. One module may back many triggers.
Replacing a module's bytes does NOT change existing triggers' modes: a
changes-mode trigger whose module loses its `on_apply` export fails
drains loudly (`lastError`) and holds its events until the module is
repaired.

### Keys mode — the contract your `run` must satisfy

- **Input is packed touched keys**: `[klen uvarint][key bytes]` repeated —
  `fluent_guest::trigger_keys()` / `parse_trigger_keys()` parses it. Up
  to `trigger_batch` (default 512) keys per invocation.
- **An event means "this key was touched", not "here is what happened".**
  Events carry no values, no op kind, no order: re-touches of one key
  coalesce into a single pending event while a backlog exists. Read the
  key at your snapshot and reconcile: present → upsert your derived state,
  absent → remove it. Written this way your module is convergent — safe
  under replay, coalescing, and reordering.
- **Updates and deletes need a back-pointer.** The event doesn't tell you
  the OLD value, so you cannot find a stale index entry from the record
  alone. Maintain your own reverse mapping (e.g. `idx/order/<id>` →
  customer) and use it to unindex — see `guests/customer_index`.

### Changes mode — the post-apply change feed (`on_apply`)

Where keys mode answers "which keys need reconciling", changes mode
delivers **the list of changes that were committed**, in commit order —
what an audit log, event-sourced projection, or value-driven index
generator needs and coalescing would destroy. Your `on_apply` receives
(little-endian, `u32` lengths — wire-style framing):

```
[u32 count]
per change:
  [u64 seqno]      the op's commit seqno: unique, strictly increasing
  [u8  kind]       0 = put (value inline)  1 = delete  2 = put, value elided
  [u32 klen][key]
  [u32 vlen][value]        — kind 0 only
```

`fluent_guest::parse_changes()` / the `Vec<Change>` input of
`#[fluent_guest::on_apply]` decodes it. Per invocation: up to
`trigger_batch` changes, bounded by `max_wasm_input`.

- **One event per committed op, in commit order.** A key written three
  times yields three changes in exactly the order the engine applied
  them; the seqno is the op's real commit seqno (capture happens inside
  the commit critical section, so the feed can never disagree with the
  store about which write won a key). Two batches' changes never
  interleave out of order.
- **Values ride the event up to `trigger_inline_value`** (default
  64 KiB): above it the change arrives as kind 2 (key only) — read the
  key if you need the payload, remembering the read reflects CURRENT
  state, which may already be newer than the change. Inlining also costs
  write amplification on the watched range (the value is written twice);
  size the knob to your records.
- **Filter in code.** The range does the coarse cut; your module drops
  what it doesn't care about (see `guests/order_feed` skipping the
  counter key that shares its range) — the "post-apply filter".
- **Exactly-once, ordered effects**: derive idempotent output keys from
  the seqno (e.g. `feed/<seqno, zero-padded>`) and replays after a
  conflict or crash overwrite instead of duplicating.
- **Old values are still your job.** A delete/update carries no
  before-image; keep a back-pointer if you need to unindex, exactly as in
  keys mode.

### Delivery semantics (both modes)

- **Durable capture**: event records commit in the SAME atomic batch as
  the triggering write — a write that survives a crash fires its trigger
  after recovery; a write that doesn't, doesn't.
- **At-least-once invocation, exactly-once effects**: the consumed events
  are deleted inside your module's own transaction. A crash or conflict
  re-runs the whole attempt; your committed writes and the events'
  consumption are inseparable. Tolerate re-execution.
- **No stacking**: writes made by a trigger invocation never generate
  events — for any trigger, including its own. Trigger chains and loops
  are impossible; derive everything you need from the one event.
- **Async lag is real**: derived state trails the base data by the
  backlog. A failing module never loses events — the queue holds, the
  runner backs off (100ms → 6.4s), and `triggers { pending lastError }`
  shows both the depth and the reason. (Changes-mode queues are per-op,
  so a hot key grows the backlog where keys mode would coalesce it —
  the price of a complete feed.)
- Non-zero exits abort (nothing is consumed) and surface in
  `triggers { lastError }`.

Registered triggers, their queues, and their backlogs are engine state
(reserved keyspace): versioned, recovered, and checkpoint-archived like
everything else.
