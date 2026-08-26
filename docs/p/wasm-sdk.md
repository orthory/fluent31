<!-- Generated from docs/index.html by scripts/build-agent-docs.py. Do not edit. -->

<!-- Section: Extending with WASM · Human version: https://orthory.github.io/fluent31/#wasm-sdk -->

# The guest SDK

> `fluent-guest` is the Rust crate you write modules against. It wraps the host ABI in safe functions, and its macros generate the exports.

## Crate setup

<!-- guests/<name>/Cargo.toml — add the crate to guests/Cargo.toml members -->
```
[package]
name = "my_module"
version = "0.1.0"
edition = "2021"

[lib]
crate-type = ["cdylib"]

[dependencies]
fluent-guest = { path = "../../crates/fluent-guest" }
serde_json = "1"      # optional; works on wasm32-unknown-unknown
```

```
cargo build --release --target wasm32-unknown-unknown
# → target/wasm32-unknown-unknown/release/my_module.wasm
```

There is no WASI, so nothing that needs an operating system links: no `std::time`, no `std::env`, no `rand`, no sockets or files. Entropy and time are inputs.

## Entry points

Put one attribute per role on a function of the shape `fn(T: FromInput) -> Result<O: IntoOutput, Fail>`. The macro generates the export, decodes the input and encodes the result: `Ok` becomes exit 0 with the encoded output, and `Err(Fail { code, message })` becomes a non-zero exit with the message in the output buffer.

```
use fluent_guest::{Change, Fail};

#[fluent_guest::query]     fn view(input: Vec<u8>)        -> Result<String, Fail> { .. }
#[fluent_guest::execute]   fn write(input: String)        -> Result<Vec<u8>, Fail> { .. }
#[fluent_guest::on_touch]  fn index(keys: Vec<Vec<u8>>)   -> Result<(), Fail>     { .. }
#[fluent_guest::on_apply]  fn feed(changes: Vec<Change>)  -> Result<(), Fail>     { .. }
fluent_guest::fluent_describe!(r#"{ ... }"#);   // optional typed surface
```

| Parameter type | Decoded from | Use with |
|---|---|---|
| `Vec<u8>` | the input blob, verbatim | `query`, `execute` |
| `String` | the input as UTF-8; invalid input fails with code 3 | `query`, `execute` |
| `Vec<Vec<u8>>` | the keys-mode trigger input | `on_touch` |
| `Vec<Change>` | the changes-mode trigger input | `on_apply` |

`IntoOutput` is implemented for `Vec<u8>`, `String` and `()`. `Fail` converts from `String` and `&str` with code 1, so `?` works on string errors, and `Fail::new(code, message)` sets the code deliberately. The annotated function must not be named after the export it generates — a `#[query]` function called `query` is a duplicate definition.

The `fluent_query!`, `fluent_execute!`, `fluent_on_touch!` and `fluent_on_apply!` macros are the declarative form of the same thing, for a module that would rather export a block than annotate a function. `fluent_describe!` has no attribute form: the descriptor is a static string.

## Data access

```
fluent_guest::get(&[u8]) -> Option<Vec<u8>>
fluent_guest::get_for_update(&[u8]) -> Result<Option<Vec<u8>>, i32>   // Err = errno
fluent_guest::put(&[u8], &[u8]) -> Result<(), i32>
fluent_guest::delete(&[u8]) -> Result<(), i32>

fluent_guest::scan(lo: Option<&[u8]>, hi: Option<&[u8]>) -> Result<Scan, i32>
fluent_guest::scan_rev(lo: Option<&[u8]>, hi: Option<&[u8]>) -> Result<Scan, i32>
fluent_guest::scan_prefix(prefix: &[u8]) -> Result<Scan, i32>
```

Reads see the invocation's snapshot, and in an executor the transaction's own buffered writes overlaid on top. `None` bounds are unbounded; the range is half-open, `[lo, hi)`. `get` fetches whole values — to read a value larger than guest memory, drop to [the raw ABI](wasm-abi.md), where `get` returns the full length and copies from an offset.

`Scan` is an `Iterator<Item = (Vec<u8>, Vec<u8>)>` that batches under the hood. One entry per iteration; a scan that hits an entry too large for its buffer stops, and `scan.skip_pending() -> bool` drops that entry so iteration can continue.

## Input, output and logging

```
fluent_guest::input() -> Vec<u8>    // the whole input blob
fluent_guest::output(&[u8])         // APPENDS to the output; call repeatedly to stream
fluent_guest::log(&str)             // stderr, only when FLUENT31_WASM_LOG is set
```

The entry macros call `input()` and `output()` for you; reach for them directly when the entry is written by hand or when the output is built in pieces. Logs are for debugging only — they are rate-capped and invisible unless the host asks for them, so results never travel that way.

## Trigger input

```
enum Change {
    Put    { seqno: u64, key: Vec<u8>, value: Option<Vec<u8>> },   // None = elided
    Delete { seqno: u64, key: Vec<u8> },
}
impl Change { fn seqno(&self) -> u64;  fn key(&self) -> &[u8]; }

fluent_guest::trigger_keys() -> Option<Vec<Vec<u8>>>   // keys mode, from input()
fluent_guest::changes()     -> Option<Vec<Change>>     // changes mode, from input()
fluent_guest::parse_trigger_keys(&[u8]) -> Option<Vec<Vec<u8>>>
fluent_guest::parse_changes(&[u8])      -> Option<Vec<Change>>
```

`value: None` means the value was above `trigger_inline_value` and was elided, not that it was empty — read the key if you need it, remembering that the read is current state and may be newer than the change in hand. `None` from the parsers means the input was not that shape, which is a programming error: the entry is bound to the wrong mode.

## Errnos

Every fallible host call returns one of these as a negative integer. `fluent_guest::errno` exports them as constants.

| Constant | Value | Means |
|---|---|---|
| `NOT_FOUND` | -1 | the key is absent |
| `EROFS` | -2 | a write, or `get_for_update`, inside a read-only query |
| `EINVAL` | -3 | a reserved, empty or oversized key, an oversized value, or bad scan flags |
| `ENOSPC` | -4 | an output, log or transaction write-set limit was reached |
| `EBADF` | -5 | a scan handle that is not open |
| `ELIMIT` | -6 | too many scan handles open at once |
| `EIO` | -8 | the engine failed; the invocation fails host-side regardless of the exit code |

## Authoring checklist

1. Pick the role or roles, and export the matching entries.
2. Define the keyspace. Validate anything from the input that becomes a key segment: non-empty, bounded length, no separator character.
3. `get_for_update` on every read-modify-write key.
4. Distinct exit codes per failure class, with the message in the output. Malformed state fails loudly.
5. Checked arithmetic everywhere a number is stored.
6. A static descriptor with prefixed type names, if the module should be API.
7. Build with `--release`, install, and confirm `typed: true, schemaError: null`.
8. Test the happy path, each failure exit, concurrency for executors, and a restart — the typed field must come back.

---

fluent31 docs · [index](../llms.txt) · [all pages](https://orthory.github.io/fluent31/llms.txt) · this page is `The guest SDK` in *Extending with WASM*
