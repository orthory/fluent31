<!-- Generated from docs/index.html by scripts/build-agent-docs.py. Do not edit. -->

<!-- Section: Extending with WASM · Human version: https://orthory.github.io/fluent31/#wasm-abi -->

# The host ABI

> The raw interface between the engine and a guest: thirteen imported functions and a handful of exports. [The guest SDK](wasm-sdk.md) wraps all of it — this page is what you need to write a module in another language, to hand-write WAT, or to reason about a limit precisely.

## Conventions

- All pointers and lengths are `u32` passed as wasm `i32`. Out-of-range memory access **traps**, and the invocation fails with `Error::Wasm`; semantic misuse returns an errno instead.
- Errnos are negative return values, in `i32` or `i64`: `NOT_FOUND -1`, `EROFS -2`, `EINVAL -3`, `ENOSPC -4`, `EBADF -5`, `ELIMIT -6`, `EIO -8`.
- Keys beginning with byte `0x00` are the engine's reserved keyspace. Reads and writes there return `EINVAL`; scans are silently clamped to the user keyspace. Empty keys are `EINVAL`.
- Entries take no parameters and return an `i32` exit code. The guest must export `memory` so the host can read and write the buffers it is given.

## Input and output

```
input_len  : () -> i32
input_read : (dst: i32, cap: i32, off: i32) -> i32
```

The invocation's input blob. `input_read` copies up to `cap` bytes starting at input offset `off` into guest memory at `dst`, and returns the number of bytes copied. Large inputs are read in as many passes as the guest has room for.

```
output_write : (ptr: i32, len: i32) -> i32
```

**Appends** `len` bytes to the invocation's output. Returns `0`, or `ENOSPC` once the total would exceed `max_wasm_output`. Check the return value wherever truncated output would be a correctness bug rather than a cosmetic one.

```
log : (level: i32, ptr: i32, len: i32) -> i32
```

Debug logging, capped at `max_wasm_log` total bytes and then `ENOSPC`. The host emits each line as a `debug` event under the `fluent31::wasm::guest` target (`RUST_LOG=fluent31::wasm::guest=debug` to see them). Never use logs to communicate results.

## Point access

```
get            : (kptr, klen, off, vbuf, vcap: i32) -> i64
get_for_update : (kptr, klen, off, vbuf, vcap: i32) -> i64
```

A point lookup at this invocation's snapshot, with an executor's own buffered writes overlaid. Both return the **full** value length as a non-negative `i64` and copy `min(vcap, len - off)` bytes from value offset `off` into `vbuf` — so a value larger than guest memory is read by calling again with a larger buffer or an advancing `off`. `NOT_FOUND` if the key is absent.

`get_for_update` additionally adds the key to the transaction's read set, which is what makes first-committer-wins apply to it; use it for every read-modify-write. In a read-only query it returns `EROFS`.

```
put    : (kptr, klen, vptr, vlen: i32) -> i32
delete : (kptr, klen: i32) -> i32
```

Buffer a write in the transaction. `EROFS` in query mode. `EINVAL` for a reserved, empty or oversized key (`max_key_size`, 16 KiB) or an oversized value (`max_value_size`, 256 MiB). `ENOSPC` once the transaction's write set would exceed `max_txn_write_bytes`. Deleting an absent key succeeds.

## Scans

```
scan_open : (lo_ptr, lo_len, hi_ptr, hi_len, flags: i32) -> i32
```

Opens an iterator over `[lo, hi)` at the snapshot. A zero-length `lo` or `hi` means unbounded on that side. `flags` bit 0 selects reverse order; every other bit is `EINVAL`. Returns a handle (≥ 0), or `ELIMIT` past `max_wasm_scans` concurrently open handles. Handles are per-invocation and never survive the entry returning.

```
scan_next : (h: i32, buf: i32, cap: i32) -> i32
```

Fills `buf` with as many whole entries as fit in `cap`, subject to a host-side batch ceiling of 16 MiB. Each entry is packed as:

```
[klen uvarint][vlen uvarint][key bytes][value bytes]
```

Returns the number of bytes written; `0` means the range is exhausted. `ENOSPC` means the *next single entry* does not fit in `cap` — grow the buffer, or ask how big it is and decide:

```
scan_entry_hint : (h: i32) -> i64   // packed size of the next entry; 0 at the end
scan_skip       : (h: i32) -> i32   // drop the next entry; 1 if skipped, 0 at the end
scan_close      : (h: i32) -> i32   // free the handle
```

`EBADF` is a handle that is not open; `EIO` is an engine error. The SDK's `Scan` iterator is exactly this loop, with `skip_pending()` exposing `scan_skip`.

## Limits

Every one of these is an engine `Options` field, so they are the operator's to set — the defaults are what an unconfigured store uses. These are the per-invocation budgets, reset for every call and, for an executor, for every retry of it:

| Option | Default | On breach |
|---|---|---|
| `wasm_fuel` | 1,000,000,000 | trap → `Error::Wasm`; this is what bounds an infinite loop |
| `wasm_memory_limit` | 64 MiB | `memory.grow` fails |
| `max_wasm_input` | 64 MiB | `InvalidArgument`, before the module runs |
| `max_wasm_output` | 32 MiB | `output_write` returns `ENOSPC` |
| `max_wasm_log` | 1 MiB | `log` returns `ENOSPC` |
| `max_wasm_scans` | 64 open handles | `scan_open` returns `ELIMIT` |
| `max_txn_write_bytes` | 256 MiB | `put` returns `ENOSPC` once the transaction's buffered writes would exceed it |

Two more bound any single write rather than the invocation, and apply to every writer on every surface: a key is at most `max_key_size` (16 KiB) and a value at most `max_value_size` (256 MiB). `put` returns `EINVAL` past either.

> **Spec.** [WASM.md](https://github.com/orthory/fluent31/blob/master/WASM.md) in the repository is the normative version of this page.

---

fluent31 docs · [index](../llms.txt) · [all pages](https://orthory.github.io/fluent31/llms.txt) · this page is `The host ABI` in *Extending with WASM*
