# fluent wire v1 — binary protocol specification

The data-plane protocol for fluent31: correlated request/response frames
over TCP with **out-of-order completion**. GraphQL (`fluent-graphql`)
remains the general/typed/admin plane; this is the heat lane — raw bytes,
no encoding tax, stateless per request. Server: `fluent-wire` (default
`127.0.0.1:8427`). Reference client: `fluent_wire::WireClient`.

All integers are **little-endian**. Lengths inside payloads are `u32`.

## 1. Frame

Identical layout both directions; the ninth byte is an opcode on requests
and a status on responses.

```
[u32 frame_len]   length of everything AFTER this field (id + op + payload)
[u64 request_id]  see §2
[u8  opcode|status]
[payload…]        frame_len - 9 bytes
```

A server MAY answer requests in any order. A client MUST tolerate
responses in any order and correlate by `request_id`.

## 2. request_id

- Scope: **one TCP connection**. Two connections may use the same ids
  concurrently; the server never compares ids across connections.
- Allocation: by the client; a per-connection monotonic counter is
  conventional. The server echoes the id verbatim and never interprets it.
- A duplicate id while the first is still in flight is a client bug (the
  server will happily answer both; the client's pending-map wins/loses).
- `request_id = 0` is **reserved** for future server-initiated frames.
  Clients start at 1.
- The connection is also the failure domain: on disconnect, every
  in-flight request has **unknown outcome** (a write may have durably
  committed with its response undelivered). The server performs no
  cross-connection dedup — retry policy belongs to the client; for
  non-idempotent multi-key logic use an `EXEC` module that is itself
  idempotent (conditional writes inside the transaction).

## 3. Opcodes

| op | name | request payload | OK response payload |
|---|---|---|---|
| 0x00 | HELLO | (empty) | `[u32 protocol_version]["fluent31"]` |
| 0x01 | GET | key bytes | value bytes (or status NOT_FOUND, empty) |
| 0x02 | PUT | `[blob key][value…]` | (empty) |
| 0x03 | DEL | key bytes | (empty) |
| 0x04 | BATCH | `[u32 count]` then per op: `[u8 kind]` (0=put: `[blob key][blob value]`, 1=del: `[blob key]`) | `[u32 ops_applied]` — atomic all-or-nothing |
| 0x05 | SCAN | `[u8 flags][opt lo][opt hi][opt after][u32 limit]` | `[u32 count]` then per pair `[blob key][blob value]`, then `[u8 has_more][opt next_after]` |
| 0x06 | QUERY | `[blob module_name][input…]` | guest output bytes (read-only WASM, pinned snapshot) |
| 0x07 | EXEC | `[blob module_name][input…]` | guest output bytes (transactional WASM, OCC-retried) |
| 0x08 | SYNC_WAL | (empty) | (empty) — durability barrier |
| 0x09 | STATS | (empty) | UTF-8 debug text (**format-unstable**, human eyes only) |

`blob` = `[u32 len][bytes]`. `opt X` = `[u8 present]` then the field when
present=1. SCAN `flags`: bit 0 = reverse; other bits MUST be 0. SCAN is
**stateless**: `after` restarts strictly past that key in iteration order
(the page executes at the then-current visible state; there is no
server-side cursor). `limit` ∈ 1..=100000.

## 4. Status codes

| st | meaning | payload |
|---|---|---|
| 0x00 | OK | per-opcode |
| 0x01 | NOT_FOUND | empty (GET miss; not an error) |
| 0x02 | INVALID | UTF-8 message (bad key, engine arg rejection) |
| 0x03 | CONFLICT | UTF-8 message (OCC retries exhausted) |
| 0x04 | GUEST_FAILED | `[i32 exit_code][guest output…]` |
| 0x05 | BACKGROUND | UTF-8 message — store degraded, reopen required |
| 0x06 | CLOSED | UTF-8 message |
| 0x07 | IO | UTF-8 message |
| 0x08 | TOO_LARGE | UTF-8 message; **connection closes** after this frame |
| 0x09 | BAD_FRAME | UTF-8 message (unknown opcode / malformed payload) |
| 0x0a | CORRUPTION | UTF-8 message |
| 0x0b | WASM | UTF-8 message (compile error, trap, fuel exhaustion) |

## 5. Server behavior & flow control

- Per-connection read buffer is refcounted and capped; payloads larger
  than 256 KiB bypass it (exact-size allocation read straight from the
  socket). Frames above `max_frame` (default ≈257 MiB, sized to the
  engine's `max_value_size`) get TOO_LARGE and the connection closes.
- At most 128 requests execute concurrently per connection; past that the
  server stops reading the socket — backpressure reaches the client as
  TCP flow control. There is no unbounded queue anywhere.
- Responses are budgeted (64 MiB in flight per connection): a slow reader
  stalls its own requests, not the server.
- Requests run concurrently and responses are sent in completion order.
  Writes go through the engine's group commit — pipelining many small
  sync writes on one or more connections is the intended fast path.

## 6. Versioning

`HELLO` returns the protocol version (currently 1). Unknown opcodes get
BAD_FRAME (the connection survives). New opcodes/status codes are minor
additions; a frame-layout change would bump the version — clients SHOULD
send HELLO first and check.
