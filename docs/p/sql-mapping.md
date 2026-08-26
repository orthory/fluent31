<!-- Generated from docs/index.html by scripts/build-agent-docs.py. Do not edit. -->

<!-- Section: Coming from SQL · Human version: https://orthory.github.io/fluent31/#sql-mapping -->

# Coming from SQL

> A complete translation of the relational vocabulary into fluent31: what each construct becomes, what it costs, and what has no equivalent at all.

Three things replace one language. **Key layout** takes the place of the schema: a record is bytes under a key you chose, and the order keys sort in is the only order the engine knows. **Modules** take the place of the query language: code installed in the database, invoked by name, running next to the data. **Triggers** take the place of the machinery that keeps derived data current: indexes, materialized views and cascades maintain themselves after every commit.

What is missing is the planner. Nothing chooses an access path for you — the access path *is* the range you scan and the order you scan it in, so the modelling decisions below are load-bearing in a way they are not in SQL.

## Data modelling

| SQL | fluent31 |
|---|---|
| `CREATE TABLE orders (…)` | Nothing to declare. Choose a prefix — `orders/` — and start writing. There is no DDL and no catalog of tables. |
| a row | one key/value pair |
| `PRIMARY KEY` | the key itself; uniqueness is intrinsic, because a key holds one value |
| composite primary key | compose the segments: `order/<customer>/<id>`. Order the segments by how you intend to scan — the leftmost segment is the only one a prefix scan can pin. |
| a column | a field inside the value. The engine never parses a value; JSON is the usual choice, a fixed binary layout the fast one. |
| column types | your encoding. Keys sort bytewise, so a number in a *key* must be zero-padded decimal or fixed-width big-endian; values are unconstrained. |
| `NULL` | an absent field inside the value, or an absent key. Note the two are distinguishable: `get` returns `None` for a missing key and `Some([])` for a key holding an empty value. |
| `DEFAULT` | filled in by the executor that writes the record. There is no engine-side default, and `DEFAULT now()` has no equivalent at all: modules have no clock, so a timestamp must be passed in by the caller. |
| `AUTO_INCREMENT`, `SEQUENCE` | a counter key read with `get_for_update` and written inside the same executor. That yields dense, gap-free ids under concurrency, at the cost of serialising writers on one key. Where gaps are acceptable, the commit seqno is free. |
| partitioning, tablespaces | key prefixes. A range is a partition; nothing needs declaring. |

The one modelling trap worth stating outright: bytewise order is not numeric order. `orders/10` sorts before `orders/9`. Zero-pad to a fixed width (`orders/00000009`) and the two agree.

## Reading

| SQL | fluent31 |
|---|---|
| `SELECT * FROM t WHERE pk = 'x'` | `db.get(b"t/x")` |
| `WHERE pk LIKE 'p%'` | a scan of `[p, p+1)` — `p` with its last byte incremented |
| `WHERE pk BETWEEN a AND b` | a scan of `[a, b)`. The upper bound is exclusive; append `0x00` to `b` to include it. |
| `ORDER BY pk` | intrinsic — a scan is already in key order |
| `ORDER BY pk DESC` | a reverse scan (`db.iter(lo, hi, true)`, `scan --rev`, `scan(reverse: true)`) |
| `ORDER BY some_column` | Not available directly: the engine can only order by key. Maintain an index keyed by that value with a trigger, then scan the index range — which *is* the ordered read. |
| `LIMIT n` | bound the scan: `.take(n)` in Rust, `--limit` in the shell, `limit` over GraphQL (default 100, maximum 10000) |
| `OFFSET n` | No offset exists. Page by cursor: resume the next scan at the last key with `0x00` appended, or pass GraphQL's `nextAfter` back as `after`. Pages of one logical read should share a snapshot. |
| `COUNT(*)`, `SUM`, `MIN`, `MAX` | a query module folding one scan — cost proportional to the range. For a constant-time count, maintain a counter with a changes-mode trigger and read the counter. |
| `DISTINCT` | one key per distinct value (an index by that value), so the distinct set is a scan; otherwise dedupe inside the module |
| `HAVING` | a filter inside the module, applied after the fold |
| `JOIN` | No join operator and no join planner. Three shapes, chosen deliberately: denormalise at write time in the executor that writes the record; look both sides up inside one module, which reads them at a single snapshot; or scan a secondary index and `get` each hit. The join order is the code you wrote. |
| subqueries, CTEs | ordinary control flow inside a module |
| `SELECT … FOR UPDATE` | `get_for_update` — it adds the key to the transaction's conflict set rather than taking a lock |
| projection (`SELECT a, b`) | the module returns only the fields it wants; over GraphQL a typed module's output fields are selected normally |
| `EXPLAIN` | Nothing to explain: there is no planner, no statistics and no index selection. The plan is the range you scan. |
| an ad-hoc query | module bytes run without being installed — `queryonce FILE.wasm` in the shell, `wasmOnce` over GraphQL, `db.query_wasm` in Rust — or a plain scan filtered client-side |

<!-- the four reads, end to end -->
```
let one = db.get(b"orders/00000042")?;                          // by primary key
let page = db.iter(Some(b"orders/"), Some(b"orders0"), false)?;   // prefix, ascending
let newest = db.iter(Some(b"orders/"), Some(b"orders0"), true)?     // ORDER BY pk DESC LIMIT 10
    .take(10);
let mut next = last_key.to_vec(); next.push(0);                  // resume after last_key
let more = db.iter(Some(&next), Some(b"orders0"), false)?;
```

## Writing

| SQL | fluent31 |
|---|---|
| `INSERT` | `put` |
| `UPDATE` | `put`. There is no distinction: a put writes the key whether or not it existed, so every write is an upsert. |
| `INSERT … ON CONFLICT DO NOTHING` / `DO UPDATE` | an executor: `get_for_update` the key, then decide. The conditional part is code, and the conflict set makes the decision safe under concurrency. |
| `UPDATE … WHERE <range>` | an executor that scans the range and writes each match — one transaction, bounded by `max_txn_write_bytes`. Past that bound, shard by cursor and drive the loop from the caller. |
| `DELETE` | `delete`, which succeeds whether or not the key exists |
| `DELETE … WHERE <range>` | scan the range, then a delete batch or an executor |
| multi-row `INSERT` | a `WriteBatch`: atomic, one contiguous seqno range, visible all at once |
| `RETURNING` | the executor's output — it returns whatever it writes to its output buffer |
| `TRUNCATE` | scan the prefix and delete it, or start from a fresh store directory |

## Transactions and locking

| SQL | fluent31 |
|---|---|
| `BEGIN` / `COMMIT` / `ROLLBACK` | `db.begin()` / `txn.commit()` / `txn.rollback()`, or an `execute` module, which is one transaction per attempt |
| isolation levels | One level: snapshot isolation with first-committer-wins. There is nothing to configure and no read-uncommitted, read-committed or serializable variant to choose between. |
| row locks, `FOR UPDATE`, `FOR SHARE` | `get_for_update` records the key in the conflict set. No lock is taken, so readers never block and writers never queue. |
| deadlock | Impossible — there are no locks to cycle. The failure mode is `Error::Conflict` at commit, with nothing written; the fix is to re-run the whole read-modify-write. |
| lock timeout, `NOWAIT` | no equivalent and none needed |
| `SAVEPOINT`, nested transactions | none |
| a long-running transaction | holds a snapshot, and a snapshot holds the GC watermark for the whole store. Keep transactions short for that reason rather than for lock contention. |
| a multi-statement transaction over the wire | A GraphQL document is **not** a transaction: each mutation field is its own atomic write, executed in document order. Anything that must land together belongs in one executor. |
| autocommit | every `put`, `delete` and `write` is atomic on its own |

## Constraints

| SQL | fluent31 |
|---|---|
| `PRIMARY KEY` | intrinsic to the key |
| `UNIQUE` on another field | an executor that holds the uniqueness key: `get_for_update` on `uname/<value>`, refuse if it is taken, write it if it is free. Concurrent claimants race through the conflict loop and exactly one wins. |
| `NOT NULL`, `CHECK` | validation inside the executor that writes the record. The engine validates nothing about a value's contents. |
| `FOREIGN KEY` | not enforced by the engine; the executor reads the parent key and refuses when it is absent |
| `ON DELETE CASCADE` | a changes-mode trigger over the parent range that sweeps the child subtree when it sees a delete |
| `ON UPDATE CASCADE` | the same shape, folding the new value forward |
| deferred constraints | no equivalent |

One difference matters more than the table can show. In SQL a constraint is enforced by the engine against every writer. Here a constraint holds only because every writer goes through the executor that checks it: a raw `put` to the same key bypasses it, and a trigger cannot help, because triggers run *after* the commit and cannot veto a write. Treat the executor as the write API for constrained data, and keep raw puts for the ranges that carry no invariant.

## Indexes

| SQL | fluent31 |
|---|---|
| `CREATE INDEX ON t (col)` | a keys-mode trigger over `t/` writing `idx/col/<value>/<id>`, plus a back-pointer `idx/t/<id>` recording what it last indexed. The lookup is a prefix scan of the index range. |
| composite index | compose the index key's value segments in the order you will scan them |
| covering index | store the projected fields as the index entry's value instead of an empty one; the scan then answers without a second read |
| partial index | a filter in the trigger module — index only what qualifies |
| unique index | Uniqueness comes from the executor, not the index. An index built by a trigger is maintained after the fact and cannot reject anything. |
| `DROP INDEX` | delete the trigger, then delete the index range |
| `REINDEX`, index creation on existing data | Registering a trigger does not backfill: keys already in the range fire no events. Either have the module scan and build on demand when its spec key is written, or re-put the range with a one-shot executor to generate the events. |
| full-text index | no built-in; a trigger that writes one key per term is the same shape as any other index |
| index-only scan | scanning the index range is exactly that |
| index selection by the planner | you select it, by choosing which range to scan |

## Views and aggregates

| SQL | fluent31 |
|---|---|
| `CREATE VIEW` | a query module: computed per call, at the caller's snapshot |
| `CREATE MATERIALIZED VIEW` | a changes-mode trigger writing the view's keys as the base data changes |
| `REFRESH MATERIALIZED VIEW` | Never needed. The fold happens once per committed change, atomically with consuming the event, so the view cannot drift. |
| `GROUP BY` answered on demand | a query module aggregating the range at read time |
| `GROUP BY` kept current | a changes-mode trigger folding each change's delta into the group's totals, with a back-pointer per record so updates and deletes subtract what that record last contributed |
| window functions, rollups | code in a query module, or a trigger-maintained table if the result must be current |

## Procedures, triggers, notifications

| SQL | fluent31 |
|---|---|
| stored procedure, user-defined function | an `execute` or `query` module, callable by name from Rust, the shell and GraphQL. A module that describes itself also becomes a typed GraphQL field. |
| `AFTER INSERT/UPDATE/DELETE … FOR EACH ROW` | a trigger bound to a key range. Keys mode delivers coalesced touched keys ("reconcile this"); changes mode delivers every op in order with its value. |
| `BEFORE` / `INSTEAD OF` triggers | No equivalent, deliberately. Triggers fire after the commit and cannot alter or reject the write. Anything that must reject belongs in the executor. |
| statement-level triggers | a drain hands the module a batch of events at once (up to `trigger_batch`) |
| trigger ordering, recursion depth | No ordering between triggers, and no recursion at all: writes made by a trigger never generate events. |
| `LISTEN` / `NOTIFY` | the change stream — `db.subscribe` in Rust, a `changes` subscription over GraphQL, or a typed feed declared by a module |
| logical decoding, CDC | a changes-mode trigger materialising a feed. History is then a scan of the feed range and live is a subscription to its tail. |

## Schema changes and migrations

| SQL | fluent31 |
|---|---|
| `CREATE` / `DROP TABLE` | nothing; a prefix needs no declaration, and dropping is deleting a range |
| `ALTER TABLE ADD COLUMN` | Write the field on new records. Readers treat it as absent on old ones, or you migrate. There is no table-wide rewrite to schedule and no lock to hold. |
| `ALTER COLUMN TYPE` | a migration that rewrites the affected records |
| a migration script | a one-shot executor: idempotent by inspection (detect an already-migrated record and skip it), one atomic transaction, sharded by cursor when the write set exceeds the transaction cap |
| testing a migration on a copy | fork the store and run the migration against the fork — under the server it is a full instance at its own endpoint |
| schema version table | a version field in the record, which is what makes the migration idempotent |

## Operations

| SQL | fluent31 |
|---|---|
| `pg_dump`, a snapshot backup | `fork(name)`: a complete, consistent copy of the database at hard-link cost. Copy the fork directory off-box at leisure. |
| point-in-time recovery | Not available. Forks and pins are named cuts, not a continuous archive; recovery lands on a cut you took deliberately. |
| WAL archiving | the journal: opt-in, off the commit path, and the source a fresh store is rebuilt from when the store directory is lost |
| read replicas | read-only replicas and key-range edge caches attached to a named master |
| `VACUUM` | compaction and value-log GC, running continuously on their own threads; the manual calls exist for tests and for reclaiming now |
| `ANALYZE`, planner statistics | nothing to collect — there is no planner |
| `information_schema`, `\dt` | the `modules`, `triggers`, `forks`, `pins` and `stats` fields |
| `max_connections`, a connection pool | One process holds the store directory (an exclusive lock); server mode is how the planes share that one handle. Concurrency is bounded per plane rather than per connection. |
| `GRANT`, roles, row-level security | None. Authentication and authorization are a layer in front — a reverse proxy for GraphQL, a network boundary for replication. |
| `pg_stat_activity`, slow query log | `stats` for the engine's shape and cache behaviour, `triggers { pending lastError }` for derived-data lag |

## What has no equivalent

Stated plainly, so the gaps are found here rather than late:

- **Ad-hoc joins chosen by a planner.** Every access path is written by hand, in a module or at the call site.
- **Engine-enforced constraints.** Foreign keys, `CHECK`, `NOT NULL` and column types hold only as far as the executor that checks them.
- **Write vetoes.** There is no `BEFORE` trigger and no rule system; nothing can reject a write after it has been made.
- **Point-in-time recovery.** Recovery targets a named cut, not an arbitrary instant.
- **Multi-node writes.** One process owns the store; replicas are read-only followers, and there are no distributed transactions.
- **`OFFSET` paging, and ordering by a value you have not indexed.** Both require an index you maintain, or a full scan.
- **The SQL wire protocol.** No `psql`, no JDBC or ODBC drivers; access is the embedded API, the shell, or GraphQL.
- **Built-in full-text, geospatial and window-function libraries.** They are code you write, or data you maintain.

---

fluent31 docs · [index](../llms.txt) · [all pages](https://orthory.github.io/fluent31/llms.txt) · this page is `Translation guide` in *Coming from SQL*
