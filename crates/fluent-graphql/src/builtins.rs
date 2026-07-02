//! Built-in root fields: direct KV operations, generic WASM access, and
//! module/checkpoint/maintenance admin.
//!
//! Every fallible field is nullable so a per-field failure stays
//! representable in the response (`errors` entry + null field) without
//! spec-invalid partial objects — sibling fields keep their data. Key/value
//! reads (`get`, `scan`, `wasm`, typed module queries, `snapshotSeqno`)
//! execute at the operation's single pinned MVCC snapshot; the admin
//! listings (`modules`, `checkpoints`, `stats`) read current state — module
//! metadata and archives are not MVCC-versioned views. Mutation fields run
//! serially in document order.

use async_graphql::dynamic::{
    Field, FieldFuture, FieldValue, InputValue, Object, ResolverContext, TypeRef,
};
use async_graphql::indexmap::IndexMap;
use async_graphql::{Error, Name, Value};
use fluent31::WriteBatch;

use crate::bytes::decode_bytes_input;
use crate::schema::{manager, pinned_snap, BytesVal, ScanPageVal};
use crate::ModuleStatus;

const DEFAULT_SCAN_LIMIT: i64 = 100;
const MAX_SCAN_LIMIT: i64 = 10_000;

/// Smallest key strictly greater than every key with this prefix, or None
/// when the prefix is all 0xFF (the range is unbounded above).
fn prefix_end(p: &[u8]) -> Option<Vec<u8>> {
    let mut end = p.to_vec();
    while let Some(last) = end.last_mut() {
        if *last == 0xFF {
            end.pop();
        } else {
            *last += 1;
            return Some(end);
        }
    }
    None
}

fn arg_bytes(ctx: &ResolverContext<'_>, name: &str) -> Result<Vec<u8>, Error> {
    decode_bytes_input(&ctx.args.try_get(name)?.object()?)
}

fn opt_arg_bytes(ctx: &ResolverContext<'_>, name: &str) -> Result<Option<Vec<u8>>, Error> {
    match ctx.args.get(name) {
        None => Ok(None),
        Some(v) if v.is_null() => Ok(None),
        Some(v) => Ok(Some(decode_bytes_input(&v.object()?)?)),
    }
}

fn arg_string(ctx: &ResolverContext<'_>, name: &str) -> Result<String, Error> {
    Ok(ctx.args.try_get(name)?.string()?.to_string())
}

fn obj(entries: Vec<(&str, Value)>) -> Value {
    Value::Object(
        entries
            .into_iter()
            .map(|(k, v)| (Name::new(k), v))
            .collect::<IndexMap<_, _>>(),
    )
}

fn checkpoint_value(c: fluent31::CheckpointInfo) -> Value {
    obj(vec![
        ("name", Value::String(c.name)),
        ("createdUnixMs", Value::String(c.created_unix_ms.to_string())),
        ("lastSeqno", Value::String(c.last_seqno.to_string())),
        ("path", Value::String(c.path.display().to_string())),
    ])
}

pub(crate) fn register(query: Object, mutation: Object) -> (Object, Object) {
    let query = query
        .field(
            Field::new("get", TypeRef::named("Bytes"), |ctx| {
                FieldFuture::new(async move {
                    let key = arg_bytes(&ctx, "key")?;
                    let mgr = manager(&ctx)?;
                    let snap = pinned_snap(&ctx, &mgr.db)?;
                    let db = mgr.db.clone();
                    Ok(mgr
                        .blocking_read(move || db.get_at(&key, &snap))
                        .await?
                        .map(|v| FieldValue::owned_any(BytesVal(v))))
                })
            })
            .argument(InputValue::new("key", TypeRef::named_nn("BytesInput")))
            .description("Point lookup at this operation's snapshot. Null when the key is absent."),
        )
        .field(
            Field::new("scan", TypeRef::named("ScanPage"), |ctx| {
                FieldFuture::new(async move {
                    let reverse = match ctx.args.get("reverse") {
                        Some(v) if !v.is_null() => v.boolean()?,
                        _ => false,
                    };
                    let limit = match ctx.args.get("limit") {
                        Some(v) if !v.is_null() => v.i64()?,
                        _ => DEFAULT_SCAN_LIMIT,
                    };
                    if !(1..=MAX_SCAN_LIMIT).contains(&limit) {
                        return Err(format!("limit must be in 1..={MAX_SCAN_LIMIT}").into());
                    }
                    let prefix = opt_arg_bytes(&ctx, "prefix")?;
                    let lo_arg = opt_arg_bytes(&ctx, "lo")?;
                    let hi_arg = opt_arg_bytes(&ctx, "hi")?;
                    let (mut lo, mut hi) = match prefix {
                        Some(p) => {
                            if lo_arg.is_some() || hi_arg.is_some() {
                                return Err("prefix cannot be combined with lo/hi".into());
                            }
                            let end = prefix_end(&p);
                            (Some(p), end)
                        }
                        None => (lo_arg, hi_arg),
                    };
                    // `after` restarts strictly past the cursor in iteration order
                    if let Some(a) = opt_arg_bytes(&ctx, "after")? {
                        if reverse {
                            hi = Some(match hi {
                                Some(h) if h < a => h,
                                _ => a,
                            });
                        } else {
                            let mut succ = a;
                            succ.push(0);
                            lo = Some(match lo {
                                Some(l) if l > succ => l,
                                _ => succ,
                            });
                        }
                    }
                    let mgr = manager(&ctx)?;
                    let snap = pinned_snap(&ctx, &mgr.db)?;
                    let db = mgr.db.clone();
                    let take = limit as usize;
                    let (pairs, has_more) = mgr
                        .blocking_read(move || {
                            let it =
                                db.iter_at(lo.as_deref(), hi.as_deref(), reverse, &snap)?;
                            let mut pairs: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
                            let mut has_more = false;
                            for item in it {
                                let (k, v) = item?;
                                if pairs.len() == take {
                                    has_more = true;
                                    break;
                                }
                                pairs.push((k, v));
                            }
                            Ok((pairs, has_more))
                        })
                        .await?;
                    let next_after = has_more
                        .then(|| pairs.last().map(|(k, _)| k.clone()))
                        .flatten();
                    Ok(Some(FieldValue::owned_any(ScanPageVal {
                        pairs,
                        has_more,
                        next_after,
                    })))
                })
            })
            .argument(InputValue::new("lo", TypeRef::named("BytesInput")))
            .argument(InputValue::new("hi", TypeRef::named("BytesInput")))
            .argument(InputValue::new("prefix", TypeRef::named("BytesInput")))
            .argument(InputValue::new("after", TypeRef::named("BytesInput")))
            .argument(InputValue::new("reverse", TypeRef::named(TypeRef::BOOLEAN)))
            .argument(InputValue::new("limit", TypeRef::named(TypeRef::INT)))
            .description(
                "Range scan over [lo, hi) — or over a key prefix — at this operation's \
                 snapshot, forward or reverse (default forward), paginated with limit \
                 (default 100, max 10000) plus the returned nextAfter cursor.",
            ),
        )
        .field(
            Field::new("wasm", TypeRef::named("Bytes"), |ctx| {
                FieldFuture::new(async move {
                    let module = arg_string(&ctx, "module")?;
                    let input = opt_arg_bytes(&ctx, "input")?.unwrap_or_default();
                    let mgr = manager(&ctx)?;
                    let snap = pinned_snap(&ctx, &mgr.db)?;
                    let db = mgr.db.clone();
                    let out = mgr
                        .blocking_read(move || db.query_at(&module, &input, &snap))
                        .await?;
                    Ok(Some(FieldValue::owned_any(BytesVal(out))))
                })
            })
            .argument(InputValue::new("module", TypeRef::named_nn(TypeRef::STRING)))
            .argument(InputValue::new("input", TypeRef::named("BytesInput")))
            .description(
                "Run any installed read-only WASM query module at this operation's \
                 snapshot: raw bytes in, raw bytes out. Typed modules additionally get \
                 their own root field.",
            ),
        )
        .field(
            Field::new("modules", TypeRef::named_nn_list("Module"), |ctx| {
                FieldFuture::new(async move {
                    let mgr = manager(&ctx)?;
                    let db = mgr.db.clone();
                    let infos = mgr.blocking_read(move || db.list_modules()).await?;
                    let statuses = mgr.statuses.read().unwrap();
                    let list: Vec<Value> = infos
                        .into_iter()
                        .map(|m| {
                            let (typed, err) = match statuses.get(&m.name) {
                                Some(ModuleStatus::Typed(_)) => (true, Value::Null),
                                Some(ModuleStatus::Invalid(e)) => {
                                    (false, Value::String(e.clone()))
                                }
                                // Untyped, or installed out-of-band since
                                // the last rebuild
                                _ => (false, Value::Null),
                            };
                            obj(vec![
                                ("name", Value::String(m.name)),
                                (
                                    "size",
                                    Value::Number(
                                        i64::try_from(m.size).unwrap_or(i64::MAX).into(),
                                    ),
                                ),
                                ("typed", Value::Boolean(typed)),
                                ("schemaError", err),
                            ])
                        })
                        .collect();
                    Ok(Some(FieldValue::value(Value::List(list))))
                })
            })
            .description("Installed WASM modules and their typed-schema status (current state, not snapshot-bound)."),
        )
        .field(
            Field::new("stats", TypeRef::named("Stats"), |ctx| {
                FieldFuture::new(async move {
                    let mgr = manager(&ctx)?;
                    let db = mgr.db.clone();
                    let s = mgr.blocking_read(move || Ok(db.stats())).await?;
                    let levels: Vec<Value> = s
                        .levels
                        .iter()
                        .map(|(runs, tables, bytes)| {
                            obj(vec![
                                (
                                    "runs",
                                    Value::Number(i64::try_from(*runs).unwrap_or(i64::MAX).into()),
                                ),
                                (
                                    "tables",
                                    Value::Number(
                                        i64::try_from(*tables).unwrap_or(i64::MAX).into(),
                                    ),
                                ),
                                ("bytes", Value::String(bytes.to_string())),
                            ])
                        })
                        .collect();
                    Ok(Some(FieldValue::value(obj(vec![
                        ("backend", Value::String(s.backend.to_string())),
                        ("visibleSeqno", Value::String(s.visible_seqno.to_string())),
                        (
                            "memtableBytes",
                            Value::String(s.memtable_bytes.to_string()),
                        ),
                        (
                            "immutableMemtables",
                            Value::Number(
                                i64::try_from(s.immutable_memtables).unwrap_or(i64::MAX).into(),
                            ),
                        ),
                        ("levels", Value::List(levels)),
                        (
                            "vlogFiles",
                            Value::Number(i64::try_from(s.vlog_files).unwrap_or(i64::MAX).into()),
                        ),
                        (
                            "vlogRetired",
                            Value::Number(
                                i64::try_from(s.vlog_retired).unwrap_or(i64::MAX).into(),
                            ),
                        ),
                        ("discardBytes", Value::String(s.discard_bytes.to_string())),
                        ("cacheHits", Value::String(s.cache_hits.to_string())),
                        ("cacheMisses", Value::String(s.cache_misses.to_string())),
                        ("commitGroups", Value::String(s.commit_groups.to_string())),
                        (
                            "commitBatches",
                            Value::String(s.commit_batches.to_string()),
                        ),
                        ("walSyncs", Value::String(s.wal_syncs.to_string())),
                    ]))))
                })
            })
            .description("Engine statistics (not snapshot-bound)."),
        )
        .field(
            Field::new("checkpoints", TypeRef::named_nn_list("Checkpoint"), |ctx| {
                FieldFuture::new(async move {
                    let mgr = manager(&ctx)?;
                    let db = mgr.db.clone();
                    let list = mgr.blocking_read(move || db.list_checkpoints()).await?;
                    Ok(Some(FieldValue::value(Value::List(
                        list.into_iter().map(checkpoint_value).collect(),
                    ))))
                })
            })
            .description("Point-in-time checkpoint archives (current state, not snapshot-bound)."),
        )
        .field(
            Field::new("snapshotSeqno", TypeRef::named("U64"), |ctx| {
                FieldFuture::new(async move {
                    let mgr = manager(&ctx)?;
                    let snap = pinned_snap(&ctx, &mgr.db)?;
                    Ok(Some(FieldValue::value(Value::String(
                        snap.seqno().to_string(),
                    ))))
                })
            })
            .description("The MVCC sequence number this query operation reads at."),
        );

    let mutation = mutation
        .field(
            Field::new("put", TypeRef::named(TypeRef::BOOLEAN), |ctx| {
                FieldFuture::new(async move {
                    let key = arg_bytes(&ctx, "key")?;
                    let value = arg_bytes(&ctx, "value")?;
                    let mgr = manager(&ctx)?;
                    let db = mgr.db.clone();
                    mgr.blocking_write(move || db.put(key, value)).await?;
                    Ok(Some(FieldValue::value(true)))
                })
            })
            .argument(InputValue::new("key", TypeRef::named_nn("BytesInput")))
            .argument(InputValue::new("value", TypeRef::named_nn("BytesInput")))
            .description("Insert or overwrite one key."),
        )
        .field(
            Field::new("delete", TypeRef::named(TypeRef::BOOLEAN), |ctx| {
                FieldFuture::new(async move {
                    let key = arg_bytes(&ctx, "key")?;
                    let mgr = manager(&ctx)?;
                    let db = mgr.db.clone();
                    mgr.blocking_write(move || db.delete(key)).await?;
                    Ok(Some(FieldValue::value(true)))
                })
            })
            .argument(InputValue::new("key", TypeRef::named_nn("BytesInput")))
            .description("Delete one key (succeeds whether or not it existed)."),
        )
        .field(
            Field::new("writeBatch", TypeRef::named(TypeRef::INT), |ctx| {
                FieldFuture::new(async move {
                    let ops_arg = ctx.args.try_get("ops")?;
                    // GraphQL input coercion: a single value is a 1-element list
                    let ops_list = ops_arg.list().ok();
                    let ops: Vec<_> = match &ops_list {
                        Some(l) => l.iter().collect(),
                        None => vec![ops_arg],
                    };
                    let mut batch = WriteBatch::new();
                    for op in ops {
                        let op = op.object()?;
                        if let Some(p) = op.get("put") {
                            let p = p.object()?;
                            batch.put(
                                decode_bytes_input(&p.try_get("key")?.object()?)?,
                                decode_bytes_input(&p.try_get("value")?.object()?)?,
                            );
                        } else if let Some(k) = op.get("delete") {
                            batch.delete(decode_bytes_input(&k.object()?)?);
                        } else {
                            return Err("WriteOp requires one of put/delete".into());
                        }
                    }
                    let n = i32::try_from(batch.len()).unwrap_or(i32::MAX);
                    let mgr = manager(&ctx)?;
                    let db = mgr.db.clone();
                    mgr.blocking_write(move || db.write(batch)).await?;
                    Ok(Some(FieldValue::value(n)))
                })
            })
            .argument(InputValue::new("ops", TypeRef::named_nn_list_nn("WriteOp")))
            .description(
                "Apply puts/deletes atomically — all-or-nothing in the WAL and memtable, \
                 one contiguous sequence-number range. Returns the number of operations \
                 applied.",
            ),
        )
        .field(
            Field::new("wasmExecute", TypeRef::named("Bytes"), |ctx| {
                FieldFuture::new(async move {
                    let module = arg_string(&ctx, "module")?;
                    let input = opt_arg_bytes(&ctx, "input")?.unwrap_or_default();
                    let mgr = manager(&ctx)?;
                    let db = mgr.db.clone();
                    let out = mgr
                        .blocking_write(move || db.execute(&module, &input))
                        .await?;
                    Ok(Some(FieldValue::owned_any(BytesVal(out))))
                })
            })
            .argument(InputValue::new("module", TypeRef::named_nn(TypeRef::STRING)))
            .argument(InputValue::new("input", TypeRef::named("BytesInput")))
            .description(
                "Run any installed WASM executor inside a transaction: raw bytes in/out, \
                 commit on guest exit 0, automatic retry on write conflicts. Typed \
                 executors additionally get their own root field.",
            ),
        )
        .field(
            Field::new("installModule", TypeRef::named("Module"), |ctx| {
                FieldFuture::new(async move {
                    let name = arg_string(&ctx, "name")?;
                    let wasm = arg_bytes(&ctx, "wasm")?;
                    let mgr = manager(&ctx)?;
                    // the whole install+rebuild runs in a spawned task: if
                    // the client disconnects mid-request, the task still
                    // completes, so a durable install can never be left
                    // without its schema rebuild
                    let task = tokio::spawn(install_task(mgr, name, wasm));
                    task.await
                        .map_err(|e| Error::new(format!("install task failed: {e}")))?
                        .map(|v| Some(FieldValue::value(v)))
                })
            })
            .argument(InputValue::new("name", TypeRef::named_nn(TypeRef::STRING)))
            .argument(InputValue::new("wasm", TypeRef::named_nn("BytesInput")))
            .description(
                "Install (or replace) a WASM module. Accepts a binary module (use base64) \
                 or WAT text (use text). A module exporting `describe` must present a \
                 valid schema (valid GraphQL name, no reserved/duplicate types) and then \
                 appears as its own typed root field; the schema is hot-swapped.",
            ),
        )
        .field(
            Field::new("uninstallModule", TypeRef::named(TypeRef::BOOLEAN), |ctx| {
                FieldFuture::new(async move {
                    let name = arg_string(&ctx, "name")?;
                    let mgr = manager(&ctx)?;
                    // spawned for the same cancellation-safety reason as
                    // installModule
                    let task = tokio::spawn(uninstall_task(mgr, name));
                    task.await
                        .map_err(|e| Error::new(format!("uninstall task failed: {e}")))?
                        .map(|()| Some(FieldValue::value(true)))
                })
            })
            .argument(InputValue::new("name", TypeRef::named_nn(TypeRef::STRING)))
            .description("Uninstall a WASM module; its typed root field (if any) is removed."),
        )
        .field(
            Field::new("reloadSchema", TypeRef::named(TypeRef::BOOLEAN), |ctx| {
                FieldFuture::new(async move {
                    let mgr = manager(&ctx)?;
                    let task = tokio::spawn(async move {
                        let _guard = mgr.rebuild_lock.lock().await;
                        let mgr2 = mgr.clone();
                        tokio::task::spawn_blocking(move || mgr2.rebuild())
                            .await
                            .map_err(|e| Error::new(format!("engine worker failed: {e}")))?
                            .map_err(crate::engine_err)
                    });
                    task.await
                        .map_err(|e| Error::new(format!("reload task failed: {e}")))?
                        .map(|()| Some(FieldValue::value(true)))
                })
            })
            .description(
                "Re-describe installed modules and hot-swap the schema. Recovers from a \
                 failed post-install rebuild and picks up modules installed outside this \
                 server (e.g. via the CLI).",
            ),
        )
        .field(
            Field::new("checkpoint", TypeRef::named("Checkpoint"), |ctx| {
                FieldFuture::new(async move {
                    let name = arg_string(&ctx, "name")?;
                    let mgr = manager(&ctx)?;
                    let db = mgr.db.clone();
                    let info = mgr.blocking_write(move || db.checkpoint(&name)).await?;
                    Ok(Some(FieldValue::value(checkpoint_value(info))))
                })
            })
            .argument(InputValue::new("name", TypeRef::named_nn(TypeRef::STRING)))
            .description("Create a named point-in-time checkpoint archive."),
        )
        .field(
            Field::new("deleteCheckpoint", TypeRef::named(TypeRef::BOOLEAN), |ctx| {
                FieldFuture::new(async move {
                    let name = arg_string(&ctx, "name")?;
                    let mgr = manager(&ctx)?;
                    let db = mgr.db.clone();
                    mgr.blocking_write(move || db.delete_checkpoint(&name)).await?;
                    Ok(Some(FieldValue::value(true)))
                })
            })
            .argument(InputValue::new("name", TypeRef::named_nn(TypeRef::STRING)))
            .description("Delete a checkpoint archive."),
        )
        .field(
            Field::new("syncWal", TypeRef::named(TypeRef::BOOLEAN), |ctx| {
                FieldFuture::new(async move {
                    let mgr = manager(&ctx)?;
                    let db = mgr.db.clone();
                    mgr.blocking_write(move || db.sync_wal()).await?;
                    Ok(Some(FieldValue::value(true)))
                })
            })
            .description(
                "Durability barrier: every write acked before this call is durable on \
                 return. The explicit companion to running the server with --sync \
                 periodic:<ms>.",
            ),
        )
        .field(
            Field::new("flush", TypeRef::named(TypeRef::BOOLEAN), |ctx| {
                FieldFuture::new(async move {
                    let mgr = manager(&ctx)?;
                    let db = mgr.db.clone();
                    mgr.blocking_write(move || db.flush()).await?;
                    Ok(Some(FieldValue::value(true)))
                })
            })
            .description("Freeze the active memtable and wait until everything is in tables."),
        )
        .field(
            Field::new("compactAll", TypeRef::named(TypeRef::BOOLEAN), |ctx| {
                FieldFuture::new(async move {
                    let mgr = manager(&ctx)?;
                    let db = mgr.db.clone();
                    mgr.blocking_write(move || db.compact_all()).await?;
                    Ok(Some(FieldValue::value(true)))
                })
            })
            .description("Run compaction until no trigger fires."),
        )
        .field(
            Field::new("gcVlog", TypeRef::named("GcResult"), |ctx| {
                FieldFuture::new(async move {
                    let mgr = manager(&ctx)?;
                    let db = mgr.db.clone();
                    let retired = mgr.blocking_write(move || db.gc_vlog()).await?;
                    Ok(Some(FieldValue::value(obj(vec![(
                        "retired",
                        retired
                            .map(|id| Value::String(id.to_string()))
                            .unwrap_or(Value::Null),
                    )]))))
                })
            })
            .description("One value-log GC pass."),
        );

    (query, mutation)
}

/// The uncancellable body of `installModule`: descriptor pre-check,
/// durable install, schema rebuild — serialized under `rebuild_lock`.
async fn install_task(
    mgr: std::sync::Arc<crate::SchemaManager>,
    name: String,
    wasm: Vec<u8>,
) -> Result<Value, Error> {
    let size = i32::try_from(wasm.len()).unwrap_or(i32::MAX);
    let _guard = mgr.rebuild_lock.lock().await;

    // typed-schema gate: if the candidate module describes itself, its
    // descriptor must validate BEFORE install
    let db = mgr.db.clone();
    let probe = wasm.clone();
    let described = mgr.blocking_write(move || db.describe_wasm(&probe)).await?;
    let mut typed = false;
    if let Some(bytes) = described {
        let parsed = crate::descriptor::parse_descriptor(&name, &bytes)
            .map_err(|e| Error::new(format!("invalid module schema: {e}")))?;
        let claimed = crate::schema::claimed_types_except(&mgr.statuses.read().unwrap(), &name);
        if let Some(t) = parsed.type_names().find(|t| claimed.contains(*t)) {
            return Err(Error::new(format!(
                "invalid module schema: type {t:?} is already declared by another module"
            )));
        }
        typed = true;
    }

    let db = mgr.db.clone();
    let stored = name.clone();
    let bytes = wasm;
    mgr.blocking_write(move || db.install_module(&stored, &bytes))
        .await?;

    // the module is durably installed from here on: a rebuild failure must
    // not read as "install failed" — report it and leave reloadSchema as
    // the resync path
    let mut schema_error = Value::Null;
    let mgr2 = mgr.clone();
    let rebuilt = tokio::task::spawn_blocking(move || mgr2.rebuild())
        .await
        .map_err(|e| Error::new(format!("engine worker failed: {e}")))?;
    match rebuilt {
        Ok(()) => {
            // report post-rebuild status (covers cross-module edge cases
            // the pre-check could not see)
            if let Some(ModuleStatus::Invalid(e)) = mgr.statuses.read().unwrap().get(&name) {
                typed = false;
                schema_error = Value::String(e.clone());
            }
        }
        Err(e) => {
            typed = false;
            schema_error = Value::String(format!(
                "module installed, but the schema rebuild failed ({e}); run reloadSchema"
            ));
        }
    }
    Ok(obj(vec![
        ("name", Value::String(name)),
        ("size", Value::Number(size.into())),
        ("typed", Value::Boolean(typed)),
        ("schemaError", schema_error),
    ]))
}

/// The uncancellable body of `uninstallModule`.
async fn uninstall_task(
    mgr: std::sync::Arc<crate::SchemaManager>,
    name: String,
) -> Result<(), Error> {
    let _guard = mgr.rebuild_lock.lock().await;
    let db = mgr.db.clone();
    mgr.blocking_write(move || db.uninstall_module(&name)).await?;
    let mgr2 = mgr.clone();
    tokio::task::spawn_blocking(move || mgr2.rebuild())
        .await
        .map_err(|e| Error::new(format!("engine worker failed: {e}")))?
        .map_err(|e| {
            Error::new(format!(
                "module uninstalled, but the schema rebuild failed ({e}); run reloadSchema"
            ))
        })
}

#[cfg(test)]
mod tests {
    use super::prefix_end;

    #[test]
    fn plain_prefix_increments_last_byte() {
        assert_eq!(prefix_end(b"scan/"), Some(b"scan0".to_vec()));
        assert_eq!(prefix_end(&[0xab, 0x01]), Some(vec![0xab, 0x02]));
    }

    #[test]
    fn trailing_ff_carries_into_earlier_byte() {
        assert_eq!(prefix_end(&[0xab, 0xff]), Some(vec![0xac]));
        assert_eq!(prefix_end(&[0xab, 0xff, 0xff]), Some(vec![0xac]));
        assert_eq!(prefix_end(&[0x01, 0xfe, 0xff]), Some(vec![0x01, 0xff]));
    }

    #[test]
    fn all_ff_prefix_is_unbounded_above() {
        assert_eq!(prefix_end(&[0xff]), None);
        assert_eq!(prefix_end(&[0xff, 0xff, 0xff]), None);
        assert_eq!(prefix_end(b""), None);
    }
}
