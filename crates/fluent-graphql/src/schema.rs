//! Dynamic schema assembly: shared scalars/objects, the two roots
//! (populated by `builtins.rs` and `modules.rs`), and the per-rebuild
//! module status collection.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Weak};

use async_graphql::dynamic::{
    Field, FieldFuture, FieldValue, InputObject, InputValue, Object, ResolverContext, Scalar,
    Schema, TypeRef,
};
use async_graphql::{Error, Value};
use fluent31::Db;

use crate::bytes::{encode_b64, encode_hex};
use crate::descriptor;
use crate::{DescribeOutcome, ModuleStatus, SchemaManager};

/// Raw bytes flowing to `Bytes` field resolvers.
pub(crate) struct BytesVal(pub Vec<u8>);

/// Parent value for `ScanPage`.
pub(crate) struct ScanPageVal {
    pub pairs: Vec<(Vec<u8>, Vec<u8>)>,
    pub has_more: bool,
    pub next_after: Option<Vec<u8>>,
}

// ---------------------------------------------------------------------------
// resolver plumbing shared by builtins.rs / modules.rs
// ---------------------------------------------------------------------------

pub(crate) fn manager(ctx: &ResolverContext<'_>) -> Result<Arc<SchemaManager>, Error> {
    ctx.data::<Weak<SchemaManager>>()?
        .upgrade()
        .ok_or_else(|| Error::new("schema manager shut down"))
}

pub(crate) fn pinned_snap(
    ctx: &ResolverContext<'_>,
    db: &Db,
) -> Result<Arc<fluent31::Snapshot>, Error> {
    Ok(ctx.data::<crate::SnapCell>()?.pin(db))
}

impl SchemaManager {
    pub(crate) async fn blocking_read<T, F>(&self, f: F) -> Result<T, Error>
    where
        F: FnOnce() -> fluent31::Result<T> + Send + 'static,
        T: Send + 'static,
    {
        self.blocking(&self.permits.read, f).await
    }

    pub(crate) async fn blocking_write<T, F>(&self, f: F) -> Result<T, Error>
    where
        F: FnOnce() -> fluent31::Result<T> + Send + 'static,
        T: Send + 'static,
    {
        self.blocking(&self.permits.write, f).await
    }

    async fn blocking<T, F>(&self, sem: &Arc<tokio::sync::Semaphore>, f: F) -> Result<T, Error>
    where
        F: FnOnce() -> fluent31::Result<T> + Send + 'static,
        T: Send + 'static,
    {
        let permit = sem
            .clone()
            .acquire_owned()
            .await
            .map_err(|e| Error::new(format!("engine gate closed: {e}")))?;
        // the permit moves INTO the blocking closure: if the request is
        // cancelled mid-call the detached engine work keeps its pool slot
        // accounted for until it actually finishes
        match tokio::task::spawn_blocking(move || {
            let _permit = permit;
            f()
        })
        .await
        {
            Ok(r) => r.map_err(crate::engine_err),
            Err(e) => Err(Error::new(format!("engine worker failed: {e}"))),
        }
    }
}

/// A field of a Value-object parent: looks itself up in the parent's
/// `Value::Object`. Lets whole output trees (Stats, Module, typed
/// module outputs) be produced as one `Value` with no per-type resolvers.
pub(crate) fn value_field(name: &str, ty: TypeRef) -> Field {
    let key = async_graphql::Name::new(name);
    Field::new(name.to_string(), ty, move |ctx: ResolverContext<'_>| {
        let key = key.clone();
        FieldFuture::new(async move {
            let parent = ctx.parent_value.try_to_value()?;
            let Value::Object(map) = parent else {
                return Err(Error::new("internal: value_field on non-object parent"));
            };
            match map.get(&key) {
                None | Some(Value::Null) => Ok(None),
                Some(v) => Ok(Some(FieldValue::value(v.clone()))),
            }
        })
    })
}

// ---------------------------------------------------------------------------
// module status collection (engine side of a rebuild)
// ---------------------------------------------------------------------------

/// Describe every installed module, reusing `prev` outcomes for modules
/// whose content hash is unchanged (a rebuild only runs `describe` — an
/// untrusted WASM execution — for modules that actually changed). Engine
/// failures (IO, closed) abort; per-module failures degrade to an error
/// outcome.
pub(crate) fn collect_outcomes(
    db: &Db,
    prev: &BTreeMap<String, (u128, DescribeOutcome)>,
) -> Result<BTreeMap<String, (u128, DescribeOutcome)>, fluent31::Error> {
    let mut out = BTreeMap::new();
    for info in db.list_modules()? {
        if let Some((hash, outcome)) = prev.get(&info.name) {
            if *hash == info.content_hash {
                out.insert(info.name, (*hash, outcome.clone()));
                continue;
            }
        }
        let outcome = match db.describe_module(&info.name) {
            Ok(None) => DescribeOutcome::NoDescribe,
            Ok(Some(bytes)) => match descriptor::parse_descriptor(&info.name, &bytes) {
                Ok(schema) => DescribeOutcome::Described(Arc::new(schema)),
                Err(e) => DescribeOutcome::DescribeError(e),
            },
            // engine-level breakage aborts; module-level breakage degrades
            Err(e @ (fluent31::Error::Io(_) | fluent31::Error::Closed)) => return Err(e),
            Err(e) => DescribeOutcome::DescribeError(e.to_string()),
        };
        out.insert(info.name, (info.content_hash, outcome));
    }
    Ok(out)
}

/// Turn describe outcomes into per-module statuses, applying the
/// cross-module duplicate-type check fresh each rebuild (it depends on the
/// whole module set, so it can never be cached per module).
pub(crate) fn statuses_from_outcomes(
    outcomes: &BTreeMap<String, (u128, DescribeOutcome)>,
) -> BTreeMap<String, ModuleStatus> {
    let mut out = BTreeMap::new();
    let mut claimed_types: BTreeSet<String> = BTreeSet::new();
    for (name, (_, outcome)) in outcomes {
        let status = match outcome {
            DescribeOutcome::NoDescribe => ModuleStatus::Untyped,
            DescribeOutcome::DescribeError(e) => ModuleStatus::Invalid(e.clone()),
            DescribeOutcome::Described(schema) => {
                let clash = schema
                    .type_names()
                    .find(|t| claimed_types.contains(*t))
                    .map(str::to_string);
                match clash {
                    Some(t) => ModuleStatus::Invalid(format!(
                        "type {t:?} is already declared by another module"
                    )),
                    None => {
                        claimed_types.extend(schema.type_names().map(str::to_string));
                        ModuleStatus::Typed(schema.clone())
                    }
                }
            }
        };
        out.insert(name.clone(), status);
    }
    out
}

/// Type names currently claimed by typed modules other than `except`.
/// Install-time guard: a new module may not redeclare them.
pub(crate) fn claimed_types_except(
    statuses: &BTreeMap<String, ModuleStatus>,
    except: &str,
) -> BTreeSet<String> {
    statuses
        .iter()
        .filter(|(name, _)| name.as_str() != except)
        .filter_map(|(_, s)| match s {
            ModuleStatus::Typed(m) => Some(m.type_names().map(str::to_string)),
            _ => None,
        })
        .flatten()
        .collect()
}

// ---------------------------------------------------------------------------
// schema build
// ---------------------------------------------------------------------------

fn scalar_u64() -> Scalar {
    Scalar::new("U64")
        .description(
            "64-bit unsigned integer, JSON-encoded as a decimal string: engine sequence \
             numbers reach 2^56, past both GraphQL Int (2^31) and JS double precision (2^53).",
        )
        .validator(|v| match v {
            Value::String(s) => s.parse::<u64>().is_ok(),
            Value::Number(n) => n.as_u64().is_some(),
            _ => false,
        })
}

fn scalar_json() -> Scalar {
    Scalar::new("Json").description("Opaque JSON value, passed through unvalidated.")
}

fn bytes_object() -> Object {
    Object::new("Bytes")
        .description("Raw bytes; request whichever representations you need.")
        .field(Field::new("text", TypeRef::named(TypeRef::STRING), |ctx| {
            FieldFuture::new(async move {
                let b = ctx.parent_value.try_downcast_ref::<BytesVal>()?;
                Ok(std::str::from_utf8(&b.0)
                    .ok()
                    .map(|s| FieldValue::value(s.to_string())))
            })
        })
        .description("The bytes decoded as UTF-8; null when not valid UTF-8."))
        .field(Field::new(
            "base64",
            TypeRef::named_nn(TypeRef::STRING),
            |ctx| {
                FieldFuture::new(async move {
                    let b = ctx.parent_value.try_downcast_ref::<BytesVal>()?;
                    Ok(Some(FieldValue::value(encode_b64(&b.0))))
                })
            },
        ))
        .field(Field::new("hex", TypeRef::named_nn(TypeRef::STRING), |ctx| {
            FieldFuture::new(async move {
                let b = ctx.parent_value.try_downcast_ref::<BytesVal>()?;
                Ok(Some(FieldValue::value(encode_hex(&b.0))))
            })
        }))
        .field(Field::new("len", TypeRef::named_nn(TypeRef::INT), |ctx| {
            FieldFuture::new(async move {
                let b = ctx.parent_value.try_downcast_ref::<BytesVal>()?;
                // values are engine-capped (max_value_size) far below 2^31
                Ok(Some(FieldValue::value(
                    i32::try_from(b.0.len()).unwrap_or(i32::MAX),
                )))
            })
        }))
}

fn bytes_input() -> InputObject {
    InputObject::new("BytesInput")
        .description("Raw bytes in exactly one encoding.")
        .oneof()
        .field(InputValue::new("text", TypeRef::named(TypeRef::STRING)))
        .field(InputValue::new("base64", TypeRef::named(TypeRef::STRING)))
        .field(InputValue::new("hex", TypeRef::named(TypeRef::STRING)))
}

fn pair_object() -> Object {
    Object::new("Pair")
        .field(Field::new("key", TypeRef::named_nn("Bytes"), |ctx| {
            FieldFuture::new(async move {
                let p = ctx.parent_value.try_downcast_ref::<(Vec<u8>, Vec<u8>)>()?;
                Ok(Some(FieldValue::owned_any(BytesVal(p.0.clone()))))
            })
        }))
        .field(Field::new("value", TypeRef::named_nn("Bytes"), |ctx| {
            FieldFuture::new(async move {
                let p = ctx.parent_value.try_downcast_ref::<(Vec<u8>, Vec<u8>)>()?;
                Ok(Some(FieldValue::owned_any(BytesVal(p.1.clone()))))
            })
        }))
}

fn scan_page_object() -> Object {
    Object::new("ScanPage")
        .field(Field::new(
            "pairs",
            TypeRef::named_nn_list_nn("Pair"),
            |ctx| {
                FieldFuture::new(async move {
                    let page = ctx.parent_value.try_downcast_ref::<ScanPageVal>()?;
                    Ok(Some(FieldValue::list(
                        page.pairs.iter().cloned().map(FieldValue::owned_any),
                    )))
                })
            },
        ))
        .field(Field::new(
            "hasMore",
            TypeRef::named_nn(TypeRef::BOOLEAN),
            |ctx| {
                FieldFuture::new(async move {
                    let page = ctx.parent_value.try_downcast_ref::<ScanPageVal>()?;
                    Ok(Some(FieldValue::value(page.has_more)))
                })
            },
        )
        .description("True when the range has more entries past this page."))
        .field(Field::new("nextAfter", TypeRef::named("Bytes"), |ctx| {
            FieldFuture::new(async move {
                let page = ctx.parent_value.try_downcast_ref::<ScanPageVal>()?;
                Ok(page
                    .next_after
                    .clone()
                    .map(|k| FieldValue::owned_any(BytesVal(k))))
            })
        })
        .description("Pass back as `after` to fetch the next page; null on the last page."))
}

fn module_object() -> Object {
    Object::new("Module")
        .field(value_field("name", TypeRef::named_nn(TypeRef::STRING)))
        .field(value_field("size", TypeRef::named_nn(TypeRef::INT)))
        .field(value_field("typed", TypeRef::named_nn(TypeRef::BOOLEAN)))
        .field(value_field("schemaError", TypeRef::named(TypeRef::STRING)))
}

fn fork_object() -> Object {
    Object::new("Fork")
        .field(value_field("name", TypeRef::named_nn(TypeRef::STRING)))
        .field(value_field("instanceId", TypeRef::named_nn(TypeRef::STRING)))
        .field(value_field("createdUnixMs", TypeRef::named_nn("U64")))
        .field(value_field("lastSeqno", TypeRef::named_nn("U64")))
        .field(value_field("path", TypeRef::named_nn(TypeRef::STRING)))
}

fn gc_result_object() -> Object {
    Object::new("GcResult").field(value_field("retired", TypeRef::named("U64")))
}

fn level_stats_object() -> Object {
    Object::new("LevelStats")
        .field(value_field("runs", TypeRef::named_nn(TypeRef::INT)))
        .field(value_field("tables", TypeRef::named_nn(TypeRef::INT)))
        .field(value_field("bytes", TypeRef::named_nn("U64")))
}

fn stats_object() -> Object {
    Object::new("Stats")
        .field(value_field("backend", TypeRef::named_nn(TypeRef::STRING)))
        .field(value_field("visibleSeqno", TypeRef::named_nn("U64")))
        .field(value_field("memtableBytes", TypeRef::named_nn("U64")))
        .field(value_field(
            "immutableMemtables",
            TypeRef::named_nn(TypeRef::INT),
        ))
        .field(value_field("levels", TypeRef::named_nn_list_nn("LevelStats")))
        .field(value_field("vlogFiles", TypeRef::named_nn(TypeRef::INT)))
        .field(value_field("vlogRetired", TypeRef::named_nn(TypeRef::INT)))
        .field(value_field("discardBytes", TypeRef::named_nn("U64")))
        .field(value_field("cacheHits", TypeRef::named_nn("U64")))
        .field(value_field("cacheMisses", TypeRef::named_nn("U64")))
        .field(value_field("commitGroups", TypeRef::named_nn("U64")))
        .field(value_field("commitBatches", TypeRef::named_nn("U64")))
        .field(value_field("walSyncs", TypeRef::named_nn("U64")))
}

fn put_op_input() -> InputObject {
    InputObject::new("PutOp")
        .field(InputValue::new("key", TypeRef::named_nn("BytesInput")))
        .field(InputValue::new("value", TypeRef::named_nn("BytesInput")))
}

fn write_op_input() -> InputObject {
    InputObject::new("WriteOp")
        .description("One entry of a writeBatch.")
        .oneof()
        .field(InputValue::new("put", TypeRef::named("PutOp")))
        .field(InputValue::new("delete", TypeRef::named("BytesInput")))
}

/// Build the full schema for the given module statuses. Only construction
/// errors internal to async-graphql can fail `finish()`; our own validation
/// (reserved names, duplicate types) runs beforehand, so a failure here is
/// a bug — surfaced as a panic rather than silently serving no schema.
pub(crate) fn build(
    weak: Weak<SchemaManager>,
    statuses: &BTreeMap<String, ModuleStatus>,
) -> Schema {
    let mut query = Object::new("Query");
    let mut mutation = Object::new("Mutation");
    (query, mutation) = crate::builtins::register(query, mutation);

    let mut module_types: Vec<Object> = Vec::new();
    for (name, status) in statuses {
        if let ModuleStatus::Typed(schema) = status {
            let (field, types) = crate::modules::typed_field(name, schema.clone());
            module_types.extend(types);
            match schema.kind {
                descriptor::ModuleKind::Query => query = query.field(field),
                descriptor::ModuleKind::Execute => mutation = mutation.field(field),
            }
        }
    }

    let mut builder = Schema::build("Query", Some("Mutation"), None)
        .register(scalar_u64())
        .register(scalar_json())
        .register(bytes_object())
        .register(bytes_input())
        .register(pair_object())
        .register(scan_page_object())
        .register(module_object())
        .register(fork_object())
        .register(gc_result_object())
        .register(level_stats_object())
        .register(stats_object())
        .register(put_op_input())
        .register(write_op_input())
        .register(query)
        .register(mutation)
        // permit semaphores are the primary defense against alias
        // amplification; these just reject absurd documents while leaving
        // room for GraphiQL's introspection query
        .limit_depth(32)
        .limit_complexity(5_000)
        .data(weak);
    for t in module_types {
        builder = builder.register(t);
    }
    builder.finish().expect("schema build: internal invariant")
}
