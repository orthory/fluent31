//! Typed module root fields: each installed module with a valid `describe`
//! descriptor becomes one root field (Query for `kind: "query"`, Mutation
//! for `kind: "execute"`) plus its declared output object types.
//!
//! Input: declared args arrive at the guest as one JSON object (every
//! declared arg present; omitted optionals are null). A module declaring no
//! `args` gets a single optional `input: BytesInput` and receives raw
//! bytes. Output: the guest's bytes are parsed as JSON and validated
//! against the declared output type (`descriptor::normalize_output`).
//!
//! The root field's *outer* nullability is always relaxed to nullable —
//! whatever the descriptor declares — so a failing module yields a null
//! field plus an `errors` entry instead of a spec-invalid partial object
//! (async-graphql's dynamic executor does not null-propagate to the root).
//!
//! Hot-swap caveat: a request validated against the pre-replacement schema
//! carries the old descriptor, while module bytes are resolved by name at
//! execution time. A request in flight across an `installModule`
//! replacement can therefore run the NEW bytes with OLD-shaped args
//! (executors; query fields are shielded once their snapshot is pinned).
//! The window is one request during an explicit replacement — acceptable
//! for this server; replace modules under quiesced writes if it matters.

use std::sync::Arc;

use async_graphql::dynamic::{
    Field, FieldFuture, FieldValue, InputValue, Object, ResolverContext, TypeRef,
};
use async_graphql::{Error, Value};
use serde_json::{Map as JsonMap, Value as Json};

use crate::bytes::decode_bytes_input;
use crate::descriptor::{FieldSpec, ModuleKind, ModuleSchema, ObjectSpec, TypeRefSpec};
use crate::schema::{manager, pinned_snap, value_field};

/// `TypeRefSpec` → dynamic `TypeRef`.
fn type_ref(spec: &TypeRefSpec) -> TypeRef {
    let base = map_scalar(&spec.base);
    match (spec.list, spec.elem_nn, spec.nn) {
        (false, _, false) => TypeRef::named(base),
        (false, _, true) => TypeRef::named_nn(base),
        (true, false, false) => TypeRef::named_list(base),
        (true, true, false) => TypeRef::named_nn_list(base),
        (true, false, true) => TypeRef::named_list_nn(base),
        (true, true, true) => TypeRef::named_nn_list_nn(base),
    }
}

/// Like [`type_ref`] but with the outer non-null dropped (root fields stay
/// nullable so failures are representable — see module docs).
fn type_ref_nullable_outer(spec: &TypeRefSpec) -> TypeRef {
    let relaxed = TypeRefSpec {
        nn: false,
        ..spec.clone()
    };
    type_ref(&relaxed)
}

fn map_scalar(base: &str) -> String {
    match base {
        "String" => TypeRef::STRING.to_string(),
        "Int" => TypeRef::INT.to_string(),
        "Float" => TypeRef::FLOAT.to_string(),
        "Boolean" => TypeRef::BOOLEAN.to_string(),
        other => other.to_string(),
    }
}

/// GraphQL argument value → JSON handed to the guest.
fn arg_to_json(spec: &FieldSpec, v: Option<async_graphql::dynamic::ValueAccessor<'_>>) -> Result<Json, Error> {
    let Some(v) = v else { return Ok(Json::Null) };
    if v.is_null() {
        return Ok(Json::Null);
    }
    if spec.ty.list {
        let elem = FieldSpec {
            name: spec.name.clone(),
            ty: TypeRefSpec {
                base: spec.ty.base.clone(),
                list: false,
                elem_nn: false,
                nn: spec.ty.elem_nn,
            },
            description: None,
        };
        // GraphQL input coercion: a single value is a 1-element list
        let Ok(list) = v.list() else {
            return Ok(Json::Array(vec![arg_to_json(&elem, Some(v))?]));
        };
        let mut out = Vec::new();
        for item in list.iter() {
            out.push(arg_to_json(&elem, Some(item))?);
        }
        return Ok(Json::Array(out));
    }
    Ok(match spec.ty.base.as_str() {
        "String" => Json::String(v.string()?.to_string()),
        "Boolean" => Json::Bool(v.boolean()?),
        // the dynamic schema's Int validator only checks is_i64; enforce
        // the spec's 32-bit range here (the derive-based schema did)
        "Int" => {
            let n = v.i64()?;
            i32::try_from(n).map_err(|_| {
                Error::new(format!(
                    "arg {}: Int out of 32-bit range: {n}",
                    spec.name
                ))
            })?;
            Json::Number(n.into())
        }
        "Float" => serde_json::Number::from_f64(v.f64()?)
            .map(Json::Number)
            .unwrap_or(Json::Null),
        // U64 travels as a decimal string or number (see the U64 scalar)
        "U64" => Json::Number(match v.as_value() {
            Value::String(s) => s
                .parse::<u64>()
                .map_err(|_| Error::new(format!("arg {}: expected decimal u64", spec.name)))?
                .into(),
            _ => v.u64()?.into(),
        }),
        "Json" => gql_to_json(v.as_value()),
        other => return Err(Error::new(format!("arg {}: unsupported type {other}", spec.name))),
    })
}

fn gql_to_json(v: &Value) -> Json {
    match v {
        Value::Null => Json::Null,
        Value::Boolean(b) => Json::Bool(*b),
        Value::Number(n) => Json::Number(n.clone()),
        Value::String(s) => Json::String(s.clone()),
        Value::Enum(e) => Json::String(e.to_string()),
        Value::List(items) => Json::Array(items.iter().map(gql_to_json).collect()),
        Value::Object(m) => Json::Object(
            m.iter()
                .map(|(k, v)| (k.to_string(), gql_to_json(v)))
                .collect::<JsonMap<_, _>>(),
        ),
        Value::Binary(b) => Json::String(crate::bytes::encode_b64(b)),
    }
}

fn object_type(spec: &ObjectSpec) -> Object {
    let mut obj = Object::new(&spec.name);
    for f in &spec.fields {
        let mut field = value_field(&f.name, type_ref(&f.ty));
        if let Some(d) = &f.description {
            field = field.description(d);
        }
        obj = obj.field(field);
    }
    obj
}

/// Build the typed root field for a module plus its declared object types.
pub(crate) fn typed_field(name: &str, schema: Arc<ModuleSchema>) -> (Field, Vec<Object>) {
    let types = schema.types.iter().map(object_type).collect();

    let resolver_schema = schema.clone();
    let module = name.to_string();
    let mut field = Field::new(
        name.to_string(),
        type_ref_nullable_outer(&schema.output),
        move |ctx: ResolverContext<'_>| {
            let schema = resolver_schema.clone();
            let module = module.clone();
            FieldFuture::new(async move {
                // assemble the guest's input
                let input: Vec<u8> = match &schema.args {
                    Some(args) => {
                        let mut m = JsonMap::new();
                        for a in args {
                            m.insert(a.name.clone(), arg_to_json(a, ctx.args.get(&a.name))?);
                        }
                        serde_json::to_vec(&Json::Object(m))
                            .map_err(|e| Error::new(format!("input encode: {e}")))?
                    }
                    None => match ctx.args.get("input") {
                        Some(v) if !v.is_null() => decode_bytes_input(&v.object()?)?,
                        _ => Vec::new(),
                    },
                };

                let mgr = manager(&ctx)?;
                let db = mgr.db.clone();
                let raw = match schema.kind {
                    ModuleKind::Query => {
                        let snap = pinned_snap(&ctx, &mgr.db)?;
                        mgr.blocking_read(move || db.query_at(&module, &input, &snap))
                            .await?
                    }
                    ModuleKind::Execute => {
                        mgr.blocking_write(move || db.execute(&module, &input)).await?
                    }
                };

                let value = crate::descriptor::normalize_output(&schema, &raw).map_err(|e| {
                    use async_graphql::ErrorExtensions;
                    // for executors the transaction has ALREADY committed:
                    // say so loudly, or a client will retry a write that
                    // durably landed
                    let committed = matches!(schema.kind, ModuleKind::Execute);
                    let msg = if committed {
                        format!(
                            "module {} COMMITTED its transaction but returned output \
                             violating its declared schema: {e}",
                            schema.module
                        )
                    } else {
                        format!("module {}: {e}", schema.module)
                    };
                    Error::new(msg).extend_with(|_, x| {
                        x.set("code", "OUTPUT_SCHEMA_VIOLATION");
                        x.set("committed", committed);
                    })
                })?;
                if value == Value::Null {
                    return Ok(None);
                }
                Ok(Some(FieldValue::value(value)))
            })
        },
    );

    if let Some(d) = &schema.description {
        field = field.description(d);
    }
    match &schema.args {
        Some(args) => {
            for a in args {
                let mut arg = InputValue::new(&a.name, type_ref(&a.ty));
                if let Some(d) = &a.description {
                    arg = arg.description(d);
                }
                field = field.argument(arg);
            }
        }
        None => {
            field = field.argument(InputValue::new("input", TypeRef::named("BytesInput")));
        }
    }
    (field, types)
}
