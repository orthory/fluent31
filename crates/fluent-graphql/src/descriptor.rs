//! Module schema descriptors — "fluentabi v1 describe".
//!
//! A WASM module may export `describe: () -> i32` that writes a JSON
//! descriptor declaring its GraphQL surface. The server calls it at
//! install/schema-build time and turns each described module into a typed
//! root field (Query for `kind: "query"`, Mutation for `kind: "execute"`).
//!
//! Descriptor shape:
//!
//! ```json
//! {
//!   "kind": "query" | "execute",
//!   "description": "optional docs for the root field",
//!   "args": [{"name": "customer", "type": "String!", "description": "..."}],
//!   "types": [{"name": "OrderStats", "fields": [{"name": "orders", "type": "U64!"}]}],
//!   "output": "OrderStats!"
//! }
//! ```
//!
//! - Scalars: `String`, `Int`, `Float`, `Boolean`, `U64` (decimal string on
//!   the wire), `Json` (opaque passthrough). One list level max: `[T]`,
//!   `[T!]`, `[T]!`, `[T!]!`.
//! - When `args` is present, the guest receives its input as a JSON object
//!   of the provided arguments; when absent, the field takes a single
//!   optional `input: BytesInput` and the guest receives raw bytes.
//! - Guest output is parsed as JSON and validated against `output`.
//! - The size cap below is checked after the describe run completes, so a
//!   hostile describe can still transiently buffer up to the engine's
//!   `max_wasm_output`; the content-hash cache in `SchemaManager` limits
//!   that to once per distinct module content.

use std::collections::{BTreeMap, BTreeSet};

use async_graphql::{Name, Value};
use serde_json::Value as Json;

pub const MAX_DESCRIPTOR_BYTES: usize = 64 << 10;
const MAX_TYPES: usize = 32;
const MAX_FIELDS: usize = 64;
const MAX_ARGS: usize = 16;

pub const SCALARS: &[&str] = &["String", "Int", "Float", "Boolean", "U64", "Json"];

/// Root field names owned by the built-in API. A module may not shadow any
/// of them (on either root, to keep one namespace).
pub const RESERVED_FIELDS: &[&str] = &[
    "get",
    "scan",
    "wasm",
    "modules",
    "stats",
    "forks",
    "snapshotSeqno",
    "put",
    "delete",
    "writeBatch",
    "wasmExecute",
    "installModule",
    "uninstallModule",
    "fork",
    "deleteFork",
    "flush",
    "compactAll",
    "gcVlog",
    "reloadSchema",
    "syncWal",
];

/// Type names owned by the built-in schema (plus GraphQL's own scalars).
pub const RESERVED_TYPES: &[&str] = &[
    "Query",
    "Mutation",
    "Subscription",
    "Bytes",
    "BytesInput",
    "U64",
    "Json",
    "Pair",
    "ScanPage",
    "Module",
    "Fork",
    "GcResult",
    "LevelStats",
    "Stats",
    "WriteOp",
    "PutOp",
    "String",
    "Int",
    "Float",
    "Boolean",
    "ID",
];

/// GraphQL name: `[_A-Za-z][_0-9A-Za-z]*`, not introspection-reserved.
pub fn is_graphql_name(s: &str) -> bool {
    let mut chars = s.chars();
    matches!(chars.next(), Some(c) if c == '_' || c.is_ascii_alphabetic())
        && chars.all(|c| c == '_' || c.is_ascii_alphanumeric())
        && !s.starts_with("__")
}

/// A type reference with at most one list level: `T`, `T!`, `[T]`, `[T!]`,
/// `[T]!`, `[T!]!`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TypeRefSpec {
    pub base: String,
    pub list: bool,
    /// Element non-null (meaningful only when `list`).
    pub elem_nn: bool,
    /// Outer non-null.
    pub nn: bool,
}

pub fn parse_type_ref(s: &str) -> Result<TypeRefSpec, String> {
    let s = s.trim();
    let (inner, nn) = match s.strip_suffix('!') {
        Some(rest) => (rest.trim(), true),
        None => (s, false),
    };
    if let Some(body) = inner.strip_prefix('[') {
        let body = body
            .strip_suffix(']')
            .ok_or_else(|| format!("malformed type {s:?}"))?
            .trim();
        let (base, elem_nn) = match body.strip_suffix('!') {
            Some(rest) => (rest.trim(), true),
            None => (body, false),
        };
        if base.starts_with('[') {
            return Err(format!("nested lists are not supported: {s:?}"));
        }
        if !is_graphql_name(base) {
            return Err(format!("invalid type name {base:?}"));
        }
        Ok(TypeRefSpec {
            base: base.to_string(),
            list: true,
            elem_nn,
            nn,
        })
    } else {
        if !is_graphql_name(inner) {
            return Err(format!("invalid type name {inner:?}"));
        }
        Ok(TypeRefSpec {
            base: inner.to_string(),
            list: false,
            elem_nn: false,
            nn,
        })
    }
}

#[derive(Clone, Debug)]
pub struct FieldSpec {
    pub name: String,
    pub ty: TypeRefSpec,
    pub description: Option<String>,
}

#[derive(Clone, Debug)]
pub struct ObjectSpec {
    pub name: String,
    pub fields: Vec<FieldSpec>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ModuleKind {
    Query,
    Execute,
}

/// A module's validated, GraphQL-ready schema declaration.
#[derive(Clone, Debug)]
pub struct ModuleSchema {
    /// Module name == root field name.
    pub module: String,
    pub kind: ModuleKind,
    pub description: Option<String>,
    /// None → single optional `input: BytesInput` argument, raw bytes in.
    /// Some → typed arguments, JSON object in.
    pub args: Option<Vec<FieldSpec>>,
    pub types: Vec<ObjectSpec>,
    pub output: TypeRefSpec,
}

impl ModuleSchema {
    /// Type names this module registers in the schema.
    pub fn type_names(&self) -> impl Iterator<Item = &str> {
        self.types.iter().map(|t| t.name.as_str())
    }
}

fn parse_fields(raw: &Json, what: &str, max: usize) -> Result<Vec<FieldSpec>, String> {
    let arr = raw
        .as_array()
        .ok_or_else(|| format!("{what} must be an array"))?;
    if arr.len() > max {
        return Err(format!("{what}: too many entries (max {max})"));
    }
    let mut seen = BTreeSet::new();
    let mut out = Vec::with_capacity(arr.len());
    for f in arr {
        let name = f
            .get("name")
            .and_then(Json::as_str)
            .ok_or_else(|| format!("{what}: entry missing \"name\""))?;
        if !is_graphql_name(name) {
            return Err(format!("{what}: {name:?} is not a valid GraphQL name"));
        }
        if !seen.insert(name.to_string()) {
            return Err(format!("{what}: duplicate name {name:?}"));
        }
        let ty = f
            .get("type")
            .and_then(Json::as_str)
            .ok_or_else(|| format!("{what}: {name:?} missing \"type\""))?;
        out.push(FieldSpec {
            name: name.to_string(),
            ty: parse_type_ref(ty)?,
            description: f
                .get("description")
                .and_then(Json::as_str)
                .map(str::to_string),
        });
    }
    Ok(out)
}

/// Parse and validate a descriptor emitted by `module`'s `describe` export.
pub fn parse_descriptor(module: &str, bytes: &[u8]) -> Result<ModuleSchema, String> {
    if bytes.len() > MAX_DESCRIPTOR_BYTES {
        return Err(format!(
            "descriptor exceeds {MAX_DESCRIPTOR_BYTES} bytes"
        ));
    }
    if !is_graphql_name(module) {
        return Err(format!(
            "module name {module:?} is not a valid GraphQL field name ([_A-Za-z][_0-9A-Za-z]*)"
        ));
    }
    if RESERVED_FIELDS.contains(&module) {
        return Err(format!("module name {module:?} shadows a built-in field"));
    }
    let root: Json =
        serde_json::from_slice(bytes).map_err(|e| format!("descriptor is not JSON: {e}"))?;
    let obj = root
        .as_object()
        .ok_or_else(|| "descriptor must be a JSON object".to_string())?;

    let kind = match obj.get("kind").and_then(Json::as_str) {
        Some("query") => ModuleKind::Query,
        Some("execute") => ModuleKind::Execute,
        other => return Err(format!("kind must be \"query\" or \"execute\", got {other:?}")),
    };
    let description = obj
        .get("description")
        .and_then(Json::as_str)
        .map(str::to_string);

    // declared object types
    let mut types = Vec::new();
    let mut declared = BTreeSet::new();
    if let Some(raw_types) = obj.get("types") {
        let arr = raw_types
            .as_array()
            .ok_or_else(|| "types must be an array".to_string())?;
        if arr.len() > MAX_TYPES {
            return Err(format!("too many types (max {MAX_TYPES})"));
        }
        for t in arr {
            let name = t
                .get("name")
                .and_then(Json::as_str)
                .ok_or_else(|| "types: entry missing \"name\"".to_string())?;
            if !is_graphql_name(name) {
                return Err(format!("type name {name:?} is not a valid GraphQL name"));
            }
            if RESERVED_TYPES.contains(&name) {
                return Err(format!("type name {name:?} is reserved"));
            }
            if !declared.insert(name.to_string()) {
                return Err(format!("duplicate type {name:?}"));
            }
            let fields = parse_fields(
                t.get("fields").unwrap_or(&Json::Null),
                &format!("type {name}"),
                MAX_FIELDS,
            )?;
            if fields.is_empty() {
                return Err(format!("type {name:?} has no fields"));
            }
            types.push(ObjectSpec {
                name: name.to_string(),
                fields,
            });
        }
    }

    // field types may reference scalars or declared objects
    let known = |base: &str| SCALARS.contains(&base) || declared.contains(base);
    for t in &types {
        for f in &t.fields {
            if !known(&f.ty.base) {
                return Err(format!(
                    "type {}: field {} references unknown type {:?}",
                    t.name, f.name, f.ty.base
                ));
            }
        }
    }

    // args: scalars only (inputs never reference output objects)
    let args = match obj.get("args") {
        None | Some(Json::Null) => None,
        Some(raw) => {
            let args = parse_fields(raw, "args", MAX_ARGS)?;
            for a in &args {
                if !SCALARS.contains(&a.ty.base.as_str()) {
                    return Err(format!(
                        "arg {}: type must be a scalar, got {:?}",
                        a.name, a.ty.base
                    ));
                }
            }
            Some(args)
        }
    };

    let output = parse_type_ref(
        obj.get("output")
            .and_then(Json::as_str)
            .ok_or_else(|| "descriptor missing \"output\"".to_string())?,
    )?;
    if !known(&output.base) {
        return Err(format!("output references unknown type {:?}", output.base));
    }

    Ok(ModuleSchema {
        module: module.to_string(),
        kind,
        description,
        args,
        types,
        output,
    })
}

// ---------------------------------------------------------------------------
// guest output → GraphQL value
// ---------------------------------------------------------------------------

fn json_to_gql(v: &Json) -> Value {
    match v {
        Json::Null => Value::Null,
        Json::Bool(b) => Value::Boolean(*b),
        Json::Number(n) => Value::Number(n.clone()),
        Json::String(s) => Value::String(s.clone()),
        Json::Array(items) => Value::List(items.iter().map(json_to_gql).collect()),
        Json::Object(m) => Value::Object(
            m.iter()
                .map(|(k, v)| (Name::new(k), json_to_gql(v)))
                .collect(),
        ),
    }
}

fn coerce_base(
    base: &str,
    v: &Json,
    objects: &BTreeMap<&str, &ObjectSpec>,
    path: &str,
) -> Result<Value, String> {
    match base {
        "Json" => Ok(json_to_gql(v)),
        "String" => v
            .as_str()
            .map(|s| Value::String(s.to_string()))
            .ok_or_else(|| format!("{path}: expected string")),
        "Boolean" => v
            .as_bool()
            .map(Value::Boolean)
            .ok_or_else(|| format!("{path}: expected boolean")),
        "Int" => v
            .as_i64()
            .filter(|n| i32::try_from(*n).is_ok())
            .map(|n| Value::Number(n.into()))
            .ok_or_else(|| format!("{path}: expected 32-bit integer")),
        "Float" => v
            .as_f64()
            .and_then(|f| serde_json::Number::from_f64(f).map(Value::Number))
            .ok_or_else(|| format!("{path}: expected number")),
        // U64 travels as a decimal string (see the U64 scalar); accept a
        // JSON number or decimal string from the guest
        "U64" => match v {
            Json::Number(n) => n
                .as_u64()
                .map(|n| Value::String(n.to_string()))
                .ok_or_else(|| format!("{path}: expected unsigned integer")),
            Json::String(s) => s
                .parse::<u64>()
                .map(|n| Value::String(n.to_string()))
                .map_err(|_| format!("{path}: expected decimal u64 string")),
            _ => Err(format!("{path}: expected u64")),
        },
        name => {
            let spec = objects
                .get(name)
                .ok_or_else(|| format!("{path}: unknown type {name:?}"))?;
            let m = v
                .as_object()
                .ok_or_else(|| format!("{path}: expected {name} object"))?;
            let mut out = async_graphql::indexmap::IndexMap::new();
            for f in &spec.fields {
                let fv = m.get(&f.name).unwrap_or(&Json::Null);
                out.insert(
                    Name::new(&f.name),
                    coerce(&f.ty, fv, objects, &format!("{path}.{}", f.name))?,
                );
            }
            // extra keys the schema doesn't declare are dropped
            Ok(Value::Object(out))
        }
    }
}

fn coerce(
    ty: &TypeRefSpec,
    v: &Json,
    objects: &BTreeMap<&str, &ObjectSpec>,
    path: &str,
) -> Result<Value, String> {
    if v.is_null() {
        return if ty.nn {
            Err(format!("{path}: null for non-null {}", ty.base))
        } else {
            Ok(Value::Null)
        };
    }
    if ty.list {
        let items = v
            .as_array()
            .ok_or_else(|| format!("{path}: expected list"))?;
        let elem = TypeRefSpec {
            base: ty.base.clone(),
            list: false,
            elem_nn: false,
            nn: ty.elem_nn,
        };
        let mut out = Vec::with_capacity(items.len());
        for (i, item) in items.iter().enumerate() {
            out.push(coerce(&elem, item, objects, &format!("{path}[{i}]"))?);
        }
        Ok(Value::List(out))
    } else {
        coerce_base(&ty.base, v, objects, path)
    }
}

/// Parse a guest's output bytes as JSON and validate/normalize them against
/// the module's declared output type.
pub fn normalize_output(schema: &ModuleSchema, raw: &[u8]) -> Result<Value, String> {
    let json: Json = serde_json::from_slice(raw)
        .map_err(|e| format!("module output is not valid JSON: {e}"))?;
    let objects: BTreeMap<&str, &ObjectSpec> =
        schema.types.iter().map(|t| (t.name.as_str(), t)).collect();
    coerce(&schema.output, &json, &objects, "output")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn type_refs_parse() {
        let t = parse_type_ref("[TopCustomer!]!").unwrap();
        assert_eq!(
            t,
            TypeRefSpec {
                base: "TopCustomer".into(),
                list: true,
                elem_nn: true,
                nn: true
            }
        );
        assert!(parse_type_ref("[[Int]]").is_err());
        assert!(parse_type_ref("9lives").is_err());
        assert!(parse_type_ref("__meta").is_err());
    }

    #[test]
    fn graphql_names() {
        assert!(is_graphql_name("placeOrder"));
        assert!(is_graphql_name("_x9"));
        assert!(!is_graphql_name("place-order"));
        assert!(!is_graphql_name("9x"));
        assert!(!is_graphql_name("__hidden"));
        assert!(!is_graphql_name(""));
    }

    #[test]
    fn descriptor_rejects_reserved_and_unknown() {
        let d = br#"{"kind":"query","output":"Missing!"}"#;
        assert!(parse_descriptor("m", d).unwrap_err().contains("unknown type"));
        let d = br#"{"kind":"query","output":"Int!"}"#;
        assert!(parse_descriptor("scan", d).unwrap_err().contains("shadows"));
        assert!(parse_descriptor("my-mod", d).unwrap_err().contains("GraphQL"));
        let d = br#"{"kind":"query","types":[{"name":"Stats","fields":[{"name":"a","type":"Int"}]}],"output":"Stats"}"#;
        assert!(parse_descriptor("m", d).unwrap_err().contains("reserved"));
    }

    #[test]
    fn output_normalization() {
        let d = br#"{
            "kind": "query",
            "types": [
                {"name": "Top", "fields": [{"name": "who", "type": "String!"}, {"name": "cents", "type": "U64!"}]},
                {"name": "Report", "fields": [{"name": "n", "type": "Int!"}, {"name": "top", "type": "[Top!]!"}, {"name": "extra", "type": "Json"}]}
            ],
            "output": "Report!"
        }"#;
        let s = parse_descriptor("report", d).unwrap();
        let out = normalize_output(
            &s,
            br#"{"n": 2, "top": [{"who": "acme", "cents": 12345678901234567890}], "junk": true}"#,
        )
        .unwrap();
        let Value::Object(m) = out else { panic!() };
        assert_eq!(m["n"], Value::Number(2.into()));
        assert!(m.get("junk").is_none(), "undeclared keys dropped");
        let Value::List(top) = &m["top"] else { panic!() };
        let Value::Object(t) = &top[0] else { panic!() };
        assert_eq!(t["cents"], Value::String("12345678901234567890".into()));
        assert_eq!(m["extra"], Value::Null);

        // violations
        assert!(normalize_output(&s, br#"{"n": null, "top": []}"#).is_err());
        assert!(normalize_output(&s, br#"{"n": 1, "top": [null]}"#).is_err());
        assert!(normalize_output(&s, b"not json").is_err());
        assert!(normalize_output(&s, br#"{"n": 4000000000, "top": []}"#).is_err());
    }
}
