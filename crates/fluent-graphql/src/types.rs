//! Output object types shared by the query and mutation roots, plus the
//! U64 scalar for engine quantities that overflow GraphQL's 32-bit Int.

use async_graphql::{InputValueError, InputValueResult, Scalar, ScalarType, SimpleObject, Value};

use crate::bytes::Bytes;

/// 64-bit unsigned integer, JSON-encoded as a decimal string: engine
/// sequence numbers reach 2^56 — past both GraphQL `Int` (2^31) and JS
/// double precision (2^53) — and byte totals/counters pass 2^31 on any
/// database over 2 GiB.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct U64(pub u64);

#[Scalar(name = "U64")]
impl ScalarType for U64 {
    fn parse(value: Value) -> InputValueResult<Self> {
        match &value {
            Value::String(s) => s.parse().map(U64).map_err(InputValueError::custom),
            Value::Number(n) => n
                .as_u64()
                .map(U64)
                .ok_or_else(|| InputValueError::expected_type(value.clone())),
            _ => Err(InputValueError::expected_type(value)),
        }
    }

    fn to_value(&self) -> Value {
        Value::String(self.0.to_string())
    }
}

/// One key-value pair.
#[derive(SimpleObject)]
pub struct Pair {
    pub key: Bytes,
    pub value: Bytes,
}

/// A page of scan results.
#[derive(SimpleObject)]
pub struct ScanPage {
    pub pairs: Vec<Pair>,
    /// True when the range has more entries past this page.
    pub has_more: bool,
    /// Pass back as `after` to fetch the next page; null on the last page.
    pub next_after: Option<Bytes>,
}

/// An installed WASM module.
#[derive(SimpleObject)]
pub struct Module {
    pub name: String,
    /// Stored module size in bytes.
    pub size: i32,
}

impl From<fluent31::ModuleInfo> for Module {
    fn from(m: fluent31::ModuleInfo) -> Self {
        Module {
            name: m.name,
            // modules are engine-capped far below 2^31
            size: i32::try_from(m.size).unwrap_or(i32::MAX),
        }
    }
}

/// A point-in-time checkpoint archive.
#[derive(SimpleObject)]
pub struct Checkpoint {
    pub name: String,
    pub created_unix_ms: U64,
    /// Every write with seqno <= this is contained in the archive.
    pub last_seqno: U64,
    pub path: String,
}

impl From<fluent31::CheckpointInfo> for Checkpoint {
    fn from(c: fluent31::CheckpointInfo) -> Self {
        Checkpoint {
            name: c.name,
            created_unix_ms: U64(c.created_unix_ms),
            last_seqno: U64(c.last_seqno),
            path: c.path.display().to_string(),
        }
    }
}

/// Result of a value-log GC pass.
#[derive(SimpleObject)]
pub struct GcResult {
    /// The retired vlog file id; null when no victim qualified.
    pub retired: Option<U64>,
}

/// Per-level shape: sorted runs, fragment files, total bytes.
#[derive(SimpleObject)]
pub struct LevelStats {
    pub runs: i32,
    pub tables: i32,
    pub bytes: U64,
}

/// Engine statistics.
#[derive(SimpleObject)]
pub struct Stats {
    pub backend: String,
    pub visible_seqno: U64,
    pub memtable_bytes: U64,
    pub immutable_memtables: i32,
    pub levels: Vec<LevelStats>,
    pub vlog_files: i32,
    pub vlog_retired: i32,
    pub discard_bytes: U64,
    pub cache_hits: U64,
    pub cache_misses: U64,
}

impl From<fluent31::DbStats> for Stats {
    fn from(s: fluent31::DbStats) -> Self {
        Stats {
            backend: s.backend.to_string(),
            visible_seqno: U64(s.visible_seqno),
            memtable_bytes: U64(s.memtable_bytes as u64),
            immutable_memtables: i32::try_from(s.immutable_memtables).unwrap_or(i32::MAX),
            levels: s
                .levels
                .into_iter()
                .map(|(runs, tables, bytes)| LevelStats {
                    runs: i32::try_from(runs).unwrap_or(i32::MAX),
                    tables: i32::try_from(tables).unwrap_or(i32::MAX),
                    bytes: U64(bytes),
                })
                .collect(),
            vlog_files: i32::try_from(s.vlog_files).unwrap_or(i32::MAX),
            vlog_retired: i32::try_from(s.vlog_retired).unwrap_or(i32::MAX),
            discard_bytes: U64(s.discard_bytes),
            cache_hits: U64(s.cache_hits),
            cache_misses: U64(s.cache_misses),
        }
    }
}
