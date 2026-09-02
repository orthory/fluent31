//! TOML config file for the server binary (`--config <path>`).
//!
//! Top-level keys and `[listen]` mirror CLI flags, Cargo-style kebab-case;
//! the tuning sections are file-only and cover everything the composed
//! crates expose as configuration: `[engine]` is the full
//! [`fluent31::Options`] tunable surface, `[graphql]` /
//! `[replication]` are the per-plane limits, `[journal]` attaches the
//! opt-in mutation journal ([`fluent31::journal`]) at its `dir`, and
//! `[log]` tunes what the process logs (levels come from `RUST_LOG`). An
//! explicit flag overrides its file value, the file overrides the
//! built-in default. Unknown keys are an error — a typo must not
//! silently fall back.
//!
//! The `[edge]` section flips the process's role: present, the binary is
//! an **edge server** — it attaches an edge replica to a master's
//! replication plane and serves the read-only edge GraphQL surface
//! (`get`/`scan`, scope-clamped) on `[listen].graphql`; `dir` is then the
//! replica's local cache directory (wiped on attach). Settings that
//! configure a store of record (`store-name`, `sync`, `[engine]`,
//! `[journal]`, `[replication]`, `[listen].replication`, the `[graphql]`
//! fork tuning) are refused in edge mode — an edge has no store to apply
//! them to.
//!
//! ```toml
//! dir = "./data"
//! store-name = "prod"
//! sync = "periodic:50"          # always | never | periodic:<ms>
//!
//! [listen]
//! graphql = "127.0.0.1:8317"
//! replication = "127.0.0.1:8428"
//!
//! [graphql]
//! max-body-bytes = 33554432
//! fork-max-open = 8             # open fork instances beyond the primary
//! fork-idle-ttl-secs = 300
//!
//! [replication]
//! max-frame-bytes = 1048576
//! ping-every-ms = 2000
//!
//! [journal]                     # present = journal attached; absent = off
//! dir = "./journal"
//! rotate-bytes = 134217728
//! compact-when-deltas-exceed = 1.0
//! compact-min-bytes = 67108864
//!
//! [log]
//! stats-every-secs = 60         # engine stats line per open store; 0 = off
//!
//! [edge]                        # present = the process is an edge server
//! master-addr = "10.0.0.5:8428" # the master's replication plane
//! scope-lo = { text = "user/" } # bytes as text or hex; omitted = unbounded
//! scope-hi = { hex = "7573657230" }
//! refresh-every-secs = 300      # periodic slice refresh; 0 = only on re-sync
//! value-cache-bytes = 268435456
//! block-cache-size = 33554432
//!
//! [engine]
//! create-if-missing = true
//! wasm-enabled = true           # false = inert WASM layer: module/trigger
//!                               # APIs refuse, no trigger capture/runs
//! io-backend = "auto"           # auto | uring | std
//! compression = "none"          # none | lz4
//! memtable-size = 8388608
//! max-immutable-memtables = 2
//! block-size = 8192
//! bloom-bits-per-key = 10
//! block-cache-size = 67108864
//! l0-compaction-trigger = 4
//! tier-width = 4
//! max-levels = 7
//! l0-stall-trigger = 12
//! target-file-size = 67108864
//! value-threshold = 4096
//! vlog-file-size = 134217728
//! vlog-gc-ratio = 0.5
//! max-key-size = 16384
//! max-value-size = 268435456
//! max-txn-write-bytes = 268435456
//! sub-queue-bytes = 8388608
//! wasm-fuel = 1000000000
//! wasm-memory-limit = 67108864
//! execute-retries = 3
//! max-wasm-input = 67108864
//! max-wasm-output = 33554432
//! max-wasm-log = 1048576
//! max-wasm-scans = 64
//! wasm-module-cache = 32
//! trigger-batch = 512
//! trigger-inline-value = 65536
//! ```

use std::path::Path;
use std::time::Duration;

use fluent31::{Compression, IoBackend, JournalConfig, Options, SyncMode};
use fluent_replication::EdgeReplicaConfig;
use serde::Deserialize;

/// One optional slot per setting the binary accepts. Doubles as the
/// holder for explicit CLI flags, so precedence is a field-wise
/// [`FileConfig::overlay`] of the two.
#[derive(Debug, Default, PartialEq, Deserialize)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub struct FileConfig {
    pub dir: Option<String>,
    pub store_name: Option<String>,
    /// Same grammar as the `--sync` flag: `always` | `never` |
    /// `periodic:<ms>`. Kept as a string here so the file and the flag
    /// share one parser ([`parse_sync`]).
    pub sync: Option<String>,
    pub listen: Option<ListenSection>,
    pub graphql: Option<GraphqlSection>,
    pub replication: Option<ReplicationSection>,
    pub journal: Option<JournalSection>,
    pub engine: Option<EngineSection>,
    pub log: Option<LogSection>,
    pub edge: Option<EdgeSection>,
}

#[derive(Debug, Default, PartialEq, Deserialize)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub struct ListenSection {
    pub graphql: Option<String>,
    pub replication: Option<String>,
}

#[derive(Debug, Default, PartialEq, Deserialize)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub struct GraphqlSection {
    pub max_body_bytes: Option<usize>,
    pub fork_max_open: Option<usize>,
    pub fork_idle_ttl_secs: Option<u64>,
}

#[derive(Debug, Default, PartialEq, Deserialize)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub struct ReplicationSection {
    pub max_frame_bytes: Option<usize>,
    pub ping_every_ms: Option<u64>,
}

/// What the process logs beyond its events. Levels are not configured
/// here: `RUST_LOG` is the one switch for them, so a log filter never
/// needs a restart-with-config to change.
#[derive(Debug, Default, PartialEq, Deserialize)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub struct LogSection {
    /// Period of the stats heartbeat (one INFO line per open store plus
    /// the fork registry's occupancy); `0` turns it off.
    pub stats_every_secs: Option<u64>,
}

/// The opt-in mutation journal ([`fluent31::journal`]): the section being
/// present attaches one. Tuning fields mirror [`JournalConfig`]; an
/// absent field keeps its default.
#[derive(Debug, Default, PartialEq, Deserialize)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub struct JournalSection {
    /// Journal directory. Required once the section exists — a `[journal]`
    /// that names no destination is refused at startup rather than
    /// silently journaling nothing.
    pub dir: Option<String>,
    pub rotate_bytes: Option<u64>,
    /// Auto-compaction ratio (deltas since the last base / that base's
    /// size). fluent31 reads `None` as "auto-compaction off", which a TOML
    /// file cannot say by omission — omitted here means the engine default.
    pub compact_when_deltas_exceed: Option<f64>,
    pub compact_min_bytes: Option<u64>,
}

impl JournalSection {
    /// The journal tuning: file values applied over
    /// [`JournalConfig::default`].
    pub fn config(&self) -> JournalConfig {
        let mut c = JournalConfig::default();
        if let Some(v) = self.rotate_bytes {
            c.rotate_bytes = v;
        }
        if let Some(v) = self.compact_when_deltas_exceed {
            c.compact_when_deltas_exceed = Some(v);
        }
        if let Some(v) = self.compact_min_bytes {
            c.compact_min_bytes = v;
        }
        c
    }
}

/// Bytes in exactly one encoding — the config-file twin of the GraphQL
/// `BytesInput` oneof.
#[derive(Debug, PartialEq, Deserialize)]
#[serde(untagged, deny_unknown_fields)]
pub enum BytesSpec {
    Text { text: String },
    Hex { hex: String },
}

impl BytesSpec {
    pub fn decode(&self) -> Result<Vec<u8>, String> {
        match self {
            BytesSpec::Text { text } => Ok(text.clone().into_bytes()),
            BytesSpec::Hex { hex } => decode_hex(hex),
        }
    }
}

fn decode_hex(s: &str) -> Result<Vec<u8>, String> {
    let well_formed = s.len().is_multiple_of(2) && s.chars().all(|c| c.is_ascii_hexdigit());
    if !well_formed {
        return Err(format!(
            "invalid hex bytes {s:?} (even number of hex digits required)"
        ));
    }
    Ok((0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).expect("validated hex"))
        .collect())
}

/// The edge server role ([`fluent_replication::EdgeReplica`] + the
/// read-only edge GraphQL plane): the section being present selects it.
/// Tuning fields mirror [`EdgeReplicaConfig`]; an absent field keeps its
/// default.
#[derive(Debug, Default, PartialEq, Deserialize)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub struct EdgeSection {
    /// The master's replication plane address. Required once the section
    /// exists — an `[edge]` that names no master is refused at startup.
    pub master_addr: Option<String>,
    /// Scope `[lo, hi)`; an omitted bound is unbounded on that side.
    pub scope_lo: Option<BytesSpec>,
    pub scope_hi: Option<BytesSpec>,
    /// Periodic slice refresh in seconds (prunes the stream overlay);
    /// `0` refreshes only on re-sync.
    pub refresh_every_secs: Option<u64>,
    pub value_cache_bytes: Option<u64>,
    pub block_cache_size: Option<usize>,
}

impl EdgeSection {
    /// The replica attachment this section describes, over
    /// [`EdgeReplicaConfig::new`]'s defaults. `dir` is the top-level `dir`
    /// (the replica's local cache directory).
    pub fn replica_config(&self, dir: &str) -> Result<EdgeReplicaConfig, String> {
        let Some(master) = &self.master_addr else {
            return Err("[edge] section needs master-addr".into());
        };
        let lo = match &self.scope_lo {
            Some(b) => b.decode().map_err(|e| format!("edge scope-lo: {e}"))?,
            None => Vec::new(),
        };
        let hi = match &self.scope_hi {
            Some(b) => Some(b.decode().map_err(|e| format!("edge scope-hi: {e}"))?),
            None => None,
        };
        let mut cfg = EdgeReplicaConfig::new(master.clone(), dir, lo, hi);
        if let Some(secs) = self.refresh_every_secs {
            cfg.refresh_every = (secs > 0).then(|| Duration::from_secs(secs));
        }
        if let Some(v) = self.value_cache_bytes {
            cfg.value_cache_bytes = v;
        }
        if let Some(v) = self.block_cache_size {
            cfg.block_cache_size = v;
        }
        Ok(cfg)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum IoBackendKey {
    Auto,
    Uring,
    Std,
}

impl From<IoBackendKey> for IoBackend {
    fn from(k: IoBackendKey) -> IoBackend {
        match k {
            IoBackendKey::Auto => IoBackend::Auto,
            IoBackendKey::Uring => IoBackend::Uring,
            IoBackendKey::Std => IoBackend::Std,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CompressionKey {
    None,
    Lz4,
}

impl From<CompressionKey> for Compression {
    fn from(k: CompressionKey) -> Compression {
        match k {
            CompressionKey::None => Compression::None,
            CompressionKey::Lz4 => Compression::Lz4,
        }
    }
}

/// The full [`fluent31::Options`] tunable surface (minus `sync` and
/// `store_name`, which are top-level keys shared with their flags).
#[derive(Debug, Default, PartialEq, Deserialize)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub struct EngineSection {
    pub create_if_missing: Option<bool>,
    /// Runtime kill-switch for the WASM layer (modules, typed GraphQL
    /// fields, triggers). Writes made while disabled never fire triggers —
    /// see `fluent31::Options::wasm_enabled`.
    pub wasm_enabled: Option<bool>,
    pub io_backend: Option<IoBackendKey>,
    pub compression: Option<CompressionKey>,
    pub memtable_size: Option<usize>,
    pub max_immutable_memtables: Option<usize>,
    pub block_size: Option<usize>,
    pub bloom_bits_per_key: Option<usize>,
    pub block_cache_size: Option<usize>,
    pub l0_compaction_trigger: Option<usize>,
    pub tier_width: Option<usize>,
    pub max_levels: Option<usize>,
    pub l0_stall_trigger: Option<usize>,
    pub target_file_size: Option<u64>,
    pub compaction_slice_bytes: Option<u64>,
    pub value_threshold: Option<usize>,
    pub vlog_file_size: Option<u64>,
    pub vlog_gc_ratio: Option<f64>,
    pub max_key_size: Option<usize>,
    pub max_value_size: Option<usize>,
    pub max_txn_write_bytes: Option<usize>,
    pub sub_queue_bytes: Option<usize>,
    pub wasm_fuel: Option<u64>,
    pub wasm_memory_limit: Option<usize>,
    pub execute_retries: Option<usize>,
    pub max_wasm_input: Option<usize>,
    pub max_wasm_output: Option<usize>,
    pub max_wasm_log: Option<usize>,
    pub max_wasm_scans: Option<usize>,
    pub wasm_module_cache: Option<usize>,
    pub trigger_batch: Option<usize>,
    pub trigger_inline_value: Option<usize>,
}

#[derive(Debug)]
pub enum ConfigError {
    Io(std::io::Error),
    Parse(toml::de::Error),
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConfigError::Io(e) => write!(f, "{e}"),
            ConfigError::Parse(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for ConfigError {}

/// Set every present slot of a section onto its target struct.
macro_rules! apply {
    ($sec:expr => $dst:expr, { $($field:ident),* $(,)? }) => {
        $(if let Some(v) = $sec.$field {
            $dst.$field = v;
        })*
    };
}

fn merge<T>(a: Option<T>, b: Option<T>, f: impl FnOnce(T, T) -> T) -> Option<T> {
    match (a, b) {
        (Some(x), Some(y)) => Some(f(x, y)),
        (x, y) => x.or(y),
    }
}

impl FileConfig {
    pub fn load(path: &Path) -> Result<FileConfig, ConfigError> {
        let text = std::fs::read_to_string(path).map_err(ConfigError::Io)?;
        toml::from_str(&text).map_err(ConfigError::Parse)
    }

    /// Field-wise precedence: `self` (the explicit CLI flags) wins over
    /// `file`; unset slots fall through. Sections the CLI has slots in
    /// (`[listen]`, `[graphql]`, `[journal]`) merge field-wise; the
    /// file-only sections pass through whole.
    pub fn overlay(self, file: FileConfig) -> FileConfig {
        FileConfig {
            dir: self.dir.or(file.dir),
            store_name: self.store_name.or(file.store_name),
            sync: self.sync.or(file.sync),
            listen: merge(self.listen, file.listen, |a, b| ListenSection {
                graphql: a.graphql.or(b.graphql),
                replication: a.replication.or(b.replication),
            }),
            graphql: merge(self.graphql, file.graphql, |a, b| GraphqlSection {
                max_body_bytes: a.max_body_bytes.or(b.max_body_bytes),
                fork_max_open: a.fork_max_open.or(b.fork_max_open),
                fork_idle_ttl_secs: a.fork_idle_ttl_secs.or(b.fork_idle_ttl_secs),
            }),
            replication: self.replication.or(file.replication),
            journal: merge(self.journal, file.journal, |a, b| JournalSection {
                dir: a.dir.or(b.dir),
                rotate_bytes: a.rotate_bytes.or(b.rotate_bytes),
                compact_when_deltas_exceed: a
                    .compact_when_deltas_exceed
                    .or(b.compact_when_deltas_exceed),
                compact_min_bytes: a.compact_min_bytes.or(b.compact_min_bytes),
            }),
            engine: self.engine.or(file.engine),
            log: self.log.or(file.log),
            edge: merge(self.edge, file.edge, |a, b| EdgeSection {
                master_addr: a.master_addr.or(b.master_addr),
                scope_lo: a.scope_lo.or(b.scope_lo),
                scope_hi: a.scope_hi.or(b.scope_hi),
                refresh_every_secs: a.refresh_every_secs.or(b.refresh_every_secs),
                value_cache_bytes: a.value_cache_bytes.or(b.value_cache_bytes),
                block_cache_size: a.block_cache_size.or(b.block_cache_size),
            }),
        }
    }

    /// Settings that configure a store of record, present although `[edge]`
    /// selected the edge role — an edge has no store to apply them to, and
    /// a setting that silently does nothing is a config error here like
    /// everywhere else. Empty = no conflict.
    pub fn edge_conflicts(&self) -> Vec<&'static str> {
        let mut out = Vec::new();
        if self.store_name.is_some() {
            out.push("store-name");
        }
        if self.sync.is_some() {
            out.push("sync");
        }
        if self.engine.is_some() {
            out.push("[engine]");
        }
        if self.journal.is_some() {
            out.push("[journal]");
        }
        if self.replication.is_some() {
            out.push("[replication]");
        }
        if self
            .listen
            .as_ref()
            .is_some_and(|l| l.replication.is_some())
        {
            out.push("listen.replication");
        }
        let fork_tuning_set = self
            .graphql
            .as_ref()
            .is_some_and(|g| g.fork_max_open.is_some() || g.fork_idle_ttl_secs.is_some());
        if fork_tuning_set {
            out.push("[graphql] fork tuning");
        }
        out
    }

    /// The edge plane's listen address and limits, applied over
    /// [`crate::EdgeServerConfig::default`].
    pub fn edge_server_config(&self) -> crate::EdgeServerConfig {
        let mut c = crate::EdgeServerConfig::default();
        if let Some(v) = self.listen.as_ref().and_then(|l| l.graphql.as_ref()) {
            c.graphql_addr = v.clone();
        }
        if let Some(v) = self.graphql.as_ref().and_then(|g| g.max_body_bytes) {
            c.max_body_bytes = v;
        }
        if let Some(v) = self.log.as_ref().and_then(|l| l.stats_every_secs) {
            c.stats_every = Duration::from_secs(v);
        }
        c
    }

    /// The listen addresses and per-plane limits, applied over
    /// [`crate::ServerConfig::default`].
    pub fn server_config(&self) -> crate::ServerConfig {
        let mut c = crate::ServerConfig::default();
        if let Some(l) = &self.listen {
            if let Some(v) = &l.graphql {
                c.graphql_addr = v.clone();
            }
            if let Some(v) = &l.replication {
                c.replication_addr = v.clone();
            }
        }
        if let Some(g) = &self.graphql {
            if let Some(v) = g.max_body_bytes {
                c.max_body_bytes = v;
            }
            if let Some(v) = g.fork_max_open {
                c.registry.max_open = v;
            }
            if let Some(v) = g.fork_idle_ttl_secs {
                c.registry.idle_ttl = Duration::from_secs(v);
            }
        }
        if let Some(r) = &self.replication {
            if let Some(v) = r.max_frame_bytes {
                c.replication.max_frame = v;
            }
            if let Some(v) = r.ping_every_ms {
                c.replication.ping_every = Duration::from_millis(v);
            }
        }
        if let Some(l) = &self.log {
            if let Some(v) = l.stats_every_secs {
                c.stats_every = Duration::from_secs(v);
            }
        }
        c
    }

    /// The engine [`Options`]: `[engine]` applied over defaults, plus the
    /// top-level `store-name` and the already-validated sync mode.
    pub fn engine_options(&self, sync: SyncMode) -> Options {
        let mut o = Options {
            sync,
            store_name: self.store_name.clone(),
            ..Options::default()
        };
        if let Some(e) = &self.engine {
            o.io_backend = e.io_backend.map_or(o.io_backend, IoBackend::from);
            o.compression = e.compression.map_or(o.compression, Compression::from);
            apply!(e => o, {
                create_if_missing, wasm_enabled, memtable_size, max_immutable_memtables,
                block_size, bloom_bits_per_key, block_cache_size,
                l0_compaction_trigger, tier_width, max_levels,
                l0_stall_trigger, target_file_size, compaction_slice_bytes, value_threshold,
                vlog_file_size, vlog_gc_ratio, max_key_size, max_value_size,
                max_txn_write_bytes, sub_queue_bytes, wasm_fuel,
                wasm_memory_limit, execute_retries, max_wasm_input,
                max_wasm_output, max_wasm_log, max_wasm_scans,
                wasm_module_cache, trigger_batch, trigger_inline_value,
            });
        }
        o
    }
}

/// The `--sync` / `sync =` grammar: `always` | `never` | `periodic:<ms>`
/// with a positive millisecond count. `None` means the value is invalid.
pub fn parse_sync(s: &str) -> Option<SyncMode> {
    match s {
        "always" => Some(SyncMode::Always),
        "never" => Some(SyncMode::Never),
        _ => {
            let ms = s
                .strip_prefix("periodic:")?
                .parse::<u64>()
                .ok()
                .filter(|ms| *ms > 0)?;
            Some(SyncMode::Periodic {
                every: std::time::Duration::from_millis(ms),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_every_section() {
        let cfg: FileConfig = toml::from_str(
            r#"
            dir = "./data"
            store-name = "prod"
            sync = "periodic:50"

            [listen]
            graphql = "127.0.0.1:1"
            replication = "127.0.0.1:3"

            [graphql]
            max-body-bytes = 1024
            fork-max-open = 2
            fork-idle-ttl-secs = 60

            [replication]
            max-frame-bytes = 2048
            ping-every-ms = 500

            [journal]
            dir = "./jrn"
            rotate-bytes = 1024

            [engine]
            io-backend = "std"
            compression = "lz4"
            memtable-size = 65536
            vlog-gc-ratio = 0.7

            [log]
            stats-every-secs = 5
            "#,
        )
        .unwrap();
        assert_eq!(cfg.log.as_ref().unwrap().stats_every_secs, Some(5));
        assert_eq!(cfg.dir.as_deref(), Some("./data"));
        assert_eq!(
            cfg.listen.as_ref().unwrap().replication.as_deref(),
            Some("127.0.0.1:3")
        );
        assert_eq!(cfg.graphql.as_ref().unwrap().fork_max_open, Some(2));
        assert_eq!(cfg.replication.as_ref().unwrap().ping_every_ms, Some(500));
        assert_eq!(cfg.journal.as_ref().unwrap().dir.as_deref(), Some("./jrn"));
        let e = cfg.engine.as_ref().unwrap();
        assert_eq!(e.io_backend, Some(IoBackendKey::Std));
        assert_eq!(e.compression, Some(CompressionKey::Lz4));
        assert_eq!(e.memtable_size, Some(65536));
        assert_eq!(e.vlog_gc_ratio, Some(0.7));
    }

    #[test]
    fn unknown_key_is_an_error_in_every_scope() {
        assert!(toml::from_str::<FileConfig>("graphqk = \"x\"").is_err());
        assert!(toml::from_str::<FileConfig>("[listen]\ngraphqk = \"x\"").is_err());
        assert!(toml::from_str::<FileConfig>("[engine]\nmemtable-sise = 1").is_err());
        assert!(toml::from_str::<FileConfig>("[engine]\nio-backend = \"turbo\"").is_err());
        assert!(toml::from_str::<FileConfig>("[journal]\nrotate-byte = 1").is_err());
        assert!(toml::from_str::<FileConfig>("[log]\nlevel = \"info\"").is_err());
    }

    #[test]
    fn journal_section_tuning_applies_over_defaults() {
        let cfg: FileConfig = toml::from_str(
            r#"
            [journal]
            dir = "./jrn"
            rotate-bytes = 1024
            compact-min-bytes = 512
            "#,
        )
        .unwrap();
        let j = cfg.journal.as_ref().unwrap();
        assert_eq!(j.dir.as_deref(), Some("./jrn"));
        let c = j.config();
        assert_eq!(c.rotate_bytes, 1024);
        assert_eq!(c.compact_min_bytes, 512);
        let d = JournalConfig::default();
        assert_eq!(c.compact_when_deltas_exceed, d.compact_when_deltas_exceed);
    }

    #[test]
    fn flags_override_file_and_gaps_fall_through() {
        let cli = FileConfig {
            listen: Some(ListenSection {
                replication: Some("cli:1".into()),
                ..ListenSection::default()
            }),
            ..FileConfig::default()
        };
        let file = FileConfig {
            dir: Some("./from-file".into()),
            listen: Some(ListenSection {
                graphql: Some("file:2".into()),
                replication: Some("file:1".into()),
            }),
            engine: Some(EngineSection {
                tier_width: Some(2),
                ..EngineSection::default()
            }),
            ..FileConfig::default()
        };
        let eff = cli.overlay(file);
        let listen = eff.listen.as_ref().unwrap();
        assert_eq!(listen.replication.as_deref(), Some("cli:1"));
        assert_eq!(listen.graphql.as_deref(), Some("file:2"));
        assert_eq!(eff.dir.as_deref(), Some("./from-file"));
        assert_eq!(eff.engine.as_ref().unwrap().tier_width, Some(2));
    }

    #[test]
    fn engine_options_apply_over_defaults() {
        let cfg: FileConfig = toml::from_str(
            r#"
            store-name = "prod"
            [engine]
            io-backend = "std"
            wasm-enabled = false
            memtable-size = 65536
            execute-retries = 9
            "#,
        )
        .unwrap();
        let o = cfg.engine_options(SyncMode::Never);
        assert_eq!(o.io_backend, IoBackend::Std);
        assert!(!o.wasm_enabled);
        assert_eq!(o.memtable_size, 65536);
        assert_eq!(o.execute_retries, 9);
        assert_eq!(o.store_name.as_deref(), Some("prod"));
        assert!(matches!(o.sync, SyncMode::Never));
        let d = Options::default();
        assert_eq!(o.compression, d.compression);
        assert_eq!(o.tier_width, d.tier_width);
        assert_eq!(o.wasm_fuel, d.wasm_fuel);
    }

    #[test]
    fn server_config_applies_sections() {
        let cfg: FileConfig = toml::from_str(
            r#"
            [listen]
            replication = "127.0.0.1:9"
            [graphql]
            fork-max-open = 3
            fork-idle-ttl-secs = 60
            [replication]
            ping-every-ms = 500
            [log]
            stats-every-secs = 0
            "#,
        )
        .unwrap();
        let c = cfg.server_config();
        let d = crate::ServerConfig::default();
        assert_eq!(c.stats_every, Duration::ZERO);
        assert_eq!(c.replication_addr, "127.0.0.1:9");
        assert_eq!(c.graphql_addr, d.graphql_addr);
        assert_eq!(c.registry.max_open, 3);
        assert_eq!(c.registry.idle_ttl, Duration::from_secs(60));
        assert_eq!(c.replication.ping_every, Duration::from_millis(500));
        assert_eq!(c.replication.max_frame, d.replication.max_frame);
        assert_eq!(c.max_body_bytes, d.max_body_bytes);
    }

    #[test]
    fn edge_section_builds_the_replica_config() {
        let cfg: FileConfig = toml::from_str(
            r#"
            dir = "./cache"
            [edge]
            master-addr = "10.0.0.5:8428"
            scope-lo = { text = "user/" }
            scope-hi = { hex = "7573657230" }
            refresh-every-secs = 0
            value-cache-bytes = 1024
            "#,
        )
        .unwrap();
        let r = cfg
            .edge
            .as_ref()
            .unwrap()
            .replica_config("./cache")
            .unwrap();
        assert_eq!(r.master_addr, "10.0.0.5:8428");
        assert_eq!(r.scope_lo, b"user/".to_vec());
        assert_eq!(r.scope_hi.as_deref(), Some(b"user0".as_slice()));
        assert_eq!(r.refresh_every, None);
        assert_eq!(r.value_cache_bytes, 1024);
        let d = EdgeReplicaConfig::new("x", "y", Vec::new(), None);
        assert_eq!(r.block_cache_size, d.block_cache_size);
    }

    #[test]
    fn edge_section_without_master_is_refused() {
        let cfg: FileConfig = toml::from_str("[edge]\nvalue-cache-bytes = 1").unwrap();
        let err = cfg
            .edge
            .as_ref()
            .unwrap()
            .replica_config("./c")
            .unwrap_err();
        assert!(err.contains("master-addr"), "{err}");
    }

    #[test]
    fn edge_bytes_are_exactly_one_encoding() {
        let both = "[edge]\nscope-lo = { text = \"a\", hex = \"61\" }";
        assert!(toml::from_str::<FileConfig>(both).is_err());
        let neither = "[edge]\nscope-lo = { txt = \"a\" }";
        assert!(toml::from_str::<FileConfig>(neither).is_err());
        let bad_hex: FileConfig =
            toml::from_str("[edge]\nmaster-addr = \"m\"\nscope-lo = { hex = \"6\" }").unwrap();
        assert!(bad_hex.edge.unwrap().replica_config("./c").is_err());
    }

    #[test]
    fn edge_conflicts_name_store_only_settings() {
        let cfg: FileConfig = toml::from_str(
            r#"
            store-name = "prod"
            sync = "always"
            [listen]
            graphql = "127.0.0.1:1"
            replication = "127.0.0.1:2"
            [graphql]
            max-body-bytes = 1024
            fork-max-open = 2
            [engine]
            memtable-size = 65536
            [edge]
            master-addr = "m:1"
            "#,
        )
        .unwrap();
        let conflicts = cfg.edge_conflicts();
        for name in [
            "store-name",
            "sync",
            "[engine]",
            "listen.replication",
            "[graphql] fork tuning",
        ] {
            assert!(
                conflicts.contains(&name),
                "{name} missing from {conflicts:?}"
            );
        }
        // the settings the edge role does serve are not conflicts
        assert!(!conflicts.contains(&"[graphql]"));
        let clean: FileConfig =
            toml::from_str("[edge]\nmaster-addr = \"m:1\"\n[graphql]\nmax-body-bytes = 1").unwrap();
        assert!(clean.edge_conflicts().is_empty());
    }

    #[test]
    fn edge_server_config_applies_sections() {
        let cfg: FileConfig = toml::from_str(
            r#"
            [listen]
            graphql = "127.0.0.1:7"
            [graphql]
            max-body-bytes = 2048
            [log]
            stats-every-secs = 0
            [edge]
            master-addr = "m:1"
            "#,
        )
        .unwrap();
        let c = cfg.edge_server_config();
        assert_eq!(c.graphql_addr, "127.0.0.1:7");
        assert_eq!(c.max_body_bytes, 2048);
        assert_eq!(c.stats_every, Duration::ZERO);
    }

    #[test]
    fn sync_grammar() {
        assert!(matches!(parse_sync("always"), Some(SyncMode::Always)));
        assert!(matches!(parse_sync("never"), Some(SyncMode::Never)));
        assert!(matches!(
            parse_sync("periodic:50"),
            Some(SyncMode::Periodic { every }) if every == std::time::Duration::from_millis(50)
        ));
        assert!(parse_sync("periodic:0").is_none());
        assert!(parse_sync("periodic:x").is_none());
        assert!(parse_sync("sometimes").is_none());
    }

    #[test]
    fn journal_dir_flag_keeps_file_tuning() {
        let file: FileConfig =
            toml::from_str("[journal]\ndir = \"./file\"\nrotate-bytes = 7").unwrap();
        let cli = FileConfig {
            journal: Some(JournalSection {
                dir: Some("./cli".into()),
                ..Default::default()
            }),
            ..Default::default()
        };
        let j = cli.overlay(file).journal.unwrap();
        assert_eq!(j.dir.as_deref(), Some("./cli"));
        assert_eq!(j.rotate_bytes, Some(7));
    }
}
