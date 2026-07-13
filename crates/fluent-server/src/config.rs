//! TOML config file for the server binary (`--config <path>`).
//!
//! Top-level keys and `[listen]` mirror CLI flags, Cargo-style kebab-case;
//! the tuning sections are file-only and cover everything the composed
//! crates expose as configuration: `[engine]` is the full
//! [`fluent31::Options`] tunable surface, `[graphql]` / `[wire]` /
//! `[replication]` are the per-plane limits. An explicit flag overrides
//! its file value, the file overrides the built-in default. Unknown keys
//! are an error — a typo must not silently fall back.
//!
//! ```toml
//! dir = "./data"
//! store-name = "prod"
//! sync = "periodic:50"          # always | never | periodic:<ms>
//!
//! [listen]
//! graphql = "127.0.0.1:8317"
//! wire = "127.0.0.1:8427"
//! replication = "127.0.0.1:8428"
//!
//! [graphql]
//! max-body-bytes = 33554432
//! fork-max-open = 8             # open fork instances beyond the primary
//! fork-idle-ttl-secs = 300
//!
//! [wire]
//! max-frame-bytes = 269484032
//!
//! [replication]
//! max-frame-bytes = 1048576
//! ping-every-ms = 2000
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

use fluent31::{Compression, IoBackend, Options, SyncMode};
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
    pub wire: Option<WireSection>,
    pub replication: Option<ReplicationSection>,
    pub engine: Option<EngineSection>,
}

#[derive(Debug, Default, PartialEq, Deserialize)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub struct ListenSection {
    pub graphql: Option<String>,
    pub wire: Option<String>,
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
pub struct WireSection {
    pub max_frame_bytes: Option<usize>,
}

#[derive(Debug, Default, PartialEq, Deserialize)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub struct ReplicationSection {
    pub max_frame_bytes: Option<usize>,
    pub ping_every_ms: Option<u64>,
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
    /// (`[listen]`, `[graphql]`) merge field-wise; the file-only sections
    /// pass through whole.
    pub fn overlay(self, file: FileConfig) -> FileConfig {
        FileConfig {
            dir: self.dir.or(file.dir),
            store_name: self.store_name.or(file.store_name),
            sync: self.sync.or(file.sync),
            listen: merge(self.listen, file.listen, |a, b| ListenSection {
                graphql: a.graphql.or(b.graphql),
                wire: a.wire.or(b.wire),
                replication: a.replication.or(b.replication),
            }),
            graphql: merge(self.graphql, file.graphql, |a, b| GraphqlSection {
                max_body_bytes: a.max_body_bytes.or(b.max_body_bytes),
                fork_max_open: a.fork_max_open.or(b.fork_max_open),
                fork_idle_ttl_secs: a.fork_idle_ttl_secs.or(b.fork_idle_ttl_secs),
            }),
            wire: self.wire.or(file.wire),
            replication: self.replication.or(file.replication),
            engine: self.engine.or(file.engine),
        }
    }

    /// The listen addresses and per-plane limits, applied over
    /// [`crate::ServerConfig::default`].
    pub fn server_config(&self) -> crate::ServerConfig {
        let mut c = crate::ServerConfig::default();
        if let Some(l) = &self.listen {
            if let Some(v) = &l.graphql {
                c.graphql_addr = v.clone();
            }
            if let Some(v) = &l.wire {
                c.wire_addr = v.clone();
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
        if let Some(w) = &self.wire {
            if let Some(v) = w.max_frame_bytes {
                c.wire.max_frame = v;
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
                l0_stall_trigger, target_file_size, value_threshold,
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
            let ms = s.strip_prefix("periodic:")?.parse::<u64>().ok().filter(|ms| *ms > 0)?;
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
            wire = "127.0.0.1:2"
            replication = "127.0.0.1:3"

            [graphql]
            max-body-bytes = 1024
            fork-max-open = 2
            fork-idle-ttl-secs = 60

            [wire]
            max-frame-bytes = 4096

            [replication]
            max-frame-bytes = 2048
            ping-every-ms = 500

            [engine]
            io-backend = "std"
            compression = "lz4"
            memtable-size = 65536
            vlog-gc-ratio = 0.7
            "#,
        )
        .unwrap();
        assert_eq!(cfg.dir.as_deref(), Some("./data"));
        assert_eq!(cfg.listen.as_ref().unwrap().wire.as_deref(), Some("127.0.0.1:2"));
        assert_eq!(cfg.graphql.as_ref().unwrap().fork_max_open, Some(2));
        assert_eq!(cfg.wire.as_ref().unwrap().max_frame_bytes, Some(4096));
        assert_eq!(cfg.replication.as_ref().unwrap().ping_every_ms, Some(500));
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
    }

    #[test]
    fn flags_override_file_and_gaps_fall_through() {
        let cli = FileConfig {
            listen: Some(ListenSection {
                wire: Some("cli:1".into()),
                ..ListenSection::default()
            }),
            ..FileConfig::default()
        };
        let file = FileConfig {
            dir: Some("./from-file".into()),
            listen: Some(ListenSection {
                graphql: Some("file:2".into()),
                wire: Some("file:1".into()),
                ..ListenSection::default()
            }),
            engine: Some(EngineSection {
                tier_width: Some(2),
                ..EngineSection::default()
            }),
            ..FileConfig::default()
        };
        let eff = cli.overlay(file);
        let listen = eff.listen.as_ref().unwrap();
        assert_eq!(listen.wire.as_deref(), Some("cli:1"));
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
            wire = "127.0.0.1:9"
            [graphql]
            fork-max-open = 3
            fork-idle-ttl-secs = 60
            [wire]
            max-frame-bytes = 4096
            [replication]
            ping-every-ms = 500
            "#,
        )
        .unwrap();
        let c = cfg.server_config();
        let d = crate::ServerConfig::default();
        assert_eq!(c.wire_addr, "127.0.0.1:9");
        assert_eq!(c.graphql_addr, d.graphql_addr);
        assert_eq!(c.registry.max_open, 3);
        assert_eq!(c.registry.idle_ttl, Duration::from_secs(60));
        assert_eq!(c.wire.max_frame, 4096);
        assert_eq!(c.replication.ping_every, Duration::from_millis(500));
        assert_eq!(c.replication.max_frame, d.replication.max_frame);
        assert_eq!(c.max_body_bytes, d.max_body_bytes);
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
}
