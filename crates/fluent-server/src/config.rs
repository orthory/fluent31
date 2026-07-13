//! TOML config file for the server binary (`--config <path>`).
//!
//! Every key mirrors a CLI flag, Cargo-style kebab-case; an explicit flag
//! overrides its file value, the file overrides the built-in default.
//! Unknown keys are an error — a typo must not silently fall back.
//!
//! ```toml
//! dir = "./data"
//! store-name = "prod"
//! sync = "periodic:50"          # always | never | periodic:<ms>
//! graphql = "127.0.0.1:8317"
//! wire = "127.0.0.1:8427"
//! replication = "127.0.0.1:8428"
//! max-body-bytes = 33554432
//! ```

use std::path::Path;

use fluent31::SyncMode;
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
    pub graphql: Option<String>,
    pub wire: Option<String>,
    pub replication: Option<String>,
    pub max_body_bytes: Option<usize>,
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

impl FileConfig {
    pub fn load(path: &Path) -> Result<FileConfig, ConfigError> {
        let text = std::fs::read_to_string(path).map_err(ConfigError::Io)?;
        toml::from_str(&text).map_err(ConfigError::Parse)
    }

    /// Field-wise precedence: `self` (the explicit CLI flags) wins over
    /// `file`; unset slots fall through.
    pub fn overlay(self, file: FileConfig) -> FileConfig {
        FileConfig {
            dir: self.dir.or(file.dir),
            store_name: self.store_name.or(file.store_name),
            sync: self.sync.or(file.sync),
            graphql: self.graphql.or(file.graphql),
            wire: self.wire.or(file.wire),
            replication: self.replication.or(file.replication),
            max_body_bytes: self.max_body_bytes.or(file.max_body_bytes),
        }
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
    fn parses_every_key() {
        let cfg: FileConfig = toml::from_str(
            r#"
            dir = "./data"
            store-name = "prod"
            sync = "periodic:50"
            graphql = "127.0.0.1:1"
            wire = "127.0.0.1:2"
            replication = "127.0.0.1:3"
            max-body-bytes = 1024
            "#,
        )
        .unwrap();
        assert_eq!(cfg.dir.as_deref(), Some("./data"));
        assert_eq!(cfg.store_name.as_deref(), Some("prod"));
        assert_eq!(cfg.sync.as_deref(), Some("periodic:50"));
        assert_eq!(cfg.graphql.as_deref(), Some("127.0.0.1:1"));
        assert_eq!(cfg.wire.as_deref(), Some("127.0.0.1:2"));
        assert_eq!(cfg.replication.as_deref(), Some("127.0.0.1:3"));
        assert_eq!(cfg.max_body_bytes, Some(1024));
    }

    #[test]
    fn unknown_key_is_an_error() {
        let err = toml::from_str::<FileConfig>("graphqk = \"127.0.0.1:1\"").unwrap_err();
        assert!(err.to_string().contains("graphqk"), "{err}");
    }

    #[test]
    fn flags_override_file_and_gaps_fall_through() {
        let cli = FileConfig {
            wire: Some("cli:1".into()),
            ..FileConfig::default()
        };
        let file = FileConfig {
            dir: Some("./from-file".into()),
            wire: Some("file:1".into()),
            ..FileConfig::default()
        };
        let eff = cli.overlay(file);
        assert_eq!(eff.wire.as_deref(), Some("cli:1"));
        assert_eq!(eff.dir.as_deref(), Some("./from-file"));
        assert_eq!(eff.graphql, None);
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
