//! Engine abstraction behind the wire dispatch: a full read-write `Db` or
//! a read-only edge replica serve the same protocol. Write/WASM/durability
//! ops default to a clean INVALID ("read-only replica") so a read-only
//! backend only implements the read surface.

use fluent31::edge::EdgeStore;
use fluent31::{Db, WriteBatch};

fn read_only(op: &str) -> fluent31::Error {
    fluent31::Error::InvalidArgument(format!("read-only replica: {op} unsupported"))
}

/// One page of scan results: visible pairs plus a has-more flag.
pub type ScanPage = (Vec<(Vec<u8>, Vec<u8>)>, bool);

pub trait WireBackend: Send + Sync + 'static {
    fn get(&self, key: &[u8]) -> fluent31::Result<Option<Vec<u8>>>;
    fn scan(
        &self,
        lo: Option<&[u8]>,
        hi: Option<&[u8]>,
        reverse: bool,
        limit: usize,
    ) -> fluent31::Result<ScanPage>;
    /// Human-readable stats (format-unstable by design, see WIRE.md).
    fn stats_text(&self) -> fluent31::Result<String>;

    fn put(&self, _key: Vec<u8>, _value: Vec<u8>) -> fluent31::Result<()> {
        Err(read_only("PUT"))
    }
    fn delete(&self, _key: Vec<u8>) -> fluent31::Result<()> {
        Err(read_only("DEL"))
    }
    fn write_batch(&self, _batch: WriteBatch) -> fluent31::Result<()> {
        Err(read_only("BATCH"))
    }
    fn query(&self, _name: &str, _input: &[u8]) -> fluent31::Result<Vec<u8>> {
        Err(read_only("QUERY"))
    }
    fn execute(&self, _name: &str, _input: &[u8]) -> fluent31::Result<Vec<u8>> {
        Err(read_only("EXEC"))
    }
    fn sync_wal(&self) -> fluent31::Result<()> {
        Err(read_only("SYNC_WAL"))
    }
}

impl WireBackend for Db {
    fn get(&self, key: &[u8]) -> fluent31::Result<Option<Vec<u8>>> {
        Db::get(self, key)
    }

    fn scan(
        &self,
        lo: Option<&[u8]>,
        hi: Option<&[u8]>,
        reverse: bool,
        limit: usize,
    ) -> fluent31::Result<ScanPage> {
        let it = self.iter(lo, hi, reverse)?;
        let mut pairs: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        let mut has_more = false;
        for item in it {
            let (k, v) = item?;
            if pairs.len() == limit {
                has_more = true;
                break;
            }
            pairs.push((k, v));
        }
        Ok((pairs, has_more))
    }

    fn stats_text(&self) -> fluent31::Result<String> {
        Ok(format!("{:#?}", self.stats()))
    }

    fn put(&self, key: Vec<u8>, value: Vec<u8>) -> fluent31::Result<()> {
        Db::put(self, key, value)
    }

    fn delete(&self, key: Vec<u8>) -> fluent31::Result<()> {
        Db::delete(self, key)
    }

    fn write_batch(&self, batch: WriteBatch) -> fluent31::Result<()> {
        self.write(batch)
    }

    fn query(&self, name: &str, input: &[u8]) -> fluent31::Result<Vec<u8>> {
        Db::query(self, name, input)
    }

    fn execute(&self, name: &str, input: &[u8]) -> fluent31::Result<Vec<u8>> {
        Db::execute(self, name, input)
    }

    fn sync_wal(&self) -> fluent31::Result<()> {
        Db::sync_wal(self)
    }
}

/// Read-only serving over an edge replica: GET/SCAN/STATS work, everything
/// else answers INVALID. Out-of-scope GETs surface the store's own
/// InvalidArgument; scans clamp into the scope (see `EdgeStore::scan`).
impl WireBackend for EdgeStore {
    fn get(&self, key: &[u8]) -> fluent31::Result<Option<Vec<u8>>> {
        EdgeStore::get(self, key)
    }

    fn scan(
        &self,
        lo: Option<&[u8]>,
        hi: Option<&[u8]>,
        reverse: bool,
        limit: usize,
    ) -> fluent31::Result<ScanPage> {
        EdgeStore::scan(self, lo, hi, reverse, limit)
    }

    fn stats_text(&self) -> fluent31::Result<String> {
        Ok(format!("{:#?}", self.stats()))
    }
}
