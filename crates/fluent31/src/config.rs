use std::path::PathBuf;

/// When appends are pushed to stable storage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncMode {
    /// fdatasync the vlog and WAL on every write batch (group-committed:
    /// concurrent writers share one fsync). Durable to the last acked op.
    Always,
    /// Acks at memory speed; a background timer fsyncs the WAL and vlog
    /// head every `every`, bounding crash loss to roughly that window.
    /// `Db::sync_wal` provides an explicit durability barrier on demand.
    /// (Never yields a corrupt store: recovery truncates at the torn tail.)
    Periodic { every: std::time::Duration },
    /// Leave flushing entirely to the OS page cache. Crash may lose recent
    /// tail writes (never corrupt: recovery truncates at the torn tail).
    Never,
}

/// Per-block SST compression codec. Each block records its codec in the
/// trailer, so reads never depend on this option: a store written with
/// `Lz4` stays readable after reopening with `None` (and vice versa).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Compression {
    /// Store blocks raw (compatible with format-1-only readers).
    None,
    /// LZ4 block compression for data and index blocks. Blocks that don't
    /// shrink are stored raw, and a table's format version is bumped only
    /// when it actually contains a compressed block.
    Lz4,
}

/// IO backend selection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IoBackend {
    /// io_uring when the platform/kernel supports it, otherwise portable IO.
    Auto,
    /// Force io_uring; `Db::open` fails where unsupported.
    Uring,
    /// Force portable positioned IO (pread/pwrite).
    Std,
}

/// Tunables. `Options::default()` is a sane starting point for tests and
/// small workloads.
#[derive(Debug, Clone)]
pub struct Options {
    pub create_if_missing: bool,
    pub sync: SyncMode,
    pub io_backend: IoBackend,

    /// Operator-chosen, fleet-unique store name. Fixes the deterministic
    /// store identity (see `identity.rs`): used at creation to mint it, on
    /// reopen it must match the persisted name, and an existing unnamed
    /// store adopts it once. Replication requires a named store; purely
    /// embedded use can leave this `None`.
    pub store_name: Option<String>,

    /// Freeze + flush the memtable once its in-memory footprint passes this.
    pub memtable_size: usize,
    /// Max frozen (unflushed) memtables before writers stall.
    pub max_immutable_memtables: usize,

    /// Target uncompressed data-block size in SSTs.
    pub block_size: usize,
    /// Per-block SST compression codec (applies to newly written tables).
    pub compression: Compression,
    /// Bloom filter budget per key, in bits.
    pub bloom_bits_per_key: usize,
    /// Shared block cache capacity in bytes.
    pub block_cache_size: usize,

    /// Runs in L0 that trigger an L0 -> L1 tier merge.
    pub l0_compaction_trigger: usize,
    /// Runs per level (tier width) that trigger a merge to the next level.
    pub tier_width: usize,
    /// Total number of levels; the last is kept as a single leveled run.
    pub max_levels: usize,
    /// L0 run count at which writers stall until compaction catches up.
    pub l0_stall_trigger: usize,

    /// Compaction output runs split into fragments of roughly this size,
    /// bounding per-file blooms/indexes and transient merge space.
    pub target_file_size: u64,

    /// Values >= this many bytes go to the value log; smaller stay inline in
    /// the LSM tree. 0 separates everything; usize::MAX disables separation.
    pub value_threshold: usize,
    /// Seal + rotate the head vlog file at this size.
    pub vlog_file_size: u64,
    /// A sealed vlog file becomes a GC victim when at least this fraction of
    /// its bytes are known-discarded.
    pub vlog_gc_ratio: f64,

    /// Hard cap on a single key.
    pub max_key_size: usize,
    /// Hard cap on a single value.
    pub max_value_size: usize,
    /// Cap on one transaction's buffered write set (also bounds WASM
    /// executor writes).
    pub max_txn_write_bytes: usize,

    /// Cap on buffered, not-yet-consumed bytes per replication
    /// subscription; a subscriber that falls further behind is dropped
    /// (it must re-sync) instead of growing an unbounded queue.
    pub sub_queue_bytes: usize,

    /// Fuel budget per WASM invocation (roughly: abstract instructions).
    pub wasm_fuel: u64,
    /// Max linear memory a WASM invocation may grow to, in bytes.
    pub wasm_memory_limit: usize,
    /// Automatic re-runs of an executor whose commit hit a conflict.
    pub execute_retries: usize,
    /// Cap on input passed to a WASM invocation (fits in i32 for the ABI).
    pub max_wasm_input: usize,
    /// Cap on bytes a WASM invocation may emit via output_write.
    pub max_wasm_output: usize,
    /// Cap on bytes a WASM invocation may emit via log.
    pub max_wasm_log: usize,
    /// Cap on concurrently open scan handles per WASM invocation.
    pub max_wasm_scans: usize,
    /// Cap on compiled modules kept in the in-memory cache.
    pub wasm_module_cache: usize,
    /// Max touched keys handed to one trigger invocation (the runner drains
    /// a backlog in chunks of this many events).
    pub trigger_batch: usize,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            create_if_missing: true,
            sync: SyncMode::Always,
            io_backend: IoBackend::Auto,
            store_name: None,
            memtable_size: 8 << 20,
            max_immutable_memtables: 2,
            block_size: 8 << 10,
            compression: Compression::None,
            bloom_bits_per_key: 10,
            block_cache_size: 64 << 20,
            l0_compaction_trigger: 4,
            tier_width: 4,
            max_levels: 7,
            l0_stall_trigger: 12,
            target_file_size: 64 << 20,
            value_threshold: 4096,
            vlog_file_size: 128 << 20,
            vlog_gc_ratio: 0.5,
            max_key_size: 16 << 10,
            max_value_size: 256 << 20,
            max_txn_write_bytes: 256 << 20,
            sub_queue_bytes: 8 << 20,
            wasm_fuel: 1_000_000_000,
            wasm_memory_limit: 64 << 20,
            execute_retries: 3,
            max_wasm_input: 64 << 20,
            max_wasm_output: 32 << 20,
            max_wasm_log: 1 << 20,
            max_wasm_scans: 64,
            wasm_module_cache: 32,
            trigger_batch: 512,
        }
    }
}

/// Internal: resolved paths for a database directory.
#[derive(Debug, Clone)]
pub(crate) struct DbPaths {
    pub dir: PathBuf,
}

impl DbPaths {
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        DbPaths { dir: dir.into() }
    }
    pub fn current(&self) -> PathBuf {
        self.dir.join("CURRENT")
    }
    pub fn manifest(&self, gen: u64) -> PathBuf {
        self.dir.join(format!("MANIFEST-{gen:06}"))
    }
    pub fn wal(&self, id: u64) -> PathBuf {
        self.dir.join(format!("wal-{id:06}.log"))
    }
    pub fn table(&self, id: u64) -> PathBuf {
        self.dir.join(format!("sst-{id:06}.tbl"))
    }
    pub fn vlog(&self, id: u64) -> PathBuf {
        self.dir.join(format!("vlog-{id:06}.vlog"))
    }
    pub fn archive_root(&self) -> PathBuf {
        self.dir.join("archive")
    }
    pub fn archive(&self, name: &str) -> PathBuf {
        self.archive_root().join(name)
    }
}
