//! Durable metadata: a full-snapshot manifest per change, flipped via
//! CURRENT (tmp + fsync + rename + dir fsync at every step).
//!
//! `MANIFEST-<gen>`: `[magic u64][format u32][payload][crc32c u32]`.

use std::collections::BTreeMap;
use std::path::Path;

use crate::coding::{crc32, put_u32, put_u64, put_uvarint, Reader};
use crate::config::DbPaths;
use crate::error::{corrupt, Error, Result};
use crate::io::{atomic_write, sync_dir};

const MANIFEST_MAGIC: u64 = 0xf1_15_e7_31_3a_a1_0001;
const MANIFEST_FORMAT: u32 = 1;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct RunMeta {
    pub id: u64,
    pub table_ids: Vec<u64>,
}

/// Everything the engine must know to reopen the directory. Table metadata
/// lives in the (self-describing) table files; the manifest only carries
/// structure.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ManifestData {
    pub next_file_id: u64,
    /// Highest seqno guaranteed present in the tables (flush watermark).
    pub last_flushed_seqno: u64,
    /// WAL files with id >= wal_floor are live and must be replayed in id
    /// order at recovery; ids below are flushed and deletable. Files newer
    /// than anything recorded here are replayed too — recovery must NEVER
    /// discard a wal-*.log with id >= wal_floor (DESIGN.md §5).
    pub wal_floor: u64,
    /// Levels, runs newest-first, fragments key-ordered.
    pub levels: Vec<Vec<RunMeta>>,
    /// Live vlog files (sealed + head).
    pub vlog_live: Vec<u64>,
    pub vlog_head: u64,
    /// GC victims awaiting physical deletion: (file_id, retired_at_seqno).
    pub vlog_retired: Vec<(u64, u64)>,
    /// Discard stats: (vlog_file_id, dead_bytes).
    pub discard: Vec<(u64, u64)>,
}

impl ManifestData {
    pub fn discard_map(&self) -> BTreeMap<u64, u64> {
        self.discard.iter().copied().collect()
    }

    fn encode(&self) -> Vec<u8> {
        let mut p = Vec::new();
        put_u64(&mut p, self.next_file_id);
        put_u64(&mut p, self.last_flushed_seqno);
        put_u64(&mut p, self.wal_floor);
        put_uvarint(&mut p, self.levels.len() as u64);
        for level in &self.levels {
            put_uvarint(&mut p, level.len() as u64);
            for run in level {
                put_u64(&mut p, run.id);
                put_uvarint(&mut p, run.table_ids.len() as u64);
                for t in &run.table_ids {
                    put_u64(&mut p, *t);
                }
            }
        }
        put_uvarint(&mut p, self.vlog_live.len() as u64);
        for v in &self.vlog_live {
            put_u64(&mut p, *v);
        }
        put_u64(&mut p, self.vlog_head);
        put_uvarint(&mut p, self.vlog_retired.len() as u64);
        for (f, s) in &self.vlog_retired {
            put_u64(&mut p, *f);
            put_u64(&mut p, *s);
        }
        put_uvarint(&mut p, self.discard.len() as u64);
        for (f, b) in &self.discard {
            put_u64(&mut p, *f);
            put_u64(&mut p, *b);
        }

        let mut out = Vec::with_capacity(p.len() + 16);
        put_u64(&mut out, MANIFEST_MAGIC);
        put_u32(&mut out, MANIFEST_FORMAT);
        out.extend_from_slice(&p);
        let crc = crc32(&out);
        put_u32(&mut out, crc);
        out
    }

    fn decode(buf: &[u8]) -> Result<ManifestData> {
        if buf.len() < 16 {
            return Err(corrupt("manifest too small"));
        }
        let body_end = buf.len() - 4;
        let stored = u32::from_le_bytes(buf[body_end..].try_into().unwrap());
        if crc32(&buf[..body_end]) != stored {
            return Err(corrupt("manifest crc mismatch"));
        }
        let mut r = Reader::new(&buf[..body_end]);
        if r.u64()? != MANIFEST_MAGIC {
            return Err(corrupt("bad manifest magic"));
        }
        let format = r.u32()?;
        if format != MANIFEST_FORMAT {
            return Err(corrupt(format!("unsupported manifest format {format}")));
        }
        let next_file_id = r.u64()?;
        let last_flushed_seqno = r.u64()?;
        let wal_floor = r.u64()?;
        let nlevels = r.uvarint()? as usize;
        let mut levels = Vec::with_capacity(nlevels.min(1024));
        for _ in 0..nlevels {
            let nruns = r.uvarint()? as usize;
            let mut runs = Vec::with_capacity(nruns.min(1024));
            for _ in 0..nruns {
                let id = r.u64()?;
                let ntables = r.uvarint()? as usize;
                let mut table_ids = Vec::with_capacity(ntables.min(1024));
                for _ in 0..ntables {
                    table_ids.push(r.u64()?);
                }
                runs.push(RunMeta { id, table_ids });
            }
            levels.push(runs);
        }
        let nv = r.uvarint()? as usize;
        let mut vlog_live = Vec::with_capacity(nv.min(1024));
        for _ in 0..nv {
            vlog_live.push(r.u64()?);
        }
        let vlog_head = r.u64()?;
        let nr = r.uvarint()? as usize;
        let mut vlog_retired = Vec::with_capacity(nr.min(1024));
        for _ in 0..nr {
            vlog_retired.push((r.u64()?, r.u64()?));
        }
        let nd = r.uvarint()? as usize;
        let mut discard = Vec::with_capacity(nd.min(1024));
        for _ in 0..nd {
            discard.push((r.u64()?, r.u64()?));
        }
        Ok(ManifestData {
            next_file_id,
            last_flushed_seqno,
            wal_floor,
            levels,
            vlog_live,
            vlog_head,
            vlog_retired,
            discard,
        })
    }
}

/// Write `MANIFEST-<gen>` durably and flip CURRENT to it. Returns only after
/// everything (manifest data, CURRENT contents, both dir entries) is fsynced.
pub(crate) fn save(paths: &DbPaths, gen: u64, data: &ManifestData) -> Result<()> {
    let mpath = paths.manifest(gen);
    {
        use std::io::Write;
        let mut f = std::fs::File::create(&mpath)?;
        f.write_all(&data.encode())?;
        f.sync_all()?;
    }
    sync_dir(&paths.dir)?;
    atomic_write(&paths.current(), format!("MANIFEST-{gen:06}\n").as_bytes())?;
    Ok(())
}

/// Read CURRENT and the manifest it names. Returns (gen, data).
pub(crate) fn load(paths: &DbPaths) -> Result<(u64, ManifestData)> {
    let cur = std::fs::read_to_string(paths.current())
        .map_err(|e| Error::Io(e))?;
    let name = cur.trim();
    let gen: u64 = name
        .strip_prefix("MANIFEST-")
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| corrupt(format!("bad CURRENT contents: {name:?}")))?;
    let bytes = std::fs::read(paths.dir.join(name))?;
    Ok((gen, ManifestData::decode(&bytes)?))
}

pub(crate) fn exists(dir: &Path) -> bool {
    dir.join("CURRENT").exists()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> ManifestData {
        ManifestData {
            next_file_id: 42,
            last_flushed_seqno: 1000,
            wal_floor: 7,
            levels: vec![
                vec![
                    RunMeta {
                        id: 10,
                        table_ids: vec![11, 12],
                    },
                    RunMeta {
                        id: 8,
                        table_ids: vec![9],
                    },
                ],
                vec![],
                vec![RunMeta {
                    id: 3,
                    table_ids: vec![4, 5, 6],
                }],
            ],
            vlog_live: vec![2, 13],
            vlog_head: 13,
            vlog_retired: vec![(1, 900)],
            discard: vec![(2, 4096)],
        }
    }

    #[test]
    fn encode_decode_roundtrip() {
        let d = sample();
        let enc = d.encode();
        assert_eq!(ManifestData::decode(&enc).unwrap(), d);
    }

    #[test]
    fn corruption_detected() {
        let d = sample();
        let mut enc = d.encode();
        let mid = enc.len() / 2;
        enc[mid] ^= 0xff;
        assert!(ManifestData::decode(&enc).is_err());
        assert!(ManifestData::decode(&enc[..enc.len() - 1]).is_err());
    }

    #[test]
    fn save_load_flip() {
        let dir = tempfile::tempdir().unwrap();
        let paths = DbPaths::new(dir.path());
        let d1 = sample();
        save(&paths, 1, &d1).unwrap();
        let (gen, got) = load(&paths).unwrap();
        assert_eq!(gen, 1);
        assert_eq!(got, d1);

        let mut d2 = d1.clone();
        d2.last_flushed_seqno = 2000;
        save(&paths, 2, &d2).unwrap();
        let (gen, got) = load(&paths).unwrap();
        assert_eq!(gen, 2);
        assert_eq!(got, d2);
    }
}
