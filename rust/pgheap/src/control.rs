//! Small files beside the relations: `pg_control`, the catalog of column
//! families, and `pg_stat`, the statistics saved at a clean shutdown.
//!
//! `pg_control` holds two 512-byte slots; each write goes to the slot the
//! last write did not use and is synced, and open takes the valid slot with
//! the higher sequence number, so a torn write never loses the previous
//! state. PostgreSQL keeps one copy and relies on 512-byte writes being
//! atomic.

use std::fs::{File, OpenOptions};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};

use nilengine_api::coding::{crc32, get_u32, get_u64};
use nilengine_api::{Error, IoContext, Result};

use crate::file::write_atomic;
use crate::wal::Lsn;

/// The last checkpoint was a shutdown checkpoint.
pub const STATE_SHUTDOWN: u8 = 1;
/// The engine is (or was, when it crashed) running.
pub const STATE_IN_PRODUCTION: u8 = 2;

const CONTROL_MAGIC: u64 = u64::from_le_bytes(*b"NILPGCTL");
const CATALOG_MAGIC: u64 = u64::from_le_bytes(*b"NILPGCAT");
const STATS_MAGIC: u64 = u64::from_le_bytes(*b"NILPGSTA");
const VERSION: u32 = 1;
const SLOT: u64 = 512;
const CATALOG_FILE: &str = "catalog";
const STATS_FILE: &str = "pg_stat";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Control {
    pub seq: u64,
    pub state: u8,
    /// Start of the last checkpoint record.
    pub checkpoint: Lsn,
    /// Where recovery starts replaying.
    pub redo: Lsn,
    pub next_xid: u64,
    pub latest_committed: u64,
}

impl Control {
    fn encode(&self) -> Vec<u8> {
        let mut b = Vec::with_capacity(64);
        b.extend_from_slice(&CONTROL_MAGIC.to_le_bytes());
        b.extend_from_slice(&VERSION.to_le_bytes());
        b.extend_from_slice(&self.seq.to_le_bytes());
        b.push(self.state);
        b.extend_from_slice(&self.checkpoint.to_le_bytes());
        b.extend_from_slice(&self.redo.to_le_bytes());
        b.extend_from_slice(&self.next_xid.to_le_bytes());
        b.extend_from_slice(&self.latest_committed.to_le_bytes());
        let c = crc32(&b);
        b.extend_from_slice(&c.to_le_bytes());
        b
    }

    fn decode(b: &[u8]) -> Option<Control> {
        const LEN: usize = 8 + 4 + 8 + 1 + 8 * 4;
        if b.len() < LEN + 4
            || get_u64(b, 0) != CONTROL_MAGIC
            || get_u32(b, 8) != VERSION
            || crc32(&b[..LEN]) != get_u32(b, LEN)
        {
            return None;
        }
        Some(Control {
            seq: get_u64(b, 12),
            state: b[20],
            checkpoint: get_u64(b, 21),
            redo: get_u64(b, 29),
            next_xid: get_u64(b, 37),
            latest_committed: get_u64(b, 45),
        })
    }
}

pub struct ControlFile {
    file: File,
    path: PathBuf,
    cur: Control,
}

impl ControlFile {
    /// Reads `pg_control`; `None` when the file does not exist.
    pub fn open(path: &Path) -> Result<Option<ControlFile>> {
        let file = match OpenOptions::new().read(true).write(true).open(path) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(Error::io(format!("open {}", path.display()), &e)),
        };
        let mut buf = vec![0u8; 2 * SLOT as usize];
        let n = file
            .read_at(&mut buf, 0)
            .ctx(|| format!("read {}", path.display()))?;
        buf.truncate(n);
        let best = buf
            .chunks(SLOT as usize)
            .filter_map(Control::decode)
            .max_by_key(|c| c.seq)
            .ok_or_else(|| Error::corruption(format!("{} has no valid slot", path.display())))?;
        Ok(Some(ControlFile {
            file,
            path: path.to_path_buf(),
            cur: best,
        }))
    }

    pub fn create(path: &Path, c: Control) -> Result<ControlFile> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)
            .ctx(|| format!("create {}", path.display()))?;
        let mut f = ControlFile {
            file,
            path: path.to_path_buf(),
            cur: Control {
                seq: 0,
                ..c.clone()
            },
        };
        f.write(c)?;
        Ok(f)
    }

    pub fn get(&self) -> &Control {
        &self.cur
    }

    /// Writes `c` into the slot the previous write did not use, and syncs.
    pub fn write(&mut self, mut c: Control) -> Result<()> {
        c.seq = self.cur.seq + 1;
        let slot = c.seq % 2;
        self.file
            .write_all_at(&c.encode(), slot * SLOT)
            .and_then(|_| self.file.sync_data())
            .ctx(|| format!("write {}", self.path.display()))?;
        self.cur = c;
        Ok(())
    }
}

/// Column family names; position n owns relations 16 + 4n to 19 + 4n.
#[derive(Clone, Debug, Default)]
pub struct Catalog {
    pub cfs: Vec<String>,
}

impl Catalog {
    pub fn read(dir: &Path) -> Result<Catalog> {
        let path = dir.join(CATALOG_FILE);
        let b = match std::fs::read(&path) {
            Ok(b) => b,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Catalog::default()),
            Err(e) => return Err(Error::io(format!("read {}", path.display()), &e)),
        };
        let bad = || Error::corruption(format!("{} is damaged", path.display()));
        if b.len() < 20 || get_u64(&b, 0) != CATALOG_MAGIC || get_u32(&b, 8) != VERSION {
            return Err(bad());
        }
        let body = b.len() - 4;
        if crc32(&b[..body]) != get_u32(&b, body) {
            return Err(bad());
        }
        let n = get_u32(&b, 12) as usize;
        let mut pos = 16;
        let mut cfs = Vec::with_capacity(n);
        for _ in 0..n {
            if pos + 2 > body {
                return Err(bad());
            }
            let len = u16::from_le_bytes([b[pos], b[pos + 1]]) as usize;
            pos += 2;
            let name = b.get(pos..pos + len).ok_or_else(bad)?;
            cfs.push(String::from_utf8(name.to_vec()).map_err(|_| bad())?);
            pos += len;
        }
        Ok(Catalog { cfs })
    }

    pub fn write(&self, dir: &Path) -> Result<()> {
        let mut b = Vec::new();
        b.extend_from_slice(&CATALOG_MAGIC.to_le_bytes());
        b.extend_from_slice(&VERSION.to_le_bytes());
        b.extend_from_slice(&(self.cfs.len() as u32).to_le_bytes());
        for n in &self.cfs {
            b.extend_from_slice(&(n.len() as u16).to_le_bytes());
            b.extend_from_slice(n.as_bytes());
        }
        let c = crc32(&b);
        b.extend_from_slice(&c.to_le_bytes());
        write_atomic(dir, CATALOG_FILE, &b)
    }
}

/// Saves one row of counters per column family.
pub fn write_stats(dir: &Path, rows: &[Vec<u64>]) -> Result<()> {
    let mut b = Vec::new();
    b.extend_from_slice(&STATS_MAGIC.to_le_bytes());
    b.extend_from_slice(&(rows.len() as u32).to_le_bytes());
    for r in rows {
        b.extend_from_slice(&(r.len() as u32).to_le_bytes());
        for v in r {
            b.extend_from_slice(&v.to_le_bytes());
        }
    }
    let c = crc32(&b);
    b.extend_from_slice(&c.to_le_bytes());
    write_atomic(dir, STATS_FILE, &b)
}

/// Reads and deletes the statistics file, so counters saved at one clean
/// shutdown are never reused after a later crash (PostgreSQL discards its
/// statistics after crash recovery too).
pub fn take_stats(dir: &Path) -> Option<Vec<Vec<u64>>> {
    let path = dir.join(STATS_FILE);
    let b = std::fs::read(&path).ok()?;
    let _ = std::fs::remove_file(&path);
    if b.len() < 16 || get_u64(&b, 0) != STATS_MAGIC {
        return None;
    }
    let body = b.len() - 4;
    if crc32(&b[..body]) != get_u32(&b, body) {
        return None;
    }
    let n = get_u32(&b, 8) as usize;
    let mut pos = 12;
    let mut rows = Vec::with_capacity(n);
    for _ in 0..n {
        let len = get_u32(b.get(pos..pos + 4)?, 0) as usize;
        pos += 4;
        let mut r = Vec::with_capacity(len);
        for _ in 0..len {
            r.push(get_u64(b.get(pos..pos + 8)?, 0));
            pos += 8;
        }
        rows.push(r);
    }
    Some(rows)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn control_survives_a_torn_slot() {
        let dir = std::env::temp_dir().join(format!("pgheap-ctl-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("pg_control");
        let base = Control {
            seq: 0,
            state: STATE_SHUTDOWN,
            checkpoint: 16,
            redo: 16,
            next_xid: 3,
            latest_committed: 0,
        };
        let mut f = ControlFile::create(&path, base.clone()).unwrap();
        f.write(Control {
            redo: 900,
            ..base.clone()
        })
        .unwrap();
        assert_eq!(ControlFile::open(&path).unwrap().unwrap().get().redo, 900);
        // Tear the newer slot: the older one wins.
        let seq = f.get().seq;
        let file = OpenOptions::new().write(true).open(&path).unwrap();
        file.write_all_at(&[0xff; 20], (seq % 2) * SLOT + 30)
            .unwrap();
        assert_eq!(ControlFile::open(&path).unwrap().unwrap().get().redo, 16);
        let cat = Catalog {
            cfs: vec!["default".into(), "alpha".into()],
        };
        cat.write(&dir).unwrap();
        assert_eq!(Catalog::read(&dir).unwrap().cfs, cat.cfs);
        write_stats(&dir, &[vec![1, 2, 3], vec![4]]).unwrap();
        assert_eq!(take_stats(&dir), Some(vec![vec![1, 2, 3], vec![4]]));
        assert_eq!(take_stats(&dir), None);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
