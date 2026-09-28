//! Log files, used for the write-ahead log and the manifest. A record is
//! `[length u32 LE][crc32 u32 LE][payload]`, where the CRC-32 covers the four
//! length bytes and the payload. Readers stop at the first record that is
//! short, zero-length or fails its checksum and treat the rest of the file
//! as lost: RocksDB's point-in-time recovery, which is what `internal/store`
//! configures.

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use nilengine_api::coding::crc32_parts;
use nilengine_api::{CrashMode, Error, IoContext, Result};

const HEADER: usize = 8;
const MAX_RECORD: usize = 1 << 30;

pub struct LogWriter {
    file: File,
    path: PathBuf,
    len: u64,
    synced: u64,
}

impl LogWriter {
    pub fn create(path: &Path) -> Result<LogWriter> {
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .ctx(|| format!("create {}", path.display()))?;
        Ok(LogWriter {
            file,
            path: path.to_path_buf(),
            len: 0,
            synced: 0,
        })
    }

    /// Appends one record made of `parts` with a single write call.
    pub fn add_record(&mut self, parts: &[&[u8]]) -> Result<()> {
        let n: usize = parts.iter().map(|p| p.len()).sum();
        if n == 0 || n > MAX_RECORD {
            return Err(Error::invalid(format!("log record of {n} bytes")));
        }
        let len = (n as u32).to_le_bytes();
        let mut crc_parts: Vec<&[u8]> = Vec::with_capacity(parts.len() + 1);
        crc_parts.push(&len);
        crc_parts.extend_from_slice(parts);
        let crc = crc32_parts(&crc_parts);
        let mut buf = Vec::with_capacity(HEADER + n);
        buf.extend_from_slice(&len);
        buf.extend_from_slice(&crc.to_le_bytes());
        for p in parts {
            buf.extend_from_slice(p);
        }
        self.file
            .write_all(&buf)
            .ctx(|| format!("append to {}", self.path.display()))?;
        self.len += buf.len() as u64;
        Ok(())
    }

    /// Syncs the file (`F_FULLFSYNC` on macOS through `File::sync_data`).
    pub fn sync(&mut self) -> Result<()> {
        if self.synced == self.len {
            return Ok(());
        }
        self.file
            .sync_data()
            .ctx(|| format!("sync {}", self.path.display()))?;
        self.synced = self.len;
        Ok(())
    }

    pub fn len(&self) -> u64 {
        self.len
    }

    /// Simulates power loss: bytes written after the last sync are dropped,
    /// or with `PartialUnsynced` a random prefix of them survives and a
    /// 512-byte sector inside that prefix may be zeroed.
    pub fn crash(self, mode: CrashMode) -> Result<()> {
        let keep = match mode {
            CrashMode::DropUnsynced => self.synced,
            CrashMode::PartialUnsynced { seed } => {
                let mut r = seed ^ 0x6a09_e667_f3bc_c909;
                let mut next = || {
                    r ^= r << 13;
                    r ^= r >> 7;
                    r ^= r << 17;
                    r
                };
                let unsynced = self.len - self.synced;
                let keep = self.synced + next() % (unsynced + 1);
                if keep > self.synced && next() % 4 == 0 {
                    let first = self.synced / 512;
                    let last = (keep - 1) / 512;
                    let sector = first + next() % (last - first + 1);
                    let start = (sector * 512).max(self.synced);
                    let end = ((sector + 1) * 512).min(keep);
                    let zeros = vec![0u8; (end - start) as usize];
                    std::os::unix::fs::FileExt::write_all_at(&self.file, &zeros, start)
                        .ctx(|| format!("zero sector of {}", self.path.display()))?;
                }
                keep
            }
        };
        self.file
            .set_len(keep)
            .ctx(|| format!("truncate {}", self.path.display()))
    }
}

/// How reading a log ended.
#[derive(Debug, PartialEq, Eq)]
pub enum LogEnd {
    Clean,
    /// A short, corrupt or rejected record at this offset; later bytes were
    /// ignored.
    Truncated(u64),
}

/// Calls `f` for each intact record of the log at `path`, in order. An error
/// from `f` stops the read like a corrupt record, except I/O errors, which
/// are returned.
pub fn read_records(path: &Path, mut f: impl FnMut(&[u8]) -> Result<()>) -> Result<LogEnd> {
    let data = std::fs::read(path).ctx(|| format!("read {}", path.display()))?;
    let mut pos = 0usize;
    while pos < data.len() {
        if data.len() - pos < HEADER {
            return Ok(LogEnd::Truncated(pos as u64));
        }
        let len_bytes: [u8; 4] = data[pos..pos + 4].try_into().unwrap();
        let len = u32::from_le_bytes(len_bytes) as usize;
        let crc = u32::from_le_bytes(data[pos + 4..pos + 8].try_into().unwrap());
        let end = pos + HEADER + len;
        if len == 0 || len > MAX_RECORD || end > data.len() {
            return Ok(LogEnd::Truncated(pos as u64));
        }
        let payload = &data[pos + HEADER..end];
        if crc32_parts(&[&len_bytes, payload]) != crc {
            return Ok(LogEnd::Truncated(pos as u64));
        }
        match f(payload) {
            Ok(()) => {}
            Err(e @ Error::Io { .. }) => return Err(e),
            Err(_) => return Ok(LogEnd::Truncated(pos as u64)),
        }
        pos = end;
    }
    Ok(LogEnd::Clean)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp(name: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("nil-log-test-{}-{name}", std::process::id()));
        let _ = std::fs::remove_file(&d);
        d
    }

    #[test]
    fn torn_tail_is_ignored() {
        let p = tmp("torn");
        let mut w = LogWriter::create(&p).unwrap();
        w.add_record(&[b"one"]).unwrap();
        w.add_record(&[b"tw", b"o"]).unwrap();
        w.sync().unwrap();
        w.add_record(&[&[7u8; 1000]]).unwrap();
        let full = w.len();
        drop(w);
        let f = OpenOptions::new().write(true).open(&p).unwrap();
        f.set_len(full - 10).unwrap();
        let mut got = Vec::new();
        let end = read_records(&p, |r| {
            got.push(r.to_vec());
            Ok(())
        })
        .unwrap();
        assert_eq!(got, vec![b"one".to_vec(), b"two".to_vec()]);
        assert!(matches!(end, LogEnd::Truncated(_)));
        let _ = std::fs::remove_file(&p);
    }

    #[test]
    fn corrupt_record_stops_replay() {
        let p = tmp("corrupt");
        let mut w = LogWriter::create(&p).unwrap();
        for i in 0..3u8 {
            w.add_record(&[&[i; 20]]).unwrap();
        }
        drop(w);
        let mut data = std::fs::read(&p).unwrap();
        data[HEADER + 20 + HEADER + 5] ^= 1;
        std::fs::write(&p, &data).unwrap();
        let mut n = 0;
        read_records(&p, |_| {
            n += 1;
            Ok(())
        })
        .unwrap();
        assert_eq!(n, 1);
        let _ = std::fs::remove_file(&p);
    }
}
