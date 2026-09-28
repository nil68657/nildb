//! Versions and the manifest. A `Version` is the set of table files per
//! level plus the flushed range tombstones, for every column family. Each
//! change (a flush, a compaction, a new column family) is a `VersionEdit`
//! appended to the manifest log and synced before it takes effect, the
//! scheme of LevelDB's `version_set.cc`. `CURRENT` names the live manifest;
//! every open writes a fresh manifest holding one snapshot edit.

use std::collections::{HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};

use nilengine_api::coding::{Reader, put_length_prefixed, put_varint32, put_varint64};
use nilengine_api::{Error, IoContext, Result};

use crate::key::{cmp_internal, user_key};
use crate::log::{LogWriter, read_records};
use crate::rangedel::{Fragments, RangeTombstone};
use crate::table::{BlockCache, Table, TableProps};

pub fn table_path(dir: &Path, number: u64) -> PathBuf {
    dir.join(format!("{number:06}.sst"))
}

pub fn log_path(dir: &Path, number: u64) -> PathBuf {
    dir.join(format!("{number:06}.log"))
}

pub fn manifest_path(dir: &Path, number: u64) -> PathBuf {
    dir.join(format!("MANIFEST-{number:06}"))
}

/// Persistent description of one table file.
#[derive(Clone, Debug, Default)]
pub struct FileData {
    pub number: u64,
    pub size: u64,
    pub smallest: Vec<u8>,
    pub largest: Vec<u8>,
    pub smallest_seq: u64,
    pub largest_seq: u64,
    pub entries: u64,
    pub deletions: u64,
}

impl FileData {
    pub fn from_props(number: u64, p: TableProps) -> FileData {
        FileData {
            number,
            size: p.size,
            smallest: p.smallest,
            largest: p.largest,
            smallest_seq: p.smallest_seq,
            largest_seq: p.largest_seq,
            entries: p.entries,
            deletions: p.deletions,
        }
    }
}

/// A live table file. The file is deleted when the last version holding it
/// is dropped after a compaction marked it obsolete.
pub struct FileMeta {
    pub d: FileData,
    path: PathBuf,
    cache: Arc<BlockCache>,
    table: OnceLock<Arc<Table>>,
    obsolete: AtomicBool,
}

impl FileMeta {
    fn new(d: FileData, dir: &Path, cache: &Arc<BlockCache>) -> FileMeta {
        FileMeta {
            path: table_path(dir, d.number),
            d,
            cache: cache.clone(),
            table: OnceLock::new(),
            obsolete: AtomicBool::new(false),
        }
    }

    pub fn number(&self) -> u64 {
        self.d.number
    }

    pub fn smallest_user(&self) -> &[u8] {
        user_key(&self.d.smallest)
    }

    pub fn largest_user(&self) -> &[u8] {
        user_key(&self.d.largest)
    }

    /// Opens the table on first use and keeps it open while the file lives.
    pub fn table(&self) -> Result<Arc<Table>> {
        if let Some(t) = self.table.get() {
            return Ok(t.clone());
        }
        let t = Arc::new(Table::open(
            &self.path,
            self.d.number,
            self.d.size,
            self.cache.clone(),
        )?);
        let _ = self.table.set(t);
        Ok(self.table.get().unwrap().clone())
    }

    /// Whether the file's user-key range meets `[lo, hi)`; `None` is open.
    pub fn overlaps(&self, lo: Option<&[u8]>, hi: Option<&[u8]>) -> bool {
        !(hi.is_some_and(|h| self.smallest_user() >= h)
            || lo.is_some_and(|l| self.largest_user() < l))
    }

    /// Whether the file's user-key range meets the closed range `[a, b]`.
    pub fn overlaps_closed(&self, a: &[u8], b: &[u8]) -> bool {
        !(self.smallest_user() > b || self.largest_user() < a)
    }

    pub fn contains_user(&self, k: &[u8]) -> bool {
        self.smallest_user() <= k && k <= self.largest_user()
    }

    fn mark_obsolete(&self) {
        self.obsolete.store(true, Ordering::Release);
    }
}

impl Drop for FileMeta {
    fn drop(&mut self) {
        if self.obsolete.load(Ordering::Acquire) {
            let _ = fs::remove_file(&self.path);
        }
    }
}

impl std::fmt::Debug for FileMeta {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "#{}({} bytes)", self.d.number, self.d.size)
    }
}

#[derive(Clone)]
pub struct CfState {
    /// Level 0 is sorted by file number (oldest first) and may overlap;
    /// deeper levels are sorted by key and do not overlap.
    pub levels: Vec<Vec<Arc<FileMeta>>>,
    pub tombstones: Vec<RangeTombstone>,
    pub frags: Arc<Fragments>,
}

impl CfState {
    fn empty(num_levels: usize) -> CfState {
        CfState {
            levels: vec![Vec::new(); num_levels],
            tombstones: Vec::new(),
            frags: Arc::new(Fragments::default()),
        }
    }
}

#[derive(Clone)]
pub struct Version {
    pub cfs: Vec<CfState>,
    pub num_levels: usize,
}

impl Version {
    pub fn empty(num_levels: usize) -> Version {
        Version {
            cfs: Vec::new(),
            num_levels,
        }
    }

    pub fn level_bytes(&self, cf: usize, level: usize) -> u64 {
        self.cfs[cf].levels[level].iter().map(|f| f.d.size).sum()
    }

    /// Deepest level of `cf` holding a file, or 0.
    pub fn deepest_level(&self, cf: usize) -> usize {
        (0..self.num_levels)
            .rev()
            .find(|&l| !self.cfs[cf].levels[l].is_empty())
            .unwrap_or(0)
    }

    pub fn live_files(&self) -> HashSet<u64> {
        self.cfs
            .iter()
            .flat_map(|c| c.levels.iter().flatten())
            .map(|f| f.d.number)
            .collect()
    }

    /// Applies an edit, returning the new version and the files it removed.
    /// Removed files are marked obsolete by the caller once the edit is
    /// durable; a file moved between levels keeps its `FileMeta`.
    fn apply(
        &self,
        edit: &VersionEdit,
        dir: &Path,
        cache: &Arc<BlockCache>,
    ) -> Result<(Version, Vec<Arc<FileMeta>>)> {
        let mut v = self.clone();
        for (id, _) in &edit.new_cfs {
            if *id as usize != v.cfs.len() {
                return Err(Error::corruption(format!(
                    "manifest adds column family {id} out of order"
                )));
            }
            v.cfs.push(CfState::empty(v.num_levels));
        }
        let check = |v: &Version, cf: u32, level: u32| -> Result<()> {
            if cf as usize >= v.cfs.len() {
                return Err(Error::corruption(format!(
                    "manifest names column family {cf}, which does not exist"
                )));
            }
            if level as usize >= v.num_levels {
                return Err(Error::invalid(format!(
                    "database has a file at level {level}; open it with num_levels above {level}"
                )));
            }
            Ok(())
        };
        let mut removed: HashMap<u64, Arc<FileMeta>> = HashMap::new();
        for &(cf, level, number) in &edit.deleted_files {
            check(&v, cf, level)?;
            let files = &mut v.cfs[cf as usize].levels[level as usize];
            let pos = files
                .iter()
                .position(|f| f.d.number == number)
                .ok_or_else(|| {
                    Error::corruption(format!(
                        "manifest deletes file {number}, which is not at level {level}"
                    ))
                })?;
            removed.insert(number, files.remove(pos));
        }
        for (cf, level, d) in &edit.new_files {
            check(&v, *cf, *level)?;
            let meta = removed
                .remove(&d.number)
                .unwrap_or_else(|| Arc::new(FileMeta::new(d.clone(), dir, cache)));
            let files = &mut v.cfs[*cf as usize].levels[*level as usize];
            if *level == 0 {
                let pos = files.partition_point(|f| f.d.number < d.number);
                files.insert(pos, meta);
            } else {
                let pos =
                    files.partition_point(|f| cmp_internal(&f.d.smallest, &d.smallest).is_lt());
                files.insert(pos, meta);
            }
        }
        let mut dirty = HashSet::new();
        for (cf, t) in &edit.new_tombstones {
            check(&v, *cf, 0)?;
            v.cfs[*cf as usize].tombstones.push(t.clone());
            dirty.insert(*cf);
        }
        for &(cf, seq) in &edit.deleted_tombstones {
            check(&v, cf, 0)?;
            v.cfs[cf as usize].tombstones.retain(|t| t.seq != seq);
            dirty.insert(cf);
        }
        for cf in dirty {
            let c = &mut v.cfs[cf as usize];
            c.frags = Arc::new(Fragments::build(&c.tombstones));
        }
        Ok((v, removed.into_values().collect()))
    }
}

const TAG_LOG_NUMBER: u32 = 1;
const TAG_NEXT_FILE: u32 = 2;
const TAG_LAST_SEQ: u32 = 3;
const TAG_NEW_CF: u32 = 4;
const TAG_DELETED_FILE: u32 = 5;
const TAG_NEW_FILE: u32 = 6;
const TAG_NEW_TOMBSTONE: u32 = 7;
const TAG_DELETED_TOMBSTONE: u32 = 8;

/// One change to the version set, as stored in the manifest: a sequence of
/// fields, each a varint tag followed by its payload.
#[derive(Clone, Debug, Default)]
pub struct VersionEdit {
    pub log_number: Option<u64>,
    pub next_file_number: Option<u64>,
    pub last_sequence: Option<u64>,
    pub new_cfs: Vec<(u32, String)>,
    pub deleted_files: Vec<(u32, u32, u64)>,
    pub new_files: Vec<(u32, u32, FileData)>,
    pub new_tombstones: Vec<(u32, RangeTombstone)>,
    pub deleted_tombstones: Vec<(u32, u64)>,
}

impl VersionEdit {
    pub fn encode(&self) -> Vec<u8> {
        let mut b = Vec::new();
        let field = |b: &mut Vec<u8>, tag: u32| put_varint32(b, tag);
        if let Some(n) = self.log_number {
            field(&mut b, TAG_LOG_NUMBER);
            put_varint64(&mut b, n);
        }
        if let Some(n) = self.next_file_number {
            field(&mut b, TAG_NEXT_FILE);
            put_varint64(&mut b, n);
        }
        if let Some(n) = self.last_sequence {
            field(&mut b, TAG_LAST_SEQ);
            put_varint64(&mut b, n);
        }
        for (id, name) in &self.new_cfs {
            field(&mut b, TAG_NEW_CF);
            put_varint32(&mut b, *id);
            put_length_prefixed(&mut b, name.as_bytes());
        }
        for &(cf, level, number) in &self.deleted_files {
            field(&mut b, TAG_DELETED_FILE);
            put_varint32(&mut b, cf);
            put_varint32(&mut b, level);
            put_varint64(&mut b, number);
        }
        for (cf, level, d) in &self.new_files {
            field(&mut b, TAG_NEW_FILE);
            put_varint32(&mut b, *cf);
            put_varint32(&mut b, *level);
            put_varint64(&mut b, d.number);
            put_varint64(&mut b, d.size);
            put_length_prefixed(&mut b, &d.smallest);
            put_length_prefixed(&mut b, &d.largest);
            put_varint64(&mut b, d.smallest_seq);
            put_varint64(&mut b, d.largest_seq);
            put_varint64(&mut b, d.entries);
            put_varint64(&mut b, d.deletions);
        }
        for (cf, t) in &self.new_tombstones {
            field(&mut b, TAG_NEW_TOMBSTONE);
            put_varint32(&mut b, *cf);
            put_varint64(&mut b, t.seq);
            put_length_prefixed(&mut b, &t.start);
            put_length_prefixed(&mut b, &t.end);
        }
        for &(cf, seq) in &self.deleted_tombstones {
            field(&mut b, TAG_DELETED_TOMBSTONE);
            put_varint32(&mut b, cf);
            put_varint64(&mut b, seq);
        }
        b
    }

    pub fn decode(src: &[u8]) -> Result<VersionEdit> {
        let mut e = VersionEdit::default();
        let mut r = Reader::new(src);
        while !r.is_empty() {
            match r.varint32()? {
                TAG_LOG_NUMBER => e.log_number = Some(r.varint64()?),
                TAG_NEXT_FILE => e.next_file_number = Some(r.varint64()?),
                TAG_LAST_SEQ => e.last_sequence = Some(r.varint64()?),
                TAG_NEW_CF => {
                    let id = r.varint32()?;
                    let name = String::from_utf8(r.bytes()?.to_vec())
                        .map_err(|_| Error::corruption("column family name is not UTF-8"))?;
                    e.new_cfs.push((id, name));
                }
                TAG_DELETED_FILE => {
                    e.deleted_files
                        .push((r.varint32()?, r.varint32()?, r.varint64()?))
                }
                TAG_NEW_FILE => {
                    let cf = r.varint32()?;
                    let level = r.varint32()?;
                    let d = FileData {
                        number: r.varint64()?,
                        size: r.varint64()?,
                        smallest: r.bytes()?.to_vec(),
                        largest: r.bytes()?.to_vec(),
                        smallest_seq: r.varint64()?,
                        largest_seq: r.varint64()?,
                        entries: r.varint64()?,
                        deletions: r.varint64()?,
                    };
                    if d.smallest.len() < 8 || d.largest.len() < 8 {
                        return Err(Error::corruption(
                            "manifest file bounds are not internal keys",
                        ));
                    }
                    e.new_files.push((cf, level, d));
                }
                TAG_NEW_TOMBSTONE => {
                    let cf = r.varint32()?;
                    let seq = r.varint64()?;
                    let start = r.bytes()?.to_vec();
                    let end = r.bytes()?.to_vec();
                    e.new_tombstones
                        .push((cf, RangeTombstone { start, end, seq }));
                }
                TAG_DELETED_TOMBSTONE => e.deleted_tombstones.push((r.varint32()?, r.varint64()?)),
                t => return Err(Error::corruption(format!("unknown manifest tag {t}"))),
            }
        }
        Ok(e)
    }
}

/// Rolls the manifest over once it grows past this size.
const MANIFEST_ROLL_BYTES: u64 = 32 << 20;

pub struct VersionSet {
    dir: PathBuf,
    cache: Arc<BlockCache>,
    pub current: Arc<Version>,
    pub next_file_number: u64,
    /// Largest sequence number stored in a table file or recorded at open.
    pub last_sequence: u64,
    /// Logs numbered below this hold only flushed data.
    pub log_number: u64,
    pub cf_names: Vec<String>,
    manifest: Option<LogWriter>,
    pub manifest_number: u64,
    /// Per column family and level, the largest key of the last file an
    /// automatic compaction took from that level (LevelDB's round robin).
    pub compact_pointers: Vec<Vec<Vec<u8>>>,
}

impl VersionSet {
    pub fn new_empty(dir: &Path, cache: Arc<BlockCache>, num_levels: usize) -> VersionSet {
        VersionSet {
            dir: dir.to_path_buf(),
            cache,
            current: Arc::new(Version::empty(num_levels)),
            next_file_number: 2,
            last_sequence: 0,
            log_number: 0,
            cf_names: Vec::new(),
            manifest: None,
            manifest_number: 0,
            compact_pointers: Vec::new(),
        }
    }

    /// Reads `CURRENT` and replays its manifest. `None` means no database.
    pub fn recover(
        dir: &Path,
        cache: Arc<BlockCache>,
        num_levels: usize,
    ) -> Result<Option<VersionSet>> {
        let current = match fs::read_to_string(dir.join("CURRENT")) {
            Ok(s) => s,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(Error::io(format!("read {}/CURRENT", dir.display()), &e)),
        };
        let name = current.trim_end_matches('\n');
        let number: u64 = name
            .strip_prefix("MANIFEST-")
            .and_then(|n| n.parse().ok())
            .ok_or_else(|| Error::corruption(format!("CURRENT names {name:?}")))?;
        let mut vs = VersionSet::new_empty(dir, cache, num_levels);
        let mut records = 0;
        let mut failure = None;
        read_records(&manifest_path(dir, number), |rec| {
            let edit = VersionEdit::decode(rec)?;
            match vs.current.apply(&edit, dir, &vs.cache) {
                Ok((v, removed)) => {
                    vs.current = Arc::new(v);
                    for f in removed {
                        f.mark_obsolete();
                    }
                }
                Err(e @ Error::InvalidArgument(_)) => {
                    failure = Some(e);
                    return Err(Error::corruption("stop"));
                }
                Err(e) => return Err(e),
            }
            vs.absorb(&edit);
            records += 1;
            Ok(())
        })?;
        if let Some(e) = failure {
            return Err(e);
        }
        if records == 0 {
            return Err(Error::corruption(format!("{name} holds no intact record")));
        }
        vs.manifest_number = number;
        Ok(Some(vs))
    }

    fn absorb(&mut self, edit: &VersionEdit) {
        if let Some(n) = edit.log_number {
            self.log_number = self.log_number.max(n);
        }
        if let Some(n) = edit.next_file_number {
            self.next_file_number = self.next_file_number.max(n);
        }
        if let Some(n) = edit.last_sequence {
            self.last_sequence = self.last_sequence.max(n);
        }
        for (_, name) in &edit.new_cfs {
            self.cf_names.push(name.clone());
            self.compact_pointers
                .push(vec![Vec::new(); self.current.num_levels]);
        }
    }

    pub fn new_file_number(&mut self) -> u64 {
        let n = self.next_file_number;
        self.next_file_number += 1;
        n
    }

    pub fn cf_id(&self, name: &str) -> Option<u32> {
        self.cf_names
            .iter()
            .position(|n| n == name)
            .map(|i| i as u32)
    }

    /// Applies `edit`, appends it to the manifest and syncs. The in-memory
    /// state changes only if the manifest write succeeds.
    pub fn log_and_apply(&mut self, mut edit: VersionEdit) -> Result<()> {
        edit.next_file_number = Some(self.next_file_number);
        edit.last_sequence = Some(self.last_sequence);
        let (v, removed) = self.current.apply(&edit, &self.dir, &self.cache)?;
        let w = self
            .manifest
            .as_mut()
            .ok_or_else(|| Error::invalid("no manifest is open"))?;
        w.add_record(&[&edit.encode()])?;
        w.sync()?;
        let roll = w.len() > MANIFEST_ROLL_BYTES;
        self.current = Arc::new(v);
        self.absorb(&edit);
        for f in removed {
            f.mark_obsolete();
        }
        if roll {
            let old = self.manifest_number;
            self.write_new_manifest()?;
            let _ = fs::remove_file(manifest_path(&self.dir, old));
        }
        Ok(())
    }

    /// Applies `edit` in memory only; `write_new_manifest` persists it.
    pub fn apply_in_memory(&mut self, edit: &VersionEdit) -> Result<()> {
        let (v, removed) = self.current.apply(edit, &self.dir, &self.cache)?;
        self.current = Arc::new(v);
        self.absorb(edit);
        for f in removed {
            f.mark_obsolete();
        }
        Ok(())
    }

    fn snapshot_edit(&self) -> VersionEdit {
        let v = &self.current;
        let mut e = VersionEdit {
            log_number: Some(self.log_number),
            next_file_number: Some(self.next_file_number),
            last_sequence: Some(self.last_sequence),
            ..VersionEdit::default()
        };
        for (i, name) in self.cf_names.iter().enumerate() {
            e.new_cfs.push((i as u32, name.clone()));
        }
        for (cf, c) in v.cfs.iter().enumerate() {
            for (level, files) in c.levels.iter().enumerate() {
                for f in files {
                    e.new_files.push((cf as u32, level as u32, f.d.clone()));
                }
            }
            for t in &c.tombstones {
                e.new_tombstones.push((cf as u32, t.clone()));
            }
        }
        e
    }

    /// Writes a new manifest holding the whole current state and points
    /// `CURRENT` at it.
    pub fn write_new_manifest(&mut self) -> Result<()> {
        let number = self.new_file_number();
        let path = manifest_path(&self.dir, number);
        let mut w = LogWriter::create(&path)?;
        w.add_record(&[&self.snapshot_edit().encode()])?;
        w.sync()?;
        set_current(&self.dir, number)?;
        self.manifest = Some(w);
        self.manifest_number = number;
        Ok(())
    }
}

fn set_current(dir: &Path, number: u64) -> Result<()> {
    let tmp = dir.join("CURRENT.tmp");
    let mut f = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&tmp)
        .ctx(|| format!("create {}", tmp.display()))?;
    f.write_all(format!("MANIFEST-{number:06}\n").as_bytes())
        .and_then(|_| f.sync_data())
        .ctx(|| format!("write {}", tmp.display()))?;
    fs::rename(&tmp, dir.join("CURRENT")).ctx(|| format!("rename {} to CURRENT", tmp.display()))?;
    sync_dir(dir)
}

/// Syncs a directory so renames and new files in it survive a crash.
pub fn sync_dir(dir: &Path) -> Result<()> {
    let d = File::open(dir).ctx(|| format!("open directory {}", dir.display()))?;
    match d.sync_all() {
        Ok(()) => Ok(()),
        // Some file systems refuse fsync on a directory descriptor.
        Err(e) if e.kind() == std::io::ErrorKind::InvalidInput || e.raw_os_error() == Some(45) => {
            Ok(())
        }
        Err(e) => Err(Error::io(format!("sync directory {}", dir.display()), &e)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn edit_round_trip() {
        let e = VersionEdit {
            log_number: Some(7),
            next_file_number: Some(9),
            last_sequence: Some(1234),
            new_cfs: vec![(0, "default".into()), (1, "meta".into())],
            deleted_files: vec![(1, 2, 44)],
            new_files: vec![(
                0,
                1,
                FileData {
                    number: 8,
                    size: 100,
                    smallest: b"aaaaaaaaaa".to_vec(),
                    largest: b"zzzzzzzzzz".to_vec(),
                    smallest_seq: 1,
                    largest_seq: 99,
                    entries: 10,
                    deletions: 2,
                },
            )],
            new_tombstones: vec![(
                1,
                RangeTombstone {
                    start: b"a".to_vec(),
                    end: b"b".to_vec(),
                    seq: 50,
                },
            )],
            deleted_tombstones: vec![(0, 3)],
        };
        let d = VersionEdit::decode(&e.encode()).unwrap();
        assert_eq!(d.encode(), e.encode());
        assert!(VersionEdit::decode(&[99]).is_err());
    }
}
