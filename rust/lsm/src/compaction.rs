//! Leveled compaction, after LevelDB's `version_set.cc` (picking) and
//! `db_impl.cc` (the merge loop). Level 0 compacts into level 1 when it holds
//! `level0_compaction_trigger` files; level n >= 1 compacts one file into
//! level n + 1 when its bytes pass `max_bytes_for_level_base *
//! level_size_multiplier^(n-1)`.
//!
//! The merge drops an entry when no reader can see it. With the live
//! snapshots sorted, the "stripe" of a sequence number is the index of the
//! first snapshot at or above it; two versions of a key in the same stripe
//! are seen by the same readers, so the older one goes. A point tombstone in
//! stripe 0 (visible to every snapshot) goes too when no deeper level holds
//! the key, and so does an entry that a range tombstone in its stripe covers.

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use nilengine_api::{Options, Result};

use crate::iter::{InternalIterator, LevelIter, MergingIterator};
use crate::key::{KIND_DELETE, cmp_internal, parse_trailer, user_key};
use crate::table::{BlockCache, TableBuilder, TableIter};
use crate::version::{FileData, FileMeta, Version, VersionEdit, table_path};

pub struct Compaction {
    pub cf: usize,
    pub level: usize,
    pub output_level: usize,
    pub inputs: Vec<Arc<FileMeta>>,
    /// Files of `output_level` that overlap `inputs`; empty when a level is
    /// compacted into itself.
    pub next_inputs: Vec<Arc<FileMeta>>,
    pub version: Arc<Version>,
    pub manual: bool,
}

impl Compaction {
    /// A single file with nothing below it moves down without a rewrite.
    pub fn is_trivial_move(&self) -> bool {
        !self.manual
            && self.level != self.output_level
            && self.inputs.len() == 1
            && self.next_inputs.is_empty()
    }

    pub fn edit(&self, outputs: &[FileData]) -> VersionEdit {
        let mut e = VersionEdit::default();
        let cf = self.cf as u32;
        for f in &self.inputs {
            e.deleted_files.push((cf, self.level as u32, f.number()));
        }
        for f in &self.next_inputs {
            e.deleted_files
                .push((cf, self.output_level as u32, f.number()));
        }
        for d in outputs {
            e.new_files.push((cf, self.output_level as u32, d.clone()));
        }
        e
    }

    /// Whether any level below the output level may hold `ukey`.
    fn is_base_level_for(&self, ukey: &[u8]) -> bool {
        let levels = &self.version.cfs[self.cf].levels;
        for files in levels.iter().skip(self.output_level + 1) {
            let i = files.partition_point(|f| f.largest_user() < ukey);
            if files.get(i).is_some_and(|f| f.smallest_user() <= ukey) {
                return false;
            }
        }
        true
    }
}

fn max_bytes(opts: &Options, level: usize) -> f64 {
    opts.max_bytes_for_level_base as f64
        * (opts.level_size_multiplier as f64).powi(level as i32 - 1)
}

/// The highest compaction score over all column families and levels:
/// `(score, cf, level)`.
fn best_score(v: &Version, opts: &Options) -> Option<(f64, usize, usize)> {
    let mut best: Option<(f64, usize, usize)> = None;
    for cf in 0..v.cfs.len() {
        for level in 0..v.num_levels - 1 {
            let score = if level == 0 {
                v.cfs[cf].levels[0].len() as f64 / opts.level0_compaction_trigger as f64
            } else {
                v.level_bytes(cf, level) as f64 / max_bytes(opts, level)
            };
            if best.is_none_or(|b| score > b.0) {
                best = Some((score, cf, level));
            }
        }
    }
    best
}

pub fn needs_compaction(v: &Version, opts: &Options) -> bool {
    best_score(v, opts).is_some_and(|b| b.0 >= 1.0)
}

/// Bytes above each level's target, a rough backlog estimate.
pub fn pending_bytes(v: &Version, cf: usize, opts: &Options) -> u64 {
    let mut n = 0u64;
    if v.cfs[cf].levels[0].len() >= opts.level0_compaction_trigger {
        n += v.level_bytes(cf, 0);
    }
    for level in 1..v.num_levels - 1 {
        let b = v.level_bytes(cf, level) as f64;
        let m = max_bytes(opts, level);
        if b > m {
            n += (b - m) as u64;
        }
    }
    n
}

fn user_range(files: &[Arc<FileMeta>]) -> (Vec<u8>, Vec<u8>) {
    let lo = files
        .iter()
        .map(|f| f.smallest_user())
        .min()
        .unwrap_or_default();
    let hi = files
        .iter()
        .map(|f| f.largest_user())
        .max()
        .unwrap_or_default();
    (lo.to_vec(), hi.to_vec())
}

fn overlapping_closed(
    v: &Version,
    cf: usize,
    level: usize,
    a: &[u8],
    b: &[u8],
) -> Vec<Arc<FileMeta>> {
    v.cfs[cf].levels[level]
        .iter()
        .filter(|f| f.overlaps_closed(a, b))
        .cloned()
        .collect()
}

pub fn pick_auto(
    v: &Arc<Version>,
    opts: &Options,
    pointers: &mut [Vec<Vec<u8>>],
) -> Option<Compaction> {
    let (score, cf, level) = best_score(v, opts)?;
    if score < 1.0 {
        return None;
    }
    if level == 0 {
        // Every level-0 file goes, so no older overlapping file stays above
        // the newer data written into level 1.
        let inputs = v.cfs[cf].levels[0].clone();
        let (a, b) = user_range(&inputs);
        let next_inputs = overlapping_closed(v, cf, 1, &a, &b);
        return Some(Compaction {
            cf,
            level: 0,
            output_level: 1,
            inputs,
            next_inputs,
            version: v.clone(),
            manual: false,
        });
    }
    let files = &v.cfs[cf].levels[level];
    let ptr = &pointers[cf][level];
    let f = files
        .iter()
        .find(|f| ptr.is_empty() || cmp_internal(&f.d.largest, ptr).is_gt())
        .unwrap_or(&files[0])
        .clone();
    pointers[cf][level] = f.d.largest.clone();
    let next_inputs = overlapping_closed(v, cf, level + 1, f.smallest_user(), f.largest_user());
    Some(Compaction {
        cf,
        level,
        output_level: level + 1,
        inputs: vec![f],
        next_inputs,
        version: v.clone(),
        manual: false,
    })
}

/// Manual compaction of the files of `level` that meet `[lo, hi)` into
/// `level + 1`.
pub fn pick_manual(
    v: &Arc<Version>,
    cf: usize,
    level: usize,
    lo: Option<&[u8]>,
    hi: Option<&[u8]>,
) -> Option<Compaction> {
    let files = &v.cfs[cf].levels[level];
    let inputs: Vec<Arc<FileMeta>> = if level == 0 {
        if files.iter().any(|f| f.overlaps(lo, hi)) {
            files.clone()
        } else {
            Vec::new()
        }
    } else {
        files
            .iter()
            .filter(|f| f.overlaps(lo, hi))
            .cloned()
            .collect()
    };
    if inputs.is_empty() {
        return None;
    }
    let (a, b) = user_range(&inputs);
    let next_inputs = overlapping_closed(v, cf, level + 1, &a, &b);
    Some(Compaction {
        cf,
        level,
        output_level: level + 1,
        inputs,
        next_inputs,
        version: v.clone(),
        manual: true,
    })
}

/// Manual compaction of the bottom level into itself, which is where point
/// and range tombstones finally go.
pub fn pick_bottom(
    v: &Arc<Version>,
    cf: usize,
    level: usize,
    lo: Option<&[u8]>,
    hi: Option<&[u8]>,
) -> Option<Compaction> {
    let inputs: Vec<Arc<FileMeta>> = v.cfs[cf].levels[level]
        .iter()
        .filter(|f| f.overlaps(lo, hi))
        .cloned()
        .collect();
    if inputs.is_empty() {
        return None;
    }
    Some(Compaction {
        cf,
        level,
        output_level: level,
        inputs,
        next_inputs: Vec::new(),
        version: v.clone(),
        manual: true,
    })
}

/// Range tombstones of `cf` that no file can still need: no file meeting
/// the range holds an entry older than the tombstone. Memtables cannot hold
/// such entries either, since they are newer than every flushed tombstone.
pub fn obsolete_tombstones(v: &Version, cf: usize) -> Vec<u64> {
    let c = &v.cfs[cf];
    c.tombstones
        .iter()
        .filter(|t| {
            !c.levels
                .iter()
                .flatten()
                .any(|f| f.overlaps(Some(&t.start), Some(&t.end)) && f.d.smallest_seq < t.seq)
        })
        .map(|t| t.seq)
        .collect()
}

#[derive(Default)]
pub struct Output {
    pub files: Vec<FileData>,
    pub bytes_read: u64,
    pub bytes_written: u64,
}

/// Merges the inputs into new files. Returns `None` if `cancel` was set.
pub fn run(
    c: &Compaction,
    snapshots: &[u64],
    opts: &Options,
    dir: &Path,
    next_number: &mut dyn FnMut() -> u64,
    cancel: &AtomicBool,
    _cache: &Arc<BlockCache>,
) -> Result<Option<Output>> {
    let mut children: Vec<Box<dyn InternalIterator>> = Vec::new();
    if c.level == 0 {
        for f in &c.inputs {
            children.push(Box::new(TableIter::new(f.table()?, false)));
        }
    } else {
        children.push(Box::new(LevelIter::new(c.inputs.clone(), false)));
    }
    if !c.next_inputs.is_empty() {
        children.push(Box::new(LevelIter::new(c.next_inputs.clone(), false)));
    }
    let mut it = MergingIterator::new(children);
    let frags = c.version.cfs[c.cf].frags.clone();
    let stripe = |seq: u64| snapshots.partition_point(|&s| s < seq);

    let mut out = Output::default();
    let mut builder: Option<(u64, TableBuilder)> = None;
    let mut cur_user: Vec<u8> = Vec::new();
    let mut have_cur = false;
    let mut last_stripe: Option<usize> = None;
    let mut n = 0u64;
    let mut failure = None;
    let mut cancelled = false;

    it.seek_to_first();
    while it.valid() {
        n += 1;
        if n.is_multiple_of(1024) && cancel.load(Ordering::Acquire) {
            cancelled = true;
            break;
        }
        let ikey = it.key();
        let value = it.value();
        out.bytes_read += (ikey.len() + value.len()) as u64;
        let (seq, kind) = parse_trailer(ikey);
        let ukey = user_key(ikey);
        if !have_cur || ukey != cur_user.as_slice() {
            cur_user.clear();
            cur_user.extend_from_slice(ukey);
            have_cur = true;
            last_stripe = None;
            // Cut output files only between user keys, so a key never spans
            // two files of one level.
            if builder
                .as_ref()
                .is_some_and(|(_, b)| b.estimated_size() >= opts.target_file_size)
            {
                let (num, b) = builder.take().unwrap();
                match b.finish() {
                    Ok(p) => out.files.push(FileData::from_props(num, p)),
                    Err(e) => {
                        failure = Some(e);
                        break;
                    }
                }
            }
        }
        let s = stripe(seq);
        let discard = if last_stripe == Some(s) {
            true
        } else {
            last_stripe = Some(s);
            frags.next_newer(ukey, seq).is_some_and(|t| stripe(t) == s)
                || (kind == KIND_DELETE && s == 0 && c.is_base_level_for(ukey))
        };
        if !discard {
            if builder.is_none() {
                let num = next_number();
                match TableBuilder::create(&table_path(dir, num), opts) {
                    Ok(b) => builder = Some((num, b)),
                    Err(e) => {
                        failure = Some(e);
                        break;
                    }
                }
            }
            if let Err(e) = builder.as_mut().unwrap().1.add(ikey, value) {
                failure = Some(e);
                break;
            }
        }
        it.next();
    }
    if failure.is_none()
        && !cancelled
        && let Err(e) = it.status()
    {
        failure = Some(e);
    }
    if failure.is_none()
        && !cancelled
        && let Some((num, b)) = builder.take()
    {
        match b.finish() {
            Ok(p) => out.files.push(FileData::from_props(num, p)),
            Err(e) => failure = Some(e),
        }
    }
    if failure.is_some() || cancelled {
        if let Some((_, b)) = builder.take() {
            b.abandon();
        }
        for f in &out.files {
            let _ = std::fs::remove_file(table_path(dir, f.number));
        }
        return match failure {
            Some(e) => Err(e),
            None => Ok(None),
        };
    }
    out.bytes_written = out.files.iter().map(|f| f.size).sum();
    Ok(Some(out))
}
