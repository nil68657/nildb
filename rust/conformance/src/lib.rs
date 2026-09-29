//! Conformance suite for NilDB's storage engines. An engine's integration
//! test calls [`conformance_tests!`] with a [`Harness`]; the macro expands to
//! one `#[test]` per case in [`cases`], and every case checks the engine
//! against [`Model`], a `BTreeMap` per column family.

use std::collections::BTreeMap;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use nilengine_api::{
    BatchOp, CfId, DbIterator, Engine, IterOptions, Options, Result, Snapshot, WriteBatch, i64add,
};

pub mod cases;

/// Opens an engine of the kind under test.
pub type OpenFn = fn(&Path, &[&str], &Options) -> Result<Box<dyn Engine>>;

#[derive(Clone)]
pub struct Harness {
    pub name: &'static str,
    pub open: OpenFn,
    /// Options every case starts from; `Options::small()` by default.
    pub options: Options,
}

impl Harness {
    pub fn new(name: &'static str, open: OpenFn) -> Self {
        Harness {
            name,
            open,
            options: Options::small(),
        }
    }

    pub fn open(&self, dir: &Path, cfs: &[&str]) -> Box<dyn Engine> {
        self.try_open(dir, cfs)
            .unwrap_or_else(|e| panic!("{}: open {}: {e}", self.name, dir.display()))
    }

    pub fn try_open(&self, dir: &Path, cfs: &[&str]) -> Result<Box<dyn Engine>> {
        (self.open)(dir, cfs, &self.options)
    }

    pub fn open_opts(&self, dir: &Path, cfs: &[&str], opts: &Options) -> Result<Box<dyn Engine>> {
        (self.open)(dir, cfs, opts)
    }
}

/// Expands to one `#[test]` per conformance case, each calling the case with
/// the harness expression.
#[macro_export]
macro_rules! conformance_tests {
    ($harness:expr) => {
        $crate::conformance_tests!(@cases $harness;
            basic_put_get_delete,
            empty_database,
            batch_atomic_across_cfs,
            concurrent_readers_and_writer,
            randomized_model_seed_1,
            randomized_model_seed_2,
            randomized_model_seed_3,
            randomized_model_seed_4,
            randomized_model_large_values,
            snapshot_isolation,
            snapshot_outlives_release_in_iterator,
            iterator_bounds_and_direction,
            iterator_many_versions,
            delete_range_semantics,
            delete_range_persists,
            empty_and_large_keys_and_values,
            reopen_after_close,
            reopen_reorders_and_adds_column_families,
            merge_i64add,
            flush_compact_and_properties,
            invalid_arguments,
            crash_drop_unsynced_seed_1,
            crash_drop_unsynced_seed_2,
            crash_partial_unsynced_seed_1,
            crash_partial_unsynced_seed_2,
            crash_partial_unsynced_seed_3,
            crash_after_reopen_cycles,
            checkpoint_is_a_consistent_copy,
            approximate_sizes_follow_the_data,
        );
    };
    (@cases $harness:expr; $($case:ident),* $(,)?) => {
        $(
            #[test]
            fn $case() {
                $crate::cases::$case(&$harness);
            }
        )*

        #[test]
        #[ignore = "long; run with --ignored"]
        fn soak() {
            $crate::cases::soak(&$harness);
        }
    };
}

/// A directory under the system temp dir (or `$NILENGINE_TEST_DIR`) that is
/// removed on drop unless `$NILENGINE_KEEP_TEST_DIRS` is set.
pub struct TempDir {
    path: PathBuf,
}

static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

impl TempDir {
    pub fn new(prefix: &str) -> TempDir {
        let base = std::env::var_os("NILENGINE_TEST_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| std::env::temp_dir().join("nilengine-tests"));
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let n = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = base.join(format!("{prefix}-{}-{nanos}-{n}", std::process::id()));
        std::fs::create_dir_all(&path).unwrap_or_else(|e| panic!("create {}: {e}", path.display()));
        TempDir { path }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        if std::env::var_os("NILENGINE_KEEP_TEST_DIRS").is_none() {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }
}

/// SplitMix64 (Steele, Lea and Flood, "Fast Splittable Pseudorandom Number
/// Generators", OOPSLA 2014). Seeded runs repeat exactly.
#[derive(Clone)]
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Rng {
        Rng(seed ^ 0x9e37_79b9_7f4a_7c15)
    }

    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    /// Uniform in `[0, n)`; `n` must be positive.
    pub fn below(&mut self, n: u64) -> u64 {
        self.next_u64() % n
    }

    pub fn index(&mut self, n: usize) -> usize {
        self.below(n as u64) as usize
    }

    pub fn one_in(&mut self, n: u64) -> bool {
        self.below(n) == 0
    }

    pub fn bytes(&mut self, len: usize) -> Vec<u8> {
        let mut v = Vec::with_capacity(len + 8);
        while v.len() < len {
            v.extend_from_slice(&self.next_u64().to_le_bytes());
        }
        v.truncate(len);
        v
    }
}

/// The reference model: one ordered map per column family.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Model {
    pub cfs: Vec<BTreeMap<Vec<u8>, Vec<u8>>>,
}

impl Model {
    pub fn new(num_cfs: usize) -> Model {
        Model {
            cfs: vec![BTreeMap::new(); num_cfs],
        }
    }

    /// Applies a batch with the engines' semantics. If a merge fails the
    /// model is left unchanged and the error returned, as engines must do.
    pub fn apply(&mut self, batch: &WriteBatch) -> Result<()> {
        let mut undo: Vec<(usize, Vec<u8>, Option<Vec<u8>>)> = Vec::new();
        let mut failed = None;
        for op in batch.iter() {
            let cf = op.cf() as usize;
            match op {
                BatchOp::Put { key, value, .. } => {
                    let old = self.cfs[cf].insert(key.to_vec(), value.to_vec());
                    undo.push((cf, key.to_vec(), old));
                }
                BatchOp::Delete { key, .. } => {
                    let old = self.cfs[cf].remove(key);
                    undo.push((cf, key.to_vec(), old));
                }
                BatchOp::DeleteRange { start, end, .. } => {
                    if start >= end {
                        continue;
                    }
                    let doomed: Vec<Vec<u8>> = self.cfs[cf]
                        .range::<[u8], _>((Bound::Included(start), Bound::Excluded(end)))
                        .map(|(k, _)| k.clone())
                        .collect();
                    for k in doomed {
                        let old = self.cfs[cf].remove(&k);
                        undo.push((cf, k, old));
                    }
                }
                BatchOp::Merge { key, operand, .. } => {
                    match i64add(self.cfs[cf].get(key).map(Vec::as_slice), operand) {
                        Ok(v) => {
                            let old = self.cfs[cf].insert(key.to_vec(), v);
                            undo.push((cf, key.to_vec(), old));
                        }
                        Err(e) => {
                            failed = Some(e);
                            break;
                        }
                    }
                }
            }
        }
        if let Some(e) = failed {
            for (cf, k, old) in undo.into_iter().rev() {
                match old {
                    Some(v) => self.cfs[cf].insert(k, v),
                    None => self.cfs[cf].remove(&k),
                };
            }
            return Err(e);
        }
        Ok(())
    }

    pub fn get(&self, cf: CfId, key: &[u8]) -> Option<&[u8]> {
        self.cfs[cf as usize].get(key).map(Vec::as_slice)
    }

    /// Entries of `cf` in `[lo, hi)` in key order.
    pub fn range(&self, cf: CfId, lo: Option<&[u8]>, hi: Option<&[u8]>) -> Vec<(Vec<u8>, Vec<u8>)> {
        let lo_b = lo.map_or(Bound::Unbounded, Bound::Included);
        let hi_b = hi.map_or(Bound::Unbounded, Bound::Excluded);
        if let (Some(l), Some(h)) = (lo, hi)
            && l >= h
        {
            return Vec::new();
        }
        self.cfs[cf as usize]
            .range::<[u8], _>((lo_b, hi_b))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }
}

/// Reads everything an iterator yields from its first (or last) position.
pub fn drain(it: &mut dyn DbIterator, reverse: bool) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut out = Vec::new();
    if reverse {
        it.seek_to_last();
    } else {
        it.seek_to_first();
    }
    while it.valid() {
        out.push((it.key().to_vec(), it.value().to_vec()));
        if reverse {
            it.prev();
        } else {
            it.next();
        }
    }
    it.status().expect("iterator status after a full scan");
    if reverse {
        out.reverse();
    }
    out
}

pub fn scan(
    e: &dyn Engine,
    cf: CfId,
    lo: Option<&[u8]>,
    hi: Option<&[u8]>,
    snap: Option<&Snapshot>,
    reverse: bool,
) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut it = e
        .iter(cf, IterOptions::bounds(lo, hi).with_snapshot(snap))
        .expect("open iterator");
    drain(it.as_mut(), reverse)
}

/// Asserts that forward and reverse scans of `[lo, hi)` match the model.
pub fn check_range(
    e: &dyn Engine,
    model: &Model,
    cf: CfId,
    lo: Option<&[u8]>,
    hi: Option<&[u8]>,
    snap: Option<&Snapshot>,
    ctx: &str,
) {
    let want = model.range(cf, lo, hi);
    let fwd = scan(e, cf, lo, hi, snap, false);
    if fwd != want {
        panic!(
            "{ctx}: forward scan of cf {cf} [{lo:?}, {hi:?}) differs from the model\n{}",
            diff(&fwd, &want)
        );
    }
    let rev = scan(e, cf, lo, hi, snap, true);
    if rev != want {
        panic!(
            "{ctx}: reverse scan of cf {cf} [{lo:?}, {hi:?}) differs from the model\n{}",
            diff(&rev, &want)
        );
    }
}

/// Asserts that every column family matches the model through full scans
/// in both directions and point reads of every model key.
pub fn check_state(e: &dyn Engine, model: &Model, snap: Option<&Snapshot>, ctx: &str) {
    for cf in 0..model.cfs.len() as CfId {
        check_range(e, model, cf, None, None, snap, ctx);
        for (k, v) in &model.cfs[cf as usize] {
            let got = e.get(cf, k, snap).expect("get");
            assert_eq!(
                got.as_deref(),
                Some(v.as_slice()),
                "{ctx}: get cf {cf} key {}",
                show(k)
            );
        }
        let keys: Vec<&[u8]> = model.cfs[cf as usize]
            .keys()
            .map(Vec::as_slice)
            .take(64)
            .collect();
        let got = e.multi_get(cf, &keys, snap).expect("multi_get");
        for (k, g) in keys.iter().zip(got) {
            assert_eq!(
                g.as_deref(),
                model.get(cf, k),
                "{ctx}: multi_get cf {cf} key {}",
                show(k)
            );
        }
    }
}

/// Printable form of a key for assertion messages.
pub fn show(k: &[u8]) -> String {
    if k.len() > 48 {
        return format!(
            "{:?}...({} bytes)",
            String::from_utf8_lossy(&k[..48]),
            k.len()
        );
    }
    format!("{:?}", String::from_utf8_lossy(k))
}

fn diff(got: &[(Vec<u8>, Vec<u8>)], want: &[(Vec<u8>, Vec<u8>)]) -> String {
    let mut i = 0;
    while i < got.len() && i < want.len() && got[i] == want[i] {
        i += 1;
    }
    let g = got
        .get(i)
        .map(|(k, v)| format!("{} => {} bytes", show(k), v.len()));
    let w = want
        .get(i)
        .map(|(k, v)| format!("{} => {} bytes", show(k), v.len()));
    format!(
        "engine has {} entries, model has {}; first difference at index {i}: engine {g:?}, model {w:?}",
        got.len(),
        want.len()
    )
}
