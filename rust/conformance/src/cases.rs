//! The conformance cases. Each takes a [`Harness`] and panics on the first
//! difference between the engine and the model.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread;

use nilengine_api::{
    CfId, CrashMode, DbIterator, Engine, Error, IterOptions, Snapshot, WriteBatch, WriteOptions,
};

use crate::{Harness, Model, Rng, TempDir, check_range, check_state, drain, scan, show};

const CFS: [&str; 3] = ["default", "alpha", "beta"];
/// Crash tests write the number of batches applied so far under this key, in
/// the same batch, so the recovered value says how many batches survived.
const MARKER: &[u8] = b"\xff\xffmarker";

fn wo(sync: bool) -> WriteOptions {
    WriteOptions { sync }
}

fn put(e: &dyn Engine, cf: CfId, k: &[u8], v: &[u8]) {
    let mut b = WriteBatch::new();
    b.put(cf, k, v);
    e.write(&b, wo(false)).unwrap();
}

fn del(e: &dyn Engine, cf: CfId, k: &[u8]) {
    let mut b = WriteBatch::new();
    b.delete(cf, k);
    e.write(&b, wo(false)).unwrap();
}

fn get(e: &dyn Engine, cf: CfId, k: &[u8]) -> Option<Vec<u8>> {
    e.get(cf, k, None).unwrap()
}

fn key(i: usize) -> Vec<u8> {
    format!("key{i:05}").into_bytes()
}

fn at(it: &dyn DbIterator) -> Option<String> {
    it.status().unwrap();
    it.valid()
        .then(|| String::from_utf8_lossy(it.key()).into_owned())
}

fn apply(e: &dyn Engine, model: &mut Model, b: &WriteBatch) {
    e.write(b, wo(false)).unwrap();
    model.apply(b).unwrap();
}

/// Random batches over a small key space, so keys are overwritten, deleted
/// and range-deleted many times. Keys look like `k0042` or `k0042/xy`; the
/// `m00`..`m11` keys of column family 0 carry `i64add` counters.
pub struct Workload {
    pub rng: Rng,
    pub keys: u64,
    /// One value in this many is 1.5 to 7.5 KB, big enough for B+ tree
    /// overflow pages and oversized LSM blocks.
    pub large_one_in: u64,
}

impl Workload {
    pub fn new(seed: u64) -> Workload {
        Workload {
            rng: Rng::new(seed),
            keys: 400,
            large_one_in: 40,
        }
    }

    pub fn key(&mut self) -> Vec<u8> {
        if self.rng.one_in(300) {
            return Vec::new();
        }
        let i = self.rng.below(self.keys);
        let mut k = format!("k{i:04}").into_bytes();
        if self.rng.one_in(3) {
            k.push(b'/');
            for _ in 0..=self.rng.below(3) {
                k.push(b'a' + self.rng.below(26) as u8);
            }
        }
        k
    }

    pub fn value(&mut self) -> Vec<u8> {
        let len = if self.rng.one_in(self.large_one_in) {
            1500 + self.rng.index(6000)
        } else {
            self.rng.index(120)
        };
        self.rng.bytes(len)
    }

    fn merge_key(&mut self) -> Vec<u8> {
        format!("m{:02}", self.rng.below(12)).into_bytes()
    }

    pub fn batch(&mut self) -> WriteBatch {
        let mut b = WriteBatch::new();
        for _ in 0..1 + self.rng.below(8) {
            let cf = self.rng.below(3) as CfId;
            match self.rng.below(100) {
                0..55 => {
                    let (k, v) = (self.key(), self.value());
                    b.put(cf, &k, &v);
                }
                55..77 => {
                    let k = self.key();
                    b.delete(cf, &k);
                }
                77..82 => {
                    let (mut lo, mut hi) = (self.key(), self.key());
                    if lo > hi {
                        std::mem::swap(&mut lo, &mut hi);
                    }
                    b.delete_range(cf, &lo, &hi);
                }
                82..92 => {
                    let k = self.merge_key();
                    let delta = self.rng.below(2000) as i64 - 1000;
                    b.merge(0, &k, &delta.to_le_bytes());
                }
                92..96 => {
                    let k = self.merge_key();
                    b.put(0, &k, &(self.rng.below(100) as i64).to_le_bytes());
                }
                _ => {
                    let k = self.merge_key();
                    b.delete(0, &k);
                }
            }
        }
        b
    }

    fn read_target(&mut self) -> (CfId, Vec<u8>) {
        if self.rng.one_in(5) {
            (0, self.merge_key())
        } else {
            (self.rng.below(3) as CfId, self.key())
        }
    }

    fn bound(&mut self) -> Option<Vec<u8>> {
        if self.rng.one_in(3) {
            None
        } else {
            Some(self.key())
        }
    }

    fn bounds(&mut self) -> (Option<Vec<u8>>, Option<Vec<u8>>) {
        let (mut lo, mut hi) = (self.bound(), self.bound());
        if let (Some(a), Some(b)) = (&lo, &hi)
            && a > b
        {
            std::mem::swap(&mut lo, &mut hi);
        }
        (lo, hi)
    }
}

/// Drives an iterator through random seeks and steps and compares every
/// position with the model.
fn iterator_walk(
    e: &dyn Engine,
    model: &Model,
    cf: CfId,
    snap: Option<&Snapshot>,
    w: &mut Workload,
    ctx: &str,
) {
    let (lo, hi) = w.bounds();
    let want = model.range(cf, lo.as_deref(), hi.as_deref());
    let mut it = e
        .iter(
            cf,
            IterOptions::bounds(lo.as_deref(), hi.as_deref()).with_snapshot(snap),
        )
        .unwrap();
    let mut pos: Option<usize> = None;
    let mut trace: Vec<String> = Vec::new();
    for _ in 0..48 {
        match w.rng.below(6) {
            0 => {
                it.seek_to_first();
                pos = (!want.is_empty()).then_some(0);
                trace.push("seek_to_first".into());
            }
            1 => {
                it.seek_to_last();
                pos = want.len().checked_sub(1);
                trace.push("seek_to_last".into());
            }
            2 => {
                let t = w.key();
                it.seek(&t);
                pos = want.iter().position(|(k, _)| k.as_slice() >= t.as_slice());
                trace.push(format!("seek {}", show(&t)));
            }
            3 => {
                let t = w.key();
                it.seek_for_prev(&t);
                pos = want.iter().rposition(|(k, _)| k.as_slice() <= t.as_slice());
                trace.push(format!("seek_for_prev {}", show(&t)));
            }
            4 => {
                let Some(p) = pos else { continue };
                it.next();
                pos = (p + 1 < want.len()).then_some(p + 1);
                trace.push("next".into());
            }
            _ => {
                let Some(p) = pos else { continue };
                it.prev();
                pos = p.checked_sub(1);
                trace.push("prev".into());
            }
        }
        let where_ = || {
            format!(
                "{ctx}: iterator on cf {cf} over [{:?}, {:?}) after {:?}",
                lo.as_deref().map(show),
                hi.as_deref().map(show),
                trace
            )
        };
        if let Err(err) = it.status() {
            panic!("{}: status {err}", where_());
        }
        assert_eq!(it.valid(), pos.is_some(), "{}: validity", where_());
        if let Some(p) = pos {
            assert_eq!(show(it.key()), show(&want[p].0), "{}: key", where_());
            assert!(it.value() == want[p].1.as_slice(), "{}: value", where_());
        }
    }
}

pub fn basic_put_get_delete(h: &Harness) {
    let dir = TempDir::new("basic");
    let e = h.open(dir.path(), &CFS);
    let names: Vec<String> = CFS.iter().map(|s| s.to_string()).collect();
    assert_eq!(e.column_families(), names);
    assert_eq!(get(&*e, 0, b"a"), None);
    put(&*e, 0, b"a", b"1");
    put(&*e, 1, b"a", b"2");
    assert_eq!(get(&*e, 0, b"a").as_deref(), Some(&b"1"[..]));
    assert_eq!(get(&*e, 1, b"a").as_deref(), Some(&b"2"[..]));
    assert_eq!(get(&*e, 2, b"a"), None);
    put(&*e, 0, b"a", b"3");
    assert_eq!(get(&*e, 0, b"a").as_deref(), Some(&b"3"[..]));
    del(&*e, 0, b"a");
    assert_eq!(get(&*e, 0, b"a"), None);
    assert_eq!(get(&*e, 1, b"a").as_deref(), Some(&b"2"[..]));
    del(&*e, 0, b"never-written");
    let seq = e.latest_sequence();
    put(&*e, 2, b"b", b"x");
    assert!(
        e.latest_sequence() > seq,
        "latest_sequence must grow after a write"
    );

    e.close().unwrap();
    e.close().unwrap();
    assert!(matches!(e.get(0, b"a", None), Err(Error::Closed)));
    let mut b = WriteBatch::new();
    b.put(0, b"k", b"v");
    assert!(matches!(e.write(&b, wo(false)), Err(Error::Closed)));
    assert!(matches!(e.snapshot(), Err(Error::Closed)));
    assert!(matches!(
        e.iter(0, IterOptions::default()),
        Err(Error::Closed)
    ));
    assert!(matches!(e.flush(), Err(Error::Closed)));
}

pub fn empty_database(h: &Harness) {
    let dir = TempDir::new("empty");
    let check = |e: &dyn Engine| {
        for cf in 0..CFS.len() as CfId {
            assert_eq!(get(e, cf, b"x"), None);
            let mut it = e.iter(cf, IterOptions::default()).unwrap();
            it.seek_to_first();
            assert!(!it.valid());
            it.seek_to_last();
            assert!(!it.valid());
            it.seek(b"a");
            assert!(!it.valid());
            it.seek_for_prev(b"z");
            assert!(!it.valid());
            it.status().unwrap();
            assert!(e.multi_get(cf, &[], None).unwrap().is_empty());
        }
    };
    let e = h.open(dir.path(), &CFS);
    check(&*e);
    e.flush().unwrap();
    e.compact_range(0, None, None).unwrap();
    e.flush_wal(true).unwrap();
    e.write(&WriteBatch::new(), wo(true)).unwrap();
    check(&*e);
    e.close().unwrap();
    let e = h.open(dir.path(), &CFS);
    check(&*e);
    e.close().unwrap();
}

pub fn batch_atomic_across_cfs(h: &Harness) {
    let dir = TempDir::new("atomic");
    let e = h.open(dir.path(), &CFS);
    let before = e.snapshot().unwrap();
    let mut b = WriteBatch::new();
    b.put(0, b"a", b"0");
    b.put(1, b"a", b"1");
    b.put(2, b"a", b"2");
    b.delete(1, b"zz");
    e.write(&b, wo(true)).unwrap();
    for cf in 0..3u32 {
        assert_eq!(get(&*e, cf, b"a"), Some(cf.to_string().into_bytes()));
        assert_eq!(e.get(cf, b"a", Some(&before)).unwrap(), None);
    }
    // A batch that fails validation applies none of its operations.
    let mut bad = WriteBatch::new();
    bad.put(0, b"b", b"x");
    bad.put(1, b"b", b"x");
    bad.put(7, b"b", b"x");
    assert!(matches!(
        e.write(&bad, wo(false)),
        Err(Error::InvalidArgument(_))
    ));
    assert_eq!(get(&*e, 0, b"b"), None);
    assert_eq!(get(&*e, 1, b"b"), None);
    e.close().unwrap();
    let e = h.open(dir.path(), &CFS);
    for cf in 0..3u32 {
        assert_eq!(get(&*e, cf, b"a"), Some(cf.to_string().into_bytes()));
    }
    e.close().unwrap();
}

fn acct(i: usize) -> Vec<u8> {
    format!("acct{i:02}").into_bytes()
}

fn be64(v: &[u8]) -> u64 {
    u64::from_be_bytes(v.try_into().expect("8-byte value"))
}

pub fn concurrent_readers_and_writer(h: &Harness) {
    const ACCOUNTS: usize = 16;
    const START: u64 = 1000;
    const TRANSFERS: u64 = 1500;
    let dir = TempDir::new("concurrent");
    let e: Arc<dyn Engine> = Arc::from(h.open(dir.path(), &CFS));
    let mut b = WriteBatch::new();
    for i in 0..ACCOUNTS {
        b.put(1, &acct(i), &START.to_be_bytes());
    }
    b.put(0, b"transfers", &0u64.to_be_bytes());
    e.write(&b, wo(false)).unwrap();

    let done = Arc::new(AtomicBool::new(false));
    let writer = {
        let (e, done) = (e.clone(), done.clone());
        thread::spawn(move || {
            let mut rng = Rng::new(5);
            let mut bal = [START; ACCOUNTS];
            for t in 1..=TRANSFERS {
                let (from, to) = (rng.index(ACCOUNTS), rng.index(ACCOUNTS));
                let amount = rng.below(bal[from] + 1);
                bal[from] -= amount;
                bal[to] += amount;
                let mut b = WriteBatch::new();
                b.put(1, &acct(from), &bal[from].to_be_bytes());
                b.put(1, &acct(to), &bal[to].to_be_bytes());
                b.put(0, b"transfers", &t.to_be_bytes());
                b.put(
                    2,
                    &t.to_be_bytes(),
                    format!("{from}->{to}:{amount}").as_bytes(),
                );
                e.write(&b, wo(false)).unwrap();
                if t % 400 == 0 {
                    e.flush().unwrap();
                }
            }
            done.store(true, Ordering::Release);
        })
    };
    let readers: Vec<_> = (0..3)
        .map(|r| {
            let (e, done) = (e.clone(), done.clone());
            thread::spawn(move || {
                let keys: Vec<Vec<u8>> = (0..ACCOUNTS).map(acct).collect();
                let refs: Vec<&[u8]> = keys.iter().map(Vec::as_slice).collect();
                let mut checks = 0;
                while !done.load(Ordering::Acquire) || checks < 10 {
                    let s = e.snapshot().unwrap();
                    let accts = scan(&*e, 1, None, None, Some(&s), r % 2 == 1);
                    assert_eq!(accts.len(), ACCOUNTS);
                    let total: u64 = accts.iter().map(|(_, v)| be64(v)).sum();
                    assert_eq!(
                        total,
                        START * ACCOUNTS as u64,
                        "reader {r} saw part of a transfer"
                    );
                    let t = be64(&e.get(0, b"transfers", Some(&s)).unwrap().unwrap());
                    let journal = scan(&*e, 2, None, None, Some(&s), false).len() as u64;
                    assert_eq!(journal, t, "reader {r}: journal and counter disagree");
                    let vals = e.multi_get(1, &refs, None).unwrap();
                    let total2: u64 = vals.iter().map(|v| be64(v.as_deref().unwrap())).sum();
                    assert_eq!(total2, START * ACCOUNTS as u64, "multi_get read two views");
                    checks += 1;
                }
                checks
            })
        })
        .collect();
    writer.join().unwrap();
    for r in readers {
        assert!(r.join().unwrap() > 0);
    }
    assert_eq!(be64(&get(&*e, 0, b"transfers").unwrap()), TRANSFERS);
    e.close().unwrap();
}

fn randomized_model(h: &Harness, seed: u64, steps: usize, large_one_in: u64) {
    let dir = TempDir::new("random");
    let mut e = h.open(dir.path(), &CFS);
    let mut w = Workload::new(seed);
    w.large_one_in = large_one_in;
    let mut model = Model::new(CFS.len());
    let mut snaps: Vec<(Snapshot, Model)> = Vec::new();
    for step in 0..steps {
        let ctx = |what: &str| format!("{} seed {seed} step {step}: {what}", h.name);
        let b = w.batch();
        if let Err(err) = e.write(&b, wo(w.rng.one_in(100))) {
            panic!("{}: {err}", ctx("write"));
        }
        model.apply(&b).unwrap();
        for _ in 0..2 {
            let (cf, k) = w.read_target();
            let got = e.get(cf, &k, None).unwrap();
            assert_eq!(
                got.as_deref(),
                model.get(cf, &k),
                "{}",
                ctx(&format!("get cf {cf} {}", show(&k)))
            );
        }
        if !snaps.is_empty() && w.rng.one_in(8) {
            let (s, m) = &snaps[w.rng.index(snaps.len())];
            let (cf, k) = w.read_target();
            let got = e.get(cf, &k, Some(s)).unwrap();
            assert_eq!(
                got.as_deref(),
                m.get(cf, &k),
                "{}",
                ctx(&format!("snapshot get cf {cf} {}", show(&k)))
            );
        }
        match w.rng.below(1000) {
            0..6 if snaps.len() < 3 => snaps.push((e.snapshot().unwrap(), model.clone())),
            6..10 if !snaps.is_empty() => {
                let i = w.rng.index(snaps.len());
                snaps.swap_remove(i);
            }
            10..20 => {
                let cf = w.rng.below(3) as CfId;
                let (lo, hi) = w.bounds();
                check_range(
                    &*e,
                    &model,
                    cf,
                    lo.as_deref(),
                    hi.as_deref(),
                    None,
                    &ctx("range"),
                );
                for (s, m) in &snaps {
                    check_range(
                        &*e,
                        m,
                        cf,
                        lo.as_deref(),
                        hi.as_deref(),
                        Some(s),
                        &ctx("snapshot range"),
                    );
                }
            }
            20..32 => {
                let cf = w.rng.below(3) as CfId;
                if snaps.is_empty() || w.rng.one_in(2) {
                    iterator_walk(&*e, &model, cf, None, &mut w, &ctx("walk"));
                } else {
                    let (s, m) = &snaps[w.rng.index(snaps.len())];
                    iterator_walk(&*e, m, cf, Some(s), &mut w, &ctx("snapshot walk"));
                }
            }
            32..36 => e.flush().unwrap(),
            36..39 => {
                let cf = w.rng.below(3) as CfId;
                let (lo, hi) = w.bounds();
                e.compact_range(cf, lo.as_deref(), hi.as_deref()).unwrap();
            }
            39..41 => {
                snaps.clear();
                e.close().unwrap();
                drop(e);
                e = h.open(dir.path(), &CFS);
                check_state(&*e, &model, None, &ctx("after reopen"));
            }
            _ => {}
        }
    }
    check_state(&*e, &model, None, &format!("{} seed {seed}: final", h.name));
    for (i, (s, m)) in snaps.iter().enumerate() {
        check_state(
            &*e,
            m,
            Some(s),
            &format!("{} seed {seed}: final snapshot {i}", h.name),
        );
    }
    snaps.clear();
    e.close().unwrap();
}

pub fn randomized_model_seed_1(h: &Harness) {
    randomized_model(h, 1, 1500, 40);
}

pub fn randomized_model_seed_2(h: &Harness) {
    randomized_model(h, 2, 1500, 40);
}

pub fn randomized_model_seed_3(h: &Harness) {
    randomized_model(h, 3, 1500, 40);
}

pub fn randomized_model_seed_4(h: &Harness) {
    randomized_model(h, 4, 1500, 40);
}

pub fn randomized_model_large_values(h: &Harness) {
    randomized_model(h, 5, 500, 3);
}

pub fn snapshot_isolation(h: &Harness) {
    let dir = TempDir::new("snapshot");
    let e = h.open(dir.path(), &CFS);
    let mut model = Model::new(CFS.len());
    let mut w = Workload::new(77);
    for i in 0..300 {
        let mut b = WriteBatch::new();
        b.put(1, &key(i), format!("v1-{i}").as_bytes());
        b.put(2, &key(i), b"beta");
        apply(&*e, &mut model, &b);
    }
    let s1 = e.snapshot().unwrap();
    let m1 = model.clone();

    let mut b = WriteBatch::new();
    for i in (0..300).step_by(2) {
        b.put(1, &key(i), format!("v2-{i}").as_bytes());
    }
    for i in (1..300).step_by(4) {
        b.delete(1, &key(i));
    }
    b.delete_range(1, &key(100), &key(150));
    for i in 300..350 {
        b.put(1, &key(i), b"new");
    }
    apply(&*e, &mut model, &b);
    let s2 = e.snapshot().unwrap();
    let m2 = model.clone();
    assert!(
        s2.seq() > s1.seq(),
        "a later snapshot has a larger sequence"
    );

    for _ in 0..400 {
        let b = w.batch();
        apply(&*e, &mut model, &b);
    }
    let s3 = e.snapshot().unwrap();
    let m3 = model.clone();
    for _ in 0..100 {
        let b = w.batch();
        apply(&*e, &mut model, &b);
    }
    e.flush().unwrap();
    for cf in 0..3 {
        e.compact_range(cf, None, None).unwrap();
    }
    check_state(&*e, &m1, Some(&s1), "snapshot 1");
    check_state(&*e, &m2, Some(&s2), "snapshot 2");
    check_state(&*e, &m3, Some(&s3), "snapshot 3");
    check_state(&*e, &model, None, "latest");
    if let Some(n) = e.property(0, "rocksdb.num-snapshots") {
        assert_eq!(n, "3");
    }

    drop(s1);
    for _ in 0..200 {
        let b = w.batch();
        apply(&*e, &mut model, &b);
    }
    e.flush().unwrap();
    for cf in 0..3 {
        e.compact_range(cf, None, None).unwrap();
    }
    check_state(&*e, &m2, Some(&s2), "snapshot 2 after releasing snapshot 1");
    check_state(&*e, &m3, Some(&s3), "snapshot 3 after releasing snapshot 1");
    check_state(&*e, &model, None, "latest after releasing snapshot 1");
    for _ in 0..30 {
        iterator_walk(&*e, &m2, 1, Some(&s2), &mut w, "snapshot 2 walk");
        iterator_walk(&*e, &model, 1, None, &mut w, "latest walk");
    }
    drop(s2);
    drop(s3);
    if let Some(n) = e.property(0, "rocksdb.num-snapshots") {
        assert_eq!(n, "0");
    }
    e.close().unwrap();
}

pub fn snapshot_outlives_release_in_iterator(h: &Harness) {
    let dir = TempDir::new("snapiter");
    let e = h.open(dir.path(), &CFS);
    for i in 0..200 {
        put(&*e, 1, &key(i), b"old");
    }
    let s = e.snapshot().unwrap();
    let mut from_snapshot = e
        .iter(1, IterOptions::default().with_snapshot(Some(&s)))
        .unwrap();
    drop(s);
    let mut implicit = e.iter(1, IterOptions::default()).unwrap();
    for i in 0..200 {
        if i % 2 == 0 {
            del(&*e, 1, &key(i));
        } else {
            put(&*e, 1, &key(i), b"new");
        }
    }
    e.flush().unwrap();
    e.compact_range(1, None, None).unwrap();
    for it in [&mut from_snapshot, &mut implicit] {
        let all = drain(it.as_mut(), false);
        assert_eq!(
            all.len(),
            200,
            "an iterator keeps the view it was opened on"
        );
        assert!(all.iter().all(|(_, v)| v == b"old"));
        let back = drain(it.as_mut(), true);
        assert_eq!(back, all);
    }
    drop(from_snapshot);
    drop(implicit);
    let now = scan(&*e, 1, None, None, None, false);
    assert_eq!(now.len(), 100);
    assert!(now.iter().all(|(_, v)| v == b"new"));
    e.close().unwrap();
}

pub fn iterator_bounds_and_direction(h: &Harness) {
    let dir = TempDir::new("iterbounds");
    let e = h.open(dir.path(), &CFS);
    for k in ["b", "d", "f", "h", "j"] {
        put(&*e, 1, k.as_bytes(), format!("v{k}").as_bytes());
    }
    put(&*e, 0, b"e", b"other family");
    put(&*e, 2, b"c", b"other family");
    let some = |s: &str| Some(s.to_string());
    let bounded = |e: &dyn Engine, lo: Option<&str>, hi: Option<&str>| {
        e.iter(
            1,
            IterOptions::bounds(lo.map(str::as_bytes), hi.map(str::as_bytes)),
        )
        .unwrap()
    };
    let full_checks = |e: &dyn Engine| {
        let mut it = bounded(e, Some("d"), Some("h"));
        it.seek_to_first();
        assert_eq!(at(&*it), some("d"));
        assert_eq!(it.value(), b"vd");
        it.next();
        assert_eq!(at(&*it), some("f"));
        it.next();
        assert_eq!(at(&*it), None);
        it.seek_to_last();
        assert_eq!(at(&*it), some("f"));
        it.prev();
        assert_eq!(at(&*it), some("d"));
        it.prev();
        assert_eq!(at(&*it), None);
        for (target, want) in [
            ("a", some("d")),
            ("e", some("f")),
            ("f", some("f")),
            ("h", None),
            ("z", None),
        ] {
            it.seek(target.as_bytes());
            assert_eq!(at(&*it), want, "seek {target} in [d, h)");
        }
        for (target, want) in [
            ("z", some("f")),
            ("h", some("f")),
            ("g", some("f")),
            ("d", some("d")),
            ("c", None),
        ] {
            it.seek_for_prev(target.as_bytes());
            assert_eq!(at(&*it), want, "seek_for_prev {target} in [d, h)");
        }
        it.seek(b"d");
        it.next();
        assert_eq!(at(&*it), some("f"));
        it.prev();
        assert_eq!(at(&*it), some("d"));
        it.next();
        assert_eq!(at(&*it), some("f"));
        it.prev();
        assert_eq!(at(&*it), some("d"));
        it.prev();
        assert_eq!(at(&*it), None);
        it.seek_for_prev(b"f");
        it.prev();
        assert_eq!(at(&*it), some("d"));
        it.next();
        assert_eq!(at(&*it), some("f"));
        it.next();
        assert_eq!(at(&*it), None);

        let mut it = bounded(e, None, None);
        it.seek_to_first();
        assert_eq!(at(&*it), some("b"));
        it.seek_to_last();
        assert_eq!(at(&*it), some("j"));
        it.seek_for_prev(b"a");
        assert_eq!(at(&*it), None);
        it.seek(b"k");
        assert_eq!(at(&*it), None);
        it.seek_for_prev(b"c");
        assert_eq!(at(&*it), some("b"));
        it.seek(b"i");
        assert_eq!(at(&*it), some("j"));
        it.prev();
        assert_eq!(at(&*it), some("h"));

        let mut it = bounded(e, Some("e"), None);
        it.seek_to_first();
        assert_eq!(at(&*it), some("f"));
        it.seek_to_last();
        assert_eq!(at(&*it), some("j"));
        it.seek_for_prev(b"e");
        assert_eq!(at(&*it), None);
        it.seek_for_prev(b"g");
        assert_eq!(at(&*it), some("f"));
        it.prev();
        assert_eq!(at(&*it), None);

        let mut it = bounded(e, None, Some("e"));
        it.seek_to_first();
        assert_eq!(at(&*it), some("b"));
        it.seek_to_last();
        assert_eq!(at(&*it), some("d"));
        it.seek(b"e");
        assert_eq!(at(&*it), None);
        it.seek_for_prev(b"z");
        assert_eq!(at(&*it), some("d"));
        it.next();
        assert_eq!(at(&*it), None);

        for (lo, hi, want) in [
            ("d", "d", 0),
            ("c", "d", 0),
            ("c", "e", 1),
            ("a", "z", 5),
            ("j", "k", 1),
        ] {
            let mut it = bounded(e, Some(lo), Some(hi));
            assert_eq!(drain(it.as_mut(), false).len(), want, "[{lo}, {hi})");
            assert_eq!(
                drain(it.as_mut(), true).len(),
                want,
                "[{lo}, {hi}) reversed"
            );
        }
    };
    full_checks(&*e);
    e.flush().unwrap();
    full_checks(&*e);

    del(&*e, 1, b"f");
    let deleted_checks = |e: &dyn Engine| {
        let mut it = bounded(e, Some("d"), Some("h"));
        it.seek_to_first();
        assert_eq!(at(&*it), some("d"));
        it.next();
        assert_eq!(at(&*it), None);
        it.seek_to_last();
        assert_eq!(at(&*it), some("d"));
        it.seek(b"e");
        assert_eq!(at(&*it), None);
        it.seek_for_prev(b"g");
        assert_eq!(at(&*it), some("d"));
        let mut it = bounded(e, None, None);
        it.seek(b"e");
        assert_eq!(at(&*it), some("h"));
        it.prev();
        assert_eq!(at(&*it), some("d"));
        it.next();
        assert_eq!(at(&*it), some("h"));
    };
    deleted_checks(&*e);
    e.close().unwrap();
    let e = h.open(dir.path(), &CFS);
    deleted_checks(&*e);
    e.flush().unwrap();
    e.compact_range(1, None, None).unwrap();
    deleted_checks(&*e);
    e.close().unwrap();
}

pub fn iterator_many_versions(h: &Harness) {
    let dir = TempDir::new("versions");
    let e = h.open(dir.path(), &CFS);
    let mut model = Model::new(CFS.len());
    let mut w = Workload::new(9);
    w.keys = 60;
    let mut snap: Option<(Snapshot, Model)> = None;
    for round in 0..6 {
        for i in 0..60 {
            let mut b = WriteBatch::new();
            let k = format!("k{i:04}").into_bytes();
            if round == 5 && i % 5 == 0 {
                b.delete(1, &k);
            } else {
                b.put(1, &k, format!("r{round}-{i}").as_bytes());
            }
            apply(&*e, &mut model, &b);
        }
        if round % 2 == 1 {
            e.flush().unwrap();
        }
        if round == 2 {
            snap = Some((e.snapshot().unwrap(), model.clone()));
        }
    }
    let (s, m) = snap.unwrap();
    check_state(&*e, &model, None, "latest");
    check_state(&*e, &m, Some(&s), "snapshot after round 2");
    for _ in 0..60 {
        iterator_walk(&*e, &model, 1, None, &mut w, "latest walk");
        iterator_walk(&*e, &m, 1, Some(&s), &mut w, "snapshot walk");
    }
    e.compact_range(1, None, None).unwrap();
    for _ in 0..30 {
        iterator_walk(&*e, &model, 1, None, &mut w, "latest walk after compaction");
        iterator_walk(
            &*e,
            &m,
            1,
            Some(&s),
            &mut w,
            "snapshot walk after compaction",
        );
    }
    drop(s);
    e.close().unwrap();
}

pub fn delete_range_semantics(h: &Harness) {
    let dir = TempDir::new("delrange");
    let e = h.open(dir.path(), &CFS);
    let mut model = Model::new(CFS.len());
    let mut b = WriteBatch::new();
    for i in 0..100 {
        b.put(1, &key(i), format!("v{i}").as_bytes());
        b.put(2, &key(i), format!("w{i}").as_bytes());
    }
    apply(&*e, &mut model, &b);
    let s = e.snapshot().unwrap();
    let ms = model.clone();

    let mut b = WriteBatch::new();
    b.delete_range(1, &key(10), &key(20));
    apply(&*e, &mut model, &b);
    assert!(get(&*e, 1, &key(9)).is_some());
    assert!(get(&*e, 1, &key(10)).is_none());
    assert!(get(&*e, 1, &key(19)).is_none());
    assert!(get(&*e, 1, &key(20)).is_some());
    assert!(
        get(&*e, 2, &key(15)).is_some(),
        "a range delete stays in its column family"
    );

    let mut b = WriteBatch::new();
    b.put(1, &key(15), b"after");
    apply(&*e, &mut model, &b);
    assert_eq!(get(&*e, 1, &key(15)).as_deref(), Some(&b"after"[..]));

    // Operations in one batch apply in order.
    let mut b = WriteBatch::new();
    b.put(1, &key(30), b"hidden");
    b.delete_range(1, &key(25), &key(35));
    b.put(1, &key(32), b"visible");
    apply(&*e, &mut model, &b);
    assert!(get(&*e, 1, &key(30)).is_none());
    assert_eq!(get(&*e, 1, &key(32)).as_deref(), Some(&b"visible"[..]));

    let mut b = WriteBatch::new();
    b.delete_range(1, &key(40), &key(40));
    b.delete_range(1, b"", &key(5));
    b.delete_range(1, &key(50), &key(70));
    b.delete_range(1, &key(60), &key(80));
    b.delete_range(1, &key(85), b"\xff");
    b.delete_range(0, b"a", b"z");
    apply(&*e, &mut model, &b);
    assert!(
        get(&*e, 1, &key(40)).is_some(),
        "an empty range deletes nothing"
    );
    let mut b = WriteBatch::new();
    b.put(1, &key(55), b"back");
    b.put(1, &key(90), b"back");
    b.put(1, b"", b"empty key");
    apply(&*e, &mut model, &b);

    check_state(&*e, &model, None, "after range deletes");
    check_state(&*e, &ms, Some(&s), "snapshot before range deletes");
    let mut w = Workload::new(3);
    for _ in 0..40 {
        iterator_walk(&*e, &model, 1, None, &mut w, "walk after range deletes");
        iterator_walk(&*e, &ms, 1, Some(&s), &mut w, "snapshot walk");
    }
    let mut b = WriteBatch::new();
    b.delete_range(1, b"", b"");
    b.delete_range(1, b"", b"\x00");
    apply(&*e, &mut model, &b);
    assert!(
        get(&*e, 1, b"").is_none(),
        "[\"\", \"\\x00\") covers the empty key"
    );
    e.flush().unwrap();
    check_state(&*e, &model, None, "after flush");
    check_state(&*e, &ms, Some(&s), "snapshot after flush");
    drop(s);
    e.close().unwrap();
}

pub fn delete_range_persists(h: &Harness) {
    let dir = TempDir::new("delrange-persist");
    let e = h.open(dir.path(), &CFS);
    let mut model = Model::new(CFS.len());
    for chunk in 0..20 {
        let mut b = WriteBatch::new();
        for i in chunk * 100..(chunk + 1) * 100 {
            b.put(1, &key(i), &[b'x'; 40]);
        }
        apply(&*e, &mut model, &b);
    }
    e.flush().unwrap();
    let s = e.snapshot().unwrap();
    let ms = model.clone();
    let mut b = WriteBatch::new();
    b.delete_range(1, &key(100), &key(1900));
    b.put(1, &key(500), b"survivor");
    apply(&*e, &mut model, &b);
    let mut b = WriteBatch::new();
    b.put(1, &key(600), b"later");
    apply(&*e, &mut model, &b);
    check_state(&*e, &model, None, "before flush");
    e.flush().unwrap();
    check_state(&*e, &model, None, "after flush");
    e.compact_range(1, None, None).unwrap();
    check_state(&*e, &model, None, "after compaction");
    check_state(&*e, &ms, Some(&s), "snapshot after compaction");
    drop(s);
    e.compact_range(1, None, None).unwrap();
    check_state(&*e, &model, None, "after compaction without snapshots");
    e.close().unwrap();
    let e = h.open(dir.path(), &CFS);
    check_state(&*e, &model, None, "after reopen");
    e.compact_range(1, Some(&key(0)), Some(&key(2000))).unwrap();
    check_state(&*e, &model, None, "after bounded compaction");
    let n: u64 = e
        .property(1, "rocksdb.estimate-num-keys")
        .expect("rocksdb.estimate-num-keys")
        .parse()
        .expect("a number");
    assert!(n <= 2000, "estimate {n} after deleting most keys");
    e.close().unwrap();
}

pub fn empty_and_large_keys_and_values(h: &Harness) {
    let dir = TempDir::new("large");
    let e = h.open(dir.path(), &CFS);
    let mut rng = Rng::new(21);
    let mut model = Model::new(CFS.len());
    let max_key = e.max_key_len().min(64 << 10);
    let mut big_key = vec![b'K'];
    big_key.extend(rng.bytes(max_key - 1));
    let keys: Vec<Vec<u8>> = vec![
        Vec::new(),
        vec![0],
        vec![0xff, 0xff, 0xff],
        rng.bytes(100.min(max_key - 1)),
        big_key.clone(),
    ];
    let values: Vec<Vec<u8>> = vec![
        Vec::new(),
        vec![7],
        rng.bytes(1000),
        rng.bytes(1100),
        rng.bytes(64 << 10),
        rng.bytes(1 << 20),
        rng.bytes((3 << 20) + 17),
    ];
    let mut b = WriteBatch::new();
    for (i, k) in keys.iter().enumerate() {
        b.put(1, k, &values[i % values.len()]);
    }
    for (i, v) in values.iter().enumerate() {
        b.put(2, format!("v{i}").as_bytes(), v);
    }
    apply(&*e, &mut model, &b);
    check_state(
        &*e,
        &model,
        None,
        "large values in the memtable or first commit",
    );
    assert_eq!(get(&*e, 1, b"").as_deref(), Some(values[0].as_slice()));
    let first = scan(&*e, 1, None, None, None, false);
    assert_eq!(first[0].0, Vec::<u8>::new(), "the empty key sorts first");

    e.close().unwrap();
    let e = h.open(dir.path(), &CFS);
    check_state(&*e, &model, None, "after reopen");
    let mut b = WriteBatch::new();
    b.put(2, b"v6", b"small now");
    b.put(2, b"v1", &values[5]);
    b.delete(1, &big_key);
    b.delete(2, b"v5");
    apply(&*e, &mut model, &b);
    e.flush().unwrap();
    check_state(&*e, &model, None, "after overwrites and flush");
    let mut b = WriteBatch::new();
    b.put(1, &big_key, &values[6]);
    b.delete_range(1, b"", b"\x01");
    apply(&*e, &mut model, &b);
    assert!(get(&*e, 1, b"").is_none());
    assert!(get(&*e, 1, &[0]).is_none());
    for cf in 0..3 {
        e.compact_range(cf, None, None).unwrap();
    }
    check_state(&*e, &model, None, "after compaction");
    e.close().unwrap();
    let e = h.open(dir.path(), &CFS);
    check_state(&*e, &model, None, "after second reopen");
    if e.max_key_len() < (1 << 20) {
        let mut b = WriteBatch::new();
        b.put(1, &vec![b'x'; e.max_key_len() + 1], b"v");
        assert!(matches!(
            e.write(&b, wo(false)),
            Err(Error::InvalidArgument(_))
        ));
    }
    e.close().unwrap();
}

pub fn reopen_after_close(h: &Harness) {
    let dir = TempDir::new("reopen");
    let mut model = Model::new(CFS.len());
    let mut w = Workload::new(31);
    for cycle in 0..4 {
        let e = h.open(dir.path(), &CFS);
        check_state(&*e, &model, None, &format!("reopen cycle {cycle}"));
        assert!(
            h.try_open(dir.path(), &CFS).is_err(),
            "a second open of a locked directory must fail"
        );
        for _ in 0..200 {
            let b = w.batch();
            apply(&*e, &mut model, &b);
        }
        if cycle % 2 == 1 {
            e.flush().unwrap();
        }
        e.close().unwrap();
    }
    let e = h.open(dir.path(), &CFS);
    check_state(&*e, &model, None, "final reopen");
    e.close().unwrap();

    let fresh = TempDir::new("reopen-missing");
    let mut opts = h.options.clone();
    opts.create_if_missing = false;
    let missing = fresh.path().join("does-not-exist");
    assert!(h.open_opts(&missing, &CFS, &opts).is_err());
}

pub fn reopen_reorders_and_adds_column_families(h: &Harness) {
    let dir = TempDir::new("cfs");
    let e = h.open(dir.path(), &["default", "alpha"]);
    put(&*e, 0, b"k", b"d");
    put(&*e, 1, b"k", b"a");
    e.close().unwrap();

    let e = h.open(dir.path(), &["alpha", "default"]);
    assert_eq!(
        e.column_families(),
        vec!["alpha".to_string(), "default".to_string()]
    );
    assert_eq!(get(&*e, 0, b"k").as_deref(), Some(&b"a"[..]));
    assert_eq!(get(&*e, 1, b"k").as_deref(), Some(&b"d"[..]));
    e.close().unwrap();

    let e = h.open(dir.path(), &["default", "alpha", "gamma"]);
    assert_eq!(get(&*e, 2, b"k"), None);
    put(&*e, 2, b"k", b"g");
    e.flush().unwrap();
    put(&*e, 2, b"k2", b"g2");
    e.close().unwrap();

    let e = h.open(dir.path(), &["gamma", "alpha", "default"]);
    assert_eq!(get(&*e, 0, b"k").as_deref(), Some(&b"g"[..]));
    assert_eq!(get(&*e, 0, b"k2").as_deref(), Some(&b"g2"[..]));
    assert_eq!(get(&*e, 1, b"k").as_deref(), Some(&b"a"[..]));
    assert_eq!(get(&*e, 2, b"k").as_deref(), Some(&b"d"[..]));
    e.close().unwrap();

    let mut opts = h.options.clone();
    opts.create_if_missing = false;
    assert!(
        h.open_opts(dir.path(), &["default", "delta"], &opts)
            .is_err()
    );
    let e = h.open_opts(dir.path(), &["default"], &opts).unwrap();
    assert_eq!(get(&*e, 0, b"k").as_deref(), Some(&b"d"[..]));
    e.close().unwrap();
}

pub fn merge_i64add(h: &Harness) {
    let dir = TempDir::new("merge");
    let e = h.open(dir.path(), &CFS);
    let n = |v: i64| v.to_le_bytes();
    let value = |e: &dyn Engine, cf: CfId, k: &[u8]| {
        get(e, cf, k).map(|v| i64::from_le_bytes(v.try_into().expect("8-byte counter")))
    };
    let merge = |e: &dyn Engine, cf: CfId, k: &[u8], d: i64| {
        let mut b = WriteBatch::new();
        b.merge(cf, k, &n(d));
        e.write(&b, wo(false)).unwrap();
    };
    merge(&*e, 0, b"c", 5);
    assert_eq!(value(&*e, 0, b"c"), Some(5));
    put(&*e, 0, b"d", &n(10));
    merge(&*e, 0, b"d", 7);
    assert_eq!(value(&*e, 0, b"d"), Some(17));
    let mut b = WriteBatch::new();
    b.merge(0, b"c", &n(1));
    b.merge(0, b"c", &n(2));
    b.put(0, b"e", &n(100));
    b.merge(0, b"e", &n(-1));
    b.merge(1, b"c", &n(40));
    e.write(&b, wo(false)).unwrap();
    assert_eq!(value(&*e, 0, b"c"), Some(8));
    assert_eq!(value(&*e, 0, b"e"), Some(99));
    assert_eq!(value(&*e, 1, b"c"), Some(40));
    del(&*e, 0, b"c");
    merge(&*e, 0, b"c", 3);
    assert_eq!(value(&*e, 0, b"c"), Some(3));
    let s = e.snapshot().unwrap();
    merge(&*e, 0, b"c", 10);
    assert_eq!(value(&*e, 0, b"c"), Some(13));
    assert_eq!(e.get(0, b"c", Some(&s)).unwrap(), Some(n(3).to_vec()));
    put(&*e, 0, b"max", &n(i64::MAX));
    merge(&*e, 0, b"max", 1);
    assert_eq!(value(&*e, 0, b"max"), Some(i64::MIN));

    let mut bad = WriteBatch::new();
    bad.put(0, b"x", b"must not appear");
    bad.merge(0, b"c", b"123");
    assert!(matches!(
        e.write(&bad, wo(false)),
        Err(Error::InvalidArgument(_))
    ));
    assert_eq!(get(&*e, 0, b"x"), None);
    put(&*e, 0, b"s", b"not a number");
    let mut bad = WriteBatch::new();
    bad.put(0, b"y", b"must not appear");
    bad.merge(0, b"s", &n(1));
    assert!(
        e.write(&bad, wo(false)).is_err(),
        "merging into a non-counter value fails"
    );
    assert_eq!(get(&*e, 0, b"y"), None);
    assert_eq!(get(&*e, 0, b"s").as_deref(), Some(&b"not a number"[..]));
    // The counter still works after the failed batches.
    merge(&*e, 0, b"c", 1);
    assert_eq!(value(&*e, 0, b"c"), Some(14));
    e.flush().unwrap();
    merge(&*e, 0, b"c", 1);
    drop(s);
    e.close().unwrap();
    let e = h.open(dir.path(), &CFS);
    assert_eq!(value(&*e, 0, b"c"), Some(15));
    assert_eq!(value(&*e, 0, b"e"), Some(99));
    assert_eq!(value(&*e, 1, b"c"), Some(40));
    e.close().unwrap();
}

pub fn flush_compact_and_properties(h: &Harness) {
    let dir = TempDir::new("props");
    let e = h.open(dir.path(), &CFS);
    let mut model = Model::new(CFS.len());
    let mut seq = e.latest_sequence();
    for chunk in 0..30 {
        let mut b = WriteBatch::new();
        for i in chunk * 100..(chunk + 1) * 100 {
            if i % 7 == 3 {
                b.delete(1, &key(i - 1));
            } else {
                b.put(1, &key(i), &[b'p'; 30]);
            }
        }
        apply(&*e, &mut model, &b);
        assert!(e.latest_sequence() > seq);
        seq = e.latest_sequence();
    }
    e.flush_wal(false).unwrap();
    e.flush_wal(true).unwrap();
    e.flush().unwrap();
    e.compact_range(1, None, None).unwrap();
    e.compact_range(1, Some(&key(100)), Some(&key(200)))
        .unwrap();
    e.compact_range(1, Some(&key(2500)), None).unwrap();
    e.compact_range(1, None, Some(&key(50))).unwrap();
    e.compact_range(0, None, None).unwrap();
    check_state(&*e, &model, None, "after flush and compactions");
    let n: u64 = e
        .property(1, "rocksdb.estimate-num-keys")
        .expect("rocksdb.estimate-num-keys")
        .parse()
        .expect("a number");
    assert!(n > 0);
    assert_eq!(e.property(1, "rocksdb.num-snapshots").as_deref(), Some("0"));
    let s1 = e.snapshot().unwrap();
    let s2 = e.snapshot().unwrap();
    assert_eq!(e.property(1, "rocksdb.num-snapshots").as_deref(), Some("2"));
    drop(s1);
    drop(s2);
    assert_eq!(e.property(1, "rocksdb.num-snapshots").as_deref(), Some("0"));
    assert_eq!(e.property(1, "nil.no-such-property"), None);
    assert_eq!(e.property(99, "rocksdb.estimate-num-keys"), None);
    assert_eq!(e.latest_sequence(), seq);
    e.close().unwrap();
}

pub fn invalid_arguments(h: &Harness) {
    let dir = TempDir::new("invalid");
    let e = h.open(dir.path(), &CFS);
    put(&*e, 0, b"k", b"v");
    let mut b = WriteBatch::new();
    b.put(0, b"first", b"v");
    b.put(3, b"k", b"v");
    assert!(matches!(
        e.write(&b, wo(false)),
        Err(Error::InvalidArgument(_))
    ));
    assert_eq!(get(&*e, 0, b"first"), None);
    assert!(matches!(
        e.get(3, b"k", None),
        Err(Error::InvalidArgument(_))
    ));
    assert!(matches!(
        e.multi_get(3, &[b"k"], None),
        Err(Error::InvalidArgument(_))
    ));
    assert!(matches!(
        e.iter(3, IterOptions::default()),
        Err(Error::InvalidArgument(_))
    ));
    assert!(e.compact_range(3, None, None).is_err());
    let mut b = WriteBatch::new();
    b.put(0, b"first", b"v");
    b.delete_range(0, b"z", b"a");
    assert!(matches!(
        e.write(&b, wo(false)),
        Err(Error::InvalidArgument(_))
    ));
    assert_eq!(get(&*e, 0, b"first"), None);

    let other_dir = TempDir::new("invalid-other");
    let other = h.open(other_dir.path(), &CFS);
    let foreign = other.snapshot().unwrap();
    assert!(matches!(
        e.get(0, b"k", Some(&foreign)),
        Err(Error::InvalidArgument(_))
    ));
    assert!(matches!(
        e.iter(0, IterOptions::default().with_snapshot(Some(&foreign))),
        Err(Error::InvalidArgument(_))
    ));
    drop(foreign);
    other.close().unwrap();
    assert_eq!(get(&*e, 0, b"k").as_deref(), Some(&b"v"[..]));
    e.close().unwrap();
}

fn marker(e: &dyn Engine) -> usize {
    match e.get(0, MARKER, None).unwrap() {
        None => 0,
        Some(v) => be64(&v) as usize,
    }
}

/// Writes batches, some synced, crashes the engine with `mode`, reopens and
/// checks that the survivors are a prefix of the batch history that
/// includes every durable batch, with no batch applied in part.
fn crash_recovery(h: &Harness, seed: u64, cycles: u64, mode: fn(u64) -> CrashMode) {
    let dir = TempDir::new("crash");
    let mut opts = h.options.clone();
    opts.track_unsynced_writes = true;
    let mut w = Workload::new(seed);
    let mut history: Vec<WriteBatch> = Vec::new();
    let mut durable = 0usize;
    let mut lost = 0usize;
    let mut drop_modes = 0;
    for cycle in 0..=cycles {
        let e = h.open_opts(dir.path(), &CFS, &opts).unwrap_or_else(|err| {
            panic!(
                "{} seed {seed} cycle {cycle}: open after crash: {err}",
                h.name
            )
        });
        let k = marker(&*e);
        assert!(
            k >= durable,
            "{} seed {seed} cycle {cycle}: recovered {k} batches but {durable} were durable",
            h.name
        );
        assert!(
            k <= history.len(),
            "{} seed {seed} cycle {cycle}: recovered {k} batches of {} written",
            h.name,
            history.len()
        );
        lost += history.len() - k;
        history.truncate(k);
        let mut model = Model::new(CFS.len());
        for b in &history {
            model.apply(b).unwrap();
        }
        check_state(
            &*e,
            &model,
            None,
            &format!(
                "{} seed {seed} cycle {cycle}: state after recovering {k} batches",
                h.name
            ),
        );
        durable = k;
        if cycle == cycles {
            e.close().unwrap();
            // Unsynced batches make up most of every cycle, so dropping
            // unsynced data must have lost some; otherwise the crash hook
            // did nothing.
            if drop_modes > 0 {
                assert!(
                    lost > 0,
                    "{} seed {seed}: {drop_modes} crashes dropped no batch",
                    h.name
                );
            }
            break;
        }
        for _ in 0..40 + w.rng.below(150) {
            let mut b = w.batch();
            b.put(0, MARKER, &(history.len() as u64 + 1).to_be_bytes());
            let sync = w.rng.one_in(10);
            e.write(&b, wo(sync)).unwrap();
            history.push(b);
            if sync {
                durable = history.len();
            }
            match w.rng.below(60) {
                0 => {
                    e.flush_wal(true).unwrap();
                    durable = history.len();
                }
                1 => {
                    e.flush().unwrap();
                    durable = history.len();
                }
                2 => e.flush_wal(false).unwrap(),
                3 => {
                    let cf = w.rng.below(3) as CfId;
                    e.compact_range(cf, None, None).unwrap();
                }
                _ => {}
            }
        }
        let m = mode(seed.wrapping_mul(1_000_003).wrapping_add(cycle));
        if matches!(m, CrashMode::DropUnsynced) {
            drop_modes += 1;
        }
        e.crash(m)
            .unwrap_or_else(|err| panic!("{} seed {seed} cycle {cycle}: crash: {err}", h.name));
        assert!(matches!(e.get(0, MARKER, None), Err(Error::Closed)));
    }
}

pub fn crash_drop_unsynced_seed_1(h: &Harness) {
    crash_recovery(h, 1, 5, |_| CrashMode::DropUnsynced);
}

pub fn crash_drop_unsynced_seed_2(h: &Harness) {
    crash_recovery(h, 2, 5, |_| CrashMode::DropUnsynced);
}

pub fn crash_partial_unsynced_seed_1(h: &Harness) {
    crash_recovery(h, 11, 5, |s| CrashMode::PartialUnsynced { seed: s });
}

pub fn crash_partial_unsynced_seed_2(h: &Harness) {
    crash_recovery(h, 12, 5, |s| CrashMode::PartialUnsynced { seed: s });
}

pub fn crash_partial_unsynced_seed_3(h: &Harness) {
    crash_recovery(h, 13, 5, |s| CrashMode::PartialUnsynced { seed: s });
}

/// Many more seeds of the randomized and crash cases. The test macro marks
/// it `#[ignore]`; run it with `cargo test -- --ignored soak`, and set
/// `NILENGINE_SOAK` to the number of seeds (default 16).
pub fn soak(h: &Harness) {
    let n: u64 = std::env::var("NILENGINE_SOAK")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(16);
    for seed in 0..n {
        randomized_model(h, 1000 + seed, 1500, if seed % 4 == 0 { 3 } else { 40 });
        let mode: fn(u64) -> CrashMode = if seed % 2 == 0 {
            |_| CrashMode::DropUnsynced
        } else {
            |s| CrashMode::PartialUnsynced { seed: s }
        };
        crash_recovery(h, 2000 + seed, 6, mode);
    }
}

/// A checkpoint taken while a writer runs is a consistent copy: it holds
/// every batch acknowledged before the call and a prefix of the rest, no
/// batch in part (the marker key, written in each batch, says how many),
/// and it lives on independently of its source.
pub fn checkpoint_is_a_consistent_copy(h: &Harness) {
    let dir = TempDir::new("ckpt-src");
    let out = TempDir::new("ckpt-out");
    let e = h.open(dir.path(), &CFS);
    let mut w = Workload::new(31);
    let mut model = Model::new(CFS.len());
    for i in 0..300 {
        let b = w.batch();
        apply(&*e, &mut model, &b);
        if i % 97 == 0 {
            e.flush().unwrap();
        }
    }
    let base = model.clone();
    let cp = out.path().join("cp");
    let written = AtomicUsize::new(0);
    let stop = AtomicBool::new(false);
    let (before, history) = thread::scope(|s| {
        let writer = s.spawn(|| {
            let mut w = Workload::new(32);
            let mut history = Vec::new();
            while !stop.load(Ordering::Acquire) || history.len() < 50 {
                let mut b = w.batch();
                b.put(0, MARKER, &(history.len() as u64 + 1).to_be_bytes());
                e.write(&b, wo(false)).unwrap();
                history.push(b);
                written.store(history.len(), Ordering::Release);
            }
            history
        });
        while written.load(Ordering::Acquire) < 20 {
            thread::yield_now();
        }
        let before = written.load(Ordering::Acquire);
        e.checkpoint(&cp)
            .unwrap_or_else(|err| panic!("{}: checkpoint: {err}", h.name));
        stop.store(true, Ordering::Release);
        (before, writer.join().unwrap())
    });
    for b in &history {
        model.apply(b).unwrap();
    }
    check_state(
        &*e,
        &model,
        None,
        &format!("{}: source after the checkpoint", h.name),
    );
    assert!(
        matches!(e.checkpoint(&cp), Err(Error::InvalidArgument(_))),
        "{}: a checkpoint into an existing directory",
        h.name
    );
    assert!(
        matches!(
            e.checkpoint(&out.path().join("missing").join("cp")),
            Err(Error::NotFound(_))
        ),
        "{}: a checkpoint under a missing parent",
        h.name
    );

    let c = h.open(&cp, &CFS);
    let k = marker(&*c);
    assert!(
        k >= before && k <= history.len(),
        "{}: the checkpoint holds {k} batches; {before} were acknowledged before it and {} written in all",
        h.name,
        history.len()
    );
    let mut want = base;
    for b in &history[..k] {
        want.apply(b).unwrap();
    }
    check_state(
        &*c,
        &want,
        None,
        &format!("{}: checkpoint with {k} batches", h.name),
    );
    // The copy takes writes of its own, and a checkpoint of it works too.
    put(&*c, 1, b"only-in-copy", b"x");
    assert_eq!(get(&*e, 1, b"only-in-copy"), None);
    let cp2 = out.path().join("cp2");
    c.checkpoint(&cp2).unwrap();
    c.close().unwrap();
    let c = h.open(&cp, &CFS);
    assert_eq!(get(&*c, 1, b"only-in-copy").as_deref(), Some(&b"x"[..]));
    c.close().unwrap();
    let c2 = h.open(&cp2, &CFS);
    assert_eq!(get(&*c2, 1, b"only-in-copy").as_deref(), Some(&b"x"[..]));
    assert_eq!(marker(&*c2), k);
    c2.close().unwrap();
    e.close().unwrap();
}

/// Approximate sizes are 0 for empty and reversed ranges and for ranges
/// without keys, grow with the data a range holds, and fall to 0 once the
/// data is deleted and compacted.
pub fn approximate_sizes_follow_the_data(h: &Harness) {
    let dir = TempDir::new("sizes");
    let e = h.open(dir.path(), &CFS);
    let sizes = |cf: CfId, ranges: &[(&[u8], &[u8])]| e.approximate_sizes(cf, ranges).unwrap();
    assert_eq!(sizes(1, &[(b"a", b"z"), (b"", b"\xff")]), [0, 0]);
    const N: usize = 3000;
    for chunk in 0..N / 100 {
        let mut b = WriteBatch::new();
        for i in chunk * 100..(chunk + 1) * 100 {
            b.put(1, &key(i), &[b'v'; 100]);
        }
        e.write(&b, wo(false)).unwrap();
    }
    // The LSM counts table files only.
    e.flush().unwrap();
    let (k0, kh, kn) = (key(0), key(N / 2), key(N));
    let got = sizes(
        1,
        &[
            (&k0, &kn),
            (&k0, &kh),
            (&kh, &kn),
            (b"a", b"b"),
            (b"zzz", b"zzzz"),
            (&kn, &k0),
            (&kh, &kh),
        ],
    );
    let full = got[0];
    assert!(full > 0, "{}: sizes {got:?}", h.name);
    for half in [got[1], got[2]] {
        assert!(
            half >= full / 10 && half <= full * 9 / 10,
            "{}: half the keys take {half} of {full} bytes",
            h.name
        );
    }
    assert_eq!(got[3..], [0, 0, 0, 0], "{}: sizes {got:?}", h.name);
    assert_eq!(sizes(2, &[(&k0, &kn)]), [0], "{}: another family", h.name);
    assert!(matches!(
        e.approximate_sizes(9, &[(&k0, &kn)]),
        Err(Error::InvalidArgument(_))
    ));
    assert_eq!(e.approximate_sizes(1, &[]).unwrap(), Vec::<u64>::new());

    let mut b = WriteBatch::new();
    b.delete_range(1, &k0, &kn);
    e.write(&b, wo(false)).unwrap();
    e.flush().unwrap();
    e.compact_range(1, None, None).unwrap();
    assert_eq!(
        sizes(1, &[(&k0, &kn)]),
        [0],
        "{}: after deleting and compacting everything",
        h.name
    );
    e.close().unwrap();
}

pub fn crash_after_reopen_cycles(h: &Harness) {
    crash_recovery(h, 99, 14, |s| {
        if s % 2 == 0 {
            CrashMode::DropUnsynced
        } else {
            CrashMode::PartialUnsynced { seed: s }
        }
    });
}
