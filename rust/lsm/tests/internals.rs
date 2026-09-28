//! LSM-specific tests: data reaches deeper levels, compaction collects range
//! tombstones, a log truncated or damaged by hand recovers a clean prefix,
//! open removes orphan files, and a damaged table block reads as corruption.

use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use nilengine_api::{Engine, Error, IterOptions, Options, WriteBatch, WriteOptions};
use nilengine_conformance::{TempDir, drain};
use nilengine_lsm::open;

const CFS: [&str; 2] = ["default", "data"];

fn key(i: usize) -> Vec<u8> {
    format!("key{i:06}").into_bytes()
}

fn put_range(e: &dyn Engine, cf: u32, from: usize, to: usize, val: &[u8]) {
    let mut i = from;
    while i < to {
        let mut b = WriteBatch::new();
        for j in i..(i + 100).min(to) {
            b.put(cf, &key(j), val);
        }
        e.write(&b, WriteOptions::default()).unwrap();
        i += 100;
    }
}

fn prop(e: &dyn Engine, cf: u32, name: &str) -> u64 {
    e.property(cf, name)
        .unwrap_or_else(|| panic!("property {name}"))
        .parse()
        .unwrap()
}

fn files(dir: &Path, ext: &str) -> Vec<PathBuf> {
    fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|x| x == ext))
        .collect()
}

fn count(e: &dyn Engine, cf: u32) -> usize {
    let mut it = e.iter(cf, IterOptions::default()).unwrap();
    drain(it.as_mut(), false).len()
}

fn settle(e: &dyn Engine, cf: u32, trigger: u64) {
    let start = Instant::now();
    while prop(e, cf, "rocksdb.num-files-at-level0") >= trigger
        && start.elapsed() < Duration::from_secs(10)
    {
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// The number after `label ` in the `nil.stats` text.
fn stat(e: &dyn Engine, cf: u32, label: &str) -> u64 {
    let text = e.property(cf, "nil.stats").unwrap();
    let rest = &text[text
        .find(&format!("{label} "))
        .unwrap_or_else(|| panic!("{label} in {text}"))
        + label.len()
        + 1..];
    rest.split(|c: char| !c.is_ascii_digit())
        .next()
        .unwrap()
        .parse()
        .unwrap()
}

#[test]
fn writes_flow_through_levels() {
    let dir = TempDir::new("lsm-levels");
    let opts = Options::small();
    let e = open(dir.path(), &CFS, &opts).unwrap();
    put_range(&e, 1, 0, 20_000, &[b'v'; 100]);
    e.flush().unwrap();
    settle(&e, 1, opts.level0_compaction_trigger as u64);
    let deeper: u64 = (1..opts.num_levels)
        .map(|l| prop(&e, 1, &format!("rocksdb.num-files-at-level{l}")))
        .sum();
    assert!(deeper > 0, "no file below level 0");
    assert!(stat(&e, 1, "flushes") > 5);
    assert!(stat(&e, 1, "compactions") > 0);
    for i in (0..20_000).step_by(997) {
        assert!(e.get(1, &key(i), None).unwrap().is_some());
    }
    assert_eq!(count(&e, 1), 20_000);
    assert_eq!(count(&e, 0), 0);
    e.close().unwrap();
}

#[test]
fn range_tombstones_are_collected_by_compaction() {
    let dir = TempDir::new("lsm-rangedel");
    let e = open(dir.path(), &CFS, &Options::small()).unwrap();
    put_range(&e, 1, 0, 3000, b"x");
    e.flush().unwrap();
    let mut b = WriteBatch::new();
    b.delete_range(1, &key(0), &key(2500));
    e.write(&b, WriteOptions::default()).unwrap();
    assert_eq!(prop(&e, 1, "nil.num-range-tombstones"), 1);
    e.flush().unwrap();
    assert_eq!(
        prop(&e, 1, "nil.num-range-tombstones"),
        1,
        "the flush moves the tombstone into the manifest"
    );
    assert_eq!(count(&e, 1), 500);
    e.compact_range(1, None, None).unwrap();
    assert_eq!(prop(&e, 1, "nil.num-range-tombstones"), 0);
    assert_eq!(prop(&e, 1, "rocksdb.estimate-num-keys"), 500);
    e.close().unwrap();
    let e = open(dir.path(), &CFS, &Options::small()).unwrap();
    assert_eq!(prop(&e, 1, "nil.num-range-tombstones"), 0);
    assert_eq!(count(&e, 1), 500);
    e.close().unwrap();
}

#[test]
fn snapshot_keeps_range_tombstone_until_released() {
    let dir = TempDir::new("lsm-rangedel-snap");
    let e = open(dir.path(), &CFS, &Options::small()).unwrap();
    put_range(&e, 1, 0, 1000, b"x");
    e.flush().unwrap();
    let s = e.snapshot().unwrap();
    let mut b = WriteBatch::new();
    b.delete_range(1, &key(0), &key(1000));
    e.write(&b, WriteOptions::default()).unwrap();
    e.compact_range(1, None, None).unwrap();
    assert_eq!(prop(&e, 1, "nil.num-range-tombstones"), 1);
    let mut it = e
        .iter(1, IterOptions::default().with_snapshot(Some(&s)))
        .unwrap();
    assert_eq!(drain(it.as_mut(), false).len(), 1000);
    drop(it);
    assert_eq!(count(&e, 1), 0);
    drop(s);
    e.compact_range(1, None, None).unwrap();
    assert_eq!(prop(&e, 1, "nil.num-range-tombstones"), 0);
    assert_eq!(prop(&e, 1, "rocksdb.total-sst-files-size"), 0);
    e.close().unwrap();
}

fn write_counted(e: &dyn Engine, n: usize) {
    for i in 0..n {
        let mut b = WriteBatch::new();
        b.put(1, &key(i), &[i as u8; 50]);
        b.put(0, b"count", &(i as u64 + 1).to_be_bytes());
        e.write(&b, WriteOptions::default()).unwrap();
    }
}

fn recovered_count(e: &dyn Engine) -> usize {
    let c = e
        .get(0, b"count", None)
        .unwrap()
        .map_or(0, |v| u64::from_be_bytes(v.try_into().unwrap()) as usize);
    for i in 0..10 {
        assert_eq!(
            e.get(1, &key(i), None).unwrap().is_some(),
            i < c,
            "key {i} with count {c}"
        );
    }
    c
}

#[test]
fn log_truncated_mid_record_recovers_a_prefix() {
    let dir = TempDir::new("lsm-trunc");
    let opts = Options {
        write_buffer_size: 64 << 20,
        ..Options::small()
    };
    let e = open(dir.path(), &CFS, &opts).unwrap();
    write_counted(&e, 10);
    e.close().unwrap();
    let logs = files(dir.path(), "log");
    assert_eq!(logs.len(), 1);
    let len = fs::metadata(&logs[0]).unwrap().len();
    fs::OpenOptions::new()
        .write(true)
        .open(&logs[0])
        .unwrap()
        .set_len(len - 7)
        .unwrap();
    let e = open(dir.path(), &CFS, &opts).unwrap();
    assert_eq!(recovered_count(&e), 9);
    // Open flushed the recovered memtable and started a new log, so writes
    // after recovery are not stuck behind the torn record.
    write_counted(&e, 10);
    e.close().unwrap();
    let e = open(dir.path(), &CFS, &opts).unwrap();
    assert_eq!(recovered_count(&e), 10);
    e.close().unwrap();
}

#[test]
fn log_with_damaged_middle_record_stops_there() {
    let dir = TempDir::new("lsm-damaged");
    let opts = Options {
        write_buffer_size: 64 << 20,
        ..Options::small()
    };
    let e = open(dir.path(), &CFS, &opts).unwrap();
    write_counted(&e, 10);
    e.close().unwrap();
    let log = &files(dir.path(), "log")[0];
    let mut data = fs::read(log).unwrap();
    let mid = data.len() / 2;
    data[mid] ^= 0x55;
    fs::write(log, &data).unwrap();
    let e = open(dir.path(), &CFS, &opts).unwrap();
    let c = recovered_count(&e);
    assert!((1..10).contains(&c), "recovered {c} batches");
    e.close().unwrap();
}

#[test]
fn orphan_files_are_removed_at_open() {
    let dir = TempDir::new("lsm-orphans");
    let e = open(dir.path(), &CFS, &Options::small()).unwrap();
    put_range(&e, 1, 0, 500, b"x");
    e.flush().unwrap();
    e.close().unwrap();
    fs::write(dir.path().join("009999.sst"), b"junk").unwrap();
    fs::write(dir.path().join("009998.log"), b"").unwrap();
    fs::write(dir.path().join("CURRENT.tmp"), b"junk").unwrap();
    fs::write(dir.path().join("MANIFEST-009997"), b"junk").unwrap();
    let e = open(dir.path(), &CFS, &Options::small()).unwrap();
    for name in ["009999.sst", "009998.log", "CURRENT.tmp", "MANIFEST-009997"] {
        assert!(!dir.path().join(name).exists(), "{name} survived open");
    }
    assert_eq!(count(&e, 1), 500);
    e.close().unwrap();
}

#[test]
fn damaged_table_block_reads_as_corruption() {
    let dir = TempDir::new("lsm-badblock");
    let e = open(dir.path(), &CFS, &Options::small()).unwrap();
    put_range(&e, 1, 0, 300, &[b'z'; 20]);
    e.flush().unwrap();
    e.close().unwrap();
    let ssts = files(dir.path(), "sst");
    assert_eq!(ssts.len(), 1);
    let mut data = fs::read(&ssts[0]).unwrap();
    data[40] ^= 0xff;
    fs::write(&ssts[0], &data).unwrap();
    let opts = Options {
        cache_bytes: 0,
        ..Options::small()
    };
    let e = open(dir.path(), &CFS, &opts).unwrap();
    assert!(matches!(e.get(1, &key(0), None), Err(Error::Corruption(_))));
    let mut it = e.iter(1, IterOptions::default()).unwrap();
    it.seek_to_first();
    assert!(!it.valid());
    assert!(matches!(it.status(), Err(Error::Corruption(_))));
    e.close().unwrap();
}

#[test]
fn sequence_numbers_survive_reopen() {
    let dir = TempDir::new("lsm-seq");
    let e = open(dir.path(), &CFS, &Options::small()).unwrap();
    put_range(&e, 1, 0, 1000, b"a");
    e.flush().unwrap();
    put_range(&e, 1, 0, 50, b"b");
    let seq = e.latest_sequence();
    assert_eq!(seq, 1050);
    e.close().unwrap();
    let e = open(dir.path(), &CFS, &Options::small()).unwrap();
    assert_eq!(e.latest_sequence(), seq);
    put_range(&e, 1, 0, 1, b"c");
    assert_eq!(e.latest_sequence(), seq + 1);
    assert_eq!(e.get(1, &key(0), None).unwrap().as_deref(), Some(&b"c"[..]));
    assert_eq!(e.get(1, &key(1), None).unwrap().as_deref(), Some(&b"b"[..]));
    assert_eq!(
        e.get(1, &key(999), None).unwrap().as_deref(),
        Some(&b"a"[..])
    );
    e.close().unwrap();
}
