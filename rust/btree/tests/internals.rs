//! B+ tree tests the shared suite cannot express: deep trees with splits,
//! merges and subtree-freeing range deletes; page reuse and the snapshots
//! that block it; the free list rebuilt at open; and hand-picked crashes
//! that lose only meta pages or only tree pages.

use nilengine_api::{Engine, Options, WriteBatch, WriteOptions};
use nilengine_btree::{BTreeEngine, open};
use nilengine_conformance::{Model, Rng, TempDir, check_state};

const CFS: [&str; 2] = ["default", "data"];

fn small() -> Options {
    Options {
        page_size: 512,
        ..Options::small()
    }
}

fn key(i: usize) -> Vec<u8> {
    format!("key{i:07}").into_bytes()
}

fn prop(e: &dyn Engine, cf: u32, name: &str) -> u64 {
    e.property(cf, name)
        .unwrap_or_else(|| panic!("property {name}"))
        .parse()
        .unwrap()
}

fn write(e: &dyn Engine, model: &mut Model, b: &WriteBatch, sync: bool) {
    e.write(b, WriteOptions { sync }).unwrap();
    model.apply(b).unwrap();
}

fn overwrite_all(e: &dyn Engine, model: &mut Model, n: usize, tag: u8) {
    for start in (0..n).step_by(100) {
        let mut b = WriteBatch::new();
        for i in start..(start + 100).min(n) {
            b.put(1, &key(i), &[tag; 24]);
        }
        write(e, model, &b, false);
    }
}

#[test]
fn deep_tree_splits_merges_and_range_deletes() {
    let dir = TempDir::new("bt-deep");
    let e = open(dir.path(), &CFS, &small()).unwrap();
    let mut model = Model::new(2);
    let mut rng = Rng::new(7);
    let mut order: Vec<usize> = (0..20_000).collect();
    for i in (1..order.len()).rev() {
        order.swap(i, rng.index(i + 1));
    }
    for chunk in order.chunks(50) {
        let mut b = WriteBatch::new();
        for &i in chunk {
            let n = rng.index(40);
            let v = rng.bytes(n);
            b.put(1, &key(i), &v);
        }
        write(&e, &mut model, &b, false);
    }
    let depth = prop(&e, 1, "nil.btree.depth");
    assert!(
        depth >= 4,
        "20000 keys in 512-byte pages gave depth {depth}"
    );
    check_state(&e, &model, None, "after random inserts");

    for chunk in (0..20_000).collect::<Vec<_>>().chunks(200) {
        let mut b = WriteBatch::new();
        for &i in chunk {
            if i % 4 != 0 {
                b.delete(1, &key(i));
            }
        }
        write(&e, &mut model, &b, false);
    }
    check_state(&e, &model, None, "after deleting three keys in four");

    let mut b = WriteBatch::new();
    b.delete_range(1, &key(1000), &key(15000));
    b.delete_range(1, &key(17001), &key(17003));
    b.put(1, &key(5000), b"inside the deleted range");
    write(&e, &mut model, &b, false);
    check_state(&e, &model, None, "after range deletes");
    assert_eq!(
        prop(&e, 1, "rocksdb.estimate-num-keys") as usize,
        model.cfs[1].len()
    );

    let mut b = WriteBatch::new();
    b.delete_range(1, b"", &key(19_990));
    write(&e, &mut model, &b, true);
    check_state(&e, &model, None, "after deleting nearly everything");
    assert!(prop(&e, 1, "nil.btree.depth") <= 2);
    e.close().unwrap();

    let e = open(dir.path(), &CFS, &small()).unwrap();
    check_state(&e, &model, None, "after reopen");
    assert_eq!(
        prop(&e, 1, "rocksdb.estimate-num-keys") as usize,
        model.cfs[1].len()
    );
    e.close().unwrap();
}

#[test]
fn freed_pages_are_reused() {
    let dir = TempDir::new("bt-reuse");
    let e = open(dir.path(), &CFS, &small()).unwrap();
    let mut model = Model::new(2);
    overwrite_all(&e, &mut model, 2000, 0);
    e.flush_wal(true).unwrap();
    let base = prop(&e, 1, "nil.btree.num-pages");
    for round in 1..=20 {
        overwrite_all(&e, &mut model, 2000, round);
        e.flush_wal(true).unwrap();
    }
    let synced = prop(&e, 1, "nil.btree.num-pages");
    assert!(
        synced <= 2 * base + 100,
        "file grew from {base} to {synced} pages with a sync per round"
    );
    // Without syncs, reuse waits for the automatic sync every
    // max_unsynced_free_pages freed pages.
    for round in 21..=40 {
        overwrite_all(&e, &mut model, 2000, round);
    }
    let unsynced = prop(&e, 1, "nil.btree.num-pages");
    let bound = synced + 3 * small().max_unsynced_free_pages as u64;
    assert!(
        unsynced <= bound,
        "file grew from {synced} to {unsynced} pages without syncs"
    );
    check_state(&e, &model, None, "after rewrites");
    e.close().unwrap();
}

#[test]
fn snapshot_pins_pages_until_released() {
    let dir = TempDir::new("bt-pin");
    let e = open(dir.path(), &CFS, &small()).unwrap();
    let mut model = Model::new(2);
    overwrite_all(&e, &mut model, 1000, 1);
    e.flush_wal(true).unwrap();
    let s = e.snapshot().unwrap();
    let old = model.clone();
    let base = prop(&e, 1, "nil.btree.num-pages");
    for round in 2..12 {
        overwrite_all(&e, &mut model, 1000, round);
        e.flush_wal(true).unwrap();
    }
    let pinned = prop(&e, 1, "nil.btree.num-pages");
    assert!(
        pinned > base + 300,
        "a snapshot must keep old pages: {base} -> {pinned}"
    );
    assert!(prop(&e, 1, "nil.btree.pending-free-pages") > 300);
    check_state(&e, &old, Some(&s), "snapshot after ten rewrites");
    drop(s);
    for round in 12..22 {
        overwrite_all(&e, &mut model, 1000, round);
        e.flush_wal(true).unwrap();
    }
    let after = prop(&e, 1, "nil.btree.num-pages");
    assert!(
        after <= pinned + 50,
        "pages kept growing after the snapshot was released: {pinned} -> {after}"
    );
    check_state(&e, &model, None, "after the snapshot was released");
    e.close().unwrap();
}

#[test]
fn free_list_is_rebuilt_at_open() {
    let dir = TempDir::new("bt-freelist");
    let e = open(dir.path(), &CFS, &small()).unwrap();
    let mut model = Model::new(2);
    overwrite_all(&e, &mut model, 3000, 1);
    let mut b = WriteBatch::new();
    b.delete_range(1, &key(100), &key(2900));
    write(&e, &mut model, &b, true);
    e.close().unwrap();
    let e = open(dir.path(), &CFS, &small()).unwrap();
    let free = prop(&e, 1, "nil.btree.free-pages");
    assert!(free > 50, "only {free} free pages after deleting most keys");
    let pages = prop(&e, 1, "nil.btree.num-pages");
    overwrite_all(&e, &mut model, 1500, 2);
    assert_eq!(
        prop(&e, 1, "nil.btree.num-pages"),
        pages,
        "new writes must reuse free pages"
    );
    check_state(&e, &model, None, "after reusing free pages");
    e.close().unwrap();
}

#[test]
fn overflow_values_round_trip_and_free() {
    let dir = TempDir::new("bt-overflow");
    let e = open(dir.path(), &CFS, &small()).unwrap();
    let mut model = Model::new(2);
    let mut rng = Rng::new(3);
    let mut b = WriteBatch::new();
    for i in 0..200 {
        b.put(1, &key(i), &rng.bytes(5000 + i));
    }
    write(&e, &mut model, &b, true);
    let pages = prop(&e, 1, "nil.btree.num-pages");
    assert!(
        pages > 200 * 10,
        "5 KB values need overflow pages, file has {pages}"
    );
    check_state(&e, &model, None, "large values");
    let mut b = WriteBatch::new();
    for i in 0..200 {
        b.put(1, &key(i), b"small");
    }
    write(&e, &mut model, &b, true);
    let mut b = WriteBatch::new();
    for i in 0..200 {
        b.put(1, &key(i), &rng.bytes(5000));
    }
    write(&e, &mut model, &b, true);
    let again = prop(&e, 1, "nil.btree.num-pages");
    assert!(
        again <= pages + 50,
        "freed overflow pages were not reused: {pages} -> {again}"
    );
    check_state(&e, &model, None, "rewritten large values");
    e.close().unwrap();
    let e = open(dir.path(), &CFS, &small()).unwrap();
    check_state(&e, &model, None, "large values after reopen");
    e.close().unwrap();
}

/// Commits A (synced), then B and C (unsynced), and returns the engine.
fn three_commits(dir: &TempDir) -> BTreeEngine {
    let opts = Options {
        track_unsynced_writes: true,
        ..small()
    };
    let e = open(dir.path(), &CFS, &opts).unwrap();
    let mut model = Model::new(2);
    let mut a = WriteBatch::new();
    for i in 0..300 {
        a.put(1, &key(i), b"A");
    }
    write(&e, &mut model, &a, true);
    let mut b = WriteBatch::new();
    for i in 0..300 {
        b.put(1, &key(i), b"B");
    }
    write(&e, &mut model, &b, false);
    let mut c = WriteBatch::new();
    c.put(1, b"c-only", b"C");
    write(&e, &mut model, &c, false);
    let unsynced = e.unsynced_pages();
    assert!(
        unsynced.contains(&0) || unsynced.contains(&1),
        "an unsynced commit writes a meta page"
    );
    assert!(
        unsynced.iter().any(|&p| p >= 2),
        "an unsynced commit writes tree pages"
    );
    e
}

fn reopen_value(dir: &TempDir) -> (Vec<u8>, bool) {
    let e = open(dir.path(), &CFS, &small()).unwrap();
    let v = e.get(1, &key(150), None).unwrap().unwrap();
    let c = e.get(1, b"c-only", None).unwrap().is_some();
    e.close().unwrap();
    (v, c)
}

#[test]
fn lost_meta_write_falls_back_to_the_durable_commit() {
    let dir = TempDir::new("bt-lost-meta");
    three_commits(&dir).crash_pages(|p| p < 2).unwrap();
    assert_eq!(reopen_value(&dir), (b"A".to_vec(), false));
}

#[test]
fn lost_tree_page_under_a_surviving_meta_falls_back() {
    let dir = TempDir::new("bt-lost-page");
    three_commits(&dir).crash_pages(|p| p >= 2).unwrap();
    assert_eq!(reopen_value(&dir), (b"A".to_vec(), false));
}

#[test]
fn data_file_truncated_mid_page_falls_back() {
    let dir = TempDir::new("bt-truncated");
    // Pages freed by B wait for a sync, so B and C append their pages at the
    // end of the file; cutting 1000 bytes damages C's tree.
    three_commits(&dir).crash_pages(|_| false).unwrap();
    let path = nilengine_btree::data_path(dir.path());
    let len = std::fs::metadata(&path).unwrap().len();
    std::fs::OpenOptions::new()
        .write(true)
        .open(&path)
        .unwrap()
        .set_len(len - 1000)
        .unwrap();
    assert_eq!(reopen_value(&dir), (b"A".to_vec(), false));
}

#[test]
fn surviving_unsynced_pages_keep_the_latest_commit() {
    let dir = TempDir::new("bt-kept");
    three_commits(&dir).crash_pages(|_| false).unwrap();
    assert_eq!(reopen_value(&dir), (b"B".to_vec(), true));
}
