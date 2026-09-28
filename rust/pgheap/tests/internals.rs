//! pgheap tests the shared suite cannot express: HOT chains and pruning,
//! TOAST at its threshold, VACUUM and the snapshots that hold it back,
//! autovacuum, and recovery from torn pages, crashes inside a checkpoint,
//! aborted and uncommitted transactions and a commit log spanning pages.

use std::os::unix::fs::FileExt;
use std::time::{Duration, Instant};

use nilengine_api::{CrashMode, Engine, Error, Options, WriteBatch, WriteOptions};
use nilengine_conformance::{Model, Rng, TempDir, check_state};
use nilengine_pgheap::{PgHeapEngine, TOAST_CHUNK, TOAST_THRESHOLD, open};

const CFS: [&str; 2] = ["default", "data"];

fn opts() -> Options {
    Options {
        autovacuum: false,
        track_unsynced_writes: true,
        ..Options::small()
    }
}

fn prop(e: &dyn Engine, cf: u32, name: &str) -> u64 {
    e.property(cf, name)
        .unwrap_or_else(|| panic!("property {name}"))
        .parse::<f64>()
        .unwrap_or_else(|_| panic!("property {name} is not a number")) as u64
}

fn put(e: &dyn Engine, cf: u32, k: &[u8], v: &[u8]) {
    let mut b = WriteBatch::new();
    b.put(cf, k, v);
    e.write(&b, WriteOptions::default()).unwrap();
}

fn get(e: &dyn Engine, cf: u32, k: &[u8]) -> Option<Vec<u8>> {
    e.get(cf, k, None).unwrap()
}

fn key(i: usize) -> Vec<u8> {
    format!("key{i:06}").into_bytes()
}

fn apply(e: &dyn Engine, model: &mut Model, b: &WriteBatch, sync: bool) {
    e.write(b, WriteOptions { sync }).unwrap();
    model.apply(b).unwrap();
}

#[test]
fn hot_updates_keep_one_index_entry_per_key() {
    let dir = TempDir::new("pg-hot");
    let e = open(dir.path(), &CFS, &opts()).unwrap();
    put(&e, 1, b"hot", b"v0");
    let mut held = None;
    for i in 1..=5000u32 {
        put(&e, 1, b"hot", format!("value-{i}").as_bytes());
        if i == 2500 {
            held = Some(e.snapshot().unwrap());
        }
        if i == 2550 {
            let s = held.take().unwrap();
            assert_eq!(
                e.get(1, b"hot", Some(&s)).unwrap().as_deref(),
                Some(&b"value-2500"[..]),
                "a snapshot reads the version it saw through the HOT chain"
            );
        }
    }
    assert_eq!(get(&e, 1, b"hot").as_deref(), Some(&b"value-5000"[..]));
    assert_eq!(prop(&e, 1, "nil.pgheap.n-tup-upd"), 5000);
    assert_eq!(
        prop(&e, 1, "nil.pgheap.n-tup-hot-upd"),
        5000,
        "pruning frees room on the page, so every update stays HOT"
    );
    assert_eq!(
        prop(&e, 1, "nil.pgheap.index-entries"),
        1,
        "the index still points at the chain's root"
    );
    assert_eq!(prop(&e, 1, "nil.pgheap.heap-bytes"), 8192);
    assert!(prop(&e, 1, "nil.pgheap.pruned") > 4000);
    e.close().unwrap();
    let e = open(dir.path(), &CFS, &opts()).unwrap();
    assert_eq!(get(&e, 1, b"hot").as_deref(), Some(&b"value-5000"[..]));
    assert_eq!(prop(&e, 1, "nil.pgheap.index-entries"), 1);
    e.close().unwrap();
}

#[test]
fn non_hot_updates_add_index_entries_until_vacuum() {
    let dir = TempDir::new("pg-nonhot");
    let e = open(dir.path(), &CFS, &opts()).unwrap();
    // Four tuples of about 1900 bytes fill a page, so most updates must go
    // to another page and need an index entry of their own.
    let big = |n: u8| vec![n; 1900];
    for k in 0..4u8 {
        put(&e, 1, &[k], &big(0));
    }
    let s = e.snapshot().unwrap();
    for round in 1..=3u8 {
        for k in 0..4u8 {
            put(&e, 1, &[k], &big(round));
        }
    }
    assert!(prop(&e, 1, "nil.pgheap.index-entries") > 4);
    assert!(prop(&e, 1, "nil.pgheap.n-tup-hot-upd") < 12);
    assert_eq!(e.get(1, &[2], Some(&s)).unwrap(), Some(big(0)));
    drop(s);
    e.compact_range(1, None, None).unwrap();
    assert_eq!(prop(&e, 1, "nil.pgheap.index-entries"), 4);
    assert_eq!(prop(&e, 1, "nil.pgheap.n-dead-tup"), 0);
    for k in 0..4u8 {
        assert_eq!(get(&e, 1, &[k]), Some(big(3)));
    }
    e.close().unwrap();
}

#[test]
fn toast_round_trips_at_the_threshold() {
    let dir = TempDir::new("pg-toast");
    let e = open(dir.path(), &CFS, &opts()).unwrap();
    // Tuple length is the 32-byte header, a one-byte key and the value.
    let inline_max = TOAST_THRESHOLD - 32 - 1;
    let mut rng = Rng::new(3);
    let mut model = Model::new(CFS.len());
    let mut b = WriteBatch::new();
    b.put(1, b"a", &rng.bytes(inline_max));
    apply(&e, &mut model, &b, false);
    assert_eq!(
        prop(&e, 1, "nil.pgheap.toast-bytes"),
        0,
        "at the threshold the value stays inline"
    );
    let mut b = WriteBatch::new();
    b.put(1, b"b", &rng.bytes(inline_max + 1));
    apply(&e, &mut model, &b, false);
    assert!(
        prop(&e, 1, "nil.pgheap.toast-bytes") > 0,
        "one byte over goes to TOAST"
    );
    assert_eq!(
        prop(&e, 1, "nil.pgheap.toast-n-live-tup"),
        2,
        "2000 bytes make two chunks"
    );
    for (k, len) in [
        (&b"c"[..], TOAST_CHUNK),
        (b"d", TOAST_CHUNK + 1),
        (b"e", 2 * TOAST_CHUNK),
        (b"f", 2 * TOAST_CHUNK - 1),
        (b"g", 1 << 20),
    ] {
        let mut b = WriteBatch::new();
        b.put(1, k, &rng.bytes(len));
        apply(&e, &mut model, &b, false);
    }
    check_state(&e, &model, None, "after TOAST inserts");
    e.close().unwrap();
    let e = open(dir.path(), &CFS, &opts()).unwrap();
    check_state(&e, &model, None, "after reopen");
    // Shrinking a value below the threshold marks its chunks dead; VACUUM
    // removes them.
    let before = prop(&e, 1, "nil.pgheap.toast-n-live-tup");
    let mut b = WriteBatch::new();
    b.put(1, b"g", b"small again");
    b.delete(1, b"b");
    apply(&e, &mut model, &b, false);
    let chunks_g = (1usize << 20).div_ceil(TOAST_CHUNK) as u64;
    assert_eq!(
        prop(&e, 1, "nil.pgheap.toast-n-live-tup"),
        before - chunks_g - 2
    );
    assert_eq!(prop(&e, 1, "nil.pgheap.toast-n-dead-tup"), chunks_g + 2);
    e.compact_range(1, None, None).unwrap();
    assert_eq!(prop(&e, 1, "nil.pgheap.toast-n-dead-tup"), 0);
    check_state(&e, &model, None, "after VACUUM of the TOAST heap");
    e.close().unwrap();
}

#[test]
fn vacuum_reclaims_space_for_reuse() {
    let dir = TempDir::new("pg-vacuum");
    let e = open(dir.path(), &CFS, &opts()).unwrap();
    let fill = |e: &PgHeapEngine, tag: u8| {
        for chunk in 0..20 {
            let mut b = WriteBatch::new();
            for i in chunk * 100..(chunk + 1) * 100 {
                b.put(1, &key(i), &[tag; 200]);
            }
            e.write(&b, WriteOptions::default()).unwrap();
        }
    };
    fill(&e, 1);
    let heap = prop(&e, 1, "nil.pgheap.heap-bytes");
    let mut b = WriteBatch::new();
    b.delete_range(1, &key(0), &key(2000));
    e.write(&b, WriteOptions::default()).unwrap();
    assert_eq!(prop(&e, 1, "nil.pgheap.n-dead-tup"), 2000);
    e.compact_range(1, None, None).unwrap();
    assert_eq!(prop(&e, 1, "nil.pgheap.n-dead-tup"), 0);
    assert_eq!(prop(&e, 1, "nil.pgheap.n-live-tup"), 0);
    assert_eq!(prop(&e, 1, "nil.pgheap.index-entries"), 0);
    assert_eq!(prop(&e, 1, "nil.pgheap.vacuum-count"), 1);
    fill(&e, 2);
    assert_eq!(
        prop(&e, 1, "nil.pgheap.heap-bytes"),
        heap,
        "the second fill reuses the freed pages"
    );
    // Update every row five more times, vacuuming between rounds: the heap
    // settles near twice the size of one generation of rows.
    for round in 3..8u8 {
        fill(&e, round);
        e.compact_range(1, None, None).unwrap();
    }
    let after = prop(&e, 1, "nil.pgheap.heap-bytes");
    assert!(
        after <= 2 * heap + 16 * 8192,
        "heap grew from {heap} to {after} bytes"
    );
    assert_eq!(get(&e, 1, &key(1234)), Some(vec![7u8; 200]));
    e.close().unwrap();
}

#[test]
fn long_snapshot_blocks_vacuum_until_released() {
    let dir = TempDir::new("pg-longsnap");
    let e = open(dir.path(), &CFS, &opts()).unwrap();
    let rows = 500;
    let write_all = |tag: u8| {
        let mut b = WriteBatch::new();
        for i in 0..rows {
            b.put(1, &key(i), &[tag; 100]);
        }
        e.write(&b, WriteOptions::default()).unwrap();
    };
    write_all(0);
    let s = e.snapshot().unwrap();
    for round in 1..=4 {
        write_all(round);
    }
    e.compact_range(1, None, None).unwrap();
    assert_eq!(
        prop(&e, 1, "nil.pgheap.n-dead-tup"),
        4 * rows as u64,
        "every version newer than the snapshot's view stays"
    );
    assert_eq!(e.get(1, &key(7), Some(&s)).unwrap(), Some(vec![0u8; 100]));
    let bloated = prop(&e, 1, "nil.pgheap.heap-bytes");
    drop(s);
    e.compact_range(1, None, None).unwrap();
    assert_eq!(prop(&e, 1, "nil.pgheap.n-dead-tup"), 0);
    for round in 5..=8 {
        write_all(round);
    }
    let now = prop(&e, 1, "nil.pgheap.heap-bytes");
    assert!(
        now <= bloated,
        "new versions fill the space VACUUM freed: {now} > {bloated} bytes"
    );
    assert_eq!(get(&e, 1, &key(7)), Some(vec![8u8; 100]));
    e.close().unwrap();
}

#[test]
fn autovacuum_removes_dead_tuples() {
    let dir = TempDir::new("pg-autovac");
    let o = Options {
        autovacuum: true,
        autovacuum_naptime_ms: 20,
        autovacuum_threshold: 50,
        autovacuum_scale_percent: 20,
        ..opts()
    };
    let e = open(dir.path(), &CFS, &o).unwrap();
    for round in 0..10u8 {
        let mut b = WriteBatch::new();
        for i in 0..200 {
            b.put(0, &key(i), &[round; 64]);
        }
        b.delete(0, &key(round as usize));
        e.write(&b, WriteOptions::default()).unwrap();
    }
    let deadline = Instant::now() + Duration::from_secs(10);
    while prop(&e, 0, "nil.pgheap.autovacuum-count") == 0
        || prop(&e, 0, "nil.pgheap.n-dead-tup") > 90
    {
        assert!(Instant::now() < deadline, "autovacuum never ran");
        std::thread::sleep(Duration::from_millis(20));
    }
    assert_eq!(get(&e, 0, &key(150)), Some(vec![9u8; 64]));
    e.close().unwrap();
}

#[test]
fn aborted_and_uncommitted_transactions_stay_invisible_after_recovery() {
    let dir = TempDir::new("pg-abort");
    let e = open(dir.path(), &CFS, &opts()).unwrap();
    put(&e, 0, b"a", b"committed");
    put(&e, 0, b"notnum", b"text");
    let committed = e.latest_sequence();
    let mut bad = WriteBatch::new();
    bad.put(0, b"b", b"aborted");
    bad.merge(0, b"notnum", &1i64.to_le_bytes());
    assert!(e.write(&bad, WriteOptions::default()).is_err());
    assert_eq!(get(&e, 0, b"b"), None);
    let mut open_txn = WriteBatch::new();
    open_txn.put(0, b"c", b"uncommitted");
    open_txn.put(0, b"a", b"overwritten by an uncommitted transaction");
    e.test_write_uncommitted(&open_txn).unwrap();
    // The checkpoint writes the pages holding the uncommitted tuples.
    e.flush().unwrap();
    e.crash(CrashMode::DropUnsynced).unwrap();

    let e = open(dir.path(), &CFS, &opts()).unwrap();
    assert_eq!(get(&e, 0, b"a").as_deref(), Some(&b"committed"[..]));
    assert_eq!(get(&e, 0, b"b"), None);
    assert_eq!(get(&e, 0, b"c"), None);
    assert_eq!(e.latest_sequence(), committed);
    put(&e, 0, b"c", b"after recovery");
    assert_eq!(get(&e, 0, b"c").as_deref(), Some(&b"after recovery"[..]));
    e.compact_range(0, None, None).unwrap();
    e.close().unwrap();
    let e = open(dir.path(), &CFS, &opts()).unwrap();
    assert_eq!(get(&e, 0, b"a").as_deref(), Some(&b"committed"[..]));
    assert_eq!(get(&e, 0, b"c").as_deref(), Some(&b"after recovery"[..]));
    e.close().unwrap();
}

fn tear(path: &std::path::Path, block: u64) {
    let f = std::fs::OpenOptions::new().write(true).open(path).unwrap();
    f.write_all_at(&[0x5a; 4096], block * 8192 + 4096).unwrap();
    f.sync_all().unwrap();
}

#[test]
fn torn_page_is_repaired_from_its_full_page_image() {
    let dir = TempDir::new("pg-torn");
    let e = open(dir.path(), &CFS, &opts()).unwrap();
    let mut b = WriteBatch::new();
    for i in 0..40 {
        b.put(0, &key(i), &[1u8; 50]);
    }
    e.write(&b, WriteOptions::default()).unwrap();
    // The checkpoint moves the redo pointer past every page's LSN, so the
    // next change to the page logs a full image of it.
    e.flush().unwrap();
    put(&e, 0, &key(5), b"after the checkpoint");
    e.flush_wal(true).unwrap();
    let heap = e.test_heap_path(0, false).unwrap();
    e.crash(CrashMode::DropUnsynced).unwrap();
    // The second half of the page reaches the disk torn.
    tear(&heap, 0);
    let e = open(dir.path(), &CFS, &opts()).unwrap();
    assert_eq!(
        get(&e, 0, &key(5)).as_deref(),
        Some(&b"after the checkpoint"[..])
    );
    for i in (0..40).filter(|&i| i != 5) {
        assert_eq!(get(&e, 0, &key(i)), Some(vec![1u8; 50]), "key {i}");
    }
    e.close().unwrap();

    // Without a change since the last checkpoint there is no image to
    // restore, and the checksum catches the torn page.
    tear(&heap, 0);
    let e = open(dir.path(), &CFS, &opts()).unwrap();
    assert!(matches!(e.get(0, &key(5), None), Err(Error::Corruption(_))));
    e.close().unwrap();
}

#[test]
fn crash_during_checkpoint_recovers_every_durable_batch() {
    let batch = |rng: &mut Rng, i: u64| {
        let mut b = WriteBatch::new();
        let cf = rng.index(2) as u32;
        let k = key(rng.index(300));
        let n = rng.index(300);
        b.put(cf, &k, &rng.bytes(n));
        b.put(0, b"marker", &(i + 1).to_be_bytes());
        b
    };
    for step in 1..=4u8 {
        let dir = TempDir::new("pg-ckptcrash");
        let e = open(dir.path(), &CFS, &opts()).unwrap();
        let mut model = Model::new(CFS.len());
        let mut rng = Rng::new(step as u64);
        for i in 0..400 {
            let b = batch(&mut rng, i);
            apply(&e, &mut model, &b, i == 299);
        }
        e.test_crash_during_checkpoint(
            step,
            CrashMode::PartialUnsynced {
                seed: 7 + step as u64,
            },
        )
        .unwrap();
        let e = open(dir.path(), &CFS, &opts()).unwrap();
        let k = u64::from_be_bytes(get(&e, 0, b"marker").unwrap().try_into().unwrap());
        assert!(
            k >= 300,
            "step {step}: recovered {k} batches, 300 were synced"
        );
        let mut want = Model::new(CFS.len());
        let mut rng = Rng::new(step as u64);
        for i in 0..k {
            want.apply(&batch(&mut rng, i)).unwrap();
        }
        check_state(&e, &want, None, &format!("crash at checkpoint step {step}"));
        e.close().unwrap();
    }
}

#[test]
fn commit_log_spanning_pages_survives_a_crash() {
    let dir = TempDir::new("pg-clog");
    let e = open(dir.path(), &CFS, &opts()).unwrap();
    let mut seed = WriteBatch::new();
    seed.put(0, b"notnum", b"text");
    e.write(&seed, WriteOptions::default()).unwrap();
    let good = |i: usize| {
        let mut b = WriteBatch::new();
        b.put(1, &key(i % 3000), &(i as u64).to_le_bytes());
        b.put(0, b"marker", &(i as u64).to_be_bytes());
        b
    };
    let mut durable = 0;
    // 34,000 transactions need two commit-log pages (32,640 xids each).
    for i in 0..34_000usize {
        if i % 997 == 0 {
            let mut b = WriteBatch::new();
            b.put(1, format!("aborted{i}").as_bytes(), b"x");
            b.merge(0, b"notnum", &1i64.to_le_bytes());
            assert!(e.write(&b, WriteOptions::default()).is_err());
            continue;
        }
        let sync = i % 5000 == 4999;
        e.write(&good(i), WriteOptions { sync }).unwrap();
        if sync {
            durable = i;
        }
    }
    assert!(prop(&e, 0, "nil.pgheap.next-xid") > 32_640);
    e.crash(CrashMode::PartialUnsynced { seed: 42 }).unwrap();
    let e = open(dir.path(), &CFS, &opts()).unwrap();
    let last = u64::from_be_bytes(get(&e, 0, b"marker").unwrap().try_into().unwrap()) as usize;
    assert!(
        last >= durable,
        "recovered through batch {last}, {durable} was synced"
    );
    let mut want = Model::new(CFS.len());
    want.apply(&seed).unwrap();
    for i in (0..=last).filter(|i| i % 997 != 0) {
        want.apply(&good(i)).unwrap();
    }
    check_state(&e, &want, None, "after the crash");
    put(&e, 1, b"after", b"recovery");
    e.close().unwrap();
    let e = open(dir.path(), &CFS, &opts()).unwrap();
    assert_eq!(get(&e, 1, b"after").as_deref(), Some(&b"recovery"[..]));
    assert_eq!(get(&e, 1, b"aborted997"), None);
    e.close().unwrap();
}
