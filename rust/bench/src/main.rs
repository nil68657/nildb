//! nilbench: db_bench-style workloads on the LSM and B+ tree engines.
//!
//! For each engine it runs, in fresh directories:
//!   fillseq     N keys in key order, `--batch` keys per commit
//!   fillrandom  N keys in random order, one key per commit
//!   readrandom  `--reads` point reads of random existing keys
//!   seekrandom  `--seeks` seeks to random keys, each followed by 10 nexts
//!   mixed       `--threads` threads doing `--mixed-ops` operations in total,
//!               80% reads and 20% single-key writes
//! Keys are 16 decimal digits; values are `--value-size` pseudo-random bytes.
//! Writes are not synced, as in db_bench's default.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use nilengine_api::{Engine, EngineKind, IterOptions, Options, WriteBatch, WriteOptions};

struct Args {
    engines: Vec<EngineKind>,
    num: u64,
    value_size: usize,
    batch: usize,
    reads: u64,
    seeks: u64,
    threads: usize,
    mixed_ops: u64,
    cache_mb: usize,
    dir: PathBuf,
    keep: bool,
}

fn usage() -> ! {
    eprintln!(
        "usage: nilbench [--engine lsm|btree|all] [--num N] [--value-size B] [--batch B] [--reads N] \
         [--seeks N] [--threads T] [--mixed-ops N] [--cache-mb M] [--dir DIR] [--keep]"
    );
    std::process::exit(2);
}

fn parse_args() -> Args {
    let mut a = Args {
        engines: vec![EngineKind::Lsm, EngineKind::BTree],
        num: 1_000_000,
        value_size: 100,
        batch: 100,
        reads: 1_000_000,
        seeks: 200_000,
        threads: 4,
        mixed_ops: 1_000_000,
        cache_mb: 256,
        dir: PathBuf::from("target/bench-data"),
        keep: false,
    };
    let mut it = std::env::args().skip(1);
    while let Some(flag) = it.next() {
        if flag == "--keep" {
            a.keep = true;
            continue;
        }
        let v = it.next().unwrap_or_else(|| usage());
        let num = || v.parse::<u64>().unwrap_or_else(|_| usage());
        match flag.as_str() {
            "--engine" => {
                a.engines = match v.as_str() {
                    "all" => vec![EngineKind::Lsm, EngineKind::BTree],
                    s => vec![EngineKind::parse(s).unwrap_or_else(|| usage())],
                }
            }
            "--num" => a.num = num(),
            "--value-size" => a.value_size = num() as usize,
            "--batch" => a.batch = num().max(1) as usize,
            "--reads" => a.reads = num(),
            "--seeks" => a.seeks = num(),
            "--threads" => a.threads = num().max(1) as usize,
            "--mixed-ops" => a.mixed_ops = num(),
            "--cache-mb" => a.cache_mb = num() as usize,
            "--dir" => a.dir = PathBuf::from(v),
            _ => usage(),
        }
    }
    a
}

/// xorshift64* (Vigna, "An experimental exploration of Marsaglia's xorshift
/// generators, scrambled", 2016).
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

fn key(i: u64) -> [u8; 16] {
    let mut k = [b'0'; 16];
    let mut n = i;
    for b in k.iter_mut().rev() {
        *b = b'0' + (n % 10) as u8;
        n /= 10;
    }
    k
}

/// A 1 MiB pool of random bytes; values are slices of it.
struct Values {
    pool: Vec<u8>,
    size: usize,
}

impl Values {
    fn new(size: usize) -> Values {
        let mut r = Rng(0x9e37_79b9_7f4a_7c15);
        let pool = (0..(1 << 20) + size).map(|_| r.next() as u8).collect();
        Values { pool, size }
    }

    fn get(&self, r: &mut Rng) -> &[u8] {
        let off = r.below((self.pool.len() - self.size) as u64) as usize;
        &self.pool[off..off + self.size]
    }
}

fn open(kind: EngineKind, dir: &Path, a: &Args) -> Box<dyn Engine> {
    let opts = Options {
        cache_bytes: a.cache_mb << 20,
        ..Options::default()
    };
    let res = match kind {
        EngineKind::Lsm => nilengine_lsm::open_boxed(dir, &["default"], &opts),
        EngineKind::BTree => nilengine_btree::open_boxed(dir, &["default"], &opts),
    };
    res.unwrap_or_else(|e| panic!("open {}: {e}", dir.display()))
}

fn dir_bytes(dir: &Path) -> u64 {
    std::fs::read_dir(dir)
        .map(|rd| {
            rd.filter_map(|e| e.ok())
                .filter_map(|e| e.metadata().ok())
                .filter(|m| m.is_file())
                .map(|m| m.len())
                .sum()
        })
        .unwrap_or(0)
}

fn report(engine: EngineKind, name: &str, ops: u64, secs: f64, note: &str) {
    println!(
        "| {:<5} | {:<10} | {:>9} | {:>7.2} | {:>10.0} | {:>7.2} | {} |",
        engine.name(),
        name,
        ops,
        secs,
        ops as f64 / secs,
        secs * 1e6 / ops as f64,
        note
    );
}

fn fill(e: &dyn Engine, order: impl Iterator<Item = u64>, batch: usize, vals: &Values) -> u64 {
    let mut r = Rng(42);
    let mut b = WriteBatch::with_capacity(batch * (vals.size + 32));
    let mut n = 0;
    for i in order {
        b.put(0, &key(i), vals.get(&mut r));
        n += 1;
        if b.len() == batch {
            e.write(&b, WriteOptions::default()).expect("write");
            b.clear();
        }
    }
    if !b.is_empty() {
        e.write(&b, WriteOptions::default()).expect("write");
    }
    n
}

fn run(kind: EngineKind, a: &Args, vals: &Values) {
    let root = a.dir.join(kind.name());
    let _ = std::fs::remove_dir_all(&root);

    let seq_dir = root.join("seq");
    let e = open(kind, &seq_dir, a);
    let t = Instant::now();
    let n = fill(&*e, 0..a.num, a.batch, vals);
    e.flush_wal(true).expect("sync");
    let secs = t.elapsed().as_secs_f64();
    let bytes = n as f64 * (16 + a.value_size) as f64;
    report(
        kind,
        "fillseq",
        n,
        secs,
        &format!("{:.1} MB/s, batch {}", bytes / secs / 1e6, a.batch),
    );
    e.close().expect("close");

    let rand_dir = root.join("rand");
    let e: Arc<dyn Engine> = Arc::from(open(kind, &rand_dir, a));
    let mut perm: Vec<u64> = (0..a.num).collect();
    let mut r = Rng(7);
    for i in (1..perm.len()).rev() {
        perm.swap(i, r.below(i as u64 + 1) as usize);
    }
    let t = Instant::now();
    let n = fill(&*e, perm.iter().copied(), 1, vals);
    e.flush_wal(true).expect("sync");
    let secs = t.elapsed().as_secs_f64();
    drop(perm);
    report(
        kind,
        "fillrandom",
        n,
        secs,
        &format!(
            "batch 1, {:.0} MB on disk",
            dir_bytes(&rand_dir) as f64 / 1e6
        ),
    );

    let t = Instant::now();
    let mut found = 0u64;
    let mut r = Rng(11);
    for _ in 0..a.reads {
        if e.get(0, &key(r.below(a.num)), None).expect("get").is_some() {
            found += 1;
        }
    }
    report(
        kind,
        "readrandom",
        a.reads,
        t.elapsed().as_secs_f64(),
        &format!("{found} found"),
    );

    let t = Instant::now();
    let mut seen = 0u64;
    let mut r = Rng(13);
    let mut it = e.iter(0, IterOptions::default()).expect("iter");
    for _ in 0..a.seeks {
        it.seek(&key(r.below(a.num)));
        for _ in 0..10 {
            if !it.valid() {
                break;
            }
            seen += it.value().len() as u64 / a.value_size.max(1) as u64;
            it.next();
        }
    }
    it.status().expect("iterator");
    drop(it);
    report(
        kind,
        "seek+10next",
        a.seeks,
        t.elapsed().as_secs_f64(),
        &format!("{seen} entries read"),
    );

    let t = Instant::now();
    let per = a.mixed_ops / a.threads as u64;
    let handles: Vec<_> = (0..a.threads)
        .map(|th| {
            let e = e.clone();
            let num = a.num;
            let pool = Values::new(vals.size);
            std::thread::spawn(move || {
                let mut r = Rng(1000 + th as u64);
                let mut b = WriteBatch::new();
                for _ in 0..per {
                    let k = key(r.below(num));
                    if r.below(10) < 2 {
                        b.clear();
                        b.put(0, &k, pool.get(&mut r));
                        e.write(&b, WriteOptions::default()).expect("write");
                    } else {
                        e.get(0, &k, None).expect("get");
                    }
                }
            })
        })
        .collect();
    for h in handles {
        h.join().expect("mixed worker");
    }
    report(
        kind,
        "mixed",
        per * a.threads as u64,
        t.elapsed().as_secs_f64(),
        &format!("{} threads, 80% reads", a.threads),
    );
    if let Some(stats) = e.property(0, "nil.stats") {
        for line in stats.lines() {
            eprintln!("  {} {line}", kind.name());
        }
    }
    e.close().expect("close");
    if !a.keep {
        let _ = std::fs::remove_dir_all(&root);
    }
}

fn main() {
    let a = parse_args();
    let vals = Values::new(a.value_size);
    println!(
        "keys: {}, value size: {} B, cache: {} MiB, directory: {}",
        a.num,
        a.value_size,
        a.cache_mb,
        a.dir.display()
    );
    println!("| engine | workload | ops | seconds | ops/s | us/op | notes |");
    println!("|---|---|---:|---:|---:|---:|---|");
    for &kind in &a.engines {
        run(kind, &a, &vals);
    }
}
