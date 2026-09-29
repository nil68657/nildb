//! The trait every engine implements. Its operations are the ones
//! `internal/store` calls on RocksDB: named column families, atomic
//! multi-family write batches, point and multi-key reads, snapshots, bounded
//! iterators in both directions, flush, log sync, compact-range, property
//! reads, checkpoints and approximate range sizes.

use std::any::Any;
use std::fmt;
use std::path::Path;
use std::sync::Arc;

use crate::batch::{CfId, WriteBatch};
use crate::error::Result;
use crate::options::{IterOptions, WriteOptions};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum EngineKind {
    Lsm,
    BTree,
    PgHeap,
}

impl EngineKind {
    pub fn name(self) -> &'static str {
        match self {
            EngineKind::Lsm => "lsm",
            EngineKind::BTree => "btree",
            EngineKind::PgHeap => "pgheap",
        }
    }

    pub fn parse(s: &str) -> Option<EngineKind> {
        match s {
            "lsm" => Some(EngineKind::Lsm),
            "btree" | "bptree" | "b+tree" => Some(EngineKind::BTree),
            "pgheap" => Some(EngineKind::PgHeap),
            _ => None,
        }
    }
}

/// A consistent read view. `seq` is a sequence number in the LSM engine, a
/// transaction id in the B+ tree engine and the last committed xid in
/// pgheap; either way every write committed at or before `seq` is visible
/// and nothing later is. Clones share one view, and the view is released
/// when the last clone and every iterator opened on it are dropped.
#[derive(Clone)]
pub struct Snapshot {
    seq: u64,
    inner: Arc<dyn Any + Send + Sync>,
}

impl Snapshot {
    /// Engines wrap their own snapshot state in `inner` and recover it with
    /// [`Snapshot::downcast`].
    pub fn new(seq: u64, inner: Arc<dyn Any + Send + Sync>) -> Self {
        Snapshot { seq, inner }
    }

    pub fn seq(&self) -> u64 {
        self.seq
    }

    pub fn downcast<T: Any + Send + Sync>(&self) -> Option<&T> {
        self.inner.downcast_ref::<T>()
    }
}

impl fmt::Debug for Snapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Snapshot").field("seq", &self.seq).finish()
    }
}

/// An iterator over one column family, restricted to the bounds it was
/// opened with. Positioning calls never fail; an I/O or corruption error
/// makes the iterator invalid and `status` returns it. `key` and `value`
/// borrow from the iterator and stay valid until the next positioning call.
///
/// The seek calls follow RocksDB: `seek` finds the first key at or after the
/// target (never below the lower bound) and `seek_for_prev` the last key at
/// or before it (never at or above the upper bound).
pub trait DbIterator: Send {
    fn valid(&self) -> bool;
    fn seek_to_first(&mut self);
    fn seek_to_last(&mut self);
    fn seek(&mut self, target: &[u8]);
    fn seek_for_prev(&mut self, target: &[u8]);
    fn next(&mut self);
    fn prev(&mut self);
    fn key(&self) -> &[u8];
    fn value(&self) -> &[u8];
    fn status(&self) -> Result<()>;
}

/// How [`Engine::crash`] damages the files it leaves behind. Both modes model
/// power loss after the last completed sync.
#[derive(Clone, Copy, Debug)]
pub enum CrashMode {
    /// Every write since the last sync is lost.
    DropUnsynced,
    /// Each unsynced write is kept, lost or torn at a 512-byte boundary,
    /// chosen by a generator seeded with `seed`.
    PartialUnsynced { seed: u64 },
}

pub trait Engine: Send + Sync {
    fn kind(&self) -> EngineKind;

    /// Column family names in `CfId` order: the order given to `open`.
    fn column_families(&self) -> Vec<String>;

    /// Longest key the engine accepts.
    fn max_key_len(&self) -> usize;

    /// Longest value the engine accepts.
    fn max_value_len(&self) -> usize;

    /// Applies `batch` atomically. With `opts.sync` the batch is durable
    /// when the call returns; without it a process crash keeps the batch and
    /// a power loss may drop it, together with every later batch.
    fn write(&self, batch: &WriteBatch, opts: WriteOptions) -> Result<()>;

    /// Reads one key from the latest state or from `snapshot`.
    fn get(&self, cf: CfId, key: &[u8], snapshot: Option<&Snapshot>) -> Result<Option<Vec<u8>>>;

    /// Reads several keys from one consistent view: `snapshot`, or a
    /// snapshot taken for the call.
    fn multi_get(
        &self,
        cf: CfId,
        keys: &[&[u8]],
        snapshot: Option<&Snapshot>,
    ) -> Result<Vec<Option<Vec<u8>>>> {
        let owned;
        let snap = match snapshot {
            Some(s) => s,
            None => {
                owned = self.snapshot()?;
                &owned
            }
        };
        keys.iter().map(|k| self.get(cf, k, Some(snap))).collect()
    }

    /// Pins the current state. Old versions that the snapshot can see stay
    /// readable until it is dropped.
    fn snapshot(&self) -> Result<Snapshot>;

    /// Opens an iterator over `cf`. It reads `opts.snapshot`, or the state at
    /// the moment of the call, and keeps that state readable until dropped,
    /// even if the snapshot it came from is released first.
    fn iter(&self, cf: CfId, opts: IterOptions) -> Result<Box<dyn DbIterator>>;

    /// Makes every write committed before the call durable. The LSM engine
    /// writes its memtables to table files; the B+ tree syncs its file.
    fn flush(&self) -> Result<()>;

    /// Pushes buffered log data to the operating system and, with `sync`,
    /// to stable storage. Both engines write each batch to the operating
    /// system before `write` returns, so `flush_wal(false)` has nothing to
    /// push.
    fn flush_wal(&self, sync: bool) -> Result<()>;

    /// Reorganizes the key range `[start, end)` of `cf` (`None` is open). The
    /// LSM engine compacts every file that overlaps the range down to the
    /// bottom level, which discards deleted and overwritten data no snapshot
    /// can see. pgheap runs VACUUM on the whole column family. The B+ tree
    /// has nothing to reclaim and returns at once.
    fn compact_range(&self, cf: CfId, start: Option<&[u8]>, end: Option<&[u8]>) -> Result<()>;

    /// Reads a named property. Names follow RocksDB where the meaning
    /// matches (`rocksdb.estimate-num-keys`, `rocksdb.num-snapshots`); the
    /// rest start with `nil.`. Unknown names return `None`.
    fn property(&self, cf: CfId, name: &str) -> Option<String>;

    /// Sequence number (LSM), transaction id (B+ tree) or xid (pgheap) of the
    /// last commit.
    fn latest_sequence(&self) -> u64;

    /// Writes an openable copy of the database into `dir`, which must not
    /// exist; its parent must. The copy holds every write committed before
    /// the call started and is a consistent state: the writes of some prefix
    /// of the commit history, none in part. It is built in `dir` with `.tmp`
    /// appended and renamed into place, so a crash never leaves a partial
    /// copy under `dir`. Writers and readers keep running meanwhile.
    fn checkpoint(&self, dir: &Path) -> Result<()>;

    /// Estimates the bytes each `[start, limit)` range of `cf` takes on disk.
    /// Both ends are keys; a range with `start >= limit` is 0, and so is a
    /// range that holds no key. The LSM counts table files only, as RocksDB's
    /// `GetApproximateSizes` does by default, so data still in memtables adds
    /// nothing; the B+ tree and pgheap estimate from one root-to-leaf descent
    /// per end.
    fn approximate_sizes(&self, cf: CfId, ranges: &[(&[u8], &[u8])]) -> Result<Vec<u64>>;

    /// Syncs, stops background work and releases the directory lock. Later
    /// calls fail with `Error::Closed`; a second `close` returns `Ok`.
    fn close(&self) -> Result<()>;

    /// Tests only: stops the engine without a clean shutdown and damages
    /// unsynced data as `mode` says, as a power loss would. The engine
    /// behaves as closed afterwards. Needs `Options::track_unsynced_writes`.
    fn crash(&self, mode: CrashMode) -> Result<()>;
}
