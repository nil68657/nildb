//! NilDB's PostgreSQL-style heap storage engine. Each column family is an
//! MVCC heap of 8 KiB pages with a B-tree index and a TOAST relation; a
//! write-ahead log with full-page images, checkpoints and redo recovery
//! makes commits durable; a commit log and snapshots decide visibility; HOT
//! updates, pruning and VACUUM reclaim dead versions. Design, on-disk
//! formats and the differences from PostgreSQL: docs/design/rust-engines.md.

mod btree;
mod bufmgr;
mod control;
mod db;
mod file;
mod fsm;
mod heap;
mod iter;
mod page;
mod redo;
mod vacuum;
mod vm;
mod wal;
mod xact;

pub use btree::MAX_KEY_LEN;
pub use db::{MAX_VALUE_LEN, PgHeapEngine, TOAST_CHUNK, TOAST_THRESHOLD, open};

use std::path::Path;

use nilengine_api::{Engine, Options, Result};

/// `open` returning a trait object, the shape the C interface and the
/// conformance suite use.
pub fn open_boxed(dir: &Path, cfs: &[&str], opts: &Options) -> Result<Box<dyn Engine>> {
    Ok(Box::new(open(dir, cfs, opts)?))
}
