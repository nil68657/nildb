//! NilDB's LSM-tree storage engine. Writes go to a checksummed write-ahead
//! log and a skip-list memtable; full memtables become level-0 table files;
//! a background thread compacts levels; reads merge memtables and levels.
//! On-disk formats and design: docs/design/rust-engines.md.

mod block;
mod compaction;
mod db;
mod filter;
mod iter;
mod key;
mod log;
mod memtable;
mod rangedel;
mod table;
mod version;

pub use db::{LsmEngine, MAX_KEY_LEN, MAX_VALUE_LEN, open};

use std::path::Path;

use nilengine_api::{Engine, Options, Result};

/// `open` returning a trait object, the shape the C interface and the
/// conformance suite use.
pub fn open_boxed(dir: &Path, cfs: &[&str], opts: &Options) -> Result<Box<dyn Engine>> {
    Ok(Box::new(open(dir, cfs, opts)?))
}
