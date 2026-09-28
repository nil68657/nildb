//! NilDB's copy-on-write B+ tree storage engine: fixed-size pages in one
//! file, two alternating meta pages, snapshots that pin a committed root,
//! overflow chains for large values and one root per column family. On-disk
//! formats and design: docs/design/rust-engines.md.

mod cursor;
mod db;
mod page;
mod pager;
mod tree;

pub use db::{BTreeEngine, DATA_FILE, MAX_VALUE_LEN, data_path, open};

use std::path::Path;

use nilengine_api::{Engine, Options, Result};

/// `open` returning a trait object, the shape the C interface and the
/// conformance suite use.
pub fn open_boxed(dir: &Path, cfs: &[&str], opts: &Options) -> Result<Box<dyn Engine>> {
    Ok(Box::new(open(dir, cfs, opts)?))
}

/// Longest key a database with this page size accepts.
pub fn max_key_len(page_size: usize) -> usize {
    page::max_key_len(page_size)
}
