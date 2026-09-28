//! The storage engine API of NilDB. The LSM tree (`nilengine-lsm`), the
//! copy-on-write B+ tree (`nilengine-btree`) and the PostgreSQL-style heap
//! (`nilengine-pgheap`) implement [`Engine`]; the C interface
//! (`nilengine-capi`) exposes it to the Go server, and
//! `nilengine-conformance` checks every engine against one model.

pub mod batch;
pub mod cache;
pub mod coding;
pub mod engine;
pub mod error;
pub mod options;

pub use batch::{BatchOp, CfId, WriteBatch, i64add};
pub use engine::{CrashMode, DbIterator, Engine, EngineKind, Snapshot};
pub use error::{Error, IoContext, Result};
pub use options::{IterOptions, Options, WriteOptions};

/// Version of the engine library, reported through `nil_version()`.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
