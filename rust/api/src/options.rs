use crate::engine::Snapshot;
use crate::error::{Error, Result};

/// Options for `open`. Each engine reads the fields it needs and ignores the
/// rest; the doc comment of each field names its engine.
#[derive(Clone, Debug)]
pub struct Options {
    /// Both: create the database and missing column families.
    pub create_if_missing: bool,
    /// All: bytes of the block cache (LSM), page cache (B+ tree) or buffer
    /// pool (pgheap).
    pub cache_bytes: usize,
    /// LSM: memtable size that triggers a switch to a new memtable and log.
    pub write_buffer_size: usize,
    /// LSM: immutable memtables allowed to wait for a flush before writers
    /// stall.
    pub max_immutable_memtables: usize,
    /// LSM: target size of a data block in a table file.
    pub block_size: usize,
    /// LSM: keys between restart points in a data block.
    pub block_restart_interval: usize,
    /// LSM: bloom filter bits per key; 0 disables the filter.
    pub bloom_bits_per_key: usize,
    /// LSM: compaction output files are cut near this size.
    pub target_file_size: u64,
    /// LSM: level-0 file count that triggers a compaction.
    pub level0_compaction_trigger: usize,
    /// LSM: level-0 file count at which writers wait for compaction.
    pub level0_stop_writes_trigger: usize,
    /// LSM: target size of level 1; level n+1 is `level_size_multiplier`
    /// times level n.
    pub max_bytes_for_level_base: u64,
    pub level_size_multiplier: u64,
    /// LSM: number of levels, including level 0.
    pub num_levels: usize,
    /// B+ tree: page size of a new database file (existing files keep theirs).
    pub page_size: usize,
    /// B+ tree: pages freed by unsynced commits before the engine syncs on
    /// its own so they can be reused.
    pub max_unsynced_free_pages: usize,
    /// pgheap: percent of a heap page that inserts may fill; the rest is kept
    /// for updates on the same page (HOT).
    pub fillfactor: usize,
    /// pgheap: WAL bytes written since the last checkpoint that trigger the
    /// next one (PostgreSQL's max_wal_size, simplified; its default is
    /// 1 GiB, this one 256 MiB, which bounds crash recovery to seconds).
    pub checkpoint_wal_bytes: u64,
    /// pgheap: run the autovacuum thread.
    pub autovacuum: bool,
    /// pgheap: autovacuum vacuums a column family once its dead tuples pass
    /// `autovacuum_threshold + autovacuum_scale_percent% * live tuples`.
    pub autovacuum_threshold: u64,
    pub autovacuum_scale_percent: u64,
    /// pgheap: pause between autovacuum rounds.
    pub autovacuum_naptime_ms: u64,
    /// Tests only: record unsynced writes so `Engine::crash` can drop them.
    pub track_unsynced_writes: bool,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            create_if_missing: true,
            cache_bytes: 64 << 20,
            write_buffer_size: 64 << 20,
            max_immutable_memtables: 2,
            block_size: 4096,
            block_restart_interval: 16,
            bloom_bits_per_key: 10,
            target_file_size: 32 << 20,
            level0_compaction_trigger: 4,
            level0_stop_writes_trigger: 20,
            max_bytes_for_level_base: 256 << 20,
            level_size_multiplier: 10,
            num_levels: 7,
            page_size: 4096,
            max_unsynced_free_pages: 16384,
            fillfactor: 90,
            checkpoint_wal_bytes: 256 << 20,
            autovacuum: true,
            autovacuum_threshold: 50,
            autovacuum_scale_percent: 20,
            autovacuum_naptime_ms: 1000,
            track_unsynced_writes: false,
        }
    }
}

impl Options {
    /// Small sizes that make the LSM switch memtables, flush and compact
    /// after a few hundred kilobytes. The conformance suite uses them.
    pub fn small() -> Options {
        Options {
            cache_bytes: 1 << 20,
            write_buffer_size: 32 << 10,
            block_size: 1024,
            target_file_size: 64 << 10,
            level0_compaction_trigger: 2,
            level0_stop_writes_trigger: 8,
            max_bytes_for_level_base: 128 << 10,
            level_size_multiplier: 4,
            num_levels: 5,
            max_unsynced_free_pages: 512,
            checkpoint_wal_bytes: 1 << 20,
            autovacuum_naptime_ms: 50,
            ..Options::default()
        }
    }

    /// Applies a `name=value` list separated by `;`, `,` or whitespace, as the
    /// C interface receives it. Sizes accept a `k`, `m` or `g` suffix
    /// (powers of 1024); booleans accept `true`, `false`, `1` and `0`.
    pub fn parse(spec: &str) -> Result<Options> {
        let mut o = Options::default();
        o.apply(spec)?;
        Ok(o)
    }

    pub fn apply(&mut self, spec: &str) -> Result<()> {
        for item in spec
            .split(|c: char| c == ';' || c == ',' || c.is_whitespace())
            .filter(|s| !s.is_empty())
        {
            let (name, value) = item
                .split_once('=')
                .ok_or_else(|| Error::invalid(format!("option {item:?} has no '='")))?;
            self.set(name.trim(), value.trim())?;
        }
        Ok(())
    }

    pub fn set(&mut self, name: &str, value: &str) -> Result<()> {
        let size = || parse_size(name, value);
        match name {
            "create_if_missing" => self.create_if_missing = parse_bool(name, value)?,
            "cache_bytes" => self.cache_bytes = size()? as usize,
            "write_buffer_size" => self.write_buffer_size = size()?.max(4096) as usize,
            "max_immutable_memtables" => self.max_immutable_memtables = size()?.max(1) as usize,
            "block_size" => self.block_size = size()?.max(64) as usize,
            "block_restart_interval" => self.block_restart_interval = size()?.max(1) as usize,
            "bloom_bits_per_key" => self.bloom_bits_per_key = size()? as usize,
            "target_file_size" => self.target_file_size = size()?.max(4096),
            "level0_compaction_trigger" => self.level0_compaction_trigger = size()?.max(1) as usize,
            "level0_stop_writes_trigger" => {
                self.level0_stop_writes_trigger = size()?.max(2) as usize
            }
            "max_bytes_for_level_base" => self.max_bytes_for_level_base = size()?.max(4096),
            "level_size_multiplier" => self.level_size_multiplier = size()?.max(2),
            "num_levels" => self.num_levels = size()?.clamp(2, 12) as usize,
            "page_size" => {
                let p = size()?;
                if !(512..=65536).contains(&p) || !p.is_power_of_two() {
                    return Err(Error::invalid(format!(
                        "page_size {p}: want a power of two from 512 to 65536"
                    )));
                }
                self.page_size = p as usize;
            }
            "max_unsynced_free_pages" => self.max_unsynced_free_pages = size()?.max(1) as usize,
            "fillfactor" => self.fillfactor = size()?.clamp(10, 100) as usize,
            "checkpoint_wal_bytes" => self.checkpoint_wal_bytes = size()?.max(64 << 10),
            "autovacuum" => self.autovacuum = parse_bool(name, value)?,
            "autovacuum_threshold" => self.autovacuum_threshold = size()?,
            "autovacuum_scale_percent" => self.autovacuum_scale_percent = size()?,
            "autovacuum_naptime_ms" => self.autovacuum_naptime_ms = size()?.max(1),
            "track_unsynced_writes" => self.track_unsynced_writes = parse_bool(name, value)?,
            _ => return Err(Error::invalid(format!("unknown option {name:?}"))),
        }
        Ok(())
    }
}

fn parse_bool(name: &str, value: &str) -> Result<bool> {
    match value {
        "true" | "1" => Ok(true),
        "false" | "0" => Ok(false),
        _ => Err(Error::invalid(format!(
            "option {name}: {value:?} is not a boolean"
        ))),
    }
}

fn parse_size(name: &str, value: &str) -> Result<u64> {
    let (digits, shift) = match value.as_bytes().last() {
        Some(b'k' | b'K') => (&value[..value.len() - 1], 10),
        Some(b'm' | b'M') => (&value[..value.len() - 1], 20),
        Some(b'g' | b'G') => (&value[..value.len() - 1], 30),
        _ => (value, 0),
    };
    let n: u64 = digits
        .parse()
        .map_err(|_| Error::invalid(format!("option {name}: {value:?} is not a size")))?;
    n.checked_mul(1 << shift)
        .ok_or_else(|| Error::invalid(format!("option {name}: {value:?} overflows")))
}

/// Options for one write.
#[derive(Clone, Copy, Debug, Default)]
pub struct WriteOptions {
    /// Make the batch durable before the write returns: an fsync of the log
    /// (LSM) or of the data file (B+ tree).
    pub sync: bool,
}

/// Options for one iterator. Bounds are copied into the iterator, which reads
/// only keys in `[lower_bound, upper_bound)`; `None` is an open bound.
#[derive(Clone)]
pub struct IterOptions {
    pub snapshot: Option<Snapshot>,
    pub lower_bound: Option<Vec<u8>>,
    pub upper_bound: Option<Vec<u8>>,
    /// Insert blocks or pages read by this iterator into the cache.
    pub fill_cache: bool,
}

impl Default for IterOptions {
    fn default() -> Self {
        IterOptions {
            snapshot: None,
            lower_bound: None,
            upper_bound: None,
            fill_cache: true,
        }
    }
}

impl IterOptions {
    pub fn bounds(lower: Option<&[u8]>, upper: Option<&[u8]>) -> Self {
        IterOptions {
            lower_bound: lower.map(<[u8]>::to_vec),
            upper_bound: upper.map(<[u8]>::to_vec),
            ..Self::default()
        }
    }

    pub fn with_snapshot(mut self, snap: Option<&Snapshot>) -> Self {
        self.snapshot = snap.cloned();
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_sizes_and_bools() {
        let o = Options::parse(
            "write_buffer_size=4m; cache_bytes=512k,page_size=8192 create_if_missing=false",
        )
        .unwrap();
        assert_eq!(o.write_buffer_size, 4 << 20);
        assert_eq!(o.cache_bytes, 512 << 10);
        assert_eq!(o.page_size, 8192);
        assert!(!o.create_if_missing);
        assert!(Options::parse("page_size=1000").is_err());
        assert!(Options::parse("nope=1").is_err());
        assert!(Options::parse("cache_bytes").is_err());
        assert!(Options::parse("").is_ok());
    }
}
