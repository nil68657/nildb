//! Runs the shared conformance suite against the LSM engine.

use nilengine_conformance::{Harness, conformance_tests};

fn harness() -> Harness {
    Harness::new("lsm", nilengine_lsm::open_boxed)
}

conformance_tests!(harness());

/// The same suite with 256-byte blocks, a restart point every two keys, no
/// bloom filter and an 8 KiB memtable: many blocks per file, many files and
/// constant compaction.
mod tiny_blocks {
    use nilengine_api::Options;
    use nilengine_conformance::{Harness, conformance_tests};

    fn harness() -> Harness {
        let mut h = Harness::new("lsm-tiny", nilengine_lsm::open_boxed);
        h.options = Options {
            block_size: 256,
            block_restart_interval: 2,
            bloom_bits_per_key: 0,
            write_buffer_size: 8 << 10,
            target_file_size: 16 << 10,
            max_bytes_for_level_base: 32 << 10,
            ..Options::small()
        };
        h
    }

    conformance_tests!(harness());
}
