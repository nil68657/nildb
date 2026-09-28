//! Runs the shared conformance suite against the pgheap engine.

use nilengine_conformance::{Harness, conformance_tests};

fn harness() -> Harness {
    Harness::new("pgheap", nilengine_pgheap::open_boxed)
}

conformance_tests!(harness());

/// The same suite with a 32-frame (256 KiB) buffer pool, so pages are
/// evicted and rewritten all the time, and a checkpoint every 64 KiB of WAL,
/// so most crashes land shortly after a checkpoint and replay pages from
/// their full-page images.
mod small_pool {
    use nilengine_api::Options;
    use nilengine_conformance::{Harness, conformance_tests};

    fn harness() -> Harness {
        let mut h = Harness::new("pgheap-small-pool", nilengine_pgheap::open_boxed);
        h.options = Options {
            cache_bytes: 32 * 8192,
            checkpoint_wal_bytes: 64 << 10,
            ..Options::small()
        };
        h
    }

    conformance_tests!(harness());
}
