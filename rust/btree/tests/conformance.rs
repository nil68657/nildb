//! Runs the shared conformance suite against the B+ tree engine.

use nilengine_conformance::{Harness, conformance_tests};

fn harness() -> Harness {
    Harness::new("btree", nilengine_btree::open_boxed)
}

conformance_tests!(harness());

/// The same suite with 512-byte pages: trees four or five levels deep, keys
/// limited to 85 bytes and most values in overflow chains.
mod small_pages {
    use nilengine_api::Options;
    use nilengine_conformance::{Harness, conformance_tests};

    fn harness() -> Harness {
        let mut h = Harness::new("btree-512", nilengine_btree::open_boxed);
        h.options = Options {
            page_size: 512,
            max_unsynced_free_pages: 256,
            ..Options::small()
        };
        h
    }

    conformance_tests!(harness());
}
