# NilDB storage engines in Rust

Status: phase 1 done on 2026-09-27. Two engines, an LSM tree and a copy-on-write B+ tree, pass one conformance suite, are callable from C and from Go, and have a first benchmark against RocksDB 11.8.1. A third engine, pgheap, modelled on PostgreSQL's heap storage, was added on 2026-09-28 and passes the same suite (section "pgheap engine"). Nothing in the server uses them yet; phase 2 puts them behind `internal/store` (last section).

Everything lives in `rust/` (a cargo workspace), `rust/include/nilengine.h` and `internal/nilengine`.

| Crate | Directory | Contents |
|---|---|---|
| `nilengine-api` | `rust/api` | `Engine` trait, `WriteBatch`, options, errors, varints, CRC-32, a sharded LRU cache |
| `nilengine-conformance` | `rust/conformance` | the suite every engine runs, a `BTreeMap` model, seeded workloads |
| `nilengine-lsm` | `rust/lsm` | LSM engine |
| `nilengine-btree` | `rust/btree` | B+ tree engine |
| `nilengine-pgheap` | `rust/pgheap` | PostgreSQL-style heap engine: MVCC heap, B-tree index, WAL, VACUUM |
| `nilengine-capi` | `rust/capi` | C interface, built as `rust/target/release/libnilengine.a` |
| `nilengine-bench` | `rust/bench` | `nilbench`, the Rust-side benchmark |

The only third-party crate is `crc32fast` (plus its `cfg-if`); pgheap added none. Tests use their own temp-dir helper and a SplitMix64 generator, so neither `tempfile` nor `rand` is needed. `Cargo.lock` is committed.

## Engine API

`internal/store` is the only package that talks to RocksDB. Reading it gives the operations an engine must provide, and the `Engine` trait (`rust/api/src/engine.rs`) covers them:

| `internal/store` uses | `Engine` |
|---|---|
| `OpenDbColumnFamilies` with eight named column families | `open(dir, cf_names, options)`; `CfId` n is the n-th name; missing families are created |
| `WriteBatch` across families: `PutCF`, `DeleteCF`, `DeleteRangeCF`, `MergeCF` (`nildb.i64add` on `default`) | `write(&WriteBatch, WriteOptions { sync })`, applied atomically and in order |
| `GetPinnedCF`, `BatchedMultiGetCF` | `get`, `multi_get` (one view for all keys) |
| `NewSnapshot` / `ReleaseSnapshot`, sequence numbers | `snapshot()` returns a `Snapshot` with `seq()`; dropping it releases it |
| `NewIteratorCF` with `iterate_lower_bound` / `iterate_upper_bound`, `Seek`, `SeekForPrev`, `SeekToFirst`, `SeekToLast`, `Next`, `Prev` | `iter(cf, IterOptions)` returning a `DbIterator` with the same calls and RocksDB's bound rules |
| `FlushCF`, `FlushWAL(sync)`, `CompactRangeCFOpt` | `flush`, `flush_wal(sync)`, `compact_range(cf, start, end)` |
| `GetPropertyCF`, `GetIntPropertyCF`, `GetLatestSequenceNumber` | `property(cf, name)`, `latest_sequence()` |

An iterator keeps the state it opened on readable even after its snapshot is released, as a RocksDB iterator does. `crash(mode)` exists for tests: it stops the engine without a clean shutdown and damages unsynced data as a power loss would (`DropUnsynced`, or `PartialUnsynced { seed }`, which keeps, drops or tears each unsynced write).

A `WriteBatch` is encoded as `[tag u8][cf varint32][key][value]` per operation, key and value varint32-length-prefixed, with no value for a delete. Tags: 1 put, 2 delete, 3 delete-range (value is the end key), 4 merge. The LSM writes this encoding into its log unchanged.

Merge is the one operator `internal/store` registers, `i64add`: 8-byte little-endian signed operand, missing value counts as 0, two's-complement wraparound. Both engines resolve it at commit, under the writer lock, by reading the current value (from earlier operations of the same batch, then the database) and storing a plain put. RocksDB instead stores operands and fails on read when a stored value has the wrong length; here the write fails. `internal/store` only ever writes 8-byte operands, so the difference does not show.

Options are a struct in Rust and a `name=value;...` string through C (`write_buffer_size=64m;cache_bytes=256m`). Sizes accept k, m and g suffixes.

## LSM engine

Design sources: LevelDB (`doc/table_format.md`, `db/dbformat.h`, `db/skiplist.h`, `db/version_set.cc`, `db/db_iter.cc`, `util/bloom.cc`) and RocksDB's range-tombstone fragmenting. The code is new Rust; nothing is copied.

**Write path.** One writer at a time takes the writer mutex, assigns consecutive sequence numbers to the batch's operations (one number per operation), appends one log record with a single `write` call, syncs the log if asked, inserts the operations into the memtable, then publishes the last sequence number. Readers use the published number, so they see all of a batch or none of it. A failed log write poisons the engine, because later records behind a partial record would be lost on replay.

**Memtable.** A skip list (Pugh 1990) of internal keys whose user part is `column family id (4 bytes big-endian) ++ user key`, so one list holds all families. Nodes live in an arena of 256 KiB chunks; readers walk the list without locks and inserts publish nodes with release stores (LevelDB's scheme). When the memtable reaches `write_buffer_size` (64 MiB default), the writer syncs the log, opens a new log, and moves the memtable to the immutable list. Writers stall when two immutable memtables wait for a flush or level 0 holds `level0_stop_writes_trigger` files (20).

**Reads.** A read takes the current `SuperVersion` (memtable, immutable memtables, version) under a read lock and holds it, which keeps every memtable and table file it needs alive. A point read searches the memtable, the immutable memtables, level-0 files newest first, then one file per deeper level (bloom filter first), and stops at the first version with a sequence at or below the read's. The user iterator merges every source with a linear scan over at most a dozen children and applies snapshot visibility, deletes and bounds, following LevelDB's `DBIter` in both directions.

**Range deletes.** A delete-range at sequence s hides entries of `[start, end)` below s. Tombstones sit in the memtable, move into the manifest when their memtable flushes, and leave it once no table file overlapping the range holds an entry older than the tombstone. Reads split the tombstones of each source into disjoint fragments with descending sequence lists (RocksDB's `FragmentedRangeTombstoneList` idea) and binary-search them. Delete-range costs O(1) at write time.

**Flush and compaction.** A flush thread writes the oldest immutable memtable into one level-0 table per column family and records the files, the tombstones and the new log number in one manifest edit; logs below that number are then deleted. A compaction thread picks the level with the highest score: level-0 file count over `level0_compaction_trigger` (4), or level bytes over `max_bytes_for_level_base * level_size_multiplier^(n-1)` (256 MiB, times 10). Level 0 compacts all its files into level 1, so an older overlapping level-0 file can never sit above newer level-1 data. Deeper levels compact one file at a time, round-robin (LevelDB's compaction pointer); a single file with nothing below moves without a rewrite. Output files are cut near `target_file_size` (32 MiB) and only between user keys, so a key never spans two files of a level.

The merge drops what no reader can see. With the live snapshots sorted, the stripe of a sequence number is the index of the first snapshot at or above it. An older version in the same stripe as a newer version of the same key goes; so does an entry covered by a range tombstone in its stripe; a point tombstone in stripe 0 goes when no deeper level holds the key. `compact_range` flushes, compacts every overlapping file down to the deepest level with data, then compacts that level into itself so tombstones go.

**Snapshots** are sequence numbers in a counted multiset. Implicit reads and iterators do not register one: holding the `SuperVersion` keeps the older files they read.

**Recovery.** Open reads `CURRENT`, replays the manifest, and replays every log at or above the manifest's log number into a fresh memtable. Replay stops at the first short, zero-length or checksum-failing record and ignores every later log, the point-in-time mode `internal/store` configures for RocksDB. The recovered memtable is written to level 0, a new manifest holding one snapshot edit replaces the old one, a new log starts, and files that no version references are deleted. Every open therefore leaves the recovered state durable.

### LSM on-disk formats

Directory: `CURRENT`, `MANIFEST-NNNNNN`, `NNNNNN.log`, `NNNNNN.sst`, `LOCK` (an `flock`ed file that stops a second open). File numbers come from one counter.

| Structure | Layout |
|---|---|
| log record (log and manifest) | `[length u32 LE][CRC-32 of the length bytes and payload, u32 LE][payload]` |
| log payload | `[first sequence u64 LE][WriteBatch encoding]` |
| internal key | `user key ++ ((sequence << 8) | kind) as u64 LE`; kind 0 delete, 1 put; sorted by user key, then trailer descending |
| table file | data blocks, filter block, index block, each followed by a CRC-32 (u32 LE); then a 56-byte footer |
| data block | LevelDB format: `[shared varint32][unshared varint32][value length varint32][key suffix][value]` per entry, a full key every `block_restart_interval` (16) entries, then the restart offsets (u32 LE each) and their count (u32 LE); target size 4 KiB |
| index block | same format, restart every entry; key = last internal key of a data block, value = `offset varint64, length varint64` |
| filter block | one bloom filter per file over distinct user keys (MurmurHash64A, double hashing, 10 bits per key, 6 probes); last byte = probe count |
| footer | filter offset, filter length, index offset, index length, entry count (five u64 LE), format version u32 (1), CRC-32 of the first 44 bytes, magic u64 `0x4e494c5353540001` |
| manifest edit | varint-tagged fields: 1 log number, 2 next file number, 3 last sequence, 4 new column family (id, name), 5 deleted file (cf, level, number), 6 new file (cf, level, number, size, smallest and largest internal key, sequence range, entries, deletions), 7 new range tombstone (cf, sequence, start, end), 8 deleted range tombstone (cf, sequence) |
| `CURRENT` | `MANIFEST-NNNNNN\n`, replaced by write, sync, rename, directory sync |

Every table file is synced before the manifest names it and the manifest is synced after every edit, so only the log tail can hold unsynced data. `sync` uses `File::sync_data`, which is `F_FULLFSYNC` on macOS, the same guarantee Homebrew's RocksDB gives.

Limits: keys up to 16 MiB, values up to 1 GiB, no compression, one flush and one compaction thread, all table files open with index and filter blocks in memory.

## B+ tree engine

Design sources: LMDB (Howard Chu, "MDB: A Memory-Mapped Database and Backend for OpenLDAP", LDAPCon 2011) for copy-on-write pages, two meta pages and reader-gated page reuse; bbolt's `NoFreelistSync` mode for rebuilding the free list at open; btrfs (Rodeh, Bacik and Mason, ACM TOS 2013) for writer ids in child pointers. No code is copied.

One file, `data.nbt`, of fixed-size pages (4 KiB default, chosen when the file is created). Pages 0 and 1 are the meta pages; each column family is a separate tree whose root the meta page records.

**Commits.** One writer at a time copies every page it changes to a free page, starting from the committed roots, so a commit never overwrites a page any committed tree uses. The transaction edits parsed nodes in memory, splits a node that no longer fits at the point that balances the halves' bytes (branch separators are shortened to the shortest key between the halves), merges a child that fell below a quarter page with a neighbour when both fit, and frees whole subtrees that a delete-range covers without copying them. It then writes its pages and, last, a meta page naming the new roots. The meta goes into the slot that does not hold the last durable meta. A synced commit syncs before and after the meta write, LMDB's order; an unsynced commit syncs nothing.

**Free pages and snapshots.** A snapshot is an `Arc` of the committed roots and transaction id; iterators and point reads hold the same `Arc`. A page freed by transaction F was last visible in tree F-1, so it becomes reusable once every live reader is at F or later and the last durable commit is at F or later. The second condition keeps the last durable tree intact after a power loss that keeps only some unsynced writes; to bound the file growth it causes, the engine syncs on its own once `max_unsynced_free_pages` (16384) pages wait for reuse.

**Recovery.** Open reads both meta pages and walks the tree of the newer one straight from the file, checking every page's checksum, page number and the writer id its parent recorded. A page that never reached the disk fails the checksum; an older page left at the same number fails the writer-id check. If the walk fails, open uses the older meta, which the reuse rule kept intact. The walk also marks reachable pages; every other page below the high-water mark is free. Open then syncs, so an adopted unsynced commit becomes durable before anything builds on it. The walk reads the whole file, which is the cost of not persisting a free list.

**Reads and cursors.** A point read descends from the root through the page cache (a sharded LRU, 64 MiB default), binary-searching slotted pages. A cursor keeps the root-to-leaf path as a stack of `(page, index)` and moves between leaves through the parents, as LMDB's cursors do, since a copy-on-write tree cannot keep sibling pointers current. Values larger than a quarter page minus the key go to overflow chains, read lazily when the iterator asks for the value.

### B+ tree on-disk formats

| Structure | Layout |
|---|---|
| page header, 40 bytes | CRC-32 of bytes 4..page end (u32), type u8 (1 meta, 2 branch, 3 leaf, 4 overflow), flags u8, cell count u16, writer transaction id u64, own page number u64, aux1 u64, aux2 u64 |
| leaf page | header, u16 slot offsets in key order, cells packed from the page end |
| leaf cell | `[flags u8][key length u16][value length u32][key][value]`; with flag bit 0 the value is `[overflow page u64][overflow txn u64]` and the length is the full value length |
| branch page | header with the leftmost child in aux1 (page) and aux2 (writer txn), slots, cells `[key length u16][child page u64][child txn u64][key]`; a cell's child holds keys at or above its key and below the next cell's |
| overflow page | header with the next page in aux1 (0 ends the chain) and the byte count in aux2, then the bytes |
| meta page | header (txn = the commit it describes), then magic `NILBPTRE`, format version u32 (1), page size u32, page count u64, family count u32, and per family `name length u8, name, root page u64, root txn u64, key count u64` |

Limits: keys up to 981 bytes with 4 KiB pages (85 with 512-byte pages); values up to 1 GiB; one writer; the file never shrinks; open walks the whole file; at most as many column families as fit in one meta page (122 with 8-byte names at 4 KiB).

## pgheap engine

Design sources: PostgreSQL's documentation (storage-page-layout.html, mvcc.html, routine-vacuuming.html) and source tree (`src/backend/access/heap/README.HOT`, `src/backend/access/nbtree/README`, `src/backend/access/transam/README`, `src/include/access/heaptoast.h` for the TOAST constants, `src/backend/postmaster/walwriter.c` for the WAL writer's delay). PostgreSQL is under the PostgreSQL License; pgheap copies none of its code.

pgheap stores each column family the way PostgreSQL stores a table with one index: a heap of 8 KiB pages holding every version of every key, a B-tree from key to heap tuple, and a TOAST heap with its own index for values too large to keep inline. An update writes a new version; a reader picks the version its snapshot sees from the versions' xmin and xmax and the commit log; pruning and VACUUM remove versions no snapshot can see.

### Files

| File | Contents |
|---|---|
| `pg_control` | two 512-byte slots, each: magic `NILPGCTL`, version u32, sequence u64, state u8 (1 shut down, 2 running), checkpoint LSN, redo LSN, next xid, last committed xid (u64 each), CRC-32. A write goes to the slot the last write did not use and is synced; open takes the valid slot with the higher sequence. |
| `catalog` | column family names in creation order, CRC-32 at the end, replaced by write, sync, rename, directory sync. Family n owns relations 16 + 4n (heap), 17 + 4n (index), 18 + 4n (TOAST heap) and 19 + 4n (TOAST index). |
| `base/<rel>`, `base/<rel>_fsm`, `base/<rel>_vm` | a relation's pages, its free-space map and its visibility map (PostgreSQL's forks) |
| `pg_xact/0000` | the commit log, relation 1 |
| `pg_wal/<16 hex digits>` | WAL segments of 16 MiB, named by segment number |
| `pg_stat` | counters saved at a clean close; open reads and deletes it, so a crash starts the counters over, as PostgreSQL's statistics do |
| `LOCK` | an `flock`ed file that stops a second open |

### Page, tuple and index layouts

All integers are little-endian unless marked big-endian.

| Structure | Layout |
|---|---|
| page header, 32 bytes | page LSN u64, checksum u32 (CRC-32 of the page with this field zeroed, seeded with the block number), flags u16, pd_lower u16, pd_upper u16, pd_special u16, page kind u8, layout version u8, unused u16, pd_prune_xid u64. PostgreSQL's header is 24 bytes; pd_prune_xid needs 8 bytes here because xids are 64-bit. |
| line pointer, 4 bytes | PostgreSQL's ItemIdData: lp_off in bits 0-14, lp_flags in bits 15-16 (0 unused, 1 normal, 2 redirect, 3 dead), lp_len in bits 17-31. Line pointers grow forward from the header, tuples backward from pd_special. At most 226 per heap page. |
| heap tuple, 32-byte header | xmin u64, xmax u64, t_ctid (block u32, offset u16), t_infomask2 u16 (0x4000 HEAP_HOT_UPDATED, 0x8000 HEAP_ONLY_TUPLE), t_infomask u16 (0x0004 HEAP_HASEXTERNAL, 0x0100 XMIN_COMMITTED, 0x0200 XMIN_INVALID, 0x0400 XMAX_COMMITTED, 0x0800 XMAX_INVALID, 0x2000 HEAP_UPDATED), key length u16, value length u32, then the key and the value |
| TOAST pointer, 16 bytes | value id u64, value length u32, chunk count u32 |
| TOAST chunk tuple | a heap tuple whose key is the value id (u64 big-endian) and chunk number (u32 big-endian) and whose value is up to 1992 bytes |
| B-tree page | slotted page with 16 bytes of special space: left sibling u32, right sibling u32, level u32 (0 for leaves), flags u16, unused u16. Item 1 of a page with a right sibling is its high key. |
| B-tree items | leaf: tid block u32, tid offset u16, key length u16, key; internal: child block u32, then the separator in leaf format. The first data item of an internal page counts as minus infinity. |
| B-tree meta page (block 0) | magic `NBTR` u32, version u32, root block u32, root level u32 |
| commit-log page | two bits per xid (0 in progress, 1 committed, 2 aborted) after the header: 32,640 xids per page |
| free-space map page | one byte per heap page, its free space in 32-byte steps: 8,160 heap pages per map page |
| visibility-map page | one bit per heap page, set when every tuple on it is visible to every snapshot: 65,280 heap pages per map page |

### Transactions, the commit log and snapshots

Each write batch is one transaction. Its xid is assigned at its first change, so a batch that changes nothing commits nothing; `latest_sequence` and snapshot sequence numbers report the last committed xid. Xids are 64-bit counters that start at 3 and are never reused, so they never wrap around and tuples never need freezing. The cost is that the commit log is never truncated: two bits per xid is 256 MB per billion transactions. PostgreSQL truncates its log below the oldest unfrozen xid; doing that here would need a freeze horizon first.

One engine-level writer lock runs batches one at a time, so at most one xid is ever in progress. NilDB's server already locks the keys each command touches, so the lock gives up little parallelism the server was using. Readers never take it.

A commit sets the transaction's commit-log bits under the page's exclusive lock, logs a commit record for that page, hands the WAL to the operating system (or syncs it for a synced write), and only then removes the xid from the running set. A snapshot records the next xid (`xmax`), the running xid if any, and the oldest xid it needs (`xmin`). It sees a version when:

- the version's xmin committed, is below the snapshot's `xmax`, and was not the running xid when the snapshot was taken; and
- its xmax is unset, aborted, was the running xid at snapshot time, or is at or above the snapshot's `xmax`.

The writer sees committed versions plus its own changes. An xid below `xmax` that the commit log still marks in progress belonged to a transaction that crashed, and counts as aborted.

Readers set hint bits (XMIN_COMMITTED, XMIN_INVALID, XMAX_COMMITTED, XMAX_INVALID) after a commit-log lookup, but only when the page's exclusive lock is free at that moment, since hints are optional. A committed hint is set only once the transaction's commit record is flushed, the rule PostgreSQL's SetHintBits follows: an unsynced commit can be lost in a crash while the hinted page survives. A page that gains hints without having been logged since the last checkpoint's redo pointer first logs a full-page image (PostgreSQL's XLogSaveBufferForHint, which runs when data checksums are on), because a torn write of that page would otherwise fail its checksum with nothing to repair it.

Every snapshot registers its xmin, including the one `get` takes for a single call and the one an iterator holds until it is dropped. The oldest registered xmin is the horizon: a version whose deleting transaction committed below it is dead to every snapshot.

### B-tree index

The index follows nbtree's Lehman and Yao design. Entries are (key, tid) pairs sorted by key and then tid, so every entry is unique, and a key can have one entry per HOT chain. A reader holds one page lock at a time and moves right when its search key is at or above a page's high key, which gets it past a split that happened after it read the parent. A backward scan follows the left link, then moves right until it reaches the page whose right link is the page it came from. A split that appends to the rightmost page leaves the left page 90% full, as PostgreSQL does; any other split balances the bytes. A split, the downlink it adds to the parent and any new root are logged as one record of full page images, so recovery never sees half a split; PostgreSQL logs the split and the parent insert separately and finishes an incomplete split later. Pages are never merged or deleted. Keys may be up to 2,000 bytes, so four entries always fit on a page.

### Writes, HOT and pruning

An insert goes to the relation's current target page if the tuple fits with (100 − fillfactor)% of the page still free, then to a page the free-space map offers, then to a new page at the end. An update puts the new version on the old version's page when it fits, ignoring fillfactor: the new tuple becomes a heap-only tuple, the old one gets HEAP_HOT_UPDATED and a t_ctid pointing at it, and the index gets no new entry. An update that does not fit goes to another page and adds an index entry. A delete sets xmax.

NilDB's default fillfactor is 90; PostgreSQL's heap default is 100. NilDB's Redis workloads overwrite values in place (SET on an existing key, HSET, INCR through `i64add`), and the 10% left free on each page is where their HOT versions go.

When the writer wants a page that is short of space and the page's pd_prune_xid is older than the horizon, it prunes the page first, following `heap_page_prune`: the dead members of each HOT chain become unused line pointers, the chain's root becomes a redirect to its first surviving member (or a dead line pointer when nothing survives), and a dead heap-only tuple that no chain reaches any more, such as the new version of an aborted update, becomes unused. The index entry keeps pointing at the root. A reader walks from the root through the redirect and the t_ctid links, checking that each member's xmin equals the previous member's xmax, which rejects a line pointer that was reused.

Delete-range sets xmax on every live version in the range: PostgreSQL has no range tombstone, so it costs O(keys in range), where the LSM writes one tombstone. A merge (`i64add`) reads the live version and writes an update. A batch whose merge fails is aborted: an abort record marks its xid, and its tuples stay behind, dead, until pruning or VACUUM takes them.

### TOAST

A tuple longer than 2,032 bytes (PostgreSQL's TOAST_TUPLE_THRESHOLD, sized for four tuples per 8 KiB page) moves its value to the column family's TOAST heap in chunks of 1,992 bytes, four chunk tuples per page. PostgreSQL's chunk is 1,996 bytes because its tuple header is smaller. The value id is the xid shifted left 24 bits plus the value's number within the batch, so ids are unique forever. The heap tuple keeps a 16-byte pointer. Updating or deleting the version marks its chunks deleted with the same xid, as `toast_delete_datum` does, and VACUUM of the TOAST heap removes them. Readers fetch chunks without a visibility check (PostgreSQL's SnapshotToast), since a chunk can only die after every snapshot has stopped seeing the version that points at it. Values may be up to 1 GiB. Compression is deferred: PostgreSQL compresses a value with pglz or lz4 before moving it out of line, and pgheap stores it as is.

### WAL, checkpoints and recovery

The WAL is one byte stream addressed by LSN and cut into 16 MiB segments; a record may span two segments. A record is a 32-byte header (total length u32, CRC-32 u32, previous record's LSN u64, xid u64, type u8, info u8, block count u8, unused u8, main-data length u32), a 12-byte reference per page it changes (relation u32, fork u8, flags u8, block u32, unused u16), each followed by a full-page image when flag 1 is set (hole offset u16, hole length u16, the page without the hole between pd_lower and pd_upper), then the main data. Replay stops at the first record that is short, fails its CRC or does not link back to the record before it.

| Record | Pages | Main data |
|---|---|---|
| checkpoint | none | redo LSN, next xid, last committed xid, shutdown flag |
| full-page image for hints | the page (always an image) | none |
| commit, abort | the commit-log page | none; the xid is in the header |
| heap insert | heap page | offset u16, flags u8, tuple |
| heap delete | heap page | offset u16, xmax u64 |
| heap update | new page, old page (one page when both are the same) | old offset u16, new offset u16, xmax u64, new tuple |
| HOT update | the page | same as heap update |
| prune | heap page | counts of redirected, dead and unused line pointers (u16 each), new pd_prune_xid u64, then the offsets |
| vacuum | heap page | count u16 and the offsets made unused |
| B-tree insert | leaf | position u16, item |
| B-tree split, B-tree new root | every page the split changed, as images | none |
| B-tree vacuum | leaf | count u16 and the positions removed |

- WAL before data: the buffer manager writes a dirty page only after the WAL is flushed up to the page's LSN.
- Full-page images: the first change to a page after the redo pointer of the latest checkpoint logs the page's new contents; redo restores the image instead of applying the change. That is what repairs a torn page write.
- Durability: `write` hands every commit record to the operating system before returning, so a process crash keeps it; a synced write syncs the WAL; the WAL writer thread syncs it every 200 ms (PostgreSQL's wal_writer_delay default); a power loss keeps a prefix of the commits.
- Checkpoints: move the redo pointer to the end of the WAL, write every dirty buffer, sync the relation files that were written, log a checkpoint record and sync it, point `pg_control` at it, delete WAL segments that end before the redo pointer. Writers keep running meanwhile. The checkpointer thread starts one after `checkpoint_wal_bytes` of WAL (256 MiB; PostgreSQL's max_wal_size defaults to 1 GiB) or after 300 s with new WAL (checkpoint_timeout); `flush` runs one, and `close` runs a shutdown checkpoint.
- Recovery: a first pass reads the WAL from the redo pointer to find where it ends and to check that the checkpoint record `pg_control` names is there; the segment holding the end is cut there and later segments are deleted, so no stale bytes can follow new records. A second pass replays each record: a page with an image is restored from it; a page without one is changed only if its LSN is below the record's end LSN. The next xid and the last committed xid advance with the replayed records, and a transaction without a commit record counts as aborted. An end-of-recovery checkpoint makes the result durable.
- Crash tests: `crash` halts the WAL so no sync starts after the simulated power loss, stops the background threads, then drops, keeps or tears each unsynced write.

### Buffer manager

The pool holds `cache_bytes` / 8 KiB frames (at least 32). A 64-shard table maps (relation, fork, block) to frames; a frame has a pin count, a usage count capped at 5 for the clock sweep, a dirty flag, a content lock and an I/O lock that keeps two writers of one frame in order. Reads verify the checksum: a torn main-fork page is a corruption error, and a torn free-space or visibility-map page reads as zeros, as PostgreSQL treats those maps.

### VACUUM, the free-space map, the visibility map and autovacuum

`compact_range` runs VACUUM on the whole column family, heap first, then TOAST heap; like PostgreSQL's VACUUM it ignores key ranges. It makes three passes over a heap: prune every page the visibility map does not mark all-visible and collect the dead line pointers pruning leaves; remove the index entries pointing at them; mark those line pointers unused, drop trailing unused ones, record each page's free space in the free-space map and set the visibility-map bit of pages left all-visible. It holds the writer lock for at most 64 heap pages at a time and for the whole index pass. A long-held snapshot keeps VACUUM from removing any version deleted after the snapshot's xmin, which is where bloat comes from (the long-snapshot benchmark below grew the heap tenfold).

The free-space map is a hint and is not logged; an insert that finds a page fuller than the map said records the real figure and looks again. The visibility map is not logged either: open empties it after a crash and the next VACUUM rebuilds it, since its bits only let VACUUM skip work. PostgreSQL logs setting and clearing visibility-map bits.

The autovacuum thread wakes every `autovacuum_naptime_ms` (default 1000; PostgreSQL's autovacuum_naptime is 60 s) and vacuums each heap whose dead tuples exceed `autovacuum_threshold + autovacuum_scale_percent% × live tuples`, with PostgreSQL's defaults of 50 and 20%. Commits keep n_live_tup and n_dead_tup; pruning subtracts what it removes; VACUUM sets n_dead_tup to the dead versions it had to leave and n_live_tup to what it counted when it saw every page.

### Properties

`rocksdb.estimate-num-keys` is n_live_tup; `rocksdb.num-snapshots`, `rocksdb.block-cache-usage` and `rocksdb.block-cache-capacity` mean what they mean elsewhere. pgheap adds, per column family, in the style of pg_stat_user_tables: `nil.pgheap.n-live-tup`, `n-dead-tup`, `n-tup-ins`, `n-tup-upd`, `n-tup-hot-upd`, `n-tup-del`, `vacuum-count`, `autovacuum-count`, `pruned`, `heap-bytes`, `index-bytes`, `toast-bytes`, `live-bytes`, `bloat-pct` (the share of the heap not taken by live tuple bytes), `toast-n-live-tup`, `toast-n-dead-tup`, `index-entries`, `index-depth`, `all-visible-pages`, and engine-wide `wal-bytes`, `wal-disk-bytes`, `checkpoints`, `buffer-hit-ratio`, `next-xid`, `oldest-xmin`, `registered-snapshots`. `nil.stats` prints them as text.

### Differences from PostgreSQL

- 64-bit xids: no wraparound, no freezing, no anti-wraparound VACUUM; the commit log is never truncated.
- One writer at a time, instead of row locks and concurrent writers.
- Pruning runs only when a writer needs space on a page; PostgreSQL also prunes during reads.
- A B-tree split and its parent insert are one WAL record of page images; PostgreSQL logs them separately and repairs incomplete splits. Empty index pages are never deleted.
- The visibility map is not WAL-logged; it is emptied after a crash.
- The WAL has no page headers and records are not 8-byte aligned; each record links to the previous one by LSN, as PostgreSQL's xl_prev does.
- No TOAST compression; chunks are 1,992 bytes.
- VACUUM never truncates the heap: freed pages go to the free-space map and the file does not shrink.
- `pg_control` has two slots instead of relying on atomic 512-byte writes.
- The free-space map is a flat array per relation, not PostgreSQL's tree of maximums.
- Page and tuple headers are 32 bytes (24 and 23 in PostgreSQL) because of 64-bit xids.
- The checkpointer, the WAL writer and autovacuum are the only background threads. There is no background writer, so the thread that evicts a dirty page writes it itself.

### What phase 2 needs from pgheap

- Delete-range is O(n). `internal/store` drops a collection or an index with one delete-range over its id prefix; on pgheap that one batch sets xmax on every row, logs a 54-byte record per row plus a full-page image of each heap page it touches first after a checkpoint, and holds the writer lock throughout. Phase 2 should split large delete-ranges into batches of a few thousand keys, run by the store's janitor, and trigger VACUUM afterwards.
- Snapshots block VACUUM for the whole database. On the LSM an abandoned snapshot only keeps old versions through compactions; on pgheap it stops pruning and VACUUM from removing anything deleted after it, and once pages fill most updates turn non-HOT (10.5% stayed HOT in the long-snapshot benchmark). The store's snapshot registry with leases and the janitor that expires them are required, not optional, and `nil.pgheap.oldest-xmin` with `registered-snapshots` should reach `ROCKS.STATS` so an operator can see what holds the horizon.
- The sweeper replaces RocksDB's compaction filters for every Rust engine. On pgheap each row it deletes becomes a dead tuple, so the sweeper should run VACUUM (`compact_range`) on a family after a large round instead of waiting for autovacuum's threshold.
- `--engine pgheap`, the `ENGINE` marker and `ROCKS.*` map as for the other engines; `ROCKS.COMPACT` runs VACUUM. A checkpoint for `ROCKS.CHECKPOINT` would be a PostgreSQL base backup: a checkpoint, a copy of the relation files, and the WAL from the redo pointer to the end of the copy.

## C interface

`rust/include/nilengine.h` is hand-written; `rust/capi/src/lib.rs` implements it; `nm` shows the 37 exported `nil_*` symbols. The archive needs `-lSystem -lc -lm`, which macOS links by default.

- Handles: `nil_db`, `nil_batch`, `nil_snapshot`, `nil_iter`, all opaque. `nil_open(engine, dir, cf_names, num_cfs, options, errptr)` takes `NIL_ENGINE_LSM` (1), `NIL_ENGINE_BTREE` (2) or `NIL_ENGINE_PGHEAP` (3); column families are numbered by position. pgheap reads the options `fillfactor`, `checkpoint_wal_bytes`, `autovacuum`, `autovacuum_threshold`, `autovacuum_scale_percent` and `autovacuum_naptime_ms`, and sizes its buffer pool from `cache_bytes`. Adding pgheap added no function: there are still 37.
- Errors: every call that can fail takes `char **errptr` and stores a malloc'd `"class: message"` string there (classes `io`, `corruption`, `invalid-argument`, `not-found`, `busy`, `closed`, `unsupported`, `panic`), freeing any string already present. `errptr` may be NULL.
- Memory: values from `nil_get` and `nil_multi_get`, property strings and error strings are malloc'd and freed with `nil_free`. A found empty value is a non-NULL pointer with length 0. `nil_iter_key` and `nil_iter_value` return views valid until the next move.
- Calls: `nil_batch_{new,put,delete,delete_range,merge,count,clear,destroy}`, `nil_write`, `nil_get`, `nil_multi_get`, `nil_snapshot_{new,seq,release}`, `nil_iter_{new,seek_to_first,seek_to_last,seek,seek_for_prev,next,prev,valid,key,value,status,destroy}`, `nil_flush`, `nil_flush_wal`, `nil_compact_range`, `nil_property`, `nil_latest_sequence`, `nil_engine_kind`, `nil_max_key_len`, `nil_version`, `nil_free`, `nil_close`.
- Panics: every exported function runs its body under `catch_unwind`. A panic comes back as a `panic: ...` error, or, in calls without `errptr`, is stored in the batch or iterator and reported by `nil_write` or `nil_iter_status`. A batch that received a NULL pointer with a nonzero length is refused at `nil_write`.

A throwaway C program compiled with `clang -std=c11 -Wall -Wextra -Werror` against the header and the archive opened both engines, wrote, read and iterated.

## Go binding

`internal/nilengine` wraps the C interface with cgo (`#cgo LDFLAGS: ${SRCDIR}/../../rust/target/release/libnilengine.a`). `Kind` is `LSM`, `BTree` or `PgHeap`. Every file except `doc.go` carries `//go:build nilengine`, so without the tag the package is empty and `go test ./...` needs no Rust. The API follows `internal/store`: `Open(kind, dir, cfs, opts)`, `Batch` (`Put`, `Delete`, `DeleteRange`, `Merge`), `Write(b, sync)`, `Get`, `GetAt(snap, ...)`, `MultiGet`, `NewSnapshot`, `NewIterator(cf, IterOptions{Snapshot, Lower, Upper, FillCache})` with `Key`/`Value` views valid until the next move, `Flush`, `FlushWAL`, `CompactRange`, `Property`, `LatestSeq`. Errors are `*nilengine.Error` values that match the sentinels (`ErrBusy`, `ErrClosed`, ...) through `errors.Is`. A `DB` is safe for concurrent use and returns `ErrClosed` after `Close`. `MultiGet` copies keys into C memory because cgo forbids passing Go memory that holds Go pointers.

## Tests

Commands, last run on 2026-09-28 with all three engines:

```
make rust-test         # cargo test --workspace: 224 passed, 6 ignored (the soak runs)
make rust-soak         # the ignored soak tests, NILENGINE_SOAK=24 seeds each
make nilengine-test    # go test -tags nilengine ./internal/nilengine: 6 tests, 12 engine subtests
```

The 224 Rust tests: 9 in `nilengine-api`, 10 LSM, 5 B+ tree and 6 pgheap unit tests, 5 C interface tests, 8 LSM, 9 B+ tree and 10 pgheap internals tests, and the conformance suite, 27 cases run twice per engine (54 per engine, 162 in all). The second LSM run uses 256-byte blocks, a restart point every two keys, no bloom filter and an 8 KiB memtable; the second B+ tree run uses 512-byte pages, which makes trees four or five levels deep and puts most values in overflow chains; the second pgheap run uses a 32-frame (256 KiB) buffer pool, so pages are evicted and rewritten constantly, and a checkpoint every 64 KiB of WAL, so most crashes land soon after a checkpoint and replay pages from their full-page images. Both pgheap runs keep autovacuum on (every 50 ms), and every `compact_range` in the suite runs VACUUM.

The conformance suite (`rust/conformance/src/cases.rs`) checks each engine against a `BTreeMap` per column family:

- randomized batches (puts, deletes, delete-ranges, merges across three families, values up to 7.5 KB), with point reads after every batch, forward and reverse range scans, and iterators driven through random seeks and steps against the model, while flushes, compactions, snapshots and reopens happen at random; four seeds of 1500 batches plus one seed dominated by large values
- snapshot isolation through flushes and compactions, and an iterator that keeps reading after its snapshot is released
- iterator bounds and direction changes on hand-written cases, before and after a flush and a reopen
- delete-range semantics (order within a batch, empty and overlapping ranges, the empty key) and persistence through flush, compaction and reopen
- empty keys and values, keys at the engine's limit, values of 1 MiB and 3 MiB
- reopen after close, a second open of a locked directory, reopening with families reordered or added
- `i64add` merges, including failures that must leave the whole batch unapplied
- a writer moving money between accounts while three readers check the total through snapshots and `multi_get`
- crash recovery: batches with a counter key written in the same batch, some synced, then `crash` with dropped or partially kept unsynced writes; after reopen the counter says how many batches survived, which must include every synced batch, and the full state must equal the model after exactly that many batches. Fifteen crash cycles in one case run crash, reopen, write, crash again. A run of drop-unsynced crashes must lose at least one batch, so a hook that does nothing fails.

The internals tests cover what the shared suite cannot see. For the LSM: data reaches levels below 0, compaction removes range tombstones (and keeps them while a snapshot needs them), a log truncated by 7 bytes recovers 9 of 10 batches, a log with a flipped byte recovers a prefix, open deletes orphan files, and a flipped byte in a table block reads as a corruption error. For the B+ tree: 20,000 random keys in 512-byte pages reach depth 4 or more, deleting three keys in four and large ranges keeps the tree correct, freed pages are reused, a snapshot blocks reuse until released, the free list is rebuilt at open, overflow pages are reused, and four hand-picked crashes: losing only the meta writes, losing only the tree pages under a surviving meta, and a data file cut 1000 bytes short (all three must fall back to the last synced commit), and keeping everything (must keep the newest commit).

For pgheap: 5,000 updates of one key stay HOT and leave one index entry and one heap page, while a snapshot held for 50 of them still reads its version; updates of 1,900-byte values that no longer fit on their page add index entries until VACUUM removes all but one per key; values at the TOAST threshold stay inline and one byte more goes out of line, with chunk-boundary sizes and a 1 MiB value reading back before and after reopen and their chunks removed by VACUUM once replaced; VACUUM after deleting 2,000 rows leaves no dead tuples or index entries and the next 2,000 rows fit in the same pages; a snapshot held across 2,000 updates keeps all 2,000 old versions through VACUUM until it is released; autovacuum starts on its own and brings dead tuples under its threshold; an aborted batch and a flushed but uncommitted transaction whose pages a checkpoint wrote are both invisible after a crash; half of a heap page overwritten on disk after a crash comes back from its full-page image, and the same damage with no image to replay reads as a corruption error; crashes at each of four points inside a checkpoint recover every synced batch and a consistent prefix; and 34,000 transactions spanning two commit-log pages, one in 997 aborted, recover to exactly the model after a crash that tears unsynced writes.

The soak tests (`make rust-soak`) ran 24 extra seeds of the randomized and crash cases under each of the six configurations on 2026-09-28 (B+ tree 57 s, LSM 410 s, pgheap 284 s). All passed. No engine code needed a fix after its first test run; the three failures seen during development were wrong expectations in tests: a fixed 100-byte key over the B+ tree's 85-byte limit at 512-byte pages, a C test expecting a key it had range-deleted, and a Go test expecting the B+ tree's transaction id to stay put when a column family is added.

## Benchmarks

Machine: Apple M3 Max (10 performance and 4 efficiency cores), 36 GiB RAM, macOS 27.2, APFS on the internal SSD. Date: 2026-09-27. Rust 1.98.1, Go 1.27.1, RocksDB 11.8.1 from Homebrew through grocksdb v1.11.1. Two other agents were working on the same machine; one of the three Rust runs measured the LSM's fillseq at 47 s where the other two measured 0.6 s, so the tables give the median of three runs.

Workload: 16-byte keys (decimal digits), 100-byte values, unsynced writes, 256 MiB cache, 64 MiB write buffer, bloom filters at 10 bits per key, no compression anywhere.

### `nilbench` (Rust, no cgo)

`make rust-bench`: 2,000,000 keys; fillseq writes them in key order, 100 per batch; fillrandom writes them in random order, one per commit, into a fresh database; readrandom does 1,000,000 point reads of existing keys; seek+10next does 200,000 seeks each followed by 10 nexts; mixed runs 1,000,000 operations on 4 threads, 80% reads and 20% single-key writes.

| Workload | LSM ops/s | LSM µs/op | B+ tree ops/s | B+ tree µs/op |
|---|---:|---:|---:|---:|
| fillseq, 100 keys per commit | 3,479,762 | 0.29 | 815,629 | 1.23 |
| fillrandom, 1 key per commit | 286,838 | 3.49 | 20,170 | 49.58 |
| readrandom | 338,943 | 2.95 | 477,278 | 2.10 |
| seek+10next | 276,417 | 3.62 | 378,707 | 2.64 |
| mixed, 4 threads | 502,661 | 1.99 | 64,211 | 15.57 |

After fillrandom and mixed, the LSM directory held 238 MB for 232 MB of keys and values: 5 flushes and 1 compaction had left one level-0 file and six level-1 files. The B+ tree file held 437 MB: 106,767 pages, of which 5,608 were free and 10,776 waited for the next sync before reuse.

### Three stores through cgo

`make nilengine-bench` with `NILENGINE_BENCH_KEYS=2000000` and `-benchtime 200000x`: every store performs exactly 200,000 operations per benchmark. Fill benchmarks start from an empty database; the read, seek and mixed benchmarks run on a 2,000,000-key dataset loaded in batches of 1,000 before the timer starts. Mixed runs on 14 goroutines. RocksDB uses an LRU block cache, a bloom filter, no compression and two background jobs, to match the LSM's one flush and one compaction thread. Key formatting with `fmt.Sprintf` and one cgo crossing per operation are in every number.

| Benchmark, ns/op | LSM | B+ tree | RocksDB |
|---|---:|---:|---:|
| FillSeq, 100 keys per commit | 384 | 1,046 | 391 |
| FillRandom, 1 key per commit | 3,548 | 27,545 | 4,114 |
| ReadRandom | 3,866 | 2,648 | 4,317 |
| SeekNext10 | 5,564 | 4,159 | 6,560 |
| Mixed, 14 goroutines | 2,179 | 15,616 | 1,397 |

What the numbers show:

- At these equal, simple settings the LSM matches RocksDB on sequential fills and comes in 10 to 15% faster on random fills, point reads and seeks, through the same cgo path. These are not NilDB's tuned RocksDB settings (HyperClockCache, Ribbon filters, LZ4 and ZSTD), so the result compares the engines, not NilDB's production configuration.
- RocksDB wins the parallel mixed load, 1,397 ns against 2,179 ns per operation. RocksDB groups concurrent writers into one log write and inserts into its memtable concurrently; the LSM runs writers one at a time behind a mutex, one `write` call each.
- The B+ tree reads fastest, 2,648 ns per point read, 39% below RocksDB, because a read is four cached page lookups with no filter probes and no level search.
- The B+ tree writes single keys slowest. Each commit copies the root-to-leaf path: about 5 pages, 20 KiB of writes for a 116-byte record (10.9 million pages over 2.2 million commits in the Rust run). With 100 keys per commit it fills sequentially at 815,000 keys per second.
- One of the three Rust runs measured the LSM's fillseq at 46.98 s; the other two took 0.57 s and 0.54 s. A rerun under macOS `sample` took 0.59 s and showed no stall inside the engine, so the outlier most likely came from other load on the shared machine. The median discards it. The other metrics varied between runs by 2% to 24% (slowest run over fastest); the LSM's Rust mixed load and the B+ tree's Rust fillseq varied most.

### Update workloads, all engines

Machine: the same Apple M3 Max, macOS 27.2. Date: 2026-09-28. Rust 1.98.1, Go 1.27.1, RocksDB 11.8.1 through grocksdb v1.11.1. Every figure is the median of three runs. Leaving out the B+ tree's `compact_range`, a no-op that takes under a microsecond, the slowest run of a configuration took at most 1.33 times as long per operation as the fastest (pgheap with a snapshot held), and 15 of the 21 were within 1.15. Another agent was using the machine through a separate worktree, as during phase 1.

Both workloads load 10,000 keys with 100-byte values, then update random keys one per commit, unsynced, with the default options (256 MiB cache or buffer pool, pgheap's fillfactor 90 and autovacuum on unless marked). Disk sizes include logs.

`nilbench --workload update-hot --hot-keys 10000 --updates 1000000`:

| Engine | µs per update | Updates/s | Disk after | pgheap counters |
|---|---:|---:|---:|---|
| LSM | 2.90 | 345,023 | 117.4 MB | |
| B+ tree | 21.69 | 46,111 | 69.7 MB | |
| pgheap | 5.44 | 183,856 | 7.1 MB | 100% HOT, heap 1.7 MB, 574 dead tuples, bloat 13.6%, autovacuum never ran |
| pgheap, autovacuum off | 5.26 | 190,079 | 7.1 MB | the same |

`nilbench --workload long-snapshot --hot-keys 10000 --updates 100000`: 100,000 updates with a snapshot taken before them, then the snapshot released and `compact_range` run, then 100,000 more updates.

| Engine | µs per update, snapshot held | Disk: before → held | Compact after release | Disk after compact | µs per update after | Disk after |
|---|---:|---:|---:|---:|---:|---:|
| LSM | 2.69 | 1.2 → 14.8 MB | 53 ms | 1.2 MB | 2.66 | 14.8 MB |
| B+ tree | 19.62 | 2.7 → 1,231 MB | nothing to do | 1,231 MB | 22.82 | 1,231 MB |
| pgheap | 5.79 | 3.1 → 46.7 MB | 17 ms (VACUUM) | 46.7 MB | 4.59 | 73.2 MB |

pgheap's counters through that run: with the snapshot held, 10.5% of updates were HOT, 100,000 dead tuples, 99,451 index entries and a 17.0 MB heap (ten times the 1.7 MB of the update-hot run); after VACUUM, no dead tuples and 10,000 index entries, the heap still 17.0 MB; after the second 100,000 updates the heap was still 17.0 MB, and the cumulative HOT ratio had risen to 55.3%, which puts nearly all of the second 100,000 at HOT.

`go test -tags nilengine -run '^$' -bench 'UpdateHot' -benchtime 500000x` and `-bench 'UpdateLongSnapshot' -benchtime 100000x` (one cgo crossing per operation):

| Store | UpdateHot ns/op | Disk after | UpdateLongSnapshot ns/op | Disk: held → compacted → after 100,000 more |
|---|---:|---:|---:|---|
| LSM | 3,316 | 59.4 MB | 3,322 | 14.8 → 1.2 → 14.8 MB |
| B+ tree | 20,751 | 69.7 MB | 18,757 | 1,231 → 1,231 → 1,231 MB |
| pgheap | 6,622 | 10.2 MB | 5,823 | 46.6 → 46.6 → 33.5 MB |
| pgheap, autovacuum off | 6,502 | 10.2 MB | | |
| RocksDB | 4,023 | 5.1 MB | 3,579 | 15.0 → 1.2 → 15.0 MB |

Through cgo pgheap reported the same shape: 100% HOT, 550 dead tuples and a 1.7 MB heap after 500,000 updates; with the snapshot held, 100,000 dead tuples and 10.6% HOT, none dead after VACUUM, and the heap at 17.0 MB in all three phases.

What the numbers show:

- On a small key set pgheap's updates are all HOT. After a million updates of 10,000 keys the heap holds 1.7 MB for 1.5 MB of live tuples and the index one entry per key. Pruning alone keeps dead tuples under 600, below autovacuum's threshold of 2,050 (50 + 20% of 10,000), so autovacuum never ran and turning it off changed nothing beyond noise.
- A single-key update costs pgheap 5.4 µs in Rust, against 2.9 µs for the LSM and 21.7 µs for the B+ tree; through cgo, 6.6 µs against 3.3 µs for the LSM and 4.0 µs for RocksDB. A pgheap update looks the key up in the index, walks its HOT chain, rewrites a page, logs an update and a commit record and makes one `write` system call.
- A snapshot held open turns about 90% of updates non-HOT and grows the heap tenfold, with 100,000 dead versions and nearly one index entry per update. One VACUUM after the release took 17 ms and left no dead tuples and one index entry per key. The heap file keeps its size, and the next 100,000 updates fit inside it, nearly all HOT.
- The LSM and RocksDB keep versions for the snapshot too (15 MB), and a compaction after the release takes the directory back to 1.2 MB, because both rewrite files rather than reuse pages. The B+ tree cannot reuse a page any snapshot can reach, so 100,000 updates under a snapshot grew its file to 1.2 GB; it never shrinks, though later updates reuse the freed pages.
- Most of pgheap's disk use in these tables is WAL: a checkpoint runs every 256 MiB of WAL and none of these runs reached one. The LSM likewise keeps up to 64 MiB of log before a memtable flush.
- Not measured this round: pgheap on the phase-1 workloads (fill, read, seek, mixed at 2,000,000 keys). `make rust-bench` and `make nilengine-bench` now include pgheap in them.

## Deferred and known gaps

- Compression: neither engine compresses. RocksDB in NilDB uses LZ4 and ZSTD, so phase 2 disk use will be higher on the Rust engines.
- LSM: writers serialize on one mutex with one `write` call per commit (no group commit); no per-column-family memtables or options, prefix bloom filters, rate limiter, subcompactions or statistics counters; range tombstones live in the manifest, which suits NilDB's few drops but not millions of range deletes; every table file stays open.
- B+ tree: 981-byte key limit at 4 KiB pages; open reads the whole file; the file never shrinks; single-key commits rewrite a root-to-leaf path (about 5 pages per commit in the benchmark); pages freed by unsynced commits wait for the next sync, which grew the 2,000,000-key fillrandom file to 437 MB.
- pgheap: one writer at a time; delete-range costs O(keys in range); no TOAST compression; the heap file never shrinks; the commit log is never truncated; pruning runs only on the write path; no background writer; the phase-1 workloads have not been benchmarked on it.
- All engines: no checkpoints, approximate sizes, read-only open or compaction filters yet (phase 2 items below); the C interface copies each value twice on the way to Go (Rust to malloc, malloc to Go) and has no pinned-read call.

## Phase 2 plan: the engines behind `internal/store`

The upper layers (`redis`, `cmddoc`, `query`, `analytics`, `admin`, `docstore`, `catalog`, `server`) keep calling `store.Store`, `store.Reader`, `store.Txn`, `store.Iterator` and `store.Snapshot` exactly as today. The engine work stays inside `internal/store`. Outside it, `internal/config` gains the flag, `cmd/nildb` passes it to the store, and `internal/admin` and `internal/server` change a few INFO lines (step 3). Those two need edits because `store.Version()` (the linked RocksDB version) and `store.ShimActive()` (RocksDB's read-options shim) are package-level functions, which cannot tell which engine a `Store` opened.

### Step 1: an engine seam inside the package

Add an unexported interface in `internal/store`, for example `kv`, with the calls the package makes on RocksDB today: open with the eight families, write a batch (with sync), get, multi-get, iterator with bounds and options, snapshot and release, flush, flush-WAL, compact-range, property, latest sequence, close. Move the grocksdb code (`store.go` open and close, `reader.go`, `iterator.go`, `snapshot.go`, `txn.go`, `admin.go`, `drop.go`, `options.go`, the filters, the merge operator, the read-options shim) behind a `rocksKV` implementation without changing its behaviour, and add `nilKV` over `internal/nilengine`. Everything that is NilDB logic rather than RocksDB stays above the seam and serves all engines: the layout marker check, the version generator, the snapshot registry with leases and the janitor, the lock manager, `ScheduleCompact`, `DeleteRanges`.

`store.Config` gains `Engine string` (`"rocksdb"` default, `"lsm"`, `"btree"`). `nilKV` compiles only with `-tags nilengine`; without the tag, `Open` with `lsm` or `btree` fails with "built without nilengine", so the default build still needs no Rust. The store writes an `ENGINE` file into a new directory and refuses to open a directory with another engine's marker; the LSM's `CURRENT` and `MANIFEST-*` names would otherwise collide with RocksDB's. A directory without the file belongs to RocksDB, so today's data directories and RocksDB checkpoints open unchanged. `Checkpoint` writes the marker into the checkpoint directory after the engine call, and `OpenReadOnly` (`--readonly`) runs the same check, so an LSM checkpoint cannot be opened as RocksDB. The LSM's open-time cleanup deletes only `*.log`, `*.sst`, `MANIFEST-*` and `*.tmp` files, and the B+ tree touches only `data.nbt` and `LOCK`, so neither engine removes the marker.

The seam also adds three methods for the callers of the package-level functions: `(*Store).Engine()` returns `"rocksdb"`, `"lsm"` or `"btree"`; `(*Store).EngineVersion()` returns RocksDB's version or `nil_version()`; `(*Store).ShimActive()` returns the package-level result under RocksDB and false with the reason "engine lsm has no rate limiter" (or `btree`) otherwise. `store.Version()` and `store.ShimActive()` stay, because RocksDB is still linked.

### Step 2: map every RocksDB feature `internal/store` relies on

**Compaction filters (expiry and garbage).** RocksDB runs `metaFilter` (expired metadata and version-map entries 300 s past expiry), `subFilter` (sub and zscore entries whose version has no live version-map entry, versions younger than 60 s skipped, a 64-slot verdict cache) and `idFilter` (entries of dropped `coll_id`/`idx_id` per the `LiveSet`). Neither Rust engine has filters, and the B+ tree has no compaction to host them. Phase 2 adds one Go-side sweeper, a store janitor job that serves both Rust engines:
- sub and zscore: skip-scan the distinct 8-byte version prefixes (seek to `version + 1` after each), look each up in the version map with `subFilter`'s rules, and delete-range `[version, version + 1)` for dead ones. No lock is needed because a version is never reused.
- doc, idx, geo, col: skip-scan the 4-byte id prefixes and delete-range ids the `LiveSet` reports dead, the job `idFilter` does as a backstop today.
- meta: scan under a snapshot for entries expired past the grace period, then delete each one under its `LockRedis` stripe after re-reading it, because a writer may recreate the key between scan and delete (the filter avoids that race by working on old versions; a new tombstone would not).
The sweeper runs every 10 minutes by default with a byte budget per round. On the LSM its range deletes cost O(1) and compaction reclaims the space; on the B+ tree they free whole subtrees. Porting the three filters into the LSM's compaction loop as built-in filters (the Go side pushing the live id set through a new C call) is a later optimization, not a phase 2 requirement.

**Merge operator.** Done: both engines implement `i64add` natively. `Txn.Merge` keeps rejecting families other than `default`.

**`WriteBatchWI` for MULTI/EXEC read-your-writes.** `server/multi.go` and `cmddoc` call `BeginIndexed`. For the Rust engines, `store` gets a Go overlay transaction: an ordered map per family (a small skip list keyed by family and key) holding each key's latest put, delete or pending merge operands, plus the operation log in order. `Get` reads the overlay, then the engine, applying pending `i64add` operands to the base value. `Iter` merges an overlay iterator with the engine iterator in both directions and inside the bounds, the job `NewIteratorWithBase` does. `Commit` writes the log as one batch. `DeleteRange` keeps returning `ErrRangeInIndexedTxn`, as with RocksDB. The overlay needs no C support and could later serve RocksDB too.

**Prefix extractors and filters.** `sub` and `zscore` use an 8-byte fixed prefix and `geo` a 12-byte one, with memtable prefix blooms and Ribbon filters (whole-key off on `zscore` and `geo`). The Rust engines have no prefix mode, so every iterator is total order: `IterOpts.TotalOrderSeek` becomes a no-op, and `PrefixSameAsStart` (geo ancestor probes) turns into an upper bound at the prefix's successor, computed in `store` before the call. Whole-key bloom filters serve point reads on the LSM. A bounded scan inside one collection touches one file per level at 1 and below, because files there are range-partitioned; a per-file prefix filter for level-0 files is a follow-up if profiles show level 0 in the way.

**Checkpoints.** `ROCKS.CHECKPOINT` uses `CreateCheckpoint(dir, 0)`, and `--readonly` opens a checkpoint with `OpenDbForReadOnlyColumnFamilies`. New engine calls: `nil_checkpoint(db, dir)`. For the LSM it flushes the memtable, hard-links every live table file into `dir` and writes a manifest and `CURRENT` there; table files are immutable, as RocksDB's checkpoint relies on. For the B+ tree it holds a snapshot, clones `data.nbt` with APFS `clonefile` (copying on other file systems), and writes a meta page for the snapshot's roots into the copy. A read-only open flag follows (shared `flock`, no writes, no background threads). `ExportCF` and `Ingest` stay RocksDB-only.

**Properties.** Both engines answer `rocksdb.estimate-num-keys`, `rocksdb.num-snapshots`, `rocksdb.block-cache-usage`, `rocksdb.block-cache-capacity` and `rocksdb.stats`. The LSM also answers `rocksdb.num-files-at-level<N>`, `rocksdb.total-sst-files-size`, `rocksdb.cur-size-all-mem-tables`, `rocksdb.cur-size-active-mem-table`, `rocksdb.num-immutable-mem-table`, `rocksdb.estimate-pending-compaction-bytes` and `rocksdb.oldest-snapshot-sequence`. Other names return nothing, which `store.Property` already reports as absent; `IntProperty` parses the string. `rocksdb.oldest-snapshot-time` comes from the store's own registry.

`internal/admin` reads nine more names: `rocksdb.estimate-live-data-size`, `rocksdb.live-sst-files-size`, `rocksdb.size-all-mem-tables`, `rocksdb.num-entries-active-mem-table`, `rocksdb.mem-table-flush-pending`, `rocksdb.compaction-pending` and `rocksdb.block-cache-pinned-usage` for `ROCKS.CF INFO`, which skips a name the engine does not report, and `rocksdb.num-running-flushes` and `rocksdb.num-running-compactions` for the INFO `rocksdb` section, which prints 0 for one. Phase 2 adds all nine to the LSM, which has the flush and compaction threads, memtables and version to answer them. On the B+ tree they stay absent, since it has no memtable, table files or compaction. With one cache per engine, the INFO `analytics_cache_*` fields repeat the main cache's figures.

`ApproxSizes` feeds the query planner (`query/planner.go`) and `DOC.STATS`, so phase 2 adds `nil_approximate_sizes`: the LSM sums the bytes of overlapping files, prorated by key range, plus the memtable share; the B+ tree estimates from key counts and the depth.

**Rate limiter and the priority shim.** `IterOpts.LowPriority` reaches RocksDB through the cgo shim that sets `rate_limiter_priority = IO_LOW`, and the rate limiter caps background I/O at 200 MiB/s. The Rust engines have neither; `(*Store).ShimActive()` (step 1) reports false with the reason "engine lsm has no rate limiter", the answer the package-level check already gives when the shim's self-check fails, and `internal/admin` switches its two calls (`ROCKS.INFO` and the INFO `rocksdb` section) to the method. A token bucket for LSM flush and compaction writes, with low-priority reads charged to it, is a follow-up.

**Other iterator options.** `FillCache` maps to `fill_cache`. `Readahead` and `AsyncIO` are ignored; the operating system reads ahead. `Deadline` is RocksDB's `ReadOptions` deadline today (`iterator.go`). For the Rust engines the store's iterator wrapper enforces it, checking the clock every 64 moves and failing with an error whose text contains `timed out`, which `readErr` in `query/op.go` already maps to `ErrTimeout`.

**Column-family options, caches and the write buffer manager.** Per-family write buffers, per-level compression, periodic compaction, the two HyperClockCaches and the `WriteBufferManager` have no counterpart. `BlockCacheBytes` maps to `cache_bytes`, `WriteBufferBytes` to `write_buffer_size` (one memtable budget for all families), and `AnalyticsCacheBytes` is ignored because each engine has one cache.

**Durability.** `--fsync always` sets `sync` per commit, `everysec` keeps its ticker calling `FlushWAL(true)`, `no` does nothing. The B+ tree keeps its tree intact without syncs and additionally syncs every `max_unsynced_free_pages` freed pages.

**The rest.** `SetOption`, `Ingest` and `ExportCF` return a new `store.ErrUnsupported` whose text reads "not supported with --engine lsm" (or `btree`). `Stats` returns the `nil.stats` property with or without `--rocks-stats`, because `Config.Statistics` only switches on RocksDB's optional counters and the engines keep none. `ROCKS.INFO` reports `EngineVersion()` as its `version` and adds an `engine` field. `LatestSeq` maps to `latest_sequence`, which on the B+ tree is a transaction id: it still grows with every commit, which is all that `ROCKS.SEQ` and the checkpoint reply promise. `ScheduleCompact` calls `compact_range`, a no-op on the B+ tree.

### Step 3: the `--engine` flag and `ROCKS.*`

`cmd/nildb` gets `--engine rocksdb|lsm|btree` (default `rocksdb`), passed through `internal/config` into `store.Config.Engine`. INFO's server section (`internal/server/info.go`) gains `nildb_engine:` and `nildb_engine_version:` next to `nildb_rocksdb_version`, which keeps naming the linked library. The INFO `rocksdb` section's `rocksdb_version` reports `EngineVersion()`, and the startup log line names the engine instead of always printing the RocksDB version.

| Command | With `--engine lsm` or `btree` |
|---|---|
| `ROCKS.CF`, `ROCKS.COMPACT`, `ROCKS.FLUSH`, `ROCKS.FLUSHWAL`, `ROCKS.GET`, `ROCKS.SCAN`, `ROCKS.SEQ`, `ROCKS.SNAPSHOT` | work as today through the engine calls |
| `ROCKS.PROPERTY`, `ROCKS.STATS`, `ROCKS.INFO` | work with the names and text the engine provides |
| `ROCKS.SIZES` | works once `nil_approximate_sizes` lands |
| `ROCKS.CHECKPOINT dir` | works once `nil_checkpoint` lands; `ROCKS.CHECKPOINT dir CF cf` stays RocksDB-only |
| `ROCKS.INGEST`, `ROCKS.SETOPTION` | RocksDB-only; reply `ERR not supported with --engine lsm` (or `btree`) |

The names stay `ROCKS.*` so scripts written against RocksDB keep working. The RocksDB-only replies need no new code in `internal/admin`: its `storeErr` prefixes `ERR ` to any store error it does not map, so the text of `store.ErrUnsupported` becomes the reply.

### Step 4: tests and measurements

- Run `internal/store`'s tests against all three engines, choosing the engine from `NILDB_TEST_ENGINE` under the `nilengine` tag. Tests of RocksDB-only features (`Ingest`, `SetOption`, filter verdicts) skip on the Rust engines; the sweeper gets its own tests.
- Run the server-level tests in `internal/testutil`, the `ROCKS.*` and INFO tests in `internal/admin` and the binary tests in `cmd/nildb` with `--engine lsm` and `--engine btree`.
- Keep the Rust conformance suite as the engines' gate, and add a store-level crash test: kill the server process during a write load and check that every acknowledged `--fsync always` write survives.
- Repeat the benchmark above through `store` and add `redis-benchmark -P 16` and `cmd/htapbench` runs per engine.

Order: the seam with `rocksKV` first, with no behaviour change and the existing test suite green; then `nilKV` with the overlay transaction, the flag and the engine-aware INFO fields; then approximate sizes, the sweeper and checkpoints; the rate limiter and compaction-loop filters last.

## Sources

- PostgreSQL documentation, current version: "Database Page Layout" (https://www.postgresql.org/docs/current/storage-page-layout.html), "Concurrency Control" (https://www.postgresql.org/docs/current/mvcc.html), "Routine Vacuuming" (https://www.postgresql.org/docs/current/routine-vacuuming.html).
- PostgreSQL source tree (PostgreSQL License), github.com/postgres/postgres: `src/backend/access/heap/README.HOT`, `src/backend/access/nbtree/README`, `src/backend/access/transam/README`, `src/include/access/heaptoast.h` (TOAST_TUPLES_PER_PAGE and EXTERN_TUPLES_PER_PAGE, both 4), `src/backend/postmaster/walwriter.c` (WalWriterDelay = 200 ms).
- Philip L. Lehman and S. Bing Yao, "Efficient Locking for Concurrent Operations on B-Trees", ACM TODS 6(4), 1981.
- LevelDB (BSD-3-Clause): `doc/table_format.md`, `doc/log_format.md`, `db/dbformat.h`, `db/skiplist.h`, `db/version_set.cc`, `db/db_iter.cc`, `util/bloom.cc`, github.com/google/leveldb.
- RocksDB wiki, "DeleteRange" and "DeleteRange Implementation" (fragmented range tombstones); RocksDB `CompactionIterator` for snapshot stripes; WAL recovery modes.
- Howard Chu, "MDB: A Memory-Mapped Database and Backend for OpenLDAP", LDAPCon 2011.
- bbolt, `DB.NoFreelistSync` documentation, github.com/etcd-io/bbolt.
- Ohad Rodeh, Josef Bacik, Chris Mason, "BTRFS: The Linux B-Tree Filesystem", ACM Transactions on Storage 9(3), 2013.
- William Pugh, "Skip Lists: A Probabilistic Alternative to Balanced Trees", CACM 33(6), 1990.
- Burton H. Bloom, "Space/Time Trade-offs in Hash Coding with Allowable Errors", CACM 13(7), 1970.
- Austin Appleby, MurmurHash64A, SMHasher (public domain).
- Guy Steele, Doug Lea, Christine Flood, "Fast Splittable Pseudorandom Number Generators" (SplitMix64), OOPSLA 2014; Sebastiano Vigna, "An experimental exploration of Marsaglia's xorshift generators, scrambled", ACM TOMS 42(4), 2016.
