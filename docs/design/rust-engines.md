# NilDB storage engines in Rust

Status: phase 1 done on 2026-09-27. Two engines, an LSM tree and a copy-on-write B+ tree, pass one conformance suite, are callable from C and from Go, and have a first benchmark against RocksDB 11.8.1. Nothing in the server uses them yet; phase 2 puts them behind `internal/store` (last section).

Everything lives in `rust/` (a cargo workspace), `rust/include/nilengine.h` and `internal/nilengine`.

| Crate | Directory | Contents |
|---|---|---|
| `nilengine-api` | `rust/api` | `Engine` trait, `WriteBatch`, options, errors, varints, CRC-32, a sharded LRU cache |
| `nilengine-conformance` | `rust/conformance` | the suite both engines run, a `BTreeMap` model, seeded workloads |
| `nilengine-lsm` | `rust/lsm` | LSM engine |
| `nilengine-btree` | `rust/btree` | B+ tree engine |
| `nilengine-capi` | `rust/capi` | C interface, built as `rust/target/release/libnilengine.a` |
| `nilengine-bench` | `rust/bench` | `nilbench`, the Rust-side benchmark |

The only third-party crate is `crc32fast` (plus its `cfg-if`). Tests use their own temp-dir helper and a SplitMix64 generator, so neither `tempfile` nor `rand` is needed. `Cargo.lock` is committed.

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

## C interface

`rust/include/nilengine.h` is hand-written; `rust/capi/src/lib.rs` implements it; `nm` shows the 37 exported `nil_*` symbols. The archive needs `-lSystem -lc -lm`, which macOS links by default.

- Handles: `nil_db`, `nil_batch`, `nil_snapshot`, `nil_iter`, all opaque. `nil_open(engine, dir, cf_names, num_cfs, options, errptr)` takes `NIL_ENGINE_LSM` (1) or `NIL_ENGINE_BTREE` (2); column families are numbered by position.
- Errors: every call that can fail takes `char **errptr` and stores a malloc'd `"class: message"` string there (classes `io`, `corruption`, `invalid-argument`, `not-found`, `busy`, `closed`, `unsupported`, `panic`), freeing any string already present. `errptr` may be NULL.
- Memory: values from `nil_get` and `nil_multi_get`, property strings and error strings are malloc'd and freed with `nil_free`. A found empty value is a non-NULL pointer with length 0. `nil_iter_key` and `nil_iter_value` return views valid until the next move.
- Calls: `nil_batch_{new,put,delete,delete_range,merge,count,clear,destroy}`, `nil_write`, `nil_get`, `nil_multi_get`, `nil_snapshot_{new,seq,release}`, `nil_iter_{new,seek_to_first,seek_to_last,seek,seek_for_prev,next,prev,valid,key,value,status,destroy}`, `nil_flush`, `nil_flush_wal`, `nil_compact_range`, `nil_property`, `nil_latest_sequence`, `nil_engine_kind`, `nil_max_key_len`, `nil_version`, `nil_free`, `nil_close`.
- Panics: every exported function runs its body under `catch_unwind`. A panic comes back as a `panic: ...` error, or, in calls without `errptr`, is stored in the batch or iterator and reported by `nil_write` or `nil_iter_status`. A batch that received a NULL pointer with a nonzero length is refused at `nil_write`.

A throwaway C program compiled with `clang -std=c11 -Wall -Wextra -Werror` against the header and the archive opened both engines, wrote, read and iterated.

## Go binding

`internal/nilengine` wraps the C interface with cgo (`#cgo LDFLAGS: ${SRCDIR}/../../rust/target/release/libnilengine.a`). Every file except `doc.go` carries `//go:build nilengine`, so without the tag the package is empty and `go test ./...` needs no Rust. The API follows `internal/store`: `Open(kind, dir, cfs, opts)`, `Batch` (`Put`, `Delete`, `DeleteRange`, `Merge`), `Write(b, sync)`, `Get`, `GetAt(snap, ...)`, `MultiGet`, `NewSnapshot`, `NewIterator(cf, IterOptions{Snapshot, Lower, Upper, FillCache})` with `Key`/`Value` views valid until the next move, `Flush`, `FlushWAL`, `CompactRange`, `Property`, `LatestSeq`. Errors are `*nilengine.Error` values that match the sentinels (`ErrBusy`, `ErrClosed`, ...) through `errors.Is`. A `DB` is safe for concurrent use and returns `ErrClosed` after `Close`. `MultiGet` copies keys into C memory because cgo forbids passing Go memory that holds Go pointers.

## Tests

Commands, run on 2026-09-27:

```
make rust-test         # cargo test --workspace: 153 passed, 4 ignored (the soak runs)
make rust-soak         # the ignored soak tests, NILENGINE_SOAK=24 seeds each
make nilengine-test    # go test -tags nilengine ./internal/nilengine: 5 tests, 8 engine subtests
```

The 153 Rust tests: 9 in `nilengine-api`, 10 LSM unit tests, 5 B+ tree unit tests, 4 C interface tests, 8 LSM and 9 B+ tree internals tests, and the conformance suite, 27 cases run twice per engine (54 per engine). The second LSM run uses 256-byte blocks, a restart point every two keys, no bloom filter and an 8 KiB memtable; the second B+ tree run uses 512-byte pages, which makes trees four or five levels deep and puts most values in overflow chains.

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

The soak tests (`make rust-soak`) ran 24 extra seeds of the randomized and crash cases under each of the four engine configurations. All passed. No engine code needed a fix after its first test run; the three failures seen during development were wrong expectations in tests: a fixed 100-byte key over the B+ tree's 85-byte limit at 512-byte pages, a C test expecting a key it had range-deleted, and a Go test expecting the B+ tree's transaction id to stay put when a column family is added.

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

## Deferred and known gaps

- Compression: neither engine compresses. RocksDB in NilDB uses LZ4 and ZSTD, so phase 2 disk use will be higher on the Rust engines.
- LSM: writers serialize on one mutex with one `write` call per commit (no group commit); no per-column-family memtables or options, prefix bloom filters, rate limiter, subcompactions or statistics counters; range tombstones live in the manifest, which suits NilDB's few drops but not millions of range deletes; every table file stays open.
- B+ tree: 981-byte key limit at 4 KiB pages; open reads the whole file; the file never shrinks; single-key commits rewrite a root-to-leaf path (about 5 pages per commit in the benchmark); pages freed by unsynced commits wait for the next sync, which grew the 2,000,000-key fillrandom file to 437 MB.
- Both: no checkpoints, approximate sizes, read-only open or compaction filters yet (phase 2 items below); the C interface copies each value twice on the way to Go (Rust to malloc, malloc to Go) and has no pinned-read call.

## Phase 2 plan: the engines behind `internal/store`

The upper layers (`redis`, `cmddoc`, `query`, `analytics`, `admin`, `docstore`, `catalog`, `server`) keep calling `store.Store`, `store.Reader`, `store.Txn`, `store.Iterator` and `store.Snapshot` exactly as today. Only `internal/store` and `cmd/nildb` change.

### Step 1: an engine seam inside the package

Add an unexported interface in `internal/store`, for example `kv`, with the calls the package makes on RocksDB today: open with the eight families, write a batch (with sync), get, multi-get, iterator with bounds and options, snapshot and release, flush, flush-WAL, compact-range, property, latest sequence, close. Move the grocksdb code (`store.go` open and close, `reader.go`, `iterator.go`, `snapshot.go`, `txn.go`, `admin.go`, `drop.go`, `options.go`, the filters, the merge operator, the read-options shim) behind a `rocksKV` implementation without changing its behaviour, and add `nilKV` over `internal/nilengine`. Everything that is NilDB logic rather than RocksDB stays above the seam and serves all engines: the layout marker check, the version generator, the snapshot registry with leases and the janitor, the lock manager, `ScheduleCompact`, `DeleteRanges`.

`store.Config` gains `Engine string` (`"rocksdb"` default, `"lsm"`, `"btree"`). `nilKV` compiles only with `-tags nilengine`; without the tag, `Open` with `lsm` or `btree` fails with "built without nilengine", so the default build still needs no Rust. The store writes an `ENGINE` file into a new directory and refuses to open a directory with another engine's marker; the LSM's `CURRENT` and `MANIFEST-*` names would otherwise collide with RocksDB's.

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

**Properties.** The engines already answer the RocksDB names they can (`rocksdb.estimate-num-keys`, `rocksdb.num-snapshots`, `rocksdb.num-files-at-level<N>`, `rocksdb.total-sst-files-size`, `rocksdb.cur-size-all-mem-tables`, `rocksdb.estimate-pending-compaction-bytes`, `rocksdb.block-cache-usage`, `rocksdb.stats`) and return nothing for others, which `store.Property` already reports as absent. `IntProperty` parses the string. `rocksdb.oldest-snapshot-time` comes from the store's own registry. `ApproxSizes` feeds the query planner (`query/planner.go`) and `DOC.STATS`, so phase 2 adds `nil_approximate_sizes`: the LSM sums the bytes of overlapping files, prorated by key range, plus the memtable share; the B+ tree estimates from key counts and the depth.

**Rate limiter and the priority shim.** `IterOpts.LowPriority` reaches RocksDB through the cgo shim that sets `rate_limiter_priority = IO_LOW`, and the rate limiter caps background I/O at 200 MiB/s. The Rust engines have neither; `store.ShimActive()` reports false with the reason "engine lsm has no rate limiter", the path it already takes when the shim's self-check fails. A token bucket for LSM flush and compaction writes, with low-priority reads charged to it, is a follow-up.

**Other iterator options.** `FillCache` maps to `fill_cache`. `Readahead` and `AsyncIO` are ignored; the operating system reads ahead. `Deadline` moves into the store's iterator wrapper, which checks the clock every 64 moves and returns the same timed-out error the query layer handles today.

**Column-family options, caches and the write buffer manager.** Per-family write buffers, per-level compression, periodic compaction, the two HyperClockCaches and the `WriteBufferManager` have no counterpart. `BlockCacheBytes` maps to `cache_bytes`, `WriteBufferBytes` to `write_buffer_size` (one memtable budget for all families), and `AnalyticsCacheBytes` is ignored because each engine has one cache.

**Durability.** `--fsync always` sets `sync` per commit, `everysec` keeps its ticker calling `FlushWAL(true)`, `no` does nothing. The B+ tree keeps its tree intact without syncs and additionally syncs every `max_unsynced_free_pages` freed pages.

**The rest.** `SetOption` returns `ErrUnsupported`. `Stats` returns the `nil.stats` property. `Version` and `ROCKS.INFO` report the engine name and `nil_version()`. `LatestSeq` maps to `latest_sequence`, which on the B+ tree is a transaction id: it still grows with every commit, which is all that `ROCKS.SEQ` and the checkpoint reply promise. `ScheduleCompact` calls `compact_range`, a no-op on the B+ tree.

### Step 3: the `--engine` flag and `ROCKS.*`

`cmd/nildb` gets `--engine rocksdb|lsm|btree` (default `rocksdb`), passed through `internal/config` into `store.Config.Engine`. `INFO` gains `engine:` in its server section.

| Command | With `--engine lsm` or `btree` |
|---|---|
| `ROCKS.CF`, `ROCKS.COMPACT`, `ROCKS.FLUSH`, `ROCKS.FLUSHWAL`, `ROCKS.GET`, `ROCKS.SCAN`, `ROCKS.SEQ`, `ROCKS.SNAPSHOT` | work as today through the engine calls |
| `ROCKS.PROPERTY`, `ROCKS.STATS`, `ROCKS.INFO` | work with the names and text the engine provides |
| `ROCKS.SIZES` | works once `nil_approximate_sizes` lands |
| `ROCKS.CHECKPOINT dir` | works once `nil_checkpoint` lands; `ROCKS.CHECKPOINT dir CF cf` stays RocksDB-only |
| `ROCKS.INGEST`, `ROCKS.SETOPTION` | RocksDB-only; reply `ERR not supported with --engine lsm` (or `btree`) |

The names stay `ROCKS.*` so scripts written against RocksDB keep working.

### Step 4: tests and measurements

- Run `internal/store`'s tests against all three engines, choosing the engine from `NILDB_TEST_ENGINE` under the `nilengine` tag. Tests of RocksDB-only features (`Ingest`, `SetOption`, filter verdicts) skip on the Rust engines; the sweeper gets its own tests.
- Run the server-level tests in `internal/testutil` and the `ROCKS.*` tests with `--engine lsm` and `--engine btree`.
- Keep the Rust conformance suite as the engines' gate, and add a store-level crash test: kill the server process during a write load and check that every acknowledged `--fsync always` write survives.
- Repeat the benchmark above through `store` and add `redis-benchmark -P 16` and `cmd/htapbench` runs per engine.

Order: the seam with `rocksKV` first, with no behaviour change and the existing test suite green; then `nilKV` with the overlay transaction and the flag; then approximate sizes, the sweeper and checkpoints; the rate limiter and compaction-loop filters last.

## Sources

- LevelDB (BSD-3-Clause): `doc/table_format.md`, `doc/log_format.md`, `db/dbformat.h`, `db/skiplist.h`, `db/version_set.cc`, `db/db_iter.cc`, `util/bloom.cc`, github.com/google/leveldb.
- RocksDB wiki, "DeleteRange" and "DeleteRange Implementation" (fragmented range tombstones); RocksDB `CompactionIterator` for snapshot stripes; WAL recovery modes.
- Howard Chu, "MDB: A Memory-Mapped Database and Backend for OpenLDAP", LDAPCon 2011.
- bbolt, `DB.NoFreelistSync` documentation, github.com/etcd-io/bbolt.
- Ohad Rodeh, Josef Bacik, Chris Mason, "BTRFS: The Linux B-Tree Filesystem", ACM Transactions on Storage 9(3), 2013.
- William Pugh, "Skip Lists: A Probabilistic Alternative to Balanced Trees", CACM 33(6), 1990.
- Burton H. Bloom, "Space/Time Trade-offs in Hash Coding with Allowable Errors", CACM 13(7), 1970.
- Austin Appleby, MurmurHash64A, SMHasher (public domain).
- Guy Steele, Doug Lea, Christine Flood, "Fast Splittable Pseudorandom Number Generators" (SplitMix64), OOPSLA 2014; Sebastiano Vigna, "An experimental exploration of Marsaglia's xorshift generators, scrambled", ACM TOMS 42(4), 2016.
