# RocksDB features NilDB should build on and expose (native RocksDB feel), with grocksdb (Go) vs rust-rocksdb (Rust) coverage and a proposed ROCKS.* admin command set

Exported on 2026-09-27 from the NilDB deep-research workflow (Claude Code run `wf_1b4f4c38-bbf`, step `research:rocksdb-native-features`, agent `a8e6f45b69cfc4fd6`, researched 2026-09-26). The text is the agent's structured output, unedited; confidence labels are the agent's own.

## Design implications

1. Target RocksDB 11.8.1, not 10.x: Homebrew ships 11.8.1 (arm64 bottle) and rust-rocksdb 0.25.0 bundles 11.8.1; grocksdb v1.11.1 was adapted to 11.0.4 but links against whatever librocksdb Homebrew provides through the additive C API. Pin the version in the build and record it in ROCKS.INFO.
2. Language: both bindings wrap the same C API, so feature reach is nearly identical. Rust (rust-rocksdb 0.25.0) gives an exact RocksDB 11.8.1 match, static linking (single binary, no Homebrew runtime dependency), compaction-filter factories, and closure-based comparators/merge operators without a runtime boundary; the costs are a long first build and the clang/libclang requirement. Go (grocksdb v1.11.1) builds in seconds against the Homebrew bottle and additionally exposes standalone WriteBatchWI and Checkpoint.ExportColumnFamily, but every call crosses cgo and comparator/merge/compaction-filter callbacks re-enter Go per key. For a Redis-protocol server whose hot path is thousands of small point ops plus HTAP scans, the per-call overhead argues for Rust; if build simplicity on the target Mac dominates, Go is workable. Recommendation: Rust, with the WBWI gap covered by OptimisticTransactionDB transactions (see below).
3. Redis MULTI/EXEC maps to one WriteBatch applied atomically at EXEC (RocksDB applies edits in order, no rollback needed, matching Redis semantics). Commands inside MULTI that return values (INCR, GETSET) need read-your-own-writes: in Go use WriteBatchWI.GetFromDBWithCF; in Rust open the store as OptimisticTransactionDB and run each MULTI block as a Transaction (its get() reads the internal WBWI). WATCH maps to GetForUpdate on an optimistic transaction created at WATCH time; a Busy at commit becomes EXEC's nil reply, and expiry-triggered WATCH failure (Redis 6.0.9 semantics) must be emulated by treating expiry as a write.
4. MongoDB multi-document transactions map to a pessimistic TransactionDB transaction begun with TransactionOptions.set_snapshot=true (the C API has no later SetSnapshot), lock_timeout tuned toward Mongo's 5 ms default (RocksDB default is 1000 ms), expiration=60000 to mirror transactionLifetimeLimitSeconds, and deadlock_detect on. TimedOut/Busy/Expired statuses become WriteConflict / TransientTransactionError. Because TransactionDB and OptimisticTransactionDB are different DB types, NilDB must pick one per store: TransactionDB (pessimistic) if Mongo-style contended multi-doc transactions are first-class, with Redis MULTI/EXEC implemented as pessimistic transactions plus WATCH via GetForUpdate; skip_concurrency_control is available for non-transactional fast paths.
5. Never call DeleteRange inside a transaction or WBWI (unsupported at the C++ level). Collection/keyspace drops (FLUSHDB, drop collection) run as a DB-level DeleteRange over a fixed-length prefix outside any transaction, followed by an optional scheduled CompactRange, and NilDB should cap the number of outstanding range tombstones per CF.
6. Key layout: keep the default BytewiseComparator and encode order into bytes (big-endian integers, fixed-width type tags, geohash/S2 cell ids as fixed-length strings). A custom comparator is a per-comparison host-language callback in both bindings and its Name() is persisted, so changing it later breaks opens. Use a fixed-length prefix (the only prefix transform in the C API): e.g. [keyspace id][type tag] so NewFixedPrefixTransform + memtable_prefix_bloom_size_ratio accelerate short scans (HGETALL, SMEMBERS, ZRANGE). Since neither binding exposes auto_prefix_mode, every prefix scan sets iterate_upper_bound and prefix_same_as_start=true; anything else uses total_order_seek=true. Do not rely on SeekToLast in prefix mode.
7. Column families: one CF per storage role (kv/hash/list/set/zset data, secondary indexes, geo index, expiry index, metadata) so each carries its own compression, bloom, prefix extractor, merge operator and compaction filter, and NilDB can DROP a CF instead of range-deleting. All mutations for one command go in one WriteBatch across CFs (atomic). Keep the WAL on so cross-CF recovery is consistent without atomic_flush.
8. Counters and appends (INCR, INCRBYFLOAT, APPEND, HINCRBY, $inc) use Merge with a single dispatching merge operator per CF keyed by an operand tag (only one operator per CF). Set max_successive_merges to a small bound on the hot CFs so memtable chains stay short, and prefer PartialMerge-capable operators. Remember that snapshots pin unmerged operands.
9. TTL/expiry: store the expiry timestamp in a fixed value header and check it on every read (RocksDB TTL is non-strict and DBWithTTL is incompatible with plain opens). Garbage-collect with a compaction filter that drops expired entries, plus periodic_compaction_seconds so cold files are revisited, plus a scheduled ROCKS.COMPACT. In Go the single CompactionFilter instance is shared across subcompactions and must be a pure function; Rust can use a factory. Document that expired keys can disappear from a pinned analytical snapshot (filters ignore snapshots since 6.0).
10. HTAP read path: every analytical command (aggregations, $geoWithin scans, SCAN over a collection) takes GetSnapshot, iterates with iterate_lower/upper_bound, fill_cache=false, readahead_size and async_io, and releases the snapshot when the reply is sent. Expose snapshots to clients as leases with a hard TTL and a per-server cap, and surface rocksdb.num-snapshots and rocksdb.oldest-snapshot-time so operators can see pinned history. For heavier analytics, offer ROCKS.CHECKPOINT and open the checkpoint read-only or as a secondary instance (both bindings expose OpenDbAsSecondary/open_as_secondary) so scans cannot pin the primary.
11. Caches: use HyperClockCache (estimated_entry_charge=0) for the main block cache, cache_index_and_filter_blocks + pin_l0_filter_and_index_blocks_in_cache, and give analytics-only CFs (secondary/geo indexes) their own BlockBasedTableOptions with a separate, smaller cache object so index scans cannot evict the OLTP working set; consider row_cache for the point-lookup CFs.
12. Durability policy modelled on Redis appendfsync: always = WriteOptions.sync=true; everysec (default) = manual_wal_flush=true plus a 1 s FlushWAL(sync=true) timer; no = disableWAL (then atomic_flush=true). Keep wal_recovery_mode at kPointInTimeRecovery, set wal_dir separately if desired, and cap max_total_wal_size. Use no_slowdown for clients that opt into fail-fast under write stalls.
13. Bulk load and restore: implement ROCKS.INGEST over SST files produced by SstFileWriter (strictly increasing keys under the DB comparator, same CF options); use ingest_behind (with allow_ingest_behind at open) for historical backfill. Checkpoint is the cheap local snapshot (hard links); BackupEngine (share_files_with_checksum, flush_before_backup) is the incremental, verifiable backup.
14. Defaults for the Apple Silicon SSD target, straight from the setup guide: level compaction with level_compaction_dynamic_level_bytes=true, max_background_jobs=6, bytes_per_sync=1 MiB, compaction_pri=kMinOverlappingRatio, block_size=16 KiB, 10-bit bloom (or ribbon) with optimize_filters_for_memory, LZ4 on upper levels and ZSTD bottommost, write_buffer_size 64 MiB, block cache about a third of the memory budget, max_open_files=-1, latest format_version, one shared auto-tuned rate limiter (writes only; it never throttles the WAL). Ship these as the default options file and expose them through CONFIG GET/SET where mutable (SetOptions).
15. Proposed admin command set (ROCKS.* follows the Redis module dot convention; all admin-flagged, RESP3 maps): ROCKS.INFO (RocksDB version, options summary, CF list; also injected as an 'rocksdb' INFO section of field:value lines) | ROCKS.PROPERTY <name> [CF cf] (GetProperty/GetIntProperty; e.g. rocksdb.stats, rocksdb.estimate-num-keys, rocksdb.cur-size-all-mem-tables, rocksdb.levelstats, rocksdb.block-cache-usage; cheap current state) | ROCKS.STATS [LEVEL n] (Statistics ToString; off by default because of the 5-10% overhead) | ROCKS.CF.LIST / ROCKS.CF.CREATE name [option value ...] / ROCKS.CF.DROP name / ROCKS.CF.INFO name (ListColumnFamilies, CreateColumnFamily with per-CF options, DropColumnFamily, cfstats) | ROCKS.FLUSH [CF ...] [WAIT 0|1] and ROCKS.FLUSHWAL [SYNC] (Flush/FlushCFs, FlushWAL(sync)) | ROCKS.COMPACT [CF cf] [RANGE start end] [BOTTOMMOST force|skip] [EXCLUSIVE 0|1] [SUBCOMPACTIONS n] (CompactRange with CompactRangeOptions; blocking in RocksDB, so run it as a background job with an id and a ROCKS.COMPACT CANCEL that flips 'canceled'; justification: read optimisation after bulk load, forcing TTL GC, option migration) | ROCKS.WAITCOMPACT (rocksdb_wait_for_compact) | ROCKS.CHECKPOINT dir [LOGSIZEFORFLUSH n] (returns the checkpoint sequence number; hard links on the same filesystem) | ROCKS.CF.EXPORT name dir (ExportColumnFamily; Go only today) | ROCKS.BACKUP CREATE [FLUSH] / LIST / VERIFY id / RESTORE id|LATEST dir / PURGE n (BackupEngine) | ROCKS.SNAPSHOT CREATE [TTL ms] / RELEASE id / LIST, and a connection-scoped ROCKS.USE SNAPSHOT id so subsequent reads/aggregations run against the pinned view (enforce the cap; report num-snapshots/oldest-snapshot-time) | ROCKS.SCAN cf start end [SNAPSHOT id] [LIMIT n] [REVERSE] [NOCACHE] (raw bounded iterator with fill_cache=false, readahead/async_io for the analytical path; REVERSE uses SeekForPrev under total order) | ROCKS.DELETERANGE cf start end (documented as [start,end), outside transactions) | ROCKS.INGEST cf path [path ...] [MOVE] [BEHIND] [NOGLOBALSEQNO] (IngestExternalFile with IngestExternalFileOptions) | ROCKS.SETOPTION cf key value and ROCKS.GETOPTIONS cf (SetOptions for mutable options such as disable_auto_compactions, write_buffer_size, level0 triggers) | ROCKS.CACHE INFO / SETCAPACITY bytes (block-cache-usage/capacity/pinned-usage, Cache.SetCapacity) | ROCKS.SEQNO and ROCKS.UPDATES SINCE seq [COUNT n] (GetLatestSequenceNumber, GetUpdatesSince for change-data-capture and future replication) | ROCKS.LIVEFILES (GetLiveFilesMetaData; feeds external backup tooling) | ROCKS.PERF ON|OFF|GET (PerfContext for the current connection) | ROCKS.BGWORK CANCEL (CancelAllBackgroundWork before shutdown). Every command name and option maps one-to-one onto a RocksDB API named in the findings, which is what gives NilDB the 'native RocksDB feel' without inventing semantics.

## Open questions

- The task assumed RocksDB 10.x but current releases are 11.x (11.8.1). Confirm the parent research and any performance numbers elsewhere in the plan refer to 11.8.1 rather than a 10.x build.
- grocksdb v1.11.x was adapted to RocksDB 11.0.4 while Homebrew ships 11.8.1. The C API is additive, so linking should work, but this was not test-built here; a smoke build (`brew install rocksdb` + `go build` with the README CGO flags) is needed before committing to Go.
- rust-rocksdb compiles RocksDB 11.8.1 from source with bindgen; first-build time and clang/libclang availability on the target Mac were not measured here. Confirm `xcode-select --install` (or Homebrew llvm) satisfies the libclang runtime link.
- Neither binding exposes auto_prefix_mode even though c.h has rocksdb_readoptions_set_auto_prefix_mode; decide whether to patch the binding (both are small wrappers) or to always use explicit iterate_upper_bound + prefix_same_as_start.
- Pessimistic TransactionDB vs OptimisticTransactionDB is a one-time choice per store (different DB types). The findings give the RocksDB trade-off (contention vs. lock overhead) but the expected NilDB workload mix (Redis-style single-key ops vs Mongo-style multi-doc transactions) decides it; this needs a load profile.
- The per-CF value-header TTL design plus compaction-filter GC interacts with pinned analytical snapshots (filters ignore snapshots since 6.0). Decide whether expired keys must remain visible to an in-flight snapshot lease or whether 'may disappear' is acceptable.
- rust-rocksdb has no standalone WriteBatchWithIndex, so Redis MULTI blocks with reads would run as OptimisticTransactionDB transactions; the overhead of a transaction per MULTI vs a plain WriteBatch was not measured.
- Exact numeric defaults for memtable_prefix_bloom_size_ratio, max_successive_merges and the snapshot-lease cap were not given by the docs; they need micro-benchmarks on the target machine.
- The Redis INFO page note that module-generated sections are excluded from INFO all suggests NilDB should also expose the RocksDB section under a distinct argument (INFO rocksdb); confirm the parent's RESP compatibility target (RESP2 vs RESP3) before fixing reply shapes for ROCKS.* commands.

## Findings (37)

### 1. Current RocksDB is 11.x, not 10.x: the latest release is v11.8.1 (Aug 2026); main branch version.h is 11.12.0; older lines still receive patches (v10.10.1 in Feb 2026).

Releases page lists RocksDB 11.8.1 (07 Aug), 11.1.2 (25 Jun), 11.1.1 (29 Apr), v11.0.4 (07 Apr), v10.10.1 (02 Feb). include/rocksdb/version.h on main: ROCKSDB_MAJOR 11, ROCKSDB_MINOR 12, ROCKSDB_PATCH 0. The wiki is unversioned living documentation; the headers I cite are from main.

- Source: https://github.com/facebook/rocksdb/releases
- Confidence: verified; design-critical: yes

### 2. Homebrew ships rocksdb 11.8.1 with an arm64 (Apple Silicon) bottle; it depends on gflags, lz4, snappy, zstd (no bzip2).

formulae.brew.sh/formula/rocksdb: stable 11.8.1; dependencies gflags 2.3.1, lz4 1.10.0, snappy 1.3.1, zstd 1.5.7 (cmake only for source builds); ARM64 bottles for Golden Gate, Tahoe, Sequoia, Sonoma. This is the library grocksdb would link against via cgo; rust-rocksdb builds its own copy from source.

- Source: https://formulae.brew.sh/formula/rocksdb
- Confidence: verified; design-critical: yes

### 3. Column families give per-CF options, atomic multi-CF writes in one WriteBatch, and a consistent cross-CF view via NewIterators; CFs share the WAL but have separate memtables and SST files, so dropping a CF is fast.

Wiki: "Atomic writes across Column Families are supported... Write({cf1, key1, value1}, {cf2, key2, value2})"; "Consistent view of the database across Column Families" through DB::NewIterators(); options split into ColumnFamilyOptions / DBOptions / Options; CFs "share write-ahead logs" but "maintain separate memtables and SST table files"; CreateColumnFamily/DropColumnFamily are on-the-fly and "reasonably fast"; DB::ListColumnFamilies enumerates names.

- Source: https://github.com/facebook/rocksdb/wiki/Column-Families
- Confidence: verified; design-critical: yes

### 4. atomic_flush is only needed when the WAL is disabled; with the WAL on, recovery is already consistent across CFs.

Wiki: with atomic_flush "either all or no memtables of the column families of interest are persisted"; "it is not necessary to use the Atomic flush option if WAL is always enabled. When WAL is enabled, a single WAL file is used to capture writes to all column families; hence, the recovered database... is guaranteed to be consistent across all column families." Cannot be changed at runtime.

- Source: https://github.com/facebook/rocksdb/wiki/Atomic-flush
- Confidence: verified; design-critical: no

### 5. WriteBatch is the atomic unit; default writes are not fsynced, so a machine crash (not a process crash) can lose the last few updates; WriteOptions.sync=true or a periodic DB::SyncWAL() closes that gap.

Basic Operations: "The WriteBatch holds a sequence of edits to be made to the database, and these edits within the batch are applied in order"; "The sync flag can be turned on for a particular write to make the write operation not return until the data being written has been pushed all the way to persistent storage"; "the downside of non-sync writes is that a crash of the machine may cause the last few updates to be lost" (process crashes alone do not); hybrid pattern of non-sync writes plus DB::SyncWAL() from another thread. A DB object is thread-safe; Iterator and WriteBatch objects need external synchronization.

- Source: https://github.com/facebook/rocksdb/wiki/Basic-Operations
- Confidence: verified; design-critical: yes

### 6. TransactionDB (pessimistic) locks keys at write time with a striped per-CF lock table (num_stripes=16), 1000 ms default lock timeouts, WRITE_COMMITTED policy by default, and still behaves as a plain DB for non-transactional ops; it is the better fit for contended workloads.

transaction_db.h: max_num_locks default -1; num_stripes default 16 ("dividing the lock table (per column family) into more sub-tables, each with their own separate mutex"); transaction_lock_timeout default 1000 ms; default_lock_timeout (writes outside a txn) default 1000 ms; write_policy default WRITE_COMMITTED (also WRITE_PREPARED, WRITE_UNPREPARED). TransactionOptions: set_snapshot, deadlock_detect (returns Status::Busy), lock_timeout, expiration (ms, txns lasting longer fail to commit), deadlock_detect_depth 50, max_write_batch_size, skip_prepare default true. TransactionDB inherits StackableDB so Put/Get work outside transactions; GetLockStatusData/GetDeadlockInfoBuffer exist. Wiki: "A TransactionDB can be better for workloads with heavy concurrency compared to an OptimisticTransactionDB" at "a small locking overhead".

- Source: https://raw.githubusercontent.com/facebook/rocksdb/main/include/rocksdb/utilities/transaction_db.h
- Confidence: verified; design-critical: yes

### 7. OptimisticTransactionDB takes no locks and validates at commit using sequence numbers; commit fails with Busy on conflict; it fits low-contention workloads with many non-transactional writes, and its validation depends on retained memtable history.

Transactions wiki: optimistic txns "rely on doing conflict-detection at commit time to validate that no other writers have modified the keys being written"; on conflict "the commit will return an error and no keys will be written"; "for workloads that do not expect high contention"; better when "you have many non-transactional writes and few transactions"; insufficient write-buffer history causes commit failures, mitigated by max_write_buffer_size_to_maintain. optimistic_transaction_db.h: validate_policy default kValidateParallel ("Validate parallelly before commit stage, BEFORE entering the write-group"), occ_lock_buckets default 1<<20; OptimisticTransactionOptions.cmp must be set if the DB uses a non-default comparator.

- Source: https://github.com/facebook/rocksdb/wiki/Transactions
- Confidence: verified; design-critical: yes

### 8. Read-write conflict detection only covers keys read with GetForUpdate; SetSnapshot tightens conflict checks to 'unchanged since txn start'; a ReadOptions.snapshot changes only what is read, not commit validation.

Transactions wiki: default check is "that no one else has written a key after the time the key was first written in this transaction"; SetSnapshot ensures "that no one else has written a key since the start of the transaction"; "No conflict since transactions only do conflict checking for keys read using GetForUpdate()"; "Setting a Snapshot in the ReadOptions only affects the version of the data that is read. This does not have any effect on whether the transaction will be able to be committed." transaction.h: Commit returns Status::Busy (optimistic conflict) or Status::Expired; SetSnapshotOnNextOperation defers snapshot creation; PutUntracked skips conflict checking; savepoints via SetSavePoint/RollbackToSavePoint/PopSavePoint; Prepare for 2PC; GetWriteBatch returns the WriteBatchWithIndex; no DeleteRange on Transaction.

- Source: https://raw.githubusercontent.com/facebook/rocksdb/main/include/rocksdb/utilities/transaction.h
- Confidence: verified; design-critical: yes

### 9. The C API (which both bindings wrap) has no rocksdb_transaction_set_snapshot; the snapshot must be requested when the transaction begins (TransactionOptions.set_snapshot). The C API also has no transaction-level DeleteRange and no capped-prefix transform.

Literal grep of include/rocksdb/c.h (6797 lines): rocksdb_transaction_set_snapshot 0 hits, rocksdb_transaction_get_snapshot 1, rocksdb_transaction_delete_range 0, rocksdb_transactiondb_delete_range 0, rocksdb_optimistictransactiondb_delete_range 0, rocksdb_slicetransform_create_capped 0, rocksdb_slicetransform_create_fixed_prefix 1. Present: rocksdb_transaction_get_for_update, rocksdb_transaction_multi_get_for_update, rocksdb_transaction_set_savepoint, rocksdb_transaction_prepare, rocksdb_transactiondb_options_set_write_policy, rocksdb_transaction_options_set_deadlock_detect.

- Source: https://raw.githubusercontent.com/facebook/rocksdb/main/include/rocksdb/c.h
- Confidence: verified; design-critical: yes

### 10. WriteBatchWithIndex gives read-your-own-writes over an uncommitted batch (GetFromBatchAndDB, NewIteratorWithBase) but does not support DeleteRange, and Merge without a base value returns MergeInProgress.

write_batch_with_index.h: constructor (backup_index_comparator, reserved_bytes, overwrite_key, max_bytes); "DeleteRange unsupported in WriteBatchWithIndex" on every overload; with overwrite_key a Merge "is added as a new update instead of overwriting the existing one"; GetFromBatch may return MergeInProgress. Wiki: WBWI "isolates the Write Path to a local in-memory store and allows you to RYOW before data is atomically written"; it is "a key component in RocksDB's Pessimistic and Optimistic Transaction utility classes".

- Source: https://raw.githubusercontent.com/facebook/rocksdb/main/include/rocksdb/utilities/write_batch_with_index.h
- Confidence: verified; design-critical: yes

### 11. Redis MULTI/EXEC queues commands and runs them serially and atomically with no rollback; WATCH is a check-and-set that makes EXEC return nil if a watched key changed (including by expiry since 6.0.9).

redis.io transactions page: "All the commands in a transaction are serialized and executed sequentially. A request sent by another client will never be served in the middle"; queuing errors abort the transaction at EXEC (2.6.5+), runtime errors do not ("even when a command fails, all the other commands in the queue are processed"); "Redis does not support rollbacks"; WATCH: "If at least one watched key is modified before the EXEC command, the whole transaction aborts, and EXEC returns a Null reply"; modifications include "expiration or eviction"; all keys are UNWATCHed at EXEC; Redis 8.4 adds SET IFEQ/IFNE/IFDEQ/IFDNE and DELEX compare-and-set/delete.

- Source: https://redis.io/docs/latest/develop/using-commands/transactions/
- Confidence: verified; design-critical: yes

### 12. MongoDB multi-document transactions use snapshot reads, abort with a write conflict when a document they later touch was modified outside the transaction, wait 5 ms by default for locks, and expire after 60 s.

Production considerations: "By default, a transaction must have a runtime of less than one minute" (transactionLifetimeLimitSeconds); "transactions wait up to 5 milliseconds to acquire locks... If the transaction cannot acquire its required locks within the 5 milliseconds, the transaction aborts" (maxTransactionLockRequestTimeoutMillis); a non-transactional write on a document locked by a txn "waits until the transaction ends"; if an outside write modifies a document the txn later modifies, "the transaction aborts because of a write conflict"; reads inside a txn can be stale. Core page: read concern snapshot returns majority-committed snapshot data when committed with w:majority.

- Source: https://www.mongodb.com/docs/manual/core/transactions-production-consideration/
- Confidence: verified; design-critical: yes

### 13. Snapshots are cheap sequence-number markers, but every held snapshot pins overwritten versions and tombstones, and very large snapshot counts stall writes; they must be released explicitly.

Snapshot wiki: "A snapshot is associated with an internal sequence number assigned by RocksDB"; keys with seqno greater than the snapshot's are invisible; snapshots live in a linked list in DBImpl; "When there are many snapshots, this scan can significantly slow down flush/compaction to the point of causing write stalls. We've noticed problems when snapshot count is in the hundreds of thousands"; compaction must preserve the point-in-time view. Basic Operations: "Snapshots provide consistent read-only views over the entire state of the key-value store", pass via ReadOptions::snapshot, release with DB::ReleaseSnapshot(). Properties rocksdb.num-snapshots and rocksdb.oldest-snapshot-time expose the count/age.

- Source: https://github.com/facebook/rocksdb/wiki/Snapshot
- Confidence: verified; design-critical: yes

### 14. Iterators read from an implicit snapshot and pin memtables, SST files and data blocks until destroyed; RocksDB says keep them short-lived or call Refresh(); ReadOptions bounds, prefix flags, readahead_size, async_io, fill_cache, pin_data, tailing tune scans.

Iterator wiki: "If it is nullptr, the iterator will read from an implicit snapshot as of the time the iterator is created"; "Even if some memtables and SST files are removed after flush or compaction, they are still preserved if an iterator pinned them"; blocks are "pinned in block cache, or in the heap"; "the best use of iterator is to keep it short-lived"; "Since release 5.7, you can call an API Iterator::Refresh()". options.h ReadOptions: snapshot, iterate_lower_bound/iterate_upper_bound (upper bound exclusive), readahead_size, fill_cache ("if false, data bypasses cache"), tailing, total_order_seek, auto_prefix_mode, prefix_same_as_start, pin_data, async_io, verify_checksums, read_tier.

- Source: https://github.com/facebook/rocksdb/wiki/Iterator
- Confidence: verified; design-critical: yes

### 15. Prefix bloom filters need an order-compatible prefix_extractor; manual prefix mode is unsafe if misused (undefined results, deleted keys can reappear); prefix_same_as_start or auto_prefix_mode (6.8+) are the safe ways; SeekToLast is unsupported in prefix mode.

Prefix Seek wiki: "A prefix extractor needs to be used with a comparator where keys with the same prefix are close to each other in the total order"; use NewCappedPrefixTransform/NewFixedPrefixTransform; memtable_prefix_bloom_size_ratio enables memtable prefix bloom; whole_key_filtering=false if only prefix queries; "RocksDB will not return error when it is misused and the iterating result will be undefined" ("deleted keys might show up, key ordering is not followed"); prefix_same_as_start guarantees Valid()=false when leaving the prefix; auto_prefix_mode (6.8) "will always generate the same result as total_order_seek = true, while prefix bloom filter might be used" and is "the recommended way"; "SeekToLast() is not supported well with prefix iterating"; filters help "very short iterator queries" most. advanced_options.h: memtable_prefix_bloom_size_ratio default 0.0 (disabled), size = write_buffer_size * ratio.

- Source: https://github.com/facebook/rocksdb/wiki/Prefix-Seek
- Confidence: verified; design-critical: yes

### 16. Merge is a first-class read-modify-write; one merge operator per CF; long operand chains raise read latency; max_successive_merges (default 0 = off) bounds memtable chains; compaction stops merging at snapshot boundaries.

Merge Operator wiki: "a generic Merge operation as a new first-class operation in RocksDB to capture the read-modify-write semantics"; FullMerge(V2)/PartialMerge(Multi), AssociativeMergeOperator for same-format associative operands; "a RocksDB DB object can only be passed 1 merge-operator at the time of construction" (multiplex by key prefix/operand tag); operands are applied lazily "the next time the user calls Get(), or... Compaction"; GetMergeOperands since 6.4. Implementation wiki: "if a snapshot is encountered, we must stop the merging process... we simply write out the un-merged operands"; unbounded chains risk memory "if there is a single key with MANY merge operations". advanced_options.h: max_successive_merges "Maximum number of successive merge operations on a key in the memtable", default 0.

- Source: https://github.com/facebook/rocksdb/wiki/Merge-Operator
- Confidence: verified; design-critical: yes

### 17. Compaction filters run only during compaction (never flush), can Keep/Remove/ChangeValue/RemoveAndSkipUntil/Purge, must be thread-safe if a single instance is shared (or use a factory), and since 6.0 ignore snapshots, so filtered data can vanish from a pinned snapshot.

compaction_filter.h: Decision enum kKeep, kRemove, kChangeValue, kRemoveAndSkipUntil ("Remove all key-values with key in [key, *skip_until)"), kPurge, kChangeWideColumnEntity, kUndetermined; FilterV2/FilterV3; Context has is_full_compaction, is_manual_compaction, column_family_id, input_start_level, reason; "RocksDB snapshots do not guarantee to preserve the state of the DB in the presence of CompactionFilter. Data seen from a snapshot might disappear"; single instance "may be called from different threads concurrently"; IgnoreSnapshots deprecated (always ignored); factory ShouldFilterTableFileCreation default "only applies CompactionFilter to files generated by compaction". Wiki: "Compaction filter will not be invoked during flush"; filter sees only the newest version of a key; since 6.0 "RocksDB always invoke filtering for any key, even if it knows it will make a snapshot not repeatable".

- Source: https://raw.githubusercontent.com/facebook/rocksdb/main/include/rocksdb/compaction_filter.h
- Confidence: verified; design-critical: yes

### 18. RocksDB's built-in TTL (DBWithTTL, options.ttl, periodic_compaction_seconds) is non-strict: expired entries stay readable until compaction, and DBWithTTL corrupts values if the DB is later opened with plain DB::Open. Redis-exact expiry needs a read-time check plus compaction-filter GC.

TTL wiki: "(int32_t)Timestamp(creation) is suffixed to values in Put internally"; "Get/Iterator may return expired entries(compaction not run on them yet)"; "Calling DB::Open directly to re-open a db created by this API will get corrupt values(timestamp suffixed)". advanced_options.h: ttl and periodic_compaction_seconds default 0xfffffffffffffffe (RocksDB picks a default); periodic compaction picks "files older than periodic_compaction_seconds" so cold files still pass through the filter.

- Source: https://github.com/facebook/rocksdb/wiki/Time-to-Live
- Confidence: verified; design-critical: yes

### 19. DeleteRange atomically deletes [start, end) with a single range tombstone; reads stay competitive, but many range tombstones cost a linear scan on reads and can inflate compactions.

DeleteRange wiki: scan-and-delete "prevents it from being an atomic operation, and makes it unsuitable for any performance-sensitive write path"; DeleteRange "creates a range tombstone represented as a single kv"; "Read performance with range tombstones is competitive to the scan-and-delete pattern"; start inclusive, end exclusive. Implementation wiki: without fragmentation "we would have to scan through all of them in order to find a potentially covering tombstone... becomes very costly as the number of range tombstones grows"; a compaction input with a range tombstone pulls in all files sharing it.

- Source: https://github.com/facebook/rocksdb/wiki/DeleteRange
- Confidence: verified; design-critical: yes

### 20. SST ingestion is the bulk-load path: SstFileWriter needs strictly increasing keys under the DB's exact comparator; IngestExternalFile links/copies the file, briefly blocks writes, flushes on memtable overlap, and picks the lowest non-overlapping level; ingest_behind skips duplicate keys.

Wiki: "inserted keys must be strictly increasing (based on options.comparator)"; "The Comparator that is passed to the SstFileWriter must be exactly the same as the Comparator used in the DB"; ingestion will "block (not skip) writes to the DB" and "if file key range overlap with memtable key range, flush memtable"; write_global_seqno (5.16+) can be false so MANIFEST supplies the seqno; ingest_behind skips duplicates; 9.5+ can ingest DB-generated files. options.h IngestExternalFileOptions defaults: move_files false, link_files false, failed_move_fall_back_to_copy true, snapshot_consistency true, allow_global_seqno true, allow_blocking_flush true, ingest_behind false (needs allow_ingest_behind), write_global_seqno true, verify_checksums_before_ingest true, fail_if_not_bottommost_level false, fill_cache true.

- Source: https://github.com/facebook/rocksdb/wiki/Creating-and-Ingesting-SST-files
- Confidence: verified; design-critical: no

### 21. Checkpoints create an openable, consistent copy in a directory using hard links on the same filesystem; log_size_for_flush=0 (default) always flushes first; the API returns the checkpoint's sequence number and can export a single CF's SSTs.

checkpoint.h: CreateCheckpoint(checkpoint_dir, log_size_for_flush, sequence_number_ptr): "if the total log file size is equal or larger than this value, then a flush is triggered for all the column families... The default value is 0, which means flush is always triggered"; sequence_number_ptr receives a seqno guaranteed to be in the checkpoint; ExportColumnFamily "Exports all live SST files of a specified Column Family onto export_dir", hard-linking when possible, "Always triggers a flush". Wiki: same filesystem "the SST files will be hard-linked", otherwise copied; multi-CF DBs copy WAL for the checkpoint window; result "can be used as a read-only copy of the DB or can be opened as a standalone DB".

- Source: https://raw.githubusercontent.com/facebook/rocksdb/main/include/rocksdb/utilities/checkpoint.h
- Confidence: verified; design-critical: yes

### 22. BackupEngine does incremental backups (immutable SSTs shared by checksum), optional flush_before_backup, checksum-verified restore, VerifyBackup and PurgeOldBackups; it adds checksums, rate limiting and remote Env support that Checkpoint lacks.

Backup wiki: CreateNewBackup copies only new data because "table files are immutable and filenames unique"; RestoreDBFromBackup verifies "checksum is calculated for any restored file and compared against the one stored during the backup time"; share_files_with_checksum names files by size+checksum in shared_checksum; flush_before_backup=true flushes first, false includes WAL ("Backup will be consistent with current state of the database regardless"); layout meta/private/shared_checksum; PurgeOldBackups(N); VerifyBackup checks sizes (optionally checksums).

- Source: https://github.com/facebook/rocksdb/wiki/How-to-backup-RocksDB
- Confidence: verified; design-critical: no

### 23. HyperClockCache is now the recommended block cache (cache.h), block-cache only, with estimated_entry_charge=0 as the default auto mode; the Block-Cache wiki page lags and still calls it advanced-only. Default LRU cache is 32 MB since 8.2.

cache.h: "HYPERCLOCKCACHE IS NOW GENERALLY RECOMMENDED OVER LRUCACHE"; "lock-free Cache alternative" with "much improved CPU efficiency vs. LRUCache under high parallel load or high contention"; "Not a general Cache implementation: can only be used for BlockBasedTableOptions::block_cache"; estimated_entry_charge==0 (default, recommended) uses "a dynamically-growing table"; NewLRUCache is deprecated in favour of LRUCacheOptions::MakeSharedCache; CompressedSecondaryCacheOptions is EXPERIMENTAL. Block-Cache wiki: default "LRU-based block cache implementation with 32MB capacity" (8.2+, previously 8MB); strict_capacity_limit rejects inserts and fails reads when full; cache_index_and_filter_blocks, cache_index_and_filter_blocks_with_high_priority, pin_l0_filter_and_index_blocks_in_cache; HCC "currently for advanced usage only" (stale relative to cache.h).

- Source: https://raw.githubusercontent.com/facebook/rocksdb/main/include/rocksdb/cache.h
- Confidence: verified; design-critical: yes

### 24. Analytical scans can bypass the block cache with ReadOptions.fill_cache=false, and each CF can carry its own BlockBasedTableOptions (so a separate cache object per CF is possible), while the setup guide recommends one shared cache and a row_cache exists for hot point reads.

options.h ReadOptions.fill_cache: "If true, read data fills the cache; if false, data bypasses cache"; DBOptions.row_cache: "A global cache for table-level rows. Used to speed up Get() queries". Column-Families wiki: "Ability to configure different Column Families independently" (ColumnFamilyOptions carries table_factory). Setup wiki: the same cache object "must be applied across all column families and databases in a process" for a shared budget, ~1/3 of memory.

- Source: https://raw.githubusercontent.com/facebook/rocksdb/main/include/rocksdb/options.h
- Confidence: verified; design-critical: yes

### 25. The generic rate limiter throttles flush and compaction I/O only (not WAL writes), defaults to 100 ms refill / fairness 10 / kWritesOnly, and auto_tuned mode adjusts within [rate/20, rate].

rate_limiter.h: NewGenericRateLimiter(rate_bytes_per_sec, refill_period_us=100*1000, fairness=10, mode=kWritesOnly, auto_tuned=false, single_burst_bytes=0); mode "Indicates which types of operations count against the limit" (kReadsOnly, kWritesOnly, kAllIo); auto_tuned "Enables dynamic adjustment of rate limit within the range [rate_bytes_per_sec / 20, rate_bytes_per_sec], according to the recent demand for background I/O". Wiki: it does not limit "anything other than flush and compaction, e.g. write to WAL"; share one limiter object across DBs.

- Source: https://raw.githubusercontent.com/facebook/rocksdb/main/include/rocksdb/rate_limiter.h
- Confidence: verified; design-critical: no

### 26. WAL durability knobs: every write goes to memtable and WAL; WAL files are deleted once all CFs flush past them; disableWAL, manual_wal_flush+FlushWAL, wal_dir, max_total_wal_size, WAL_ttl_seconds/WAL_size_limit_MB archival; wal_recovery_mode defaults to kPointInTimeRecovery (6.6+).

WAL wiki: "RocksDB guarantees process crash consistency by flushing the WAL after every user write"; a WAL is deleted "when all column families have flushed beyond the largest sequence number contained in the WAL"; WriteOptions::disableWAL "useful when users rely on other logging or don't care about data loss"; manual_wal_flush with FlushWAL; max_total_wal_size forces CF flushes; wal_filter for recovery. Recovery modes wiki: kTolerateCorruptedTailRecords ignores tail errors; kAbsoluteConsistency treats any IO error as corruption; kPointInTimeRecovery (default since 6.6) stops replay at the first error; kSkipAnyCorruptedRecords ignores all read errors.

- Source: https://github.com/facebook/rocksdb/wiki/Write-Ahead-Log-(WAL)
- Confidence: verified; design-critical: yes

### 27. DB properties are cheap current-state reads with fixed string names (rocksdb.stats, rocksdb.estimate-num-keys, rocksdb.cur-size-all-mem-tables, rocksdb.num-snapshots, rocksdb.block-cache-usage, ...), while Statistics accumulate history at a 5-10% cost.

db.h DB::Properties: kStats "rocksdb.stats", kSSTables "rocksdb.sstables", kCFStats "rocksdb.cfstats", kDBStats "rocksdb.dbstats", kLevelStats "rocksdb.levelstats", kNumFilesAtLevelPrefix "rocksdb.num-files-at-level<N>", kEstimateNumKeys "rocksdb.estimate-num-keys", kCurSizeActiveMemTable, kCurSizeAllMemTables "rocksdb.cur-size-all-mem-tables", kSizeAllMemTables, kNumSnapshots, kOldestSnapshotTime, kNumLiveVersions, kEstimateLiveDataSize, kTotalSstFilesSize, kLiveSstFilesSize, kEstimatePendingCompactionBytes, kActualDelayedWriteRate, kIsWriteStopped, kCompactionPending, kNumRunningCompactions, kNumRunningFlushes, kBackgroundErrors, kBlockCacheCapacity/Usage/PinnedUsage, kEstimateTableReadersMem, kOptionsStatistics, kAggregatedTableProperties, kBlockCacheEntryStats; accessors GetProperty, GetIntProperty, GetMapProperty, GetAggregatedIntProperty. Statistics wiki: "We usually observe an overhead of 5%-10%"; levels kAll... kExceptHistogramOrTimers; tickers never decrease; "statistics accumulate stats for history, while DB properties report current state of the database".

- Source: https://raw.githubusercontent.com/facebook/rocksdb/main/include/rocksdb/db.h
- Confidence: verified; design-critical: yes

### 28. CompactRange is a blocking manual compaction with options for exclusivity, level placement, bottommost handling, write-stall permission, subcompactions and cancellation; its main uses are read optimization, forcing a compaction filter, and option migration.

Manual Compaction wiki: "DB::CompactRange waits while compaction is performed on the background threads"; for universal/FIFO "the begin and end arguments are ignored and all files are compacted"; exclusive_manual_compaction default true ("no other compaction will run"); change_level/target_level; bottommost_level_compaction kSkip/kIfHaveCompactionFilter/force; allow_write_stall; max_subcompactions; canceled; CompactFiles for explicit inputs; concurrent callers wait. options.h CompactRangeOptions defaults: exclusive_manual_compaction true, change_level false, target_level -1, allow_write_stall false, max_subcompactions 0, canceled false; FlushOptions wait true, allow_write_stall false.

- Source: https://github.com/facebook/rocksdb/wiki/Manual-Compaction
- Confidence: verified; design-critical: no

### 29. A Comparator must be thread-safe and its Name() is persisted and checked on open; changing ordering requires a new name; built-ins are BytewiseComparator, ReverseBytewiseComparator and their U64 timestamp variants. In Go and Rust a custom comparator is a per-comparison callback into the host language.

comparator.h: "A Comparator implementation must be thread-safe since rocksdb may invoke its methods concurrently from multiple threads"; "The client of this package should switch to a new name whenever the comparator implementation changes in a way that will cause the relative ordering of any two keys to change"; Compare, Equal, FindShortestSeparator, FindShortSuccessor, timestamp_size. Basic Operations: "change the name if and only if the new key format and comparison function are incompatible". grocksdb comparator.go: NewComparator(name string, compare func(a, b []byte) int) dispatches through exported C callbacks gorocksdb_comparator_compare(idx, ...). rust-rocksdb Options::set_comparator / set_comparator_with_ts take a Rust closure.

- Source: https://raw.githubusercontent.com/facebook/rocksdb/main/include/rocksdb/comparator.h
- Confidence: verified; design-critical: yes

### 30. RocksDB's SSD guidance is to start from defaults plus a short list of settings: shared block cache ~1/3 of memory, 10-bit bloom, cache_index_and_filter_blocks + pin_l0 blocks, 16 KB blocks, bytes_per_sync 1 MB, max_background_jobs 6, dynamic level bytes, LZ4 upper levels and ZSTD bottommost, latest format_version, and stop tuning once it is good enough.

Setup wiki: block cache "about 1/3 of your total memory budget"; bloom "A good value is 10, which yields a filter with ~1% false positive rate" plus optimize_filters_for_memory; LZ4/Snappy for first n-1 levels, ZSTD/Zlib last level; cf_options.level_compaction_dynamic_level_bytes = true; max_background_jobs = 6; bytes_per_sync = 1048576; compaction_pri = kMinOverlappingRatio; block_size = 16 KB; cache_index_and_filter_blocks = true; pin_l0_filter_and_index_blocks_in_cache = true; format_version latest ("Default format version usually lags behind the recommended version"); default write_buffer_size 64 MB; sst_file_manager to cap deletion rate on flash with discard; "If the performance... is good enough for you, we don't recommend that you tune it further". Tuning Guide: "If you have a normal application on SSD, we don't recommend you to fine tune RocksDB at all"; write/read/space amplification definitions; target_file_size_base ~ max_bytes_for_level_base/10; max_open_files=-1 "avoids expensive table cache calls". advanced_options.h defaults: level_compaction_dynamic_level_bytes true, compaction_pri kMinOverlappingRatio, num_levels 7, level0_slowdown 20, level0_stop 36, target_file_size_base 64 MB.

- Source: https://github.com/facebook/rocksdb/wiki/Setup-Options-and-Basic-Tuning
- Confidence: verified; design-critical: no

### 31. grocksdb is at v1.11.1, wraps the RocksDB C API via cgo against a system librocksdb, and advertises support for RocksDB 11.x; its v1.11.0 release note says it was adapted to RocksDB 11.0.4.

pkg.go.dev: github.com/linxGnu/grocksdb v1.11.1 (versions tab: v1.11.1 and v1.11.0 dated 7 Sep 2026, v1.10.8 Apr 2026). README: "Support 11.x, 10.x, 9.x, 8.x, 7.x, 6.x"; requires librocksdb, libsnappy, libz, liblz4, libzstd (libbz2 optional); build with CGO_CFLAGS=-I.../include and CGO_LDFLAGS="-L... -lrocksdb -lstdc++ -lm -lz -lsnappy -llz4 -lzstd"; default flags "-lrocksdb -pthread -lstdc++ -ldl -lm -lzstd -llz4 -lz -lsnappy"; tags grocksdb_clean_link and grocksdb_no_link. Releases page: v1.11.0 "Adapt RocksDB 11.0.4". Repo has testing_darwin_arm64.go, so macOS arm64 is a tested target.

- Source: https://pkg.go.dev/github.com/linxGnu/grocksdb
- Confidence: verified; design-critical: yes

### 32. grocksdb exposes almost the whole surface NilDB needs, verified in source: CFs, WriteBatch/WriteBatchWI, TransactionDB and OptimisticTransactionDB (each with NewCheckpoint), Transaction GetForUpdate/SetSavePoint/Prepare, snapshots, iterator ReadOptions, HyperClockCache, TTL/periodic compaction, atomic flush, manual WAL flush, rate limiter with mode+auto-tune, Checkpoint with ExportColumnFamily, BackupEngine, SSTFileWriter (incl. DeleteRange), ingest options, properties, CompactRange, GetUpdatesSince, secondary/read-only opens, PerfContext.

db.go: OpenDbColumnFamilies, OpenDbForReadOnly, OpenDbAsSecondary, OpenDbWithTTL, GetPinnedCF, MultiGetCF, DeleteRangeCF, NewIterators, NewSnapshot/ReleaseSnapshot, GetProperty/GetPropertyCF/GetIntProperty/GetIntPropertyCF, GetApproximateSizesCF, CompactRange/CompactRangeCF (+Opt variants), Flush/FlushCF/FlushCFs, FlushWAL(sync), SetOptions, IngestExternalFileCF, GetLatestSequenceNumber, GetUpdatesSince, GetLiveFilesMetaData, CreateColumnFamily(ies)/DropColumnFamily/ListColumnFamilies, CancelAllBackgroundWork, SuggestCompactRange, TryCatchUpWithPrimary. write_batch_wi.go: NewWriteBatchWI(reservedBytes, overwriteKeys), PutCF/MergeCF/DeleteCF/DeleteRangeCF, GetFromDBWithCF, GetPinnableFromDBWithCF, NewIteratorWithBaseCF(ReadOpts), SetSavePoint/RollbackToSavePoint. transactiondb.go: OpenTransactionDbColumnFamilies, TransactionBegin(writeOpts, txnOpts, oldTxn), NewSnapshot, NewCheckpoint, FlushWAL, GetProperty, GetBaseDB. optimistic_transaction_db.go: OpenOptimisticTransactionDbColumnFamilies, TransactionBegin, NewCheckpoint, GetBaseDB. transaction.go: Commit/Rollback/Prepare, GetForUpdateWithCF, GetPinnedForUpdateWithCF, PutCF/MergeCF/DeleteCF, NewIteratorCF, SetSavePoint/RollbackToSavePoint, GetSnapshot, SetName, GetWriteBatchWI, RebuildFromWriteBatch(WI). options_transaction.go: TransactionOptions SetSetSnapshot/SetDeadlockDetect/SetLockTimeout/SetExpiration/SetDeadlockDetectDepth/SetMaxWriteBatchSize/SetSkipPrepare; OptimisticTransactionOptions SetSetSnapshot. options_transactiondb.go: SetMaxNumLocks/SetNumStripes/SetTransactionLockTimeout/SetDefaultLockTimeout. options_read.go: SetSnapshot, SetIterateUpperBound/LowerBound, SetPrefixSameAsStart, SetTotalOrderSeek, SetReadaheadSize, SetFillCache, SetAsyncIO, SetTailing, SetPinData, SetReadTier, SetDeadline, SetIOTimeout, SetAutoReadaheadSize. options.go (grep): SetAtomicFlush, SetTTL, SetPeriodicCompactionSeconds, SetMaxSuccessiveMerges, SetManualWALFlush, SetWALRecoveryMode, SetMaxBackgroundJobs, SetWALBytesPerSync, SetRateLimiter, EnableStatistics/SetStatisticsLevel/GetStatisticsString, SetStatsDumpPeriodSec, SetLevelCompactionDynamicLevelBytes, SetBottommostCompression, SetMemTablePrefixBloomSizeRatio, SetMemtableWholeKeyFiltering, SetCompactionFilter, SetMergeOperator, SetUint64AddMergeOperator, SetComparator, SetRowCache, EnableBlobFiles/SetMinBlobSize/SetBlobCache, SetDbWriteBufferSize/SetWriteBufferManager, SetMaxWriteBufferSizeToMaintain, SetUnorderedWrite, SetEnablePipelinedWrite, SetMaxSubcompactions, SetAllowIngestBehind, SetCreateIfMissingColumnFamilies, SetMaxTotalWalSize, SetWalDir. cache.go: NewLRUCache, NewHyperClockCache(capacity, estimatedEntryCharge), Cache.GetUsage/GetPinnedUsage/SetCapacity. ratelimiter.go: NewRateLimiter, NewAutoTunedRateLimiter, NewGenericRateLimiter(rate, refill, fairness, mode, autoTuned). checkpoint.go: CreateCheckpoint(dir, logSizeForFlush), ExportColumnFamily. backup.go: OpenBackupEngine(WithOpt), CreateNewBackup(Flush), RestoreDBFromLatestBackup/RestoreDBFromBackup, PurgeOldBackups, VerifyBackup, GetInfo. sst_file_writer.go: NewSSTFileWriter(WithComparator), Put/PutWithTS/Merge/Delete/DeleteRange/Finish. options_ingest.go: SetMoveFiles, SetSnapshotConsistency, SetAllowGlobalSeqNo, SetAllowBlockingFlush, SetIngestionBehind, SetFailIfNotBottommostLevel. merge_operator.go: MergeOperator{FullMerge, Name} + optional PartialMerger/MultiMerger. compaction_filter.go: CompactionFilter{Filter(level, key, val) (remove, newVal), Name, SetIgnoreSnapshots}.

- Source: https://raw.githubusercontent.com/linxGnu/grocksdb/master/db.go
- Confidence: verified; design-critical: yes

### 33. grocksdb gaps that affect the design: no ReadOptions.SetAutoPrefixMode, no SetCompactionFilterFactory (single shared filter must be thread-safe), no capped prefix transform, no Transaction.SetSnapshot/GetID/PopSavePoint/DeleteRange, no SetTwoWriteQueues/SetAllow2pc; WriteBatchWI.DeleteRange exists in Go but the C++ WBWI rejects DeleteRange.

options_read.go has no SetAutoPrefixMode (c.h has rocksdb_readoptions_set_auto_prefix_mode). options.go grep: SetCompactionFilterFactory 0, SetTwoWriteQueues 0, SetAllow2pc 0, SetSstFileManager 0 (c.h has rocksdb_options_set_compaction_filter_factory, rocksdb_options_set_two_write_queues, rocksdb_options_set_allow_2pc, rocksdb_options_set_sst_file_manager). slice_transform.go: only NewFixedPrefixTransform and NewNoopPrefixTransform (c.h has no capped constructor). transaction.go lacks SetSnapshot, ClearSnapshot, GetID, PopSavePoint, MultiGetForUpdate, SingleDelete, DeleteRange. write_batch_wi.go declares DeleteRange/DeleteRangeCF, but write_batch_with_index.h says "DeleteRange unsupported in WriteBatchWithIndex".

- Source: https://raw.githubusercontent.com/linxGnu/grocksdb/master/options_read.go
- Confidence: verified; design-critical: yes

### 34. rust-rocksdb 0.25.0 (2026-08-16) bundles RocksDB 11.8.1 built from source, needs Rust 1.88 and clang/LLVM (bindgen), and offers lz4/zstd/snappy/zlib/bzip2/jemalloc/io-uring/multi-threaded-cf/lto features.

CHANGELOG.md 0.25.0: "Update to RocksDB 11.8.1" (after 10.7.5), plus set_cf_paths, checkpoint on TransactionDB, delete_range in SstFileWriter, custom logging callbacks, ReadOptions deadline/io-timeout setters; 0.24.0 (2025-08-10) tracked RocksDB 9.10.0 through 10.4.2; 0.23.0 added delete_range for OptimisticTransactionDB and user-defined timestamps. README: MSRV 1.88.0; features snappy, lz4, zstd, zlib, bzip2, multi-threaded-cf, lto, mt_static, bindgen-static/bindgen-runtime, jemalloc, io-uring, valgrind, serde1; "Clang and LLVM" required. docs.rs index shows crate 0.25.0, 29.46% documented.

- Source: https://raw.githubusercontent.com/rust-rocksdb/rust-rocksdb/master/CHANGELOG.md
- Confidence: verified; design-critical: yes

### 35. rust-rocksdb covers the same core surface (CFs, delete_range_cf, prefix/full/raw iterators, snapshots, properties, compact_range, flush_wal, get_updates_since, set_options_cf, ingest, TransactionDB/OptimisticTransactionDB, Transaction get_for_update/savepoints/prepare, checkpoint, backup, SstFileWriter incl. delete_range, HyperClockCache, compaction filter + factory, merge operators, comparator, ttl/periodic compaction, atomic flush, manual WAL flush, rate limiter incl. auto-tuned, write buffer manager, blob files) but has no standalone WriteBatchWithIndex, no set_auto_prefix_mode, no ExportColumnFamily, and no set_allow_2pc/set_two_write_queues/set_sst_file_manager.

docs.rs DBCommon: open_cf(_descriptors), open_cf_for_read_only, open_as_secondary, list_cf, create_cf, drop_cf, cf_handle, get_pinned, multi_get_cf, batched_multi_get_cf, delete_range_cf, write(_opt), iterator_cf, raw_iterator_cf, prefix_iterator_cf, full_iterator, snapshot, property_value(_cf), property_int_value(_cf), compact_range(_cf), flush(_cf), flush_wal(sync), latest_sequence_number, get_updates_since, set_options(_cf), live_files, ingest_external_file(_cf), get_approximate_sizes(_cf), cancel_all_background_work, try_catch_up_with_primary. ReadOptions grep of docs page: set_snapshot, set_iterate_upper_bound/lower_bound/range, set_prefix_same_as_start, set_total_order_seek, set_readahead_size, fill_cache, set_async_io, set_tailing, set_pin_data present; set_auto_prefix_mode 0 hits. Options grep: set_atomic_flush, set_ttl, set_periodic_compaction_seconds, set_max_successive_merges, set_manual_wal_flush, set_wal_recovery_mode, set_compaction_filter_factory, set_merge_operator_associative, set_comparator, set_ratelimiter, set_auto_tuned_ratelimiter, set_max_background_jobs, set_row_cache, set_enable_blob_files, set_min_blob_size, set_max_write_buffer_size_to_maintain, set_memtable_prefix_bloom_ratio, set_unordered_write, set_enable_pipelined_write, set_bottommost_compression_type, set_level_compaction_dynamic_level_bytes, set_write_buffer_manager, set_db_write_buffer_size, enable_statistics, set_statistics_level, set_stats_dump_period_sec present; set_allow_2pc, set_two_write_queues, set_sst_file_manager 0 hits. Transaction: commit/rollback/prepare, set_savepoint/rollback_to_savepoint, snapshot(), get_for_update(_cf)(key, exclusive), get_pinned_for_update, put/merge/delete(_cf), iterator/prefix_iterator/full_iterator, get_writebatch, rebuild_from_writebatch; no delete_range. TransactionDBOptions: set_max_num_locks, set_num_stripes, set_txn_lock_timeout, set_default_lock_timeout; TransactionOptions: set_snapshot, set_deadlock_detect, set_lock_timeout, set_expiration, set_deadlock_detect_depth, set_max_write_batch_size, set_skip_prepare. OptimisticTransactionDB is DBCommon<T, OptimisticTransactionDBInner> with transaction(_opt), write, delete_range_cf. WriteBatchWithTransaction<false> has delete_range(_cf); <true> (transaction batches) does not. Cache::new_hyper_clock_cache(capacity, estimated_entry_charge) quotes "HyperClockCache is now generally recommended over LRUCache". BlockBasedOptions: set_block_cache, set_bloom_filter, set_ribbon_filter, set_cache_index_and_filter_blocks, set_pin_l0_filter_and_index_blocks_in_cache, set_whole_key_filtering, set_format_version, set_optimize_filters_for_memory. checkpoint::Checkpoint::create_checkpoint(path). backup::BackupEngine: open, create_new_backup(_flush), restore_from_latest_backup/restore_from_backup, purge_old_backups, get_backup_info, verify_backup. SstFileWriter: put/merge/delete/delete_range/finish. IngestExternalFileOptions: set_move_files, set_snapshot_consistency, set_allow_global_seqno, set_allow_blocking_flush, set_ingest_behind. properties module constants match db.h strings. compaction_filter Decision Keep/Remove/Change. SliceTransform: create, create_fixed_prefix, create_noop (no capped). docs.rs struct.WriteBatchWithIndex.html returns 404 and the crate index has no WriteBatchWithIndex.

- Source: https://docs.rs/rocksdb/latest/rocksdb/struct.DBCommon.html
- Confidence: verified; design-critical: yes

### 36. The Redis convention for non-core commands is MODULE.COMMAND, and INFO is organised into named sections of field:value lines that modules may extend (Redis 6+), which is the natural home for a curated subset of RocksDB properties.

Redis modules intro: "it is a good idea for modules to call commands with the name of the module followed by a dot, and finally the command name, like in the case of HELLOWORLD.RAND"; commands register with flags such as "fast random" (also write, readonly, admin). INFO page: sections server, clients, memory, persistence, stats, replication, cpu, commandstats, latencystats, cluster, modules, keyspace, errorstats, all, default, everything; "All the properties are in the form of field:value terminated by \r\n"; "Starting with Redis 6, modules can inject their information into the INFO command. These are excluded by default even when the all argument is provided... you must use either the modules argument or everything."

- Source: https://redis.io/docs/latest/develop/reference/modules/
- Confidence: verified; design-critical: no

### 37. WriteOptions offers no_slowdown (fail fast with Incomplete during stalls) and low_pri (deprioritise when compaction lags), which map cleanly to Redis-style back-pressure errors.

docs.rs WriteOptions (mirroring options.h): set_sync "If true, the write will be flushed from the operating system buffer cache before the write is considered complete"; disable_wal "the write may got lost after a crash"; set_no_slowdown "If true and we need to wait or sleep for the write request, fails immediately with Status::Incomplete()"; set_low_pri "this write request is of lower priority if compaction is behind". options.h DBOptions also documents enable_pipelined_write (separate WAL/memtable queues), unordered_write (trades snapshot immutability for throughput), two_write_queues, allow_2pc.

- Source: https://docs.rs/rocksdb/latest/rocksdb/struct.WriteOptions.html
- Confidence: verified; design-critical: no
