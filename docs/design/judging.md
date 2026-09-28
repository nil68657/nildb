# Design panel scorecards

Exported on 2026-09-27 from Claude Code run `wf_b09e8159-aff`. Three judges were planned. `judge:delivery` (buildability and HTAP credibility) stopped at the session limit twice and scored nothing, so each total below is out of 60 per judge and 120 across the two that finished.

| Design | judge:rocksdb | judge:compat | Total |
|---|---|---|---|
| mongo-parity | 45 | 48 | 93 |
| rocksdb-native | 44 | 45 | 89 |
| htap-first | 44 | 43 | 87 |

## judge:rocksdb: RocksDB correctness and performance (agent `a3f607dd6062991b6`, winner `mongo-parity`)

### rocksdb-native: 44/60

rocksdb_correctness 7 | redis_mongo_fidelity 7 | geo_fidelity 8 | htap_credibility 7 | buildability_v1 7 | testability 8

The storage layer is the best-reasoned of the three under this lens: plain OpenDbColumnFamilies with a WriteBatch per autocommit command and WBWI only inside EXEC (matching the WBWI digest: no DeleteRange, so FLUSHDB/DOC.DROP are NoMulti and Txn.DeleteRange returns ErrRangeInIndexedTxn); Merge confined to catalog counters with the correct argument that INCR must return a value and refuse non-integers without writing; a version-first 9-byte prefix so NewFixedPrefixTransform(9) plus memtable_prefix_bloom_size_ratio serve HGETALL/SMEMBERS/ZRANGE while meta stays total-order for SCAN; two HyperClock caches with fill_cache=false on analytics; leases capped at 64 with a 60 s janitor and rocksdb.num-snapshots surfaced; MultiGet in batches of 64 for index fan-out; and the only accurate statement of what manual_wal_flush plus a 1 s FlushWAL means on a process crash. Checked against grocksdb v1.11.1: SetAsyncIO, SetReadaheadSize, NewRibbonHybridFilterPolicy(10,1), NewHyperClockCache(cap,0), SetPeriodicCompactionSeconds, ExportColumnFamily, NewIteratorWithBaseCF and the zero hits for rate_limiter_priority in c.h all hold. What pulls the score down are three races written into the text (RENAME keeping the version under a (db,version)-keyed filter verdict cache, lazy-expiry deletes issued from unlocked reads, unique checks locked on _id only), a missing TotalOrderSeek knob on CFs that all carry prefix extractors, no WriteBufferManager across ten CFs, a backfill consistency claim that does not hold, and three API names that do not exist. Each fix is local; none changes the architecture. Redis and Mongo surfaces are broad, the numeric keystring is exact across int64/double and keeps Decimal128 out of indexes, geo is split correctly with redis-server transcripts and a callback-driven kNN, and the HTAP acceptance target (<=20% loss) is the only realistic single-node number among the three. Under the RocksDB lens alone this storage layer is the one to keep; it loses the total by one point on Mongo depth and testing breadth.

Flaws:

- §5 subFilter + §6 RENAME: the verdict cache is keyed by the 9-byte (db, version) prefix, but RENAME copies the metadata with the same version and rewrites elements under the new user key; old-key and new-key elements share that prefix and sit adjacent in sort order, so a 'drop' verdict computed from an old-key element (its metadata is gone) is applied to the renamed collection's live elements. Mint a new version on RENAME (the O(n) rewrite is already paid) or key the cache by the full |db|version|klen|key| prefix.
- §4 'Single-key reads lock nothing' + §5 'Commands with a Txn delete the metadata key': GET on an expired key issues an unlocked Delete on meta; a concurrent SET that recreated the key under its lock can commit between the read and the Delete, and the Delete removes the fresh value. Route lazy-expiry deletes through the key lock (htap-first §5) or an expiry worker that re-checks under the lock (mongo-parity §5).
- §3 Secondary indexes / §4 DOC commands: the unique check is a Get before Put under a lock on (coll_id, ks(_id)); two inserts with different _ids and the same unique value both see 'absent' and both write. Lock the unique value as mongo-parity does with (uniq, idx_id, enc(values)).
- §1 IterOpts has no TotalOrderSeek field, yet sub, zscore, doc, idx, geo, col and ttl all carry fixed prefix extractors; ROCKS.SCAN cf FROM hex (§6) and any Iter whose [lo,hi) spans prefixes runs in prefix mode, which the Prefix-Seek digest marks undefined ('deleted keys can reappear').
- §4 EXEC checks the WATCH dirty flag before taking the lock, and the lock set is the queued KeySpec keys only; a write to a watched key between the check and Commit is not detected. Lock queued ∪ watched keys, then check (htap-first §4).
- §7 DOC.COLUMNAR backfill 'the result is consistent' is false: a backfill Put of the snapshot's old value lands after a concurrent DOC.UPDATE's new col value, or re-creates an entry for a document deleted after the snapshot, leaving stale or orphan columnar rows that the k-way merge emits. Backfill batches must re-read the live row under the document lock or skip _ids that already have an entry.
- §2: ten CFs with 64/32/16/4 MiB write buffers and no NewWriteBufferManager; memtable memory is unbounded across CFs (mongo-parity caps it at 256 MiB with allowStall).
- §2/§4/§6 API names: NewRateLimiterWithMode does not exist in grocksdb v1.11.1 (NewGenericRateLimiter(rate, refill, fairness, mode, autoTuned) does); GetFromDBCF is GetFromDBWithCF; rocksdb_checkpoint_create(checkpoint, dir, log_size_for_flush, errptr) returns no sequence number, so ROCKS.CHECKPOINT cannot report the checkpoint's exact seq.
- §2/§3: the n/<coll_id> merge counter has no SetMaxSuccessiveMerges; under a 60 s lease the operands are pinned (digest: 'snapshots pin unmerged operands') and every DOC.COUNT read resolves the chain.
- §8: the geo index value holds only the kind byte, so $near must load every candidate document to compute a point distance; the geo digest recommends keeping the leaf cell or coordinates in the index value (mongo-parity and htap-first do).

Ideas to graft:

- Plain WriteBatch for autocommit, WBWI only inside EXEC, with Txn.DeleteRange returning ErrRangeInIndexedTxn under WBWI (replaces mongo-parity's WBWI-per-command Ctx).
- Merge restricted to catalog counters; INCR/APPEND/HINCRBY as read-then-Put because the reply needs the new value and the error path must not write.
- Version-first fixed prefix on sub/zscore (or htap-first's version-only 8 bytes) so NewFixedPrefixTransform plus memtable_prefix_bloom_size_ratio 0.1 accelerate HGETALL/SMEMBERS/ZRANGE; meta kept extractor-free for total-order SCAN.
- Numeric keystring classes (sortable-double below 2^53, two's-complement int64 between 2^53 and 2^63, sortable-double above) and Decimal128 rejected as an index key, replacing mongo-parity's approximate Decimal128 encoding.
- metaFilter that needs no DB lookups, and a filter race test that runs CompactRange with max_subcompactions=4 under -race.
- MultiGet on doc in batches of 64 after an idx scan (one cgo call per batch).
- Two HyperClock caches with the analytics cache on col and fill_cache=false on every analytical iterator.
- Honest everysec statement: with manual_wal_flush the WAL buffer is user-space, so a process crash loses up to 1 s; 'always' is one flag away.
- geo.Near driven by Scan/Load callbacks so the kNN driver is unit-testable without RocksDB.
- --clock-offset-ms test flag for expiry tests; redis-server-recorded GEO transcript compared byte-for-byte.
- Background columnar backfill (with the lock-per-batch fix) instead of a synchronous index build under the collection lock.
- <=20% throughput loss as the single-node HTAP acceptance bar, with the explicit note that TiDB's 10% assumes separate servers.

### mongo-parity: 45/60

rocksdb_correctness 6 | redis_mongo_fidelity 8 | geo_fidelity 9 | htap_credibility 7 | buildability_v1 6 | testability 9

This design gets the document store, geo and testing right and the RocksDB hot path wrong in two places. Right: drops are one plain WriteBatch with DeleteRangeCF plus the catalog deletes, followed by a scheduled CompactRange (grocksdb's WriteBatch has DeleteRangeCF; only WBWI refuses it); unique values are lock keys, so E11000 checks cannot race; expired keys are removed by a worker that re-locks and re-checks; NewWriteBufferManager(256 MiB, allowStall) bounds memtables across eight CFs; SetDeadline, SetAsyncIO and SetFillCache(false) on analytical iterators all exist in v1.11.1; the geo CF uses a 12-byte fixed prefix with SetTotalOrderSeek(true) for descendant ranges and SetPrefixSameAsStart(true) for ancestor probes, which is the textbook use of the Prefix-Seek rules in the digest; the 64-slot sharded filter cache respects the no-factory constraint; GetPinnedCF and KeySlice/ValueSlice are the zero-copy paths; the claim of zero hits for rocksdb_readoptions_set_rate_limiter_priority is confirmed. Wrong: INCR/INCRBY/INCRBYFLOAT/APPEND/HINCRBY as Merge operands (the operator cannot emit WRONGTYPE or the not-an-integer error, the reply needs a resolving read anyway, chains are pinned by up to 32 analytical snapshots, and max_successive_merges is never set), and a WriteBatchWI plus lazily created snapshot on every autocommit command, which taxes each HGET-class read with a skiplist index, a batch-then-DB lookup and a NewSnapshot/ReleaseSnapshot pair through the DBImpl mutex. Both choices follow suggestions in the research digest (Kvrocks Context, Merge for counters), but the digest's own hazard list applies and rocksdb-native's counter-argument is correct. Smaller: LiveSet publication order can drop freshly written index entries, the Decimal128 index approximation yields false negatives at range boundaries, the WATCH lock set omits watched keys, and the everysec claim ignores the manual_wal_flush process-crash window. On the other axes it leads: MongoDB find/insert/update reply shapes and exact error texts, positional and pipeline updates, FINDANDMODIFY, the deepest 2dsphere semantics (legacy pairs, $centerSphere, coordinates in the index value, CrossingEdgeQuery, $geoNear options, NIL.GEOGROUP), and a -tags mongod differential job against MongoDB 8.0 that no other design has. It wins the total by one point; before build, its Ctx and string type should be rebuilt on rocksdb-native's autocommit/merge/prefix decisions and htap-first's WATCH and expiry sequencing.

Flaws:

- §2/§6: INCR, INCRBY, INCRBYFLOAT, APPEND, HINCRBY, HINCRBYFLOAT as Merge operands. INCR must return the new value and must reply 'ERR value is not an integer or out of range' or WRONGTYPE without writing; a merge operator cannot return an error to the client, so a pre-read is needed and Merge buys nothing while the digest's costs apply: operand chains raise read latency, snapshots pin unmerged operands (32 leases at 60-600 s), and max_successive_merges is never set. The | flags | expire_ms | payload | header also has to be reconstructed inside the operator on a missing base. Read-then-Put (rocksdb-native §2) is simpler and exact.
- §4 Ctx runs every autocommit command on NewWriteBatchWI(0, true) and reads through GetFromDBWithCF: the C++ skiplist index is built per command and every read is batch-then-DB; with the snapshot 'created on the first iterator or the second Get', a plain HGET/SISMEMBER/ZSCORE (meta + element) also pays NewSnapshot/ReleaseSnapshot per call. Kvrocks does this, but against the stated 100k ops/s target it is the largest per-command cgo/C++ cost in any of the three designs; reserve WBWI for EXEC and take a snapshot only for multi-key or iterating commands.
- §5 idFilter/LiveSet: the catalog 'republishes on every DDL'; if the id enters the LiveSet after the batch that writes the first idx/geo/col entries (synchronous DOC.CREATEINDEX under the collection lock), a flush plus compaction in that window drops live index entries. Add the id before the first write, remove it after DeleteRangeCF, and treat a nil LiveSet as keep.
- §3 Decimal128 'approximate' index encoding (float64 of the decimal string, residual 0): a Decimal128 just above a query bound encodes equal to the boundary double and an exclusive bound excludes it, so index scans return false negatives the exact matcher never sees. Reject Decimal128 as an index key (rocksdb-native §3, htap-first §3).
- §4 EXEC locks the queued lock sets, checks WATCH-dirty, then commits; a watched key not written by the transaction is outside the lock set, so a write between the check and WriteWI is not detected. Include watched keys in the lock set (htap-first §4).
- §2/§10 durability: 'a machine crash can lose up to one second, exactly as Redis with the same setting' is wrong for SetManualWALFlush(true); the WAL bytes sit in RocksDB's user-space buffer, so a process crash loses them too, unlike Redis everysec. rocksdb-native §4 states this correctly.
- §2: SetCacheIndexAndFilterBlocksWithHighPriority(true) has no effect on a HyperClockCache, which has no priority pools; the isolation argument built on 'high-priority index/filter' does not hold (from RocksDB cache.h, not from the research files).
- §2/§3: no prefix extractor on sub/zscore ('user keys vary in length'), so HGETALL/SMEMBERS/ZRANGE get no memtable or SST prefix bloom and rely on index blocks and bounds alone; the version-first layouts (rocksdb-native §3, htap-first §3) get the bloom for free.
- §6/§7: the columnar index is built synchronously under the collection write lock; a large collection blocks all writes for the duration of the build, and there is no background backfill path.
- §8: query coverer levels are not clamped to the index's [coarsest, finest]; with MaxCells 16 and leaf-level query cells the ancestor probe loop runs up to ~28 bounded iterators per cell (MongoDB clamps with internalQueryS2GeoFinestLevel).
- §6: SPOP returns the first members in byte order and RANDOMKEY is non-uniform; HRANDFIELD, ZRANDMEMBER, SINTERCARD and non-STORE ZUNION/ZINTER/ZDIFF are unsupported, so the Redis 7 collection surface is the thinnest of the three.

Ideas to graft:

- Collection and index drop as one plain WriteBatch (catalog deletes + DeleteRangeCF over each CF range) followed by a scheduled CompactRangeCF and an estimate-pending-compaction-bytes readout.
- Unique-index values as lock keys (uniq, idx_id, enc(values)) so E11000 checks cannot race across _ids.
- Expiry worker: lazy reads enqueue the key; the worker locks, re-checks, deletes and touches WATCH.
- NewWriteBufferManager(256 MiB, allowStall=true) shared by every CF.
- SetDeadline on analytical iterators so a runaway scan returns TimedOut from Iterator.Err().
- geo CF with a 12-byte (idx_id+cell_id) fixed prefix, whole_key_filtering=false, memtable prefix bloom, TotalOrderSeek for descendant ranges and PrefixSameAsStart for ancestor probes.
- Point lat/lng stored in the geo index value so $near computes point distances without loading the document.
- 64-slot hash-sharded filter verdict cache with per-slot mutexes, safe across subcompaction threads without a factory.
- idFilter/LiveSet backstop for dropped collections and indexes, with the corrected publish-before-write ordering.
- find-shaped cursor reply {cursor:{id, ns, firstBatch}} with a 101-document first batch, ORDERED inserts with writeErrors, exact E11000 text with dup key.
- -tags mongod differential tests running the matcher, update and aggregation fixtures against MongoDB 8.0 via testcontainers.
- Shared planner for DOC.AGGREGATE (OLTP resources, 32 MiB MAXMEM) and NIL.AGGREGATE (analytics pool), AT lease for consistent multi-command reads, four-phase htapbench with a freshness marker.
- NIL.GEOGROUP: per-parent-cell counts from a snapshot prefix scan, the HTAP-on-geo idea from the geo digest.
- Missing columnar field = no entry, so the k-way merge reproduces missing fields with no sentinel.

### htap-first: 44/60

rocksdb_correctness 8 | redis_mongo_fidelity 7 | geo_fidelity 7 | htap_credibility 8 | buildability_v1 7 | testability 7

The cleanest concurrency and garbage story of the three: subkeys are | version (8) | subkey | with an 8-byte fixed prefix, a v reverse map (version -> expire, db, key) makes the subkey filter a single existence Get with no key parsing and makes RENAME and MOVE O(1) metadata rewrites, a t time index feeds a 100 ms active expirer so expires= and DBSIZE stay honest, expired keys are deleted only under the key lock, EXEC locks queued and watched keys before checking the dirty flag (the only design that sequences WATCH correctly), ReadOpts.TotalOrder is explicit and applied wherever a scan may cross a prefix, DeleteRange is never issued through WBWI and the EXEC queue is capped to bound WBWI memory, outstanding range tombstones are capped at 32, and every grocksdb constant it names (KMinOverlappingRatioCompactionPri, PointInTimeRecovery, RateLimiterModeAllIo, SetMaxSuccessiveMerges, SetMemTablePrefixBloomSizeRatio, IngestExternalFileCF, SetOptionsCF, GetPinnedCF, KeySlice/ValueSlice) exists in v1.11.1. HTAP is where the design puts its effort: a semaphore that fails fast with -BUSY after 50 ms, a Go byte budget with the correct statement that the C API lacks rate_limiter_priority, a separate colCache, owner-tagged leases, AnalyticsMaxRows, and INFO analytics plus commandstats histograms so htapbench reads baseline and interference numbers from the server. The costs: the subkey filter does an uncached GetPinnedCF per stale subkey, exact DBSIZE rides on hot merge counters that analytical snapshots pin, collection drops are several non-atomic DB-level writes, and the backfill has the same overwrite hazard as rocksdb-native. The fidelity hit is in the key codec: ints and doubles encode differently, so 1 and 1.0 are different index keys and different $group buckets, contradicting the KeyString finding in the document digest; 2dsphere distances deliberately use golang/geo's Earth radius rather than MongoDB's, and GeometryCollection is rejected. Redis surface is the broadest of the three; the Mongo surface is the narrowest.

Flaws:

- §3 keyenc numeric class: 1 encodes 0x20|class|bits(1.0)|0x01|i64(1) and 1.0 encodes 0x20|class|bits(1.0)|0x00, so they are different index keys. MongoDB compares all numerics as one class (document digest, KeyString: '1 and 1.0 compare as equal'); index equality lookups, unique indexes and §7's $group 'keyed by keyenc(group key)' all split ints from doubles. Use rocksdb-native's class scheme (§3) or normalise to double when |v| <= 2^53 and keep the integer suffix only beyond it.
- §5 gc.SubFilter: one GetPinnedCF(meta, 'v'|version) per subkey for versions older than 60 s, with no cache ('RocksDB's block cache makes repeated v lookups cheap'). A block-cache hit is still a C->Go->C round trip plus a full Get per key; compacting a 1 M-element hash costs 1 M Gets where Kvrocks' cached_key_, rocksdb-native's verdict cache and mongo-parity's 64-slot cache cost about one. Add a sharded version -> verdict cache.
- §2/§3/§10 hot merge counters 'K'|db, 'E'|db, 'N'|coll_id: every key-creating or deleting write appends an operand to one key; SetMaxSuccessiveMerges(64) bounds the memtable chain, but 30-300 s analytical snapshots pin the operands (digest: 'snapshots pin unmerged operands') and the counter drifts whenever the compaction backstop rather than the expirer removes a key (acknowledged, repaired by ROCKS.RECOUNT). Consider estimate-num-keys plus a periodic recount, or the documented O(n) DBSIZE the other two accept.
- §5 collection drop: DeleteRange per CF, then catalog deletes, as separate DB-level writes ('never inside a batch'); a crash between them leaves partial state. grocksdb's WriteBatch.DeleteRangeCF exists (only WBWI lacks it), so the whole drop fits one plain WriteBatch as mongo-parity §5 does.
- §6 NIL.COLUMNS ADD backfill under a snapshot: a backfill Put of the snapshot-old value overwrites a concurrent update's new col value or re-creates an entry for a deleted document. Same fix as rocksdb-native §7: re-read the live row under the lock per batch, or skip _ids that already have an entry.
- §8: $geoNear and $near distances use golang/geo's mean Earth radius (6371.01 km) instead of MongoDB's 6378100 m, 'documented as possibly differing'; $maxDistance in metres then admits or excludes documents about 0.11% off MongoDB. The constant was one header away and the other two designs use it. GeometryCollection is rejected and $centerSphere is absent.
- §5 'SetIgnoreSnapshots(true) is set explicitly': grocksdb forwards that call to C only for nativeCompactionFilter; on a Go-implemented filter it is an interface method nothing calls. Harmless because RocksDB >= 6.0 ignores snapshots regardless, but the text overstates what the binding does.
- §2/§10 durability: 'loses <= 1 s on power loss' understates it; with SetManualWALFlush(true) a process crash also loses the buffered WAL bytes, unlike Redis everysec (rocksdb-native §4 states this correctly).
- §4 store contract Checkpoint(dir) (seq uint64, err error) and §6 'ROCKS.CHECKPOINT dir returns sequence number': rocksdb_checkpoint_create takes no sequence-number out-param.
- §4 locks: every keyed command, reads included, takes stripe locks, and document writes lock the whole collection (db|collection name), which serialises all writes to one collection; correct, but a throughput ceiling for DOC.INSERT-heavy loads that the per-_id locks of the other two avoid.
- §8 query coverer MinLevel 0, MaxLevel 30, MaxCells 8 with ancestor probes from Parent(Coarsest) down to Level()-1: a leaf-level query cell costs up to 28 probes per cell; clamp MaxLevel to Finest.

Ideas to graft:

- v reverse map ('v'|version -> expire_ms, db, user key) so the subkey compaction filter is one existence Get with no key decoding, and RENAME/MOVE become O(1) metadata rewrites.
- Version-only 8-byte subkey prefix (no db, no user key), which shrinks every subkey and makes the prefix extractor a single fixed transform.
- t time-ordered expiry index in meta plus a 100 ms active expirer (200 keys per tick, lock, re-check, delete, touch WATCH), so the compaction filter is a backstop rather than the primary path.
- EXEC sequencing: lock queued ∪ watched keys, then check the dirty flag, then run the WBWI and WriteWI once.
- Expired keys deleted only when the command holds the key lock.
- ReadOpts.TotalOrder as an explicit field, set on every scan that may cross a prefix, with PrefixSameAsStart for point-op prefix scans.
- Filter freshness skip: versions whose timestamp bits are younger than 60 s are kept without any lookup.
- store.Store as an interface with NewBatch(indexed bool) and DeleteRange outside batches, for mocking and for keeping WBWI misuse impossible by construction.
- Cap on outstanding range tombstones (32 drops) before a forced CompactRangeCF; cap on the MULTI queue (10 000) to bound WBWI memory.
- -BUSY fast-fail analytics semaphore, AnalyticsMaxRows hard stop, INFO analytics and commandstats histograms read by the bench.
- Geo fixture re-run after deleting 30% of documents and after a columnar backfill to catch stale index entries.
- ROCKS.INGEST via IngestExternalFileCF and ROCKS.SETOPTION via SetOptionsCF for the native-RocksDB admin surface.
- Per-level compression list (none, none, LZ4 x4, ZSTD) on row CFs and SetMaxSuccessiveMerges(64) on the counter CF.

## judge:compat: Redis and MongoDB fidelity plus geo correctness (agent `afaae79837a28971d`, winner `mongo-parity`)

### rocksdb-native: 45/60

rocksdb_correctness 7 | redis_mongo_fidelity 7 | geo_fidelity 7 | htap_credibility 8 | buildability_v1 8 | testability 8

The storage layer is the most carefully reasoned of the three: version-first 9-byte prefixes give prefix blooms on every collection scan, `meta` stays total-order for SCAN, the numeric keystring sub-tag scheme (NaN / |x|>=2^63 / 2^53..2^63 as int64 / <2^53 as sortable double / zero, mirrored) is exact for int64 and double and collapses 1 and 1.0 in 10 bytes, objects encode value-type-before-name like KeyString, and compaction filters are pure functions with a verdict cache. The HTAP path is complete (three planner paths, EXPLAIN, analytics cache, semaphore, leases, backfill that stays consistent because the declaration precedes the scan, an interference bench with stated acceptance numbers). It loses on fidelity because of concurrency holes that Redis semantics do not tolerate: lazy expiry deletes metadata from an unlocked read command, the unique-index check is not protected by a lock on the indexed value, the WATCH dirty check runs before the EXEC lock, and there is no active expiry so a watched key that expires quietly never marks the transaction dirty (multi.tcl 'WATCH will consider touched expired keys' fails). Several named APIs do not exist in the pinned libraries (NewRateLimiterWithMode, GetFromDBCF, Checkpoint returning a sequence number, s2.Polygon.Distance).

Flaws:

- §4 'Single-key reads lock nothing' + §5 'Commands with a Txn delete the metadata key' on an expired read: GET reads expired meta, a concurrent locked SET recreates the key, GET commits its Delete afterwards and the new value is lost.
- §3/§4 unique index: the E11000 check is a Get before Put under a lock on (coll_id, ks(_id)) only; two inserts with different _id and the same unique value both miss and both Put. Needs a lock keyed by (index_id, ks(values)) as mongo-parity has.
- §4 MULTI/EXEC: 'if a watched key was touched, null array. Otherwise EXEC unions the KeySpec keys ... takes one Lock' checks WATCH before locking, so a write committing between the check and the lock is missed; watched keys outside the write set are never locked.
- §5 'There is no active sampler for Redis keys in v1': a WATCHed key that expires without being accessed never calls Touch, so EXEC returns the command replies instead of the null array Redis 6.0.9+ returns; expired strings also linger in `meta` until compaction on a low-write server.
- §3 Redis elements: `klen (2)` caps user keys at 64 KiB while Redis allows 512 MiB (Kvrocks uses a 4-byte length); not documented as a limit.
- §8 2dsphere: `Polygon.Distance` does not exist in the pinned golang/geo (polygon.go lists DistanceToPoint/Project as unimplemented); shape distance needs ClosestEdgeQuery over a ShapeIndex. `Polyline.Intersects` takes a *Polyline, not a loop, so 'against each loop of the query' needs a conversion step. Points store no coordinates in the geo value, so every $near candidate costs a document read.
- §2/§1/§6 API names: `NewRateLimiterWithMode` is not in grocksdb (it is `NewGenericRateLimiter(rate, refill, fairness, mode, autoTuned)`); `GetFromDBCF` is `GetFromDBWithCF`; `Store.Checkpoint(dir) (seq, err)` and 'ROCKS.CHECKPOINT ... returning the sequence number' cannot be met because grocksdb's `CreateCheckpoint(dir, logSizeForFlush)` returns only an error.
- §6 Hashes: 'HSCAN (RESP3 pairs when WITHVALUES form applies)': HSCAN has no WITHVALUES option and its RESP3 reply is the same flat field,value array as RESP2.
- §3 sortable double: -0 and +0 encode to different bytes, so ZADD k 0 a / ZADD k -0 b orders by sign instead of lexicographically and ZCOUNT k 0 0 misses -0; scores need canonicalisation (mongo-parity does this).
- §6 Keys: the RENAME note contradicts itself ('elements are untouched because P carries the version' then 'RENAME of a collection rewrites its elements ... O(n)').
- §6 not-supported list drops ZRANGESTORE and the GEORADIUS STORE/STOREDIST forms that unit/geo exercises, although both other designs include them.
- §2/§7: no WriteBufferManager and no ReadOptions.SetDeadline although grocksdb v1.11.1 exposes both; a runaway analytical scan has no server-side time bound.

Ideas to graft:

- Deterministic SCAN cursors: cursor = xxhash64(lastkey) with a cursor->lastkey table, so repeating a call or reconnecting yields the same cursor for the same position.
- Version-first subkey prefix `| db | version |` (9 bytes) as the fixed prefix extractor on `sub`/`zscore` so HGETALL/SMEMBERS/ZRANGE get memtable and SST prefix blooms; mongo-parity's `sub` has none.
- The numeric keystring class-tag scheme (NaN, >=2^63, 2^53..2^63 as int64, <2^53 as sortable double, zero; mirrored) is exact for int64/double in 10 bytes instead of mongo-parity's 17-byte float+residual.
- Implement INCR/INCRBY/INCRBYFLOAT/APPEND as Get+Put under the key lock, with the explicit argument that a merge operator cannot return the new value or refuse non-integers without writing.
- Take `Store.Snapshot()` at command start for every multi-key read (MGET, EXISTS, SUNION, ZINTER) instead of lazily on the second Get.
- Background columnar backfill under a snapshot with writes maintaining `col` concurrently because the declaration is recorded before the scan; replaces the synchronous build under the collection lock.
- Compaction-filter verdict cache keyed by the collection prefix so consecutive elements of one collection cost one metadata Get per subcompaction run.
- `geo.Ranges` that sorts and merges overlapping descendant ranges and ancestor probes before scanning, and the `Scan`/`Load` callback pair that lets the geo package be tested without RocksDB.
- ROCKS.SCAN with a SNAPSHOT id, ROCKS.SIZES over hex ranges, ROCKS.CHECKPOINT CF via ExportColumnFamily, and NIL.KEYSTATS (per-type key counts from a meta scan).
- Realistic v1 HTAP acceptance gate (throughput drop <= 20%, p99 <= 2x) with the note that TiDB's <=10% assumes separate servers.

### mongo-parity: 48/60

rocksdb_correctness 8 | redis_mongo_fidelity 8 | geo_fidelity 9 | htap_credibility 8 | buildability_v1 6 | testability 9

Under a fidelity-and-geo lens this design is the strongest. The order-preserving encoder copies MongoDB KeyString's type bytes exactly (10/15/20/30/31/60/70/80/90/100/110/120/130/140/150/160/170/240), the float+residual numeric form is exact for int64/double and makes 1 and 1.0 identical, objects encode type-before-name, descending inverts whole encodings with a sound argument about 0xFF. DOC.* replies mirror the MongoDB commands they wrap (cursor/firstBatch, n/writeErrors with E11000 text and code 11000, n/nModified/upsertedId), and the operator set includes positional updates, arrayFilters, pipeline-form updates and findAndModify. The Redis half has an active expiry sampler (Redis's own 20-key/25% loop), an expiry worker that locks and re-checks before deleting, -0 canonicalisation and NaN rejection, a HELLO version rationale, COMMAND DOCS generated from commands/*.json, and a code-level table of exact error strings. Geo is the most complete: correct radii for both worlds, computed S2 levels, a 12-byte prefix bloom that makes ancestor probes cheap, refinement built only on APIs that exist (CrossingEdgeQuery, ClosestEdgeQuery, Polygon.Contains/Intersects), lat/lng stored in the index value so $near never reads documents for points, full $geoNear options, GeometryCollection, and a differential replay against MongoDB 8.0. Every grocksdb name I checked is correct (NewGenericRateLimiter, NewWriteBatchWI(0,true), GetFromDBWithCF, NewIteratorWithBaseCFReadOpts, SetDeadline, SetAsyncIO, OpenDbForReadOnlyColumnFamilies, KeySlice/ValueSlice). It is marked down for the merge-based INCR/APPEND, which cannot reproduce Redis's refuse-without-writing errors, for a lazily created snapshot that leaves the first Get at a different sequence number, for a WATCH window on keys outside the write set, and above all for scope: the v1 surface is the largest of the three with the same four engineers.

Flaws:

- §2/§6 Strings: INCR/INCRBY/INCRBYFLOAT/APPEND through the `nildb.meta` merge operator. Redis must reply 'ERR value is not an integer or out of range', 'ERR increment or decrement would overflow' or WRONGTYPE without writing, INCR must return the new value, and an expired key must restart from 0; a merge operand cannot see `now`, cannot refuse, and the command has to Get anyway to build its reply, so merge adds risk (a poison operand under a snapshot) and no benefit. No `SetMaxSuccessiveMerges` is set, so hot counters grow long operand chains that pinned snapshots keep alive.
- §4 Ctx: the snapshot is created 'on the first iterator or the second Get', so the first Get runs at a later sequence than every other read of the command; SUNION/SDIFF/SINTER over several keys are not atomic across keys as Redis guarantees.
- §4 MULTI/EXEC: the EXEC lock set is 'the union of the queued lock sets'; a watched key not in the write set stays unlocked, so a write to it can commit after the dirty check and before WriteWI (htap-first locks queued plus watched keys).
- §6 DOC.CREATEINDEX 'builds synchronously under the collection lock (v1 has no background builds)': declaring `{"f":"columnar"}` or a btree index on a large collection blocks every write to it for the build, which undercuts the §7 isolation story for the columnar path.
- §2 CF table: `sub` and `zscore` have 'none' as prefix extractor, so HGETALL/SMEMBERS/ZRANGE/LRANGE scans get no memtable or SST prefix bloom even though the design configures exactly that for `geo`.
- §6 Sets: 'SPOP [count] (first members in byte order, not random; documented)': the randomness tests in unit/type/set (SPOP/SRANDMEMBER distribution) fail, and SRANDMEMBER's behaviour is not specified at all.
- §3 numeric encoding: 17 bytes per number where a class-tag scheme needs 10, and Decimal128 collapsed to float64 in index keys means two distinct decimals can collide in a unique index and raise a false E11000 (documented as approximate, but user-visible).
- §6/§11 scope: positional `$`/`$[]`/`$[id]`, arrayFilters, `$bit`, pipeline-form updates, DOC.FINDANDMODIFY, `$convert`/`$to*`, `$dateToString`, `$switch`, `$stdDevPop/Samp`, `$mergeObjects`, ordered/unordered inserts with writeErrors, cursor LIST, DOC.AGGREGATE plus NIL.AGGREGATE, testcontainers differential job, all inside M1-M4 for four engineers with no cut list if M4 slips.
- §6 Connection: 'RESP3 map of 7 fields `server nildb`? no: `server redis`' leaves an unresolved authoring note inside the specification of the HELLO reply.
- §5 Lazy expiry: the expiry worker's channel (depth 4096) drops under a burst of expired-key reads and the design does not say so; TTL/EXISTS callers see -2/0 while `meta` keeps the key until the sampler or compaction reaches it.
- §3 catalog `| 0x04 | db | -> epoch` 'bumped by FLUSHDB' is declared but FLUSHDB in §6 is a DeleteRange and nothing reads the epoch; dead design element that suggests an earlier, different FLUSHDB plan.

Ideas to graft:

- Winner. Apply before M1: replace merge-based INCR/APPEND with Get+Put under the key lock; create the Ctx snapshot at command start for multi-key reads; include watched keys in the EXEC lock set; give `sub`/`zscore` a fixed prefix extractor; move columnar and btree index builds to a background snapshot scan with concurrent maintenance.

### htap-first: 43/60

rocksdb_correctness 7 | redis_mongo_fidelity 6 | geo_fidelity 7 | htap_credibility 8 | buildability_v1 7 | testability 8

The Redis side is the tightest of the three: version-only subkeys make DEL, RENAME and MOVE O(1) metadata rewrites, a `t` TTL index in `meta` drives a precise 100 ms active expirer, per-db K/E counters give an exact DBSIZE and INFO keyspace, EXEC locks the union of queued and watched keys so the WATCH check has no window, and the compaction filter skips lookups for versions younger than 60 s because versions carry their creation time. The HTAP section adds measurement hooks (INFO analytics, commandstats histograms) and a row cap, and the RocksDB surface is the most native (ROCKS.INGEST, ROCKS.SETOPTION). Every grocksdb name I checked is spelled correctly (NewGenericRateLimiter, SetMemTablePrefixBloomSizeRatio, KMinOverlappingRatioCompactionPri, GetFromDBWithCF). The MongoDB side is where it loses: the numeric keyenc encodes `bits(double(v))` plus an int/double suffix, so 5 and 5.0 are different keys (unique indexes accept both, `$group` by keyenc splits them) and an int64 whose double rounds up sorts after that double although it is smaller; descending inverts only the payload so cross-type order is not reversed; objects encode name before type. Collection drop is several DeleteRange calls plus separate catalog deletes, so it is not atomic, KEYS takes the global write lock, and all DOC writes to one collection serialize on a name lock. Geo uses the mean Earth radius instead of MongoDB's 6378100 m and rejects GeometryCollection.

Flaws:

- §3 keyenc numeric: primary bytes are `bits(double(v))` with suffix `0x01|i64(v)` for integers and `0x00` for doubles. (a) `5` and `5.0` encode differently, so a unique index accepts both, `$group` keyed by keyenc (§7) splits 1 and 1.0 that MongoDB groups together, and equality lookups must become range scans; (b) int64 2^53+3 rounds to double 2^53+4, so it sorts after the double 2^53+4 (suffix 0x01 > 0x00) although it is smaller: the encoding is not order-preserving at rounding ties.
- §3 keyenc: 'descending index fields invert every payload byte' leaves the tag byte alone, so a descending field does not reverse cross-type order (Number still precedes String); Undefined is merged into Null (MongoDB orders Undefined before Null); Object encodes `keyenc(name)` before `keyenc(value)` while MongoDB compares value type first, then name.
- §4 store contract `DeleteRange(cf, start, end) // never inside a batch` and §5 'Collection drop is DeleteRange per CF ... then catalog deletes': several separate writes, so a crash mid-drop leaves a catalogued collection with missing rows or index ranges. `WriteBatch.DeleteRangeCF` is supported by RocksDB; only WBWI and transactions forbid it, and the other two designs put the drop in one batch.
- §4 Locks: 'Commands without a static key set (FLUSHDB, FLUSHALL, KEYS, DOC.DROP, DOC.CREATEINDEX) take the global write lock': KEYS stalls every writer for the length of a full meta scan when a snapshot iterator would do; the TCL suite calls KEYS constantly.
- §4 Locks: 'collections lock on db|collection name' and §3 'Uniqueness is a Get on the batch-plus-DB before Put, under the collection's write lock': every DOC.* write to one collection is serialized, so the OLTP half of the HTAP claim has no per-document write parallelism.
- §8 2dsphere: 'Distances use the mean Earth radius constant from golang/geo/s2's Earth helpers' (6371.01 km) instead of MongoDB's kRadiusOfEarthInMeters = 6378100 m, which both other designs name; $near/$geoNear/$maxDistance differ from MongoDB by about 0.11% by construction. GeometryCollection is rejected although MongoDB and the digest list it as a supported type.
- §7 '-BUSY analytics concurrency limit reached': BUSY is Redis's error class for 'a Lua script is running'; redis-cli and several clients react to it by offering or sending SCRIPT KILL.
- §6 DOC.FIND returns `[cursor_id, total_or_-1, [docs]]`: neither MongoDB's cursor/firstBatch shape nor a Redis-idiomatic one, and `total_or_-1` is undefined for filtered queries.
- §4 store contract `Checkpoint(dir string) (seq uint64, err error)` and §6 'ROCKS.CHECKPOINT dir (2) returns sequence number': grocksdb's CreateCheckpoint returns only an error. §5 'SetIgnoreSnapshots(true) is set explicitly' is a no-op since RocksDB 6.0, as grocksdb's own interface comment says.
- §5 SubFilter: one cgo->Go->cgo `GetPinnedCF` per subkey older than 60 s with 'no caching state', so compacting a 1 M-element hash costs 1 M metadata Gets; the other designs cache one verdict per collection run.
- §10 counters: DBSIZE and INFO keyspace drift whenever the compaction backstop rather than the expirer removes a key, and `ROCKS.RECOUNT` (a full scan) is the only repair; the exact-DBSIZE advantage is therefore conditional.
- §7 NIL.COLUMNS 'declares columnar fields and backfills under a snapshot': nothing says writes arriving during the backfill also maintain `col`, so a column can miss documents written in the backfill window (rocksdb-native states the ordering that makes this safe).

Ideas to graft:

- Lock the union of queued keys and WATCHed keys at EXEC before checking the dirty flag, closing the window mongo-parity leaves on watched keys outside the write set.
- A `t | expire_ms | db | key` TTL index in `meta` so active expiry walks due keys in time order (200 per 100 ms tick) instead of random sampling, plus per-db K/E counters merged in the same batch for O(1) DBSIZE and INFO keyspace `keys=,expires=`.
- SubFilter age shortcut: versions embed their creation micros, so subkeys younger than 60 s are kept without a metadata lookup.
- INFO analytics (rows/bytes scanned, plan, throttle sleep, 1-minute histogram) and INFO commandstats-style p50/p99/p999 per command group, so the HTAP bench reads baseline and interference numbers from the server.
- ROCKS.INGEST via IngestExternalFileCF and ROCKS.SETOPTION via SetOptionsCF; both exist in grocksdb v1.11.1 and complete the 'ldb from the wire' feel.
- NIL.STATS coll field (count/min/max/sum/avg over one column) and NIL.GROUP coll field [COUNT|SUM f|AVG f] [TOP n] as RediSearch-style token shortcuts over the same planner.
- `SetMaxSuccessiveMerges(64)` on the counter merge operator's CF, and `cfg.AnalyticsMaxRows` as a hard stop alongside the byte bucket.
- Re-run the geo fixture after deleting 30% of documents and after a columnar backfill to catch stale index entries.
- MOVE as a metadata rewrite, which falls out once subkeys are keyed by version rather than by user key; if mongo-parity keeps Kvrocks-style subkeys, at least document MOVE/RENAME as O(n) for collections.
- Version-only subkeys `| version (8) | subkey |` with a `v` version map: O(1) DEL/RENAME/MOVE, an 8-byte fixed prefix extractor, no key-length field and no db byte in subkeys (a larger change, worth weighing against rocksdb-native's version-first-with-key layout).
