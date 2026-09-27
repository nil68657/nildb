# NilDB v1 architecture

Agreed 2026-09-27. The base is the MongoDB-parity proposal, [proposals/mongo-parity.md](proposals/mongo-parity.md), which won the design panel with 93 points against 89 (RocksDB-native) and 87 (HTAP-first) from the two judges who finished ([judging.md](judging.md)). This file records the scope cut Arka chose ("basic") and the changes both judges asked for before any code. Where this file and the proposal disagree, this file wins. Sections of the proposal not mentioned here stand as written.

## Scope

v1 ships:

- Redis strings, hashes, lists, sets, sorted sets and GEO; key and expiry commands; `SCAN` and its `H/S/Z` variants; `MULTI`/`EXEC`/`WATCH`.
- `DOC.*` reads and writes: `INSERT`, `FIND`, `FINDONE`, `COUNT`, `DISTINCT`, `UPDATE`, `REPLACE`, `DELETE`, `CURSOR`, `CREATEINDEX`, `DROPINDEX`, `INDEXES`, `COLLECTIONS`, `CREATE`, `DROP`, `STATS`.
- Regular indexes (single-field, compound, unique, sparse) and 2dsphere indexes with `$geoWithin`, `$geoIntersects`, `$near`, `$nearSphere` and the `$geoNear` stage.
- Update operators `$set $unset $inc $mul $rename $min $max $currentDate $setOnInsert $push` (with `$each`) `$pop $pull $pullAll $addToSet`.
- Snapshot aggregations through `DOC.AGGREGATE` and `NIL.AGGREGATE` with `$match`, `$group`, `$sort`, `$limit`, plus `NIL.SNAPSHOT` leases.
- `ROCKS.*` admin commands.

Later, not v1: the columnar index and its `col` column family, positional updates (`$`, `$[]`, `$[id]`), `arrayFilters`, pipeline-form updates, `$bit`, `DOC.FINDANDMODIFY`, the `-tags mongod` differential tests against real MongoDB, document TTL indexes, background index builds, `NIL.GEOGROUP`, and aggregation stages beyond the four above and `$geoNear`. Anything in the proposal's section 6 that is not listed under "v1 ships" is later unless a milestone needs it.

## Fixed inputs

- Go 1.27.1 with cgo, `github.com/linxGnu/grocksdb` v1.11.1, Homebrew RocksDB 11.8.1 (`brew pin rocksdb`), module `github.com/nil68657/nildb`. Every `go` command needs `CGO_CFLAGS=-I/opt/homebrew/include CGO_LDFLAGS=-L/opt/homebrew/lib`; the Makefile exports them. `make rocksmoke` is the gate after any RocksDB upgrade.
- A plain `grocksdb.DB`, not `TransactionDB` or `OptimisticTransactionDB`.
- An in-house RESP codec (`internal/resp`): RESP2 byte-exact, RESP3 after `HELLO 3`.
- Documents are raw BSON (`go.mongodb.org/mongo-driver/v2/bson`) behind `DOC.*` commands on the Redis port. NilDB does not speak the MongoDB wire protocol.
- Redis GEO is a port of Redis 7.2 `geohash.c` and `geohash_helper.c` (BSD-3-Clause). Never copy Redis 8.x source. 2dsphere uses `github.com/golang/geo` S2.

## Column families

Seven, all opened on every start so later milestones add no migrations: `default` (catalog), `meta`, `sub`, `zscore`, `doc`, `idx`, `geo`. `col` is deferred with the columnar index. Per-CF options follow the proposal's section 2 with these changes:

- No merge operators on any CF (change 1 below).
- `sub` and `zscore` use `NewFixedPrefixTransform(8)` and a memtable prefix bloom (change 4).
- One `NewHyperClockCache` block cache, 512 MiB by default, with `cache_index_and_filter_blocks` and `pin_l0_filter_and_index_blocks_in_cache`. HyperClockCache has no priority pools, so drop the proposal's `SetCacheIndexAndFilterBlocksWithHighPriority` isolation argument. Analytical iterators set `fill_cache=false`; a separate analytics cache waits for the columnar index.

## Key layouts

`meta` keys are `| db (1) | user key |` with `db` 0 to 15, exactly as the proposal's section 3. The first byte `'v'` (0x76) is reserved for the version map in change 4; any future `meta`-resident index takes another byte above 15. Metadata values keep the proposal's headers:

```
string | flags (1) | expire_ms (8, BE) | payload |
coll   | flags (1) | expire_ms (8, BE) | version (8) | size (8) |
list   | flags (1) | expire_ms (8, BE) | version (8) | size (8) | head (8) | tail (8) |
flags  bits 0-3 type (1 string, 2 hash, 3 list, 4 set, 5 zset), bits 4-7 layout version 0
```

Drop the catalog's `| 0x04 | db | -> epoch` entry; nothing reads it.

## Changes to the proposal

1. **No merge operators.** `INCR`, `INCRBY`, `DECR`, `DECRBY`, `INCRBYFLOAT`, `APPEND`, `SETRANGE`, `HINCRBY` and `HINCRBYFLOAT` read the value, compute, and `Put` under the key lock. Errors (`ERR value is not an integer or out of range`, `ERR increment or decrement would overflow`, `WRONGTYPE ...`) go out before anything is written, and an expired key counts from zero. A merge operand can do none of that and pinned snapshots keep operand chains alive (both judges).
2. **Plain batches outside EXEC.** Autocommit commands collect writes in one plain `WriteBatch` and call `db.Write` once. `WriteBatchWithIndex` is used only inside `EXEC`, where later queued commands must read earlier ones. Single-key point reads go to the live DB with no snapshot. A command that reads more than one key or iterates takes one snapshot when it starts and releases it when it ends, so `MGET`, `SUNION` and `HGETALL` see one sequence number (both judges).
3. **EXEC locks watched keys.** `EXEC` sorts and locks the stripes of every queued key and every watched key, then checks the WATCH dirty flag, then runs and commits. `WATCH` records whether each key is already logically expired; `EXEC` re-reads the expiry of the others and returns a null array if one has expired since (Redis 6.2.5 behaviour, [08-gap-transactions-and-watch.md](../research/findings/08-gap-transactions-and-watch.md)). Key the watch table by `(db, key)`.
4. **Version-first sub-keys (decide in M2, recommended).** Take the HTAP-first layout: `sub` keys `| version (8) | subkey |`, `zscore` keys `| version (8) | score (8) | member |`, versions `unix_micros << 11 | counter` unique across databases, and a reverse map in `meta` at `| 'v' | version (8) | -> | expire_ms (8) | db (1) | user key |` written in the same batch as the metadata. The sub-key compaction filter then needs one cached lookup of the version, and `DEL`, `RENAME` of a collection stay single metadata writes ([proposals/htap-first.md](proposals/htap-first.md) section 3).
5. **Durability.** Leave `manual_wal_flush` off, so every committed batch reaches the OS before the reply and a NilDB crash loses nothing acknowledged. `--fsync everysec` (default) calls `db.FlushWAL(true)` once a second, so power loss costs at most about one second, matching Redis `appendfsync everysec`. `--fsync always` sets `WriteOptions.SetSync(true)`; `--fsync no` never syncs. The proposal's manual WAL flush would also lose a second on a process crash (judge:rocksdb).
6. **Expiry is lazy plus compaction filter.** Reads treat an expired key as absent and never write. Writes treat it as absent and overwrite it; collections get a new version. The `meta` compaction filter drops expired entries, and `periodic_compaction_seconds` of 7 days makes cold files get filtered. v1 has no active-expiry sampler and no expiry worker, so the proposal's 4096-deep worker channel and its silent drops go away. Kvrocks works the same way. `DBSIZE`, `KEYS` and `SCAN` skip expired keys; `DBSIZE` iterates the database's `meta` range and is O(n), which the command docs must say.
7. **Catalog live-set ordering.** Publish a new collection or index id to the compaction filters' live set before the first write that uses it, remove it only after the `DeleteRangeCF` batch commits, and treat a nil live set as "keep everything" (judge:rocksdb).
8. **No Decimal128 index keys.** Reject a Decimal128 value in an indexed field. The proposal's float64 approximation gives false negatives at range bounds and false duplicate-key errors in unique indexes (both judges).
9. **Index builds block writers, documented.** v1 builds regular and 2dsphere indexes synchronously under the collection's write lock. The judges asked for background builds; they are on the later list with the columnar index, which was the case that hurt most.
10. **Clamp S2 query coverings** to the index's `[coarsest, finest]` levels, as MongoDB does, so ancestor probes stay bounded (judge:rocksdb).
11. **HELLO reply.** `server` is `redis`, `version` is `7.2.0` (config `nil-redis-version`), then `proto`, `id`, `mode standalone`, `role master`, `modules []`. This replaces the unresolved note in the proposal's section 6.
12. **Random picks.** `SPOP`, `SRANDMEMBER` and `RANDOMKEY` seek to a random position inside the key's range and wrap around, rather than taking the first members in byte order. Selection is not uniform, and the command docs must say so.

## Concurrency model

One goroutine per connection reads a command, runs it, and appends the reply to a `bufio.Writer`, flushing when the reader has no more buffered input. Pipelined replies therefore leave in request order with one syscall per batch. The proposal's reader-plus-executor pair is not needed for ordering. Write commands lock their key stripes (4096 `sync.Mutex`, sorted, deduplicated) around read-compute-write; reads take no locks. Analytical commands hold no server lock while they iterate, only their snapshot.
