> Design-panel proposal, exported on 2026-09-27 from Claude Code run `wf_b09e8159-aff` (agent `aa043b9779be466e1`, written 2026-09-26). Unedited. Where it disagrees with [../architecture.md](../architecture.md), the architecture file wins.

# NilDB design: HTAP-first Redis/Mongo-compatible store on RocksDB

Fixed inputs honoured throughout: Go 1.27 + cgo, grocksdb v1.11.1 against Homebrew RocksDB 11.8.1, module `github.com/nil68657/nildb`, single node, default bytewise comparator only, raw BSON documents, golang/geo S2, Redis GEO as 52-bit geohash scores, RESP2 exact plus RESP3 via `HELLO 3`. The existing `cmd/rocksmoke` stays as the day-one link gate.

Two decisions that shape everything below:

- **Subkeys are keyed by version, not by user key.** Every Redis collection gets a 64-bit version (`unix_micros << 11 | counter`) at creation. Hash fields, set members, list elements and zset entries live under `| version (8) | subkey |`. The version is unique across all keys and databases, so the prefix extractor is a fixed 8 bytes (the only prefix transform the C API offers), DEL/RENAME/MOVE of any collection are O(1) metadata rewrites, and stale subkeys are reclaimed by a compaction filter that checks one thing: does the version still exist. Titan does this with 16-byte UUIDs; Kvrocks needs a 4-byte length field plus the whole user key in every subkey.
- **Analytics run on a snapshot over a columnar CF that is written in the same WriteBatch as the row.** Freshness is therefore zero by construction (one RocksDB, one sequence number). All engineering effort goes into isolation: bounded concurrency, a Go-side byte budget for scans, a separate block cache, and snapshot leases with a cap and timeout.

RESP codec decision: **in-house**, not redcon. The reply writer is the part that differs between RESP2 and RESP3 (map, set, double, null, boolean, verbatim), and redcon's `Writer` has none of those types, so wrapping it means bypassing it on every RESP3 reply. The parser also has to emit the exact `Protocol error: ...` strings from `networking.c` and enforce the pre-AUTH limits, which redcon does not. Cost is roughly 700 lines plus a protocol.tcl-derived test file; benefit is three external dependencies total (grocksdb, mongo-driver/v2/bson, golang/geo).

## 1. Package layout

All paths are relative to the repository root. Arrows list imports of other internal packages; the graph is a DAG rooted at `cmd/nildb`.

| Package | Responsibility | Imports (internal) |
|---|---|---|
| `cmd/nildb` | Parse config, open store, bind GC filters, start server, handle signals. | config, store, gc, server, command |
| `cmd/rocksmoke` | Existing link/feature smoke test for the RocksDB build. | none |
| `cmd/htapbench` | HTAP interference and freshness measurement driver (section 9). | none (talks RESP over TCP) |
| `internal/config` | Config struct, flag parsing, defaults, validation. | none |
| `internal/resp` | RESP2/RESP3 reader and writer, `Reply` values, fixed Redis error table. | none |
| `internal/layout` | Byte layouts for Redis metadata, version map, TTL index, subkeys, zscore keys; version generator; order-preserving float64/int64. | none |
| `internal/keyenc` | Order-preserving encoding of BSON values for `_id`, secondary indexes, columnar values; type-order table. | none |
| `internal/lock` | Striped key lock manager plus one global RWMutex; sorted multi-key acquisition. | none |
| `internal/store` | `Store` interface and its grocksdb implementation: CF definitions and options, caches, rate limiter, batches (plain and indexed), iterators, snapshot lease registry, checkpoints, properties. | none |
| `internal/gc` | Compaction filters (meta, subkey), active TTL expirer, doc TTL sweeper. Bound to the store after open. | store, layout |
| `internal/geohash` | Redis geohash: 52-bit interleave, decode, neighbours, step estimate, haversine, base32. | none |
| `internal/rtypes` | Redis data types over the store: strings, hashes, lists, sets, zsets, GEO. Typed results, no RESP. | store, layout, geohash |
| `internal/doc/matcher` | Query predicate evaluation on `bson.Raw` with MongoDB null/missing/array rules. | none |
| `internal/doc/update` | Update operators on BSON (`$set`, `$inc`, `$push`, ...). | none |
| `internal/doc/index` | Key generation for secondary indexes (multikey, sparse, unique) and index-usable predicate extraction. | keyenc |
| `internal/s2index` | GeoJSON parsing to S2 geometries, covering, query cell ranges, exact predicates, distances, expanding-ring kNN plan. | none |
| `internal/columnar` | Columnar CF entries from a document given a field dictionary; per-field iterators; k-way merge by `_id`. | store, keyenc |
| `internal/doc` | Catalog (collections, indexes, field dictionary), insert/find/update/delete composing docs + docidx + geoidx + col + counters into one batch, cursors. | store, keyenc, doc/matcher, doc/update, doc/index, s2index, columnar |
| `internal/pipeline` | Aggregation planner and pull-based executor with memory accounting: `$match`, `$project`, `$group`, `$sort`, `$limit`, `$skip`, `$count`, `$unwind`, `$geoNear`. | doc, columnar, doc/matcher, keyenc, store, s2index |
| `internal/rocksadmin` | `ROCKS.*` implementations over the store: properties, compaction, flush, checkpoint, snapshot leases, raw scans, ingest. | store |
| `internal/command` | Command table (name, arity, flags, key extractor, handler), `Ctx`, handlers grouped by file, result-to-`resp.Reply` conversion. | resp, store, layout, rtypes, doc, pipeline, rocksadmin |
| `internal/server` | Listener, per-connection reader/executor/writer, FIFO pipelining, MULTI queue, WATCH registry, lock acquisition, analytics semaphore, SCAN cursor table. | resp, command, lock, store |
| `internal/testutil` | Start an in-process server on a temp dir, raw TCP client, go-redis client factory. Test-only. | server, store, gc, command, config |

## 2. Column families

Eight CFs in one DB opened with `grocksdb.OpenDbColumnFamilies`. Shared objects: `mainCache := grocksdb.NewHyperClockCache(cfg.CacheBytes, 0)`, `colCache := grocksdb.NewHyperClockCache(cfg.ColCacheBytes, 0)` (default 64 MiB), one `grocksdb.NewGenericRateLimiter(cfg.BgIOBytesPerSec, 100_000, 10, grocksdb.RateLimiterModeAllIo, true)` set on every CF's options, one `grocksdb.NewWriteBufferManager`.

DB-level options: `SetCreateIfMissing(true)`, `SetCreateIfMissingColumnFamilies(true)`, `SetMaxBackgroundJobs(6)`, `SetBytesPerSync(1<<20)`, `SetMaxOpenFiles(-1)`, `SetWALRecoveryMode(grocksdb.PointInTimeRecovery)`, `SetManualWALFlush(true)` with a 1 s `db.FlushWAL(true)` ticker (Redis `appendfsync everysec` semantics; `always` sets `WriteOptions.SetSync(true)`), `SetMaxTotalWalSize(1<<30)`, `SetLevelCompactionDynamicLevelBytes(true)`, `SetCompactionPri(grocksdb.KMinOverlappingRatioCompactionPri)`, `SetWriteBufferSize(64<<20)`.

Row-CF table options: `bbto.SetBlockSize(16<<10)`, `SetBlockCache(mainCache)`, `SetCacheIndexAndFilterBlocks(true)`, `SetPinTopLevelIndexAndFilter(true)`, `SetFormatVersion(6)`, `SetOptimizeFiltersForMemory(true)`, `SetFilterPolicy(grocksdb.NewRibbonHybridFilterPolicy(10, 1))`. Row-CF compression: `SetCompressionPerLevel([]CompressionType{NoCompression, NoCompression, LZ4Compression, LZ4Compression, LZ4Compression, LZ4Compression, ZSTDCompression})`.

| CF | Contents | Prefix extractor | Bloom | Merge op | Compaction filter | Cache | Compression | Notes |
|---|---|---|---|---|---|---|---|---|
| `default` (sys) | Catalog: collections, indexes, field dictionary, counters, server state | none | whole key 10 | `nildb.counter` (int64 add, associative, `SetMaxSuccessiveMerges(64)`) | none | main | LZ4 | counters give exact DBSIZE, doc counts |
| `meta` | Redis metadata + string values (`m`), version map (`v`), TTL index (`t`) | none | whole key ribbon-hybrid 10 | none | `gc.MetaFilter` | main | per-level | SCAN/KEYS source; `SetPeriodicCompactionSeconds(7d)` |
| `sub` | Hash fields, set members, list elements, zset member->score | `NewFixedPrefixTransform(8)` | prefix only (`SetWholeKeyFiltering(false)`), `SetMemTablePrefixBloomSizeRatio(0.1)` | none | `gc.SubFilter` | main | per-level | periodic compaction 7d |
| `zscore` | zset score index | fixed 8 | prefix only | none | `gc.SubFilter` | main | per-level | same options as `sub` |
| `docs` | BSON rows | fixed 4 (coll_id) | whole key 10 | none | none | main | per-level | drops via `DeleteRange` |
| `docidx` | Secondary index entries | fixed 4 (index_id) | prefix only | none | none | main | per-level | eager delete on update |
| `geoidx` | 2dsphere S2 entries | fixed 4 (index_id) | prefix only | none | none | main | per-level | block size 32 KiB |
| `col` | Columnar analytical entries | fixed 6 (coll_id+field_id) | prefix ribbon 8 | none | none | `colCache` (own `BlockBasedTableOptions`) | ZSTD all levels, `SetBottommostCompression(ZSTDCompression)` | block size 64 KiB, scans set `fill_cache=false` |

## 3. Byte layouts

Integers are big-endian. `dbl(x)` is the Kvrocks sign-flip: `b := math.Float64bits(x); if b>>63 == 1 { b = ^b } else { b |= 1<<63 }`. `i64(x)` is `uint64(x) ^ (1<<63)`. Versions are `(uint64(unix_micros) << 11) | (counter & 0x7FF)`, monotonic under an atomic, seeded at startup from `max(now, sys['S','version'] + 1)`.

**Namespaces.** `SELECT` is real: 16 databases, encoded as one byte `db` in `meta` keys. Subkeys carry no `db` (the version already identifies the key), which makes `MOVE` a metadata rewrite. Document collections belong to the selected `db` through the catalog key.

**Redis metadata (`meta` CF).**

```
m-key   | 'm' (1) | db (1) | user key (n) |
m-value string:      | flags (1) | expire_ms (8) | payload (n) |
m-value hash/set/zset:| flags (1) | expire_ms (8) | version (8) | size (8) |
m-value list:        | flags (1) | expire_ms (8) | version (8) | size (8) | head (8) | tail (8) |
flags: bits 0-3 type (1 string, 2 hash, 3 list, 4 set, 5 zset); bit 7 layout version (0)
expire_ms 0 = never.  head = tail = 1<<63 on creation.

v-key   | 'v' (1) | version (8) |          -> | expire_ms (8) | db (1) | user key (n) |
t-key   | 't' (1) | expire_ms (8) | db (1) | user key (n) |   -> empty  (only when expire_ms != 0)
```

`v` exists only for collection types. `SET` over an existing collection deletes its `v` in the same batch. `EXPIRE`/`PERSIST` rewrite `m`, `v` and `t` together.

**Subkeys (`sub` CF) and score index (`zscore` CF).**

```
hash    | version (8) | field (n) |            -> | value (n) |
set     | version (8) | member (n) |           -> empty
list    | version (8) | index (8) |            -> | element (n) |     LPUSH: head--, RPUSH: tail++; LINSERT renumbers (O(n), documented)
zset    | version (8) | member (n) |           -> | dbl(score) (8) |
zscore  | version (8) | dbl(score) (8) | member (n) |   -> empty
```

**Redis GEO.** A zset whose score is `float64(geohash52)`; `dbl()` of a non-negative integer-valued double preserves integer order, so a cell's score range `[h<<shift, (h+1)<<shift)` is one `zscore` range scan.

**Documents (`docs` CF).**

```
| coll_id (4) | keyenc(_id) (n) |   -> raw BSON (as received, _id moved first, 16 MiB cap)
```

**keyenc type-order table** (one tag byte, then payload; descending index fields invert every payload byte).

| Tag | BSON types | Payload |
|---|---|---|
| 0x05 | MinKey | none |
| 0x10 | Null, missing, Undefined | none |
| 0x20 | Int32, Int64, Double (one numeric class) | class byte: 0x00 NaN, 0x01 -Inf, 0x02 negative (`^bits(double)`), 0x03 zero, 0x04 positive (`bits(double)`), 0x05 +Inf; then for integer types `| 0x01 | i64(v) (8) |`, for doubles `| 0x00 |`. Range scans use the double prefix; the suffix keeps integers beyond 2^53 exact. Decimal128 is rejected in indexed and columnar fields in v1. |
| 0x30 | String, Symbol | UTF-8 with `0x00 -> 0x00 0xFF`, terminated `0x00` |
| 0x40 | Object | repeated `| keyenc(name as string) | keyenc(value) |`, terminated `0x00` |
| 0x50 | Array | repeated `keyenc(element)`, terminated `0x00` (used only for `_id` and whole-array equality; index entries are per element) |
| 0x60 | BinData | `| len (4) | subtype (1) | bytes |` |
| 0x70 | ObjectId | 12 bytes |
| 0x80 | Bool | `0x00` / `0x01` |
| 0x90 | Date | `i64(ms)` |
| 0xA0 | Timestamp | `u64` |
| 0xB0 | Regex | `keyenc(pattern) keyenc(options)` |
| 0xF0 | MaxKey | none |

The tags reproduce MongoDB's cross-type order (Null < Number < String < Object < Array < BinData < ObjectId < Bool < Date < Timestamp < Regex). Decoding back to BSON loses int-vs-double only for values read from index keys; the row is authoritative.

**Secondary indexes (`docidx` CF).**

```
non-unique | index_id (4) | keyenc(f1) | ... | keyenc(fk) | keyenc(_id) |   -> empty
unique     | index_id (4) | keyenc(f1) | ... | keyenc(fk) |                 -> keyenc(_id)
```

Missing fields index as Null unless the index is sparse. Arrays emit one entry per distinct element (at most one array field per compound index). Uniqueness is a `Get` on the batch-plus-DB before `Put`, under the collection's write lock.

**2dsphere (`geoidx` CF).**

```
| index_id (4) | cell_id (8) | keyenc(_id) |   -> | kind (1) | leaf_cell (8, points only) |
kind: 1 Point, 2 LineString, 3 Polygon, 4 MultiPoint, 5 MultiLineString, 6 MultiPolygon
```

**Columnar (`col` CF).**

```
| coll_id (4) | field_id (2) | keyenc(_id) |   -> keyenc(value)      scalars only
                                             -> | 0x40 | or | 0x50 | (marker) for object/array
```

One entry per declared column per document. All field iterators are ordered by `keyenc(_id)`, so a projection of k columns is a k-way merge with no row read.

**Catalog (`default` CF).**

```
| 'C' | db (1) | collection name |      -> | coll_id (4) |
| 'c' | coll_id (4) |                    -> BSON {name, db, columns:[path...], created, options}
| 'I' | coll_id (4) | index name |       -> BSON {index_id, keys, unique, sparse, s2:{coarsest,finest,maxCells}, expireAfterSeconds}
| 'F' | coll_id (4) | field path |       -> | field_id (2) |
| 'N' | coll_id (4) |                    -> counter (doc count, Merge)
| 'K' | db (1) |   | 'E' | db (1) |     -> counters (keys, keys with expiry, Merge)
| 'S' | name |                            -> server state (last version, next coll_id, next index_id)
```

**SCAN cursors.** A cursor is a server-side table entry: `id = epoch16<<48 | seq48` (epoch random per process start) mapping to `{cf, resume key, deadline}`; entries expire after 10 minutes idle, table capped at 10 000. `SCAN 0` iterates `meta` over `['m'|db, 'm'|db+1)`, returns up to COUNT live `m` keys (expired keys skipped and lazily deleted), and either `0` or a fresh id whose resume key is the next key. Unknown or evicted ids reply `-ERR invalid cursor` (documented divergence: Redis never fails here). `HSCAN`/`SSCAN`/`ZSCAN` use the same table with the subkey as resume point.

## 4. Concurrency and transactions

**Connection model.** One reader goroutine parses frames into a bounded channel; one executor goroutine runs commands strictly in order and appends replies to a `bufio.Writer`, flushing when the input channel is empty or 64 KiB is buffered. This gives FIFO pipelining and works with `redis-benchmark -P` and `redis-cli --pipe`.

**Locks.** `internal/lock` holds a global `sync.RWMutex` and 4096 striped `sync.Mutex` keyed by `hash(db|key)` (collections lock on `db|collection name`). A command's `Keys()` extractor returns its key set; the server sorts the stripe indexes, takes them under the global read lock, runs the handler, writes the batch, releases. Commands without a static key set (`FLUSHDB`, `FLUSHALL`, `KEYS`, `DOC.DROP`, `DOC.CREATEINDEX`) take the global write lock. Analytical commands take no locks at all; they read snapshots.

**Autocommit commands** build one `grocksdb.WriteBatch` spanning any CFs and call `db.Write`. Reads inside the command hit the DB directly; the key lock makes that consistent.

**MULTI/EXEC.** Queue-time validation (unknown command, arity) replies the error and marks the transaction dirty; `EXEC` then returns `-EXECABORT Transaction discarded because of previous errors.`. On `EXEC` the server computes the union of queued keys and watched keys, locks them (global write lock if any queued command lacks a key set), checks the WATCH dirty flag (null array if set), then runs every queued handler against a `grocksdb.NewWriteBatchWI(0, true)`. Reads inside the batch use `wb.GetFromDBWithCF(db, ro, cf, key)` and `wb.NewIteratorWithBaseCFReadOpts(...)`, so `INCR` after `SET` sees the `SET`. Commit is `db.WriteWI(wo, wb)`: one atomic write, runtime errors become array elements exactly as Redis does. `DeleteRange` is never issued through a WBWI (unsupported), so `FLUSHDB` inside `MULTI` is rejected with `-ERR FLUSHDB is not allowed inside MULTI` (documented). Queue length is capped (default 10 000) to bound WBWI memory.

**WATCH** is Redis's own algorithm: a registry `map[db|key][]*conn`. After every successful `db.Write`, the server marks watchers of the touched keys dirty; lazy and active expiry, `FLUSHDB`/`FLUSHALL` and collection drops touch too. Because `EXEC` locks the watched keys before checking, a write that landed before the lock has already marked the flag. No `OptimisticTransactionDB`: the store stays a plain `DB`, keeping `WriteBatchWI`, `DeleteRange` and checkpoints unrestricted.

**DOC commands** are each one atomic batch (row + all index entries + columnar entries + counters). Multi-document interactive transactions are not in v1; `MULTI ... DOC.INSERT ... DOC.UPDATE ... EXEC` gives atomic multi-statement writes without interactive reads, and the supported-command table says so.

**Snapshots.** Single-key and multi-key point commands read without explicit snapshots (locks provide consistency). Iterating commands (`HGETALL`, `ZRANGE`, `LRANGE`) rely on the iterator's implicit snapshot. `DOC.FIND`, `NIL.*`, `DOC.CURSOR` and `ROCKS.SNAPSHOT` use explicit leases from the store (section 7).

Store contract:

```go
package store

type CF uint8
const (CFSys CF = iota; CFMeta; CFSub; CFZScore; CFDocs; CFDocIdx; CFGeoIdx; CFCol)

type ReadOpts struct {
    Snapshot   *Snapshot // nil = latest
    FillCache  bool      // false for analytics
    TotalOrder bool      // true whenever a scan may cross prefix boundaries
    Readahead  int       // bytes; 0 = RocksDB auto
    AsyncIO    bool
}

type Value interface{ Data() []byte; Exists() bool; Free() }

type Iterator interface {
    Seek(key []byte); SeekForPrev(key []byte); Next(); Prev(); Valid() bool
    Key() []byte; Value() []byte // valid until next move
    Err() error; Close()
}

type Batch interface {
    Put(cf CF, key, val []byte); Delete(cf CF, key []byte); Merge(cf CF, key, val []byte)
    Get(ro ReadOpts, cf CF, key []byte) (Value, error)             // read-your-writes when indexed
    Iter(ro ReadOpts, cf CF, lower, upper []byte) Iterator          // base iterator merged with batch when indexed
    Count() int; Close()
}

type Snapshot struct { ID uint64; Seq uint64; Deadline time.Time; owner string }

type Store interface {
    Get(ro ReadOpts, cf CF, key []byte) (Value, error)
    MultiGet(ro ReadOpts, cf CF, keys [][]byte) ([]Value, error)
    Iter(ro ReadOpts, cf CF, lower, upper []byte) Iterator
    NewBatch(indexed bool) Batch            // false: WriteBatch; true: WriteBatchWI
    Write(b Batch, sync bool) error
    DeleteRange(cf CF, start, end []byte) error // never inside a batch
    AcquireSnapshot(owner string, ttl time.Duration) (*Snapshot, error) // ErrSnapshotCap when at cap
    ReleaseSnapshot(*Snapshot)
    Snapshots() []Snapshot
    Property(cf CF, name string) (string, bool)
    IntProperty(cf CF, name string) (uint64, bool)
    Checkpoint(dir string) (seq uint64, err error)
    CompactRange(cf CF, start, end []byte) error
    Flush(cf CF) error
    SetOptions(cf CF, kv map[string]string) error
    Close() error
}
```

Command contract:

```go
package command

type Flag uint16
const (Write Flag = 1 << iota; ReadOnly; Admin; Analytics; NoMulti; NeedsGlobalLock; Fast)

type Command struct {
    Name    string
    Arity   int                       // Redis convention; negative = minimum
    Flags   Flag
    Keys    func(args [][]byte) [][]byte // db-relative keys for locking and WATCH; nil = NeedsGlobalLock
    Handler func(ctx *Ctx, args [][]byte) resp.Reply
}

type Ctx struct {
    DB     uint8            // selected database
    Proto  int              // 2 or 3
    Now    int64            // ms, fixed for the command
    Store  store.Store
    Batch  store.Batch      // WriteBatch (autocommit) or WriteBatchWI (inside EXEC)
    Snap   *store.Snapshot  // set by the server for Analytics commands
    Conn   ConnState        // name, id, MULTI state, watched keys
    Touch  func(keys ...[]byte) // notifies WATCH registry after commit
}
```

Codec contract:

```go
package resp

type Reply interface{ encode(w *Writer, proto int) }
func Status(s string) Reply; func Err(code, msg string) Reply; func Int(n int64) Reply
func Bulk(b []byte) Reply; func Null() Reply; func NullArray() Reply
func Array(r ...Reply) Reply; func Map(kv ...Reply) Reply; func Set(r ...Reply) Reply
func Double(f float64) Reply; func Bool(b bool) Reply; func Verbatim(fmt3, s string) Reply

type Reader struct{ /* bufio, limits */ }
func (r *Reader) Next() (args [][]byte, err error) // inline or multibulk; *ProtocolError carries the exact Redis text
type Writer struct{ /* bufio */ }
func (w *Writer) Write(r Reply, proto int); func (w *Writer) Flush() error
```

Encoding rules: `Null` is `$-1` / `_`; `NullArray` is `*-1` / `_`; `Map` is a flat array / `%`; `Set` is `*` / `~`; `Double` is a bulk string / `,`; `Bool` is `:1`/`:0` / `#t`/`#f`; `Verbatim` is bulk / `=`.

## 5. TTL and garbage

**Lazy expiry.** Every read of an `m` entry compares `expire_ms` with `ctx.Now`; an expired key is treated as absent and, when the command holds the key lock, deleted (`m`, `v`, `t`, counters) in the command's batch. WATCHers are touched.

**Active expiry.** `gc.Expirer` runs every 100 ms: iterate `meta` over `['t'|0, 't'|now]`, up to 200 keys per tick, lock each key, re-check `m`, delete `m`/`v`/`t`, decrement `K`/`E` counters, touch WATCHers. This keeps `DBSIZE` exact to within a tick and keeps compaction filters as the backstop rather than the primary path.

**Compaction filters.** grocksdb offers one shared `CompactionFilter` instance per CF and no factory, and RocksDB may call it from several subcompaction threads, so both filters are stateless functions of `(key, value, now)` plus read-only `Get`s. They hold a `*store.DB` set by `gc.Bind(db)` after open and return keep until bound.

- `gc.MetaFilter` (`meta` CF): `m` with `expire_ms != 0 && expire_ms + 60_000 < now` drops; `v` with the same condition drops; `t` with `expire_ms + 60_000 < now` drops. The 60 s grace protects a command that read `m` and is about to iterate subkeys.
- `gc.SubFilter` (`sub`, `zscore` CFs): read the 8-byte version; if `version >> 11` is younger than 60 s, keep without a lookup (versions carry their creation time, which removes the lookup for fresh data); else `GetPinnedCF(meta, 'v'|version)`; missing means the collection was deleted, overwritten or expired, so drop. No caching state is shared; RocksDB's block cache makes repeated `v` lookups cheap. `SetIgnoreSnapshots(true)` is set explicitly; the docs state that an expired key can vanish from a held snapshot, as RocksDB's own compaction-filter contract says.

**O(1) collection delete.** `DEL` of a hash/list/set/zset deletes `m`, `v`, `t` and decrements counters; nothing touches `sub`/`zscore`. New writes to that key mint a new version. `RENAME` rewrites `m` under the new key and rewrites `v`'s stored key; `MOVE` likewise.

**Documents.** Row deletes and updates remove their `docidx`, `geoidx` and `col` entries in the same batch (Luo & Carey sync-full; the old document is already read for update semantics, so old index keys are recomputed from it, including the S2 covering, which is deterministic for frozen per-index parameters). Collection drop is `DeleteRange` per CF over `| coll_id |` in `docs` and `col`, `| index_id |` per index in `docidx`/`geoidx`, then catalog deletes, then a scheduled `CompactRangeCF` on the dropped ranges. Outstanding drops are capped at 32 before compaction is forced, to bound range tombstones. TTL indexes (`expireAfterSeconds`) are ordinary `docidx` indexes on a Date field; `gc.DocSweeper` runs every 60 s, scans `[index_id | keyenc(MinKey), index_id | keyenc(now - ttl)]` under a snapshot and deletes matching documents through `doc.Delete` in batches of 1 000 per index per tick.

## 6. Command surface v1

Arity follows Redis (negative = at least). RESP3 differences noted; anything unmarked is byte-identical in both. Reported version: `7.2.0` (Kvrocks reports 7.0.0; clients only gate on HELLO support).

**Connection.** `PING [msg]` (-1) `+PONG`/bulk; `ECHO` (2); `QUIT` (1); `SELECT` (2) `-ERR DB index is out of range`; `AUTH [user] pass` (-2); `HELLO [proto [AUTH u p] [SETNAME n]]` (-1) map of server, version, proto, id, mode, role, modules (flat array in RESP2; `-NOPROTO unsupported protocol version` for other protovers); `CLIENT ID|SETNAME|GETNAME|INFO|LIST|SETINFO` (-2); `COMMAND [COUNT|INFO|DOCS]` (-1) static table generated from the registry; `INFO [section]` (-1) verbatim in RESP3, sections server, clients, memory, keyspace, rocksdb; `DBSIZE` (1); `FLUSHDB`/`FLUSHALL [SYNC|ASYNC]` (-1); `CONFIG GET` (3) returns `save`/`appendonly`/`databases`/`maxmemory`, `CONFIG SET` only for `nildb.*` runtime knobs.

**Keys.** `DEL` (-2), `UNLINK` (-2), `EXISTS` (-2), `TYPE` (2), `KEYS` (2), `SCAN cursor [MATCH p] [COUNT n] [TYPE t]` (-2), `RENAME` (3), `RENAMENX` (3), `MOVE` (3), `EXPIRE|PEXPIRE|EXPIREAT|PEXPIREAT key t [NX|XX|GT|LT]` (-3), `TTL|PTTL|EXPIRETIME|PEXPIRETIME` (2), `PERSIST` (2), `RANDOMKEY` (1, seeks a random `m` position).

**Strings.** `SET key val [NX|XX] [GET] [EX|PX|EXAT|PXAT|KEEPTTL]` (-3) (`IFEQ`/`IFNE`/digest options not supported), `GET` (2), `GETDEL` (2), `GETEX` (-2), `MGET` (-2, MultiGet), `MSET` (-3), `MSETNX` (-3), `SETNX` (3), `SETEX`/`PSETEX` (4), `GETSET` (3), `INCR`/`DECR` (2), `INCRBY`/`DECRBY` (3), `INCRBYFLOAT` (3, bulk in both protocols as Redis does), `APPEND` (3), `STRLEN` (2), `GETRANGE` (4), `SETRANGE` (4).

**Hashes.** `HSET` (-4), `HSETNX` (4), `HGET` (3), `HMGET` (-3), `HMSET` (-4), `HDEL` (-3), `HLEN` (2), `HEXISTS` (3), `HKEYS`/`HVALS` (2), `HGETALL` (2, map in RESP3), `HINCRBY` (4), `HINCRBYFLOAT` (4), `HSTRLEN` (3), `HRANDFIELD` (-2), `HSCAN` (-3, `NOVALUES` accepted).

**Lists.** `LPUSH`/`RPUSH` (-3), `LPUSHX`/`RPUSHX` (-3), `LPOP`/`RPOP [count]` (-2), `LLEN` (2), `LRANGE` (4), `LINDEX` (3), `LSET` (4), `LREM` (4), `LTRIM` (4), `LINSERT` (5, O(n)), `LPOS` (-3), `LMOVE` (5), `RPOPLPUSH` (3). Blocking variants are not in v1.

**Sets.** `SADD` (-3), `SREM` (-3), `SMEMBERS` (2, set in RESP3), `SISMEMBER` (3), `SMISMEMBER` (-3), `SCARD` (2), `SPOP` (-2), `SRANDMEMBER` (-2), `SMOVE` (4), `SINTER`/`SUNION`/`SDIFF` (-2, set in RESP3), `SINTERSTORE`/`SUNIONSTORE`/`SDIFFSTORE` (-3), `SINTERCARD` (-3), `SSCAN` (-3).

**Sorted sets.** `ZADD key [NX|XX] [GT|LT] [CH] [INCR] score member ...` (-4; `INCR` returns double in RESP3), `ZREM` (-3), `ZSCORE` (3, double in RESP3), `ZMSCORE` (-3), `ZINCRBY` (4, double in RESP3), `ZCARD` (2), `ZCOUNT` (4), `ZLEXCOUNT` (4), `ZRANK`/`ZREVRANK [WITHSCORE]` (-3), `ZRANGE key start stop [BYSCORE|BYLEX] [REV] [LIMIT off n] [WITHSCORES]` (-4; WITHSCORES is `[member, double]` pairs in RESP3), `ZRANGESTORE` (-5), `ZREVRANGE`/`ZRANGEBYSCORE`/`ZREVRANGEBYSCORE`/`ZRANGEBYLEX`/`ZREVRANGEBYLEX` (legacy forms), `ZPOPMIN`/`ZPOPMAX` (-2), `ZREMRANGEBYRANK`/`BYSCORE`/`BYLEX` (4), `ZUNIONSTORE`/`ZINTERSTORE` (-4), `ZUNION`/`ZINTER`/`ZDIFF` (-3), `ZRANDMEMBER` (-2), `ZSCAN` (-3).

**GEO.** `GEOADD key [NX|XX] [CH] lon lat member ...` (-5), `GEOPOS` (-2), `GEODIST m1 m2 [unit]` (-4, bulk with 4 decimals), `GEOHASH` (-2), `GEOSEARCH key FROMMEMBER|FROMLONLAT BYRADIUS|BYBOX [ASC|DESC] [COUNT n [ANY]] [WITHCOORD] [WITHDIST] [WITHHASH]` (-7), `GEOSEARCHSTORE` (-8), `GEORADIUS`/`GEORADIUSBYMEMBER` (-6, deprecated aliases).

**Transactions.** `MULTI` (1), `EXEC` (1), `DISCARD` (1), `WATCH` (-2), `UNWATCH` (1). Exact strings: `-ERR MULTI calls can not be nested`, `-ERR EXEC without MULTI`, `-ERR DISCARD without MULTI`, `-ERR WATCH inside MULTI is not allowed`.

**DOC.\*** (documents are MongoDB Extended JSON on the wire, BSON on disk; replies are Extended JSON bulk strings).
`DOC.CREATE coll [COLUMNS path ...]` (-2); `DOC.DROP coll` (2); `DOC.LIST` (1); `DOC.INSERT coll json [json ...]` (-3) returns `[n, [ids...]]` (map `{n, ids, writeErrors}` in RESP3); `DOC.GET coll id` (3); `DOC.FIND coll filter [PROJECT json] [SORT json] [SKIP n] [LIMIT n] [BATCH n]` (-3) returns `[cursor_id, total_or_-1, [docs]]` (map in RESP3); `DOC.UPDATE coll filter update [MULTI] [UPSERT]` (-4) returns `[n, nModified, upserted_id|nil]`; `DOC.REPLACE coll filter doc [UPSERT]` (-4); `DOC.DELETE coll filter [LIMIT 0|1]` (-3); `DOC.COUNT coll [filter]` (-2); `DOC.CREATEINDEX coll name keys-json [UNIQUE] [SPARSE] [EXPIREAFTERSECONDS n] [S2 coarsest finest maxcells]` (-4); `DOC.DROPINDEX coll name` (3); `DOC.INDEXES coll` (2); `DOC.CURSOR READ id [COUNT n]` / `DOC.CURSOR DEL id` (-3). Query operators: `$eq $ne $gt $gte $lt $lte $in $nin $and $or $nor $not $exists $type $all $elemMatch $size $regex` (RE2 syntax) `$mod $geoWithin $geoIntersects $near $nearSphere`. Update operators: `$set $unset $inc $mul $rename $min $max $currentDate $setOnInsert $push $pop $pull $pullAll $addToSet` with `$each $slice $sort $position`; replacement documents.

**NIL.\* analytics.** `NIL.AGGREGATE coll pipeline-json [CURSOR n] [SNAPSHOT id]` (-3) returns `[cursor_id, [docs]]`; stages `$match $project $addFields $unset $unwind $group $sort $limit $skip $count $geoNear`; accumulators `$sum $avg $min $max $count $first $last $push $addToSet`. `NIL.COUNT coll [filter]` (-2); `NIL.STATS coll field` (3) returns map count/min/max/sum/avg over the column; `NIL.GROUP coll field [COUNT|SUM f|AVG f] [TOP n]` (-3) token form of `$group + $sort + $limit`; `NIL.COLUMNS coll [ADD path ...|DROP path ...]` (-2) declares columnar fields and backfills under a snapshot; `NIL.EXPLAIN coll pipeline-json` (3) returns the chosen plan (`index`, `columnar`, `rowscan`) and estimated rows.

**ROCKS.\* admin** (all Admin-flagged; maps in RESP3, flat arrays in RESP2).
`ROCKS.INFO` (1) version, options digest, CF list; `ROCKS.PROPERTY name [CF cf]` (-2); `ROCKS.CF LIST` (2); `ROCKS.COMPACT [cf]` (-1); `ROCKS.FLUSH [cf]` (-1); `ROCKS.CHECKPOINT dir` (2) returns sequence number; `ROCKS.SNAPSHOT CREATE [TIMEOUT ms] | RELEASE id | LIST` (-2); `ROCKS.SCAN cf start-hex [COUNT n] [SNAPSHOT id]` (-3) raw key/value hex; `ROCKS.GET cf key-hex` (3); `ROCKS.STATS` (1) `Statistics.ToString` when enabled at startup; `ROCKS.INGEST cf path ...` (-3) via `IngestExternalFileCF`; `ROCKS.SETOPTION cf name value` (4) via `SetOptionsCF`.

**Not supported in v1** (published with the same x-mark table style as Kvrocks): blocking list ops (`BLPOP`, `BRPOP`, `BLMOVE`, `BRPOPLPUSH`), Pub/Sub, Streams, Lua (`EVAL`, `SCRIPT`), `OBJECT`, `MEMORY`, `DEBUG`, `SORT`, `SWAPDB`, `COPY`, `DUMP`/`RESTORE`, `MIGRATE`, `CLUSTER`, `CLIENT TRACKING|CACHING|PAUSE|KILL|UNBLOCK`, `SET IFEQ|IFNE|IFDEQ|IFDNE`, `LCS`, `BITOP`/`BITFIELD`/`SETBIT`/`GETBIT`, HyperLogLog, hash field expiration, `ZRANGEBYSCORE`-style `WITHSCORES` on `ZRANK` for RESP2 edge cases beyond Redis 7.2, `$where`, `$jsonSchema`, `$expr`, `$lookup`, `$facet`, `$out`, `$merge`, Decimal128 in indexes or arithmetic, collations, GeometryCollection, MongoDB wire protocol.

## 7. HTAP path

**Execution model.** The server owns `analyticsSem := make(chan struct{}, cfg.AnalyticsMaxConcurrent)` (default 2). An Analytics-flagged command acquires a slot (or replies `-BUSY analytics concurrency limit reached` after 50 ms), acquires a snapshot lease, and runs on the connection's executor goroutine. Point ops on other connections never wait on it: analytics take no key locks, and at most two OS threads are pinned inside RocksDB iterators, leaving the rest of the machine for point operations. Every iterator step is a cgo call that releases the Go scheduler, so goroutine preemption is unaffected.

**Snapshot and iterators.** `snap, _ := store.AcquireSnapshot("agg:"+connID, cfg.AnalyticsSnapshotTTL)`; internally `db.NewSnapshot()`, `ro.SetSnapshot(snap.native)`, `ro.SetFillCache(false)`, `ro.SetReadaheadSize(2<<20)`, `ro.SetAsyncIO(true)`, `ro.SetTotalOrderSeek(true)`, `ro.SetIterateLowerBound(lo)`, `ro.SetIterateUpperBound(hi)`, then `db.NewIteratorCF(ro, cf)`. Release runs in a `defer` when the last batch is sent; cursors keep the lease until `DOC.CURSOR DEL` or the idle timeout.

**Plan choice** (`NIL.EXPLAIN` shows it):
1. `$match` as first stage with an equality or range predicate on an indexed field: `docidx` range scan under the snapshot, then row reads; index-usable predicates are `$eq`, `$gt/$gte/$lt/$lte`, `$in`, anchored `$regex`, compound-prefix rule.
2. Otherwise, if every field referenced by the pipeline is a declared column: `columnar` plan, one `col` iterator per field, k-way merge on `keyenc(_id)`, values decoded from `keyenc` without touching `docs`.
3. Otherwise `rowscan` over `| coll_id |` in `docs`, field access via `bson.Raw.Lookup` (no full unmarshal).

**Operators and memory.** Pull-based chain. `$match` and `$project` stream. `$group` is a hash aggregate keyed by `keyenc(group key)` with accounted state (key bytes + 64 bytes per accumulator + pushed values); `$sort` followed by `$limit`/`$skip` is a bounded heap of size limit+skip with `_id` as tie-breaker; `$sort` alone buffers under the same accountant. The accountant's cap is `cfg.AggMemoryBytes` (default 100 MiB); exceeding it fails the command with `-ERR aggregation exceeded memory limit of 104857600 bytes`, the same contract as MongoDB without `allowDiskUse`. `$count` and `$sum`/`$avg`/`$min`/`$max` without `$group` are constant-space. `$geoNear` is rewritten into the kNN scan of section 8 plus a distance projection.

**Isolation controls.**
1. Background I/O: one `grocksdb.NewGenericRateLimiter(cfg.BgIOBytesPerSec, 100_000, 10, grocksdb.RateLimiterModeAllIo, true)` shared by all CFs so flush and compaction (which analytics-induced cache misses do not increase, but the columnar CF's extra writes do) stay bounded. `ReadOptions.rate_limiter_priority` does not exist in RocksDB's C API (`c.h` has no such symbol), so grocksdb cannot charge our scans; NilDB throttles them in Go: the analytics iterator wrapper counts key+value bytes against a token bucket (`cfg.AnalyticsReadBytesPerSec`, default 256 MiB/s) and sleeps when it overdraws.
2. Cache: `ro.SetFillCache(false)` on every analytics read; the `col` CF has its own `BlockBasedTableOptions` with `SetBlockCache(colCache)`, so a columnar scan cannot evict the row working set even when a planner mistake fills cache.
3. Snapshot leases: `store.AcquireSnapshot` refuses beyond `cfg.SnapshotCap` (default 64) with `-ERR snapshot limit reached`; a janitor goroutine calls `db.ReleaseSnapshot` on expired leases (implicit analytics 30 s, explicit `ROCKS.SNAPSHOT CREATE` up to 300 s); `INFO rocksdb` reports `rocksdb.num-snapshots`, `rocksdb.oldest-snapshot-time` and the lease list.
4. Concurrency: the semaphore above; plus `cfg.AnalyticsMaxRows` (default 50 M rows per command) as a hard stop.

**Measurement hooks.** Every analytics command records rows scanned, bytes scanned, plan, wall time and throttle sleep time; `INFO analytics` exposes totals and a 1-minute histogram. Point-op latency histograms (p50/p99/p999 per command group) live in `INFO commandstats`-style lines so `cmd/htapbench` can read baseline and interference numbers from the server itself.

## 8. Geo

**Redis GEO** reproduces `geohash.c`/`geohash_helper.c`/`geo.c`: `GEOADD` validates lon in [-180, 180] and lat in [-85.05112878, 85.05112878] (`-ERR invalid longitude,latitude pair %f,%f`), encodes 26 steps per axis with the lookup-table interleave (lat in even bits, lon in odd), aligns to 52 bits and does a `ZADD`. `GEOSEARCH` estimates the step from the radius (double until ≥ MERCATOR_MAX 20037726.37, back off 2, minus 1 above 66° and again above 80°, clamp 1..26), builds the bounding box (BYBOX width at the centre latitude), takes the centre cell plus 8 neighbours via `geohash_move_x/y`, lowers the step when the 9 cells do not cover the box, zeroes neighbours outside the box, dedupes identical cell ranges, and runs one `zscore` range scan per cell over `[dbl(h<<shift), dbl((h+1)<<shift))`. Candidates are decoded from the score and filtered by haversine with R = 6372797.560856 m (radius) or by the box test; `COUNT n ANY` stops early, otherwise results are sorted then cut. `GEODIST` prints 4 decimals; `GEOHASH` re-encodes with ±90 latitude to 11 base32 characters; `GEOPOS` decodes cell centres. Correctness fixture in section 9.

**2dsphere (S2).** Package `s2index`:

```go
package s2index

type Params struct{ Coarsest, Finest, MaxCells int } // defaults: ClosestLevel(2000 km), ClosestLevel(110 m), 20

type Geometry struct {
    Kind   Kind            // Point, LineString, Polygon, MultiPoint, MultiLineString, MultiPolygon
    Point  s2.Point
    Lines  []*s2.Polyline
    Polys  []*s2.Polygon
    shape  *s2.ShapeIndex  // lazily built for distance and crossing queries
}

func ParseGeoJSON(v bson.RawValue, strictWinding bool) (Geometry, error) // also accepts legacy [x, y]
func (p Params) Cover(g Geometry) []s2.CellID          // points: one leaf cell; shapes: RegionCoverer{MinLevel: Coarsest, MaxLevel: Finest, LevelMod: 1, MaxCells: MaxCells}
func (p Params) QueryRanges(region s2.Region, maxCells int) (ranges []CellRange, ancestors []s2.CellID)
type CellRange struct{ Lo, Hi s2.CellID }              // inclusive descendant range of a covering cell
func Within(doc, query Geometry) bool                  // exact
func Intersects(doc, query Geometry) bool              // exact
func Distance(doc Geometry, p s2.Point) s1.Angle       // nearest edge or point
type Ring struct{ Inner, Outer s1.Angle; Cells s2.CellUnion } // one kNN round
func NextRing(prev *Ring, center s2.Point, maxOuter s1.Angle, results int) Ring
```

Index time: `Cover` returns one `| index_id | cell | _id |` entry per cell; points store their leaf cell in the value so `$near` never reads the row for distance. Parameters are frozen in the index spec so deletes recompute identical keys. Defaults are MongoDB v3's: `s2.AvgEdgeMetric.ClosestLevel(2000e3/R)`, `ClosestLevel(110/R)`, 20 cells (the exact level numbers are computed at index creation and stored, since the research flagged the table-derived levels 2 and 16 as unconfirmed).

Query time: `QueryRanges` covers the query region with `RegionCoverer{MinLevel: 0, MaxLevel: 30, MaxCells: 8}`; for every covering cell the executor scans `[index_id | cell.RangeMin(), index_id | cell.RangeMax() + 1)` and, for every ancestor from `cell.Parent(Coarsest)` down to `cell.Level()-1`, seeks `| index_id | ancestor |` (a large indexed shape may sit at a coarser cell than the query cell, exactly MongoDB's `S2CellIdsToIntervalsWithParents`). Candidate `_id`s are deduped, the row is read, its geometry parsed and checked exactly.

- `$geoWithin` (`$geometry` Polygon/MultiPolygon, or `$centerSphere` as an `s2.Cap`): points via `Polygon.ContainsPoint`; polylines require every vertex inside and no edge crossing, tested with `s2.NewCrossingEdgeQuery` on the query polygon's `ShapeIndex`; polygons via `Polygon.Contains`. Results are unsorted.
- `$geoIntersects` (any supported GeoJSON): `ContainsPoint`, `Polygon.Intersects`, edge crossings for polylines, `IntersectsCell` short-circuits.
- `$near`/`$nearSphere`/`$geoNear` (query geometry must be a Point; `$minDistance`/`$maxDistance` in metres): expanding annulus. Round 0 covers `Cap(center, r0)` with `r0 = max(minDistance, 1 km)`; each later round covers `Cap(r_k) minus Cap(r_{k-1})` as a `CellUnion` difference against cells already scanned, scans it, computes exact distances, and appends to a sorted buffer. After round k every document with distance ≤ r_k has been seen, so those entries are emitted in order. The increment doubles when a round returned fewer than 300 results and halves above 600 (MongoDB's rule); the scan stops at the limit, at `maxDistance`, or when the union covers the sphere. Distances use the mean Earth radius constant from `golang/geo/s2`'s Earth helpers, recorded in the docs at implementation time; MongoDB's own constant was not verified by the research, so `$geoNear` distances are documented as possibly differing from MongoDB by the ratio of radii.

GeoJSON rules enforced in `ParseGeoJSON`: coordinates are `[lon, lat]`; lon in [-180, 180], lat in [-90, 90]; rings closed (first equals last) with at least four positions; exterior ring first, holes after; loops built with `s2.LoopFromPoints` (duplicate closing point dropped), validated with `Loop.Validate()`, then `Normalize()` so a ring denotes the smaller hemisphere unless the document or query carries `crs: urn:x-mongodb:crs:strictwinding:EPSG:4326`, in which case the counter-clockwise (interior on the left) orientation is kept; polygons assembled with `s2.PolygonFromLoops` (shells even depth, holes odd). Legacy `[x, y]` pairs become Points. GeometryCollection is rejected with a clear error. Null, missing and empty arrays are not indexed (2dsphere is always sparse).

## 9. Test strategy

- **Unit tests per package** (table-driven, `go test ./...` with the Makefile exporting `CGO_CFLAGS=-I/opt/homebrew/include CGO_LDFLAGS=-L/opt/homebrew/lib`): `layout` round-trips and a property test that `dbl()`/`i64()` byte order equals numeric order over 100k random values; `keyenc` property test against a reference comparator implementing MongoDB's comparison-order page over random BSON values, plus descending inversion; `resp` golden bytes for every reply type in both protocols; `lock` deadlock test with sorted multi-key acquisition under `-race`; `store` tests against a temp RocksDB (batch atomicity across CFs, WBWI read-your-writes, snapshot cap, lease expiry, `DeleteRange` then read); `gc` filter tests that flush, `CompactRangeCF` and assert subkeys of deleted/expired/overwritten versions are gone and fresh versions are kept; `matcher`/`update` tables lifted from MongoDB documentation examples; `geohash` vectors captured from a real `redis-server` 7.2 (`GEOADD`, `GEOHASH`, `GEOPOS`, `GEODIST`, `GEOSEARCH` on the Sicily example plus edge cases at ±85° and the antimeridian); `s2index` covering determinism and predicate tests.
- **Raw-socket protocol tests** (`internal/server`, modelled on `tests/unit/protocol.tcl` and Kvrocks `protocol_test.go`): empty query, negative/out-of-range/non-numeric multibulk and bulk lengths, `expected '$', got 'f'`, command split across reads, 64 KiB inline cap, unbalanced quotes, pre-AUTH 10-arg/16 KiB caps, the RESP2 vs RESP3 shape table (`HGETALL`, `SMEMBERS`, `ZSCORE`, `ZRANGE WITHSCORES`, `INFO`, nulls), pipelines of 10 000 commands, and the exact `HELLO 3` map.
- **go-redis v9 integration** (`internal/testutil`, default `Protocol: 3` and again with `Protocol: 2`): CRUD per type, TTL semantics (`SET` clears, `INCR` keeps, `RENAME` transfers), `SCAN` full-iteration guarantee under concurrent inserts, `MULTI`/`EXEC`/`WATCH` including expiry-triggered abort, `DOC.*` CRUD with Extended JSON round-trips (ObjectId, Date, Int64, binary), unique index E11000, cursor paging, `NIL.AGGREGATE` results compared with an in-test brute-force evaluation on the same fixture.
- **redis-cli and redis-benchmark** (`make compat`, needs a Redis install): `redis-cli -3 PING`, `redis-cli --pipe < 100k.txt`, `redis-cli --scan --pattern 'user:*'`, `redis-cli --bigkeys`; `redis-benchmark -q -n 200000 -P 16`, `redis-benchmark -3 -t set,get,incr,lpush,rpush,lpop,rpop,sadd,hset,zadd,zpopmin,lrange`; the upstream TCL suite via `./runtest --host 127.0.0.1 --port 6380 --singledb --ignore-encoding --ignore-digest --tags -needs:debug --tags -needs:repl --tags -needs:save --single unit/type/string` and the same for `hash`, `list`, `set`, `zset`, `incr`, `expire`, `scan`, `multi`, `geo`, `protocol`, with the pass/fail table committed to `docs/compat/tcl-results.md` and regenerated by CI.
- **Geo correctness fixture** (`testdata/geo/`): 5 000 GeoJSON points, 200 polygons (with holes, one crossing the antimeridian, one larger than a hemisphere requiring strictwinding), 100 linestrings; 50 query shapes and 50 `$near` centres. The oracle is the brute-force path: `Within`/`Intersects`/`Distance` over every document without the index. The test asserts the indexed path returns identical sets (and identical order for `$near`), then repeats after deleting 30% of documents and after an `NIL.COLUMNS ADD` backfill to catch stale index entries.
- **HTAP interference measurement** (`cmd/htapbench`): load 1 M documents with four declared columns into `orders`; phase A runs 50 connections of mixed `SET`/`GET`/`HSET`/`DOC.GET` for 60 s and records throughput and p50/p99/p999 from `INFO commandstats`; phase B repeats with a loop of `NIL.AGGREGATE` `$group` by `region` summing `amount` (columnar plan) running back-to-back; phase C repeats with a `rowscan` plan. Report throughput degradation and p99 ratio per phase (the TiDB-style isolation numbers; acceptance for v1: ≤ 10% throughput loss and p99 ≤ 2× under phase B), and freshness: insert a marker document immediately before each aggregate and assert it is counted (expected gap 0).

## 10. Risks and mitigations

| Risk | Mitigation |
|---|---|
| grocksdb v1.11.1 is tested upstream against RocksDB 11.1.2 on Ubuntu only; Homebrew ships 11.8.1 on macOS | `cmd/rocksmoke` is the gate in `make check`; fallback is grocksdb's `build.sh` static RocksDB 11.1.2, not a language change. |
| No compaction filter factory: one shared Go filter across subcompactions | Filters are stateless; the only side effect is a read-only `GetPinnedCF`; versions younger than 60 s skip the lookup entirely; cost measured by the `gc` benchmark before M2 closes. |
| `Get` from inside a compaction callback re-enters RocksDB from a cgo callback | Kvrocks and Pika do the same in C++; RocksDB permits reads from filters. The Go callback runs on a RocksDB thread attached to a runtime M; the M count is bounded by `max_background_jobs`. |
| No `auto_prefix_mode` or capped prefix transform in grocksdb | Every iterator sets both bounds; point-op prefix scans set `PrefixSameAsStart(true)`; analytics set `TotalOrderSeek(true)`. No iterator ever runs past its prefix without a bound. |
| No `ReadOptions.rate_limiter_priority` in the C API | Go-side byte budget plus the concurrency semaphore (section 7). `cmd/htapbench` verifies the effect. |
| cgo overhead (~100-200 ns per call plus copies) makes point ops slower than Redis | `GetPinnedCF` and `KeySlice()/ValueSlice()` avoid copies; `MultiGetCF` for `MGET`/`HMGET`/`MGET`; one `WriteBatch` per command; reply encoding straight into the connection buffer. Target stated in the README: within 3× of Redis on `redis-benchmark -P 16` for `SET`/`GET`; measured, not promised. |
| Held snapshots pin history and stall writes | Lease cap 64, timeouts, janitor, `INFO rocksdb` visibility, `ROCKS.SNAPSHOT LIST`. |
| Compaction filters ignore snapshots since RocksDB 6.0 | Documented: a key expired more than 60 s ago may vanish from a still-open snapshot; the grace window makes this only possible for leases older than 60 s. |
| Range tombstones from collection drops | Cap of 32 outstanding drops, then `CompactRangeCF`; drops never happen inside batches. |
| Counter drift for DBSIZE when the compaction backstop, not the expirer, removes a key | The active expirer runs every 100 ms with a TTL index, so the backstop only fires for keys that expired while the process was down; `FLUSHDB` and startup recount (`ROCKS.RECOUNT` scans `m` under a snapshot) reset counters. |
| WBWI memory inside `EXEC` | Queue cap 10 000 commands; `MULTI` blocks with `DeleteRange`-based commands are refused. |
| `keyenc` numeric collapsing and Decimal128 | Integer suffix keeps large ints exact; Decimal128 rejected in indexed/columnar fields with a clear error until an arithmetic package is chosen. |
| Regex dialect (RE2 vs PCRE2) | Documented subset; `$regex` with backreferences or lookaround returns `-ERR unsupported regex feature`. |
| golang/geo has no tags and marks Polygon "mostly complete" | Pin the pseudo-version in `go.mod`; validate loops with `Loop.Validate()`; no boolean operations promised; geo fixture guards behaviour across upgrades. |
| Version generator depends on the wall clock | Monotonic guard: never issue a version ≤ the last; on clock step-back, continue from last+1 (counter bits absorb bursts). |
| Durability default `everysec` loses ≤ 1 s on power loss | Same contract as Redis `appendfsync everysec`; `--fsync always` sets `WriteOptions.SetSync(true)`. |

## 11. Build order

Each milestone compiles, has passing tests, and ends with `make check` (vet, `go test -race ./...`, rocksmoke). Lanes within a milestone touch disjoint packages.

**M0, toolchain (1 engineer, done except Makefile).** `Makefile` exporting CGO flags, `make check`, CI on macOS arm64. Pin `go.mongodb.org/mongo-driver/v2@v2.9.1` and `github.com/golang/geo` pseudo-version.

**M1, protocol and skeleton.**
- Lane A: `internal/resp` (reader, writer, error table, protocol.tcl tests), `internal/config`.
- Lane B: `internal/layout`, `internal/lock`, `internal/keyenc` (pure, fully testable without RocksDB).
- Lane C: `internal/store` (open with 8 CFs and the options of section 2, batches, iterators, leases, checkpoint, properties) with tests.
Exit: `cmd/nildb` answers `PING`, `ECHO`, `HELLO`, `CLIENT`, `SELECT`, `COMMAND`, `INFO`, `QUIT` over both protocols using `internal/server` and an empty command table.

**M2, keys, strings, TTL.**
- Lane A: `internal/rtypes/strings.go`, `internal/command/{keys,strings,expire,scan}.go`, SCAN cursor table, counters.
- Lane B: `internal/gc` (meta and sub filters, active expirer) with compaction tests.
- Lane C: `internal/rtypes/{hash,set,list}.go` and their command files (compile against the M1 store contract; land when A merges).
Exit: TCL `unit/type/string`, `unit/expire`, `unit/scan`, `unit/keyspace` results recorded; `redis-benchmark` string tests pass.

**M3, collections, transactions, Redis GEO.**
- Lane A: `internal/rtypes/zset.go`, `internal/command/zsets.go`.
- Lane B: `internal/server` MULTI/EXEC/WATCH with WBWI, lock integration, `unit/multi`.
- Lane C: `internal/geohash` with Redis-captured vectors, then `internal/rtypes/geo.go` and `internal/command/geo.go` once A lands.
Exit: TCL `type/hash`, `type/list`, `type/set`, `type/zset`, `geo`, `multi` recorded; go-redis suite green.

**M4, documents.**
- Lane A: `internal/doc/matcher`, `internal/doc/update` (pure).
- Lane B: `internal/doc/index`, `internal/doc` catalog, insert/find/update/delete, unique indexes, cursors, `internal/command/doc.go`.
- Lane C: `internal/s2index` (pure) with the geo fixture oracle, and `internal/columnar` writer/reader.
Exit: DOC.* CRUD, indexes and cursors pass integration tests; 2dsphere index maintained on writes (queries land in M6).

**M5, HTAP.**
- Lane A: `internal/pipeline` planner and executor with memory accounting; `NIL.*` commands; isolation knobs wired (semaphore, byte budget, leases, `colCache`).
- Lane B: `internal/rocksadmin` and `ROCKS.*` commands, `INFO rocksdb`/`analytics`/`commandstats`.
- Lane C: `cmd/htapbench`.
Exit: `NIL.AGGREGATE` chooses `index`/`columnar`/`rowscan` correctly per `NIL.EXPLAIN` tests; htapbench report committed under `docs/perf/`.

**M6, geo queries and hardening.**
- Lane A: `$geoWithin`, `$geoIntersects`, `$near` in `doc` and `$geoNear` in `pipeline`, full geo fixture green.
- Lane B: doc TTL sweeper, `NIL.COLUMNS` backfill, collection drop compaction scheduling, RECOUNT.
- Lane C: `docs/compat/` supported-command table with x-marks, `redis-cli`/`redis-benchmark` CI script, README build and run instructions.
Exit: v1 tag: every command in section 6 implemented and listed, every not-supported item listed, TCL and htapbench results published.