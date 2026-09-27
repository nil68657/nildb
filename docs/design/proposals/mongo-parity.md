> Design-panel proposal, exported on 2026-09-27 from Claude Code run `wf_b09e8159-aff` (agent `abf0f69b668193cbf`, written 2026-09-26). Unedited. Where it disagrees with [../architecture.md](../architecture.md), the architecture file wins.

# NilDB v1 design: MongoDB-parity-first on RocksDB 11.8.1

Scope in one line: a single-node Go 1.27 server at `github.com/nil68657/nildb` that speaks RESP2 (byte-exact) and RESP3 (via `HELLO 3`), stores Redis types and BSON documents in one RocksDB 11.8.1 instance through grocksdb v1.11.1, indexes GeoJSON with S2 cells, and runs aggregation pipelines over snapshots of the same data. The document model comes first: the order-preserving key codec, the matcher, the update operators, cursors and 2dsphere are the core; Redis types are a thin layer sharing the metadata and expiry machinery.

State of the scaffold: the repository has `go.mod` (grocksdb v1.11.1) and `cmd/rocksmoke`, which opens three CFs, writes a multi-CF batch, runs a merge operator, a compaction filter, a snapshot, a bounded iterator, a checkpoint and property reads against Homebrew's `librocksdb.dylib`. That program is the milestone-0 gate; nothing below assumes a feature it does not exercise except WriteBatchWithIndex and DeleteRange, which the grocksdb module source confirms (`db.WriteWI`, `NewWriteBatchWI`, `WriteBatch.DeleteRangeCF`).

**RESP codec decision: in-house.** redcon's writer emits only simple/bulk/integer/error/array/null, its reader produces its own protocol-error text, and it has no per-connection protocol version. Wrapping it means bypassing its writer for every RESP3 type and re-implementing its reader for Redis's exact `Protocol error:` strings anyway. A RESP reader/writer with the Redis limits is about 600 lines and is fully under test control, so NilDB writes it (`internal/resp`).

**Transaction decision: plain `grocksdb.DB`, not TransactionDB.** MULTI/EXEC runs on a WriteBatchWithIndex under an in-process striped lock manager; WATCH is an in-memory touch table; collection drops use DB-level `DeleteRangeCF`, which TransactionDB forbids. Interactive multi-document transactions are out of v1 (FerretDB v2 ships without them too); atomic multi-document writes exist through `MULTI ... DOC.* ... EXEC`.

---

## 1. Package layout

Import edges point downward; the graph is a DAG (`server` never imports the command packages; `cmd/nildb` wires them into the registry).

```
cmd/nildb          flags + config file, opens store, registers command packages, serves.       imports: server, store, catalog, redis, cmddoc, analytics, admin, ttl
cmd/rocksmoke      existing link/feature smoke test (kept as the M0 gate).                      imports: none
cmd/nildb-htapbench OLTP+analytics interference benchmark (section 9).                          imports: testutil

internal/resp      RESP2/RESP3 reader (multibulk+inline, limits, exact Protocol error strings), writer with per-connection proto, Reply values, shared Redis error table.   imports: none
internal/enc       order-preserving BSON value encoder/decoder (type-order table), sign-flipped doubles, string escaping, descending inversion. imports: none (mongo-driver bson)
internal/keys      byte layouts for meta/sub/zscore/doc/idx/geo/col/sys keys and metadata headers; version generator; db byte. imports: enc
internal/store     grocksdb open/options per CF, caches, rate limiter, WBM; Ctx (lazy snapshot + WBWI); LockManager; compaction filters; merge operators; snapshot leases; properties. imports: keys
internal/catalog   collections, indexes, field ids, db epochs in the default CF; in-memory cache; publishes LiveSet to store. imports: store, keys
internal/command   Spec/Registry/Handler contract, arity checks, COMMAND/COMMAND DOCS tables, error helpers. imports: resp, store
internal/server    TCP accept, per-connection reader + executor goroutines, HELLO/AUTH/SELECT/CLIENT state, MULTI queue, WATCH table, SCAN cursor table. imports: resp, command, store
internal/doc       pure BSON semantics: matcher, update operators, projection, sort-key extraction, Extended JSON in/out, path walking. imports: enc
internal/geo       Redis geohash52 + 9-cell search; GeoJSON parse/validate; S2 covering, query plans (ranges+probes), annulus planning, exact refinement, distance. imports: none (golang/geo)
internal/docstore  single-document write path: insert/replace/delete with idx/geo/col maintenance, unique checks, TTL-index keys. imports: store, keys, catalog, enc, doc, geo
internal/query     planner (index selection, bounds), pull operators (idx scan, col merge, doc scan, filter, project, unwind, sort/top-k, group, limit/skip, geoNear), cursors, memory accounting. imports: store, keys, catalog, enc, doc, geo, docstore
internal/redis     Redis types, one file per type (string, hash, list, set, zset, geo, generic/expire, scan). imports: store, keys, command, resp, geo
internal/cmddoc    DOC.* handlers. imports: command, resp, catalog, docstore, query, doc
internal/analytics NIL.* handlers: analytical executor pool, byte throttle, snapshot lease commands, NIL.GEOGROUP. imports: command, resp, store, catalog, query
internal/admin     ROCKS.* handlers and INFO sections. imports: command, resp, store, catalog
internal/ttl       Redis active-expire sampler and document TTL sweeper. imports: store, keys, catalog, docstore
internal/testutil  spawns a server on a free port, raw TCP client, go-redis client, fake clock. imports: server, cmd wiring helpers
```

---

## 2. Column families

One RocksDB instance, eight CFs opened with `OpenDbColumnFamilies`. All CFs share the WAL, so one `WriteBatch` across CFs is atomic and `atomic_flush` stays off.

| CF | Contents | Prefix extractor | Bloom | Merge operator | Compaction filter | Block cache | Compression |
|---|---|---|---|---|---|---|---|
| `default` | system catalog (collections, indexes, field ids, counters, db epochs, layout version) | none | ribbon 10 bits, whole key | none | none | OLTP | LZ4 |
| `meta` | Redis key metadata for every type; string payloads inline | none (user keys vary in length) | `NewRibbonHybridFilterPolicy(10, 1)`, whole key | `nildb.meta` (INCR/INCRBY/INCRBYFLOAT/APPEND operands, tag byte) | `metaFilter`: drop expired | OLTP, high-priority index/filter | none L0-L1, LZ4 mid, ZSTD bottom |
| `sub` | hash fields, set members, list elements, zset member→score | none | ribbon 10 bits, whole key (HGET/SISMEMBER/ZSCORE) | `nildb.sub` (HINCRBY/HINCRBYFLOAT) | `subFilter`: version/type/expiry check against `meta` | OLTP | same |
| `zscore` | zset score index (also Redis GEO) | none | none (never point-read) | none | `subFilter` | OLTP | same |
| `doc` | BSON rows | none | ribbon 10 bits, whole key (`_id` lookups) | none (documents rewritten whole) | `idFilter`: drop rows of dropped collections | OLTP | LZ4 mid, ZSTD bottom, block 16 KiB |
| `idx` | secondary indexes incl. unique, multikey, TTL | none | ribbon 10 bits, whole key (unique-key existence) | none | `idFilter` on idx_id | OLTP | LZ4/ZSTD |
| `geo` | 2dsphere cell entries | `NewFixedPrefixTransform(12)` (idx_id+cell_id); `memtable_prefix_bloom_size_ratio` 0.1 | ribbon 10 bits, `whole_key_filtering=false` (prefix bloom serves ancestor probes) | none | `idFilter` | OLTP | LZ4 |
| `col` | columnar projections of declared fields | none | none (scan only) | none | `idFilter` | separate `analytics` cache, 64 MiB, index/filter blocks only | ZSTD all levels |

DB-wide: `NewWriteBufferManager(256 MiB, allowStall=true)`, `write_buffer_size` 32 MiB per CF, `max_background_jobs` 6, `level_compaction_dynamic_level_bytes` true, `compaction_pri` kMinOverlappingRatio, `bytes_per_sync` 1 MiB, `max_open_files` -1, `periodic_compaction_seconds` 7 days on every CF so filters revisit cold files, `format_version` latest, `NewGenericRateLimiter(200<<20, 100_000, 10, RateLimiterModeWritesOnly, autoTuned=true)`. OLTP cache: `NewHyperClockCache(budget, 0)` with `SetCacheIndexAndFilterBlocksWithHighPriority(true)` and `SetPinL0FilterAndIndexBlocksInCache(true)`; `col` gets its own `BlockBasedTableOptions` via `SetBlockBasedTableFactory`. Durability defaults to Redis `everysec`: `SetManualWALFlush(true)` plus a 1 s `FlushWAL(true)` ticker; `--fsync always` sets `WriteOptions.SetSync(true)`.

---

## 3. Key and value layouts

Notation: `| field (bytes) |`. Integers are big-endian. `BE` marks that where it matters.

**Namespaces.** Redis `SELECT n` (0..15) becomes a 1-byte `db` prefix on `meta`, `sub`, `zscore` keys; `SCAN`/`KEYS`/`DBSIZE`/`FLUSHDB` for db n iterate `[n, n+1)` in `meta`. Documents live in MongoDB-style namespaces `database.collection` resolved by the catalog to a `coll_id`; `SELECT` does not affect `DOC.*`. No key escaping anywhere: `meta` keys are bounded by the db byte and `sub`/`zscore` keys carry a key-length field.

**Metadata (`meta`)**

```
key    | db (1) | user key (n) |
string | flags (1) | expire_ms (8) | payload (n) |
coll   | flags (1) | expire_ms (8) | version (8) | size (8) |
list   | flags (1) | expire_ms (8) | version (8) | size (8) | head (8) | tail (8) |
```
`flags` bits 0-3: type (1 string, 2 hash, 3 list, 4 set, 5 zset); bits 4-7: layout version, 0. `expire_ms` 0 means no expiry. `version` = `| µs timestamp (53 bits) | counter (11 bits) |`, generated when a collection key is created or recreated after DEL/expiry/type overwrite (Kvrocks scheme). Lists start with `head = tail = 1<<63`; live indexes are `[head, tail)`.

**Sub-keys (`sub`)**

```
| db (1) | keylen (4) | key (keylen) | version (8) | subkey (n) |
hash: subkey = field            value = field value
set:  subkey = member           value = empty
list: subkey = index (8)        value = element
zset: subkey = member           value = | score (8, order-preserving) |
```

**Score index (`zscore`)**

```
| db (1) | keylen (4) | key | version (8) | score (8) | member (n) |    value = empty
```
Score encoding: `bits = float64bits(s); if bits>>63 == 1 { bits ^= 0xFFFF_FFFF_FFFF_FFFF } else { bits |= 1<<63 }`, big-endian. `-0` is canonicalised to `+0` at ZADD (documented divergence); NaN is rejected with `ERR value is not a valid float`.

**Redis GEO** is a zset whose score is `float64(geohash52)`; 52 bits fit a double exactly.

**Document rows (`doc`)**

```
| coll_id (4) | enc(_id) (n) |     value = raw BSON document, no header
```
`coll_id` comes from a catalog counter and is never reused.

**Order-preserving value encoding `enc(v)`** (type bytes follow MongoDB KeyString so `bytes.Compare` equals MongoDB's cross-type order):

| BSON type | type byte | payload |
|---|---|---|
| MinKey | 10 | none |
| Undefined | 15 | none |
| Null, missing field | 20 | none |
| Number NaN | 30 | none |
| Number (int32/int64/double/decimal128) | 31 | `| sign-flipped float64(v) (8) | residual (8) |` |
| String, Symbol | 60 | bytes with `0x00 → 0x00 0xFF`, terminator `0x00` |
| Object | 70 | per field: `| value type byte | field name escaped, 0x00 | value payload |`; terminator `0x00` |
| Array | 80 | `enc(elem)` repeated; terminator `0x00` |
| BinData | 90 | `| len (4) | subtype (1) | bytes |` |
| ObjectId | 100 | 12 bytes |
| Bool | 110 | `0x00` / `0x01` |
| Date | 120 | `int64 ms + 2^63` (8) |
| Timestamp | 130 | `| t (4) | i (4) |` |
| Regex | 140 | pattern cstring, options cstring |
| DBPointer | 150 | enc(ns string), oid (12) |
| JavaScript | 160 | enc(code string) |
| CodeWScope | 170 | enc(code), enc(scope object) |
| MaxKey | 240 | none |

Numeric residual: `r = v - trunc(float64(v))` for int64, 0 for doubles and for `|v| <= 2^53`, encoded as `r + 2^63`. This makes `1` and `1.0` encode identically (equality preserved) and orders `2^53` below `2^53+1`. Decimal128 is encoded through `float64` of its decimal string with residual 0 (approximate; the matcher compares Decimal128 exactly via `big.Float`). A descending index field XORs every byte of its encoding with `0xFF`; `0xFF` is never a type byte and is legal inside strings only after the escape, so prefixes stay unambiguous.

**Secondary indexes (`idx`)**

```
non-unique | idx_id (4) | enc(f1) | ... | enc(fk) | enc(_id) |     value = empty
unique     | idx_id (4) | enc(f1) | ... | enc(fk) |               value = | enc(_id) |
```
Missing fields index as Null unless the index is sparse; an array field yields one entry per distinct element, one array field per compound key. A TTL index is a single Date-field index whose catalog entry carries `expireAfterSeconds`; the sweeper scans `[idx_id][enc(Date < now - ttl)]`.

**2dsphere (`geo`)**

```
| idx_id (4) | cell_id (8) | enc(_id) |    value: point  | 0x01 | lat f64 (8) | lng f64 (8) |
                                                  shape  | 0x02 |
```
S2 cell ids as big-endian u64 sort along the Hilbert curve; the descendants of cell C occupy `[C.RangeMin(), C.RangeMax()]` inclusive, so each covering cell is one bounded iterator.

**Columnar (`col`)**

```
| idx_id (4) | enc(_id) |    value = | BSON type byte (1) | BSON element value bytes (n) |
```
One columnar "index" per declared field. A missing field has no entry, so a k-way merge over k columns on `enc(_id)` reproduces missing fields for free.

**System catalog (`default`)**

```
| 0x00 |                              → "nildb-layout-1" (open refuses anything else)
| 0x01 | namespace string |           → | coll_id (4) | created_ms (8) |
| 0x02 | coll_id (4) | index name |   → BSON {key, kind: btree|2dsphere|columnar|ttl, unique, sparse, expireAfterSeconds, s2:{finest,coarsest,maxCells}} + | idx_id (4) |
| 0x03 | "seq" |                      → | next coll_id (4) | next idx_id (4) |
| 0x04 | db (1) |                     → | epoch (8) |   (bumped by FLUSHDB)
```

**SCAN cursors.** Redis cursors must be unsigned 64-bit integers (go-redis parses them with `ParseUint`), and an arbitrary RocksDB key cannot be packed into 8 bytes. Cursors therefore live in a server-side table: id = 48 random bits | 16-bit generation, value = {db, kind, last key, last subkey, expiry}, capacity 65,536, 15-minute idle TTL. `SCAN 0` starts at `[db]`, each call iterates from the stored key with `SetIterateUpperBound([db+1])`, returns `0` and deletes the entry at the end. An unknown or expired cursor replies `ERR invalid cursor`. HSCAN/SSCAN/ZSCAN use the same table with the subkey as position. This is a documented divergence from Redis's stateless cursors.

---

## 4. Concurrency and transactions

**Per-connection FIFO.** Two goroutines per connection: a reader that parses frames into a bounded channel (1024 commands) and an executor that runs them in order and writes replies through a `bufio.Writer`, flushing when the channel drains or at 64 KiB. Replies leave in request order, which is all pipelining requires; `redis-cli --pipe` and `redis-benchmark -P 16` need nothing else.

**Locking.** `store.LockManager` has 4096 `sync.Mutex` stripes. A lock key is `(kind, ns, bytes)`: `(redis, db, key)` for Redis keys, `(doc, coll_id, enc(_id))` for documents, `(uniq, idx_id, enc(values))` for unique-index values. Each `command.Spec.Keys(args)` returns the lock set; the executor sorts stripe indexes, deduplicates, locks ascending, and unlocks after commit. Reads take no locks. Each catalog `Collection` also has a `sync.RWMutex`: point document writes `RLock`, filter-driven multi-document writes (`DOC.UPDATE ... MULTI`, `DOC.DELETE` without `_id` equality) and DDL `Lock`.

**Ctx.** Every command runs inside a `store.Ctx` holding `now`, a lazily created `grocksdb.Snapshot` (created on the first iterator or the second `Get`, so multi-read commands like `HGETALL` and `ZRANGE` see one sequence number), and a `WriteBatchWI` (`NewWriteBatchWI(0, true)`). `Ctx.Get` calls `batch.GetFromDBWithCF`, `Ctx.Iter` wraps `NewIteratorWithBaseCFReadOpts`, so a command's writes are visible to its own later reads. `Ctx.Commit` does one `db.WriteWI`, then `watch.Touch(lockSet)`.

**MULTI/EXEC.** `MULTI` sets the connection's queue. Each queued command is checked for existence and arity at queue time; a failure replies the error immediately and marks the transaction dirty, otherwise `+QUEUED`. `EXEC`: dirty → `-EXECABORT Transaction discarded because of previous errors.`; otherwise take the union of the queued lock sets, lock, check the connection's WATCH-dirty flag (dirty → null array), run every command against one `Ctx`/one WBWI, collect replies (runtime errors are array elements, the batch still commits, as in Redis), `WriteWI` once, unlock, UNWATCH. Commands flagged `NoMulti` (`WATCH`, `MULTI`, `DOC.CURSOR`, `NIL.*`, `ROCKS.*`) are rejected inside MULTI. Because `DOC.INSERT/UPDATE/DELETE` are ordinary handlers, `MULTI; DOC.INSERT ...; DOC.UPDATE ...; EXEC` is the v1 multi-document atomic write.

**WATCH.** Redis's own design: `server.WatchTable` maps lock keys to watching connections. Every committed batch calls `Touch(keys)` and marks watchers dirty; lazy-expiry deletions touch too; `FLUSHDB` marks every watcher of that db dirty. `EXEC`, `DISCARD`, `UNWATCH` and disconnect remove entries.

**Snapshots for reads.** Point reads use `GetPinnedCF` at the Ctx snapshot when one exists, otherwise the live DB. Cursors (`DOC.FIND` batches, `DOC.AGGREGATE`) pin their snapshot for the cursor's life (section 7 caps them). Nothing reads under a lock, so a long scan never blocks a writer.

---

## 5. TTL and garbage

**Lazy expiry.** Decoding `meta` returns "absent" when `expire_ms != 0 && expire_ms <= ctx.now`. The reading command enqueues the key to an expiry worker (channel, depth 4096) that locks the key, re-checks, deletes the metadata, and touches WATCH. Writers that recreate an expired key allocate a new `version`, so old sub-keys are unreachable immediately.

**Active expiry.** `ttl.Sampler` runs every 100 ms per db: `Seek` from a rotating position in `meta`, inspect 20 headers, delete the expired ones, repeat while more than 25% were expired. This keeps `INFO keyspace expires=` close to Redis's and bounds the garbage a lazily-read key leaves behind.

**Compaction filters.** grocksdb has `SetCompactionFilter` but no factory, so one instance per CF is shared by every subcompaction thread. Each filter is a struct of atomics and fixed-size sharded state only:

- `metaFilter` (`meta`): decode header, drop when expired. No DB access.
- `subFilter` (`sub`, `zscore`): parse `| db | keylen | key | version |`, fetch the owning metadata via `db.GetCF(meta)` through a 64-slot cache (`[64]struct{ mu sync.Mutex; key []byte; meta Meta }`, slot = hash(key)&63; sorted input means each subcompaction hits its slot repeatedly), drop when metadata is missing, is a string, has a different version, or is expired. The `*grocksdb.DB` pointer is an `atomic.Pointer` set after open; before it is set the filter keeps everything.
- `idFilter` (`doc`, `idx`, `geo`, `col`): read the leading 4-byte id, drop when it is not in the `LiveSet` (an `atomic.Pointer` to an immutable id set that `catalog` republishes on every DDL). No DB access.

Filters ignore snapshots (RocksDB ≥ 6.0), so an expired Redis key may vanish from a pinned analytical snapshot; lazy expiry would have hidden it anyway, so the observable behaviour is unchanged. `periodic_compaction_seconds` = 7 days guarantees cold files are eventually filtered; `ROCKS.COMPACT` forces it.

**O(1) deletes.** `DEL` of a hash/list/set/zset deletes the metadata entry only (one tombstone); sub-keys are reclaimed by `subFilter`. `DOC.DROP` and `DOC.DROPINDEX` write one plain `WriteBatch` containing the catalog deletes plus `DeleteRangeCF` over `[coll_id]` in `doc` and `[idx_id]` in `idx`/`geo`/`col`, then schedule `CompactRangeCF` on those ranges so range tombstones do not accumulate. `idFilter` is the backstop if a tombstone is ever lost.

**Index, geo and columnar entries of changed or deleted documents** are maintained sync-full: the write path already reads the old document (update semantics need it), computes old index/geo/col keys, deletes them and inserts the new ones in the same batch as the row. Coverings are deterministic for fixed index parameters and parameters are immutable per `idx_id`, so recomputing the old covering is exact and no reverse `_id → cells` entries are needed. Expired documents are removed by the TTL sweeper (`ttl.DocSweeper`, every 60 s per TTL index): snapshot, iterate `[idx_id][enc(Date)]` up to `enc(now - ttl)`, collect up to 1,000 ids, then `docstore.Delete` each under its lock with full index cleanup; at most 50,000 documents or 1 s per index per round.

---

## 6. Command surface v1

Arity uses Redis's convention (`-N` = at least N arguments including the command name). RESP3 shapes are given where they differ from RESP2.

**Connection.** `PING [msg]` (-1), `ECHO` (2), `QUIT` (1), `SELECT` (2, 0..15), `AUTH [user] pass` (-2, single password; user must be `default`), `HELLO [2|3 [AUTH u p] [SETNAME n]]` (-1; RESP3 map of 7 fields `server nildb`? no: `server redis`, `version 7.2.0`, `proto`, `id`, `mode standalone`, `role master`, `modules []`; RESP2 flat 14-element array; other versions `-NOPROTO unsupported protocol version`), `CLIENT ID|SETNAME|GETNAME|INFO|LIST|SETINFO` (-2; SETINFO accepted and stored), `COMMAND [COUNT|INFO|DOCS]` (-1, static table generated from Redis `commands/*.json` for the supported set), `INFO [section]` (-1; sections server, clients, memory, persistence, stats, keyspace, rocksdb; RESP3 verbatim `txt:`), `DBSIZE` (1, `rocksdb.estimate-num-keys` on `meta` per db is not per-db, so DBSIZE iterates `meta[db]` and is O(n); documented), `FLUSHDB|FLUSHALL [SYNC|ASYNC]`, `CONFIG GET|SET` (save, appendonly, databases, maxmemory return fixed values; NilDB keys `nil-*`), `TIME`.

**Keys.** `DEL`/`UNLINK` (-2), `EXISTS` (-2), `TYPE` (2), `KEYS` (2), `SCAN cursor [MATCH p] [COUNT n] [TYPE t]` (-2), `RENAME`/`RENAMENX` (3; copy metadata + sub-keys under a new version, delete old), `EXPIRE|PEXPIRE|EXPIREAT|PEXPIREAT key t [NX|XX|GT|LT]` (-3), `TTL|PTTL|EXPIRETIME|PEXPIRETIME` (2), `PERSIST` (2), `RANDOMKEY` (1; seek to a random 8-byte key, not uniform, documented).

**Strings.** `SET key v [NX|XX] [GET] [EX|PX|EXAT|PXAT|KEEPTTL]` (-3; `IFEQ/IFNE/IFDEQ/IFDNE` unsupported), `GET`, `GETDEL`, `GETEX`, `GETSET`, `MGET` (MultiGetCF), `MSET`, `MSETNX`, `SETNX`, `SETEX`, `PSETEX`, `INCR|DECR|INCRBY|DECRBY` (merge), `INCRBYFLOAT` (bulk string in both protocols, as Redis), `APPEND` (merge), `STRLEN`, `GETRANGE`, `SETRANGE`.

**Hashes.** `HSET` (-4), `HSETNX`, `HGET`, `HMGET`, `HMSET`, `HDEL`, `HEXISTS`, `HLEN`, `HKEYS`, `HVALS`, `HGETALL` (RESP3 map), `HINCRBY`, `HINCRBYFLOAT`, `HSTRLEN`, `HSCAN [NOVALUES]` (RESP2/3 flat array of field,value).

**Lists.** `LPUSH|RPUSH|LPUSHX|RPUSHX`, `LPOP|RPOP [count]`, `LLEN`, `LRANGE`, `LINDEX`, `LSET`, `LTRIM`, `LREM`, `LINSERT` (O(n): renumbers the shorter side), `LPOS`, `LMOVE`, `RPOPLPUSH`.

**Sets.** `SADD`, `SREM`, `SMEMBERS` (RESP3 set `~`), `SISMEMBER`, `SMISMEMBER`, `SCARD`, `SPOP [count]` (first members in byte order, not random; documented), `SRANDMEMBER`, `SSCAN`, `SMOVE`, `SINTER|SUNION|SDIFF` and `*STORE` (in-memory; bounded by `nil-setop-max-members`, default 1,000,000).

**Sorted sets.** `ZADD key [NX|XX] [GT|LT] [CH] [INCR] score member ...` (RESP3 `INCR` returns double), `ZREM`, `ZSCORE`/`ZMSCORE` (RESP3 double), `ZCARD`, `ZCOUNT`, `ZINCRBY` (RESP3 double), `ZRANGE key start stop [BYSCORE|BYLEX] [REV] [LIMIT off n] [WITHSCORES]` (RESP3 `WITHSCORES` → array of `[member, double]` pairs), `ZRANGESTORE`, `ZREVRANGE`, `ZRANGEBYSCORE`, `ZREVRANGEBYSCORE`, `ZRANGEBYLEX`, `ZREVRANGEBYLEX`, `ZRANK|ZREVRANK [WITHSCORE]` (O(rank) scan; documented), `ZREMRANGEBYSCORE|BYRANK|BYLEX`, `ZLEXCOUNT`, `ZPOPMIN|ZPOPMAX [count]`, `ZSCAN`, `ZUNIONSTORE|ZINTERSTORE` (in-memory, same bound as sets).

**GEO.** `GEOADD key [NX|XX] [CH] lon lat member ...`, `GEOPOS`, `GEODIST [M|KM|FT|MI]` (4 decimals), `GEOHASH` (11 chars), `GEOSEARCH key FROMMEMBER|FROMLONLAT BYRADIUS|BYBOX [ASC|DESC] [COUNT n [ANY]] [WITHCOORD] [WITHDIST] [WITHHASH]`, `GEOSEARCHSTORE`, and the deprecated `GEORADIUS`, `GEORADIUSBYMEMBER` (incl. `STORE`/`STOREDIST`) as aliases. Exact Redis error strings for invalid coordinates and option conflicts.

**Transactions.** `MULTI`, `EXEC`, `DISCARD`, `WATCH`, `UNWATCH` with the exact Redis errors (`ERR MULTI calls can not be nested`, `ERR EXEC without MULTI`, `ERR DISCARD without MULTI`, `ERR WATCH inside MULTI is not allowed`).

**DOC.\*** (filters, documents, updates, pipelines are MongoDB Extended JSON strings; documents in replies are canonical Extended JSON bulk strings, or raw BSON with `FORMAT BSON`):

- `DOC.INSERT ns doc [doc ...] [ORDERED yes|no]` → RESP3 map `{n, writeErrors:[{index, code, errmsg}]}`; RESP2 `[n, [[index, code, errmsg]...]]`. Duplicate key → code 11000 with MongoDB's `E11000 duplicate key error collection: ... index: ... dup key: {...}` text.
- `DOC.FIND ns filter [PROJECT p] [SORT s] [SKIP n] [LIMIT n] [BATCH n] [HINT name] [FORMAT EJSON|BSON] [AT lease] [EXPLAIN]` → RESP3 `{cursor:{id, ns, firstBatch:[...]}}`; RESP2 `[id, ns, [docs...]]`. First batch = min(101 docs, 16 MiB) like `find`.
- `DOC.FINDONE ns filter [PROJECT p] [SORT s]` → document or null.
- `DOC.COUNT ns [filter]`, `DOC.DISTINCT ns field [filter]`.
- `DOC.UPDATE ns filter update [MULTI] [UPSERT] [ARRAYFILTERS json]` → `{n, nModified, upsertedId}`; `update` may be an operator document or a pipeline of `$set/$unset/$replaceRoot`.
- `DOC.REPLACE ns filter doc [UPSERT]`.
- `DOC.DELETE ns filter [MULTI]` → `{n}`.
- `DOC.FINDANDMODIFY ns filter UPDATE u|REMOVE [NEW] [UPSERT] [SORT s] [PROJECT p]`.
- `DOC.AGGREGATE ns pipeline [BATCH n] [MAXMEM bytes] [FORMAT ...] [EXPLAIN]` → cursor shape as FIND.
- `DOC.CURSOR READ id [COUNT n]` | `DEL id` | `LIST`.
- `DOC.CREATEINDEX ns keyspec [NAME n] [UNIQUE] [SPARSE] [EXPIREAFTERSECONDS s] [S2 finest coarsest maxcells]` where keyspec is `{"f":1,"g":-1}`, `{"loc":"2dsphere"}` or `{"f":"columnar"}`; builds synchronously under the collection lock (v1 has no background builds).
- `DOC.DROPINDEX ns name`, `DOC.INDEXES ns`, `DOC.COLLECTIONS [database]`, `DOC.CREATE ns`, `DOC.DROP ns`, `DOC.STATS ns` (count, sizes from `GetApproximateSizesCF`).

Query operators: `$eq $ne $gt $gte $lt $lte $in $nin $and $or $nor $not $all $elemMatch $size $exists $type $regex` (RE2 syntax) `$mod $bitsAllSet $bitsAllClear $bitsAnySet $bitsAnyClear $geoWithin $geoIntersects $near $nearSphere`. Update operators: `$set $unset $inc $mul $rename $min $max $currentDate $setOnInsert $push $pop $pull $pullAll $addToSet` (with `$each $slice $sort $position`), positional `$`, `$[]`, `$[id]`, `$bit`. Stages: `$match $project $addFields/$set $unset $unwind $sort $limit $skip $group $count $sortByCount $replaceRoot/$replaceWith $geoNear`. Accumulators: `$sum $avg $min $max $count $push $addToSet $first $last $stdDevPop $stdDevSamp $mergeObjects`. Expression subset: arithmetic, comparison, boolean, `$cond $ifNull $switch`, string (`$concat $toLower $toUpper $substrCP $strLenCP $split`), array (`$size $arrayElemAt $in $concatArrays $slice`), date part extractors and `$dateToString`, `$type` and `$convert`/`$to*`, `$literal`, field paths, `$$ROOT`, `$$CURRENT`.

**NIL.\*** (analytical path, section 7): `NIL.AGGREGATE ns pipeline [BATCH n] [MAXMEM bytes] [TIMEOUT ms] [AT lease] [EXPLAIN]`, `NIL.COUNT ns [filter]`, `NIL.GEOGROUP ns index LEVEL l [filter]` (count of indexed geometries per S2 parent cell, map of cell token → count), `NIL.SNAPSHOT CREATE [TTL s] | RELEASE id | LIST` (RESP3 map with `id`, `seq`, `expires_ms`).

**ROCKS.\*** (admin flag; RESP3 maps, RESP2 flat arrays): `ROCKS.INFO` (linked version from `rocksdb_version`-derived property, CF list, cache sizes), `ROCKS.PROPERTY name [CF cf]` (`GetPropertyCF`/`GetIntPropertyCF`), `ROCKS.CFLIST`, `ROCKS.COMPACT [CF cf] [BEGIN hex END hex]`, `ROCKS.FLUSH [CF cf]`, `ROCKS.FLUSHWAL`, `ROCKS.CHECKPOINT path` (`NewCheckpoint().CreateCheckpoint(path, 0)`), `ROCKS.SNAPSHOTS` (`rocksdb.num-snapshots`, `rocksdb.oldest-snapshot-time`, lease table), `ROCKS.SCAN cf prefix-hex [COUNT n]` and `ROCKS.GET cf key-hex` (raw bytes in hex; the "ldb from the wire" feature).

**Not supported in v1** (published as x-marks in `docs/compat/commands.md`, with Dragonfly's caveat that "supported" is not byte-identical): Pub/Sub, Lua/`FUNCTION`, Streams, HyperLogLog, bitmaps (`SETBIT/GETBIT/BITCOUNT/BITOP/BITFIELD`), blocking list/zset commands, `OBJECT *`, `MEMORY *`, `CLIENT TRACKING/CACHING/UNBLOCK/PAUSE/KILL`, `SWAPDB`, `MOVE`, `COPY`, `DUMP/RESTORE`, `MIGRATE`, `SORT`, `WAIT/WAITAOF`, `SAVE/BGSAVE/BGREWRITEAOF` (use `ROCKS.CHECKPOINT`), replication and `CLUSTER *`, `MONITOR`, `DEBUG`, `LATENCY`, `SLOWLOG`, `ACL *`, `SET IFEQ/IFNE/IFDEQ/IFDNE`, `HEXPIRE` family, `HRANDFIELD`, `ZRANDMEMBER`, `SINTERCARD`, `LCS`, RESP3 push and attribute types. Document side: `$where`, `$jsonSchema`, `$text`, `$expr`, `$lookup`, `$graphLookup`, `$facet`, `$bucket*`, `$setWindowFields`, `$merge`, `$out`, `$unionWith`, `$sample`, collation, sessions and interactive transactions, change streams, the MongoDB wire protocol, 2d/hashed/text/wildcard indexes, compound 2dsphere indexes, background index builds, `allowDiskUse`, Decimal128 arithmetic in `$inc`/`$sum`, regex backreferences and lookaround.

---

## 7. HTAP path

`DOC.AGGREGATE` and `NIL.AGGREGATE` share one planner and operator set; they differ in resources. `DOC.AGGREGATE` runs on the connection's executor with the OLTP cache and a 32 MiB default `MAXMEM`. `NIL.AGGREGATE` runs on the analytics pool.

**Execution of an analytical command.**

1. Take a snapshot lease: `store.Leases.Acquire(ttl)` calls `db.NewSnapshot()`; the table caps total pinned snapshots (leases + cursors) at `nil-max-snapshots` (default 32) and refuses with `ERR too many open snapshots` beyond it. Default lease TTL 60 s, hard maximum 600 s; a lease past its TTL is released by the sweeper and later reads return `ERR snapshot lease expired`. `AT lease` lets several commands read the same sequence number.
2. Plan the source. Options in order: `idx` range scan when the leading `$match` predicates bound an index (equality, `$in`, ranges, anchored `$regex`, compound prefix rule, sort direction match); `col` k-way merge when every field the pipeline touches has a columnar index and no stage needs the whole document; otherwise `doc` scan with `bson.Raw.LookupErr` for lazy field access. `EXPLAIN` returns the stage list, `source`, bounds and estimated rows.
3. ReadOptions for every analytical iterator: `SetSnapshot(lease.Snap)`, `SetFillCache(false)`, `SetIterateLowerBound`/`SetIterateUpperBound`, `SetReadaheadSize(2<<20)`, `SetAsyncIO(true)`, `SetTotalOrderSeek(true)` on `geo` (it has a prefix extractor), `SetDeadline(timeout µs)` so a runaway scan returns `TimedOut` from `Iterator.Err()`.
4. Throttle. grocksdb v1.11.1's `c.h` has no `rocksdb_readoptions_set_rate_limiter_priority` (verified: zero hits), so analytical reads cannot be charged to the RocksDB rate limiter from Go. The analytics executor throttles itself: a `golang.org/x/time/rate` bucket in bytes (`nil-analytics-read-bps`, default 256 MiB/s) charged `len(key)+len(value)` per step, a worker pool of `nil-analytics-threads` goroutines (default 2) so at most two scans run at once, and queued requests waiting up to `TIMEOUT`. The RocksDB limiter (`NewGenericRateLimiter(..., RateLimiterModeWritesOnly, autoTuned=true)`) still smooths the flush and compaction bursts that the `col` CF's extra write amplification produces.
5. Operators are pull-based (`Next() (Row, bool)`). `$match` reuses `doc.Matcher`. `$group` is a hash aggregate whose map key is `enc(groupIdValue)` (the index encoding doubles as a canonical hash key: `1` and `1.0` group together, as MongoDB requires); accounted bytes = keys + accumulator state; exceeding `MAXMEM` (default 100 MiB for `NIL.AGGREGATE`) returns `ERR $group exceeded memory limit of N bytes (allowDiskUse is not supported)`. `$sort` followed by `$limit`/`$skip` becomes a bounded min-heap of `limit+skip` entries keyed by encoded sort keys with `enc(_id)` appended for determinism; a bare `$sort` is an in-memory sort under the same cap. `$limit`/`$skip`/adjacent `$match` fuse. `$geoNear` must be first and becomes the kNN operator of section 8. Results stream through a cursor pinned to the lease.
6. Isolation controls in one place: `WriteBufferManager` (memtables cannot grow past 256 MiB no matter how many CFs), the separate `analytics` HyperClock cache on `col` with `fill_cache=false` (only index/filter blocks stay cached, OLTP blocks are never evicted by scans), the write-only auto-tuned rate limiter, the snapshot cap and lease TTL, the two-goroutine analytics pool and byte bucket, and `rocksdb.num-snapshots`/`rocksdb.oldest-snapshot-time` in `INFO rocksdb`.
7. Freshness is zero: the snapshot is taken at request start and sees every committed write across all CFs.
8. For jobs longer than a lease allows, `ROCKS.CHECKPOINT dir` followed by `nildb --readonly dir` opens the checkpoint with `OpenDbForReadOnlyColumnFamilies` in a second process: full isolation, freshness equal to the checkpoint's age.

---

## 8. Geo

**Redis GEO (`internal/geo`, geohash52).** `GEOADD` validates `lon ∈ [-180,180]`, `lat ∈ [-85.05112878, 85.05112878]` (else `ERR invalid longitude,latitude pair %f,%f`), encodes 26 steps per axis with latitude in even bits and longitude in odd bits (Redis's `interleave64`), and stores `float64(hash)` as the zset score. `GEOSEARCH` follows `geohash_helper.c`: estimate the step by doubling the radius until it exceeds `MERCATOR_MAX = 20037726.37`, subtract 2, subtract 1 more above 66° and again above 80°, clamp to [1,26]; compute the bounding box (BYBOX at the centre latitude), the centre cell and eight neighbours at that step, drop a step if the nine cells fail to cover the box, zero out neighbours outside the box and deduplicate identical cells for huge radii; each surviving cell becomes one `zscore` iterator over `[hash << (52-2·step), (hash+1) << (52-2·step))`; each candidate's hash is decoded to lon/lat and kept if haversine distance (R = 6372797.560856 m) ≤ radius, or for BYBOX if the per-axis distances are within w/2 and h/2. Results are sorted when `ASC|DESC` is given or when `COUNT` lacks `ANY`; `COUNT n ANY` stops at n. `GEODIST` prints `%.4f`; `GEOPOS` returns the decoded cell centre (Redis's drift); `GEOHASH` re-encodes with the ±90° latitude range into 11 base32 characters.

**MongoDB 2dsphere.** Earth radius 6378100 m (MongoDB's `kRadiusOfEarthInMeters`); the two radii differ by design and both are documented.

*GeoJSON parsing.* An object with `type` and `coordinates` (or `geometries` for `GeometryCollection`); positions are `[lon, lat]` with `lat ∈ [-90, 90]`, `lon ∈ [-180, 180]`; a bare `[x, y]` array or `{x, y}` object is a legacy pair converted to a Point (GeoJSON is tried first). Rings need at least four positions, first equal to last, no self-intersection (`s2.Loop.Validate`), exterior ring first, holes inside it. Loops are built with `LoopFromPoints`; without a `crs` the loop is `Normalize()`d so the polygon means the smaller of the two areas it cuts; with `crs: urn:x-mongodb:crs:strictwinding:EPSG:4326` (single ring only) the winding is honoured with interior on the left (counter-clockwise). Rings become `s2.PolygonFromLoops` (nested: shells at even depth, holes at odd). Invalid input returns `ERR Loop is not valid: <reason>` (MongoDB code 2, BadValue).

*Index time.* Parameters per index: finest = `s2.AvgEdgeMetric.ClosestLevel(110 / R)`, coarsest = `ClosestLevel(2_000_000 / R)`, `maxCells` 20 (MongoDB v3 defaults; overridable per index with `S2 finest coarsest maxcells`). Points produce one key at their level-30 leaf cell with lat/lng in the value. LineString, Polygon and Multi\* produce `RegionCoverer{MinLevel: coarsest, MaxLevel: finest, LevelMod: 1, MaxCells: maxCells}.Covering(region)` cells (Polyline and Polygon implement `s2.Region`); `GeometryCollection` and `MultiPoint` are unions of their parts. Null, missing and empty arrays produce nothing (2dsphere is always sparse).

*`$geoWithin`.* Accepts `$geometry` (Polygon, MultiPolygon) and `$centerSphere` (`s2.CapFromCenterAngle`, radius in radians). Legacy flat shapes (`$box`, `$polygon`, `$center`) need a 2d index and return `ERR $geoWithin legacy shapes require a 2d index (not supported)`. Plan: cover the query region with `MaxCells` 16 (`nil-geo-query-max-cells`), and for every covering cell c scan `[idx][c.RangeMin()]..[idx][c.RangeMax()]` inclusive; then for each level from `c.Level()-1` down to the coarsest indexed level probe the prefix `[idx][c.Parent(l)]` with `SetPrefixSameAsStart(true)` (the 12-byte prefix bloom makes misses cheap) because a large shape may be indexed at a coarser cell than the query. Deduplicate `_id`s in a set bounded by `MAXMEM`. Refine each candidate exactly on its stored geometry: point → `Polygon.ContainsPoint`; polyline → every vertex contained and no edge crossing (`CrossingEdgeQuery` over the query polygon's `ShapeIndex`); polygon → `Polygon.Contains`. Without an index the same predicate runs over a collection scan (MongoDB allows this).

*`$geoIntersects`.* Same candidate plan; refinement: point → `ContainsPoint`; polyline vs polygon → any vertex inside or any edge crossing; polygon vs polygon → `Polygon.Intersects`; polyline vs polyline → edge crossing test; point query vs polygon document → the document's `ContainsPoint`.

*`$near` / `$nearSphere` / `$geoNear` (kNN).* Centre must be a Point (MongoDB 8.0 rule). `$minDistance`/`$maxDistance` in metres become angles via `/R`. Expanding annulus, after MongoDB's `geo_near.cpp`: `r_lo = minDistance`, `r_hi = min(maxDistance, 1 km)` initially; each round covers `Cap(P, r_hi)` with 16 cells, subtracts the already-scanned `CellUnion` (`Difference`), scans the remaining ranges plus ancestor probes (each ancestor probed once), computes exact distances for new candidates (points from the index value without reading the document; shapes by reading the document and using `ClosestEdgeQuery` against a per-document `ShapeIndex`, 0 when the polygon contains P), applies the rest of the filter, and pushes into a min-heap. Because the covering is a superset of the cap, every document within `r_hi` has been seen after the round, so results with `dist ≤ r_hi` are emitted in ascending order. Then `r_lo = r_hi`, and the increment doubles when the round returned fewer than 300 results or halves when more than 600, until the limit is met, `r_hi ≥ maxDistance`, or the covering is the whole sphere. `$geoNear` adds `distanceField` (× `distanceMultiplier`), `includeLocs`, `query`, `key` (required when the collection has more than one 2dsphere index).

*Analytics on geo.* `NIL.GEOGROUP` truncates each indexed cell to `Parent(level)` while iterating the index prefix under a snapshot and counts per parent; polygons indexed at cells coarser than `level` count once under their own cell.

---

## 9. Test strategy

**Unit tests per package.** `enc`: a property test drawing 100,000 random pairs of BSON values and asserting `bytes.Compare(enc(a), enc(b))` equals a reference implementation of MongoDB's comparison order, plus round-trip decoding and goldens for the residual trick (`2^53`, `2^53+1`, `-0`, `NaN`, `1` vs `1.0`). `keys`: hex goldens for every layout in section 3. `resp`: a table lifted from `tests/unit/protocol.tcl` and Kvrocks's `protocol_test.go` (empty query, negative/out-of-range/non-numeric lengths, `expected '$', got 'f'`, unbalanced quotes, 64 KiB inline, pre-AUTH caps) asserting exact bytes, and a RESP2-vs-RESP3 shape table for double, map, set, null, verbatim, bool. `doc`: about 300 matcher fixtures (null vs missing under `$ne/$nin/$not/$exists`, type bracketing, array "any element" vs `$elemMatch` vs dotted paths, `$size`, `$all`, regex), update-operator fixtures including positional forms and operator ordering, projection fixtures. `store`: lock manager ordering, WBWI read-your-writes, filters under forced `Flush` + `CompactRangeCF` (expired meta, stale versions, dropped ids vanish). `query`: planner goldens for bounds and index selection. `geo`: Redis's own vectors (`GEOADD Sicily 13.361389 38.115556 Palermo` → hash `3479099956230698`, `GEOHASH` `sqc8b49rny0`, `GEODIST Palermo Catania` `166274.1516`), level mapping asserted via `AvgEdgeMetric.ClosestLevel`. Optional `-tags mongod` differential tests run the matcher/update/aggregation fixtures against a MongoDB 8.0 container via testcontainers and compare results.

**Raw-socket protocol tests.** `testutil.Spawn` starts the server on a free port with a fake clock; tests write bytes and compare replies byte-for-byte: 10,000-command pipelines, `HELLO 2`/`HELLO 3` handshakes, MULTI/EXEC/WATCH sequences from `multi.tcl` (EXECABORT, null array after a touched key, expiry-triggered WATCH failure, FLUSHDB), lazy and active expiry with the clock advanced.

**go-redis v9 integration.** RESP3 by default: strings, hashes, lists, sets, zsets, GEO, transactions, SCAN family; plus the same suite with `Protocol: 2`.

**Redis TCL suite.** `make tcl REDIS_SRC=...` runs `./runtest --host 127.0.0.1 --port 6380 --singledb --ignore-encoding --ignore-digest --tags -needs:debug --tags -needs:repl --tags -needs:save --single unit/type/string` (and hash, list, set, zset, geo, expire, multi, scan, protocol, keyspace) and writes the per-unit pass/fail table to `docs/compat/redis-tcl.md`.

**redis-cli and redis-benchmark in CI.** `redis-cli -p 6380 PING`, `--pipe` with 100k `SET`s ending in `ECHO`, `--scan`, `--bigkeys`, `-3 HGETALL`; `redis-benchmark -p 6380 -t set,get,incr,lpush,rpush,lpop,rpop,sadd,hset,zadd,zpopmin,lrange_100 -n 200000 -P 16 -q` and again with `-3`; zero protocol errors is the gate, p50/p99 are appended to `bench/history.csv`.

**Geo correctness fixture** (`testdata/geo/`, generated deterministically). Redis side: 10,000 points within ±85° and 1,000 `GEOSEARCH` queries (radius and box, several units, ASC/DESC/COUNT/ANY) whose expected member sets and 4-decimal distances come from a brute-force haversine oracle. S2 side: 5,000 points, 500 polygons (convex, concave, with holes, one crossing the antimeridian, one hemisphere-plus with `strictwinding`), 200 polylines; queries for `$geoWithin`, `$geoIntersects` and `$near` (with min/max distance) whose expected results are computed by running the exact predicates over every document without the index. Any set difference is a covering or ancestor bug; kNN ordering must match and distances agree to 1e-6 relative. The `-tags mongod` job replays the same fixture against MongoDB 8.0.

**HTAP interference measurement** (`cmd/nildb-htapbench`). Load 2,000,000 documents with three columnar fields and one btree index, plus 1,000,000 Redis keys. Phase A: 60 s of a 50-connection OLTP mix (`GET/SET/HSET/ZADD` and `DOC.FIND` by `_id`); record QPS, p50, p99. Phase B: the same mix while a loop of `NIL.AGGREGATE` `$match`+`$group` over all documents runs on the `col` path; Phase C: the `doc` path; Phase D: throttle disabled. Report throughput degradation and p99 inflation per phase against TiDB's published ≤10% target, and assert freshness by inserting a marker document immediately before each aggregate and checking it is counted. Output is a markdown table in `bench/results/`.

---

## 10. Risks and mitigations

- **grocksdb gaps.** No compaction filter factory: one shared instance per CF, written as atomics plus a 64-slot sharded cache (section 5); its `Filter` must never touch Go maps or unsynchronised fields. No `ReadOptions.SetAutoPrefixMode`: every iterator sets an explicit upper bound; the only CF with a prefix extractor (`geo`) uses `SetTotalOrderSeek(true)` for range scans and `SetPrefixSameAsStart(true)` for probes. No `rocksdb_readoptions_set_rate_limiter_priority` in `c.h`: analytics throttle in Go (section 7) plus `SetDeadline`. `WriteBatchWI.DeleteRange` exists in Go but the C++ WBWI rejects it: `store` never exposes it; drops use a plain `WriteBatch`. No `Transaction.SetSnapshot`: irrelevant because NilDB does not open a TransactionDB.
- **Homebrew pairing.** grocksdb v1.11.1 is tested upstream against RocksDB 11.1.2 on Ubuntu; NilDB links 11.8.1 on macOS. `cmd/rocksmoke` is the gate, `make check-rocksdb` compares `rocksdb_ldb --version` to 11.8.1, `brew pin rocksdb` is in the setup docs, and `ROCKS.INFO` reports the linked version at runtime. Fallback: build 11.1.2 statically with grocksdb's `build.sh`.
- **cgo overhead.** Every `Get`, `Put` and iterator step is a cgo transition (roughly 100-200 ns on Apple Silicon). Mitigations: one `WriteWI` per command, `MultiGetCF` for `MGET/HMGET/MSET`, `KeySlice()/ValueSlice()` (`OptimizedSlice`, no allocation) in hot iterators, batch reply encoding. The M2 `redis-benchmark` run sets the baseline; the target is ≥100k ops/s for `SET/GET` at `-P 16`, and if it misses, the first fix is coalescing pipelined reads into `MultiGetCF`.
- **Filter re-entrancy cost.** `subFilter` calls `db.GetCF` from inside compaction (Kvrocks does the same); each call is cgo→Go→cgo. The cache keeps it to about one Get per distinct key per subcompaction. If `rocksdb.compaction.times.micros` shows it dominating, the fallback is moving sub-key GC to a background scanner and letting the filter only drop expired metadata.
- **Snapshot pinning.** Long cursors and leases block compaction from dropping overwritten versions. Caps and TTLs in section 7, and `ROCKS.SNAPSHOTS` for visibility.
- **Range tombstones** from frequent collection drops slow reads: `CompactRangeCF` is scheduled after every `DeleteRangeCF`, and `ROCKS.PROPERTY rocksdb.estimate-pending-compaction-bytes` is surfaced.
- **Memory caps without spill.** `$group` and `$sort` fail loudly at `MAXMEM`; `allowDiskUse` is declared unsupported rather than approximated.
- **Numeric index encoding.** The residual scheme is exact for int64/double; Decimal128 is approximate in indexes. Property tests cover the former; the latter is documented.
- **Regex.** Go's RE2 has no backreferences or lookaround; `$regex` documents the subset and rejects unsupported syntax with `ERR Regular expression is invalid`.
- **Redis version string.** `HELLO`/`INFO` report `7.2.0` so clients do not attempt 8.x features (`SET IFEQ`, `HEXPIRE`, client-side caching); the string is a config knob.
- **SCAN cursor table** diverges from Redis's stateless cursors (section 3); documented with the 15-minute idle TTL.
- **golang/geo has no tags** and `Polygon` is "mostly complete": pinned to a pseudo-version in `go.mod`; NilDB uses only `ContainsPoint`, `Contains`, `Intersects`, `ContainsCell/IntersectsCell`, loops and coverers, all exercised by the geo fixture; no boolean operations are promised.
- **Durability default** is `everysec`; a machine crash can lose up to one second, exactly as Redis with the same setting. `--fsync always` is available.

---

## 11. Build order

Each milestone ends with `go build ./... && go test ./...` green and a tagged commit. Engineers are named A-D; file ownership never overlaps within a milestone.

**M0 (done): scaffold.** `go.mod`, `cmd/rocksmoke`, Makefile exporting `CGO_CFLAGS=-I/opt/homebrew/include CGO_LDFLAGS=-L/opt/homebrew/lib`, `brew pin rocksdb`.

**M1: wire, codec, store, strings.** A: `internal/resp`, `internal/command`, `internal/server` (connection loop, HELLO/AUTH/SELECT/CLIENT/COMMAND/PING/ECHO/QUIT, pipelining) with protocol tests. B: `internal/enc`, `internal/keys`, `internal/store` (open, per-CF options, Ctx, LockManager, filters, merge operators) with property and filter tests. C: `internal/doc` (matcher, update operators, projection, Extended JSON) with fixture tests; depends only on `enc`'s type-order table, so it starts on day one. D: `internal/redis/string.go`, `generic.go`, `expire.go`, `internal/ttl/sampler.go`, `cmd/nildb` wiring. Exit: `redis-cli SET/GET/INCR/EXPIRE`, go-redis string suite, `redis-benchmark -t set,get,incr`.

**M2: Redis collections and transactions; document core.** A: `hash.go`, `set.go`, SCAN family and the cursor table. B: `list.go`, `zset.go` (score encoding, ZRANGE forms). D: MULTI/EXEC/WATCH in `server`, `INFO`, `FLUSHDB`. C: `internal/catalog`, `internal/docstore`, `internal/query` (btree planner, doc scan, filter/project/sort/limit, cursors), `internal/cmddoc` (INSERT/FIND/FINDONE/COUNT/UPDATE/REPLACE/DELETE/FINDANDMODIFY/CREATEINDEX/DROP), unique and multikey indexes. Exit: TCL units string/hash/list/set/zset/expire/multi/scan recorded; go-redis full suite; `DOC.*` CRUD with index-backed finds.

**M3: GEO, aggregation, admin.** A: `internal/geo` geohash52 and `internal/redis/geo.go`. B: `internal/admin` (ROCKS.\*, INFO rocksdb), snapshot leases in `store`. C: `$group/$unwind/$sort` top-k, expression evaluator, `DOC.AGGREGATE`, `DOC.DISTINCT`. D: `internal/ttl/docsweeper.go`, TTL indexes, `DOC.STATS`, compat table generator. Exit: TCL geo unit; Redis geo fixture passes; aggregation fixtures pass.

**M4: 2dsphere and HTAP.** A: `internal/geo` GeoJSON parser, coverings, plans, refinement, kNN; `$geoWithin/$geoIntersects/$near` operators in `query` (files `geo_*.go`). B: columnar indexes in `docstore` and the `col` k-way merge operator in `query` (`col_scan.go`); `internal/analytics` executor pool, throttle, `NIL.*`. C: `$geoNear` stage, `NIL.GEOGROUP`, EXPLAIN. D: `cmd/nildb-htapbench`, S2 geo fixture generator and tests. Exit: S2 fixture passes; HTAP bench table produced; `NIL.AGGREGATE` on `col` shows the planner choosing it.

**M5: hardening and release.** All: TCL pass table, `docs/compat/commands.md` supported/unsupported table with footnotes, redis-cli/redis-benchmark CI job, optional `-tags mongod` differential job, `ROCKS.CHECKPOINT` + `--readonly` documented, v1.0 tag.

---

## Go interfaces

```go
// internal/store
package store

type CF uint8

const (
	CFDefault CF = iota
	CFMeta
	CFSub
	CFZScore
	CFDoc
	CFIdx
	CFGeo
	CFCol
)

type Config struct {
	Dir              string
	MemoryBudget     int64         // split: 60% OLTP block cache, 25% write buffers, 64 MiB analytics cache
	Fsync            string        // "always" | "everysec" | "no"
	MaxSnapshots     int
	LeaseTTL, MaxTTL time.Duration
}

type Store struct{ /* db, cf handles, caches, limiter, wbm, filters, leases */ }

func Open(cfg Config) (*Store, error)
func (s *Store) Close() error
func (s *Store) NewCtx(now time.Time) *Ctx
func (s *Store) Locks() *LockManager
func (s *Store) Leases() *Leases
func (s *Store) SetLiveSet(LiveSet)                 // catalog publishes after every DDL
func (s *Store) Handle(cf CF) *grocksdb.ColumnFamilyHandle
func (s *Store) Raw() *grocksdb.DB                   // ROCKS.* only
func (s *Store) WritePlain(wb *grocksdb.WriteBatch, sync bool) error // drops: DeleteRangeCF allowed here
func (s *Store) ScheduleCompact(cf CF, lo, hi []byte)
func (s *Store) Property(cf CF, name string) (string, bool)
func (s *Store) IntProperty(cf CF, name string) (uint64, bool)

// Ctx is one command's (or one EXEC's) read/write context.
type Ctx struct{ /* store, now, snap (lazy), ro, batch *grocksdb.WriteBatchWI, lease *Lease */ }

func (c *Ctx) Now() int64 // unix ms fixed for the command
func (c *Ctx) UseLease(l *Lease)
func (c *Ctx) Get(cf CF, key []byte) (val []byte, ok bool, err error) // WBWI first, then DB at snapshot
func (c *Ctx) MultiGet(cf CF, keys [][]byte) ([][]byte, error)
func (c *Ctx) Iter(cf CF, lo, hi []byte, o IterOpts) Iterator          // hi exclusive; merged with WBWI when dirty
func (c *Ctx) Put(cf CF, key, val []byte)
func (c *Ctx) Delete(cf CF, key []byte)
func (c *Ctx) Merge(cf CF, key, operand []byte)
func (c *Ctx) Dirty() bool
func (c *Ctx) Commit(sync bool) error // one db.WriteWI
func (c *Ctx) Close()                 // release snapshot, destroy batch

type IterOpts struct {
	FillCache        bool
	Readahead        int
	TotalOrderSeek   bool // required on CFGeo range scans
	PrefixSameAsStart bool // CFGeo ancestor probes
	AsyncIO          bool
	Deadline         time.Duration
}

type Iterator interface {
	Seek(key []byte); SeekForPrev(key []byte); SeekToFirst(); SeekToLast()
	Valid() bool; Next(); Prev()
	Key() []byte; Value() []byte // valid until the next move
	Err() error; Close()
}

type LockKind uint8
const (LockRedis LockKind = iota; LockDoc; LockUnique; LockColl)

type LockKey struct {
	Kind LockKind
	NS   uint32 // db for Redis, coll_id / idx_id for documents
	Key  []byte
}

type LockManager struct{ stripes [4096]sync.Mutex }
func (m *LockManager) Lock(keys []LockKey) (release func()) // sorted, deduplicated, deadlock-free

type LiveSet interface{ Live(id uint32) bool }

type Lease struct{ ID uint64; Snap *grocksdb.Snapshot; Seq uint64; Expires time.Time }
type Leases struct{ /* cap, table, sweeper */ }
func (l *Leases) Acquire(ttl time.Duration) (*Lease, error) // ErrTooManySnapshots
func (l *Leases) Get(id uint64) (*Lease, bool)
func (l *Leases) Release(id uint64)
```

```go
// internal/command
package command

type Flags uint16

const (
	Write      Flags = 1 << iota
	ReadOnly
	Admin
	NoMulti     // rejected inside MULTI
	Analytical  // runs on the analytics pool
	NoAuth      // allowed before AUTH (HELLO, AUTH, QUIT)
)

type Spec struct {
	Name  string                              // lowercase: "zadd", "doc.find"
	Arity int                                 // Redis convention; -N means at least N
	Flags Flags
	Keys  func(args [][]byte) []store.LockKey // lock set; nil for none
	Run   Handler
}

type Handler func(c *Ctx, args [][]byte) resp.Reply

type Ctx struct {
	Store *store.Ctx
	Conn  Conn
	Reg   *Registry
}

type Conn interface {
	ID() uint64
	DB() uint8
	SetDB(uint8)
	Proto() int // 2 or 3
	Name() string
	SetName(string)
	Authenticated() bool
	InMulti() bool
	Watch(keys []store.LockKey)
	Unwatch()
	Addr() string
}

type Registry struct{ /* map[string]*Spec */ }
func (r *Registry) Register(specs ...Spec)
func (r *Registry) Lookup(name []byte) (*Spec, bool)
func (r *Registry) CheckArity(s *Spec, argc int) error // "ERR wrong number of arguments for 'x' command"
```

```go
// internal/resp
package resp

type Limits struct {
	InlineMax  int   // 64 KiB
	BulkMax    int64 // 512 MiB
	PreAuthArgs int  // 10
	PreAuthBulk int  // 16 KiB
}

type Reader struct{ /* bufio.Reader, limits, authed flag */ }
func NewReader(r io.Reader, l Limits) *Reader
func (r *Reader) SetAuthenticated(bool)
// ReadCommand returns the argument vector of the next multibulk or inline request.
// A *ProtocolError carries the exact Redis text ("Protocol error: invalid multibulk length");
// the caller replies "-ERR <text>" and closes the connection.
func (r *Reader) ReadCommand() ([][]byte, error)

type Writer struct{ /* bufio.Writer, proto */ }
func NewWriter(w io.Writer) *Writer
func (w *Writer) SetProto(v int)
func (w *Writer) Proto() int
func (w *Writer) Status(s string)         // +s
func (w *Writer) Error(s string)          // -s
func (w *Writer) Int(n int64)
func (w *Writer) Bulk(b []byte)
func (w *Writer) Null()                   // $-1 | _
func (w *Writer) NullArray()              // *-1 | _
func (w *Writer) ArrayHeader(n int)
func (w *Writer) MapHeader(n int)         // %n | *2n
func (w *Writer) SetHeader(n int)         // ~n | *n
func (w *Writer) Double(f float64)        // ,f | bulk string
func (w *Writer) Bool(b bool)             // #t | :1
func (w *Writer) Verbatim(format, s string) // =len\r\ntxt:s | bulk
func (w *Writer) Flush() error

// Reply is a value a handler returns; the executor emits it on the connection's Writer.
type Reply interface{ Emit(w *Writer) }

func OK() Reply
func Int(n int64) Reply
func Bulk(b []byte) Reply
func Str(s string) Reply
func Null() Reply
func NullArray() Reply
func Array(items ...Reply) Reply
func Map(pairs ...Reply) Reply   // even length, key/value alternating
func Set(items ...Reply) Reply
func Double(f float64) Reply
func Bool(b bool) Reply
func Verbatim(format, s string) Reply
func Err(text string) Reply      // text already includes the prefix word: "WRONGTYPE ...", "ERR ..."
func Errorf(format string, a ...any) Reply

// Shared exact strings.
var (
	ErrWrongType   = Err("WRONGTYPE Operation against a key holding the wrong kind of value")
	ErrSyntax      = Err("ERR syntax error")
	ErrNoSuchKey   = Err("ERR no such key")
	ErrOutOfRange  = Err("ERR index out of range")
	ErrNotInteger  = Err("ERR value is not an integer or out of range")
	ErrNotFloat    = Err("ERR value is not a valid float")
	ErrNoAuth      = Err("NOAUTH Authentication required.")
	ErrExecAbort   = Err("EXECABORT Transaction discarded because of previous errors.")
	ErrInvalidCursor = Err("ERR invalid cursor")
)
```

```go
// internal/geo
package geo

// Redis GEO.
type Hash52 uint64

func Encode(lon, lat float64) (Hash52, error) // "ERR invalid longitude,latitude pair %f,%f"
func (h Hash52) Decode() (lon, lat float64)    // cell centre, Redis semantics
func (h Hash52) Base32() string                // 11 chars, ±90 latitude range
func Haversine(lon1, lat1, lon2, lat2 float64) float64 // metres, R = 6372797.560856

type ScoreRange struct{ Min, Max float64 } // half-open [Min, Max) zset score bounds

type Shape struct {
	Lon, Lat float64
	Radius   float64 // metres; 0 when box
	Width, Height float64
}

func SearchCells(s Shape) []ScoreRange              // centre + up to 8 neighbours, deduplicated
func (s Shape) Contains(lon, lat float64) (dist float64, ok bool)

// MongoDB 2dsphere.
type Kind uint8
const (Point Kind = iota; MultiPoint; LineString; MultiLineString; Polygon; MultiPolygon; Collection)

type Geometry struct {
	Kind   Kind
	Points []s2.Point
	Lines  []*s2.Polyline
	Polys  []*s2.Polygon
	Parts  []*Geometry // Collection
}

type ParseOpts struct{ AllowLegacyPair bool }
func ParseGeoJSON(v bson.RawValue, o ParseOpts) (*Geometry, error) // "Loop is not valid: ..." etc.
func (g *Geometry) Region() s2.Region
func (g *Geometry) LatLng() (lat, lng float64, ok bool) // points only

type Params struct{ Finest, Coarsest, MaxCells int }
func DefaultParams() Params // 110 m, 2000 km, 20 at R = 6378100 m
func (p Params) Cover(g *Geometry) []s2.CellID // points: leaf; shapes: RegionCoverer covering

type Range struct{ Lo, Hi s2.CellID } // inclusive descendant range of one covering cell
type Plan struct {
	Ranges []Range     // scan [idx][Lo]..[idx][Hi]
	Probes []s2.CellID // ancestor point probes down to Params.Coarsest
}
func PlanRegion(r s2.Region, p Params, queryMaxCells int) Plan

// PlanAnnulus covers Cap(center, hi) minus cells already scanned; returns the plan and the new scanned union.
func PlanAnnulus(center s2.Point, hi s1.Angle, scanned s2.CellUnion, p Params, queryMaxCells int) (Plan, s2.CellUnion)

type Predicate uint8
const (Within Predicate = iota; Intersects)

func Refine(pred Predicate, query, doc *Geometry) bool
func Distance(from s2.Point, doc *Geometry) s1.Angle // 0 when a polygon contains from
func Metres(a s1.Angle) float64                       // × 6378100
```

The `query` package consumes these through one operator interface, `type Operator interface { Open(*store.Ctx) error; Next() (Row, bool, error); Close() }`, with `Row` holding a `bson.Raw` and a small cache of decoded fields; every stage from `$match` to `$geoNear` implements it, and cursors are operators paused between batches.