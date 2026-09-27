> Design-panel proposal, exported on 2026-09-27 from Claude Code run `wf_b09e8159-aff` (agent `a00954f166796a5a4`, written 2026-09-26). Unedited. Where it disagrees with [../architecture.md](../architecture.md), the architecture file wins.

# NilDB v1 design: a RocksDB-native Redis and Mongo store in Go

NilDB puts one cgo boundary between a client command and RocksDB. Ten column families, each tuned for one storage role, hold every byte; the default bytewise comparator does all ordering, so every key layout below is chosen to sort correctly as raw bytes. Redis and Mongo semantics are functions over key ranges. Anything RocksDB can do (snapshots, checkpoints, compaction, properties, CF listing, WriteBatch atomicity) has a `ROCKS.*` command.

Three decisions the fixed list leaves open, made here:

- **RESP codec in-house, not redcon.** redcon's writer has no map, set, double, null or verbatim types, so every RESP3 reply would go through `WriteRaw` and nothing of redcon would remain but its accept loop. Its handler-per-command model also hides the reader, so the pre-AUTH argument caps and the exact `Protocol error: ...` strings from `networking.c` cannot be enforced. The codec is about 800 lines including tests, with `tests/unit/protocol.tcl` as the specification.
- **Plain `OpenDbColumnFamilies`, not TransactionDB or OptimisticTransactionDB.** A pessimistic TransactionDB adds a C++ lock manager to every write, including autocommit ones. An optimistic DB cannot back WATCH: its commit-time validation reads memtable history only and returns TryAgain once a flush has moved the watched key to an SST, and it cannot see expiry. MULTI/EXEC runs on a WriteBatchWithIndex under Go key locks; WATCH is an in-memory table touched after every commit, which is what Redis itself does.
- **Version-first subkeys.** Every collection element key starts with `| db (1) | version (8) |`, where `version` is unique per key incarnation. That gives a 9-byte `NewFixedPrefixTransform` prefix shared by every element of one hash, set, list or zset, so memtable and SST prefix blooms work for HGETALL, SMEMBERS and ZRANGE. The user key still follows the version so the compaction filter can find the owning metadata. DEL of a collection deletes one metadata key.

## 1. Package layout

| package | responsibility | internal imports |
|---|---|---|
| `cmd/nildb` | flags, config file, open store, start server, signal handling | `config`, `store`, `server` |
| `cmd/rocksmoke` | existing link-and-feature smoke test for the Homebrew RocksDB | none |
| `internal/config` | typed config struct, defaults, flag and file parsing | none |
| `internal/resp` | RESP2/RESP3 reader and writer, Redis limits, exact protocol error strings, shared error table | none |
| `internal/keyenc` | byte codecs: Redis meta/sub keys, version generator, sortable double, BSON keystring with the type-order table, catalog keys | none (bson only) |
| `internal/store` | the only package that imports grocksdb: open, CF options, `Txn` over WriteBatch and WBWI, snapshots and leases, striped key locks, compaction filters, merge operator, admin calls, RocksDB version via cgo header | `keyenc` |
| `internal/geohash` | Redis 52-bit geohash: encode, decode, step estimate, neighbours, score ranges, haversine | none |
| `internal/geo` | S2 2dsphere index: GeoJSON parsing, coverings, key ranges, exact predicates, expanding-ring kNN driver | `keyenc` |
| `internal/rtypes` | Redis data types over `store.Txn`: metadata read with lazy expiry, string, hash, list, set, zset, SCAN cursors, glob matching | `store`, `keyenc` |
| `internal/docs` | collections and indexes catalog, field dictionary, BSON validation, insert/update/delete with index and columnar maintenance, matcher, update operators, TTL sweeper | `store`, `keyenc`, `geo` |
| `internal/pipeline` | aggregation planner and pull-based executor, columnar k-way merge, hash `$group`, top-k sort, expression evaluator, memory accounting, analytics semaphore | `docs`, `store`, `keyenc`, `geo` |
| `internal/command` | command table with arity, flags and key specs; dispatcher; MULTI/EXEC/WATCH state machine; INFO; one file per group (`strings.go`, `hashes.go`, ..., `doc.go`, `nil.go`, `rocks.go`) | `resp`, `store`, `rtypes`, `docs`, `pipeline`, `geo`, `geohash`, `keyenc` |
| `internal/server` | TCP listener, one goroutine per connection, FIFO pipelining, reply buffering, `Session` implementation, snapshot-lease janitor tick | `resp`, `command`, `store` |

The import graph is a DAG: `config`, `resp`, `keyenc`, `geohash` are leaves; `store` and `geo` sit on `keyenc`; `rtypes` and `docs` sit on `store`; `pipeline` on `docs`; `command` on all of those; `server` on `command`; `cmd/nildb` on `server`.

Store layer contract (`internal/store`):

```go
type CF uint8
const (
    CFDefault CF = iota // format marker only
    CFMeta; CFSub; CFZScore; CFDoc; CFIdx; CFGeo; CFCol; CFCatalog; CFTTL
)

func Open(path string, cfg Config) (*Store, error)
func (s *Store) Close() error

// Reader is implemented by *Store (committed state), *snapReader and every Txn.
type Reader interface {
    Get(cf CF, key []byte) ([]byte, error)             // nil, nil when absent; Go-owned copy
    MultiGet(cf CF, keys [][]byte) ([][]byte, error)
    Iter(cf CF, lo, hi []byte, o IterOpts) Iterator    // [lo, hi) via iterate_lower/upper_bound
}
type IterOpts struct{ FillCache bool; Readahead int; LowPriority bool }

type Iterator interface {
    Seek(k []byte); SeekForPrev(k []byte); SeekToFirst(); SeekToLast()
    Valid() bool; Next(); Prev()
    Key() []byte; Value() []byte   // views into C memory, valid until the next move
    Err() error; Close()
}

// Txn is the unit of atomicity. Begin wraps a WriteBatch and reads committed state.
// BeginIndexed wraps a WriteBatchWithIndex(overwrite_key=true) and reads its own writes.
type Txn interface {
    Reader
    Put(cf CF, key, val []byte)
    Delete(cf CF, key []byte)
    Merge(cf CF, key, operand []byte)
    DeleteRange(cf CF, lo, hi []byte) error   // ErrRangeInIndexedTxn under WBWI
    Len() int
    Commit(sync bool) error                   // db.Write / db.WriteWI
    Discard()
}
func (s *Store) Begin() Txn
func (s *Store) BeginIndexed() Txn

type Snapshot struct{ ID uint64; Seq uint64; Created time.Time }
func (s *Store) Snapshot() *Snapshot
func (s *Store) Lease(ttl time.Duration) (*Snapshot, error)   // capped, janitor-released
func (s *Store) Release(*Snapshot)
func (s *Store) At(snap *Snapshot) Reader
func (s *Store) Leases() []*Snapshot

type LockKey struct{ DB uint8; Key []byte }
func (s *Store) Lock(keys ...LockKey) (unlock func())   // sorted, deduplicated stripes

// Admin surface behind ROCKS.*
func (s *Store) Property(cf CF, name string) (string, bool)
func (s *Store) IntProperty(cf CF, name string) (uint64, bool)
func (s *Store) Compact(cf CF, lo, hi []byte) error
func (s *Store) Flush(cf CF) error
func (s *Store) FlushWAL(sync bool) error
func (s *Store) Checkpoint(dir string) (seq uint64, err error)
func (s *Store) ExportCF(cf CF, dir string) error
func (s *Store) ApproxSizes(cf CF, ranges [][2][]byte) []uint64
func (s *Store) LatestSeq() uint64
func (s *Store) CFNames() []string
func (s *Store) Stats() (string, bool)   // Statistics.ToString when enabled
func Version() (major, minor, patch int) // from <rocksdb/version.h> via cgo
```

Command handler contract (`internal/command`):

```go
type Flags uint32
const (
    Write Flags = 1 << iota // takes key locks, touches WATCH table
    ReadOnly
    Admin
    NoMulti                 // rejected inside MULTI: WATCH, FLUSHDB, DOC.DROP, ROCKS.*
    Analytic                // runs under the analytics semaphore on a snapshot
    NoAuth                  // AUTH, HELLO, QUIT before authentication
)
type KeySpec struct{ First, Last, Step int; Fn func(args [][]byte) []store.LockKey }

type Def struct {
    Name  string
    Arity int      // Redis convention; negative means "at least"
    Flags Flags
    Keys  KeySpec
    Run   func(c *Ctx, args [][]byte, w *resp.Writer)  // args excludes the command name
}

type Session interface {
    ID() uint64; DB() uint8; SetDB(uint8); Proto() int; SetProto(int)
    Name() string; SetName(string); Authed() bool; SetAuthed(bool)
    Multi() *MultiState; Close()
}
type Ctx struct {
    Sess  Session
    Store *store.Store
    Txn   store.Txn      // Begin() per autocommit command; the EXEC-wide WBWI inside MULTI
    Snap  *store.Snapshot // set for Analytic commands and multi-key reads
    Now   time.Time
    Cfg   *config.Config
}
var Table map[string]*Def   // populated by init() in each group file
func Dispatch(c *Ctx, args [][]byte, w *resp.Writer)
```

Codec contract (`internal/resp`):

```go
type Limits struct{ InlineMax, BulkMax int; PreAuthArgs, PreAuthBulk int }
type Reader struct{ /* bufio over net.Conn */ }
func NewReader(r io.Reader, l Limits) *Reader
func (r *Reader) SetAuthed(bool)
// Next returns one command (multibulk or inline). A *ProtoError carries the exact
// Redis string ("Protocol error: invalid multibulk length"); the caller replies and closes.
func (r *Reader) Next() ([][]byte, error)
func (r *Reader) Buffered() int   // >0 means more pipelined bytes; flush when it hits 0

type Writer struct{ /* proto, buf, sink */ }
func NewWriter(w io.Writer) *Writer
func NewBufferWriter(proto int) *Writer      // for EXEC replay
func (w *Writer) Proto() int; func (w *Writer) SetProto(int)
func (w *Writer) OK(); func (w *Writer) Simple(string); func (w *Writer) Error(string)
func (w *Writer) Int(int64); func (w *Writer) Bulk([]byte); func (w *Writer) BulkString(string)
func (w *Writer) Null()                // $-1 | _
func (w *Writer) NullArray()           // *-1 | _
func (w *Writer) Double(float64)       // bulk | ,
func (w *Writer) Bool(bool)            // :1/:0 | #t/#f
func (w *Writer) Verbatim(format, s string) // bulk | =txt:
func (w *Writer) Array(n int)          // *n
func (w *Writer) Map(n int)            // *2n | %n
func (w *Writer) Set(n int)            // *n | ~n
func (w *Writer) Raw([]byte); func (w *Writer) Bytes() []byte; func (w *Writer) Flush() error
```

## 2. Column families

All CFs share one DB: `max_background_jobs=6`, `bytes_per_sync=1 MiB`, `max_open_files=-1`, `level_compaction_dynamic_level_bytes=true`, one auto-tuned rate limiter (`NewRateLimiterWithMode(rate, 100000, 10, RateLimiterModeAllIo, true)`), statistics off unless `--rocks-stats`, WAL always on, `manual_wal_flush` per the fsync policy in §4. Two block caches: `main` (`NewHyperClockCache(cfg.BlockCacheBytes, 0)`, default 512 MiB) and `analytics` (`NewHyperClockCache(cfg.AnalyticsCacheBytes, 0)`, default 64 MiB). Every table factory sets `cache_index_and_filter_blocks=true`, `pin_l0_filter_and_index_blocks_in_cache=true`, latest `format_version`. "Ribbon 10" below means `NewRibbonHybridFilterPolicy(10, 1)`.

| CF | contents | prefix extractor | filter policy | whole_key_filtering | merge op | compaction filter | cache | compression | write buffer |
|---|---|---|---|---|---|---|---|---|---|
| `default` | one key `nildb.format=1` | none | none | yes | none | none | main | none | 4 MiB |
| `meta` | Redis key metadata and whole string values | none (SCAN needs total order) | Ribbon 10 | yes | none | `metaFilter` | main | LZ4, ZSTD bottommost | 64 MiB |
| `sub` | hash fields, set members, list elements, zset member→score | fixed 9, memtable prefix bloom ratio 0.1 | Ribbon 10 | yes (HGET, SISMEMBER, ZSCORE, LINDEX) | none | `subFilter` | main | LZ4, ZSTD bottommost | 64 MiB |
| `zscore` | zset score index, Redis GEO | fixed 9, memtable prefix bloom 0.1 | Ribbon 10 | no (range scans only) | none | `subFilter` | main | LZ4, ZSTD bottommost | 32 MiB |
| `doc` | BSON rows | fixed 4 (collection) | Ribbon 10 | yes | none | none | main | LZ4, ZSTD bottommost | 64 MiB |
| `idx` | secondary indexes | fixed 4 (index id) | Ribbon 10 | yes (unique index point lookups) | none | none | main | LZ4, ZSTD bottommost | 32 MiB |
| `geo` | 2dsphere S2 cells | fixed 4 | none | no | none | none | main | LZ4 | 16 MiB |
| `col` | columnar analytical entries | fixed 8 (collection, field) | none | no | none | none | analytics, block size 64 KiB | ZSTD all levels | 64 MiB |
| `catalog` | collections, indexes, field dictionary, counters | none | none | yes | `i64add` | none | main | none | 4 MiB |
| `ttl` | document TTL index | fixed 4 | none | no | none | none | main | LZ4 | 4 MiB |

`meta`, `sub` and `zscore` set `periodic_compaction_seconds` to 7 days so the filters revisit cold files. `i64add` is an associative merge operator over 8-byte little-endian deltas, used only for per-collection document counts; Redis INCR does not use Merge because INCR must return the new value and must refuse non-integers without writing, which a merge operator cannot express.

## 3. Byte-level layouts

Every integer is big-endian unless stated. `db` is the SELECT index, one byte, `databases` config in 1..256 (default 16). Documents are not per-db; collections form one global namespace.

**Redis metadata (`meta`)**

```
key:              | db (1) | user key (n) |
string value:     | type=0x01 (1) | flags (1) | expire_ms (8) | payload (n) |
collection value: | type (1) | flags (1) | expire_ms (8) | version (8) | size (8) |
list adds:        | head (8) | tail (8) |
```

`type`: 1 string, 2 hash, 3 list, 4 set, 5 zset. `expire_ms` is absolute Unix milliseconds, 0 means none. `version = (unix_micros << 11) | counter`, generated by `keyenc.NewVersion()` with a monotonic guard (`max(now, last+1)`), so it is unique across restarts and orders by time. `size` is the element count. `head` and `tail` start at `1<<63`; LPUSH writes at `head-1`, RPUSH at `tail`.

**Redis elements (`sub`, `zscore`)** share prefix `P`:

```
P:                | db (1) | version (8) | klen (2) | user key (klen) |     extractor = first 9 bytes
hash   (sub):     | P | field |               ->  | value |
set    (sub):     | P | member |              ->  (empty)
list   (sub):     | P | index (8) |           ->  | value |
zset   (sub):     | P | member |              ->  | score sortable (8) |
zscore:           | P | score sortable (8) | member |  ->  (empty)
```

Sortable double: `bits := Float64bits(f); if bits>>63 == 1 { bits = ^bits } else { bits |= 1<<63 }`, then big-endian. A full-collection scan is `Iter(cf, P, P+1)` where `P+1` is the prefix with its last byte incremented (with carry).

**Redis GEO** is a zset. GEOADD computes the 52-bit interleaved geohash, converts it to `float64` (exact below 2^53) and writes it as an ordinary score. GEOSEARCH reads `zscore` ranges `[P|enc(lo), P|enc(hi)]` per cell.

**Documents (`doc`)**

```
key:   | coll_id (4) | ks(_id) (n) |     ->  raw BSON document
```

`ks()` is the order-preserving keystring from `internal/keyenc`. `_id` of type array, regex, undefined or MinKey/MaxKey is rejected at insert.

**Keystring type-order table** (first byte of every encoded value; ascending):

| byte | BSON types | payload |
|---|---|---|
| 0x05 | MinKey | none |
| 0x0A | Null, missing field, empty array (multikey) | none |
| 0x14 | int32, int64, double | sub-tag (1) + 0 or 8 bytes, see below |
| 0x1E | String, Symbol | UTF-8 with `0x00 -> 0x00 0xFF`, terminator `0x00` |
| 0x28 | Object | per field: value type byte, escaped field name + `0x00`, value payload; `0x00` terminator |
| 0x32 | Array (only inside `col` values, as raw BSON) | u32 length + bytes |
| 0x3C | BinData | length (4), subtype (1), bytes |
| 0x46 | ObjectId | 12 bytes |
| 0x50 | Bool | 0x00 / 0x01 |
| 0x5A | Date | int64 ms with sign bit flipped |
| 0x64 | Timestamp | u64 |
| 0x6E | Regex | escaped pattern + `0x00`, escaped options + `0x00` |
| 0xFA | MaxKey | none |

Numeric sub-tags keep MongoDB's single numeric class exact across int64 and double: `0x00` NaN; `0x01` negative with |x| ≥ 2^63 (doubles, sortable-double); `0x02` negative with 2^53 ≤ |x| < 2^63 (int64 two's complement; doubles here are integral and convert exactly); `0x03` negative with |x| < 2^53 (sortable-double); `0x04` zero; `0x05`, `0x06`, `0x07` mirror for positives. Decimal128 is stored in documents and compared in the matcher via `big.Float`, but rejected as an index key, as `_id`, and in `$inc`/`$mul`/accumulators (`ERR Decimal128 not supported in v1`). Descending index fields store the encoded value bit-inverted; the trailing `_id` is always ascending. Objects and arrays as whole values are compared in the matcher on BSON, not through keystring, except objects used as `_id`.

**Secondary indexes (`idx`)**

```
non-unique: | index_id (4) | ks(f1) | ... | ks(fk) | ks(_id) |   ->  (empty)
unique:     | index_id (4) | ks(f1) | ... | ks(fk) |            ->  | ks(_id) |
```

Missing fields index as null; sparse indexes skip null and missing; arrays emit one entry per distinct element (at most one array field per compound index). Unique violation is a `Get` on the exact key before the `Put` inside the same locked transaction; the error text is `E11000 duplicate key error collection: <coll> index: <name>`.

**2dsphere (`geo`)**

```
| index_id (4) | cell_id (8) | ks(_id) |   ->  | kind (1) |    0x01 point (cell is its leaf), 0x02 shape
```

**Columnar (`col`)**

```
| coll_id (4) | field_id (4) | ks(_id) |   ->  | ks(value) |
```

`field_id` comes from the per-collection field dictionary in `catalog`. Only fields declared columnar (§7) are written. Nested scalars are declared by dotted path; arrays are stored whole as `0x32` + raw BSON.

**TTL index (`ttl`)**, only for `expireAfterSeconds` indexes on Date fields:

```
| index_id (4) | expire_ms (8) | ks(_id) |   ->  (empty)      expire_ms = date + expireAfterSeconds*1000
```

**Catalog** keys are printable so `ROCKS.SCAN catalog` reads like a directory: `c/<name>` → collection record (BSON: `id`, `created`, `columnar:[paths]`), `i/<coll_id>/<name>` → index record (keys, unique, sparse, ttl, s2 params, `index_id`), `f/<coll_id>/<path>` → `field_id`, `n/<coll_id>` → int64 LE count (merged), `seq/coll`, `seq/index` → counters.

**SCAN cursors.** SCAN iterates `meta` from `| db | lastkey+0x00 |` under a per-call snapshot, returns up to COUNT live keys, and derives the cursor as `xxhash64(lastkey)` rendered as an unsigned decimal, because go-redis parses cursors as `uint64`. The server keeps a bounded map `cursor -> lastkey` (16k entries, 10 min idle); an unknown cursor replies `ERR invalid cursor`. Repeating a call returns the same cursor for the same position, so reconnects work. HSCAN, SSCAN and ZSCAN use the same scheme over `P|element`. Iteration on a total order gives Redis's guarantee that keys present for the whole iteration are returned once.

## 4. Concurrency and transactions

**Connections.** `server` runs one goroutine per connection: `resp.Reader.Next` → `command.Dispatch` → reply appended to the connection's `resp.Writer`; the writer flushes when `Reader.Buffered() == 0`, which batches pipelined replies the way Redis does. Execution is FIFO by construction, so `redis-cli --pipe` (which waits for a trailing ECHO) and `redis-benchmark -P` work without a scheduler.

**Key locks.** `store.Lock` hashes `(db, key)` with xxhash into 4096 `sync.Mutex` stripes, sorts and deduplicates stripe indexes, and locks in order (no deadlock). Write commands lock the keys from their `KeySpec` for the duration of read-modify-write plus commit. Single-key reads lock nothing (a RocksDB `Get` or bounded iterator is consistent on its own). Multi-key reads (MGET, EXISTS, SUNION, ZINTER without STORE) take `Store.Snapshot()` for the command so they are atomic as in Redis.

**Autocommit.** Each command gets `Txn = Store.Begin()` (a WriteBatch). Reads go to the DB; writes accumulate; `Commit` happens once with `Len() > 0`. A read command that meets an expired key deletes it through the same Txn.

**MULTI/EXEC.** MULTI sets `MultiState{queue, dirty}`. Each queued command is validated for existence, arity and `NoMulti` at queue time; failures reply immediately and set `dirty`; otherwise `+QUEUED`. EXEC: if `dirty`, `-EXECABORT Transaction discarded because of previous errors.`; if a watched key was touched, null array. Otherwise EXEC unions the `KeySpec` keys of the queue, takes one `Lock`, opens `Store.BeginIndexed()` (WBWI, `overwrite_key=true`), runs every command against it (reads via `GetFromDBCF` / `NewIteratorWithBaseCF`, so INCR then GET sees the increment), collects each reply into a `NewBufferWriter(proto)`, commits with `db.WriteWI` and replies `*N` followed by the raw buffers. Runtime errors are elements of the array. EXEC and DISCARD always UNWATCH. Commands that need `DeleteRange` (FLUSHDB, FLUSHALL, DOC.DROP) carry `NoMulti` because WBWI has no range deletes.

**WATCH.** `command/watch.go` keeps `map[dbkey]map[*Session]struct{}` under a mutex. After a write command commits and before it releases its locks, the dispatcher calls `Touch(db, key)` for each key in its `KeySpec`; lazy-expiry deletion calls `Touch` too; FLUSHDB marks every watcher of that db dirty. `Touch` sets `dirty` on each watching session. WATCH inside MULTI replies `ERR WATCH inside MULTI is not allowed`; a session's watch set is cleared on disconnect.

**DOC commands.** Each `DOC.*` write is one WriteBatch across `doc`, `idx`, `geo`, `col`, `ttl` and `catalog` under a lock on `(coll_id, ks(_id))`; RocksDB makes the multi-CF batch atomic and recoverable because the WAL is shared. DOC.* commands may be queued inside MULTI, giving multi-document atomicity through the WBWI. There are no interactive multi-document sessions in v1 (FerretDB v2 also ships without them).

**Snapshots for reads.** `Analytic` commands and DOC.FIND cursors read through `Store.At(snap)`. A lease from `Store.Lease(ttl)` is capped at `snapshot-lease-max` (64) concurrent snapshots and released by a janitor after `snapshot-lease-ttl` (60 s); a full table replies `ERR snapshot lease limit reached`. Held snapshots pin overwritten versions, so the cap and TTL are deliberately small.

**Durability.** `wal-fsync=everysec` (default) sets `manual_wal_flush=true` and a 1 s ticker calling `FlushWAL(true)`; `always` sets `WriteOptions.SetSync(true)` per commit; `no` leaves the OS to flush. Unlike Redis `appendfsync everysec`, the default can lose up to 1 s of writes on a process crash, not only on power loss, because the bytes sit in RocksDB's WAL buffer. The README states this.

## 5. TTL and garbage

**Lazy expiry.** `rtypes.GetMeta(r Reader, db, key)` returns `notFound` when `expire_ms != 0 && expire_ms <= now`. Commands with a Txn delete the metadata key (one `Delete`, O(1) regardless of collection size) and `Touch` it. TTL/PTTL return -2, EXISTS 0, TYPE none. There is no active sampler for Redis keys in v1; compaction reclaims them.

**Compaction filters.** grocksdb has no filter factory, so each CF gets one shared instance that RocksDB may call from several subcompaction threads at once. Both filters are pure functions of `(key, value, now)` plus a bounded, mutex-guarded verdict cache:

- `metaFilter` (`meta`): drop when `expire_ms != 0 && expire_ms < now`. No lookups.
- `subFilter` (`sub`, `zscore`): decode `| db | version | klen | key |`, look up the metadata (verdict cache keyed by the 9-byte prefix, 4096 entries, cleared when full, so consecutive elements of one collection cost one `Get`), drop when the metadata is missing, is type string, has a different `version`, or expired more than 300 s ago. The grace window is Kvrocks' and covers clock skew between the filter and the read path.

Filters need the DB handle, which exists only after `Open`, so each holds an `atomic.Pointer[grocksdb.DB]` set right after `OpenDbColumnFamilies`; before it is set they return keep. Filters never run during flush, only compaction. Since RocksDB 6.0 filters ignore snapshots, so a lease taken before a DEL may see a hash's metadata but miss elements already compacted away; the 60 s lease TTL bounds the window and the README says so.

**O(1) collection deletion.** DEL, SET over a collection, RENAME and expiry only touch `meta`. Stale elements are unreachable (readers always go through the metadata's `version`) and are reclaimed by `subFilter`. `ROCKS.COMPACT CF sub` forces reclamation.

**FLUSHDB / FLUSHALL / DOC.DROP.** One WriteBatch with `DeleteRangeCF` over `[db, db+1)` on `meta`, `sub`, `zscore` (FLUSH), or over the collection's prefixes on `doc`, `col`, `ttl` and each index prefix on `idx`, `geo`, plus catalog deletes (DROP). Outside MULTI only. Range tombstones are cheap to write; the server schedules a `CompactRange` over the same range 60 s later.

**Document index and columnar reclamation.** Sync-full maintenance: every update or delete already holds the old document (needed for `nModified` and for `$inc` semantics), so the batch deletes the old `idx`, `geo`, `col` and `ttl` entries and inserts the new ones. Nothing in the document CFs relies on compaction filters. The TTL sweeper runs every 60 s per TTL index: `Iter(ttl, |index_id|, |index_id|now+1|)`, deletes through the normal document delete path in batches of 1000 per WriteBatch, at most 50,000 documents per pass.

## 6. Command surface v1

Arity follows Redis (`-n` = at least n including the name). RESP3 shapes differ only where noted; everything else is identical in both protocols. Reported version is `7.0.0`, `mode standalone`, `role master`, `modules` empty (Kvrocks reports the same and go-redis accepts it).

**Connection** (`NoAuth` where marked): PING [-1], ECHO [2], QUIT [1], AUTH [-2] (single `requirepass`; `HELLO ... AUTH default pw`), HELLO [-1] (RESP2 flat 14-element array, RESP3 `%7`; other versions `-NOPROTO unsupported protocol version`), SELECT [2], CLIENT ID/SETNAME/GETNAME/INFO/LIST/SETINFO [-2] (SETINFO stores LIB-NAME/LIB-VER), COMMAND [-1] with COUNT, INFO, DOCS (static table generated from the `Def` list; DOCS returns summaries only), INFO [-1] (sections server, clients, memory, persistence, stats, keyspace, rocksdb; RESP3 verbatim `=txt:`), DBSIZE [1] (scans `meta` for the db under a snapshot; O(keys), documented), FLUSHDB/FLUSHALL [-1] (SYNC/ASYNC accepted, both synchronous), CONFIG GET [-2] (save, appendonly, databases, maxmemory, plus every NilDB knob), CONFIG SET [-3] (NilDB knobs only), SHUTDOWN [-1], TIME [1].

**Keys**: DEL [-2], UNLINK [-2], EXISTS [-2], TYPE [2], KEYS [2], SCAN [-2] (MATCH, COUNT, TYPE), RENAME [3], RENAMENX [3] (metadata copy with the same version; elements are untouched because `P` carries the version, not the key... see note), EXPIRE/PEXPIRE/EXPIREAT/PEXPIREAT [-3] (NX XX GT LT), TTL/PTTL/EXPIRETIME/PEXPIRETIME [2], PERSIST [2], RANDOMKEY [1] (seek to a random point in the db prefix), TOUCH [-2]. Note on RENAME: because element keys embed the user key after the version, RENAME of a collection rewrites its elements; v1 does this in one WriteBatch and documents RENAME as O(n) for collections and O(1) for strings.

**Strings**: SET [-3] (NX XX GET EX PX EXAT PXAT KEEPTTL), GET [2], GETDEL [2], GETEX [-2], MGET [-2], MSET [-3], MSETNX [-3], SETNX [3], SETEX/PSETEX [4], INCR/DECR [2], INCRBY/DECRBY [3], INCRBYFLOAT [3], APPEND [3], STRLEN [2], GETRANGE [4], SETRANGE [4], GETSET [3].

**Hashes**: HSET [-4], HSETNX [4], HGET [3], HMGET [-3], HMSET [-4], HGETALL [2] (RESP3 map), HDEL [-3], HLEN [2], HEXISTS [3], HKEYS [2], HVALS [2], HINCRBY [4], HINCRBYFLOAT [4], HSTRLEN [3], HRANDFIELD [-2], HSCAN [-3] (RESP3 pairs when WITHVALUES form applies; NOVALUES).

**Lists**: LPUSH/RPUSH [-3], LPUSHX/RPUSHX [-3], LPOP/RPOP [-2], LLEN [2], LINDEX [3], LSET [4], LRANGE [4], LTRIM [4], LREM [4] (O(n) renumber), LINSERT [5] (O(n) renumber), LPOS [-3], LMOVE [5], RPOPLPUSH [3].

**Sets**: SADD [-3], SREM [-3], SMEMBERS [2] (RESP3 set), SISMEMBER [3], SMISMEMBER [-3], SCARD [2], SPOP [-2], SRANDMEMBER [-2], SMOVE [4], SUNION/SINTER/SDIFF [-2] (RESP3 set), SUNIONSTORE/SINTERSTORE/SDIFFSTORE [-3], SINTERCARD [-3], SSCAN [-3].

**Sorted sets**: ZADD [-4] (NX XX GT LT CH INCR; INCR reply RESP3 double), ZREM [-3], ZSCORE [3] (RESP3 double), ZMSCORE [-3], ZCARD [2], ZCOUNT [4], ZLEXCOUNT [4], ZINCRBY [4] (RESP3 double), ZRANGE [-4] (BYSCORE BYLEX REV LIMIT WITHSCORES; RESP3 WITHSCORES = array of `[member, double]`), ZREVRANGE/ZRANGEBYSCORE/ZREVRANGEBYSCORE/ZRANGEBYLEX/ZREVRANGEBYLEX [-4], ZRANK/ZREVRANK [-3] (WITHSCORE), ZPOPMIN/ZPOPMAX [-2], ZRANDMEMBER [-2], ZREMRANGEBYRANK/BYSCORE/BYLEX [4], ZUNIONSTORE/ZINTERSTORE [-4], ZUNION/ZINTER/ZDIFF [-3], ZDIFFSTORE [-4], ZSCAN [-3]. Ranks are computed by counting through the `zscore` range (O(log n) is not available on an LSM; documented O(n)).

**GEO**: GEOADD [-5] (NX XX CH), GEOPOS [-2] (RESP3 coordinates as doubles), GEODIST [-4] (bulk string, 4 decimals, both protocols), GEOHASH [-2], GEOSEARCH [-7] (FROMMEMBER FROMLONLAT BYRADIUS BYBOX ASC DESC COUNT ANY WITHCOORD WITHDIST WITHHASH; WITHCOORD doubles in RESP3), GEOSEARCHSTORE [-8], GEORADIUS/GEORADIUSBYMEMBER [-6] (read-only forms, mapped onto GEOSEARCH).

**Transactions**: MULTI [1], EXEC [1], DISCARD [1], WATCH [-2], UNWATCH [1].

**DOC.\*** (all arguments after the collection are MongoDB Extended JSON v2 strings; replies that return documents use canonical Extended JSON bulk strings):
DOC.INSERT coll doc [doc ...] [-3] → array of `_id`; DOC.GET coll id [3] → bulk or null; DOC.FIND coll filter [PROJECT j] [SORT j] [SKIP n] [LIMIT n] [BATCH n] [SNAPSHOT id] [-3] → RESP2 `[cursor, [docs]]`, RESP3 map `{cursor, docs}` (cursor 0 = exhausted); DOC.CURSOR READ id [COUNT n] / DOC.CURSOR DEL id [-3]; DOC.UPDATE coll filter update [MULTI] [UPSERT] [-4] → RESP2 `[matched, modified, upserted_id|null]`, RESP3 map; DOC.REPLACE coll filter doc [UPSERT] [-4] same shape; DOC.DELETE coll filter [MULTI] [-3] → integer; DOC.COUNT coll [filter] [-2]; DOC.CREATEINDEX coll keys [NAME n] [UNIQUE] [SPARSE] [EXPIREAFTERSECONDS s] [S2 MINLEVEL a MAXLEVEL b MAXCELLS c] [-3] → index name; DOC.DROPINDEX coll name [3]; DOC.INDEXES coll [2] (RESP3 array of maps); DOC.COLLECTIONS [1]; DOC.DROP coll [2] (`NoMulti`); DOC.COLUMNAR coll [ADD|REMOVE path ...] [-2]. Query operators: `$eq $ne $gt $gte $lt $lte $in $nin $and $or $nor $not $exists $type $regex` (RE2 syntax) `$all $elemMatch $size $mod $geoWithin $geoIntersects $near $nearSphere`, with MongoDB's null/missing and array "any element" rules. Update operators: `$set $unset $inc $mul $min $max $rename $currentDate $setOnInsert $push($each $slice $position) $pop $pull $pullAll $addToSet($each)`.

**NIL.\*** (all `Analytic`): NIL.AGGREGATE coll pipeline [SNAPSHOT id] [-3] → RESP2 array of Extended JSON bulk strings, RESP3 array of maps; stages `$match $project $addFields $set $unset $unwind $group $sort $limit $skip $count $geoNear`; NIL.DISTINCT coll path [filter] [-3]; NIL.EXPLAIN coll pipeline [3] → map (path chosen, index, snapshot seq, columnar fields, estimated rows); NIL.STATS coll [2] → map (count from `n/<coll_id>`, `ApproxSizes` per CF, columnar fields, indexes); NIL.KEYSTATS [db] [-1] → map of Redis key counts per type and expiring count from a `meta` scan.

**ROCKS.\*** (`Admin`, `NoMulti`, RESP3 maps, RESP2 flat arrays): ROCKS.INFO [1] (version from `<rocksdb/version.h>`, CF names, cache sizes, fsync policy, lease count); ROCKS.PROPERTY name [CF cf] [-2] (`GetProperty`/`GetPropertyCF`, e.g. `rocksdb.stats`, `rocksdb.estimate-num-keys`, `rocksdb.num-snapshots`, `rocksdb.block-cache-usage`); ROCKS.CF LIST | INFO cf [-2]; ROCKS.SNAPSHOT CREATE | RELEASE id | LIST [-2]; ROCKS.CHECKPOINT dir [CF cf] [-2] (`CreateCheckpoint(dir, 0)` returning the sequence number, or `ExportColumnFamily`); ROCKS.COMPACT [CF cf] [FROM hex TO hex] [-1]; ROCKS.FLUSH [CF cf] [-1]; ROCKS.FLUSHWAL [SYNC] [-1]; ROCKS.STATS [1] (Statistics string, or `ERR statistics disabled, start with --rocks-stats`); ROCKS.SCAN cf [FROM hex] [COUNT n] [SNAPSHOT id] [-2] → hex key/value pairs, an in-process `ldb scan`; ROCKS.SEQ [1]; ROCKS.SIZES cf fromhex tohex [4].

**Not supported in v1** (replies `ERR unknown command` or a specific message): pub/sub (SUBSCRIBE, PUBLISH, PSUBSCRIBE and friends), blocking list ops (BLPOP, BRPOP, BLMOVE, BRPOPLPUSH), Lua and functions (EVAL, SCRIPT, FUNCTION), streams, HyperLogLog, bitmaps (SETBIT..BITFIELD), OBJECT, DEBUG, MEMORY, CLUSTER, SWAPDB, MOVE, COPY, DUMP/RESTORE/MIGRATE, SORT, LCS, SET IFEQ/IFNE/IFDEQ/IFDNE, CLIENT TRACKING/CACHING/UNBLOCK, ACL beyond `requirepass`, SAVE/BGSAVE/BGREWRITEAOF (use ROCKS.CHECKPOINT), REPLICAOF/WAIT, keyspace notifications, hash-field expiry (HEXPIRE), ZRANGESTORE, GEOSEARCH with STORE options other than GEOSEARCHSTORE, RESP3 push and attribute types. Document side: `$where`, `$jsonSchema`, `$text`, `$expr`, `$lookup`, `$graphLookup`, `$facet`, `$unionWith`, `$merge`, `$out`, `$setWindowFields`, `$bucket`, positional update operators (`$`, `$[]`, `$[id]`), `$bit`, collation, Decimal128 arithmetic, `allowDiskUse` spilling, regex features outside RE2 (backreferences, lookaround), interactive multi-document transactions, change streams, the MongoDB wire protocol.

## 7. HTAP path

Freshness is zero by construction: analytics and OLTP read the same RocksDB, and a snapshot taken at command start includes every commit before it. The engineering goes into isolation.

**Execution.** `pipeline.Run` acquires the analytics semaphore (`analytics-max-concurrent`, default 2, wait up to 5 s then `ERR too many analytical queries running`), takes `Store.Snapshot()` (or the caller's lease), builds a plan, pulls rows through an operator chain, and releases the snapshot before the reply is flushed. Every analytical `ReadOptions` is built once per scan:

```go
ro := grocksdb.NewDefaultReadOptions()
ro.SetSnapshot(snap.h)
ro.SetFillCache(false)
ro.SetIterateLowerBound(lo); ro.SetIterateUpperBound(hi)
ro.SetReadaheadSize(2 << 20)
ro.SetAsyncIO(true)
it := db.NewIteratorCF(ro, cf)
```

**Scan choice**, rule-based and printed by NIL.EXPLAIN:
1. `$match` is the first stage and contains an equality or bounded range on a prefix of some index (`$eq`, `$in` as one seek per value, `$gt/$gte/$lt/$lte`, anchored `$regex`): index scan over `idx` yielding `_id`s, then `MultiGet` on `doc` in batches of 64 (one cgo call per batch), then the full matcher. A `$sort` matching the index direction is dropped.
2. Otherwise, if every field referenced by every stage is declared columnar or is `_id`: columnar path. Open one bounded iterator per field on `col` (prefix `|coll_id|field_id|`), all ordered by `ks(_id)`, and k-way merge by `_id`; a field absent for a row is null. Values are already keystring-typed, so `$group` keys need no re-encoding.
3. Otherwise: row scan over `doc` with `bson.Raw.Lookup` for field access (no full unmarshal).

**Columnar declaration.** `DOC.COLUMNAR coll ADD path ...` records the paths in the collection's catalog record, then a background job scans `doc` under a snapshot and backfills `col` in 10 MiB WriteBatches at `IO_LOW`. Writes arriving during backfill maintain `col` too, since the declaration precedes the scan, so the result is consistent. Undeclared collections cost no extra write amplification; declared fields roughly double the write bytes for those fields.

**Operators and memory.** `$match` reuses `docs.Matcher`. `$project/$addFields/$set/$unset` evaluate a small expression set (`$add $subtract $multiply $divide $mod $concat $toLower $toUpper $cond $ifNull $eq..$lte $and $or $not $size $arrayElemAt $year $month $dayOfMonth`, field paths, literals). `$group` is a hash aggregate keyed by the keystring of the `_id` expression with `$sum $avg $min $max $count $first $last $push $addToSet`; state size is accounted (key bytes + 64 + payload) and capped at `analytics-group-mem` (100 MiB, MongoDB's figure) with `ERR $group exceeded memory limit`. `$sort` followed by `$limit n` (with optional `$skip`) becomes a bounded heap of size `n+skip` over `ks(sortkeys)+ks(_id)`, so ties are deterministic; `$sort` without a limit sorts in memory under the same 100 MiB cap. `$unwind` is streaming. `$count`, `$skip`, `$limit` are trivial; adjacent `$match` stages merge and `$match` moves ahead of `$sort` and `$project` when it does not reference projected names. `$geoNear` must be first and rewrites to the `geo.Near` driver of §8 plus a distance field.

**Isolation controls**, all exposed in config and CONFIG SET:
- Block cache separation: `col` uses the `analytics` HyperClockCache; every analytical iterator sets `fill_cache=false`, so a full scan of `doc` cannot evict the OLTP working set from `main`.
- IO priority: the shared limiter runs in `RateLimiterModeAllIo` so flushes (`IO_HIGH`) and compactions (`IO_LOW`) are throttled together. Charging analytical *reads* to the limiter needs `ReadOptions::rate_limiter_priority`; the `c.h` shipped with 11.8.1 has no confirmed setter for it, so M0 checks `options_read.go`. If absent, `pipeline` enforces `analytics-read-bytes-per-sec` (default 200 MiB/s) with a Go token bucket on `len(key)+len(value)` per iterator step, which throttles the scan at the same granularity.
- Concurrency: the semaphore above; analytical goroutines are ordinary connection goroutines, so a long scan blocks only its own connection.
- Snapshot lifetime: lease cap 64, TTL 60 s (`Store.Lease`); DOC.FIND cursors hold an internal snapshot with the same TTL and reply `ERR cursor not found` afterwards. `rocksdb.num-snapshots` and `rocksdb.oldest-snapshot-time` surface through ROCKS.PROPERTY and INFO rocksdb.
- Escape hatch: `ROCKS.CHECKPOINT dir` followed by `OpenDbForReadOnly` from another process gives unlimited-duration analytics with zero interference; the on-disk format is plain RocksDB plus BSON.

## 8. Geo

Two independent implementations, because Redis and MongoDB disagree on latitude range, Earth radius and precision.

**Redis GEO** (`internal/geohash`, constants from `geohash.c` and `geohash_helper.c`): `Encode(lon, lat)` rejects `|lat| > 85.05112878` or `|lon| > 180` with `ERR invalid longitude,latitude pair %f,%f`, scales each axis to 26 bits and interleaves them (lat even bits, lon odd) into a 52-bit integer, which GEOADD stores as the zset score. `Decode` returns the cell centre, so GEOPOS differs slightly from input as in Redis. `String11` re-encodes with latitude in ±90 and emits 11 base32 characters. `Distance` is haversine with R = 6372797.560856 m; GEODIST and WITHDIST print `%.4f`. GEOSEARCH: `StepForRadius` doubles the radius until it exceeds `MERCATOR_MAX = 20037726.37`, backs off two steps, subtracts one above |66°| and another above |80°|, clamps to [1, 26]; the centre cell at that step plus its eight neighbours (`geohash_move_x/y` arithmetic) form the search set; the step drops by one if the nine cells fail to cover the bounding box (BYBOX computes the box at the centre latitude); neighbours entirely outside the box are zeroed; duplicate neighbour ranges at huge radii are skipped. Each remaining cell becomes `ScoreRange(hash, step) = [hash << (52-2*step), (hash+1) << (52-2*step))`, read as one bounded iterator over `zscore`; every candidate is re-checked exactly (haversine ≤ radius, or box containment), then sorted unless COUNT n ANY stops early. Output order and formatting are checked byte-for-byte against a golden file recorded from redis-server (§9).

**2dsphere** (`internal/geo`, golang/geo). Earth radius 6378100 m (MongoDB `kRadiusOfEarthInMeters`; the digest did not read this constant, so M6 confirms it against `geoconstants.h`).

Index-time parameters, stored per index and overridable with `S2 MINLEVEL/MAXLEVEL/MAXCELLS`: `MinLevel = s2.AvgEdgeMetric.ClosestLevel(2000e3 / R)`, `MaxLevel = ClosestLevel(110 / R)`, `MaxCells = 20`, `LevelMod = 1` (MongoDB v3 defaults, computed rather than hard-coded). A Point is indexed as its level-30 leaf cell, one key. Every other geometry gets `RegionCoverer{...}.Covering(region)` and one key per cell. Updates recompute the old covering from the old document with the stored parameters (the coverer is deterministic), so no reverse entries are needed.

GeoJSON parsing: `coordinates` are `[lon, lat]`, lon in [-180, 180], lat in [-90, 90]; a bare two-element numeric array is a legacy Point; supported types are Point, LineString, Polygon, MultiPoint, MultiLineString, MultiPolygon, GeometryCollection. Rings need at least four positions with first equal to last, no self-intersection (`Loop.Validate`), exterior first, holes inside the exterior (every hole vertex passes `shell.ContainsPoint`). Orientation: by default every ring is `Normalize()`d so it encloses the smaller side of the sphere; with `crs: urn:x-mongodb:crs:strictwinding:EPSG:4326` rings are used as given (counter-clockwise exterior means interior on the left), which is how a hemisphere-or-larger polygon is expressed. Polygons are built with `PolygonFromLoops`, so holes at odd depth are excluded.

`$geoWithin` and `$geoIntersects`: cover the query geometry with a query coverer (`MaxCells 16`, levels clamped to the index's range); for each cell `c` emit the range `[|idx|c.RangeMin()|, |idx|c.RangeMax()|+1)` plus one prefix scan `|idx|a|` for every ancestor `a` of `c` down to `MinLevel` (a large indexed shape may sit at a coarser cell than the query cell); `Ranges` sorts and merges overlaps. Candidates are deduplicated by `_id`, loaded from `doc`, parsed, and refined exactly: within uses `Polygon.ContainsPoint` for points, `Polygon.Contains` for polygons, and vertex containment plus no boundary crossing for polylines; intersects uses `ContainsPoint`, `Polygon.Intersects`, and `Polyline.Intersects` against each loop of the query. `$geoWithin` results are unsorted, as in MongoDB.

`$near`, `$nearSphere`, `$geoNear`: expanding annulus. Radius `r0 = max(minDistance, 1 km)` (or `maxDistance` if smaller); each round covers `Cap(center, r_i)` with the query coverer, subtracts the `CellUnion` already scanned, scans those ranges, loads and measures every new candidate exactly (`LatLng.Distance × R` for points, `Polygon.Distance` or `Polyline.Project` for shapes), pushes into a min-heap by distance, and then emits heap entries with `dist ≤ r_i` in order, because every document within `r_i` has been seen once all cells intersecting the cap were scanned. The increment doubles when a round yields fewer than 300 candidates and halves above 600 (MongoDB's `geo_near.cpp` thresholds). Termination: limit reached, `r_i ≥ maxDistance`, or the covering is the whole sphere. Memory is bounded by the current ring's candidates plus a seen-set of `_id`s.

```go
package geo

type Params struct{ MinLevel, MaxLevel, MaxCells int }
func DefaultParams() Params

type Kind uint8 // Point, LineString, Polygon, MultiPoint, MultiLineString, MultiPolygon, Collection
type Geometry struct{ Kind Kind; Points []s2.Point; Lines []*s2.Polyline; Polys []*s2.Polygon; Parts []*Geometry }
func Parse(v bson.RawValue) (*Geometry, error)   // honours the strictwinding crs
func (g *Geometry) Region() s2.Region

type Index struct{ ID uint32; P Params }
func (ix Index) Cells(g *Geometry) []s2.CellID              // leaf for points, covering otherwise
func (ix Index) Keys(g *Geometry, ksID []byte) [][]byte     // full CFGeo keys
type Range struct{ Lo, Hi []byte }
func (ix Index) Ranges(q *Geometry, qp Params) []Range     // descendants + ancestors, merged

func Within(doc, q *Geometry) bool
func Intersects(doc, q *Geometry) bool
func Distance(doc *Geometry, p s2.Point) float64            // metres

type Scan func(r Range, visit func(cell s2.CellID, ksID []byte) bool) error
type Load func(ksID []byte) (*Geometry, error)
func Near(ix Index, center s2.LatLng, minM, maxM float64, limit int,
          scan Scan, load Load, emit func(ksID []byte, metres float64) bool) error
```

## 9. Test strategy

**Unit tests per package.** `keyenc`: property tests that `bytes.Compare(ks(a), ks(b))` matches MongoDB's comparison order over random BSON pairs, including int64/double boundaries at 2^53 and 2^63, and round-trips for every layout in §3. `resp`: table tests for every reply type in both protocols, and a parser table lifted from `protocol.tcl` (empty query, negative and out-of-range lengths, `expected '$', got 'f'`, unbalanced quotes, oversized inline, commands split across reads). `store`: open with all CFs, WriteBatch vs WBWI read-your-writes, DeleteRange refusal under WBWI, lease cap and janitor, both compaction filters against synthetic SSTs (write, flush, `CompactRange`, assert dropped), lock ordering under `-race`. `geohash`: golden vectors from redis-server. `geo`: brute-force oracle (§ below). `rtypes`, `docs`, `pipeline`: semantics tables (null/missing rules, array matching, `$inc` on non-numeric, unique index E11000, TTL sweeper).

**Raw-socket protocol tests** (`tests/protocol`): dial the binary, send byte strings, assert exact reply bytes, including the HELLO 2 and HELLO 3 handshakes, Kvrocks' RESP2/RESP3 conformance table (double, set, map, null, verbatim, ZRANGE WITHSCORES shapes), pipelined `PING\r\nPING\r\nPING\r\n`, and connection close after each protocol error.

**go-redis integration** (`tests/gocase`, modelled on Kvrocks): a helper starts `nildb` on a free port with a temp data dir and returns `redis.NewClient` with `Protocol: 3` and another with `Protocol: 2`; suites per command group, MULTI/WATCH races with two clients, expiry with a fake clock via `--clock-offset-ms` test flag, SCAN full-iteration guarantee under concurrent inserts, DOC.* CRUD and index semantics, NIL.AGGREGATE against an in-memory reference computed in Go.

**redis-cli and redis-benchmark** (`make compat`): `redis-benchmark -q -P 16` and `-3` over the default test list; `redis-cli --pipe` with 100k SETs then DBSIZE; `redis-cli --scan --pattern`, `--bigkeys`, and an interactive session (COMMAND DOCS path). The upstream TCL suite runs with `./runtest --host 127.0.0.1 --port <p> --singledb --ignore-encoding --ignore-digest --tags -needs:debug --tags -needs:repl --tags -needs:save` for `unit/protocol`, `unit/type/{string,incr,hash,list,set,zset}`, `unit/expire`, `unit/multi`, `unit/scan`, `unit/geo`; results land in `docs/compat/redis-tcl.md` per test name, and the README's supported-command table links to it with Dragonfly's caveat that supported does not mean byte-identical everywhere.

**Geo correctness fixture** (`tests/geo/fixture.json`): 10,000 random points plus 200 polygons (small, antimeridian-crossing, polar caps, with holes, one hemisphere-plus with strictwinding) and 100 polylines. For every query geometry and every `$near` centre, expected results come from brute force over all documents with the same golang/geo predicates; the test asserts the indexed path returns the same set (within/intersects) or the same ordered prefix (near, distances within 1e-6 relative). The Redis half replays `GEOADD`, `GEOHASH`, `GEOPOS`, `GEODIST`, `GEOSEARCH` calls against a checked-in transcript recorded from redis-server 7.x and compares reply bytes.

**HTAP interference measurement** (`tests/htap/run.sh`): load 5 M documents with three columnar fields and 1 M Redis strings; run `redis-benchmark -t get,set -c 50 -P 8 -n 3000000` alone and record throughput and p99; rerun while a loop issues `NIL.AGGREGATE` with `$group` over the columnar fields and again with a row-scan pipeline; report the degradation percentages and `rocksdb.block-cache-usage` before and after. Acceptance for v1: throughput drop ≤ 20% and p99 ≤ 2× baseline with default knobs (TiDB reports ≤ 10% with separate servers; a single node will not match that). A freshness test inserts on one connection and aggregates on another in the same millisecond and asserts the row is counted.

## 10. Risks and mitigations

- **grocksdb v1.11.1 against Homebrew 11.8.1 untested upstream.** `cmd/rocksmoke` exercises exactly the features this design relies on (CFs, multi-CF batch, merge, compaction filter, snapshot, bounded iterator, checkpoint, properties). It is the M0 gate; `brew pin rocksdb` freezes the pairing, and `ROCKS.INFO` reports the header version so a drift is visible.
- **No compaction filter factory.** The single instance is shared across subcompaction threads; both filters are stateless apart from a mutex-guarded bounded cache, and a test runs `CompactRange` with `max_subcompactions=4` under `-race`.
- **cgo callback cost in filters.** Every compacted key in `meta`, `sub`, `zscore` re-enters Go. Fast paths (type byte and `expire_ms` comparison) run before any lookup; the verdict cache makes lookups one per collection run; `doc`, `idx`, `col`, `geo`, `ttl` carry no filter. Measured on the 5 M-document dataset in `tests/htap`.
- **No `auto_prefix_mode` in grocksdb.** Every prefix scan sets `iterate_upper_bound` explicitly and never relies on `prefix_same_as_start`; `meta` has no prefix extractor, so SCAN and KEYS are total-order by construction.
- **WBWI has no DeleteRange.** FLUSHDB, FLUSHALL and DOC.DROP are `NoMulti`; the error text says why.
- **Filters ignore snapshots.** Leases are short (60 s) and the README states that a lease may miss elements of a collection deleted and compacted after the snapshot.
- **cgo call overhead per operation** (hundreds of nanoseconds). One `Get` or one bounded iterator plus one `Write` per command; `MultiGet` for MGET and index fan-out; iterator `Key()/Value()` are views, not copies. Target on Apple Silicon: ≥ 100k ops/s for `redis-benchmark -P 16 GET/SET`; if profiles show copies dominating, `GetPinned` replaces `Get` on the metadata path.
- **Read-priority rate limiting may be missing from the C API.** Go token bucket fallback described in §7; the RocksDB limiter still governs flush and compaction.
- **`everysec` fsync semantics differ from Redis** on process crash. Documented; `always` is one flag away.
- **Numeric keystring correctness.** The property test against MongoDB's comparison order is the guard; Decimal128 is excluded from indexes rather than approximated.
- **RE2 vs PCRE.** Regexes needing backreferences or lookaround fail with a clear error instead of matching wrongly.
- **Clock going backwards** would break version uniqueness; `NewVersion` never returns less than `last+1`, and the last version is persisted in `catalog` on shutdown.
- **RENAME of a collection is O(n)** because element keys embed the user key. Documented; strings stay O(1).

## 11. Build order

Each milestone compiles, has passing tests, and is a tag. Parallel tracks touch disjoint packages once M0 fixes the interfaces in §1.

- **M0 (1 engineer, 3 days): toolchain and contracts.** Makefile exporting `CGO_CFLAGS=-I/opt/homebrew/include CGO_LDFLAGS=-L/opt/homebrew/lib`, `go.mod` with grocksdb v1.11.1, mongo-driver v2.9.1, golang/geo pseudo-version; `rocksmoke` green; `internal/config`; `internal/keyenc` complete with property tests; `internal/resp` complete with the protocol table; `internal/store` open/close, CF options table, `Txn`, `Snapshot`, `Lock`, admin calls, filters and merge operator. M0 also answers the two open API questions (`SetAsyncIO`, read rate-limiter priority) by reading `options_read.go`.
- **M1: strings and the server.** `rtypes` metadata, expiry and strings; `command` connection, keys, strings groups and dispatcher; `server` with pipelining; MULTI/EXEC/WATCH over strings. Gate: `redis-benchmark -t ping,set,get,incr -P 16` and the `unit/type/string`, `unit/expire`, `unit/multi`, `unit/protocol` TCL units.
- **M2: collections.** Hash, list, set, zset in `rtypes` and `command`; SCAN family; compaction-filter GC tests. Gate: remaining `unit/type/*` and `unit/scan`.
- **M3: Redis GEO.** `geohash` plus the GEO command file; golden transcript test. Gate: `unit/geo`.
- **M4: documents.** `docs` catalog, BSON validation, insert/get/update/delete, matcher, update operators, secondary indexes, TTL sweeper; `DOC.*` commands and cursors. Gate: go-redis DOC suite.
- **M5: analytics.** `pipeline` planner and operators, columnar declaration and backfill, `NIL.*` commands, memory caps. Gate: aggregation reference tests and NIL.EXPLAIN output for the three paths.
- **M6: 2dsphere.** `geo` package, `$geoWithin/$geoIntersects/$near/$geoNear`, S2 index DDL. Gate: the brute-force fixture.
- **M7: RocksDB surface and isolation.** `ROCKS.*` commands, INFO rocksdb section, lease janitor, analytics cache and read budget, `tests/htap` numbers in the README.
- **M8: compatibility hardening.** Full TCL matrix, `make compat`, supported-command table with footnotes, `docs/compat`.

Parallelism after M0: engineer A owns `rtypes` and the Redis command files (M1–M3); engineer B owns `docs` and `pipeline` (M4–M5) against `store` and `keyenc` only; engineer C owns `geohash` then `geo` (M3, M6), needing only `keyenc` and a `Scan`/`Load` callback pair; engineer D owns `server`, `resp` tests, `tests/protocol`, `tests/gocase` scaffolding, and later `ROCKS.*` (M1, M7). The only shared file is `command/table.go`, which each group file extends through its own `init()`, so merges never conflict.