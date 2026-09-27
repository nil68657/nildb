# NilDB v1 architecture

Final design, synthesised 2026-09-27 from the design panel. The base is the MongoDB-parity proposal ([proposals/mongo-parity.md](proposals/mongo-parity.md)), which the three judges ranked first ([judging.md](judging.md)). Every flaw the judges listed against it is fixed below, and the ideas they asked to graft from the other two proposals are grafted. The earlier amendments memo from the Cursor session is kept at [proposals/amendments-2026-09-27.md](proposals/amendments-2026-09-27.md); this file carries its twelve changes forward and replaces it. Where this file and any proposal disagree, this file wins.

Fixed inputs: Go 1.27.1 with cgo, `github.com/linxGnu/grocksdb` v1.11.1 against Homebrew RocksDB 11.8.1 (`/opt/homebrew/include/rocksdb/version.h` reads 11.8.1), module `github.com/nil68657/nildb`, single node, default bytewise comparator, raw BSON documents through `go.mongodb.org/mongo-driver/v2/bson` v2.9.1, S2 through `github.com/golang/geo` at pseudo-version `v0.0.0-20260818125358-b200a1149890`, Redis GEO as 52-bit geohash scores, RESP2 byte-exact plus RESP3 after `HELLO 3`. Every API name in this document was checked against those module sources on 2026-09-27; two proposals cited names that do not exist (`NewRateLimiterWithMode`, `GetFromDBCF`, a sequence number from `CreateCheckpoint`, `Polygon.Distance`) and none of them appear here.

Scope word from Arka: "basic", meaning a working, testable v1 with an honest supported-command table. The v1 document surface is cut to what one lane can ship; the cut items are listed with a date in section 6.

## Three decisions the fixed inputs leave open

**RESP codec: in-house, not redcon.** redcon speaks RESP2 only: its writer has no map, set, double, null, boolean or verbatim types, its reader emits its own protocol-error text, and it has no per-connection protocol version. Wrapping it means bypassing the writer on every RESP3 reply and re-implementing the reader for Redis's exact `Protocol error:` strings and pre-AUTH limits anyway. The in-house codec is about 700 lines plus a test table lifted from `tests/unit/protocol.tcl`, and NilDB's dependency list stays at grocksdb, mongo-driver/v2/bson and golang/geo. redcon leaves `go.mod`.

**Storage: a plain `grocksdb.DB`, not TransactionDB or OptimisticTransactionDB.** A pessimistic TransactionDB locks every key of every non-transactional write unless `skip_concurrency_control` is set, which defeats it. An optimistic DB validates at commit from memtable history only, returns `TryAgain` once a flush has moved a watched key to an SST, and forbids `DeleteRange`, which is the cheapest `FLUSHDB` and collection drop. So MULTI/EXEC runs on a `WriteBatchWithIndex` under NilDB's own striped locks, WATCH is an in-memory table with an EXEC-time expiry re-check (what Redis 6.2.5+ and Dragonfly do), and drops are one plain `WriteBatch` with `DeleteRangeCF`.

**Sub-keys are keyed by version only.** Every Redis collection (hash, list, set, zset) gets a 64-bit version at creation, unique across all databases. Its elements live under `| version (8) | subkey |`. That gives a fixed 8-byte prefix extractor for memtable and SST prefix blooms on `HGETALL`, `SMEMBERS`, `ZRANGE` and `LRANGE`; makes `DEL`, `RENAME` and `MOVE` of any collection an O(1) metadata rewrite; and lets the sub-key compaction filter decide with one cached existence lookup of the version. This is the HTAP-first layout with the verdict cache the judges asked for; it replaces the MongoDB-parity proposal's `| db | keylen | key | version | subkey |`, which had no prefix bloom and made `RENAME` O(n).

## 1. Package layout

All paths are relative to the repository root. Import edges point downward and form a DAG rooted at `cmd/nildb`.

| Package | Responsibility | Imports (internal) |
|---|---|---|
| `cmd/nildb` | Flags and config file, open store, register the command packages, serve, signals, WAL flush ticker, lease janitor. | config, store, catalog, docstore, server, redis, cmddoc, analytics, admin |
| `cmd/rocksmoke` | Existing link-and-feature smoke test for the Homebrew RocksDB; the M0 gate. | none |
| `cmd/htapbench` | HTAP interference and freshness measurement; talks RESP over TCP. | none |
| `internal/config` | `Config` struct, defaults, flag and file parsing, validation, `Clock` interface. | none |
| `internal/resp` | RESP2/RESP3 reader (multibulk and inline, Redis limits, exact `Protocol error:` strings), writer with a per-connection protocol version, `Reply` values, the shared Redis error table. | none |
| `internal/keyenc` | Order-preserving encoding of BSON values (`_id`, index keys, group keys) with the type-order table; sortable double and int64 helpers. | none (bson) |
| `internal/layout` | Byte layouts: Redis metadata, version map, sub-keys, score index, version generator; document, index, geo, columnar and catalog key builders and parsers. | keyenc |
| `internal/store` | The only package that imports grocksdb: open with eight CFs and their options, caches, write buffer manager, rate limiter, the `rate_limiter_priority` cgo shim, `Txn` over `WriteBatch` and `WriteBatchWI`, `Reader`, snapshots and leases, `LockManager`, the three compaction filters, the one merge operator, admin calls. | layout |
| `internal/catalog` | Collections, indexes, id counters, index state machine; in-memory cache; publishes the `LiveSet` to `store`; per-collection `sync.RWMutex`. | store, layout, keyenc |
| `internal/command` | `Spec`, `Registry`, `Handler`, `Ctx`, `Conn`; arity checks; `COMMAND`/`COMMAND DOCS` tables generated from specs. | resp, store, config |
| `internal/server` | Listener, one goroutine per connection, FIFO pipelining, connection commands, MULTI queue, WATCH table, SCAN cursor ring, latency histograms, `INFO`. | resp, command, store, config |
| `internal/geohash` | Port of Redis 7.2 `geohash.c`, `geohash_helper.c` and the search helpers of `geo.c` (BSD-3-Clause; the licence file is already at `internal/geohash/LICENSE-REDIS-BSD.txt`). | none |
| `internal/redis` | Redis types over the store, one file per type: string, generic and expire, hash, list, set, zset, geo, scan. | command, resp, store, layout, geohash |
| `internal/doc` | Pure BSON semantics: matcher, update operators, projection, sort-key extraction, Extended JSON in and out, path walking, document validation. | keyenc |
| `internal/geo` | 2dsphere: GeoJSON to S2, coverings, query plans (ranges and probes), annulus planning, exact refinement, distance, the storage-agnostic kNN driver. | none (golang/geo, bson) |
| `internal/docstore` | Single-document write path with index, geo and columnar maintenance in one batch; unique checks; drops; the background index builder; the TTL sweeper. | store, layout, keyenc, catalog, doc, geo |
| `internal/query` | Planner and pull operators (index scan, columnar merge, row scan, match, project, unwind, sort and top-k, group, limit and skip, geoNear), cursors, memory accountant, EXPLAIN. | store, layout, keyenc, catalog, doc, geo |
| `internal/cmddoc` | `DOC.*` handlers. | command, resp, store, catalog, docstore, query, doc |
| `internal/analytics` | `NIL.*` handlers: semaphore, byte bucket, leases, row cap, `NIL.GEOGROUP`, `NIL.STATS`, `NIL.GROUP`. | command, resp, store, catalog, query, geo |
| `internal/admin` | `ROCKS.*` handlers and the `rocksdb` section of `INFO`. | command, resp, store, catalog |
| `internal/testutil` | Starts an in-process server on a free port with a temp dir and a fake clock; raw TCP client; go-redis clients for RESP2 and RESP3. | server, store, config, catalog, docstore, redis, cmddoc, analytics, admin |

Leaves: `config`, `resp`, `keyenc`, `geohash`, `geo`, `doc` (`doc` uses only `keyenc`). `layout` sits on `keyenc`; `store` on `layout`; `catalog` on `store`; `command` on `resp` and `store`; `server` on `command`; `redis`, `cmddoc`, `analytics` and `admin` on `command` plus their storage packages; `docstore` and `query` on `catalog`; `cmddoc` on both; `cmd/nildb` on everything. `query` never imports `docstore` (reads only); `cmddoc` composes the two for filter-driven updates. Each command package exports `Register(*command.Registry)`, and `cmd/nildb` calls the five of them; no package registers itself through `init()`, so a test can build a registry with one group.

### Store contract (`internal/store`)

```go
package store

type CF uint8

const (
	CFDefault CF = iota // catalog, layout marker
	CFMeta              // Redis metadata, string payloads, version map
	CFSub               // hash fields, set members, list elements, zset member->score
	CFZScore            // zset score index, Redis GEO
	CFDoc               // BSON rows
	CFIdx               // secondary indexes (btree, unique, TTL)
	CFGeo               // 2dsphere cells
	CFCol               // columnar projections
)

type Config struct {
	Dir                 string
	BlockCacheBytes     int64         // main HyperClockCache, default 512 MiB
	AnalyticsCacheBytes int64         // col cache, default 64 MiB
	WriteBufferBytes    int64         // WriteBufferManager cap, default 256 MiB
	BgIOBytesPerSec     int64         // rate limiter, default 200 MiB/s
	Fsync               string        // "always" | "everysec" | "no"
	MaxSnapshots        int           // leases + cursors, default 32
	LeaseTTL, LeaseMax  time.Duration // 60 s, 600 s
	Statistics          bool          // --rocks-stats
	Clock               func() time.Time
}

func Open(cfg Config) (*Store, error)
func (s *Store) Close() error

// Reader is implemented by *Store (live committed state), by the value of
// Store.At(snap), and by every Txn.
type Reader interface {
	Get(cf CF, key []byte) (val []byte, ok bool, err error)                  // Go-owned copy
	GetPinned(cf CF, key []byte, fn func(val []byte) error) (ok bool, err error) // zero-copy view for the callback's duration
	MultiGet(cf CF, keys [][]byte) ([][]byte, error)                          // nil entry when absent
	Iter(cf CF, lo, hi []byte, o IterOpts) Iterator                           // [lo, hi) through iterate_lower/upper_bound
}

type IterOpts struct {
	FillCache         bool          // false on every analytical iterator
	TotalOrderSeek    bool          // required whenever [lo, hi) may cross a prefix on a CF with an extractor
	PrefixSameAsStart bool          // geo ancestor probes only
	Readahead         int           // bytes; 0 = RocksDB auto
	AsyncIO           bool
	Deadline          time.Duration // 0 = none; converted to an absolute micros timestamp for SetDeadline
	LowPriority       bool          // rate_limiter_priority = IO_LOW via the cgo shim
}

type Iterator interface {
	Seek(key []byte); SeekForPrev(key []byte); SeekToFirst(); SeekToLast()
	Valid() bool; Next(); Prev()
	Key() []byte   // view into C memory, valid until the next move (KeySlice)
	Value() []byte // same (ValueSlice)
	Err() error
	Close()
}

// Txn is the unit of atomicity. Begin wraps a plain WriteBatch whose reads see
// committed state; BeginIndexed wraps a WriteBatchWI(overwrite_key=true) whose
// reads see its own writes. Only EXEC uses BeginIndexed.
type Txn interface {
	Reader
	Put(cf CF, key, val []byte)
	Delete(cf CF, key []byte)
	Merge(cf CF, key, operand []byte)          // CFDefault only (nildb.i64add)
	DeleteRange(cf CF, lo, hi []byte) error    // ErrRangeInIndexedTxn under BeginIndexed
	Len() int
	Commit() error                             // db.Write / db.WriteWI with the configured WriteOptions
	Discard()
}

var ErrRangeInIndexedTxn = errors.New("store: DeleteRange is not supported inside an indexed transaction")

func (s *Store) Begin() Txn
func (s *Store) BeginIndexed() Txn

// Snapshots. Snapshot() is command-scoped and uncounted; Lease() is counted
// against MaxSnapshots and released by the janitor after its TTL.
type Snapshot struct {
	ID      uint64
	Seq     uint64
	Owner   string
	Created time.Time
	Expires time.Time // zero for command-scoped snapshots
}

func (s *Store) Snapshot() *Snapshot
func (s *Store) Lease(owner string, ttl time.Duration) (*Snapshot, error) // ErrTooManySnapshots
func (s *Store) Lookup(id uint64) (*Snapshot, bool)
func (s *Store) Release(*Snapshot)
func (s *Store) Leases() []*Snapshot
func (s *Store) At(snap *Snapshot) Reader

// Locks: one stripe array per kind, 4096 sync.Mutex each; LockColl is a
// per-collection RWMutex owned by catalog and acquired through the same call.
type LockKind uint8

const (
	LockColl  LockKind = iota // NS = coll_id; Key[0] = 'r' shared, 'w' exclusive
	LockDoc                   // NS = coll_id; Key = ks(_id)
	LockUniq                  // NS = idx_id;  Key = ks(values)
	LockRedis                 // NS = db;      Key = user key
)

type LockKey struct {
	Kind LockKind
	NS   uint32
	Key  []byte
}

// Lock sorts by (kind, stripe), deduplicates, locks ascending. A caller may
// call Lock again only with keys of a strictly higher kind than any it holds.
func (s *Store) Lock(keys []LockKey) (unlock func())

// LiveSet backs idFilter on doc, idx, geo and col. nil means keep everything.
type LiveSet interface{ Live(id uint32) bool }
func (s *Store) SetLiveSet(LiveSet)

// Drops and admin. Drops are a plain WriteBatch with DeleteRangeCF plus the
// caller's point deletes, committed once.
type RangeDelete struct{ CF CF; Lo, Hi []byte }
func (s *Store) DeleteRanges(ranges []RangeDelete, alsoDelete []struct{ CF CF; Key []byte }) error
func (s *Store) ScheduleCompact(cf CF, lo, hi []byte) // 60 s later; immediate when 32 are outstanding

func (s *Store) Property(cf CF, name string) (string, bool)
func (s *Store) IntProperty(cf CF, name string) (uint64, bool)
func (s *Store) ApproxSizes(cf CF, ranges [][2][]byte) ([]uint64, error)
func (s *Store) Compact(cf CF, lo, hi []byte) error
func (s *Store) Flush(cf CF) error
func (s *Store) FlushWAL(sync bool) error
func (s *Store) Checkpoint(dir string) error                 // CreateCheckpoint(dir, 0); no sequence number exists in the C API
func (s *Store) ExportCF(cf CF, dir string) error            // Checkpoint.ExportColumnFamily
func (s *Store) Ingest(cf CF, paths []string) error           // IngestExternalFileCF
func (s *Store) SetOption(cf CF, name, value string) error   // SetOptionsCF
func (s *Store) LatestSeq() uint64
func (s *Store) CFNames() []string
func (s *Store) Stats() (string, bool)                      // GetStatisticsString when Statistics is on
func Version() (major, minor, patch int)                     // from <rocksdb/version.h> via cgo
```

### Command handler contract (`internal/command`)

```go
package command

type Flags uint16

const (
	Write     Flags = 1 << iota // takes locks from Keys; touches WATCH after a non-empty commit
	ReadOnly
	MultiRead                   // reads more than one key or iterates across keys: snapshot at start
	NoMulti                     // rejected inside MULTI: WATCH, MULTI, FLUSHDB, FLUSHALL, DOC.DROP, DOC.DROPINDEX, DOC.CREATEINDEX, DOC.CURSOR, NIL.*, ROCKS.*
	Analytic                    // runs under the analytics semaphore on a lease
	Admin
	NoAuth                      // AUTH, HELLO, QUIT before authentication
)

// Keys returns the lock set and may return a parsed form of args that the
// handler receives as Ctx.Parsed, so DOC.INSERT parses its documents once.
type KeysFunc func(kc *KeyCtx, args [][]byte) (keys []store.LockKey, parsed any, err error)

type KeyCtx struct {
	DB      uint8
	Catalog CatalogView // ResolveNS, UniqueIndexes(coll_id); nil for Redis groups
}

type Spec struct {
	Name    string   // lowercase, "zadd", "doc.find"
	Arity   int      // Redis convention; -N = at least N including the name
	Flags   Flags
	Keys    KeysFunc // nil = no locks
	Summary string   // COMMAND DOCS
	Run     Handler
}

type Handler func(c *Ctx, args [][]byte) resp.Reply

type Ctx struct {
	Conn   Conn
	Store  *store.Store
	Txn    store.Txn       // Begin() per autocommit command; the EXEC-wide WriteBatchWI inside MULTI
	Snap   *store.Snapshot // set for MultiRead and Analytic commands
	Now    time.Time       // fixed for the command
	DB     uint8
	Parsed any
	Cfg    *config.Config
}

// Reader returns the Txn inside EXEC (read-your-writes), the snapshot reader
// when Snap is set, otherwise the live store.
func (c *Ctx) Reader() store.Reader

type Conn interface {
	ID() uint64
	Addr() string
	DB() uint8; SetDB(uint8)
	Proto() int; SetProto(int) // 2 or 3
	Name() string; SetName(string)
	LibInfo() (name, ver string); SetLibInfo(name, ver string)
	Authenticated() bool; SetAuthenticated(bool)
	InMulti() bool
	Watch(db uint8, keys [][]byte, stale []bool)
	Unwatch()
	Close()
}

type Registry struct{ /* map[string]*Spec, insertion order for COMMAND */ }
func NewRegistry() *Registry
func (r *Registry) Register(specs ...Spec)
func (r *Registry) Lookup(name []byte) (*Spec, bool) // case-insensitive
func (r *Registry) CheckArity(s *Spec, argc int) error
func (r *Registry) Specs() []*Spec
```

### Codec contract (`internal/resp`)

```go
package resp

type Limits struct {
	InlineMax   int   // 64 KiB
	BulkMax     int64 // 512 MiB (proto-max-bulk-len)
	PreAuthArgs int   // 10
	PreAuthBulk int   // 16 KiB
}

type Reader struct{ /* bufio.Reader, limits, authed flag */ }
func NewReader(r io.Reader, l Limits) *Reader
func (r *Reader) SetAuthenticated(bool)
// Next returns the argument vector of the next multibulk or inline request.
// A *ProtocolError carries the exact Redis text ("Protocol error: invalid
// multibulk length"); the caller replies "-ERR <text>" and closes.
func (r *Reader) Next() ([][]byte, error)
func (r *Reader) Buffered() int // > 0 means more pipelined bytes are waiting

type Writer struct{ /* bufio.Writer, proto */ }
func NewWriter(w io.Writer) *Writer
func (w *Writer) SetProto(v int)
func (w *Writer) Proto() int
func (w *Writer) Write(r Reply)
func (w *Writer) Flush() error

// Reply is a value a handler returns; the connection loop emits it.
type Reply interface{ emit(w *Writer) }

func OK() Reply
func Status(s string) Reply
func Int(n int64) Reply
func Bulk(b []byte) Reply
func Str(s string) Reply
func Null() Reply                 // $-1 | _
func NullArray() Reply            // *-1 | _
func Array(items ...Reply) Reply  // *n
func Map(pairs ...Reply) Reply    // *2n | %n
func Set(items ...Reply) Reply    // *n | ~n
func Double(f float64) Reply      // bulk | ,
func Bool(b bool) Reply           // :1 / :0 | #t / #f
func Verbatim(format, s string) Reply // bulk | =len\r\ntxt:...
func Err(text string) Reply       // "WRONGTYPE ...", "ERR ..."
func Errorf(format string, a ...any) Reply
func Raw(b []byte) Reply          // pre-encoded bytes, EXEC replay

var (
	ErrWrongType   = Err("WRONGTYPE Operation against a key holding the wrong kind of value")
	ErrSyntax      = Err("ERR syntax error")
	ErrNoSuchKey   = Err("ERR no such key")
	ErrOutOfRange  = Err("ERR index out of range")
	ErrNotInteger  = Err("ERR value is not an integer or out of range")
	ErrNotFloat    = Err("ERR value is not a valid float")
	ErrOverflow    = Err("ERR increment or decrement would overflow")
	ErrNaNOrInf    = Err("ERR increment would produce NaN or Infinity")
	ErrNoAuth      = Err("NOAUTH Authentication required.")
	ErrWrongPass   = Err("WRONGPASS invalid username-password pair or user is disabled.")
	ErrExecAbort   = Err("EXECABORT Transaction discarded because of previous errors.")
	ErrNestedMulti = Err("ERR MULTI calls can not be nested")
	ErrExecNoMulti = Err("ERR EXEC without MULTI")
	ErrDiscardNoMulti = Err("ERR DISCARD without MULTI")
	ErrWatchInMulti = Err("ERR WATCH inside MULTI is not allowed")
	ErrDBIndex     = Err("ERR DB index is out of range")
	ErrInvalidCursor = Err("ERR invalid cursor")
	ErrNoProto     = Err("NOPROTO unsupported protocol version")
)
func ErrArity(name string) Reply   // "ERR wrong number of arguments for '<name>' command"
func ErrUnknown(name string, args [][]byte) Reply // "ERR unknown command '<name>', with args beginning with: ..."
```

The geo index contract is in section 8.

## 2. Column families

One RocksDB instance, eight column families opened with `grocksdb.OpenDbColumnFamilies` on every start, so later milestones add no migrations. All CFs share the WAL, so one `WriteBatch` across CFs is atomic and `atomic_flush` stays off.

Shared objects, created once in `store.Open`:

```go
mainCache := grocksdb.NewHyperClockCache(int(cfg.BlockCacheBytes), 0)     // 512 MiB default
colCache  := grocksdb.NewHyperClockCache(int(cfg.AnalyticsCacheBytes), 0) // 64 MiB default
wbm       := grocksdb.NewWriteBufferManager(int(cfg.WriteBufferBytes), true) // 256 MiB, allowStall
rl        := grocksdb.NewGenericRateLimiter(cfg.BgIOBytesPerSec, 100_000, 10, grocksdb.RateLimiterModeAllIo, true)
```

DB-level options: `SetCreateIfMissing(true)`, `SetCreateIfMissingColumnFamilies(true)`, `SetMaxBackgroundJobs(6)`, `SetMaxSubcompactions(2)`, `SetBytesPerSync(1<<20)`, `SetMaxOpenFiles(-1)`, `SetWALRecoveryMode(grocksdb.PointInTimeRecovery)`, `SetMaxTotalWalSize(1<<30)`, `SetLevelCompactionDynamicLevelBytes(true)`, `SetCompactionPri(grocksdb.KMinOverlappingRatioCompactionPri)`, `SetRateLimiter(rl)`, `SetWriteBufferManager(wbm)` on every CF's options, `EnableStatistics()` only with `--rocks-stats`. `SetManualWALFlush` is left false (section 4 explains).

Row table options (`default`, `meta`, `sub`, `zscore`, `doc`, `idx`, `geo`): `bbto.SetBlockSize(16<<10)`, `SetBlockCache(mainCache)`, `SetCacheIndexAndFilterBlocks(true)`, `SetPinL0FilterAndIndexBlocksInCache(true)`, `SetPinTopLevelIndexAndFilter(true)`, `SetFormatVersion(6)`, `SetOptimizeFiltersForMemory(true)`, `SetFilterPolicy(grocksdb.NewRibbonHybridFilterPolicy(10, 1))`. Row compression: `SetCompressionPerLevel([]CompressionType{No, No, LZ4, LZ4, LZ4, LZ4, ZSTD})`. HyperClockCache has no priority pools, so the `SetCacheIndexAndFilterBlocksWithHighPriority` isolation argument of the base proposal is dropped; pinning does the work.

| CF | Contents | Prefix extractor | Filter, whole-key | Merge | Compaction filter | Cache | Compression | Write buffer |
|---|---|---|---|---|---|---|---|---|
| `default` | catalog: collections, indexes, counters, layout marker | none | Ribbon 10, yes | `nildb.i64add`, `SetMaxSuccessiveMerges(64)` | none | main | LZ4 | 4 MiB |
| `meta` | Redis metadata with inline string payloads; version map `v` | none (SCAN needs total order) | Ribbon 10, yes | none | `metaFilter` | main | per-level | 64 MiB |
| `sub` | hash fields, set members, list elements, zset member→score | `NewFixedPrefixTransform(8)`, `SetMemTablePrefixBloomSizeRatio(0.1)` | Ribbon 10, yes (HGET, SISMEMBER, ZSCORE, LINDEX) | none | `subFilter` | main | per-level | 64 MiB |
| `zscore` | zset score index, Redis GEO | fixed 8, memtable prefix bloom 0.1 | Ribbon 10, `SetWholeKeyFiltering(false)` | none | `subFilter` | main | per-level | 32 MiB |
| `doc` | BSON rows | none | Ribbon 10, yes (`_id` lookups) | none | `idFilter` | main | per-level | 64 MiB |
| `idx` | btree, unique and TTL indexes | none | Ribbon 10, yes (unique existence) | none | `idFilter` | main | per-level | 32 MiB |
| `geo` | 2dsphere cells | `NewFixedPrefixTransform(12)` (idx_id + cell_id), memtable prefix bloom 0.1 | Ribbon 10, `SetWholeKeyFiltering(false)` | none | `idFilter` | main | per-level | 16 MiB |
| `col` | columnar projections | none (scan only) | none | none | `idFilter` | own `BlockBasedTableOptions`: `colCache`, block 64 KiB, index and filter blocks cached and pinned | ZSTD every level, `SetBottommostCompression(ZSTD)` | 64 MiB |

`SetPeriodicCompactionSeconds(7*86400)` on every CF that carries a filter, so cold files get filtered. `meta` has no extractor because `SCAN`, `KEYS` and `DBSIZE` iterate it in total order. `doc`, `idx` and `col` have no extractor: their scans always carry both bounds, and an extractor would only add the prefix-mode hazard the Prefix-Seek wiki warns about. The two CFs with an extractor and cross-prefix scans (`sub`, `zscore` only under `ROCKS.SCAN`; `geo` under descendant range scans) set `TotalOrderSeek` explicitly; `IterOpts.TotalOrderSeek` exists for that reason.

The one merge operator is `nildb.i64add` on `default`, an associative 8-byte little-endian add used for the per-collection document count `n/<coll_id>` only. `SetMaxSuccessiveMerges(64)` bounds the memtable chain; the count is read by `DOC.COUNT` without a filter, `DOC.STATS` and `NIL.STATS`, none of them hot. Redis counters and `APPEND` do not use Merge: `INCR` must return the new value and must refuse a non-integer or a `WRONGTYPE` key without writing, which a merge operator cannot do, and analytical leases would pin operand chains on hot counters.

## 3. Byte layouts

Integers are big-endian unless marked `LE`. `dbl(x)` is the sign-flip: `b := math.Float64bits(x); if b>>63 == 1 { b = ^b } else { b |= 1<<63 }`. `-0` is canonicalised to `+0` before encoding and NaN scores are rejected with `ERR value is not a valid float`, so `ZADD k 0 a` and `ZADD k -0 b` order lexicographically and `ZCOUNT k 0 0` finds both. Versions are `(unix_micros << 11) | (counter & 0x7FF)` from an atomic with a monotonic guard (never below `last+1`), seeded at startup from `max(now, catalog 's/version' + 1)` and written back at shutdown.

**Namespaces.** `SELECT n` is real for `n` in 0..15 and becomes the first byte of every `meta` key. Sub-keys carry no `db` (the version identifies the collection). Byte `0x76` (`'v'`) is reserved in `meta` for the version map; any later `meta`-resident index takes another byte above 15. Documents live in namespaces (`orders`, `shop.orders`) resolved by the catalog to a `coll_id`; `SELECT` does not affect `DOC.*`.

**Redis metadata (`meta`).**

```
m-key    | db (1) | user key (n) |
string   | flags (1) | expire_ms (8) | payload (n) |
hash/set/zset
         | flags (1) | expire_ms (8) | version (8) | size (8) |
list     | flags (1) | expire_ms (8) | version (8) | size (8) | head (8) | tail (8) |
v-key    | 0x76 (1) | version (8) |            ->  | expire_ms (8) | db (1) | user key (n) |
```

`flags` bits 0-3: type (1 string, 2 hash, 3 list, 4 set, 5 zset); bits 4-7: layout version, 0. `expire_ms` 0 means never. Lists start with `head = tail = 1<<63`; live indexes are `[head, tail)`; `LPUSH` writes at `head-1`, `RPUSH` at `tail`. The `v` entry exists only for collection types, is written in the same batch as its metadata, is rewritten by `EXPIRE`, `PERSIST`, `RENAME` and `MOVE`, and is deleted in the same batch whenever the collection's version is replaced (`DEL`, `SET` over a collection, overwrite of an expired collection, `RENAME` onto it).

**Sub-keys (`sub`) and score index (`zscore`).**

```
hash   | version (8) | field (n) |          ->  | value (n) |
set    | version (8) | member (n) |         ->  (empty)
list   | version (8) | index (8) |          ->  | element (n) |
zset   | version (8) | member (n) |         ->  | dbl(score) (8) |
zscore | version (8) | dbl(score) (8) | member (n) |   ->  (empty)
```

A whole-collection scan is `Iter(cf, version, version+1)`; both bounds share the 8-byte prefix, so the iterator never leaves prefix mode. `LINSERT` and `LREM` renumber the shorter side and are documented O(n).

**Redis GEO** is a zset whose score is `float64(geohash52)`; 52 bits fit a double exactly, and `dbl()` of a non-negative integer-valued double preserves integer order, so a cell's score range `[h<<shift, (h+1)<<shift)` is one `zscore` range.

**Document rows (`doc`).**

```
| coll_id (4) | ks(_id) (n) |   ->  raw BSON, _id moved first, validated, <= 16 MiB, depth <= 100
```

`_id` of type array, regex, undefined, MinKey or MaxKey is rejected at insert (`ERR can't use an array for _id` and MongoDB's other texts). Field names containing `.` or starting with `$` are rejected at insert with MongoDB 5.0's reason.

**Order-preserving encoding `ks(v)` (`internal/keyenc`).** One type byte, then a payload. The type bytes are MongoDB KeyString's, so `bytes.Compare` reproduces MongoDB's cross-type order.

| Type byte | BSON types | Payload |
|---|---|---|
| 10 | MinKey | none |
| 15 | Undefined | none |
| 20 | Null, missing field | none |
| 30 | int32, int64, double (one numeric class) | class (1) + 0 or 8 bytes, table below |
| 60 | String, Symbol | UTF-8 with `0x00` → `0x00 0xFF`, terminator `0x00` |
| 70 | Object | per field: value type byte, escaped field name + `0x00`, value payload; terminator `0x00` |
| 80 | Array | `ks(elem)` repeated; terminator `0x00` (whole-array equality and `_id` only; index entries are per element) |
| 90 | BinData | `\| len (4) \| subtype (1) \| bytes \|` |
| 100 | ObjectId | 12 bytes |
| 110 | Bool | `0x00` / `0x01` |
| 120 | Date | `int64 ms ^ (1<<63)` |
| 130 | Timestamp | `\| t (4) \| i (4) \|` |
| 140 | Regex | escaped pattern + `0x00`, escaped options + `0x00` |
| 150 | DBPointer | `ks(ns)`, 12-byte oid |
| 160 | JavaScript | `ks(code)` |
| 170 | CodeWScope | `ks(code)`, `ks(scope)` |
| 240 | MaxKey | none |

Numeric class byte, exact for every int64 and double and equal for `1` and `1.0` (10 bytes per number instead of the base proposal's 17):

| Class | Range | Payload |
|---|---|---|
| 0x00 | NaN | none |
| 0x01 | x < -2^63 (doubles only, includes -Inf) | `dbl(x)` |
| 0x02 | -2^63 ≤ x ≤ -2^53 (integral; doubles convert exactly) | `uint64(int64(x)) ^ (1<<63)` |
| 0x03 | -2^53 < x < 0 (int64s convert exactly to double) | `dbl(x)` |
| 0x04 | zero (0, -0, 0.0) | none |
| 0x05 | 0 < x < 2^53 | `dbl(x)` |
| 0x06 | 2^53 ≤ x < 2^63 | `uint64(x)` |
| 0x07 | x ≥ 2^63 (doubles only, includes +Inf) | `dbl(x)` |

Decimal128 is stored in documents and compared exactly by the matcher through `big.Float`, but it is rejected as an index key, as `_id`, in columnar fields and in `$inc`/`$mul`/accumulators with `ERR Decimal128 is not supported in indexes or arithmetic in v1`. The base proposal's float64 approximation gave false negatives at range bounds and false `E11000` errors. A descending index field XORs every byte of its encoding with `0xFF`, tag included; the index spec tells the decoder the direction, and the type-before-name object encoding keeps prefixes unambiguous.

**Secondary indexes (`idx`).**

```
non-unique | idx_id (4) | ks(f1) | ... | ks(fk) | ks(_id) |   ->  (empty)
unique     | idx_id (4) | ks(f1) | ... | ks(fk) |            ->  | ks(_id) |
```

Missing fields index as Null unless the index is sparse (a sparse compound index skips a document only when every indexed field is missing). One array field per compound key, one entry per distinct element; a second array field fails the write with `ERR cannot index parallel arrays`. A TTL index is a single-field btree index on a Date field whose catalog record carries `expireAfterSeconds`; documents whose field is not a Date (or, for arrays, whose earliest Date) are not swept.

**2dsphere (`geo`).**

```
| idx_id (4) | cell_id (8) | ks(_id) |   ->  point:  | 0x01 | lat f64 (8) | lng f64 (8) |
                                             shape or multi-geometry: | 0x02 |
```

S2 cell ids as big-endian `uint64` sort along the Hilbert curve; the descendants of cell `c` occupy `[c.RangeMin(), c.RangeMax()]` inclusive, so each covering cell is one bounded iterator. The 12-byte prefix `idx_id|cell_id` carries the prefix bloom that makes ancestor probes cheap.

**Columnar (`col`).**

```
| idx_id (4) | ks(_id) |   ->  | BSON type byte (1) | BSON element value bytes (n) |
```

One columnar index per declared field, created with `DOC.CREATEINDEX ns {"f":"columnar"}`, so it shares the `idx_id` counter, the catalog record, the `LiveSet` and the drop path with every other index. A missing field has no entry, so a k-way merge over k columns on `ks(_id)` reproduces missing fields with no sentinel. Values are raw BSON element bytes, not `ks()`, because `$sum` and `$avg` need the number and `$unwind` needs the array.

**Catalog (`default`).** Keys are printable so `ROCKS.SCAN default` reads like a directory.

```
| 0x00 |                          ->  "nildb-layout-1"   (Open refuses anything else)
| 'c' | namespace (utf8) |        ->  BSON {coll_id u32, created_ms i64}
| 'i' | coll_id (4) | name |      ->  BSON {idx_id u32, kind: "btree"|"2dsphere"|"columnar", keys, unique, sparse,
                                        expireAfterSeconds, s2: {coarsest, finest, maxCells}, state: "building"|"ready"}
| 'n' | coll_id (4) |             ->  int64 LE document count (merge nildb.i64add)
| 's' | "coll" |  | 's' | "idx" | ->  next id u32
| 's' | "version" |               ->  last issued Redis version u64
```

**SCAN cursors.** Redis cursors must be unsigned 64-bit integers (`redis-cli --scan` and go-redis parse them with `strtoull`/`ParseUint`), and a RocksDB key does not fit in 8 bytes. The real position is the last returned key, kept in a 16384-entry ring in `server` (Kvrocks's scheme, about 2 MiB), and the cursor packs `| type (3) | db (4) | hash25(position) | epoch (16) | counter (16) |` from the top bit down. `counter` indexes the ring; `epoch` is random per process start so a cursor from a previous life fails; `db` makes a cursor replayed after `SELECT` fail instead of seeking the wrong keyspace; `type` separates `SCAN`, `HSCAN`, `SSCAN` and `ZSCAN`. `SCAN 0` seeks `[db]`; each call iterates from `position+1` with `SetIterateUpperBound([db+1])` under a command-scoped snapshot, visits at most `COUNT*10` rows (Redis's cap), skips expired keys in place, applies `MATCH` and `TYPE`, and returns `0` when the iterator ends. An unknown, evicted, foreign-db or wrong-type cursor replies `ERR invalid cursor` (Redis's text for a malformed cursor; Kvrocks restarts silently, which breaks the full-iteration guarantee). `HSCAN`/`SSCAN`/`ZSCAN` store `(version, last subkey)`; a version that no longer matches the key's metadata (the key was deleted and recreated) replies cursor `0` with an empty array. Iteration on a total order gives Redis's guarantee that every key present for the whole iteration is returned once.

## 4. Concurrency and transactions

**Connections.** One goroutine per connection: `resp.Reader.Next`, dispatch, `Writer.Write(reply)`, and `Flush` when `Reader.Buffered() == 0` or 64 KiB is buffered. Replies leave in request order with one syscall per pipelined batch, which is all `redis-benchmark -P 16` and `redis-cli --pipe` (which waits for a trailing `ECHO`) need. No reader/executor pair.

**Locks.** `store.LockManager` keeps four stripe arrays of 4096 `sync.Mutex`, one per `LockKind`, plus the per-collection `sync.RWMutex` that `catalog` owns. Stripe = `xxhash64(ns, key) & 4095`. `Spec.Keys` returns the lock set; the executor sorts by `(kind, stripe)`, deduplicates and locks ascending, and unlocks after commit. Two-phase acquisition is allowed only in ascending kind order (`LockColl` < `LockDoc` < `LockUniq` < `LockRedis`), which keeps it deadlock-free across arrays. Reads take no locks. Redis write commands lock `(LockRedis, db, key)` for each key in their key spec. Document point writes take the collection `RLock`, the document stripe, then the unique-value stripes; filter-driven multi-document writes (`DOC.UPDATE ... MULTI` or `DOC.DELETE` with a non-`_id` filter) and DDL take the collection write lock, which serialises them against every writer of that collection and is documented as such.

**Autocommit.** Each command gets `Txn = Store.Begin()`, a plain `WriteBatch`. Reads go to the live DB (or to `Snap` for `MultiRead` commands); writes accumulate; `Commit` runs once when `Len() > 0`. Single-key point reads take no snapshot: one `Get` or one bounded iterator is consistent on its own, and a `HGETALL` that reads metadata then iterates the version's prefix sees a consistent hash even if the key is replaced meanwhile, because the old version's elements stay readable until compaction. `MultiRead` commands (`MGET`, `EXISTS k1 k2`, `SUNION`, `SINTER`, `SDIFF`, `ZUNION`, `ZINTER`, `ZDIFF`, `SMISMEMBER` is single-key) take `Store.Snapshot()` at command start and release it at the end, so they see one sequence number as Redis guarantees. This replaces the base proposal's `WriteBatchWI` and lazily created snapshot on every command, which taxed every `HGET` with a C++ skiplist index, a batch-then-DB lookup and a `NewSnapshot`/`ReleaseSnapshot` pair through the DBImpl mutex.

Counters and appends (`INCR`, `INCRBY`, `DECR`, `DECRBY`, `INCRBYFLOAT`, `APPEND`, `SETRANGE`, `HINCRBY`, `HINCRBYFLOAT`) read the value under the key lock, compute, and `Put`. `ERR value is not an integer or out of range`, `ERR increment or decrement would overflow`, `ERR increment would produce NaN or Infinity` and `WRONGTYPE` go out before anything is written, and an expired key counts from zero. The old header `| flags | expire_ms |` is preserved (INCR keeps the TTL).

**MULTI/EXEC.** `MULTI` sets the connection's queue. Each queued command is checked for existence, arity and `NoMulti` at queue time; a failure replies the error immediately and marks the transaction dirty, otherwise `+QUEUED`. The queue is capped at 100,000 commands to bound the `WriteBatchWI`; the 100,001st replies `ERR MULTI queue limit reached` and marks dirty. `EXEC` runs this sequence:

1. dirty → `-EXECABORT Transaction discarded because of previous errors.`, clear the queue, `UNWATCH`.
2. Lock set = union of the queued `Keys` sets ∪ every watched `(db, key)` as `LockRedis`. Sort, deduplicate, lock.
3. WATCH dirty flag set → unlock, `UNWATCH`, null array.
4. For every watched key not flagged stale at `WATCH` time: read its metadata live; if it is logically expired, or missing (only compaction of an expired key can make it missing without a touch), → unlock, `UNWATCH`, null array. This is Redis 6.2.5's `isWatchedKeyExpired` and what `multi.tcl`'s "EXEC fail on lazy expired WATCHed key" tests.
5. `Txn = Store.BeginIndexed()` (`NewWriteBatchWI(0, true)`). Run every queued handler against it; reads go through `GetFromDBWithCF` and `NewIteratorWithBaseCFReadOpts`, so `INCR` after `SET` sees the `SET`. Runtime errors become array elements and the batch still commits, as in Redis.
6. `Commit` (`db.WriteWI`) once if the batch is non-empty; `Touch` the write keys; unlock; `UNWATCH`; reply the array.

Locking watched keys before the dirty check closes the window the base proposal left on watched keys outside the write set. Commands that need `DeleteRange` (`FLUSHDB`, `FLUSHALL`, `DOC.DROP`, `DOC.DROPINDEX`) are `NoMulti`, and `Txn.DeleteRange` returns `ErrRangeInIndexedTxn` under `BeginIndexed` in case a handler forgets, because the C++ `WriteBatchWithIndex` rejects range deletes.

**WATCH.** `server.WatchTable` maps `(db, key)` to the watching connections; each connection keeps its watched keys, a `stale` bit per key (whether the key was already missing or expired when `WATCH` ran; Redis's `wk->expired`) and one dirty flag. `WATCH` inside `MULTI` replies `ERR WATCH inside MULTI is not allowed`. After a write command commits a non-empty batch, the executor calls `Touch(db, key)` for each key in its key spec; a `SET k v NX` that fails writes nothing and touches nothing. `FLUSHDB` and `FLUSHALL` touch only watched keys that currently exist and are not expired, and clear the `stale` bit of watched keys that were stale, so `multi.tcl`'s "FLUSHALL does not touch non affected keys" and "FLUSHDB while watching stale keys should not fail EXEC" hold. `EXEC`, `DISCARD`, `UNWATCH` and disconnect remove the connection's entries. When no key is watched, `Touch` is one atomic load.

**DOC commands.** Each `DOC.INSERT`, `DOC.REPLACE`, `DOC.UPDATE` and `DOC.DELETE` writes the row, its index, geo and columnar entries and the `n/<coll_id>` merge in one batch across `doc`, `idx`, `geo`, `col` and `default`, under the locks above; the shared WAL makes the multi-CF batch atomic and recoverable. `DOC.*` writes may be queued inside `MULTI`, giving multi-document atomicity through the `WriteBatchWI`. There are no interactive multi-document transactions in v1 (FerretDB v2 ships without them too).

**Snapshots for reads.** `DOC.FIND` cursors and every `NIL.*` command read through `Store.At(lease)`. `Store.Lease` counts leases and cursors together against `--max-snapshots` (32), refuses beyond it with `ERR too many open snapshots`, and a janitor releases a lease after its TTL (60 s default, 600 s maximum with `NIL.SNAPSHOT CREATE TTL`); a later read on a released lease replies `ERR snapshot lease expired`. Held snapshots pin overwritten versions and tombstones, so the cap and TTL are small on purpose.

**Durability.** `SetManualWALFlush` stays false, so RocksDB pushes every committed batch to the OS before the reply and a NilDB process crash loses nothing acknowledged. `--fsync everysec` (default) runs `db.FlushWAL(true)` once a second, so power loss costs at most about one second, the same contract as Redis `appendfsync everysec`. `--fsync always` sets `WriteOptions.SetSync(true)` per commit; on this Mac that is an `F_FULLFSYNC` (Homebrew's `librocksdb` is built with `HAVE_FULLFSYNC`), which costs about 2.5 ms per commit on APFS, so `always` caps a single connection near 400 writes/s. `--fsync no` never calls `FlushWAL`. The WAL is always on; disabling it would need `atomic_flush` for cross-CF recovery. The base proposal's `manual_wal_flush` plus timer would have lost up to a second on a process crash, which Redis `everysec` does not.

## 5. TTL and garbage

**Lazy expiry.** Decoding a `meta` value returns "absent" when `expire_ms != 0 && expire_ms <= ctx.Now`. Reads never write: `GET`, `TTL`, `EXISTS`, `TYPE`, `SCAN`, `KEYS` and `DBSIZE` treat the key as gone and leave the bytes for compaction. Writes treat an expired key as absent and overwrite it under the key lock; a collection gets a new version and the old `v` entry is deleted in the same batch. Because reads never delete, the race the judges found in the RocksDB-native proposal (an unlocked read deleting a key a locked `SET` had just recreated) cannot occur, and the base proposal's expiry worker with a 4096-deep channel that silently dropped under bursts is gone.

**No active expirer in v1.** WATCH over expiry is a pull check at `EXEC` (section 4), `INFO keyspace` and `DBSIZE` compute `keys=` and `expires=` from the same O(n) scan that skips expired keys, and storage for an expired key that nobody touches is reclaimed by the filters at the next compaction of its file, at the latest after `periodic_compaction_seconds` (7 days) or a `ROCKS.COMPACT`. The HTAP-first `t` time index and its 100 ms expirer are the v1.1 route to Redis-exact `expires=` and prompt reclamation; nothing in the layout blocks adding them (`meta` bytes 16 and above are reserved).

**Compaction filters.** grocksdb has `SetCompactionFilter` but no factory, so one instance per CF is shared by every subcompaction thread; each filter is a pure function of `(key, value, now)` plus fixed-size sharded state, holds an `atomic.Pointer` to the DB set after `Open`, and keeps everything until it is set. Filters run only during compaction, never during flush. `SetIgnoreSnapshots` is an interface method grocksdb calls only on native filters; RocksDB ≥ 6.0 ignores snapshots regardless, so an expired key may vanish from a pinned lease. Lazy expiry would have hidden it anyway; the docs state it.

- `metaFilter` (`meta`): an `m` entry with `expire_ms != 0 && expire_ms + 300 s < now` is dropped; a `v` entry with the same condition is dropped. No DB access. The 300 s grace (Kvrocks's window) covers a command that read the metadata and is still iterating.
- `subFilter` (`sub`, `zscore`): read the leading 8-byte version. If `now_micros - (version >> 11) < 60 s`, keep without any lookup: versions carry their creation time, so fresh data costs nothing. Otherwise consult a 64-slot verdict cache (`[64]struct{ mu sync.Mutex; version uint64; drop bool }`, slot `version & 63`; sorted input means one subcompaction hits the same slot for thousands of consecutive keys). On a miss, `GetPinnedCF(meta, 'v'|version)`: missing → drop; present with `expire_ms != 0 && expire_ms + 300 s < now` → drop; else keep. Caching is safe because a version is never reused, a "drop" verdict is permanent, and a stale "keep" only delays reclamation to the next compaction.
- `idFilter` (`doc`, `idx`, `geo`, `col`): read the leading 4-byte id (`coll_id` on `doc`, `idx_id` elsewhere) and drop it when the `LiveSet` says it is not live. `nil` LiveSet means keep. No DB access.

**LiveSet ordering.** `catalog` publishes a new `coll_id` or `idx_id` to `store.SetLiveSet` before the first batch that writes under it, and removes an id only after the drop batch (below) has committed. Publishing after the first write would let a flush plus compaction in that window drop live entries.

**O(1) deletes.** `DEL` of a hash, list, set or zset deletes its `m` and `v` entries: two tombstones, no element touched. New writes to that key mint a new version. `RENAME` rewrites `m` under the new key and the `v` entry's stored key; `MOVE` does the same with the db byte. Stale sub-keys are unreachable immediately (readers go through the metadata's version) and reclaimed by `subFilter`.

**Drops.** `FLUSHDB`, `FLUSHALL`, `DOC.DROP` and `DOC.DROPINDEX` are one plain `WriteBatch` through `Store.DeleteRanges`: `DeleteRangeCF` over `[db, db+1)` on `meta`, plus the `v` entries of the flushed collections (found by the same scan that touches watchers), for `FLUSH*`; over `[coll_id, coll_id+1)` on `doc` and `[idx_id, idx_id+1)` on `idx`, `geo` and `col` for each index, plus the catalog deletes, for `DOC.DROP`. `sub` and `zscore` are not range-deleted by `FLUSHDB`; their versions lose their `v` entries and `subFilter` reclaims them. `Store.ScheduleCompact` runs `CompactRangeCF` on each dropped range 60 s later; when 32 drops are outstanding the compaction runs at once, so range tombstones never pile up. `idFilter` is the backstop if a tombstone is ever lost. `ROCKS.PROPERTY rocksdb.estimate-pending-compaction-bytes` shows the backlog.

**Index, geo and columnar entries of changed or deleted documents** are maintained sync-full (Luo & Carey): the write path reads the old row (update semantics need it), computes the old index, geo and columnar keys, deletes them and inserts the new ones in the same batch as the row. Coverings are deterministic for the parameters frozen in the index record, so recomputing the old covering is exact and no reverse `_id → cells` entries are needed. Nothing in the document CFs waits for a compaction filter except a dropped id.

**Document TTL.** `docstore.Sweeper` runs every 60 s per TTL index: take a snapshot, iterate `[idx_id | ks(MinKey), idx_id | ks(Date(now - expireAfterSeconds)))` on `idx`, collect up to 1,000 `_id`s, then delete each through `docstore.Delete` under its locks with full index cleanup; at most 50,000 documents or 1 s per index per round (MongoDB's figures).

**Background index builds.** `DOC.CREATEINDEX` for a non-unique btree, 2dsphere or columnar index returns after recording the index with `state: building` under the collection write lock, publishing its `idx_id`, and adding it to the collection's in-memory index list, so every later write maintains it. A builder goroutine then takes a snapshot, iterates the `doc` prefix under it in batches of 256 `_id`s, and for each batch locks the document stripes, re-reads each row live with `GetPinned`, writes the entries computed from the live row (or skips a row that is gone), commits, and unlocks. Re-reading the live row under the lock is the fix for the lost-update race the judges found in the RocksDB-native and HTAP-first backfills: a concurrent update can only land before or after the batch, and either way the entries match the live row. At the end the builder sets `state: ready` under the collection write lock; the planner uses only `ready` indexes. Unique indexes build synchronously under the collection write lock, because a concurrent insert could pass the uniqueness check against an index the backfill has not reached; the stall is documented per command. A `building` index found at startup is rebuilt from scratch (entries are idempotent).

## 6. Command surface v1

Arity follows Redis (`-N` = at least N arguments including the command name). RESP3 shapes are noted where they differ; everything else is byte-identical in both protocols. Reported version is `7.2.0` (config `redis-version`); it stops clients from attempting 8.x features such as `SET IFEQ`, `HEXPIRE` and client-side caching.

**Connection.** `PING [msg]` (-1) `+PONG` or bulk; `ECHO` (2); `QUIT` (1); `SELECT` (2) `ERR DB index is out of range` outside 0..15; `AUTH [user] pass` (-2; single `--requirepass`, user must be `default`; `ERR AUTH <password> called without any password configured for the default user. Are you sure your configuration is correct?` when none is set); `HELLO [2|3 [AUTH u p] [SETNAME n]]` (-1; RESP3 `%7` map `server redis`, `version 7.2.0`, `proto 3`, `id`, `mode standalone`, `role master`, `modules []`; RESP2 a flat 14-element array; other versions `NOPROTO unsupported protocol version`); `CLIENT ID|SETNAME|GETNAME|INFO|LIST|SETINFO` (-2; `SETINFO LIB-NAME|LIB-VER` stored and shown in `CLIENT LIST`); `COMMAND [COUNT|INFO name...|DOCS [name...]]` (-1; generated from the registry: name, arity, flags, first/last/step key, summary); `INFO [section]` (-1; sections `server`, `clients`, `memory`, `persistence`, `stats`, `commandstats`, `keyspace`, `rocksdb`, `analytics`; RESP3 verbatim `txt:`); `DBSIZE` (1; O(n) scan of `meta[db]` skipping expired keys, documented); `FLUSHDB|FLUSHALL [SYNC|ASYNC]` (-1; both synchronous; `NoMulti`); `CONFIG GET pattern` (3; `save`, `appendonly`, `databases`, `maxmemory` return fixed values, `nildb.*` knobs return live values); `CONFIG SET name value` (4; `nildb.*` runtime knobs only); `TIME` (1); `SHUTDOWN [NOSAVE|SAVE]` (-1).

**Keys.** `DEL` (-2), `UNLINK` (-2), `EXISTS` (-2), `TYPE` (2), `KEYS pattern` (2; literal prefix seek, total order, under a snapshot, no lock), `SCAN cursor [MATCH p] [COUNT n] [TYPE t]` (-2), `RENAME` (3), `RENAMENX` (3), `MOVE key db` (3; O(1) metadata rewrite), `EXPIRE|PEXPIRE|EXPIREAT|PEXPIREAT key t [NX|XX|GT|LT]` (-3), `TTL|PTTL|EXPIRETIME|PEXPIRETIME` (2), `PERSIST` (2), `TOUCH` (-2), `RANDOMKEY` (1; seeks a random 8-byte position inside `[db, db+1)`, wraps, retries 100 times past expired keys; not uniform, documented).

**Strings.** `SET key v [NX|XX] [GET] [EX s|PX ms|EXAT t|PXAT t|KEEPTTL]` (-3; `IFEQ/IFNE/IFDEQ/IFDNE` unsupported), `GET` (2), `GETDEL` (2), `GETEX` (-2), `GETSET` (3), `MGET` (-2; `MultiRead`, `MultiGetCF`), `MSET` (-3), `MSETNX` (-3), `SETNX` (3), `SETEX|PSETEX` (4), `INCR|DECR` (2), `INCRBY|DECRBY` (3), `INCRBYFLOAT` (3; bulk string in both protocols, as Redis), `APPEND` (3), `STRLEN` (2), `GETRANGE|SUBSTR` (4), `SETRANGE` (4).

**Hashes.** `HSET` (-4), `HSETNX` (4), `HGET` (3), `HMGET` (-3), `HMSET` (-4), `HDEL` (-3), `HLEN` (2), `HEXISTS` (3), `HKEYS|HVALS` (2), `HGETALL` (2; RESP3 map), `HINCRBY` (4), `HINCRBYFLOAT` (4), `HSTRLEN` (3), `HRANDFIELD key [count [WITHVALUES]]` (-2; random seek, documented non-uniform; RESP3 `WITHVALUES` returns an array of pairs), `HSCAN key cursor [MATCH p] [COUNT n] [NOVALUES]` (-3; flat array in both protocols).

**Lists.** `LPUSH|RPUSH` (-3), `LPUSHX|RPUSHX` (-3), `LPOP|RPOP [count]` (-2), `LLEN` (2), `LRANGE` (4), `LINDEX` (3), `LSET` (4), `LTRIM` (4), `LREM` (4; O(n)), `LINSERT` (5; O(n)), `LPOS` (-3), `LMOVE` (5), `RPOPLPUSH` (3).

**Sets.** `SADD` (-3), `SREM` (-3), `SMEMBERS` (2; RESP3 set), `SISMEMBER` (3), `SMISMEMBER` (-3), `SCARD` (2), `SPOP [count]` (-2; random seek), `SRANDMEMBER [count]` (-2; random seek), `SMOVE` (4), `SINTER|SUNION|SDIFF` (-2; `MultiRead`; RESP3 set), `SINTERSTORE|SUNIONSTORE|SDIFFSTORE` (-3), `SINTERCARD numkeys key... [LIMIT n]` (-3), `SSCAN` (-3). Set operations are in-memory, bounded by `nildb.setop-max-members` (1,000,000).

**Sorted sets.** `ZADD key [NX|XX] [GT|LT] [CH] [INCR] score member ...` (-4; RESP3 `INCR` returns a double), `ZREM` (-3), `ZSCORE` (3; RESP3 double), `ZMSCORE` (-3), `ZCARD` (2), `ZCOUNT` (4), `ZLEXCOUNT` (4), `ZINCRBY` (4; RESP3 double), `ZRANGE key start stop [BYSCORE|BYLEX] [REV] [LIMIT off n] [WITHSCORES]` (-4; RESP3 `WITHSCORES` is an array of `[member, double]` pairs), `ZRANGESTORE` (-5), `ZREVRANGE|ZRANGEBYSCORE|ZREVRANGEBYSCORE|ZRANGEBYLEX|ZREVRANGEBYLEX` (-4), `ZRANK|ZREVRANK key member [WITHSCORE]` (-3; O(rank) scan, documented), `ZPOPMIN|ZPOPMAX [count]` (-2), `ZRANDMEMBER key [count [WITHSCORES]]` (-2; random seek), `ZREMRANGEBYRANK|BYSCORE|BYLEX` (4), `ZUNIONSTORE|ZINTERSTORE` (-4), `ZUNION|ZINTER|ZDIFF numkeys key... [WEIGHTS] [AGGREGATE] [WITHSCORES]` (-3; `MultiRead`), `ZDIFFSTORE` (-4), `ZSCAN` (-3). Exact errors: `ERR XX and NX options at the same time are not compatible`, `ERR GT, LT, and/or NX options at the same time are not compatible`, `ERR INCR option supports a single increment-element pair`, `ERR min or max is not a float`, `ERR min or max not valid string range item`.

**GEO.** `GEOADD key [NX|XX] [CH] lon lat member ...` (-5), `GEOPOS` (-2; pairs of bulk strings in both protocols), `GEODIST m1 m2 [M|KM|FT|MI]` (-4; bulk with 4 decimals), `GEOHASH` (-2; 11 characters), `GEOSEARCH key FROMMEMBER m|FROMLONLAT lon lat BYRADIUS r unit|BYBOX w h unit [ASC|DESC] [COUNT n [ANY]] [WITHCOORD] [WITHDIST] [WITHHASH]` (-7), `GEOSEARCHSTORE dst src ... [STOREDIST]` (-8), `GEORADIUS|GEORADIUSBYMEMBER` (-6; deprecated forms including `STORE`/`STOREDIST`, mapped onto the search). Exact strings: `ERR invalid longitude,latitude pair %f,%f`, `ERR exactly one of FROMMEMBER or FROMLONLAT can be specified for GEOSEARCH`, `ERR exactly one of BYRADIUS and BYBOX can be specified for GEOSEARCH`, `ERR unsupported unit provided. please use M, KM, FT, MI`.

**Transactions.** `MULTI` (1), `EXEC` (1), `DISCARD` (1), `WATCH key...` (-2), `UNWATCH` (1), with the exact strings from the error table.

**DOC.\*** Filters, documents, updates and pipelines are MongoDB Extended JSON v2 strings; replies that return documents use canonical Extended JSON bulk strings (`FORMAT BSON` returns raw BSON bulk strings). `ns` is a namespace string.

- `DOC.INSERT ns doc [doc ...] [ORDERED yes|no]` (-3) → RESP3 map `{n, ids:[...], writeErrors:[{index, code, errmsg}]}`; RESP2 `[n, [ids], [[index, code, errmsg]...]]`. Duplicate key → code 11000 with `E11000 duplicate key error collection: <ns> index: <name> dup key: { <field>: <value> }`.
- `DOC.GET ns id` (3) → document or null (fast path, no snapshot).
- `DOC.FIND ns filter [PROJECT p] [SORT s] [SKIP n] [LIMIT n] [BATCH n] [HINT name] [FORMAT EJSON|BSON] [AT lease] [EXPLAIN]` (-3) → RESP3 `{cursor: {id, ns, firstBatch: [...]}}`; RESP2 `[id, ns, [docs...]]`. First batch = min(101 documents, 16 MiB) like `find`; `id 0` means exhausted.
- `DOC.FINDONE ns filter [PROJECT p] [SORT s]` (-3) → document or null.
- `DOC.COUNT ns [filter]` (-2; without a filter reads `n/<coll_id>`), `DOC.DISTINCT ns field [filter]` (-3).
- `DOC.UPDATE ns filter update [MULTI] [UPSERT]` (-4) → `{n, nModified, upsertedId}`; `update` is an operator document or a replacement document.
- `DOC.REPLACE ns filter doc [UPSERT]` (-4).
- `DOC.DELETE ns filter [MULTI]` (-3) → `{n}`.
- `DOC.AGGREGATE ns pipeline [BATCH n] [MAXMEM bytes] [FORMAT ...] [EXPLAIN]` (-3) → cursor shape as `FIND`; runs on the connection with OLTP read options and a 32 MiB default `MAXMEM`.
- `DOC.CURSOR READ id [COUNT n]` | `DEL id` | `LIST` (-3; `NoMulti`).
- `DOC.CREATEINDEX ns keyspec [NAME n] [UNIQUE] [SPARSE] [EXPIREAFTERSECONDS s] [S2 coarsest finest maxcells]` (-3; `keyspec` is `{"f":1,"g":-1}`, `{"loc":"2dsphere"}` or `{"f":"columnar"}`; background unless `UNIQUE`; `NoMulti`), `DOC.DROPINDEX ns name` (3; `NoMulti`), `DOC.INDEXES ns` (2; includes `state`), `DOC.COLLECTIONS [database]` (-1), `DOC.CREATE ns` (2), `DOC.DROP ns` (2; `NoMulti`), `DOC.STATS ns` (2; count from `n/<coll_id>`, `ApproxSizes` per CF, indexes).

Query operators: `$eq $ne $gt $gte $lt $lte $in $nin $and $or $nor $not $all $elemMatch $size $exists $type $regex` (RE2 syntax; backreferences and lookaround reply `ERR Regular expression is invalid`) `$mod $geoWithin $geoIntersects $near $nearSphere`, with MongoDB's null-versus-missing rules (`$ne`, `$nin`, `$not` match missing; `$exists:true` includes null), type bracketing, and array "any element" semantics versus `$elemMatch`. Update operators: `$set $unset $inc $mul $rename $min $max $currentDate $setOnInsert $push $pop $pull $pullAll $addToSet` with `$each $slice $sort $position`; replacement documents. Stages (shared with `NIL.AGGREGATE`): `$match $project $addFields $set $unset $unwind $group $sort $limit $skip $count $geoNear`. Accumulators: `$sum $avg $min $max $count $push $addToSet $first $last`. Expressions: arithmetic (`$add $subtract $multiply $divide $mod`), comparison, boolean, `$cond $ifNull`, `$concat $toLower $toUpper`, `$size $arrayElemAt`, `$year $month $dayOfMonth $hour $minute $second`, `$literal`, field paths, `$$ROOT`.

**NIL.\*** (all `Analytic`, `NoMulti`; section 7). `NIL.AGGREGATE ns pipeline [BATCH n] [MAXMEM bytes] [TIMEOUT ms] [AT lease] [EXPLAIN]` (-3) → cursor shape as `FIND`; `NIL.COUNT ns [filter]` (-2); `NIL.DISTINCT ns field [filter]` (-3); `NIL.STATS ns [field]` (-2) → map of count and, with a columnar field, min, max, sum, avg over the column; `NIL.GROUP ns field [COUNT|SUM f|AVG f] [TOP n]` (-3) → token form of `$group + $sort + $limit`; `NIL.EXPLAIN ns pipeline` (3) → map `{plan: index|columnar|rowscan, index, bounds, columns, estimatedRows, stages}`; `NIL.GEOGROUP ns index LEVEL l [filter]` (-5) → map of S2 cell token → count; `NIL.KEYSTATS [db]` (-1) → map of Redis key counts per type and expiring count from a `meta` scan; `NIL.SNAPSHOT CREATE [TTL s] | RELEASE id | LIST` (-2; RESP3 maps with `id`, `seq`, `owner`, `expires_ms`).

**ROCKS.\*** (`Admin`, `NoMulti`; RESP3 maps, RESP2 flat arrays). `ROCKS.INFO` (1; linked version from `<rocksdb/version.h>`, CF list, cache sizes, fsync policy, lease count), `ROCKS.PROPERTY name [CF cf]` (-2; `GetPropertyCF`/`GetIntPropertyCF`, e.g. `rocksdb.stats`, `rocksdb.estimate-num-keys`, `rocksdb.num-snapshots`, `rocksdb.oldest-snapshot-time`, `rocksdb.block-cache-usage`, `rocksdb.estimate-pending-compaction-bytes`), `ROCKS.CF LIST | INFO cf` (-2), `ROCKS.COMPACT [CF cf] [FROM hex TO hex]` (-1), `ROCKS.FLUSH [CF cf]` (-1), `ROCKS.FLUSHWAL [SYNC]` (-1), `ROCKS.CHECKPOINT dir [CF cf]` (-2; `CreateCheckpoint(dir, 0)` or `ExportColumnFamily`; returns `OK` plus the latest sequence number read separately with `GetLatestSequenceNumber`, since the C API's checkpoint call returns none), `ROCKS.SNAPSHOT CREATE [TTL s] | RELEASE id | LIST` (-2; alias of `NIL.SNAPSHOT`), `ROCKS.SCAN cf [FROM hex] [COUNT n] [SNAPSHOT id]` (-2; raw hex key/value pairs, `TotalOrderSeek` always; an in-process `ldb scan`), `ROCKS.GET cf key-hex` (3), `ROCKS.SIZES cf fromhex tohex` (4; `GetApproximateSizesCF`), `ROCKS.SEQ` (1), `ROCKS.STATS` (1; `GetStatisticsString`, or `ERR statistics disabled, start with --rocks-stats`), `ROCKS.INGEST cf path [path ...]` (-3; `IngestExternalFileCF`), `ROCKS.SETOPTION cf name value` (4; `SetOptionsCF`).

**Not supported in v1**, published as x-marks in `docs/compat/commands.md` with Dragonfly's caveat that "supported" is not byte-identical everywhere. Redis: Pub/Sub, Lua and `FUNCTION`, Streams, HyperLogLog, bitmaps (`SETBIT` through `BITFIELD`), blocking list and zset commands (`BLPOP`, `BRPOP`, `BLMOVE`, `BRPOPLPUSH`, `BZPOPMIN`), `OBJECT *`, `MEMORY *`, `DEBUG`, `MONITOR`, `LATENCY`, `SLOWLOG`, `CLIENT TRACKING|CACHING|PAUSE|KILL|UNBLOCK|NO-EVICT|NO-TOUCH`, `SWAPDB`, `COPY`, `DUMP`/`RESTORE`, `MIGRATE`, `SORT`, `WAIT`/`WAITAOF`, `SAVE`/`BGSAVE`/`BGREWRITEAOF` (use `ROCKS.CHECKPOINT`), replication and `CLUSTER *`, `ACL *`, `SET IFEQ|IFNE|IFDEQ|IFDNE`, `HEXPIRE` and hash-field expiry, `LCS`, keyspace notifications, RESP3 push and attribute types. Document side (v1.1 list, dated 2026-09-27): positional `$`, `$[]`, `$[id]`, `arrayFilters`, pipeline-form updates, `$bit`, `DOC.FINDANDMODIFY`, `$bitsAllSet` family, `$expr`, `$where`, `$jsonSchema`, `$text`, `$lookup`, `$graphLookup`, `$facet`, `$bucket*`, `$sortByCount`, `$replaceRoot`, `$sample`, `$unionWith`, `$setWindowFields`, `$merge`, `$out`, `$switch`, `$dateToString`, `$convert`/`$to*`, `$stdDevPop|Samp`, `$mergeObjects`, collation, interactive multi-document transactions, change streams, the MongoDB wire protocol, 2d/hashed/text/wildcard indexes, compound 2dsphere indexes, `allowDiskUse`, Decimal128 in indexes or arithmetic, the `-tags mongod` differential job against MongoDB 8.0 (fixture files are written in a replayable shape so the job can be added without changing them). Redis-side v1.1: the `t` time index and active expirer, `DBSIZE` counters.

## 7. HTAP path

Freshness is zero by construction: `DOC.*` and `NIL.*` read the same RocksDB, and a snapshot taken at command start includes every commit before it. The engineering goes into isolation.

**Execution of a `NIL.*` command.**

1. Acquire a slot on the analytics semaphore (`chan struct{}` of size `nildb.analytics-max-concurrent`, default 2), waiting up to `nildb.analytics-queue-timeout` (5 s), then `ERR too many analytical queries running`. `BUSY` is not used: redis-cli and several clients treat a `BUSY` prefix as "a script is running" and offer `SCRIPT KILL`.
2. Take a lease: `AT lease` reuses one; otherwise `Store.Lease("nil:"+connID, cfg.LeaseTTL)`, which calls `db.NewSnapshot()`, counts it against `--max-snapshots`, and is released when the reply is flushed or the cursor is deleted.
3. Plan the source (`NIL.EXPLAIN` prints it), in this order:
   - `index`: the leading `$match` bounds a `ready` btree index (`$eq`, `$in` as one seek per value, `$gt/$gte/$lt/$lte`, anchored `$regex`, compound-prefix rule, `HINT`); scan `idx` under the lease, collect `_id`s, `MultiGet` on `doc` in batches of 64 (one cgo call per batch), then run the full matcher. A `$sort` that matches the index direction (or its exact reverse) is dropped.
   - `columnar`: every field any stage touches has a `ready` columnar index, or is `_id`, and no stage needs the whole document (`$$ROOT`, `$project` exclusion). Open one bounded iterator per column on `col`, all ordered by `ks(_id)`, and k-way merge by `_id`; a column with no entry for a row yields a missing field.
   - `rowscan`: iterate the `doc` prefix, access fields with `bson.Raw.LookupErr` (no full unmarshal).
4. Every analytical iterator is built as:

```go
ro := grocksdb.NewDefaultReadOptions()
ro.SetSnapshot(lease.snap)
ro.SetFillCache(false)
ro.SetIterateLowerBound(lo)
ro.SetIterateUpperBound(hi)
ro.SetReadaheadSize(2 << 20)
ro.SetAsyncIO(true)
ro.SetTotalOrderSeek(true)                              // geo descendant ranges; harmless elsewhere
ro.SetDeadline(uint64(time.Now().UnixMicro()) + uint64(timeout/time.Microsecond)) // absolute micros
setRateLimiterPriority(ro, ioLow)                       // cgo shim, below
it := db.NewIteratorCF(ro, cf)
```

   `SetDeadline` takes an absolute microsecond timestamp (grocksdb's doc: "env->NowMicros() + some timeout"), not a duration; a runaway scan then returns `TimedOut` from `Iterator.Err()` and the command replies `ERR analytics timeout`.
5. Charge reads to the RocksDB limiter. grocksdb v1.11.1 has no wrapper for `rocksdb_readoptions_set_rate_limiter_priority`, but the linked Homebrew `c.h` (11.8.1, line 3139) declares it and a getter beside it. `internal/store/cgo_readopts.go` is a twelve-line cgo file that reads the `*C.rocksdb_readoptions_t` from the first field of `grocksdb.ReadOptions` (`c`, verified in `options_read.go`) and calls the setter with `IO_LOW` (0). A test asserts with `reflect` that field 0 is still named `c` and round-trips the value through `rocksdb_readoptions_get_rate_limiter_priority`; if either check fails the build tag `nildb_no_shim` disables the shim and the Go bucket below is the only throttle. With the limiter in `RateLimiterModeAllIo`, analytical file reads share the same budget as flushes (`IO_HIGH`) and compactions (`IO_LOW`), and OLTP reads stay at `IO_TOTAL` (uncharged). A block-cache hit is not IO and is not charged.
6. Bound the rest in Go: a token bucket over `len(key)+len(value)` per iterator step (`nildb.analytics-read-bps`, default 256 MiB/s, 0 = off), a hard row cap (`nildb.analytics-max-rows`, default 50,000,000, then `ERR analytics row limit reached`), and the memory accountant below. Rows scanned, bytes, plan, wall time and throttle sleep are recorded per command and exposed in `INFO analytics` with a one-minute histogram.
7. Operators are pull-based (`Next() (Row, bool, error)`). `$match` reuses `doc.Matcher`. `$group` is a hash aggregate keyed by `ks(groupIdValue)`, so `1` and `1.0` group together as MongoDB requires; accounted bytes = keys + accumulator state, capped by `MAXMEM` (100 MiB default for `NIL.AGGREGATE`, 32 MiB for `DOC.AGGREGATE`) with `ERR $group exceeded memory limit of N bytes (allowDiskUse is not supported)`. `$sort` followed by `$limit`/`$skip` is a bounded min-heap of `limit+skip` entries keyed by the encoded sort keys with `ks(_id)` appended for determinism; a bare `$sort` sorts in memory under the same cap. `$limit`/`$skip` and adjacent `$match` fuse, `$match` moves ahead of `$sort` and `$project` when it does not reference projected names. `$unwind` streams. `$geoNear` must be first and becomes the kNN operator of section 8 plus a distance field.
8. Results stream through a cursor pinned to the lease.

`DOC.AGGREGATE` runs the same planner and operators on the connection goroutine with OLTP read options (`fill_cache` true, no priority, no semaphore) and a 32 MiB `MAXMEM`, so a small aggregation behaves like a query and a large one is told to use `NIL.AGGREGATE`.

**Isolation controls**, all in config and `CONFIG SET nildb.*`:

- Block cache separation: `col` uses `colCache`; every analytical iterator sets `fill_cache=false`, so a full scan of `doc` cannot evict the OLTP working set from `mainCache`.
- IO priority: the shared auto-tuned limiter (`NewGenericRateLimiter(200<<20, 100_000, 10, RateLimiterModeAllIo, true)`) with analytical reads at `IO_LOW` through the shim.
- Memory: `NewWriteBufferManager(256<<20, true)` caps memtables across all eight CFs.
- Concurrency: the semaphore; analytical goroutines are ordinary connection goroutines, so a long scan blocks only its own connection and never a writer (it holds no server lock).
- Snapshot lifetime: `--max-snapshots` 32, lease TTL 60 s, maximum 600 s, janitor every second; `rocksdb.num-snapshots` and `rocksdb.oldest-snapshot-time` in `INFO rocksdb`.
- Deadline, row cap, byte bucket, `MAXMEM`.
- Escape hatch: `ROCKS.CHECKPOINT dir` then `nildb --readonly dir` opens the checkpoint with `OpenDbForReadOnlyColumnFamilies` in a second process: full isolation, freshness equal to the checkpoint's age. The on-disk format is plain RocksDB plus BSON.

**Columnar declaration.** `DOC.CREATEINDEX ns {"amount":"columnar"}` runs the background builder of section 5 (re-reading live rows under the document lock per batch), and writes arriving during the build maintain `col` because the index is in the collection's list before the snapshot is taken. Undeclared collections pay no extra write amplification; a declared field roughly doubles the write bytes for that field.

**Measurement hooks.** `INFO commandstats` carries per-command-group `calls`, `usec`, `p50`, `p99`, `p999` from a log-linear histogram in `server`; `INFO analytics` carries totals and the last minute. `cmd/htapbench` reads both.

## 8. Geo

Two implementations, because Redis and MongoDB disagree on latitude range, Earth radius and output precision.

**Redis GEO (`internal/geohash`, `internal/redis/geo.go`).** A port of Redis 7.2 `geohash.c`, `geohash_helper.c` and the search helpers of `geo.c` (BSD-3-Clause; Redis 8.x is tri-licensed and is never copied). `Encode(lon, lat)` rejects `|lat| > 85.05112878` or `|lon| > 180` with `ERR invalid longitude,latitude pair %f,%f`, scales each axis to 26 bits and interleaves them with the lookup-table `interleave64` (latitude in even bits, longitude in odd), giving a 52-bit integer that `GEOADD` stores as the zset score. `Decode` returns the cell centre, so `GEOPOS` drifts slightly from the input as in Redis. `Base32` re-encodes with latitude in ±90 and emits 11 characters. `Haversine` uses R = 6372797.560856 m; `GEODIST` and `WITHDIST` print `%.4f`. `GEOSEARCH`: `StepForRadius` doubles the radius until it exceeds `MERCATOR_MAX = 20037726.37`, backs off two steps, subtracts one above |66°| and another above |80°|, clamps to [1, 26]; the bounding box (BYBOX at the centre latitude), the centre cell at that step and its eight neighbours (`move_x`/`move_y` arithmetic) form the search set; the step drops by one if the nine cells fail to cover the box; neighbours entirely outside the box are zeroed; duplicate neighbour ranges at huge radii are skipped. Each remaining cell becomes `ScoreRange(hash, step) = [hash<<(52-2*step), (hash+1)<<(52-2*step))`, one bounded `zscore` iterator; every candidate is decoded from its score and re-checked exactly (haversine ≤ radius, or per-axis distance within w/2 and h/2 for BYBOX); results are sorted when `ASC|DESC` is given or `COUNT` lacks `ANY`; `COUNT n ANY` stops at n. Output order and bytes are checked against a transcript recorded from a `redis-server` 7.2 binary (section 9).

**2dsphere (`internal/geo`, golang/geo).** Earth radius 6378100 m (`kRadiusOfEarthInMeters`, confirmed in the fact checks), not golang/geo's `earth.Radius` of 6371.01 km; `Metres(a) = a.Radians() * 6378100`.

GeoJSON parsing (`Parse`): an object with `type` and `coordinates` (or `geometries` for `GeometryCollection`); positions are `[lon, lat]` with `lon ∈ [-180, 180]`, `lat ∈ [-90, 90]`; a bare `[x, y]` array or `{x, y}` object is a legacy pair converted to a Point, tried after GeoJSON (MongoDB v4 order). Supported types: Point, LineString, Polygon, MultiPoint, MultiLineString, MultiPolygon, GeometryCollection. Rings need at least four positions with first equal to last (the closing point is dropped for `LoopFromPoints`), no self-intersection (`Loop.Validate`), exterior ring first, holes inside it (every hole vertex passes the shell's `ContainsPoint`). Orientation: without a `crs`, every loop is `Normalize()`d so the polygon means the smaller of the two areas it cuts; with `crs: urn:x-mongodb:crs:strictwinding:EPSG:4326` (single ring only) the winding is kept with interior on the left, which is how a hemisphere-or-larger polygon is expressed. Polygons are assembled with `PolygonFromLoops` (shells at even depth, holes at odd). Invalid input replies `ERR Loop is not valid: <reason>` (MongoDB code 2). Null, missing and empty arrays index nothing; 2dsphere is always sparse.

Index time: parameters per index, frozen in the catalog record: `Coarsest = s2.AvgEdgeMetric.ClosestLevel(2_000_000 / R)`, `Finest = s2.AvgEdgeMetric.ClosestLevel(110 / R)`, `MaxCells = 20` (MongoDB v3 and v4 defaults, computed at index creation rather than hard-coded; the fact check expects levels 2 and 16). A Point produces one key at its level-30 leaf cell with lat/lng in the value. LineString, Polygon and Multi\* produce `RegionCoverer{MinLevel: Coarsest, MaxLevel: Finest, LevelMod: 1, MaxCells: MaxCells}.Covering(region)`; `Polyline` and `Polygon` implement `s2.Region`; a `GeometryCollection` or `MultiPoint` is the union of its parts' coverings, value kind `0x02`.

`$geoWithin` accepts `$geometry` (Polygon, MultiPolygon) and `$centerSphere` (`s2.CapFromCenterAngle`, radius in radians); legacy `$box`, `$polygon` and `$center` reply `ERR $geoWithin legacy shapes require a 2d index (not supported)`. `$geoIntersects` accepts any supported type. Plan (`PlanRegion`): cover the query region with a `RegionCoverer` whose `MinLevel` and `MaxLevel` are clamped to the index's `[Coarsest, Finest]` and `MaxCells` is `nildb.geo-query-max-cells` (16); for every covering cell `c` emit the descendant range `[idx|c.RangeMin(), idx|c.RangeMax()]` as one iterator with `TotalOrderSeek` (the range crosses 12-byte prefixes), and for every level from `c.Level()-1` down to `Coarsest` one probe of the 12-byte prefix `idx|c.Parent(l)` with `PrefixSameAsStart`, because a large shape may be indexed at a coarser cell than the query cell. Clamping to `Finest` bounds the probes per cell to `Finest - Coarsest` (14 with the defaults) instead of the 28 the base proposal allowed. `Ranges` are sorted and merged, probes deduplicated. Candidate `_id`s are deduplicated in a set bounded by `MAXMEM`, loaded from `doc`, parsed and refined exactly: within, point → `Polygon.ContainsPoint`; polyline → every vertex contained and no edge crossing (`NewCrossingEdgeQuery` over the query polygon's `ShapeIndex`); polygon → `Polygon.Contains`. Intersects, point → `ContainsPoint`; polyline vs polygon → any vertex inside or any crossing; polygon vs polygon → `Polygon.Intersects`; polyline vs polyline → `Polyline.Intersects`; point query vs polygon document → the document's `ContainsPoint`. Without an index the same predicate runs over a collection scan (MongoDB allows this). `$geoWithin` results are unsorted, as in MongoDB.

`$near`, `$nearSphere`, `$geoNear` (kNN). The centre must be a Point (MongoDB 8.0). `$minDistance`/`$maxDistance` in metres become angles by `/ R`. Expanding annulus after MongoDB's `geo_near.cpp`: `r_lo = minDistance`, `r_hi = min(maxDistance, 1 km)` at first; each round covers `Cap(P, r_hi)` with the clamped query coverer, subtracts the already-scanned `CellUnion` (`CellUnionFromDifference`), scans the remaining ranges plus ancestor probes (each ancestor once), computes exact distances for new candidates (points from the index value without reading the row, `LatLng.Distance`; shapes by reading the row and running `NewClosestEdgeQuery(shapeIndex, opts).Distance(NewMinDistanceToPointTarget(P))`, zero when the polygon contains `P`, because golang/geo's `Polygon` has no `DistanceToPoint`), applies the rest of the filter, and pushes into a min-heap. Because the covering is a superset of the cap, every document within `r_hi` has been seen after the round, so heap entries with `dist ≤ r_hi` are emitted in ascending order; then `r_lo = r_hi`, and the increment doubles when the round returned fewer than 300 results or halves above 600, until the limit is met, `r_hi ≥ maxDistance`, or the covering is the whole sphere. Memory is the current ring's candidates plus a seen-set of `_id`s. `$geoNear` adds `distanceField` (× `distanceMultiplier`), `includeLocs`, `query`, `key` (required when the collection has more than one 2dsphere index).

`NIL.GEOGROUP` truncates each indexed cell to `Parent(level)` while iterating the index prefix under a lease and counts per parent; shapes indexed at cells coarser than `level` count once under their own cell.

```go
package geo

const EarthRadiusMetres = 6378100.0

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
func Parse(v bson.RawValue, o ParseOpts) (*Geometry, error) // "Loop is not valid: ..." and friends
func (g *Geometry) Region() s2.Region                        // one part, or a CellUnion bound of the parts
func (g *Geometry) LatLng() (s2.LatLng, bool)                // single Point only
func (g *Geometry) ShapeIndex() *s2.ShapeIndex               // built lazily for distance and crossing queries

type Params struct{ Coarsest, Finest, MaxCells int }
func DefaultParams() Params // ClosestLevel(2000 km / R), ClosestLevel(110 m / R), 20

type Entry struct {
	Cell   s2.CellID
	Kind   byte      // 0x01 point (LatLng valid), 0x02 shape or multi
	LatLng s2.LatLng
}
func (p Params) Entries(g *Geometry) []Entry // leaf cell for a Point; covering otherwise

type Range struct{ Lo, Hi s2.CellID } // inclusive descendant range
type Plan struct {
	Ranges []Range     // sorted, merged
	Probes []s2.CellID // ancestor probes, deduplicated
}
func (p Params) PlanRegion(r s2.Region, queryMaxCells int) Plan
func (p Params) PlanAnnulus(center s2.Point, outer s1.Angle, scanned s2.CellUnion, queryMaxCells int) (Plan, s2.CellUnion)

type Predicate uint8
const (Within Predicate = iota; Intersects)
func Refine(pred Predicate, query, doc *Geometry) bool
func Distance(from s2.Point, doc *Geometry) s1.Angle // 0 when a polygon contains from
func Metres(a s1.Angle) float64
func Angle(metres float64) s1.Angle

// The kNN driver is storage-agnostic so it is unit-tested without RocksDB.
type Scan  func(r Range, visit func(cell s2.CellID, ksID, val []byte) bool) error
type Probe func(cell s2.CellID, visit func(ksID, val []byte) bool) error
type Load  func(ksID []byte) (*Geometry, error)
type NearOpts struct {
	Min, Max      s1.Angle
	Limit         int // 0 = unlimited
	QueryMaxCells int
	Filter        func(ksID []byte) bool // the rest of the query, nil = accept
}
func Near(p Params, center s2.Point, o NearOpts, scan Scan, probe Probe, load Load,
	emit func(ksID []byte, d s1.Angle) bool) error
```

## 9. Test strategy

**Unit tests per package**, run with `make test` (the Makefile exports `CGO_CFLAGS=-I/opt/homebrew/include CGO_LDFLAGS=-L/opt/homebrew/lib`).

- `keyenc`: a property test drawing 100,000 random pairs of BSON values and asserting `bytes.Compare(ks(a), ks(b))` equals a reference implementation of MongoDB's comparison order, with the int64/double boundaries at 2^53 and 2^63 explicitly enumerated (`2^53`, `2^53+1`, `2^53+3` vs `2^53+4`, `-2^53`, `-2^63`, `2^63` as double, `±0`, `NaN`, `±Inf`, `1` vs `1.0`); descending inversion; round trips.
- `layout`: hex goldens for every layout in section 3; version generator monotonicity under a clock that steps back.
- `resp`: a table lifted from `tests/unit/protocol.tcl` and Kvrocks's `protocol_test.go` (empty query, negative, out-of-range and non-numeric lengths, `expected '$', got 'f'`, unbalanced quotes, 64 KiB inline, pre-AUTH caps, command split across reads) asserting exact reply bytes and connection close; a RESP2-versus-RESP3 shape table for double, map, set, null, verbatim, bool, `ZRANGE WITHSCORES`, `HGETALL`, `SMEMBERS`, `INFO`.
- `store`: open with eight CFs; batch atomicity across CFs; `BeginIndexed` read-your-writes; `DeleteRange` refusal under `BeginIndexed`; lease cap and janitor; the three compaction filters against forced `Flush` plus `CompactRangeCF` (expired metadata, stale versions, dropped ids vanish; fresh versions and live ids survive); a filter race test with `max_subcompactions=4` under `-race`; lock ordering and two-phase acquisition under `-race`; the cgo shim's field check and round trip.
- `geohash`: vectors captured from `redis-server` 7.2 and `tests/unit/geo.tcl` (`GEOADD Sicily 13.361389 38.115556 Palermo` → hash `3479099956230698`, `GEOHASH` `sqc8b49rny0`, `GEODIST Palermo Catania` between 166274 and 166275 at `%.4f`), edge cases at ±85° and the antimeridian, step estimation per radius.
- `geo`: covering determinism; predicate tests; the brute-force fixture below driven through `Near` with in-memory `Scan`/`Probe`/`Load`.
- `doc`: about 300 matcher fixtures (null versus missing under `$ne`/`$nin`/`$not`/`$exists`, type bracketing, array "any element" versus `$elemMatch` versus dotted paths, `$size`, `$all`, regex), update-operator fixtures including operator ordering, projection fixtures, Extended JSON round trips.
- `catalog`, `docstore`, `query`: index maintenance on update and delete (old entries gone, new present), unique `E11000` across `_id`s under concurrency, sparse and multikey rules, TTL sweeper, background build against concurrent writers (asserts no stale or orphan entries), planner goldens for bounds and index selection, `$group` memory cap.
- `server`: MULTI/EXEC/WATCH sequences from `multi.tcl` against the fake clock (EXECABORT, null array after a touched key, expiry-triggered abort, stale watched keys survive FLUSHDB), SCAN cursor ring eviction and foreign-db rejection.

**Raw-socket protocol tests** (`tests/protocol`): `testutil.Spawn` starts the server on a free port; tests write bytes and compare replies byte-for-byte, including the `HELLO 2` and `HELLO 3` handshakes, 10,000-command pipelines, `PING\r\nPING\r\nPING\r\n` inline, and close after each protocol error.

**go-redis v9 integration** (`tests/gocase`, modelled on Kvrocks): clients with `Protocol: 3` and `Protocol: 2`; suites per command group; MULTI/WATCH races with two clients; expiry with the fake clock advanced through `NIL.DEBUG CLOCK-ADVANCE ms` (enabled by `--enable-debug-commands`); the `scan.tcl` "guarantees check under write load" port; `DOC.*` CRUD with Extended JSON round trips (ObjectId, Date, Int64, binary), unique index `E11000`, cursor paging, `NIL.AGGREGATE` results compared with an in-test brute-force evaluation on the same fixture.

**redis-cli and redis-benchmark** (`make compat`, needs `brew install redis`; ask before installing): `redis-cli -3 PING`, `--pipe` with 100,000 `SET`s ending in `ECHO` then `DBSIZE`, `--scan --pattern`, `--bigkeys`, an interactive session (the `COMMAND DOCS` path); `redis-benchmark -q -n 200000 -P 16 -t ping,set,get,incr,lpush,rpush,lpop,rpop,sadd,hset,zadd,zpopmin,lrange_100` and again with `-3`; zero protocol errors is the gate and p50/p99 land in `bench/history.csv`. The upstream TCL suite runs from the Redis 7.2 tag with `./runtest --host 127.0.0.1 --port 6380 --singledb --ignore-encoding --ignore-digest --tags -needs:debug --tags -needs:repl --tags -needs:save --single unit/type/string` and the same for `incr`, `hash`, `list`, `set`, `zset`, `expire`, `multi`, `scan`, `keyspace`, `geo`, `protocol`; the per-test pass/fail table is committed to `docs/compat/redis-tcl.md`, and the known failures (randomness distribution tests for `SPOP`/`SRANDMEMBER`, tests that need `DEBUG`) are listed with reasons.

**Geo correctness fixture** (`testdata/geo/`, generated deterministically from a seed). Redis side: 10,000 points within ±85° and 1,000 `GEOSEARCH` queries (radius and box, every unit, `ASC`/`DESC`/`COUNT`/`ANY`) whose expected member sets and 4-decimal distances come from a brute-force haversine oracle, plus a transcript of `GEOADD`, `GEOHASH`, `GEOPOS`, `GEODIST` and `GEOSEARCH` replies recorded from `redis-server` 7.2 and compared byte-for-byte. S2 side: 5,000 points, 500 polygons (convex, concave, with holes, one crossing the antimeridian, one polar cap, one hemisphere-plus with `strictwinding`), 200 polylines; queries for `$geoWithin`, `$geoIntersects` and `$near` with min and max distance whose expected results come from running the exact predicates over every document without the index. Any set difference is a covering or ancestor bug; kNN order must match and distances agree to 1e-6 relative. The fixture is re-run after deleting 30% of the documents and after a columnar build to catch stale index entries.

**HTAP interference measurement** (`cmd/htapbench`). Load 2,000,000 documents with three columnar fields and one btree index, plus 1,000,000 Redis keys. Phase A: 60 s of a 50-connection OLTP mix (`GET`/`SET`/`HSET`/`ZADD` and `DOC.GET` by `_id`); read QPS, p50 and p99 from `INFO commandstats`. Phase B: the same mix while `NIL.AGGREGATE` `$match`+`$group` over all documents runs back-to-back on the `columnar` plan; Phase C: the `rowscan` plan; Phase D: throttle and priority disabled. Report throughput degradation and p99 inflation per phase and `rocksdb.block-cache-usage` before and after. Acceptance for v1: throughput drop ≤ 20% and p99 ≤ 2× baseline under Phase B with default knobs; TiDB's ≤ 10% assumes TiKV and TiFlash on separate servers and a single node sharing one SSD and one CPU will not match it. Freshness: insert a marker document immediately before each aggregate and assert it is counted (expected gap 0). Output is a markdown table in `bench/results/`.

## 10. Risks and mitigations

- **grocksdb v1.11.1 against Homebrew 11.8.1 is untested upstream** (CI builds 11.1.2 statically on Ubuntu). The fact check diffed `c.h` across that range: 743 added functions, no type or arity change among the 1048 shared, one dead declaration removed. `make rocksmoke` exercises CFs, multi-CF batches, merge, compaction filter, snapshot, bounded iterator, checkpoint and properties and passes on this Mac; `brew pin rocksdb` freezes the pairing; `ROCKS.INFO` reports the linked version. Fallback: build 11.1.2 statically with grocksdb's `build.sh`.
- **No compaction filter factory.** One shared instance per CF; each filter is a pure function plus a 64-slot sharded cache with per-slot mutexes, never a Go map; the `-race` test with `max_subcompactions=4` guards it.
- **cgo callback cost in filters.** Every compacted key on `meta`, `sub`, `zscore`, `doc`, `idx`, `geo` and `col` re-enters Go. Fast paths (header decode, the 60 s version-age shortcut, the LiveSet lookup) run before any DB access; the verdict cache makes `subFilter` about one `Get` per distinct version per subcompaction. Measured on the htapbench dataset; if `rocksdb.compaction.times.micros` shows the callback dominating, the fallback is a background scanner for `sub` garbage with the filter reduced to the age check.
- **No `auto_prefix_mode` in grocksdb.** Every iterator sets both bounds; scans that may cross a prefix on `sub`, `zscore` or `geo` set `TotalOrderSeek`; point-op prefix scans set `PrefixSameAsStart`; `meta`, `doc`, `idx` and `col` carry no extractor.
- **The `rate_limiter_priority` shim reads a private field.** Guarded by the reflect check and getter round trip in `store`'s tests and by the `nildb_no_shim` tag; the Go byte bucket and the semaphore remain the isolation floor.
- **WBWI has no `DeleteRange`.** `FLUSHDB`, `FLUSHALL`, `DOC.DROP`, `DOC.DROPINDEX` are `NoMulti`, and `Txn.DeleteRange` refuses under `BeginIndexed`.
- **Filters ignore snapshots** (RocksDB ≥ 6.0). Leases are short (60 s default, 600 s maximum, 300 s filter grace); the docs say an expired key may vanish from a held snapshot.
- **cgo call overhead per operation** (about 100-200 ns on Apple Silicon plus copies). One `Get` or bounded iterator plus one `Write` per command; `MultiGetCF` for `MGET`, `HMGET` and index fan-out; `KeySlice`/`ValueSlice` views in hot iterators; `GetPinned` on the metadata path; replies encoded straight into the connection buffer. Target on this Mac: ≥ 100,000 ops/s for `redis-benchmark -P 16 -t set,get`; the M1 run sets the baseline and, if it misses, the first fix is coalescing pipelined reads into `MultiGetCF`.
- **Snapshot pinning.** Long cursors and leases block compaction from dropping overwritten versions: cap 32, TTLs, janitor, `NIL.SNAPSHOT LIST` and `INFO rocksdb` visibility.
- **Range tombstones** from frequent drops: `CompactRangeCF` scheduled after every drop, forced at 32 outstanding, backlog visible through `rocksdb.estimate-pending-compaction-bytes`.
- **Memory caps without spill.** `$group` and `$sort` fail loudly at `MAXMEM`; `allowDiskUse` is declared unsupported rather than approximated.
- **Numeric keystring correctness.** The property test against MongoDB's order is the guard; Decimal128 is excluded from indexes rather than approximated.
- **Regex dialect.** RE2 has no backreferences or lookaround; `$regex` documents the subset and replies `ERR Regular expression is invalid`.
- **Clock going backwards** would break version uniqueness; `NewVersion` never returns less than `last+1`, the counter absorbs bursts, and the last version is persisted at shutdown.
- **golang/geo has no tags and marks `Polygon` "mostly complete".** Pinned pseudo-version; NilDB uses `ContainsPoint`, `Contains`, `Intersects`, `ContainsCell`, `IntersectsCell`, loops, coverers, `CrossingEdgeQuery` and `EdgeQuery`, all exercised by the fixture; `Polygon.DistanceToPoint` is on the library's own TODO list, so distances go through `EdgeQuery`; no boolean operations are promised.
- **Redis version string.** `7.2.0` keeps clients away from 8.x features; it is a config knob.
- **Durability on macOS.** `always` costs an `F_FULLFSYNC` per commit (about 2.5 ms on APFS); `everysec` is the default and the README states the one-second power-loss window.
- **Collection write lock for filter-driven updates and DDL** serialises those against every writer of the collection; documented per command, and point writes keep per-document parallelism.
- **SCAN cursor ring** diverges from Redis's stateless cursors: 16384 in-flight cursors, `ERR invalid cursor` on eviction, and cursors do not survive a restart; documented.
- **Licensing.** Code is copied only from Redis 7.2 (BSD-3-Clause, `geohash*`) and Apache-2.0 projects; MongoDB server, CockroachDB `pkg/geo` and `mongodb/specifications` are read for parameters and algorithms only, and no CC BY-NC-SA fixture ships in the repo.

## 11. Build order

Ten milestones; each compiles, has passing tests and ends with `make vet test` green and a tag. Lanes within a milestone touch disjoint packages. The file-by-file plan is in [build-plan.md](build-plan.md).

- **M0, store and codec** (4 lanes in parallel, all against contracts fixed in this file): `internal/config`; `internal/resp` with the protocol table; `internal/keyenc` and `internal/layout` with the property tests; `internal/store` with eight CFs, options, `Txn`, `Reader`, snapshots and leases, `LockManager`, `metaFilter`, `nildb.i64add`, the cgo shim and admin calls. Also `go.mod` trims redcon and pins bson and golang/geo; the Makefile gains `compat` and `tcl` targets. Gate: `make rocksmoke` and every package's tests.
- **M1, RESP server plus strings and keys**: `internal/command`, `internal/server` (connection loop, connection commands, `INFO server|clients|memory`), `internal/redis/{string,generic,expire}.go`, `internal/testutil`, `cmd/nildb`. Gate: `redis-cli SET/GET/INCR/EXPIRE`, go-redis string suite in both protocols, `redis-benchmark -t ping,set,get,incr -P 16` baseline recorded, TCL `unit/type/string`, `unit/type/incr`, `unit/expire`, `unit/keyspace` results.
- **M2, hashes, lists, sets, sorted sets**: `internal/redis/{hash,list,set,zset}.go`. Gate: TCL `unit/type/{hash,list,set,zset}`, go-redis suites.
- **M3, MULTI/EXEC/WATCH, SCAN, expiry filters**: MULTI queue, WATCH table and EXEC in `server`; the cursor ring and `SCAN`/`HSCAN`/`SSCAN`/`ZSCAN`; `subFilter` and `idFilter` in `store` with the `-race` compaction tests; `KEYS`, `DBSIZE`, `RANDOMKEY`, `MOVE`, `FLUSHDB`. Gate: TCL `unit/multi`, `unit/scan`; the write-load SCAN test; filter tests.
- **M4, GEO\***: `internal/geohash` (can start at M0, pure) and `internal/redis/geo.go`. Gate: TCL `unit/geo`; the Redis geo fixture and transcript.
- **M5, documents, indexes, cursors**: `internal/doc` (pure, can start at M0), `internal/catalog`, `internal/docstore` (insert, replace, update, delete, unique, multikey, sparse, drops, background builder, TTL sweeper), `internal/query` (planner, doc scan, index scan, match, project, sort, limit, skip, cursors), `internal/cmddoc`. Gate: go-redis `DOC.*` suite, index-backed finds, `E11000` under concurrency, builder race test.
- **M6, S2 geo queries**: `internal/geo` (pure, can start at M1), 2dsphere maintenance in `docstore`, `$geoWithin`/`$geoIntersects`/`$near` operators in `query`, `$geoNear`. Gate: the S2 fixture including the delete and rebuild re-runs.
- **M7, HTAP**: columnar maintenance in `docstore`, `col` k-way merge, `$group`/`$unwind`/expression evaluator in `query`, `internal/analytics` (semaphore, bucket, leases, row cap, `NIL.*`), isolation knobs wired, `INFO analytics`. Gate: `NIL.EXPLAIN` chooses `index`/`columnar`/`rowscan` per fixture; aggregation results match the brute-force oracle; `cmd/htapbench` produces its table.
- **M8, ROCKS.\* and INFO**: `internal/admin`, `INFO rocksdb|commandstats|persistence|stats`, `--readonly` open of a checkpoint. Gate: every `ROCKS.*` command has a test; `ROCKS.CHECKPOINT` plus `--readonly` round trip.
- **M9, integration tests and README**: `tests/protocol`, `tests/gocase` completed, `make compat`, `make tcl` with `docs/compat/redis-tcl.md`, `docs/compat/commands.md` with the supported and unsupported tables and footnotes, README with build, run, durability and HTAP numbers, `bench/results`, v1.0 tag.

Parallel ownership: lane R (Redis types) owns `internal/redis` through M1-M4; lane D (documents) owns `internal/doc` from M0 and `catalog`, `docstore`, `query`, `cmddoc` from M1 onward, landing at M5; lane G (geo) owns `geohash` from M0 and `geo` from M1, landing at M4 and M6; lane S (server) owns `command`, `server`, `testutil` from M0 and `admin` at M8; lane A (analytics) owns `analytics`, the analytical operators in `query` (files `group.go`, `col_scan.go`, `expr.go`) and `cmd/htapbench`, starting when M5's `query` skeleton lands. The only files two lanes touch are `cmd/nildb/main.go` (one `Register` line per package) and `internal/testutil/server.go` (the same), which are one-line merges.
