# NilDB v1 build plan

Section 11 of [architecture.md](architecture.md) expanded file by file. Every package below has a stable contract (the exported names listed here), so an engineer can write it against the contract while another package is still a stub. Signatures repeat the ones in architecture.md; where this file is more specific, it is the one to code against.

Conventions: Go 1.27.1, `gofmt`, `go vet`, table-driven tests, `testing.Short()` skips anything that opens RocksDB. The Makefile exports `CGO_CFLAGS=-I/opt/homebrew/include CGO_LDFLAGS=-L/opt/homebrew/lib`; run `go` through it. No commits or pushes unless Arka asks. `tmp/` is gitignored, `docs/` is not.

## Milestone table

| Milestone | Delivers | Packages | Concurrent lanes (disjoint files) | Gate |
|---|---|---|---|---|
| M0 store + codec | toolchain, contracts, storage core | `config`, `resp`, `keyenc`, `layout`, `store`; `go.mod` trim | 4: config+resp / keyenc+layout / store / geohash (pure, starts early) | `make rocksmoke`; `go test ./internal/...` |
| M1 RESP server + strings/keys | a Redis you can talk to | `command`, `server`, `redis/{string,generic,expire}`, `testutil`, `cmd/nildb` | 3: command+server+testutil / redis strings+keys / doc (pure, starts here) | redis-cli, go-redis strings RESP2+3, redis-benchmark baseline, TCL string/incr/expire/keyspace |
| M2 hashes/lists/sets/zsets | Redis collections | `redis/{hash,list,set,zset}` | 2: hash+set / list+zset (+ geo pure lane, catalog lane) | TCL type/hash, list, set, zset; go-redis suites |
| M3 MULTI/EXEC/WATCH + SCAN + expiry filter | transactions, iteration, GC | `server/{multi,watch,cursor}`, `redis/scan.go`, `store/{filter_sub,filter_id}`, `redis/generic` additions | 3: server txn / scan+keys / store filters | TCL multi, scan; SCAN write-load test; filter -race tests |
| M4 GEO* | Redis GEO | `geohash`, `redis/geo.go` | 1 (+ documents lane continues) | TCL geo; Redis geo fixture + transcript |
| M5 documents + indexes + cursors | `DOC.*` | `doc`, `catalog`, `docstore`, `query` (core), `cmddoc` | 3: catalog+docstore / query core+cursors / cmddoc | go-redis DOC suite; E11000 race; builder race |
| M6 S2 geo queries | 2dsphere | `geo`, `docstore/geo_maint.go`, `query/geo_*.go` | 2: geo package / docstore+query integration | S2 fixture incl. 30% delete and rebuild re-runs |
| M7 HTAP | columnar CF, `NIL.AGGREGATE`, isolation | `docstore/col_maint.go`, `query/{col_scan,group,expr,...}`, `analytics`, `store/analytics_ro.go` | 3: query analytical operators / analytics package / htapbench | EXPLAIN plan tests; oracle comparison; htapbench table |
| M8 ROCKS.* + INFO | native RocksDB surface | `admin`, `server/info.go` additions, `cmd/nildb --readonly` | 2: admin / info+readonly | every ROCKS.* tested; checkpoint round trip |
| M9 integration tests + README | release | `tests/protocol`, `tests/gocase`, `docs/compat/*`, `README.md`, `bench/` | 3: protocol+gocase / compat+tcl tables / README+bench | `make compat`, `make tcl`, v1.0 tag |

"Concurrent lanes" means engineers can work on those groups in the same milestone without editing the same file. The two shared files, `cmd/nildb/main.go` and `internal/testutil/server.go`, receive one `Register` line per command package and nothing else.

## Repository changes at M0

- `go.mod`: drop `github.com/tidwall/redcon` (and `tidwall/btree`, `tidwall/match`); promote `go.mongodb.org/mongo-driver/v2 v2.9.1` and `github.com/golang/geo v0.0.0-20260818125358-b200a1149890` to direct requirements; keep `github.com/redis/go-redis/v9 v9.22.0` as a test dependency; `github.com/cespare/xxhash/v2` becomes direct (stripe hashing). `go mod tidy`.
- `Makefile`: add `compat` (redis-cli and redis-benchmark script), `tcl` (`REDIS_SRC=... ./runtest ...`), `bench` (htapbench), `check` (`vet test-race rocksmoke`).
- `docs/compat/` created empty with a README line; `testdata/geo/` created by the fixture generator at M4/M6.

## internal/config (M0)

Files: `config.go`, `flags.go`, `config_test.go`.

```go
type Config struct {
	Dir, Addr, RequirePass, Fsync   string        // Fsync: always|everysec|no
	Databases                       int           // fixed 16 in v1; validated
	RedisVersion                    string        // "7.2.0"
	BlockCacheMB, AnalyticsCacheMB  int           // 512, 64
	WriteBufferMB                   int           // 256
	BgIOMBPerSec                    int           // 200
	MaxSnapshots                    int           // 32
	LeaseTTL, LeaseMax              time.Duration // 60 s, 600 s
	AnalyticsMaxConcurrent          int           // 2
	AnalyticsQueueTimeout           time.Duration // 5 s
	AnalyticsReadBPS                int64         // 256 MiB/s, 0 = off
	AnalyticsMaxRows                int64         // 50_000_000
	AnalyticsGroupMem, DocAggMem    int64         // 100 MiB, 32 MiB
	GeoQueryMaxCells                int           // 16
	SetOpMaxMembers                 int           // 1_000_000
	MultiQueueMax                   int           // 100_000
	RocksStats, EnableDebugCommands bool
	ReadOnly                        string        // checkpoint dir for --readonly
	Clock                           Clock
}
type Clock interface{ Now() time.Time }
type OffsetClock struct{ /* atomic offset */ }   // used by testutil and NIL.DEBUG CLOCK-ADVANCE
func (c *OffsetClock) Now() time.Time
func (c *OffsetClock) Advance(d time.Duration)
func Default() Config
func Parse(args []string) (Config, error)        // flags then optional --config file (TOML-free key=value)
func (c *Config) Validate() error
// Runtime knobs reachable through CONFIG GET/SET nildb.*
func (c *Config) Get(name string) (string, bool)
func (c *Config) Set(name, value string) error
```

Imports: none. Tests: defaults, validation errors, `Set` round trips.

## internal/resp (M0)

Files: `reader.go`, `writer.go`, `reply.go`, `errors.go`, `reader_test.go`, `writer_test.go`, `shape_test.go`.

Exported: `Limits`, `DefaultLimits()`, `Reader`, `NewReader`, `(*Reader).SetAuthenticated`, `(*Reader).Next`, `(*Reader).Buffered`, `ProtocolError{Text string}`, `Writer`, `NewWriter`, `(*Writer).SetProto/Proto/Write/Flush`, `Reply`, the constructors `OK Status Int Bulk Str Null NullArray Array Map Set Double Bool Verbatim Err Errorf Raw`, the error variables and `ErrArity`, `ErrUnknown` from architecture.md, plus `ParseInt(b []byte) (int64, bool)` and `ParseFloat(b []byte) (float64, bool)` that accept exactly what Redis accepts (`string2ll` rules: no leading `+`, no whitespace, no leading zeros beyond `0`; floats accept `inf`, `-inf`, `+inf`).

Reader rules: first byte `*` → multibulk with Redis limits and the exact `Protocol error:` strings (`invalid multibulk length`, `unauthenticated multibulk length`, `expected '$', got '%c'`, `invalid bulk length`, `unauthenticated bulk length`, `too big mbulk count string`, `too big bulk count string`, `too big inline request`, `unbalanced quotes in request`); otherwise an inline line split with Redis quoting; empty inline line and `*0` are no-ops. `Next` returns `[][]byte` slices that alias the reader's buffer only until the next call.

Writer: encodes into a `bufio.Writer` of 64 KiB; `Double` prints with `strconv.FormatFloat(f, 'f', -1, 64)` in RESP2 bulk form and `,` form in RESP3 (`inf`, `-inf`, `nan` spelled as Redis does); `Verbatim` in RESP2 is a plain bulk.

Imports: none. Tests: the protocol.tcl table (each row: input bytes, expected reply bytes, close expected), the RESP2/RESP3 shape table, pipelined 10,000 `PING`s through a `bytes.Buffer`.

## internal/keyenc (M0)

Files: `encode.go`, `decode.go`, `numeric.go`, `compare_test.go`, `numeric_test.go`, `golden_test.go`.

```go
func Encode(dst []byte, v bson.RawValue) ([]byte, error)      // ErrUnsupported for Decimal128, MinKey/MaxKey allowed
func EncodeDesc(dst []byte, v bson.RawValue) ([]byte, error)  // XOR 0xFF over the encoding
func EncodeMissing(dst []byte) []byte                          // type byte 20
func Decode(b []byte) (v bson.RawValue, rest []byte, err error) // for cursors, EXPLAIN and NIL.GEOGROUP keys
func DecodeDesc(b []byte) (bson.RawValue, []byte, error)
func SortableDouble(f float64) uint64
func SortableInt64(i int64) uint64
func Next(prefix []byte) []byte                                 // prefix with its last byte incremented, with carry; nil when all 0xFF
func TypeOrder(t bsontype.Type) byte
const (TMinKey=10; TUndefined=15; TNull=20; TNumber=30; TString=60; TObject=70; TArray=80; TBinData=90; TOID=100; TBool=110; TDate=120; TTimestamp=130; TRegex=140; TDBPointer=150; TCode=160; TCodeWScope=170; TMaxKey=240)
```

Imports: bson only. Tests: the 100,000-pair property test against a reference comparator that implements MongoDB's comparison-order page; the enumerated int64/double boundary cases; descending; `Next` carries.

## internal/layout (M0)

Files: `redis.go` (meta keys and headers), `version.go`, `subkey.go`, `doc.go` (doc, idx, geo, col keys), `catalog.go`, `layout_test.go`.

```go
// Redis
func MetaKey(dst []byte, db uint8, key []byte) []byte
func SplitMetaKey(k []byte) (db uint8, key []byte, ok bool)
func VersionKey(dst []byte, version uint64) []byte              // 0x76 | version
type Meta struct {
	Type       Type   // TString..TZSet
	ExpireMS   int64
	Version    uint64 // collections
	Size       int64
	Head, Tail uint64 // lists
	Payload    []byte // strings; aliases the input
}
func DecodeMeta(v []byte) (Meta, error)
func (m Meta) Encode(dst []byte) []byte
func (m Meta) Expired(nowMS int64) bool
type VersionEntry struct{ ExpireMS int64; DB uint8; Key []byte }
func (e VersionEntry) Encode(dst []byte) []byte
func DecodeVersionEntry(v []byte) (VersionEntry, error)
func SubKey(dst []byte, version uint64, sub []byte) []byte
func ListKey(dst []byte, version uint64, index uint64) []byte
func ScoreKey(dst []byte, version uint64, score float64, member []byte) []byte
func SplitScoreKey(k []byte) (version uint64, score float64, member []byte)
func VersionOf(k []byte) uint64                                 // first 8 bytes
func VersionMicros(v uint64) int64
type VersionGen struct{ /* atomic last */ }
func NewVersionGen(seed uint64) *VersionGen
func (g *VersionGen) Next(now time.Time) uint64
func (g *VersionGen) Last() uint64
// Documents
func DocKey(dst []byte, coll uint32, ksID []byte) []byte
func IdxKey(dst []byte, idx uint32, ksValues ...[]byte) []byte   // caller appends ksID for non-unique
func GeoKey(dst []byte, idx uint32, cell uint64, ksID []byte) []byte
func ColKey(dst []byte, idx uint32, ksID []byte) []byte
func IDPrefix(dst []byte, id uint32) []byte                     // [id, id+1) bounds via keyenc.Next
func SplitDocKey(k []byte) (coll uint32, ksID []byte)
func SplitGeoKey(k []byte) (idx uint32, cell uint64, ksID []byte)
// Catalog
func CollKey(ns string) []byte; func IndexKey(coll uint32, name string) []byte; func CountKey(coll uint32) []byte; func SeqKey(name string) []byte
var LayoutMarkerKey = []byte{0}; const LayoutMarker = "nildb-layout-1"
```

Imports: keyenc. Tests: hex goldens for every layout; `VersionGen` under a clock stepping back; `Expired` at boundaries.

## internal/store (M0, filters completed M3, analytics read options M7)

Files: `store.go` (Open, Close, CF handles), `options.go` (per-CF options table), `txn.go` (plain and indexed), `reader.go` (live and snapshot readers, `GetPinned`, `MultiGet`, `Iter`), `iterator.go`, `snapshot.go` (leases, janitor), `lock.go`, `liveset.go`, `filter_meta.go` (M0), `filter_sub.go` (M3), `filter_id.go` (M3), `merge.go` (`nildb.i64add`), `admin.go`, `drop.go` (`DeleteRanges`, `ScheduleCompact`), `cgo_readopts.go` and `cgo_readopts_noshim.go` (build tags), `version.go` (cgo read of `<rocksdb/version.h>`), tests per file plus `filter_race_test.go`, `shim_test.go`.

Exported: everything in architecture.md's store contract, plus:

```go
type Type = layout.Type
func (s *Store) Clock() func() time.Time
func (s *Store) VersionGen() *layout.VersionGen
func (s *Store) WriteOpts() (sync bool)               // from Fsync
func (s *Store) Handle(cf CF) any                    // *grocksdb.ColumnFamilyHandle, for tests in this package only; unexported in practice
var ErrTooManySnapshots = errors.New("store: too many open snapshots")
var ErrLeaseExpired   = errors.New("store: snapshot lease expired")
```

Filters:

```go
type metaFilter struct{ now func() int64 }                         // pure
type subFilter struct{ db atomic.Pointer[grocksdb.DB]; meta *grocksdb.ColumnFamilyHandle; now func() int64; cache [64]struct{ mu sync.Mutex; version uint64; drop bool } }
type idFilter struct{ live atomic.Pointer[liveSetBox] }
```

Each implements `Filter(level int, key, val []byte) (bool, []byte)`, `Name() string`, `SetIgnoreSnapshots(bool)` (no-op), `Destroy()`. The grace constants `metaGrace = 300 * time.Second`, `subYoung = 60 * time.Second` live in `filter_meta.go`.

Imports: layout (and grocksdb, the only package that does). Tests: open/close twice on the same dir; eight CF names listed by `grocksdb.ListColumnFamilies`; batch atomicity across CFs; indexed read-your-writes and `ErrRangeInIndexedTxn`; iterator bounds and `TotalOrderSeek` on `geo`; lease cap, lookup, janitor release, `ErrLeaseExpired`; lock ordering (1,000 goroutines locking random 3-key sets under `-race`), two-phase order check panics on misuse; `metaFilter` (M0), `subFilter` and `idFilter` (M3) after `Flush` and `CompactRangeCF`, and the `max_subcompactions=4` race run; merge add; `DeleteRanges` then read; shim field check and getter round trip.

## internal/geohash (M0-M4, pure)

Files: `geohash.go` (encode, decode, interleave tables, `Align52`), `helper.go` (`StepForRadius`, bounding box, neighbours, `ScoreRange`, `Haversine`, `Base32`), `search.go` (`SearchCells`, `Shape`, `Contains`), `LICENSE-REDIS-BSD.txt` (present), `geohash_test.go` with the recorded vectors.

```go
const EarthRadiusM = 6372797.560856
const MercatorMax = 20037726.37
type Hash52 uint64
func Encode(lon, lat float64) (Hash52, bool)     // false on out-of-range
func (h Hash52) Decode() (lon, lat float64)
func (h Hash52) Base32() string
func Haversine(lon1, lat1, lon2, lat2 float64) float64
func StepForRadius(radiusM, lat float64) int
type ScoreRange struct{ Min, Max float64 }        // [Min, Max)
type Shape struct{ Lon, Lat, RadiusM, WidthM, HeightM float64 } // RadiusM 0 means box
func SearchCells(s Shape) []ScoreRange           // centre + 8 neighbours, deduplicated, zeroed outside the box
func (s Shape) Contains(lon, lat float64) (distM float64, ok bool)
func UnitToMetres(unit string) (float64, bool)   // m km ft mi
```

Imports: none. Tests: Sicily vectors, ±85° and antimeridian cases, step table against `redis-server` output.

## internal/command (M1)

Files: `spec.go`, `registry.go`, `ctx.go`, `conn.go`, `command_table.go` (`COMMAND`, `COMMAND INFO`, `COMMAND DOCS` replies), `arity_test.go`, `registry_test.go`.

Exported: the contract in architecture.md, plus `func (c *Ctx) Touch(keys ...[]byte)` (records keys for the WATCH table; called by the executor, exposed for `FLUSHDB`), `func (c *Ctx) Lock(keys []store.LockKey) func()` (two-phase acquisition for `DOC.*` unique stripes), `func KeysFirstLastStep(first, last, step int) KeysFunc` (helper building a `LockRedis` set from positional keys), `func CommandInfoReply(r *Registry, names []string) resp.Reply`, `func CommandDocsReply(r *Registry, names []string) resp.Reply`.

Imports: resp, store, config. Tests: arity error text, case-insensitive lookup, `KeysFirstLastStep` for `MSET`-style steps.

## internal/server (M1; MULTI/WATCH/cursors M3; INFO sections M8)

Files: `server.go` (listen, accept, shutdown), `conn.go` (per-connection loop, `Conn` implementation, flush policy), `exec.go` (dispatch: auth check, `NoMulti`, `Keys`, locks, snapshot for `MultiRead`, `Txn` lifecycle, touch, latency histogram), `connection_cmds.go` (`PING ECHO QUIT SELECT AUTH HELLO CLIENT COMMAND CONFIG TIME SHUTDOWN`), `info.go` (`INFO` sections; `rocksdb` and `analytics` sections registered by `admin` and `analytics` through `RegisterInfoSection`), `multi.go` (M3), `watch.go` (M3), `cursor.go` (M3, the ring), `hist.go` (log-linear histogram), `debug.go` (`NIL.DEBUG CLOCK-ADVANCE`, behind the flag), tests per file.

```go
type Server struct{ /* listener, store, registry, cfg, watch, cursors, hist, sections */ }
func New(cfg *config.Config, st *store.Store, reg *command.Registry) *Server
func (s *Server) Serve(ctx context.Context) error   // blocks until ctx is done
func (s *Server) Addr() net.Addr
func (s *Server) RegisterInfoSection(name string, fn func(w *strings.Builder))
func (s *Server) Cursors() *CursorRing
func (s *Server) Watch() *WatchTable

type CursorKind uint8
const (CursorScan CursorKind = 1 + iota; CursorHScan; CursorSScan; CursorZScan)
type CursorRing struct{ /* [16384]entry, counter, epoch */ }
type Position struct{ DB uint8; Kind CursorKind; Version uint64; Key []byte; Sub []byte }
func (r *CursorRing) Put(p Position) uint64
func (r *CursorRing) Get(cursor uint64, db uint8, kind CursorKind) (Position, bool)

type WatchTable struct{ /* map[watchKey]map[*conn]struct{}, count atomic */ }
func (t *WatchTable) Touch(db uint8, key []byte)
func (t *WatchTable) TouchExisting(db uint8, exists func(key []byte) bool) // FLUSHDB/FLUSHALL
```

`exec.go` is the executor described in architecture.md section 4 and is the only place that calls `store.Lock`, `Store.Begin`, `Store.BeginIndexed` and `Txn.Commit` for Redis and document commands (`docstore`'s builder and sweeper own their own batches).

Imports: resp, command, store, config. Tests: connection loop with a `net.Pipe`; flush policy (pipelined replies leave in one write); `HELLO` replies in both protocols byte-for-byte; `AUTH` flows; MULTI/EXEC/WATCH sequences from `multi.tcl` with the fake clock; cursor ring eviction, epoch mismatch, foreign db; histogram percentiles.

## internal/redis (M1-M4)

One file per type, each with a `func register<Type>(r *command.Registry)` called from `Register`; `common.go` holds the shared helpers.

`common.go`: `func loadMeta(c *command.Ctx, key []byte) (layout.Meta, bool, error)` (lazy expiry applied), `func requireType(m layout.Meta, ok bool, t layout.Type) resp.Reply` (`WRONGTYPE`), `func newVersion(c *command.Ctx) uint64`, `func writeMeta(c *command.Ctx, key []byte, m layout.Meta)` (writes `m` and, for collections, `v`; deletes the old `v` when `oldVersion != 0`), `func deleteKey(c *command.Ctx, key []byte, m layout.Meta)`, `func randomSeek(it store.Iterator, lo, hi []byte)`.

`string.go` (M1): `SET GET GETDEL GETEX GETSET MGET MSET MSETNX SETNX SETEX PSETEX INCR DECR INCRBY DECRBY INCRBYFLOAT APPEND STRLEN GETRANGE SUBSTR SETRANGE`. `generic.go` (M1, extended M3): `DEL UNLINK EXISTS TYPE RENAME RENAMENX TOUCH` at M1; `KEYS DBSIZE RANDOMKEY MOVE FLUSHDB FLUSHALL` at M3 (`FLUSHDB` uses `store.DeleteRanges` and `WatchTable.TouchExisting`). `expire.go` (M1): `EXPIRE PEXPIRE EXPIREAT PEXPIREAT TTL PTTL EXPIRETIME PEXPIRETIME PERSIST`. `hash.go`, `set.go` (M2, lane 1), `list.go`, `zset.go` (M2, lane 2). `scan.go` (M3): `SCAN HSCAN SSCAN ZSCAN` over `server.CursorRing` through an interface `command.Cursors` set on the `Ctx` (so `redis` does not import `server`). `geo.go` (M4): `GEOADD GEOPOS GEODIST GEOHASH GEOSEARCH GEOSEARCHSTORE GEORADIUS GEORADIUSBYMEMBER` over `zset.go`'s helpers and `geohash`. `glob.go`: Redis `stringmatchlen` port for `MATCH` and `KEYS`.

`func Register(r *command.Registry)`.

Imports: command, resp, store, layout, geohash. Tests: semantics tables per type against a real temp store (`testutil` is not needed; tests build a `command.Ctx` directly); TTL transfer rules (`SET` clears, `INCR` keeps, `RENAME` transfers); `ZADD` option errors; `ZRANGE` forms; `LINSERT` renumbering; `SPOP` returns members that were present and removes them; GEO vectors.

## internal/testutil (M1)

Files: `server.go` (spawn in-process with a temp dir, free port, `config.OffsetClock`, all five `Register` calls), `tcp.go` (raw client with `Send(bytes)`/`Expect(bytes)`), `clients.go` (go-redis `Protocol: 2` and `3`), `fixture.go` (seeded generators used by the geo and HTAP fixtures).

```go
type Env struct{ Addr string; Clock *config.OffsetClock; Cfg *config.Config; Store *store.Store; Stop func() }
func Spawn(t testing.TB, mutate func(*config.Config)) *Env
func (e *Env) TCP(t testing.TB) *TCP
func (e *Env) Client(t testing.TB, proto int) *redis.Client
```

Imports: server, store, config, catalog, docstore, redis, cmddoc, analytics, admin (the two later ones are nil-safe stubs until their milestone lands).

## cmd/nildb (M1; `--readonly` M8)

Files: `main.go` (parse config, open store, `catalog.Open`, build registry with the five `Register` calls, `docstore.StartBackground`, WAL flush ticker, lease janitor, signal handling), `readonly.go` (M8: `OpenDbForReadOnlyColumnFamilies` path with `DOC.*` reads and `NIL.*` only).

## internal/doc (M1-M5, pure)

Files: `path.go` (dotted path walking over `bson.Raw`, array index steps), `match.go` (`Matcher` compile and evaluate), `compare.go` (BSON value comparison with type bracketing, Decimal128 through `big.Float`), `update.go` (operators), `project.go`, `sortkey.go`, `ejson.go`, `validate.go` (16 MiB, depth, `_id` types, dotted and `$` field names), `regex.go` (RE2 with MongoDB option letters), tests with the fixture tables in `testdata/matcher/*.json`, `testdata/update/*.json`, `testdata/project/*.json`.

```go
type Matcher struct{ /* compiled tree */ }
func Compile(filter bson.Raw, geo GeoHooks) (*Matcher, error)   // GeoHooks nil until M6
func (m *Matcher) Match(doc bson.Raw) (bool, error)
func (m *Matcher) IndexablePredicates() []Predicate           // for the planner: field, op, values, anchored regex prefix
type Predicate struct{ Field string; Op Op; Values []bson.RawValue; Prefix string }
type Update struct{ /* compiled operators or replacement */ }
func CompileUpdate(u bson.Raw) (*Update, error)
func (u *Update) Apply(doc bson.Raw, upsertFilter bson.Raw, now time.Time) (out bson.Raw, modified bool, err error)
func (u *Update) IsReplacement() bool
type Projection struct{}; func CompileProjection(p bson.Raw) (*Projection, error); func (p *Projection) Apply(doc bson.Raw) (bson.Raw, error)
type SortSpec []struct{ Field string; Desc bool }
func ParseSort(s bson.Raw) (SortSpec, error)
func SortKey(dst []byte, doc bson.Raw, s SortSpec) []byte       // keyenc of the fields, arrays reduced to min/max as MongoDB
func Compare(a, b bson.RawValue) int
func ParseEJSON(s []byte) (bson.Raw, error); func FormatEJSON(doc bson.Raw) []byte // canonical
func ValidateInsert(doc bson.Raw) (withID bson.Raw, id bson.RawValue, err error)   // moves or generates _id
type GeoHooks interface{ Compile(field string, op string, spec bson.Raw) (func(doc bson.Raw) (bool, error), error) }
```

Imports: keyenc. Tests: the fixture tables; `Compare` against the same reference comparator `keyenc` uses; EJSON round trips for every BSON type.

## internal/catalog (M5)

Files: `catalog.go` (open, cache, `Resolve`, `Create`, `Drop`), `index.go` (index records, state machine, `AddIndex`, `SetState`, `DropIndex`), `liveset.go` (immutable sorted set published to store), `lock.go` (per-collection `RWMutex` registry that `store.Lock` calls back into), `catalog_test.go`.

```go
type Collection struct {
	ID      uint32
	NS      string
	Indexes []*Index   // snapshot slice; replaced atomically on DDL
	mu      sync.RWMutex
}
type IndexKind uint8; const (Btree IndexKind = iota; TwoDSphere; Columnar)
type Index struct {
	ID       uint32; Name string; Kind IndexKind
	Keys     []struct{ Field string; Desc bool }  // Btree; one field for 2dsphere/columnar
	Unique, Sparse bool
	ExpireAfter int64 // seconds; -1 none
	S2       geo.Params
	State    IndexState // Building | Ready
}
type Catalog struct{}
func Open(st *store.Store) (*Catalog, error)      // loads records, publishes LiveSet
func (c *Catalog) Resolve(ns string) (*Collection, bool)
func (c *Catalog) Create(ns string) (*Collection, error)
func (c *Catalog) List(prefix string) []string
func (c *Catalog) AddIndex(coll *Collection, ix Index) (*Index, error)   // assigns ID, publishes LiveSet, writes record with State=Building (or Ready for unique/sync)
func (c *Catalog) SetIndexState(coll *Collection, ix *Index, s IndexState) error
func (c *Catalog) DropIndex(coll *Collection, name string) (*Index, error) // caller runs store.DeleteRanges, then c.Unpublish(id)
func (c *Catalog) DropCollection(coll *Collection) ([]uint32, error)      // returns ids to range-delete
func (c *Catalog) Unpublish(ids ...uint32)
func (c *Catalog) Count(coll *Collection) (int64, error)                  // n/<coll_id>
func (c *Catalog) NextVersionSeed() uint64; func (c *Catalog) SaveVersion(v uint64) error
```

Imports: store, layout, keyenc, geo (for `Params` only; `geo` is pure and lands its `Params` type at M1). Tests: create/drop/resolve, LiveSet publish-before-write ordering (asserted by a hook), index state transitions, reopen reads the same catalog.

## internal/docstore (M5; geo M6; columnar M7)

Files: `write.go` (`Insert`, `Replace`, `Update`, `Delete`, `Upsert`), `entries.go` (index entry generation: btree, unique, sparse, multikey), `unique.go` (E11000 check under uniq locks), `drop.go` (collection and index drops through `store.DeleteRanges` + `ScheduleCompact`), `build.go` (background builder), `sweeper.go` (TTL), `geo_maint.go` (M6: `geo.Entries` per index), `col_maint.go` (M7: column values), `errors.go` (`DupKeyError{Index, Key bson.Raw}` with the exact `E11000` text), tests.

```go
type Store struct{ st *store.Store; cat *catalog.Catalog; clock func() time.Time }
func New(st *store.Store, cat *catalog.Catalog) *Store
type WriteResult struct{ N, NModified int; UpsertedID bson.RawValue; Errors []WriteError }
func (d *Store) LockKeys(coll *catalog.Collection, id bson.RawValue, doc bson.Raw) []store.LockKey // doc + uniq keys
func (d *Store) Insert(txn store.Txn, coll *catalog.Collection, doc bson.Raw) error                 // caller holds locks; returns *DupKeyError
func (d *Store) Replace(txn store.Txn, coll *catalog.Collection, old, doc bson.Raw) error
func (d *Store) Delete(txn store.Txn, coll *catalog.Collection, old bson.Raw) error
func (d *Store) Get(r store.Reader, coll *catalog.Collection, id bson.RawValue) (bson.Raw, bool, error)
func (d *Store) DropCollection(coll *catalog.Collection) error
func (d *Store) DropIndex(coll *catalog.Collection, name string) error
func (d *Store) StartBackground(ctx context.Context)   // builder queue and sweeper
func (d *Store) Build(coll *catalog.Collection, ix *catalog.Index) error    // background unless unique
```

Imports: store, layout, keyenc, catalog, doc, geo. Tests: entries for every index kind; update deletes old entries and inserts new; unique across `_id`s with two goroutines under `-race` (exactly one `E11000`); sparse and multikey; parallel-arrays error; drop removes ranges and unpublishes after commit; builder with concurrent writers asserts the index equals a fresh rebuild; sweeper deletes only expired Dates.

## internal/query (M5 core; M6 geo; M7 analytics)

Files: `plan.go` (`Plan`, `Explain`), `planner.go` (index selection, bounds, sort match), `op.go` (`Operator`, `Row`), `scan_doc.go`, `scan_idx.go` (with `MultiGet` batches of 64), `op_match.go`, `op_project.go`, `op_sort.go` (top-k and full), `op_limit.go`, `cursor.go` (cursor table pinned to leases), `mem.go` (accountant), `readopts.go` (OLTP versus analytical `store.IterOpts`), M6: `geo_within.go`, `geo_near.go`, `op_geonear.go`; M7: `scan_col.go` (k-way merge), `op_group.go`, `op_unwind.go`, `expr.go`, tests with `testdata/pipelines/*.json`.

```go
type Mode uint8; const (OLTP Mode = iota; Analytic)
type Options struct{ Mode Mode; Snap *store.Snapshot; MaxMem int64; MaxRows int64; Deadline time.Duration; Bucket *Bucket; Hint string }
type Row struct{ Doc bson.Raw; Fields map[string]bson.RawValue /* lazily decoded */; Dist s1.Angle }
type Operator interface{ Open(r store.Reader) error; Next() (Row, bool, error); Close() }
type Plan struct{ Source string /* index|columnar|rowscan */; Index string; Bounds [][2][]byte; Columns []string; Stages []string; EstimatedRows int64 }
func PlanFind(cat *catalog.Catalog, coll *catalog.Collection, filter *doc.Matcher, sort doc.SortSpec, o Options) (Plan, Operator, error)
func PlanPipeline(cat *catalog.Catalog, coll *catalog.Collection, pipeline bson.Raw, o Options) (Plan, Operator, error)
func (p Plan) Explain() resp.Reply
type Cursor struct{ ID uint64; NS string; op Operator; lease *store.Snapshot }
type Cursors struct{}; func (c *Cursors) Open(op Operator, ns string, lease *store.Snapshot, batch int) (*Cursor, []Row, error); func (c *Cursors) Read(id uint64, n int) ([]Row, bool, error); func (c *Cursors) Del(id uint64) bool; func (c *Cursors) List() []*Cursor
type Bucket struct{}; func NewBucket(bytesPerSec int64) *Bucket; func (b *Bucket) Take(n int) // sleeps
type Accountant struct{}; func (a *Accountant) Add(n int64) error // ErrMemLimit with the exact text
```

Imports: store, layout, keyenc, catalog, doc, geo. Tests: planner goldens (which index, bounds, sort dropped); operators against an in-memory `store.Reader` fake for the pure ones and a temp store for scans; top-k determinism with `_id` tie-break; group memory limit text; expression evaluator table; k-way merge with missing columns; cursor exhaustion and lease release.

## internal/cmddoc (M5; DOC.AGGREGATE M7)

Files: `insert.go`, `find.go` (`FIND`, `FINDONE`, `GET`, `COUNT`, `DISTINCT`, `CURSOR`), `update.go` (`UPDATE`, `REPLACE`, `DELETE`), `ddl.go` (`CREATE`, `DROP`, `CREATEINDEX`, `DROPINDEX`, `INDEXES`, `COLLECTIONS`, `STATS`), `aggregate.go` (M7), `reply.go` (cursor shapes for RESP2 and RESP3, `writeErrors`), `keys.go` (`KeysFunc`s that parse once and return `Parsed`), tests.

`func Register(r *command.Registry, cat *catalog.Catalog, ds *docstore.Store, q *query.Cursors)`.

Imports: command, resp, store, catalog, docstore, query, doc. Tests: reply shapes byte-for-byte in both protocols; `ORDERED` semantics; upsert `_id` derivation; `nModified < n` on no-op updates; `EXPLAIN`.

## internal/geo (M1-M6, pure)

Files: `geojson.go` (`Parse`, validation, strictwinding), `geometry.go` (`Region`, `ShapeIndex`, `LatLng`), `params.go` (`DefaultParams`, `Entries`), `plan.go` (`PlanRegion`, `PlanAnnulus`, range merge, probe dedupe, level clamping), `refine.go` (`Refine`, `Distance`), `near.go` (`Near` driver), `fixture_test.go` (brute-force oracle over in-memory Scan/Probe/Load), tests per file.

Exported: the contract in architecture.md section 8. Imports: none internal. Tests: covering determinism; `DefaultParams` resolves to levels 2 and 16; predicates for every kind pair; the fixture through `Near` with limit, min and max distance; polygon parsing errors with MongoDB's texts.

## internal/analytics (M7)

Files: `semaphore.go`, `lease_cmds.go` (`NIL.SNAPSHOT`), `aggregate.go` (`NIL.AGGREGATE`, `NIL.COUNT`, `NIL.DISTINCT`, `NIL.EXPLAIN`), `sugar.go` (`NIL.STATS`, `NIL.GROUP`), `geogroup.go`, `keystats.go`, `info.go` (`INFO analytics` section and counters), tests.

`func Register(r *command.Registry, srv InfoRegistrar, cat *catalog.Catalog, q *query.Cursors)` where `InfoRegistrar` is `interface{ RegisterInfoSection(string, func(*strings.Builder)) }`.

Imports: command, resp, store, catalog, query, geo. Tests: semaphore timeout text; lease reuse with `AT`; row cap; `NIL.GEOGROUP` counts against the fixture; `NIL.GROUP` equals the equivalent pipeline.

## internal/admin (M8)

Files: `rocks.go` (all `ROCKS.*`), `info.go` (`INFO rocksdb` and `persistence`), tests.

`func Register(r *command.Registry, srv InfoRegistrar, st *store.Store, cat *catalog.Catalog)`.

Imports: command, resp, store, catalog. Tests: each command against a temp store; `ROCKS.SCAN` hex round trip with `TotalOrderSeek`; `ROCKS.CHECKPOINT` then `OpenDbForReadOnlyColumnFamilies` in-process reads the same keys.

## cmd/htapbench (M7-M9)

Files: `main.go` (phases A-D, marker freshness check, markdown table), `load.go`, `mix.go`. Imports: none internal (RESP over TCP with a minimal client, so it can also run against Redis for a sanity baseline).

## tests (M9)

`tests/protocol/*_test.go` (raw-socket), `tests/gocase/{string,hash,list,set,zset,geo,multi,scan,expire,doc,nil,rocks}_test.go`, `tests/compat/run.sh` (`make compat`), `tests/tcl/run.sh` (`make tcl`, writes `docs/compat/redis-tcl.md`). All use `internal/testutil`.

## Per-milestone exit checklists

**M0.** `go build ./...` with `cmd/nildb` still absent is fine; `go test ./internal/...` green; `make rocksmoke` green; the keyenc property test runs 100,000 pairs in under 5 s; `store` opens, lists eight CFs, and the shim round trip passes (or the `nildb_no_shim` build is documented as the fallback).

**M1.** `./bin/nildb --dir ./data --addr 127.0.0.1:6380` answers `redis-cli -p 6380 PING`, `SET`, `GET`, `INCR`, `EXPIRE`, `TTL`, `HELLO 3`; go-redis string suite green in both protocols; `redis-benchmark -p 6380 -t ping,set,get,incr -P 16 -q` completes with zero errors and its numbers are recorded in `bench/history.csv`; TCL `unit/type/string`, `unit/type/incr`, `unit/expire`, `unit/keyspace` results recorded.

**M2.** TCL `unit/type/{hash,list,set,zset}` recorded with the known-failure list (randomness distribution tests); go-redis suites green.

**M3.** TCL `unit/multi` and `unit/scan` recorded; "SCAN guarantees check under write load" port green; filter tests green under `-race` with `max_subcompactions=4`; `redis-cli --scan` and `--bigkeys` report all 1,000 keys.

**M4.** TCL `unit/geo` recorded; the Redis geo transcript compares byte-for-byte; the brute-force `GEOSEARCH` oracle agrees on 1,000 queries.

**M5.** go-redis `DOC.*` suite green; `E11000` race test green; builder race test green; TTL sweeper test green; `DOC.FIND` uses an index when `EXPLAIN` says so.

**M6.** S2 fixture green, then green again after deleting 30% of documents and after a rebuild; `$near` distances match the oracle to 1e-6 relative.

**M7.** `NIL.EXPLAIN` chooses the expected plan on each fixture pipeline; `NIL.AGGREGATE` results equal the in-test oracle; `cmd/htapbench` writes `bench/results/<date>.md` with phases A-D; freshness gap 0.

**M8.** Every `ROCKS.*` command tested; `ROCKS.CHECKPOINT dir` then `nildb --readonly dir` serves `DOC.FIND` and `NIL.AGGREGATE`; `INFO` sections complete.

**M9.** `make compat` and `make tcl` green with tables committed; `docs/compat/commands.md` lists every supported command with arity and every unsupported one with a reason; README states build steps, durability contract, the HTAP numbers and the known divergences; v1.0 tag.
