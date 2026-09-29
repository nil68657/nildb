// Package store is NilDB's storage layer and the only package that imports
// grocksdb or internal/nilengine. It opens one engine with eight column
// families: RocksDB with the options of architecture.md section 2, or,
// built with -tags nilengine, one of the Rust engines (lsm, btree,
// pgheap). On top of the engine it provides transactions over plain and
// indexed write batches, live and snapshot readers, bounded iterators,
// counted snapshot leases, the lock manager, the three compaction filters
// on RocksDB and the sweeper that replaces them elsewhere, the
// nildb.i64add merge operator, range drops and the admin calls behind
// ROCKS.*.
//
// Lifetime rule: Close must run after every Reader call, Iterator and Txn
// of the store has finished. Calls made after Close return ErrClosed, but a
// call racing with Close is not protected.
package store

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nil68657/nildb/internal/layout"
)

// CF names a column family.
type CF uint8

// The eight column families, in the order Open opens them.
const (
	CFDefault CF = iota // catalog, layout marker
	CFMeta              // Redis metadata, string payloads, version map
	CFSub               // hash fields, set members, list elements, zset member->score
	CFZScore            // zset score index, Redis GEO
	CFDoc               // BSON rows
	CFIdx               // secondary indexes (btree, unique, TTL)
	CFGeo               // 2dsphere cells
	CFCol               // columnar projections; opened with its options, unused in v1
)

// NumCFs is the number of column families.
const NumCFs = 8

var cfNames = [NumCFs]string{"default", "meta", "sub", "zscore", "doc", "idx", "geo", "col"}

// String returns the RocksDB name of the column family.
func (cf CF) String() string {
	if int(cf) < NumCFs {
		return cfNames[cf]
	}
	return fmt.Sprintf("CF(%d)", uint8(cf))
}

// ParseCF returns the column family with the given RocksDB name.
func ParseCF(name string) (CF, bool) {
	for i, n := range cfNames {
		if n == name {
			return CF(i), true
		}
	}
	return 0, false
}

func checkCF(cf CF) {
	if int(cf) >= NumCFs {
		panic(fmt.Sprintf("store: unknown column family %d", cf))
	}
}

// Type is the Redis type of a metadata entry.
type Type = layout.Type

// Fsync policies for Config.Fsync.
const (
	FsyncAlways   = "always"   // every commit syncs the WAL
	FsyncEverySec = "everysec" // the caller runs FlushWAL(true) once a second
	FsyncNo       = "no"       // the WAL is written to the OS and never synced by NilDB
)

// Defaults applied by Open to zero Config fields.
const (
	DefaultBlockCacheBytes     = 512 << 20
	DefaultAnalyticsCacheBytes = 64 << 20
	DefaultWriteBufferBytes    = 256 << 20
	DefaultBgIOBytesPerSec     = 200 << 20
	DefaultMaxSnapshots        = 32
	DefaultLeaseTTL            = 60 * time.Second
	DefaultLeaseMax            = 600 * time.Second
)

// Config configures Open. Zero numeric fields take the defaults above, an
// empty Engine means EngineRocksDB, an empty Fsync means FsyncEverySec
// and a nil Clock means time.Now.
//
// The Rust engines have one cache and one memtable budget: BlockCacheBytes
// sizes the block cache, page cache or buffer pool, WriteBufferBytes the
// LSM's memtable. AnalyticsCacheBytes, BgIOBytesPerSec and Statistics
// apply to RocksDB only.
type Config struct {
	Dir                 string
	Engine              string        // "rocksdb" | "lsm" | "btree" | "pgheap"
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

func (c Config) withDefaults() (Config, error) {
	if c.Dir == "" {
		return c, errors.New("store: Config.Dir is empty")
	}
	if c.Engine == "" {
		c.Engine = EngineRocksDB
	}
	if err := checkEngine(c.Engine); err != nil {
		return c, err
	}
	def := func(v *int64, d int64, name string) error {
		switch {
		case *v == 0:
			*v = d
		case *v < 0:
			return fmt.Errorf("store: Config.%s is negative", name)
		}
		return nil
	}
	for _, f := range []struct {
		v    *int64
		d    int64
		name string
	}{
		{&c.BlockCacheBytes, DefaultBlockCacheBytes, "BlockCacheBytes"},
		{&c.AnalyticsCacheBytes, DefaultAnalyticsCacheBytes, "AnalyticsCacheBytes"},
		{&c.WriteBufferBytes, DefaultWriteBufferBytes, "WriteBufferBytes"},
		{&c.BgIOBytesPerSec, DefaultBgIOBytesPerSec, "BgIOBytesPerSec"},
	} {
		if err := def(f.v, f.d, f.name); err != nil {
			return c, err
		}
	}
	switch c.Fsync {
	case "":
		c.Fsync = FsyncEverySec
	case FsyncAlways, FsyncEverySec, FsyncNo:
	default:
		return c, fmt.Errorf("store: Config.Fsync %q: want always, everysec or no", c.Fsync)
	}
	switch {
	case c.MaxSnapshots == 0:
		c.MaxSnapshots = DefaultMaxSnapshots
	case c.MaxSnapshots < 0:
		return c, errors.New("store: Config.MaxSnapshots is negative")
	}
	if c.LeaseTTL <= 0 {
		c.LeaseTTL = DefaultLeaseTTL
	}
	if c.LeaseMax <= 0 {
		c.LeaseMax = DefaultLeaseMax
	}
	if c.LeaseTTL > c.LeaseMax {
		c.LeaseTTL = c.LeaseMax
	}
	if c.Clock == nil {
		c.Clock = time.Now
	}
	return c, nil
}

// Errors returned by the store.
var (
	ErrRangeInIndexedTxn = errors.New("store: DeleteRange is not supported inside an indexed transaction")
	ErrTooManySnapshots  = errors.New("store: too many open snapshots")
	ErrLeaseExpired      = errors.New("store: snapshot lease expired")
	ErrClosed            = errors.New("store: closed")
)

// janitorInterval is how often the janitor releases expired leases. Tests
// in this package shorten it before Open.
var janitorInterval = time.Second

// Store is an open NilDB database.
type Store struct {
	cfg      Config
	readOnly bool
	kv       kv

	live atomic.Pointer[liveSetBox] // the LiveSet idFilter and the sweeper read

	vgen  *layout.VersionGen
	vseed uint64 // version seed read at Open

	locks   lockManager
	snaps   snapRegistry
	compact compactor
	sweep   sweeper

	closeMu sync.Mutex
	closed  atomic.Bool
	stop    chan struct{}
	janitor sync.WaitGroup
}

// Open opens (creating if needed) the database in cfg.Dir with all eight
// column families. A new database gets the layout marker; an existing one
// must carry it, and Open refuses a non-empty database without it. The
// directory must belong to cfg.Engine (see engineFile).
func Open(cfg Config) (*Store, error) { return open(cfg, false) }

// OpenReadOnly opens an existing database, typically a checkpoint, without
// write access. Writes fail (RocksDB's "not supported in read only mode"
// error, or "store: opened read-only"), no RocksDB compaction runs, and
// Close persists nothing. The Rust engines open the directory with their
// normal open, which may replay a log or write an end-of-recovery
// checkpoint into it; the store refuses writes on top.
func OpenReadOnly(cfg Config) (*Store, error) { return open(cfg, true) }

func open(cfg Config, readOnly bool) (*Store, error) {
	cfg, err := cfg.withDefaults()
	if err != nil {
		return nil, err
	}
	if err := engineAvailable(cfg.Engine); err != nil {
		return nil, err
	}
	if err := checkEngineMarker(cfg.Dir, cfg.Engine, readOnly); err != nil {
		return nil, err
	}
	s := &Store{cfg: cfg, readOnly: readOnly, stop: make(chan struct{})}
	s.kv, err = openKV(cfg, readOnly, s)
	if err != nil {
		return nil, err
	}
	if err := s.checkLayout(); err != nil {
		_ = s.kv.close()
		return nil, err
	}
	seed, err := s.loadVersionSeed()
	if err != nil {
		_ = s.kv.close()
		return nil, err
	}
	s.vseed = seed
	s.vgen = layout.NewVersionGen(seed)
	s.snaps.init()
	s.compact.init(s.runCompact)
	s.kv.bind(s)

	s.janitor.Add(1)
	go s.runJanitor(janitorInterval)
	if s.kv.sweeps() && !readOnly {
		s.janitor.Add(1)
		go s.runSweeper()
	}
	return s, nil
}

// checkLayout verifies the layout marker, writing it into an empty
// database.
func (s *Store) checkLayout() error {
	v, ok, err := s.Get(CFDefault, layout.LayoutMarkerKey)
	if err != nil {
		return fmt.Errorf("store: read layout marker: %w", err)
	}
	if ok {
		if string(v) != layout.LayoutMarker {
			return fmt.Errorf("store: %s has layout marker %q, want %q", s.cfg.Dir, v, layout.LayoutMarker)
		}
		return nil
	}
	if s.readOnly {
		return fmt.Errorf("store: %s has no layout marker %q", s.cfg.Dir, layout.LayoutMarker)
	}
	for cf := range CF(NumCFs) {
		it := s.Iter(cf, nil, nil, IterOpts{TotalOrderSeek: true})
		it.SeekToFirst()
		nonEmpty, err := it.Valid(), it.Err()
		it.Close()
		if err != nil {
			return fmt.Errorf("store: scan %s: %w", cf, err)
		}
		if nonEmpty {
			return fmt.Errorf("store: %s holds data in %s but no layout marker %q; refusing to open", s.cfg.Dir, cf, layout.LayoutMarker)
		}
	}
	if err := s.putSynced(CFDefault, layout.LayoutMarkerKey, []byte(layout.LayoutMarker)); err != nil {
		return fmt.Errorf("store: write layout marker: %w", err)
	}
	return nil
}

// putSynced writes one key and syncs, whatever Config.Fsync says: the
// store's own records (the layout marker, the version seed) must survive.
func (s *Store) putSynced(cf CF, key, val []byte) error {
	b := s.kv.newBatch()
	defer b.destroy()
	b.put(cf, key, val)
	return s.kv.write(b, true)
}

// loadVersionSeed returns the highest collection version known to the
// database: the larger of the saved s/version value and the last key of
// the version map. The generator starts above it.
func (s *Store) loadVersionSeed() (uint64, error) {
	var seed uint64
	v, ok, err := s.Get(CFDefault, layout.SeqKey(layout.SeqVersion))
	if err != nil {
		return 0, fmt.Errorf("store: read version seed: %w", err)
	}
	if ok {
		if len(v) != 8 {
			return 0, fmt.Errorf("store: %w: s/version of %d bytes", layout.ErrCorrupt, len(v))
		}
		seed = binary.BigEndian.Uint64(v)
	}
	it := s.Iter(CFMeta, []byte{layout.VersionMapPrefix}, []byte{layout.VersionMapPrefix + 1}, IterOpts{})
	defer it.Close()
	it.SeekToLast()
	if it.Valid() {
		if ver, ok := layout.SplitVersionKey(it.Key()); ok && ver > seed {
			seed = ver
		}
	}
	if err := it.Err(); err != nil {
		return 0, fmt.Errorf("store: scan version map: %w", err)
	}
	return seed, nil
}

// saveVersion writes the generator's last version to s/version.
func (s *Store) saveVersion() error {
	last := s.vgen.Last()
	if last == s.vseed {
		return nil
	}
	val := binary.BigEndian.AppendUint64(nil, last)
	if err := s.putSynced(CFDefault, layout.SeqKey(layout.SeqVersion), val); err != nil {
		return fmt.Errorf("store: save version seed: %w", err)
	}
	return nil
}

// Close stops the janitor and the sweeper, cancels pending scheduled
// compactions, saves the version generator, releases every snapshot and
// closes the database. A second Close returns nil.
func (s *Store) Close() error {
	s.closeMu.Lock()
	defer s.closeMu.Unlock()
	if s.closed.Load() {
		return nil
	}
	s.closed.Store(true)
	close(s.stop)
	s.janitor.Wait()
	s.kv.stopBackground()
	s.compact.close()
	var err error
	if !s.readOnly {
		err = s.saveVersion()
	}
	s.releaseAll()
	if cerr := s.kv.close(); err == nil {
		err = cerr
	}
	return err
}

// Clock returns the store's clock (Config.Clock or time.Now).
func (s *Store) Clock() func() time.Time { return s.cfg.Clock }

// VersionGen returns the collection version generator. Open seeds it above
// every version the database holds (the saved s/version value and the
// version map); Close saves its last version to s/version.
func (s *Store) VersionGen() *layout.VersionGen { return s.vgen }

// WriteOpts reports whether commits sync the WAL (Fsync "always").
func (s *Store) WriteOpts() (sync bool) { return s.syncCommits() }

// Config returns the configuration Open used, defaults applied.
func (s *Store) Config() Config { return s.cfg }

// ReadOnly reports whether the store was opened with OpenReadOnly.
func (s *Store) ReadOnly() bool { return s.readOnly }

// Engine returns the name of the engine the store runs on: "rocksdb",
// "lsm", "btree" or "pgheap".
func (s *Store) Engine() string { return s.cfg.Engine }

// EngineVersion returns the version of the engine library: RocksDB's
// "11.8.1" from <rocksdb/version.h>, or nil_version() of the Rust engines.
func (s *Store) EngineVersion() string { return s.kv.version() }

// ShimActive reports whether IterOpts.LowPriority reaches a rate limiter,
// and if not, why: the package-level ShimActive under RocksDB, and false
// with "engine lsm has no rate limiter" (or btree, pgheap) otherwise.
func (s *Store) ShimActive() (bool, error) { return s.kv.shimActive() }

// Handle returns the engine's handle of cf: a *grocksdb.ColumnFamilyHandle
// on RocksDB. It exists for tests inside this package; other packages must
// not use it.
func (s *Store) Handle(cf CF) any {
	checkCF(cf)
	return s.kv.handle(cf)
}

// TestEngine returns the engine the test suites of every package run on:
// $NILDB_TEST_ENGINE, or rocksdb when it is unset. `make test-engines`
// sets it once per engine.
func TestEngine() string {
	if e := os.Getenv("NILDB_TEST_ENGINE"); e != "" {
		return e
	}
	return EngineRocksDB
}
