// Package store is NilDB's storage engine and the only package that imports
// grocksdb. It opens one RocksDB instance with eight column families and
// the options of architecture.md section 2, and provides transactions over
// WriteBatch and WriteBatchWI, live and snapshot readers, bounded
// iterators, counted snapshot leases, the lock manager, the three
// compaction filters, the nildb.i64add merge operator, range drops and the
// admin calls behind ROCKS.*.
//
// Lifetime rule: Close must run after every Reader call, Iterator and Txn
// of the store has finished. Calls made after Close return ErrClosed, but a
// call racing with Close is not protected.
package store

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/linxGnu/grocksdb"
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
// empty Fsync means FsyncEverySec and a nil Clock means time.Now.
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

func (c Config) withDefaults() (Config, error) {
	if c.Dir == "" {
		return c, errors.New("store: Config.Dir is empty")
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

	db   *grocksdb.DB
	cfs  [NumCFs]*grocksdb.ColumnFamilyHandle
	opts *dbOptions
	ro   *grocksdb.ReadOptions         // shared by live point reads
	wo   *grocksdb.WriteOptions        // commit options; sync under FsyncAlways
	cro  *grocksdb.CompactRangeOptions // manual compactions, non-exclusive

	metaF *metaFilter
	subF  *subFilter
	idF   *idFilter

	vgen  *layout.VersionGen
	vseed uint64 // version seed read at Open

	locks   lockManager
	snaps   snapRegistry
	compact compactor

	closeMu sync.Mutex
	closed  atomic.Bool
	stop    chan struct{}
	janitor sync.WaitGroup
}

// Open opens (creating if needed) the database in cfg.Dir with all eight
// column families. A new database gets the layout marker; an existing one
// must carry it, and Open refuses a non-empty database without it.
func Open(cfg Config) (*Store, error) { return open(cfg, false) }

// OpenReadOnly opens an existing database, typically a checkpoint, without
// write access. Writes fail with RocksDB's "not supported in read only
// mode" error, no compaction runs, and Close persists nothing.
func OpenReadOnly(cfg Config) (*Store, error) { return open(cfg, true) }

func open(cfg Config, readOnly bool) (*Store, error) {
	cfg, err := cfg.withDefaults()
	if err != nil {
		return nil, err
	}
	s := &Store{cfg: cfg, readOnly: readOnly, stop: make(chan struct{})}
	clock := cfg.Clock
	nowMS := func() int64 { return clock().UnixMilli() }
	s.metaF = &metaFilter{now: nowMS}
	s.subF = &subFilter{now: nowMS}
	s.idF = &idFilter{}

	o := newDBOptions(cfg, s.metaF, s.subF, s.idF)
	var handles []*grocksdb.ColumnFamilyHandle
	if readOnly {
		s.db, handles, err = grocksdb.OpenDbForReadOnlyColumnFamilies(o.db, cfg.Dir, cfNames[:], o.cf, false)
	} else {
		s.db, handles, err = grocksdb.OpenDbColumnFamilies(o.db, cfg.Dir, cfNames[:], o.cf)
	}
	if err != nil {
		o.release()
		return nil, fmt.Errorf("store: open %s: %w", cfg.Dir, err)
	}
	copy(s.cfs[:], handles)
	s.opts = o
	s.ro = grocksdb.NewDefaultReadOptions()
	s.wo = grocksdb.NewDefaultWriteOptions()
	s.wo.SetSync(cfg.Fsync == FsyncAlways)
	s.cro = grocksdb.NewCompactRangeOptions()
	s.cro.SetExclusiveManualCompaction(false)

	if err := s.checkLayout(); err != nil {
		s.shutdownDB()
		return nil, err
	}
	seed, err := s.loadVersionSeed()
	if err != nil {
		s.shutdownDB()
		return nil, err
	}
	s.vseed = seed
	s.vgen = layout.NewVersionGen(seed)
	s.snaps.init()
	s.compact.init(s.runCompact)

	if !readOnly {
		s.metaF.bound.Store(true)
		s.subF.bind(s.db, s.cfs[CFMeta])
		s.idF.bound.Store(true)
	}

	s.janitor.Add(1)
	go s.runJanitor(janitorInterval)
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
	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()
	wo.SetSync(true)
	if err := s.db.PutCF(wo, s.cfs[CFDefault], layout.LayoutMarkerKey, []byte(layout.LayoutMarker)); err != nil {
		return fmt.Errorf("store: write layout marker: %w", err)
	}
	return nil
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
	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()
	wo.SetSync(true)
	val := binary.BigEndian.AppendUint64(nil, last)
	if err := s.db.PutCF(wo, s.cfs[CFDefault], layout.SeqKey(layout.SeqVersion), val); err != nil {
		return fmt.Errorf("store: save version seed: %w", err)
	}
	return nil
}

// Close stops the janitor, cancels pending scheduled compactions, saves
// the version generator, releases every snapshot and closes the database.
// A second Close returns nil.
func (s *Store) Close() error {
	s.closeMu.Lock()
	defer s.closeMu.Unlock()
	if s.closed.Load() {
		return nil
	}
	s.closed.Store(true)
	close(s.stop)
	s.janitor.Wait()
	if !s.readOnly {
		s.db.DisableManualCompaction()
	}
	s.compact.close()
	var err error
	if !s.readOnly {
		err = s.saveVersion()
	}
	s.releaseAll()
	s.shutdownDB()
	return err
}

// shutdownDB unbinds the filters and closes RocksDB. It frees the option
// objects NilDB owns; see dbOptions for the ones it leaks on purpose.
func (s *Store) shutdownDB() {
	s.metaF.bound.Store(false)
	s.idF.bound.Store(false)
	s.subF.unbind()
	if !s.readOnly {
		s.db.CancelAllBackgroundWork(true)
	}
	for i, h := range s.cfs {
		if h != nil {
			h.Destroy()
			s.cfs[i] = nil
		}
	}
	s.db.Close()
	s.subF.destroy()
	if s.ro != nil {
		s.ro.Destroy()
		s.ro = nil
	}
	if s.wo != nil {
		s.wo.Destroy()
		s.wo = nil
	}
	if s.cro != nil {
		s.cro.Destroy()
		s.cro = nil
	}
	s.opts.release()
}

// Clock returns the store's clock (Config.Clock or time.Now).
func (s *Store) Clock() func() time.Time { return s.cfg.Clock }

// VersionGen returns the collection version generator. Open seeds it above
// every version the database holds (the saved s/version value and the
// version map); Close saves its last version to s/version.
func (s *Store) VersionGen() *layout.VersionGen { return s.vgen }

// WriteOpts reports whether commits sync the WAL (Fsync "always").
func (s *Store) WriteOpts() (sync bool) { return s.cfg.Fsync == FsyncAlways }

// Config returns the configuration Open used, defaults applied.
func (s *Store) Config() Config { return s.cfg }

// ReadOnly reports whether the store was opened with OpenReadOnly.
func (s *Store) ReadOnly() bool { return s.readOnly }

// Handle returns the *grocksdb.ColumnFamilyHandle of cf. It exists for
// tests inside this package; other packages must not use it.
func (s *Store) Handle(cf CF) any { return s.handle(cf) }

func (s *Store) handle(cf CF) *grocksdb.ColumnFamilyHandle {
	if int(cf) >= NumCFs {
		panic(fmt.Sprintf("store: unknown column family %d", cf))
	}
	return s.cfs[cf]
}
