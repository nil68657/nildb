package store

import (
	"errors"
	"fmt"
	"slices"
)

// Engine names for Config.Engine and the ENGINE marker file.
const (
	EngineRocksDB = "rocksdb"
	EngineLSM     = "lsm"
	EngineBTree   = "btree"
	EnginePgHeap  = "pgheap"
)

// Engines lists the engine names Config.Engine accepts, RocksDB first.
var Engines = []string{EngineRocksDB, EngineLSM, EngineBTree, EnginePgHeap}

// ErrUnsupported is returned by calls the configured engine does not
// provide: Ingest, SetOption and ExportCF on the Rust engines. The error
// text reads "not supported with --engine lsm", which ROCKS.* sends after
// "ERR ".
var ErrUnsupported = errors.New("not supported")

// kv is the storage engine under a Store: rocksKV over grocksdb, or nilKV
// over internal/nilengine (build tag nilengine). What is NilDB logic rather
// than engine logic stays in Store and serves every engine: the layout
// marker, the version generator, the snapshot registry and its leases, the
// lock manager, scheduled compactions, drops, the overlay transaction and
// the sweeper.
type kv interface {
	name() string    // one of Engines
	version() string // the engine library's version, "11.8.1" for RocksDB

	// Reads. snap is nil for the latest committed state, otherwise a value
	// the same kv's snapshot returned and has not released.
	get(snap kvSnap, cf CF, key []byte) ([]byte, bool, error)
	getPinned(snap kvSnap, cf CF, key []byte) (pinned, error)
	multiGet(snap kvSnap, cf CF, keys [][]byte) ([][]byte, error)
	iter(snap kvSnap, cf CF, lo, hi []byte, o IterOpts) Iterator

	// Writes. A batch belongs to one goroutine; write applies it
	// atomically and, with sync, durably.
	newBatch() kvBatch
	write(b kvBatch, sync bool) error
	// beginIndexed returns the read-your-writes transaction of EXEC.
	beginIndexed(s *Store) Txn

	snapshot() (snap kvSnap, seq uint64, err error)
	release(snap kvSnap)

	property(cf CF, name string) (string, bool)
	intProperty(cf CF, name string) (uint64, bool)
	approxSizes(cf CF, ranges [][2][]byte) ([]uint64, error)
	compact(cf CF, lo, hi []byte) error
	flush(cf CF) error
	flushWAL(sync bool) error
	checkpoint(dir string) error
	latestSeq() uint64
	stats() (string, bool)
	handle(cf CF) any
	shimActive() (bool, error)

	// sweeps reports whether the store's sweeper must stand in for
	// compaction filters on this engine.
	sweeps() bool
	// bind runs once the layout marker and version seed are read: RocksDB
	// arms its compaction filters.
	bind(s *Store)
	// stopBackground runs first in Close, before the snapshots go.
	stopBackground()
	close() error
}

// kvSnap is an engine snapshot: *rocksSnap or *nilengine.Snapshot.
type kvSnap any

// kvBatch collects writes for kv.write. Put, delete and merge copy their
// arguments.
type kvBatch interface {
	put(cf CF, key, val []byte)
	delete(cf CF, key []byte)
	merge(cf CF, key, operand []byte)
	deleteRange(cf CF, lo, hi []byte)
	destroy()
}

// pinned is a value read by kv.getPinned: *grocksdb.PinnableSlice, or a Go
// copy for the Rust engines. Data is valid until Destroy.
type pinned interface {
	Exists() bool
	Data() []byte
	Destroy()
}

// goPinned is a pinned value held in Go memory.
type goPinned struct {
	v  []byte
	ok bool
}

func (p *goPinned) Exists() bool { return p.ok }
func (p *goPinned) Data() []byte { return p.v }
func (p *goPinned) Destroy()     {}

// openKV opens the engine named by cfg.Engine; openNil is in kv_nil.go or,
// without the nilengine tag, kv_nil_stub.go.
func openKV(cfg Config, readOnly bool, s *Store) (kv, error) {
	if cfg.Engine != EngineRocksDB {
		return openNil(cfg, readOnly)
	}
	k, err := openRocks(cfg, readOnly, s)
	if err != nil {
		return nil, err
	}
	return k, nil
}

func checkEngine(name string) error {
	if slices.Contains(Engines, name) {
		return nil
	}
	return fmt.Errorf("store: Config.Engine %q: want rocksdb, lsm, btree or pgheap", name)
}

// failedIter is an iterator that could not open.
type failedIter struct{ err error }

func (i *failedIter) Seek([]byte)        {}
func (i *failedIter) SeekForPrev([]byte) {}
func (i *failedIter) SeekToFirst()       {}
func (i *failedIter) SeekToLast()        {}
func (i *failedIter) Valid() bool        { return false }
func (i *failedIter) Next()              {}
func (i *failedIter) Prev()              {}
func (i *failedIter) Key() []byte        { return nil }
func (i *failedIter) Value() []byte      { return nil }
func (i *failedIter) Err() error         { return i.err }
func (i *failedIter) Close()             {}
