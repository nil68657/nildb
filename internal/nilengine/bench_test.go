//go:build nilengine

package nilengine

// One workload on four stores: the LSM, B+ tree and pgheap engines through
// this binding, and RocksDB through grocksdb, the library internal/store
// uses. Every store crosses cgo once per operation, so the comparison
// includes the same binding cost NilDB pays.
//
// Settings shared by all four: 64 MiB write buffer, 256 MiB block cache,
// page cache or buffer pool, bloom filters at 10 bits per key (LSM and
// RocksDB), no compression, unsynced writes. RocksDB keeps its other
// defaults and runs two background jobs, matching the LSM engine's flush and
// compaction threads.
//
// BenchmarkUpdateHot and BenchmarkUpdateLongSnapshot update a small key set
// (hotKeys keys) and report disk use; on pgheap they also report the HOT
// ratio, dead tuples and heap size.
//
//	make rust-build
//	go test -tags nilengine -run '^$' -bench . -benchtime 200000x ./internal/nilengine
//
// NILENGINE_BENCH_KEYS sets the size of the dataset the read, seek and mixed
// benchmarks run on (default 1000000).

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/linxGnu/grocksdb"
)

const (
	benchValueSize = 100
	writeBuffer    = 64 << 20
	cacheBytes     = 256 << 20
)

// store is the slice of the engine API the benchmark needs.
type store interface {
	write(keys [][]byte, vals [][]byte) error
	get(key []byte) (bool, error)
	seekNext(key []byte, n int) (int, error)
	// snapshot pins the current state until the returned func runs.
	snapshot() (func(), error)
	// compact runs a full compaction (VACUUM on pgheap).
	compact() error
	prop(name string) (string, bool)
	close() error
}

type nilStore struct{ db *DB }

func openNil(kind Kind, dir string) (store, error) {
	return openNilWith(kind, dir, "")
}

func openNilWith(kind Kind, dir, extra string) (store, error) {
	opts := fmt.Sprintf("write_buffer_size=%d;cache_bytes=%d;bloom_bits_per_key=10;%s", writeBuffer, cacheBytes, extra)
	db, err := Open(kind, dir, []string{"default"}, opts)
	if err != nil {
		return nil, err
	}
	return nilStore{db}, nil
}

func (s nilStore) write(keys, vals [][]byte) error {
	b := NewBatch()
	defer b.Destroy()
	for i := range keys {
		b.Put(0, keys[i], vals[i])
	}
	return s.db.Write(b, false)
}

func (s nilStore) get(key []byte) (bool, error) {
	_, ok, err := s.db.Get(0, key)
	return ok, err
}

func (s nilStore) seekNext(key []byte, n int) (int, error) {
	it, err := s.db.NewIterator(0, IterOptions{FillCache: true})
	if err != nil {
		return 0, err
	}
	defer it.Close()
	seen := 0
	for it.Seek(key); it.Valid() && seen < n; it.Next() {
		seen += len(it.Value()) / benchValueSize
	}
	return seen, it.Err()
}

func (s nilStore) close() error { return s.db.Close() }

func (s nilStore) snapshot() (func(), error) {
	snap, err := s.db.NewSnapshot()
	if err != nil {
		return nil, err
	}
	return snap.Release, nil
}

func (s nilStore) compact() error { return s.db.CompactRange(0, nil, nil) }

func (s nilStore) prop(name string) (string, bool) { return s.db.Property(0, name) }

type rocksStore struct {
	db   *grocksdb.DB
	opts *grocksdb.Options
	bbto *grocksdb.BlockBasedTableOptions
	wo   *grocksdb.WriteOptions
	ro   *grocksdb.ReadOptions
}

func openRocks(dir string) (store, error) {
	bbto := grocksdb.NewDefaultBlockBasedTableOptions()
	bbto.SetBlockCache(grocksdb.NewLRUCache(cacheBytes))
	bbto.SetFilterPolicy(grocksdb.NewBloomFilter(10))
	opts := grocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)
	opts.SetWriteBufferSize(writeBuffer)
	opts.SetCompression(grocksdb.NoCompression)
	opts.SetMaxBackgroundJobs(2)
	opts.SetBlockBasedTableFactory(bbto)
	db, err := grocksdb.OpenDb(opts, dir)
	if err != nil {
		return nil, err
	}
	return &rocksStore{db: db, opts: opts, bbto: bbto, wo: grocksdb.NewDefaultWriteOptions(), ro: grocksdb.NewDefaultReadOptions()}, nil
}

func (s *rocksStore) write(keys, vals [][]byte) error {
	wb := grocksdb.NewWriteBatch()
	defer wb.Destroy()
	for i := range keys {
		wb.Put(keys[i], vals[i])
	}
	return s.db.Write(s.wo, wb)
}

func (s *rocksStore) get(key []byte) (bool, error) {
	v, err := s.db.GetPinned(s.ro, key)
	if err != nil {
		return false, err
	}
	defer v.Destroy()
	return v.Exists(), nil
}

func (s *rocksStore) seekNext(key []byte, n int) (int, error) {
	it := s.db.NewIterator(s.ro)
	defer it.Close()
	seen := 0
	for it.Seek(key); it.Valid() && seen < n; it.Next() {
		seen += len(it.Value().Data()) / benchValueSize
	}
	return seen, it.Err()
}

func (s *rocksStore) close() error {
	s.db.Close()
	s.wo.Destroy()
	s.ro.Destroy()
	return nil
}

func (s *rocksStore) snapshot() (func(), error) {
	snap := s.db.NewSnapshot()
	return func() { s.db.ReleaseSnapshot(snap) }, nil
}

func (s *rocksStore) compact() error {
	s.db.CompactRange(grocksdb.Range{})
	return nil
}

func (s *rocksStore) prop(name string) (string, bool) {
	v := s.db.GetProperty(name)
	return v, v != ""
}

type namedStore struct {
	name string
	open func(dir string) (store, error)
}

var stores = []namedStore{
	{"lsm", func(d string) (store, error) { return openNil(LSM, d) }},
	{"btree", func(d string) (store, error) { return openNil(BTree, d) }},
	{"pgheap", func(d string) (store, error) { return openNil(PgHeap, d) }},
	{"rocksdb", openRocks},
}

func benchKey(i uint64) []byte { return []byte(fmt.Sprintf("%016d", i)) }

// valuePool returns fixed pseudo-random values; the same bytes go to every
// store.
var valuePool = func() [][]byte {
	pool := make([][]byte, 1024)
	x := uint64(0x9e3779b97f4a7c15)
	for i := range pool {
		v := make([]byte, benchValueSize)
		for j := range v {
			x ^= x << 13
			x ^= x >> 7
			x ^= x << 17
			v[j] = byte(x)
		}
		pool[i] = v
	}
	return pool
}()

type rng uint64

func (r *rng) below(n uint64) uint64 {
	*r ^= *r << 13
	*r ^= *r >> 7
	*r ^= *r << 17
	return uint64(*r) % n
}

func datasetKeys() uint64 {
	if s := os.Getenv("NILENGINE_BENCH_KEYS"); s != "" {
		if n, err := strconv.ParseUint(s, 10, 64); err == nil && n > 0 {
			return n
		}
	}
	return 1_000_000
}

func fill(b *testing.B, s store, order func(i uint64) uint64, n uint64, batch int) {
	keys := make([][]byte, 0, batch)
	vals := make([][]byte, 0, batch)
	for i := range n {
		keys = append(keys, benchKey(order(i)))
		vals = append(vals, valuePool[i%uint64(len(valuePool))])
		if len(keys) == batch {
			if err := s.write(keys, vals); err != nil {
				b.Fatal(err)
			}
			keys, vals = keys[:0], vals[:0]
		}
	}
	if len(keys) > 0 {
		if err := s.write(keys, vals); err != nil {
			b.Fatal(err)
		}
	}
}

// permutation maps 0..n-1 onto itself in a scattered order: an affine map
// modulo n with a multiplier coprime to n.
func permutation(n uint64) func(uint64) uint64 {
	a := uint64(2654435761) % n
	for a == 0 || gcd(a, n) != 1 {
		a++
	}
	return func(i uint64) uint64 { return (i*a + 12345) % n }
}

func gcd(a, b uint64) uint64 {
	for b != 0 {
		a, b = b, a%b
	}
	return a
}

func BenchmarkFillSeq(b *testing.B) {
	for _, st := range stores {
		b.Run(st.name, func(b *testing.B) {
			s, err := st.open(b.TempDir())
			if err != nil {
				b.Fatal(err)
			}
			defer s.close()
			b.SetBytes(16 + benchValueSize)
			b.ResetTimer()
			fill(b, s, func(i uint64) uint64 { return i }, uint64(b.N), 100)
		})
	}
}

func BenchmarkFillRandom(b *testing.B) {
	for _, st := range stores {
		b.Run(st.name, func(b *testing.B) {
			s, err := st.open(b.TempDir())
			if err != nil {
				b.Fatal(err)
			}
			defer s.close()
			perm := permutation(uint64(b.N))
			b.SetBytes(16 + benchValueSize)
			b.ResetTimer()
			fill(b, s, perm, uint64(b.N), 1)
		})
	}
}

// Prefilled datasets, one per store, shared by the read benchmarks and
// removed by TestMain.
var (
	prefillMu   sync.Mutex
	prefilled   = map[string]store{}
	prefillDirs []string
)

func dataset(b *testing.B, name string, open func(string) (store, error)) (store, uint64) {
	n := datasetKeys()
	prefillMu.Lock()
	defer prefillMu.Unlock()
	if s, ok := prefilled[name]; ok {
		return s, n
	}
	dir, err := os.MkdirTemp("", "nilengine-bench-"+name+"-")
	if err != nil {
		b.Fatal(err)
	}
	prefillDirs = append(prefillDirs, dir)
	s, err := open(dir)
	if err != nil {
		b.Fatal(err)
	}
	fill(b, s, permutation(n), n, 1000)
	prefilled[name] = s
	return s, n
}

func TestMain(m *testing.M) {
	code := m.Run()
	for _, s := range prefilled {
		_ = s.close()
	}
	for _, d := range prefillDirs {
		_ = os.RemoveAll(d)
	}
	os.Exit(code)
}

func BenchmarkReadRandom(b *testing.B) {
	for _, st := range stores {
		b.Run(st.name, func(b *testing.B) {
			s, n := dataset(b, st.name, st.open)
			r := rng(11)
			b.ResetTimer()
			for range b.N {
				ok, err := s.get(benchKey(r.below(n)))
				if err != nil || !ok {
					b.Fatalf("get: %v %v", ok, err)
				}
			}
		})
	}
}

func BenchmarkSeekNext10(b *testing.B) {
	for _, st := range stores {
		b.Run(st.name, func(b *testing.B) {
			s, n := dataset(b, st.name, st.open)
			r := rng(13)
			b.ResetTimer()
			for range b.N {
				if _, err := s.seekNext(benchKey(r.below(n)), 10); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkMixed runs 80% reads and 20% single-key writes from parallel
// goroutines (GOMAXPROCS of them).
func BenchmarkMixed(b *testing.B) {
	for _, st := range stores {
		b.Run(st.name, func(b *testing.B) {
			s, n := dataset(b, st.name, st.open)
			var seed atomic.Uint64
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				r := rng(seed.Add(1)*7919 + 1)
				key := [][]byte{nil}
				val := [][]byte{nil}
				for pb.Next() {
					k := benchKey(r.below(n))
					if r.below(10) < 2 {
						key[0], val[0] = k, valuePool[r.below(uint64(len(valuePool)))]
						if err := s.write(key, val); err != nil {
							b.Fatal(err)
						}
					} else if _, err := s.get(k); err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}

// hotKeys is the key set the update benchmarks rewrite.
const hotKeys = 10_000

// updateStores adds pgheap without autovacuum to the four stores.
var updateStores = append(append([]namedStore{}, stores...), namedStore{
	"pgheap-noautovacuum",
	func(d string) (store, error) { return openNilWith(PgHeap, d, "autovacuum=false") },
})

func dirBytes(dir string) int64 {
	var n int64
	_ = filepath.WalkDir(dir, func(_ string, d fs.DirEntry, err error) error {
		if err == nil && !d.IsDir() {
			if info, err := d.Info(); err == nil {
				n += info.Size()
			}
		}
		return nil
	})
	return n
}

func propNum(s store, name string) (float64, bool) {
	v, ok := s.prop(name)
	if !ok {
		return 0, false
	}
	f, err := strconv.ParseFloat(v, 64)
	return f, err == nil
}

// reportHeap adds pgheap's counters to a benchmark result, with `suffix`
// naming the phase.
func reportHeap(b *testing.B, s store, suffix string) {
	upd, ok := propNum(s, "nil.pgheap.n-tup-upd")
	if !ok {
		return
	}
	hot, _ := propNum(s, "nil.pgheap.n-tup-hot-upd")
	dead, _ := propNum(s, "nil.pgheap.n-dead-tup")
	heap, _ := propNum(s, "nil.pgheap.heap-bytes")
	if upd > 0 {
		b.ReportMetric(100*hot/upd, "hot-%"+suffix)
	}
	b.ReportMetric(dead, "dead-tuples"+suffix)
	b.ReportMetric(heap/1e6, "heap-MB"+suffix)
}

func loadHot(b *testing.B, s store) {
	fill(b, s, func(i uint64) uint64 { return i }, hotKeys, 1000)
}

func updateRandom(b *testing.B, s store, n int, seed uint64) {
	r := rng(seed)
	key := [][]byte{nil}
	val := [][]byte{nil}
	for i := range n {
		key[0] = benchKey(r.below(hotKeys))
		val[0] = valuePool[i%len(valuePool)]
		if err := s.write(key, val); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkUpdateHot rewrites random keys of a 10,000-key set, one key per
// commit: the case HOT updates and pruning exist for.
func BenchmarkUpdateHot(b *testing.B) {
	for _, st := range updateStores {
		b.Run(st.name, func(b *testing.B) {
			dir := b.TempDir()
			s, err := st.open(dir)
			if err != nil {
				b.Fatal(err)
			}
			defer s.close()
			loadHot(b, s)
			b.ResetTimer()
			updateRandom(b, s, b.N, 17)
			b.StopTimer()
			b.ReportMetric(float64(dirBytes(dir))/1e6, "disk-MB")
			reportHeap(b, s, "")
		})
	}
}

// BenchmarkUpdateLongSnapshot times b.N updates of the 10,000-key set while
// a snapshot taken before them stays open, then releases it, compacts
// (VACUUM on pgheap) and runs b.N more updates untimed. It reports disk use
// at the end of each phase: -held, -compacted and -reuse.
func BenchmarkUpdateLongSnapshot(b *testing.B) {
	for _, st := range stores {
		b.Run(st.name, func(b *testing.B) {
			dir := b.TempDir()
			s, err := st.open(dir)
			if err != nil {
				b.Fatal(err)
			}
			defer s.close()
			loadHot(b, s)
			release, err := s.snapshot()
			if err != nil {
				b.Fatal(err)
			}
			b.ResetTimer()
			updateRandom(b, s, b.N, 29)
			b.StopTimer()
			b.ReportMetric(float64(dirBytes(dir))/1e6, "disk-MB-held")
			reportHeap(b, s, "-held")
			release()
			if err := s.compact(); err != nil {
				b.Fatal(err)
			}
			b.ReportMetric(float64(dirBytes(dir))/1e6, "disk-MB-compacted")
			reportHeap(b, s, "-compacted")
			updateRandom(b, s, b.N, 31)
			b.ReportMetric(float64(dirBytes(dir))/1e6, "disk-MB-reuse")
			reportHeap(b, s, "-reuse")
		})
	}
}
