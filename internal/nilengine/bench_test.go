//go:build nilengine

package nilengine

// One workload on three stores: the LSM engine and the B+ tree engine
// through this binding, and RocksDB through grocksdb, the library
// internal/store uses. Every store crosses cgo once per operation, so the
// comparison includes the same binding cost NilDB pays.
//
// Settings shared by all three: 64 MiB write buffer, 256 MiB block or page
// cache, bloom filters at 10 bits per key (LSM and RocksDB), no compression,
// unsynced writes. RocksDB keeps its other defaults and runs two background
// jobs, matching the LSM engine's flush and compaction threads.
//
//	make rust-build
//	go test -tags nilengine -run '^$' -bench . -benchtime 200000x ./internal/nilengine
//
// NILENGINE_BENCH_KEYS sets the size of the dataset the read, seek and mixed
// benchmarks run on (default 1000000).

import (
	"fmt"
	"os"
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
	close() error
}

type nilStore struct{ db *DB }

func openNil(kind Kind, dir string) (store, error) {
	opts := fmt.Sprintf("write_buffer_size=%d;cache_bytes=%d;bloom_bits_per_key=10", writeBuffer, cacheBytes)
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

var stores = []struct {
	name string
	open func(dir string) (store, error)
}{
	{"lsm", func(d string) (store, error) { return openNil(LSM, d) }},
	{"btree", func(d string) (store, error) { return openNil(BTree, d) }},
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
