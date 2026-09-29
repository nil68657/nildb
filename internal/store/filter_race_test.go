package store

import (
	"sync"
	"testing"
	"time"

	"github.com/nil68657/nildb/internal/layout"
)

// TestFilterRace runs the shared subFilter (one instance on sub and
// zscore, one verdict cache) from several compaction threads at once:
// max_subcompactions=4 with 16 KiB target files, non-exclusive manual
// compactions of both CFs in parallel, and a writer creating and killing
// versions meanwhile. An instrumented run of this test saw four Filter
// calls in flight at once. Run it under -race. Afterwards dead versions
// are gone and live ones intact.
func TestFilterRace(t *testing.T) {
	rocksOnly(t, "runs RocksDB subcompactions against the shared filter; the sweeper has no shared state")
	old := maxSubcompactions
	maxSubcompactions = 4
	defer func() { maxSubcompactions = old }()
	clock := newFakeClock()
	s := openTest(t, func(c *Config) { c.Clock = clock.Now })
	// Keep the loaded files in L0, and size output files at 16 KiB so one
	// compaction splits into several subcompaction ranges.
	for _, cf := range []CF{CFSub, CFZScore} {
		for name, value := range map[string]string{"disable_auto_compactions": "true", "target_file_size_base": "16384"} {
			if err := s.SetOption(cf, name, value); err != nil {
				t.Fatal(err)
			}
		}
	}

	const versions, elems, files = 480, 24, 8
	oldT := clock.Now().Add(-10 * time.Minute)
	ver := func(i int) uint64 { return versionAt(oldT, uint64(i)) }
	live := func(i int) bool { return i%3 != 0 }
	member := func(j int) []byte { return []byte{'m', byte(j)} }

	// Eight overlapping L0 files per CF: file f holds every version i with
	// i % files == f, so every file spans the whole key range.
	for f := range files {
		txn := s.Begin()
		for i := f; i < versions; i += files {
			if live(i) {
				txn.Put(CFMeta, layout.VersionKey(nil, ver(i)), layout.VersionEntry{Key: []byte("k")}.Encode(nil))
			}
			for j := range elems {
				txn.Put(CFSub, layout.SubKey(nil, ver(i), member(j)), []byte("v"))
				txn.Put(CFZScore, layout.ScoreKey(nil, ver(i), float64(j), member(j)), nil)
			}
		}
		commit(t, txn)
		for _, cf := range []CF{CFSub, CFZScore} {
			if err := s.Flush(cf); err != nil {
				t.Fatal(err)
			}
		}
	}

	stop := make(chan struct{})
	var bg sync.WaitGroup
	// Writer: versions in their own counter range, some killed at once.
	bg.Add(1)
	go func() {
		defer bg.Done()
		for n := 0; ; n++ {
			select {
			case <-stop:
				return
			default:
			}
			v := versionAt(oldT, uint64(10_000+n))
			txn := s.Begin()
			txn.Put(CFMeta, layout.VersionKey(nil, v), layout.VersionEntry{Key: []byte("w")}.Encode(nil))
			for j := range 8 {
				txn.Put(CFSub, layout.SubKey(nil, v, member(j)), []byte("w"))
				txn.Put(CFZScore, layout.ScoreKey(nil, v, float64(j), member(j)), nil)
			}
			if n%2 == 0 {
				txn.Delete(CFMeta, layout.VersionKey(nil, v))
			}
			if err := txn.Commit(); err != nil {
				t.Error(err)
				return
			}
			if n%50 == 0 {
				s.Flush(CFSub)
			}
		}
	}()
	// Reader on live versions.
	bg.Add(1)
	go func() {
		defer bg.Done()
		for n := 0; ; n++ {
			select {
			case <-stop:
				return
			default:
			}
			i := (n*7)%versions | 1
			if _, _, err := s.Get(CFSub, layout.SubKey(nil, ver(i), member(n%elems))); err != nil {
				t.Error(err)
				return
			}
		}
	}()

	var comp sync.WaitGroup
	for round := range 3 {
		for _, cf := range []CF{CFSub, CFZScore} {
			comp.Add(1)
			go func() {
				defer comp.Done()
				if err := s.Compact(cf, nil, nil); err != nil {
					t.Errorf("round %d Compact(%s): %v", round, cf, err)
				}
			}()
		}
		comp.Wait()
	}
	close(stop)
	bg.Wait()

	for i := range versions {
		for j := range elems {
			_, okSub := mustGet(t, s, CFSub, layout.SubKey(nil, ver(i), member(j)))
			_, okZ := mustGet(t, s, CFZScore, layout.ScoreKey(nil, ver(i), float64(j), member(j)))
			if okSub != live(i) || okZ != live(i) {
				t.Fatalf("version %d element %d: sub %v zscore %v, want %v", i, j, okSub, okZ, live(i))
			}
		}
	}
}
