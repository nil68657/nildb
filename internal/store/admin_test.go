package store

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/linxGnu/grocksdb"
	"github.com/nil68657/nildb/internal/layout"
)

func TestCheckpointOpenReadOnly(t *testing.T) {
	s := openTest(t, nil)
	txn := s.Begin()
	txn.Put(CFDoc, layout.DocKey(nil, 1, []byte("id")), []byte("row"))
	txn.Merge(CFDefault, layout.CountKey(1), layout.AppendCount(nil, 3))
	commit(t, txn)
	seq := s.LatestSeq()
	dir := filepath.Join(t.TempDir(), "cp")
	if err := s.Checkpoint(dir); err != nil {
		t.Fatal(err)
	}
	if b, err := os.ReadFile(filepath.Join(dir, "ENGINE")); err != nil || string(b) != s.Engine()+"\n" {
		t.Fatalf("checkpoint ENGINE file = %q, %v", b, err)
	}
	if err := s.Checkpoint(dir); err == nil {
		t.Fatal("a second checkpoint into the same directory succeeded")
	}
	// Writes after the checkpoint stay out of it.
	txn = s.Begin()
	txn.Put(CFDoc, layout.DocKey(nil, 1, []byte("later")), []byte("row"))
	commit(t, txn)

	ro, err := OpenReadOnly(Config{Dir: dir, Engine: s.Engine(), BlockCacheBytes: 8 << 20})
	if err != nil {
		t.Fatalf("OpenReadOnly(checkpoint): %v", err)
	}
	defer ro.Close()
	if !ro.ReadOnly() || ro.LatestSeq() != seq {
		t.Fatalf("read-only store: ReadOnly %v, seq %d want %d", ro.ReadOnly(), ro.LatestSeq(), seq)
	}
	wantValue(t, ro, CFDoc, layout.DocKey(nil, 1, []byte("id")), []byte("row"))
	wantAbsent(t, ro, CFDoc, layout.DocKey(nil, 1, []byte("later")))
	wantValue(t, ro, CFDefault, layout.CountKey(1), layout.AppendCount(nil, 3))
	lease, err := ro.Lease("readonly", 0)
	if err != nil {
		t.Fatal(err)
	}
	wantValue(t, ro.At(lease), CFDoc, layout.DocKey(nil, 1, []byte("id")), []byte("row"))
	txn = ro.Begin()
	txn.Put(CFDoc, []byte("x"), []byte("y"))
	if err := txn.Commit(); err == nil {
		t.Fatal("write to a read-only store succeeded")
	}

	t.Run("raw open", func(t *testing.T) {
		rocksOnly(t, "opens the checkpoint with grocksdb")
		opts := make([]*grocksdb.Options, NumCFs)
		for i := range opts {
			opts[i] = grocksdb.NewDefaultOptions()
		}
		db, cfs, err := grocksdb.OpenDbForReadOnlyColumnFamilies(grocksdb.NewDefaultOptions(), dir, cfNames[:], opts, false)
		if err != nil {
			t.Fatal(err)
		}
		rdo := grocksdb.NewDefaultReadOptions()
		v, err := db.GetCF(rdo, cfs[CFDoc], layout.DocKey(nil, 1, []byte("id")))
		if err != nil || string(v.Data()) != "row" {
			t.Fatalf("raw read-only Get = %q, %v", v.Data(), err)
		}
		v.Free()
		rdo.Destroy()
		for _, h := range cfs {
			h.Destroy()
		}
		db.Close()
	})

	if _, err := OpenReadOnly(Config{Dir: t.TempDir(), Engine: s.Engine()}); err == nil {
		t.Fatal("OpenReadOnly of an empty directory succeeded")
	}
}

// fillDoc writes 200 rows of collection 1 and flushes them, so the LSM
// and RocksDB have table files to report on.
func fillDoc(t *testing.T, s *Store) {
	t.Helper()
	txn := s.Begin()
	for i := range 200 {
		txn.Put(CFDoc, fmt.Appendf(nil, "\x00\x00\x00\x01k%04d", i), []byte(strings.Repeat("v", 100)))
	}
	commit(t, txn)
	if err := s.Flush(CFDoc); err != nil {
		t.Fatal(err)
	}
}

func TestAdminCalls(t *testing.T) {
	s := openTest(t, func(c *Config) { c.Statistics = true })
	fillDoc(t, s)
	if _, ok := s.Property(CFDoc, "rocksdb.no-such-property"); ok {
		t.Fatal("unknown property reported present")
	}
	if n, ok := s.IntProperty(CFDoc, "rocksdb.estimate-num-keys"); !ok || n == 0 {
		t.Fatalf("IntProperty(estimate-num-keys) = %d, %v", n, ok)
	}
	if n, ok := s.IntProperty(CFDefault, "rocksdb.num-snapshots"); !ok || n != 0 {
		t.Fatalf("num-snapshots = %d, %v", n, ok)
	}
	if n, ok := s.IntProperty(CFDefault, "rocksdb.oldest-snapshot-time"); !ok || n != 0 {
		t.Fatalf("oldest-snapshot-time with no snapshot = %d, %v", n, ok)
	}
	snap := s.Snapshot()
	if n, ok := s.IntProperty(CFDefault, "rocksdb.num-snapshots"); !ok || n != 1 {
		t.Fatalf("num-snapshots with one open = %d, %v", n, ok)
	}
	if n, ok := s.IntProperty(CFDefault, "rocksdb.oldest-snapshot-time"); !ok || n == 0 {
		t.Fatalf("oldest-snapshot-time with one open = %d, %v", n, ok)
	}
	s.Release(snap)
	lo, hi := layout.IDBounds(1)
	sizes, err := s.ApproxSizes(CFDoc, [][2][]byte{{lo, hi}, {hi, layout.IDPrefix(nil, 9)}})
	if err != nil || len(sizes) != 2 || sizes[0] == 0 {
		t.Fatalf("ApproxSizes = %v, %v", sizes, err)
	}
	stats, ok := s.Stats()
	want := "rocksdb."
	if s.Engine() != EngineRocksDB {
		want = "column family"
	}
	if !ok || !strings.Contains(stats, want) {
		t.Fatalf("Stats = %v, %.60q", ok, stats)
	}
}

func TestAdminCallsRocksDB(t *testing.T) {
	rocksOnly(t, "SetOption, ExportCF, Ingest and RocksDB's statistics dump")
	s := openTest(t, func(c *Config) { c.Statistics = true })
	fillDoc(t, s)
	if v, ok := s.Property(CFDoc, "rocksdb.stats"); !ok || !strings.Contains(v, "Compaction Stats") {
		t.Fatalf("Property(rocksdb.stats) = %v, %.60q", ok, v)
	}
	if err := s.SetOption(CFDoc, "disable_auto_compactions", "true"); err != nil {
		t.Fatalf("SetOption: %v", err)
	}
	if err := s.SetOption(CFDoc, "no_such_option", "1"); err == nil {
		t.Fatal("SetOption accepted an unknown option")
	}
	if active, _ := s.ShimActive(); active != func() bool { a, _ := ShimActive(); return a }() {
		t.Fatal("Store.ShimActive disagrees with ShimActive")
	}

	exportDir := filepath.Join(t.TempDir(), "export")
	if err := s.ExportCF(CFDoc, exportDir); err != nil {
		t.Fatalf("ExportCF: %v", err)
	}
	if entries, err := os.ReadDir(exportDir); err != nil || len(entries) == 0 {
		t.Fatalf("export dir: %v, %d entries", err, len(entries))
	}

	// Ingest an SST built with grocksdb's writer.
	sst := filepath.Join(t.TempDir(), "in.sst")
	w := grocksdb.NewSSTFileWriter(grocksdb.NewDefaultEnvOptions(), grocksdb.NewDefaultOptions())
	if err := w.Open(sst); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"\x00\x00\x00\x07a", "\x00\x00\x00\x07b"} {
		if err := w.Put([]byte(k), []byte("ingested")); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Finish(); err != nil {
		t.Fatal(err)
	}
	w.Destroy()
	if err := s.Ingest(CFDoc, []string{sst}); err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	wantValue(t, s, CFDoc, []byte("\x00\x00\x00\x07b"), []byte("ingested"))

	if s2 := openTest(t, nil); func() bool { _, ok := s2.Stats(); return ok }() {
		t.Fatal("Stats reported with statistics off")
	}
}

func TestAdminCallsUnsupported(t *testing.T) {
	rustOnly(t, "RocksDB supports every admin call")
	s := openTest(t, nil)
	want := "not supported with --engine " + s.Engine()
	for name, err := range map[string]error{
		"SetOption": s.SetOption(CFDoc, "disable_auto_compactions", "true"),
		"ExportCF":  s.ExportCF(CFDoc, filepath.Join(t.TempDir(), "export")),
		"Ingest":    s.Ingest(CFDoc, []string{filepath.Join(t.TempDir(), "in.sst")}),
	} {
		if !errors.Is(err, ErrUnsupported) || err.Error() != want {
			t.Errorf("%s: %v, want %q", name, err, want)
		}
	}
	if active, why := s.ShimActive(); active || why == nil || why.Error() != "engine "+s.Engine()+" has no rate limiter" {
		t.Errorf("ShimActive = %v, %v", active, why)
	}
	if stats, ok := s.Stats(); !ok || !strings.Contains(stats, "column family") {
		t.Errorf("Stats without --rocks-stats = %v, %.60q", ok, stats)
	}
}

func TestClosedStoreRefusesWork(t *testing.T) {
	if testing.Short() {
		t.Skip("opens the engine")
	}
	s, err := Open(testConfig(t, nil))
	if err != nil {
		t.Fatal(err)
	}
	txn := s.Begin()
	txn.Put(CFDoc, []byte("k"), []byte("v"))
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	for name, err := range map[string]error{
		"Commit":       txn.Commit(),
		"Flush":        s.Flush(CFDoc),
		"Compact":      s.Compact(CFDoc, nil, nil),
		"FlushWAL":     s.FlushWAL(true),
		"Checkpoint":   s.Checkpoint(t.TempDir()),
		"DeleteRanges": s.DeleteRanges(nil, nil),
		"Iter":         s.Iter(CFDoc, nil, nil, IterOpts{}).Err(),
	} {
		if !errors.Is(err, ErrClosed) {
			t.Errorf("%s after Close: %v, want ErrClosed", name, err)
		}
	}
	if _, err := s.MultiGet(CFDoc, [][]byte{[]byte("k")}); !errors.Is(err, ErrClosed) {
		t.Errorf("MultiGet after Close: %v", err)
	}
}
