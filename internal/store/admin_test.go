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
	// Writes after the checkpoint stay out of it.
	txn = s.Begin()
	txn.Put(CFDoc, layout.DocKey(nil, 1, []byte("later")), []byte("row"))
	commit(t, txn)

	ro, err := OpenReadOnly(Config{Dir: dir, BlockCacheBytes: 8 << 20})
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

	// The checkpoint also opens through grocksdb directly.
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

	if _, err := OpenReadOnly(Config{Dir: t.TempDir()}); err == nil {
		t.Fatal("OpenReadOnly of an empty directory succeeded")
	}
}

func TestAdminCalls(t *testing.T) {
	s := openTest(t, func(c *Config) { c.Statistics = true })
	txn := s.Begin()
	for i := range 200 {
		txn.Put(CFDoc, fmt.Appendf(nil, "\x00\x00\x00\x01k%04d", i), []byte(strings.Repeat("v", 100)))
	}
	commit(t, txn)
	if err := s.Flush(CFDoc); err != nil {
		t.Fatal(err)
	}

	if v, ok := s.Property(CFDoc, "rocksdb.stats"); !ok || !strings.Contains(v, "Compaction Stats") {
		t.Fatalf("Property(rocksdb.stats) = %v, %.60q", ok, v)
	}
	if _, ok := s.Property(CFDoc, "rocksdb.no-such-property"); ok {
		t.Fatal("unknown property reported present")
	}
	if n, ok := s.IntProperty(CFDoc, "rocksdb.estimate-num-keys"); !ok || n == 0 {
		t.Fatalf("IntProperty(estimate-num-keys) = %d, %v", n, ok)
	}
	if n, ok := s.IntProperty(CFDefault, "rocksdb.num-snapshots"); !ok || n != 0 {
		t.Fatalf("num-snapshots = %d, %v", n, ok)
	}
	lo, hi := layout.IDBounds(1)
	sizes, err := s.ApproxSizes(CFDoc, [][2][]byte{{lo, hi}, {hi, layout.IDPrefix(nil, 9)}})
	if err != nil || len(sizes) != 2 || sizes[0] == 0 {
		t.Fatalf("ApproxSizes = %v, %v", sizes, err)
	}
	if err := s.SetOption(CFDoc, "disable_auto_compactions", "true"); err != nil {
		t.Fatalf("SetOption: %v", err)
	}
	if err := s.SetOption(CFDoc, "no_such_option", "1"); err == nil {
		t.Fatal("SetOption accepted an unknown option")
	}
	if stats, ok := s.Stats(); !ok || !strings.Contains(stats, "rocksdb.") {
		t.Fatalf("Stats = %v, %.60q", ok, stats)
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

func TestClosedStoreRefusesWork(t *testing.T) {
	if testing.Short() {
		t.Skip("opens RocksDB")
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
