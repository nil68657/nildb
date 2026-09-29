package store

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/linxGnu/grocksdb"
	"github.com/nil68657/nildb/internal/layout"
)

func TestOpenCloseTwice(t *testing.T) {
	if testing.Short() {
		t.Skip("opens RocksDB")
	}
	cfg := testConfig(t, nil)
	for round := range 2 {
		s, err := Open(cfg)
		if err != nil {
			t.Fatalf("Open round %d: %v", round, err)
		}
		key := []byte{byte('a' + round)}
		txn := s.Begin()
		txn.Put(CFDoc, key, []byte("v"))
		commit(t, txn)
		wantValue(t, s, CFDefault, layout.LayoutMarkerKey, []byte(layout.LayoutMarker))
		if round == 1 {
			wantValue(t, s, CFDoc, []byte("a"), []byte("v"))
		}
		if err := s.Close(); err != nil {
			t.Fatalf("Close round %d: %v", round, err)
		}
		if err := s.Close(); err != nil {
			t.Fatalf("second Close round %d: %v", round, err)
		}
		if _, _, err := s.Get(CFDoc, key); err != ErrClosed {
			t.Fatalf("Get after Close: %v, want ErrClosed", err)
		}
	}
}

func TestEightColumnFamilies(t *testing.T) {
	s := openTest(t, nil)
	want := []string{"default", "meta", "sub", "zscore", "doc", "idx", "geo", "col"}
	if got := s.CFNames(); !slices.Equal(got, want) {
		t.Fatalf("CFNames = %v, want %v", got, want)
	}
	for i, name := range want {
		cf, ok := ParseCF(name)
		if !ok || cf != CF(i) || cf.String() != name {
			t.Errorf("ParseCF(%q) = %v, %v", name, cf, ok)
		}
		txn := s.Begin()
		txn.Put(cf, []byte("k"), []byte(name))
		commit(t, txn)
	}
	for i, name := range want {
		wantValue(t, s, CF(i), []byte("k"), []byte(name))
	}
	if _, ok := ParseCF("nope"); ok {
		t.Error("ParseCF(nope) succeeded")
	}
	t.Run("rocksdb families", func(t *testing.T) {
		rocksOnly(t, "lists the column families with grocksdb")
		got, err := grocksdb.ListColumnFamilies(grocksdb.NewDefaultOptions(), s.Config().Dir)
		if err != nil {
			t.Fatal(err)
		}
		slices.Sort(got)
		sorted := slices.Clone(want)
		slices.Sort(sorted)
		if !slices.Equal(got, sorted) {
			t.Fatalf("ListColumnFamilies = %v, want %v", got, sorted)
		}
		for i, name := range want {
			if h := s.Handle(CF(i)).(*grocksdb.ColumnFamilyHandle); h.Name() != name {
				t.Errorf("Handle(%s).Name() = %q", CF(i), h.Name())
			}
		}
	})
}

func TestEngineMarker(t *testing.T) {
	if testing.Short() {
		t.Skip("opens the engine")
	}
	engine := TestEngine()
	cfg := testConfig(t, nil)
	s, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if s.Engine() != engine || s.EngineVersion() == "" {
		t.Fatalf("Engine() = %q, EngineVersion() = %q", s.Engine(), s.EngineVersion())
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if b, err := os.ReadFile(filepath.Join(cfg.Dir, "ENGINE")); err != nil || string(b) != engine+"\n" {
		t.Fatalf("ENGINE file = %q, %v; want %q", b, err, engine+"\n")
	}

	// A directory another engine's marker names opens with that one only.
	other := EngineLSM
	if engine == EngineLSM {
		other = EngineBTree
	}
	foreign := testConfig(t, nil)
	if err := os.WriteFile(filepath.Join(foreign.Dir, "ENGINE"), []byte(other+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, open := range []func(Config) (*Store, error){Open, OpenReadOnly} {
		if _, err := open(foreign); err == nil || !strings.Contains(err.Error(), "open it with --engine "+other) {
			t.Fatalf("open of a %s directory with engine %s: %v", other, engine, err)
		}
	}
	// A directory with files and no marker is a RocksDB directory from
	// before the marker existed.
	if engine != EngineRocksDB {
		legacy := testConfig(t, nil)
		if err := os.WriteFile(filepath.Join(legacy.Dir, "CURRENT"), []byte("MANIFEST-000005\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := Open(legacy); err == nil || !strings.Contains(err.Error(), "open it with --engine rocksdb") {
			t.Fatalf("open of an unmarked directory with %s: %v", engine, err)
		}
	}
	if _, err := Open(Config{Dir: t.TempDir(), Engine: "leveldb"}); err == nil || !strings.Contains(err.Error(), "want rocksdb, lsm, btree or pgheap") {
		t.Fatalf("Open with an unknown engine: %v", err)
	}
}

func TestLayoutMarkerChecks(t *testing.T) {
	if testing.Short() {
		t.Skip("opens RocksDB")
	}
	t.Run("wrong marker", func(t *testing.T) {
		cfg := testConfig(t, nil)
		s, err := Open(cfg)
		if err != nil {
			t.Fatal(err)
		}
		txn := s.Begin()
		txn.Put(CFDefault, layout.LayoutMarkerKey, []byte("nildb-layout-0"))
		commit(t, txn)
		s.Close()
		if _, err := Open(cfg); err == nil || !strings.Contains(err.Error(), "layout marker") {
			t.Fatalf("Open with a wrong marker: %v", err)
		}
	})
	t.Run("foreign data", func(t *testing.T) {
		rocksOnly(t, "writes the foreign database with grocksdb")
		cfg := testConfig(t, nil)
		opts := grocksdb.NewDefaultOptions()
		opts.SetCreateIfMissing(true)
		db, err := grocksdb.OpenDb(opts, cfg.Dir)
		if err != nil {
			t.Fatal(err)
		}
		wo := grocksdb.NewDefaultWriteOptions()
		if err := db.Put(wo, []byte("x"), []byte("y")); err != nil {
			t.Fatal(err)
		}
		db.Close()
		if _, err := Open(cfg); err == nil || !strings.Contains(err.Error(), "no layout marker") {
			t.Fatalf("Open of a foreign database: %v", err)
		}
	})
}

func TestConfigDefaultsAndValidation(t *testing.T) {
	c, err := Config{Dir: "x"}.withDefaults()
	if err != nil {
		t.Fatal(err)
	}
	if c.BlockCacheBytes != 512<<20 || c.AnalyticsCacheBytes != 64<<20 || c.WriteBufferBytes != 256<<20 ||
		c.BgIOBytesPerSec != 200<<20 || c.Fsync != FsyncEverySec || c.MaxSnapshots != 32 ||
		c.LeaseTTL != 60*time.Second || c.LeaseMax != 600*time.Second || c.Clock == nil {
		t.Fatalf("defaults = %+v", c)
	}
	for _, bad := range []Config{
		{},
		{Dir: "x", Fsync: "sometimes"},
		{Dir: "x", BlockCacheBytes: -1},
		{Dir: "x", MaxSnapshots: -1},
	} {
		if _, err := bad.withDefaults(); err == nil {
			t.Errorf("withDefaults(%+v) accepted", bad)
		}
	}
}

func TestFsyncPolicy(t *testing.T) {
	for _, tc := range []struct {
		fsync string
		sync  bool
	}{{FsyncAlways, true}, {FsyncEverySec, false}, {FsyncNo, false}} {
		s := openTest(t, func(c *Config) { c.Fsync = tc.fsync })
		if got := s.WriteOpts(); got != tc.sync {
			t.Errorf("Fsync %q: WriteOpts() = %v, want %v", tc.fsync, got, tc.sync)
		}
		txn := s.Begin()
		txn.Put(CFMeta, []byte("\x00k"), []byte("v"))
		commit(t, txn)
		if err := s.FlushWAL(true); err != nil {
			t.Errorf("Fsync %q: FlushWAL: %v", tc.fsync, err)
		}
	}
}

func TestVersionSeedPersists(t *testing.T) {
	if testing.Short() {
		t.Skip("opens RocksDB")
	}
	cfg := testConfig(t, nil)
	s, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	var last uint64
	for range 5 {
		last = s.VersionGen().Next(time.Now())
	}
	// A version-map entry above anything issued also raises the seed.
	mapped := last + 1000
	txn := s.Begin()
	txn.Put(CFMeta, layout.VersionKey(nil, mapped), layout.VersionEntry{Key: []byte("k")}.Encode(nil))
	commit(t, txn)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s, err = Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	v, ok := mustGet(t, s, CFDefault, layout.SeqKey(layout.SeqVersion))
	if !ok || binary.BigEndian.Uint64(v) != last {
		t.Fatalf("s/version = %x, %v; want %d", v, ok, last)
	}
	if got := s.VersionGen().Last(); got != mapped {
		t.Fatalf("seed after reopen = %d, want the version-map maximum %d", got, mapped)
	}
	// A clock far in the past still yields versions above every old one.
	if n := s.VersionGen().Next(time.Unix(0, 0)); n <= mapped {
		t.Fatalf("Next after reopen = %d, want > %d", n, mapped)
	}
}

func TestVersion(t *testing.T) {
	major, minor, patch := Version()
	if major < 9 || minor < 0 || patch < 0 {
		t.Fatalf("Version() = %d.%d.%d", major, minor, patch)
	}
	t.Logf("RocksDB headers %d.%d.%d", major, minor, patch)
}
