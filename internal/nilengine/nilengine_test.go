//go:build nilengine

package nilengine

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
)

var cfs = []string{"default", "meta", "sub"}

// forEach runs fn once per engine in a fresh directory.
func forEach(t *testing.T, fn func(t *testing.T, kind Kind, dir string)) {
	for _, k := range []Kind{LSM, BTree, PgHeap} {
		t.Run(k.String(), func(t *testing.T) { fn(t, k, t.TempDir()) })
	}
}

func mustOpen(t *testing.T, kind Kind, dir string, names []string) *DB {
	t.Helper()
	db, err := Open(kind, dir, names, "write_buffer_size=64k;cache_bytes=4m")
	if err != nil {
		t.Fatalf("Open(%s): %v", kind, err)
	}
	return db
}

func key(i int) []byte { return []byte(fmt.Sprintf("key%05d", i)) }

func write(t *testing.T, db *DB, fill func(b *Batch)) {
	t.Helper()
	b := NewBatch()
	defer b.Destroy()
	fill(b)
	if err := db.Write(b, false); err != nil {
		t.Fatalf("Write: %v", err)
	}
}

func mustGet(t *testing.T, db *DB, snap *Snapshot, cf int, k []byte) (string, bool) {
	t.Helper()
	v, ok, err := db.GetAt(snap, cf, k)
	if err != nil {
		t.Fatalf("Get(%d, %q): %v", cf, k, err)
	}
	return string(v), ok
}

// collect scans an iterator from its first (or last) position.
func collect(t *testing.T, it *Iterator, reverse bool) []string {
	t.Helper()
	var out []string
	if reverse {
		for it.SeekToLast(); it.Valid(); it.Prev() {
			out = append(out, string(it.Key())+"="+string(it.Value()))
		}
	} else {
		for it.SeekToFirst(); it.Valid(); it.Next() {
			out = append(out, string(it.Key())+"="+string(it.Value()))
		}
	}
	if err := it.Err(); err != nil {
		t.Fatalf("iterator: %v", err)
	}
	return out
}

func TestBatchesAcrossColumnFamilies(t *testing.T) {
	forEach(t, func(t *testing.T, kind Kind, dir string) {
		db := mustOpen(t, kind, dir, cfs)
		defer db.Close()
		if db.Kind() != kind || len(db.ColumnFamilies()) != 3 {
			t.Fatalf("Kind %v, families %v", db.Kind(), db.ColumnFamilies())
		}
		write(t, db, func(b *Batch) {
			b.Put(0, []byte("k"), []byte("default"))
			b.Put(1, []byte("k"), []byte("meta"))
			b.Put(2, []byte("k"), nil)
			b.Merge(0, []byte("n"), binary.LittleEndian.AppendUint64(nil, 5))
			if b.Len() != 4 {
				t.Fatalf("Len = %d", b.Len())
			}
		})
		for cf, want := range []string{"default", "meta", ""} {
			v, ok := mustGet(t, db, nil, cf, []byte("k"))
			if !ok || v != want {
				t.Fatalf("cf %d: got %q %v, want %q", cf, v, ok, want)
			}
		}
		if v, ok, _ := db.Get(2, []byte("k")); !ok || v == nil || len(v) != 0 {
			t.Fatalf("an empty value must be found and non-nil: %v %v", v, ok)
		}
		if _, ok := mustGet(t, db, nil, 1, []byte("absent")); ok {
			t.Fatal("absent key found")
		}
		write(t, db, func(b *Batch) { b.Merge(0, []byte("n"), binary.LittleEndian.AppendUint64(nil, 2)) })
		if v, _ := mustGet(t, db, nil, 0, []byte("n")); binary.LittleEndian.Uint64([]byte(v)) != 7 {
			t.Fatalf("merged counter %x", v)
		}
		vals, err := db.MultiGet(nil, 1, [][]byte{[]byte("k"), []byte("absent"), nil})
		if err != nil || string(vals[0]) != "meta" || vals[1] != nil || vals[2] != nil {
			t.Fatalf("MultiGet = %q, %v", vals, err)
		}
		write(t, db, func(b *Batch) {
			for i := range 100 {
				b.Put(1, key(i), []byte("x"))
			}
			b.Delete(1, key(3))
			b.DeleteRange(1, key(10), key(90))
		})
		it, err := db.NewIterator(1, IterOptions{Lower: []byte("key"), Upper: []byte("kez")})
		if err != nil {
			t.Fatal(err)
		}
		defer it.Close()
		if got := collect(t, it, false); len(got) != 100-1-80 {
			t.Fatalf("%d keys after delete and delete-range: %v", len(got), got)
		}
		// A batch naming a column family that does not exist applies nothing.
		b := NewBatch()
		defer b.Destroy()
		b.Put(0, []byte("first"), []byte("v"))
		b.Put(7, []byte("k"), []byte("v"))
		if err := db.Write(b, false); !errors.Is(err, ErrInvalidArgument) {
			t.Fatalf("write to cf 7: %v", err)
		}
		if _, ok := mustGet(t, db, nil, 0, []byte("first")); ok {
			t.Fatal("part of a failed batch was applied")
		}
	})
}

func TestIterateBothDirectionsWithBounds(t *testing.T) {
	forEach(t, func(t *testing.T, kind Kind, dir string) {
		db := mustOpen(t, kind, dir, cfs)
		defer db.Close()
		write(t, db, func(b *Batch) {
			for _, k := range []string{"b", "d", "f", "h", "j"} {
				b.Put(1, []byte(k), []byte("v"+k))
			}
			b.Put(0, []byte("e"), []byte("other family"))
		})
		it, err := db.NewIterator(1, IterOptions{Lower: []byte("d"), Upper: []byte("h"), FillCache: true})
		if err != nil {
			t.Fatal(err)
		}
		defer it.Close()
		if got := collect(t, it, false); fmt.Sprint(got) != "[d=vd f=vf]" {
			t.Fatalf("forward %v", got)
		}
		if got := collect(t, it, true); fmt.Sprint(got) != "[f=vf d=vd]" {
			t.Fatalf("reverse %v", got)
		}
		at := func() string {
			if !it.Valid() {
				return "-"
			}
			return string(it.Key())
		}
		steps := []struct {
			move func()
			want string
		}{
			{func() { it.Seek([]byte("a")) }, "d"},
			{func() { it.Seek([]byte("e")) }, "f"},
			{func() { it.Seek([]byte("h")) }, "-"},
			{func() { it.SeekForPrev([]byte("z")) }, "f"},
			{func() { it.SeekForPrev([]byte("c")) }, "-"},
			{func() { it.Seek([]byte("d")) }, "d"},
			{it.Next, "f"},
			{it.Prev, "d"},
			{it.Next, "f"},
			{it.Next, "-"},
		}
		for i, s := range steps {
			s.move()
			if got := at(); got != s.want {
				t.Fatalf("step %d: at %q, want %q", i, got, s.want)
			}
		}
		open, err := db.NewIterator(1, IterOptions{})
		if err != nil {
			t.Fatal(err)
		}
		defer open.Close()
		if got := collect(t, open, true); len(got) != 5 || got[0] != "j=vj" {
			t.Fatalf("unbounded reverse %v", got)
		}
	})
}

func TestSnapshotsAndReopen(t *testing.T) {
	forEach(t, func(t *testing.T, kind Kind, dir string) {
		db := mustOpen(t, kind, dir, cfs)
		write(t, db, func(b *Batch) {
			for i := range 500 {
				b.Put(2, key(i), []byte("old"))
			}
		})
		snap, err := db.NewSnapshot()
		if err != nil {
			t.Fatal(err)
		}
		write(t, db, func(b *Batch) {
			b.DeleteRange(2, key(0), key(250))
			b.Put(2, key(499), []byte("new"))
		})
		if s2, _ := db.NewSnapshot(); s2.Seq() <= snap.Seq() {
			t.Fatalf("snapshot sequence did not grow: %d then %d", snap.Seq(), s2.Seq())
		} else {
			s2.Release()
		}
		if v, ok := mustGet(t, db, snap, 2, key(10)); !ok || v != "old" {
			t.Fatalf("snapshot read %q %v", v, ok)
		}
		if _, ok := mustGet(t, db, nil, 2, key(10)); ok {
			t.Fatal("deleted key visible at the latest state")
		}
		it, err := db.NewIterator(2, IterOptions{Snapshot: snap})
		if err != nil {
			t.Fatal(err)
		}
		snap.Release()
		if err := db.Flush(); err != nil {
			t.Fatal(err)
		}
		if err := db.CompactRange(2, nil, nil); err != nil {
			t.Fatal(err)
		}
		got := collect(t, it, false)
		it.Close()
		if len(got) != 500 || got[499] != string(key(499))+"=old" {
			t.Fatalf("iterator on a released snapshot saw %d entries, last %q", len(got), got[len(got)-1])
		}
		if n, ok := db.Property(2, "rocksdb.estimate-num-keys"); !ok || n == "" {
			t.Fatalf("property %q %v", n, ok)
		}
		if _, ok := db.Property(2, "nil.no-such-property"); ok {
			t.Fatal("unknown property reported")
		}
		if err := db.FlushWAL(true); err != nil {
			t.Fatal(err)
		}
		seq := db.LatestSeq()
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		if _, _, err := db.Get(2, key(1)); !errors.Is(err, ErrClosed) {
			t.Fatalf("Get after Close: %v", err)
		}
		if err := db.Close(); err != nil {
			t.Fatalf("second Close: %v", err)
		}

		// Reopen with the families reordered: data follows the names.
		db = mustOpen(t, kind, dir, []string{"sub", "default", "meta", "extra"})
		defer db.Close()
		// The B+ tree records the new "extra" family with a commit of its
		// own; the LSM uses no sequence number for it.
		if db.LatestSeq() < seq {
			t.Fatalf("latest sequence %d after reopen, below %d", db.LatestSeq(), seq)
		}
		it, err = db.NewIterator(0, IterOptions{})
		if err != nil {
			t.Fatal(err)
		}
		defer it.Close()
		got = collect(t, it, false)
		if len(got) != 250 || got[0] != string(key(250))+"=old" || got[249] != string(key(499))+"=new" {
			t.Fatalf("after reopen: %d entries, first %q", len(got), got[0])
		}
		if _, ok := mustGet(t, db, nil, 3, []byte("anything")); ok {
			t.Fatal("new column family is not empty")
		}
	})
}

func TestOpenErrors(t *testing.T) {
	dir := t.TempDir()
	db := mustOpen(t, LSM, dir, cfs)
	defer db.Close()
	if _, err := Open(LSM, dir, cfs, ""); !errors.Is(err, ErrBusy) {
		t.Fatalf("second open: %v", err)
	}
	if _, err := Open(Kind(9), t.TempDir(), cfs, ""); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("unknown kind: %v", err)
	}
	if _, err := Open(BTree, t.TempDir(), cfs, "page_size=1000"); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("bad option: %v", err)
	}
	if _, err := Open(BTree, t.TempDir()+"/missing", cfs, "create_if_missing=false"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing database: %v", err)
	}
	if Version() == "" {
		t.Fatal("empty version")
	}
}

// TestPgHeapHotUpdatesAndVacuum checks the PostgreSQL behaviour the other
// engines lack: updates stay HOT, a held snapshot keeps dead versions from
// VACUUM, and VACUUM (CompactRange) removes them once it is released.
func TestPgHeapHotUpdatesAndVacuum(t *testing.T) {
	db, err := Open(PgHeap, t.TempDir(), cfs, "cache_bytes=4m;autovacuum=false")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	prop := func(name string) uint64 {
		t.Helper()
		v, ok := db.Property(0, name)
		if !ok {
			t.Fatalf("property %s missing", name)
		}
		var n uint64
		if _, err := fmt.Sscan(v, &n); err != nil {
			t.Fatalf("property %s = %q: %v", name, v, err)
		}
		return n
	}
	update := func(round int) {
		write(t, db, func(b *Batch) {
			for i := range 50 {
				b.Put(0, key(i), []byte(fmt.Sprintf("round %03d", round)))
			}
		})
	}
	update(0)
	snap, err := db.NewSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	for r := 1; r <= 20; r++ {
		update(r)
	}
	if upd, hot := prop("nil.pgheap.n-tup-upd"), prop("nil.pgheap.n-tup-hot-upd"); upd != 1000 || hot == 0 {
		t.Fatalf("%d updates, %d HOT", upd, hot)
	}
	if err := db.CompactRange(0, nil, nil); err != nil {
		t.Fatal(err)
	}
	if dead := prop("nil.pgheap.n-dead-tup"); dead != 1000 {
		t.Fatalf("with a snapshot held VACUUM left %d dead tuples, want 1000", dead)
	}
	if v, _ := mustGet(t, db, snap, 0, key(3)); v != "round 000" {
		t.Fatalf("snapshot read %q", v)
	}
	snap.Release()
	if err := db.CompactRange(0, nil, nil); err != nil {
		t.Fatal(err)
	}
	if dead := prop("nil.pgheap.n-dead-tup"); dead != 0 {
		t.Fatalf("after the snapshot was released VACUUM left %d dead tuples", dead)
	}
	if v, _ := mustGet(t, db, nil, 0, key(3)); v != "round 020" {
		t.Fatalf("latest read %q", v)
	}
	if s, ok := db.Property(0, "nil.stats"); !ok || !strings.Contains(s, "HOT") {
		t.Fatalf("nil.stats = %q", s)
	}
}

func TestConcurrentReadersAndWriter(t *testing.T) {
	forEach(t, func(t *testing.T, kind Kind, dir string) {
		db := mustOpen(t, kind, dir, cfs)
		defer db.Close()
		const accounts, start = 8, 100
		write(t, db, func(b *Batch) {
			for i := range accounts {
				b.Put(1, key(i), binary.BigEndian.AppendUint64(nil, start))
			}
		})
		var wg sync.WaitGroup
		stop := make(chan struct{})
		errs := make(chan error, 4)
		for r := range 3 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					select {
					case <-stop:
						return
					default:
					}
					keys := make([][]byte, accounts)
					for i := range keys {
						keys[i] = key(i)
					}
					vals, err := db.MultiGet(nil, 1, keys)
					if err != nil {
						errs <- err
						return
					}
					sum := uint64(0)
					for _, v := range vals {
						sum += binary.BigEndian.Uint64(v)
					}
					if sum != accounts*start {
						errs <- fmt.Errorf("reader %d saw total %d", r, sum)
						return
					}
				}
			}()
		}
		bal := make([]uint64, accounts)
		for i := range bal {
			bal[i] = start
		}
		b := NewBatch()
		for n := range 2000 {
			from, to := n%accounts, (n*7+3)%accounts
			amt := bal[from] / 2
			bal[from] -= amt
			bal[to] += amt
			b.Clear()
			b.Put(1, key(from), binary.BigEndian.AppendUint64(nil, bal[from]))
			b.Put(1, key(to), binary.BigEndian.AppendUint64(nil, bal[to]))
			if err := db.Write(b, false); err != nil {
				t.Fatal(err)
			}
		}
		b.Destroy()
		close(stop)
		wg.Wait()
		close(errs)
		for err := range errs {
			t.Fatal(err)
		}
		v, _ := mustGet(t, db, nil, 1, key(0))
		if !bytes.Equal([]byte(v), binary.BigEndian.AppendUint64(nil, bal[0])) {
			t.Fatalf("final balance %x, want %d", v, bal[0])
		}
	})
}
