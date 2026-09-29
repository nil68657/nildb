package command

import (
	"testing"
	"time"

	"github.com/nil68657/nildb/internal/store"
)

func openStore(t *testing.T) *store.Store {
	t.Helper()
	if testing.Short() {
		t.Skip("opens RocksDB")
	}
	st, err := store.Open(store.Config{Dir: t.TempDir(), Engine: store.TestEngine(), BlockCacheBytes: 8 << 20, AnalyticsCacheBytes: 8 << 20, WriteBufferBytes: 32 << 20})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	return st
}

func TestCtxReaderChoice(t *testing.T) {
	st := openStore(t)
	txn := st.BeginIndexed()
	defer txn.Discard()
	txn.Put(store.CFMeta, []byte("\x00k"), []byte("batch"))

	exec := &Ctx{Store: st, Txn: txn, InExec: true}
	if v, ok, _ := exec.Reader().Get(store.CFMeta, []byte("\x00k")); !ok || string(v) != "batch" {
		t.Errorf("inside EXEC the reader does not see the batch: %q %v", v, ok)
	}

	b := st.Begin()
	b.Put(store.CFMeta, []byte("\x00k"), []byte("v1"))
	if err := b.Commit(); err != nil {
		t.Fatal(err)
	}
	snap := st.Snapshot()
	defer st.Release(snap)
	b = st.Begin()
	b.Put(store.CFMeta, []byte("\x00k"), []byte("v2"))
	if err := b.Commit(); err != nil {
		t.Fatal(err)
	}
	withSnap := &Ctx{Store: st, Txn: st.Begin(), Snap: snap}
	if v, _, _ := withSnap.Reader().Get(store.CFMeta, []byte("\x00k")); string(v) != "v1" {
		t.Errorf("snapshot reader read %q, want v1", v)
	}
	live := &Ctx{Store: st, Txn: st.Begin()}
	if v, _, _ := live.Reader().Get(store.CFMeta, []byte("\x00k")); string(v) != "v2" {
		t.Errorf("live reader read %q, want v2", v)
	}
	if (&Ctx{Now: time.UnixMilli(1234)}).NowMS() != 1234 {
		t.Error("NowMS")
	}
}

func TestCtxLockSecondPhase(t *testing.T) {
	st := openStore(t)
	held := []store.LockKey{{Kind: store.LockColl, NS: 1, Key: []byte("r")}, {Kind: store.LockDoc, NS: 1, Key: []byte("id")}}
	unlock := st.Lock(held)
	c := &Ctx{Store: st}
	c.SetHeld(held)
	u := c.Lock([]store.LockKey{{Kind: store.LockUniq, NS: 9, Key: []byte("v")}})
	u()
	c.Lock([]store.LockKey{{Kind: store.LockUniq, NS: 9, Key: []byte("w")}})
	c.ReleaseLocks() // releases the second lock the handler kept

	// The same stripe can be taken again, so both were released.
	done := make(chan struct{})
	go func() {
		st.Lock([]store.LockKey{{Kind: store.LockUniq, NS: 9, Key: []byte("v")}, {Kind: store.LockUniq, NS: 9, Key: []byte("w")}})()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("second-phase locks were not released")
	}

	func() {
		defer func() {
			if recover() == nil {
				t.Error("Lock below a held kind did not panic")
			}
		}()
		c.Lock([]store.LockKey{{Kind: store.LockColl, NS: 2, Key: []byte("r")}})
	}()
	unlock()
}
