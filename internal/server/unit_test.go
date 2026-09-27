package server

import (
	"bytes"
	"math"
	"sync"
	"testing"
	"time"
)

func TestHistogramBuckets(t *testing.T) {
	prev := -1
	for _, v := range []uint64{0, 1, 15, 16, 17, 31, 32, 33, 100, 1000, 1 << 20, 1<<40 + 12345, math.MaxUint64} {
		i := histBucket(v)
		if i < prev {
			t.Errorf("bucket(%d) = %d below %d", v, i, prev)
		}
		prev = i
		lo := histLow(i)
		if lo > v {
			t.Errorf("bucket %d low %d above value %d", i, lo, v)
		}
		if i+1 < histBuckets {
			if hi := histLow(i + 1); v >= hi {
				t.Errorf("value %d not below next bucket low %d", v, hi)
			}
			if lo >= 16 && float64(histLow(i+1)-lo)/float64(lo) > 1.0/16+1e-9 {
				t.Errorf("bucket %d wider than 1/16 of %d", i, lo)
			}
		}
	}
	if histBucket(math.MaxUint64) != histBuckets-1 {
		t.Errorf("max value in bucket %d, want %d", histBucket(math.MaxUint64), histBuckets-1)
	}
}

func TestHistogramQuantiles(t *testing.T) {
	var h Histogram
	if h.Quantile(0.5) != 0 {
		t.Fatal("empty histogram has a median")
	}
	for i := 1; i <= 10000; i++ {
		h.Record(time.Duration(i) * time.Microsecond)
	}
	h.Record(-time.Second) // counts as zero
	if h.Count() != 10001 {
		t.Fatalf("count %d", h.Count())
	}
	check := func(q float64, want time.Duration) {
		got := h.Quantile(q)
		if rel := math.Abs(float64(got-want)) / float64(want); rel > 0.07 {
			t.Errorf("q%.3f = %v, want about %v (off %.1f%%)", q, got, want, rel*100)
		}
	}
	check(0.5, 5*time.Millisecond)
	check(0.99, 9900*time.Microsecond)
	check(0.999, 9990*time.Microsecond)
	check(1, 10*time.Millisecond)
	h.Reset()
	if h.Count() != 0 || h.Quantile(0.99) != 0 || h.Sum() != 0 {
		t.Error("Reset left data")
	}
}

func TestHistogramConcurrent(t *testing.T) {
	var h Histogram
	var wg sync.WaitGroup
	for g := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 1000 {
				h.Record(time.Duration(g*1000 + i))
			}
		}()
	}
	wg.Wait()
	if h.Count() != 8000 {
		t.Errorf("count %d", h.Count())
	}
}

func TestCursorRingRoundTrip(t *testing.T) {
	r := newCursorRingEpoch(0xBEEF)
	p := Position{DB: 3, Kind: CursorHScan, Version: 42, Key: []byte("myhash"), Sub: []byte("field")}
	cur := r.Put(p)
	if cur == 0 {
		t.Fatal("cursor 0 issued")
	}
	p.Key[0] = 'X' // Put copied
	got, ok := r.Get(cur, 3, CursorHScan)
	if !ok || got.Version != 42 || string(got.Key) != "myhash" || string(got.Sub) != "field" || got.DB != 3 || got.Kind != CursorHScan {
		t.Fatalf("Get = %+v, %v", got, ok)
	}
	got.Sub[0] = 'Y' // Get returned a copy
	again, _ := r.Get(cur, 3, CursorHScan)
	if string(again.Sub) != "field" {
		t.Error("Get exposed the stored slice")
	}
	if cur>>61 != uint64(CursorHScan) || cur>>57&15 != 3 || uint16(cur>>16) != 0xBEEF {
		t.Errorf("cursor %x does not carry kind, db and epoch", cur)
	}
}

func TestCursorRingRejects(t *testing.T) {
	r := newCursorRingEpoch(7)
	cur := r.Put(Position{DB: 1, Kind: CursorScan, Key: []byte("k")})
	cases := []struct {
		name string
		cur  uint64
		db   uint8
		kind CursorKind
	}{
		{"zero", 0, 1, CursorScan},
		{"foreign db", cur, 2, CursorScan},
		{"wrong kind", cur, 1, CursorSScan},
		{"unknown", cur + 1, 1, CursorScan},
		{"tampered hash", cur ^ 1<<40, 1, CursorScan},
	}
	for _, c := range cases {
		if _, ok := r.Get(c.cur, c.db, c.kind); ok {
			t.Errorf("%s: cursor accepted", c.name)
		}
	}
	other := newCursorRingEpoch(8)
	for range 5 {
		other.Put(Position{DB: 1, Kind: CursorScan, Key: []byte("k")})
	}
	if _, ok := other.Get(cur, 1, CursorScan); ok {
		t.Error("cursor from another epoch accepted")
	}
	if _, ok := r.Get(cur, 1, CursorScan); !ok {
		t.Error("valid cursor refused")
	}
}

func TestCursorRingEviction(t *testing.T) {
	r := newCursorRingEpoch(1)
	first := r.Put(Position{Kind: CursorScan, Key: []byte("first")})
	for i := 1; i < RingSize; i++ {
		r.Put(Position{Kind: CursorScan, Key: []byte{byte(i), byte(i >> 8)}})
	}
	if _, ok := r.Get(first, 0, CursorScan); !ok {
		t.Fatal("cursor evicted before the ring was full")
	}
	r.Put(Position{Kind: CursorScan, Key: []byte("one more")})
	if _, ok := r.Get(first, 0, CursorScan); ok {
		t.Error("oldest cursor survived a full ring turn")
	}
	// The counter wraps at 65536 without ever issuing 0 or a live duplicate.
	seen := make(map[uint64]bool)
	for i := range 70000 {
		c := r.Put(Position{Kind: CursorZScan, DB: 15, Key: []byte("z"), Sub: []byte{byte(i)}})
		if c == 0 {
			t.Fatal("cursor 0")
		}
		if i >= 70000-RingSize {
			if seen[c] {
				t.Fatalf("duplicate live cursor %x", c)
			}
			seen[c] = true
		}
	}
}

func TestCursorRingPutPanicsOnBadKind(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("no panic for kind 0")
		}
	}()
	NewCursorRing().Put(Position{})
}

func TestCursorRingConcurrent(t *testing.T) {
	r := NewCursorRing()
	var wg sync.WaitGroup
	for g := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 500 {
				key := []byte{byte(g), byte(i), byte(i >> 8)}
				c := r.Put(Position{Kind: CursorScan, DB: uint8(g), Key: key})
				if p, ok := r.Get(c, uint8(g), CursorScan); ok && !bytes.Equal(p.Key, key) {
					t.Errorf("cursor %x returned %q, want %q", c, p.Key, key)
				}
			}
		}()
	}
	wg.Wait()
}

func TestGlobMatch(t *testing.T) {
	cases := []struct {
		pat, s string
		want   bool
	}{
		{"*", "anything", true},
		{"*", "", true},
		{"save", "save", true},
		{"SAVE", "save", true},
		{"sav?", "save", true},
		{"sav?", "sav", false},
		{"nildb.*", "nildb.lease-ttl", true},
		{"nildb.*", "maxmemory", false},
		{"*mem*", "maxmemory-policy", true},
		{"[mn]*", "maxmemory", true},
		{"[^mn]*", "maxmemory", false},
		{"[a-c]ind", "bind", true},
		{"[c-a]ind", "bind", true},
		{"\\*", "*", true},
		{"\\*", "a", false},
		{"a*b*c", "aXXbYYc", true},
		{"a*b*c", "aXXbYY", false},
		{"**x", "abcx", true},
	}
	for _, c := range cases {
		if got := globMatch(c.pat, c.s, true); got != c.want {
			t.Errorf("globMatch(%q, %q) = %v", c.pat, c.s, got)
		}
	}
	if globMatch("SAVE", "save", false) {
		t.Error("case-sensitive match folded case")
	}
}
