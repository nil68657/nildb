//go:build !nildb_no_shim

package store

import (
	"testing"

	"github.com/linxGnu/grocksdb"
)

func TestShimRoundTrip(t *testing.T) {
	active, why := ShimActive()
	if !active {
		t.Skipf("rate_limiter_priority shim is off: %v", why)
	}
	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()
	if p, ok := rateLimiterPriority(ro); !ok || p != ioTotal {
		t.Fatalf("default priority = %d, %v; want IO_TOTAL", p, ok)
	}
	setIOLow(ro)
	if p, ok := rateLimiterPriority(ro); !ok || p != ioLow {
		t.Fatalf("priority after setIOLow = %d, %v; want IO_LOW", p, ok)
	}
	if err := shimSelfCheck(); err != nil {
		t.Fatalf("self-check: %v", err)
	}

	// An iterator with LowPriority carries IO_LOW into RocksDB.
	s := openTest(t, nil)
	it := s.Iter(CFDoc, nil, nil, IterOpts{LowPriority: true}).(*iterator)
	defer it.Close()
	if p, ok := rateLimiterPriority(it.ro); !ok || p != ioLow {
		t.Fatalf("iterator priority = %d, %v", p, ok)
	}
	plain := s.Iter(CFDoc, nil, nil, IterOpts{}).(*iterator)
	defer plain.Close()
	if p, _ := rateLimiterPriority(plain.ro); p != ioTotal {
		t.Fatalf("OLTP iterator priority = %d, want IO_TOTAL", p)
	}
}
