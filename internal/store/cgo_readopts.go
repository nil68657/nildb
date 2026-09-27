//go:build !nildb_no_shim

package store

// grocksdb v1.11.1 has no wrapper for rocksdb_readoptions_set_rate_limiter_priority,
// but the linked Homebrew c.h (11.8.1) declares it and its getter. This
// shim reads the C pointer out of grocksdb.ReadOptions' unexported first
// field and calls them. A self-check runs once before first use: the field
// must be named c, sit at offset 0 and be a pointer, the pointer must
// round-trip readahead_size through the C getter, and the priority must
// round-trip IO_TOTAL -> IO_LOW. If any check fails the shim turns itself
// off and LowPriority does nothing; build with -tags nildb_no_shim to
// remove it entirely.

/*
#cgo LDFLAGS: -lrocksdb
#include <stdint.h>
#include <rocksdb/c.h>
*/
import "C"

import (
	"errors"
	"fmt"
	"reflect"
	"sync"
	"unsafe"

	"github.com/linxGnu/grocksdb"
)

// RocksDB Env::IOPriority values.
const (
	ioLow   = 0 // Env::IO_LOW
	ioTotal = 4 // Env::IO_TOTAL, the default: reads are not charged
)

var (
	shimOnce sync.Once
	shimErr  error
)

// readOptsPtr returns the rocksdb_readoptions_t* inside ro.
func readOptsPtr(ro *grocksdb.ReadOptions) *C.rocksdb_readoptions_t {
	return *(**C.rocksdb_readoptions_t)(unsafe.Pointer(ro))
}

func shimSelfCheck() error {
	t := reflect.TypeFor[grocksdb.ReadOptions]()
	if t.NumField() == 0 {
		return errors.New("grocksdb.ReadOptions has no fields")
	}
	f := t.Field(0)
	if f.Name != "c" || f.Offset != 0 || f.Type.Kind() != reflect.Pointer {
		return fmt.Errorf("grocksdb.ReadOptions field 0 is %s %s at offset %d, want pointer c at 0", f.Name, f.Type, f.Offset)
	}
	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()
	p := readOptsPtr(ro)
	if p == nil {
		return errors.New("grocksdb.ReadOptions.c is nil")
	}
	const probe = 0x5A5A5A
	ro.SetReadaheadSize(probe)
	if got := uint64(C.rocksdb_readoptions_get_readahead_size(p)); got != probe {
		return fmt.Errorf("readahead_size read back %#x through the shim pointer, want %#x", got, probe)
	}
	if got := int(C.rocksdb_readoptions_get_rate_limiter_priority(p)); got != ioTotal {
		return fmt.Errorf("default rate_limiter_priority %d, want IO_TOTAL (%d)", got, ioTotal)
	}
	C.rocksdb_readoptions_set_rate_limiter_priority(p, ioLow)
	if got := int(C.rocksdb_readoptions_get_rate_limiter_priority(p)); got != ioLow {
		return fmt.Errorf("rate_limiter_priority read back %d, want IO_LOW (%d)", got, ioLow)
	}
	return nil
}

func shimCheck() error {
	shimOnce.Do(func() { shimErr = shimSelfCheck() })
	return shimErr
}

// ShimActive reports whether IterOpts.LowPriority reaches RocksDB, and if
// not, why. It is false under the nildb_no_shim build tag.
func ShimActive() (bool, error) {
	if err := shimCheck(); err != nil {
		return false, err
	}
	return true, nil
}

// setIOLow charges reads through ro to the rate limiter at IO_LOW.
func setIOLow(ro *grocksdb.ReadOptions) {
	if shimCheck() != nil {
		return
	}
	C.rocksdb_readoptions_set_rate_limiter_priority(readOptsPtr(ro), ioLow)
}

// rateLimiterPriority reads ro's priority back; ok is false when the shim
// is off.
func rateLimiterPriority(ro *grocksdb.ReadOptions) (prio int, ok bool) {
	if shimCheck() != nil {
		return 0, false
	}
	return int(C.rocksdb_readoptions_get_rate_limiter_priority(readOptsPtr(ro))), true
}
