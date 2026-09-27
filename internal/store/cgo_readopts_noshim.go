//go:build nildb_no_shim

package store

import (
	"errors"

	"github.com/linxGnu/grocksdb"
)

// errNoShim explains why LowPriority does nothing in this build.
var errNoShim = errors.New("store: built with nildb_no_shim; analytical reads are not charged to the rate limiter")

// ShimActive reports whether IterOpts.LowPriority reaches RocksDB. Under
// nildb_no_shim it never does; the analytics byte bucket and semaphore
// are the only throttle.
func ShimActive() (bool, error) { return false, errNoShim }

func setIOLow(*grocksdb.ReadOptions) {}

func rateLimiterPriority(*grocksdb.ReadOptions) (int, bool) { return 0, false }
