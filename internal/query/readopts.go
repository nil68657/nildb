package query

import (
	"time"

	"github.com/nil68657/nildb/internal/store"
)

// analyticReadahead is the readahead of every analytical iterator.
const analyticReadahead = 2 << 20

// iterOpts returns the iterator options of the run's mode. OLTP iterators
// fill the block cache (store's zero IterOpts is the analytical profile).
// Analytical iterators skip the cache, run at IO_LOW priority through the
// store's cgo shim, read ahead 2 MiB with async IO, and carry the batch
// deadline, which store turns into RocksDB's absolute deadline; a batch
// whose deadline already passed fails with ErrTimeout.
func (x *Exec) iterOpts() (store.IterOpts, error) {
	if x.mode == OLTP {
		return store.IterOpts{FillCache: true}, nil
	}
	o := store.IterOpts{LowPriority: true, Readahead: analyticReadahead, AsyncIO: true}
	if !x.deadline.IsZero() {
		d := time.Until(x.deadline)
		if d <= 0 {
			return o, ErrTimeout
		}
		o.Deadline = max(d, time.Microsecond)
	}
	return o, nil
}

// iter opens an iterator over [lo, hi) of cf with the run's options.
// totalOrder is for geo descendant ranges, which cross the geo CF's 12-byte
// prefix; prefixProbe is for geo ancestor probes.
func (x *Exec) iter(cf store.CF, lo, hi []byte, totalOrder, prefixProbe bool) (store.Iterator, error) {
	o, err := x.iterOpts()
	if err != nil {
		return nil, err
	}
	o.TotalOrderSeek = totalOrder
	o.PrefixSameAsStart = prefixProbe
	return x.r.Iter(cf, lo, hi, o), nil
}
