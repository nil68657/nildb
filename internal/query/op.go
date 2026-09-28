package query

import (
	"errors"
	"strings"
	"sync/atomic"
	"time"

	"github.com/nil68657/nildb/internal/doc"
	"github.com/nil68657/nildb/internal/store"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// Mode picks the read profile of a plan run.
type Mode uint8

const (
	// OLTP is the profile of DOC.* commands: iterators fill the block
	// cache and nothing throttles them.
	OLTP Mode = iota
	// Analytic is the profile of NIL.* commands (architecture.md section
	// 7): no block-cache fill, IO_LOW rate-limiter priority, 2 MiB
	// readahead, async IO, an iterator deadline, the byte bucket and the
	// row cap.
	Analytic
)

func (m Mode) String() string {
	if m == Analytic {
		return "analytic"
	}
	return "oltp"
}

// Row is one document flowing between operators.
type Row struct {
	Doc bson.Raw
	// ID is ks(_id) of the source document. Sorts append it to their keys
	// so ties order deterministically; $group sets it to ks(group _id).
	ID []byte
	// Dist is the distance a geo source computed, in metres (or radians
	// for a legacy coordinate pair), which $geoNear writes to distanceField.
	Dist float64
}

// Operator is one pull-based plan node.
type Operator interface {
	// Next returns the next row; ok is false once the operator is
	// exhausted.
	Next() (row Row, ok bool, err error)
	// Suspend closes every open iterator. The next Next call reopens them
	// where the operator stopped, reading through the Exec's current
	// reader, so a cursor holds no RocksDB iterator between commands.
	Suspend()
	// Close releases everything. It is idempotent.
	Close()
}

// Errors a run reports. Every one is a *doc.Error, so a handler replies
// resp.Err(err.Error()).
var (
	// ErrRowLimit is the analytics row cap (nildb.analytics-max-rows).
	ErrRowLimit = &doc.Error{Code: doc.CodeBadValue, Msg: "analytics row limit reached"}
	// ErrTimeout is an expired TIMEOUT: MongoDB's MaxTimeMSExpired code.
	ErrTimeout = &doc.Error{Code: 50, Msg: "analytics timeout"}
	// ErrLeaseExpired is a read through a released or expired lease.
	ErrLeaseExpired = &doc.Error{Code: doc.CodeBadValue, Msg: "snapshot lease expired"}
)

// CodeMemoryLimit is MongoDB's QueryExceededMemoryLimitNoDiskUseAllowed.
const CodeMemoryLimit = 292

// fetchBatch is the number of rows one MultiGet reads (architecture.md
// section 7).
const fetchBatch = 64

// Stats accumulates what analytical runs read, for INFO analytics.
type Stats struct {
	Rows  atomic.Int64 // documents read
	Bytes atomic.Int64 // key plus value bytes of every iterator step and row
}

// Exec is the state one run of a plan shares across its operators: the
// reader, the read profile and the limits. One goroutine drives a run at a
// time; a cursor serialises its reads.
type Exec struct {
	r        store.Reader
	mode     Mode
	mem      *Accountant
	bucket   *Bucket
	maxRows  int64
	timeout  time.Duration
	deadline time.Time
	geoCells int
	stats    *Stats

	rows, bytes     int64 // this run
	flushR, flushB  int64 // already added to stats
	owed            int   // bytes read but not yet taken from the bucket
	checkedDeadline int64
}

func newExec(o Options, live store.Reader) *Exec {
	r := o.Reader
	if r == nil {
		r = live
	}
	return &Exec{
		r:        r,
		mode:     o.Mode,
		mem:      NewAccountant(o.MaxMem),
		bucket:   o.Bucket,
		maxRows:  o.MaxRows,
		timeout:  o.Timeout,
		geoCells: o.GeoMaxCells,
		stats:    o.Stats,
	}
}

// Rows returns the number of documents the run has read.
func (x *Exec) Rows() int64 { return x.rows }

// Bytes returns the key and value bytes the run has read.
func (x *Exec) Bytes() int64 { return x.bytes }

// startBatch arms the deadline for the next batch of rows.
func (x *Exec) startBatch() {
	if x.timeout > 0 {
		x.deadline = time.Now().Add(x.timeout)
	} else {
		x.deadline = time.Time{}
	}
}

// step charges one iterator step that is not a document (an index entry)
// to the byte bucket.
func (x *Exec) step(n int) {
	x.bytes += int64(n)
	if x.bucket != nil {
		x.owed += n
		if x.owed >= 64<<10 {
			x.bucket.Take(x.owed)
			x.owed = 0
		}
	}
}

// row charges one document read: the row cap, the bucket and, every 256
// rows, the deadline.
func (x *Exec) row(n int) error {
	x.rows++
	x.step(n)
	if x.maxRows > 0 && x.rows > x.maxRows {
		return ErrRowLimit
	}
	if !x.deadline.IsZero() && x.rows-x.checkedDeadline >= 256 {
		x.checkedDeadline = x.rows
		if time.Now().After(x.deadline) {
			return ErrTimeout
		}
	}
	return nil
}

// checkDeadline reports ErrTimeout once the batch deadline has passed. In-
// memory stages call it as they work.
func (x *Exec) checkDeadline() error {
	if !x.deadline.IsZero() && time.Now().After(x.deadline) {
		return ErrTimeout
	}
	return nil
}

// flush pays the bucket what is owed and adds this run's counts to stats.
func (x *Exec) flush() {
	if x.bucket != nil && x.owed > 0 {
		x.bucket.Take(x.owed)
		x.owed = 0
	}
	if x.stats != nil {
		x.stats.Rows.Add(x.rows - x.flushR)
		x.stats.Bytes.Add(x.bytes - x.flushB)
		x.flushR, x.flushB = x.rows, x.bytes
	}
}

// readErr maps a store error to the error a client sees.
func (x *Exec) readErr(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, store.ErrLeaseExpired):
		return ErrLeaseExpired
	case !x.deadline.IsZero() && (time.Now().After(x.deadline) || strings.Contains(err.Error(), "timed out")):
		return ErrTimeout
	}
	return err
}

// get reads one row through the run's reader.
func (x *Exec) get(cf store.CF, key []byte) ([]byte, bool, error) {
	v, ok, err := x.r.Get(cf, key)
	return v, ok, x.readErr(err)
}

// multiGet reads rows in one batched call.
func (x *Exec) multiGet(cf store.CF, keys [][]byte) ([][]byte, error) {
	v, err := x.r.MultiGet(cf, keys)
	return v, x.readErr(err)
}
