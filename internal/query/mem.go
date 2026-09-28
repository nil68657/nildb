package query

import (
	"fmt"

	"github.com/nil68657/nildb/internal/doc"
)

// Accountant charges the memory that blocking stages ($group, $sort, the
// geo candidate sets, distinct values) hold against one limit per run:
// MAXMEM, 100 MiB by default for NIL.AGGREGATE and 32 MiB for DOC.*. There
// is no spill; crossing the limit fails the run with MongoDB's wording
// (architecture.md section 7). A run is driven by one goroutine at a time,
// so the Accountant takes no lock.
type Accountant struct {
	limit, used, peak int64
}

// NewAccountant returns an accountant for limit bytes; 0 means unlimited.
func NewAccountant(limit int64) *Accountant { return &Accountant{limit: limit} }

// Grow charges n bytes on behalf of stage ("$group", "$sort", ...) and
// fails once the total passes the limit.
func (a *Accountant) Grow(stage string, n int64) error {
	a.used += n
	a.peak = max(a.peak, a.used)
	if a.limit > 0 && a.used > a.limit {
		return MemLimitError(stage, a.limit)
	}
	return nil
}

// Shrink returns n bytes.
func (a *Accountant) Shrink(n int64) { a.used = max(a.used-n, 0) }

// Used returns the bytes charged now.
func (a *Accountant) Used() int64 { return a.used }

// Peak returns the most bytes charged at once.
func (a *Accountant) Peak() int64 { return a.peak }

// Limit returns the limit, 0 for none.
func (a *Accountant) Limit() int64 { return a.limit }

// MemLimitError is the error a stage reports when the run's memory limit
// is crossed: "ERR $group exceeded memory limit of N bytes (allowDiskUse
// is not supported)".
func MemLimitError(stage string, limit int64) error {
	return &doc.Error{Code: CodeMemoryLimit, Msg: fmt.Sprintf("%s exceeded memory limit of %d bytes (allowDiskUse is not supported)", stage, limit)}
}
