package store

import (
	"github.com/nil68657/nildb/internal/layout"
)

// i64Add is the nildb.i64add merge operator on CFDefault: operands and the
// stored value are 8-byte little-endian signed integers
// (layout.AppendCount / layout.DecodeCount), and a merge adds them with
// two's-complement wraparound. A missing base value counts as zero. Any
// operand or value of another length fails the merge, which RocksDB
// reports as a Corruption error on read.
type i64Add struct{}

// Name implements grocksdb.MergeOperator.
func (i64Add) Name() string { return "nildb.i64add" }

// FullMerge implements grocksdb.MergeOperator.
func (i64Add) FullMerge(key, existing []byte, operands [][]byte) ([]byte, bool) {
	var sum int64
	if len(existing) > 0 {
		v, err := layout.DecodeCount(existing)
		if err != nil {
			return nil, false
		}
		sum = v
	}
	for _, op := range operands {
		v, err := layout.DecodeCount(op)
		if err != nil {
			return nil, false
		}
		sum += v
	}
	return layout.AppendCount(nil, sum), true
}

// PartialMerge implements grocksdb.PartialMerger; the add is associative,
// so two operands collapse into one.
func (i64Add) PartialMerge(key, left, right []byte) ([]byte, bool) {
	a, err := layout.DecodeCount(left)
	if err != nil {
		return nil, false
	}
	b, err := layout.DecodeCount(right)
	if err != nil {
		return nil, false
	}
	return layout.AppendCount(nil, a+b), true
}
