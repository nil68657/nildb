package layout

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/nil68657/nildb/internal/keyenc"
)

// Collection elements live in sub under their collection's version, and
// sorted sets add a score index in zscore:
//
//	hash    | version (8) | field (n) |                 ->  | value (n) |
//	set     | version (8) | member (n) |                ->  (empty)
//	list    | version (8) | index (8) |                 ->  | element (n) |
//	zset    | version (8) | member (n) |                ->  | dbl(score) (8) |
//	zscore  | version (8) | dbl(score) (8) | member (n) | ->  (empty)
//
// dbl is keyenc.SortableDouble. Scores are canonicalised (-0 becomes +0)
// before encoding, so ZADD k 0 a and ZADD k -0 b order by member and
// ZCOUNT k 0 0 finds both. Callers reject NaN scores before they get here.

// SubKey appends | version | sub |, the key of a hash field or of a set or
// sorted-set member.
func SubKey(dst []byte, version uint64, sub []byte) []byte {
	dst = binary.BigEndian.AppendUint64(dst, version)
	return append(dst, sub...)
}

// SplitSubKey returns the version and the field or member of a sub key.
// ok is false for keys shorter than 8 bytes.
func SplitSubKey(k []byte) (version uint64, sub []byte, ok bool) {
	if len(k) < 8 {
		return 0, nil, false
	}
	return binary.BigEndian.Uint64(k), k[8:], true
}

// ListKey appends | version | index |, the key of a list element.
func ListKey(dst []byte, version uint64, index uint64) []byte {
	dst = binary.BigEndian.AppendUint64(dst, version)
	return binary.BigEndian.AppendUint64(dst, index)
}

// SplitListKey parses a list element key. ok is false unless k is exactly
// 16 bytes.
func SplitListKey(k []byte) (version, index uint64, ok bool) {
	if len(k) != 16 {
		return 0, 0, false
	}
	return binary.BigEndian.Uint64(k), binary.BigEndian.Uint64(k[8:]), true
}

// ScoreKey appends | version | dbl(score) | member |, the zscore index key
// of a sorted-set member. A nil member gives the lowest key for that score,
// the lower bound of a score range scan.
func ScoreKey(dst []byte, version uint64, score float64, member []byte) []byte {
	dst = binary.BigEndian.AppendUint64(dst, version)
	dst = binary.BigEndian.AppendUint64(dst, keyenc.SortableDouble(canonScore(score)))
	return append(dst, member...)
}

// SplitScoreKey parses a zscore key. It returns zeros and a nil member for
// keys shorter than 16 bytes.
func SplitScoreKey(k []byte) (version uint64, score float64, member []byte) {
	if len(k) < 16 {
		return 0, 0, nil
	}
	return binary.BigEndian.Uint64(k), keyenc.FromSortableDouble(binary.BigEndian.Uint64(k[8:])), k[16:]
}

// ScoreValue appends dbl(score), the sub value of a sorted-set member.
func ScoreValue(dst []byte, score float64) []byte {
	return binary.BigEndian.AppendUint64(dst, keyenc.SortableDouble(canonScore(score)))
}

// DecodeScoreValue parses the sub value of a sorted-set member.
func DecodeScoreValue(v []byte) (float64, error) {
	if len(v) != 8 {
		return 0, fmt.Errorf("%w: score value of %d bytes", ErrCorrupt, len(v))
	}
	return keyenc.FromSortableDouble(binary.BigEndian.Uint64(v)), nil
}

func canonScore(f float64) float64 {
	if f == 0 {
		return 0 // turns -0 into +0
	}
	return f
}

// VersionOf returns the version in the first 8 bytes of a sub or zscore
// key, or 0 when k is shorter.
func VersionOf(k []byte) uint64 {
	if len(k) < 8 {
		return 0
	}
	return binary.BigEndian.Uint64(k)
}

// VersionPrefix appends the 8-byte version prefix shared by every element
// of one collection.
func VersionPrefix(dst []byte, version uint64) []byte {
	return binary.BigEndian.AppendUint64(dst, version)
}

// VersionBounds returns [lo, hi) holding every sub or zscore key of one
// collection version: lo is the version, hi the version plus one. hi is nil
// (no upper bound) for the maximum version, which VersionGen never issues.
func VersionBounds(version uint64) (lo, hi []byte) {
	lo = VersionPrefix(nil, version)
	if version == math.MaxUint64 {
		return lo, nil
	}
	return lo, VersionPrefix(nil, version+1)
}
