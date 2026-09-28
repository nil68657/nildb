package query

import (
	"encoding/binary"
	"math"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// num is one int32, int64 or double. Decimal128 is not a num: v1 refuses
// it in arithmetic and accumulators (scope-v1.md).
type num struct {
	kind bson.Type // TypeInt32, TypeInt64 or TypeDouble
	i    int64
	f    float64
}

func numOf(v bson.RawValue) (num, bool) {
	switch v.Type {
	case bson.TypeInt32:
		if len(v.Value) == 4 {
			return num{kind: v.Type, i: int64(int32(binary.LittleEndian.Uint32(v.Value)))}, true
		}
	case bson.TypeInt64:
		if len(v.Value) == 8 {
			return num{kind: v.Type, i: int64(binary.LittleEndian.Uint64(v.Value))}, true
		}
	case bson.TypeDouble:
		if len(v.Value) == 8 {
			return num{kind: v.Type, f: math.Float64frombits(binary.LittleEndian.Uint64(v.Value))}, true
		}
	}
	return num{}, false
}

func (n num) isInt() bool { return n.kind != bson.TypeDouble }

func (n num) float() float64 {
	if n.kind == bson.TypeDouble {
		return n.f
	}
	return float64(n.i)
}

func (n num) value() bson.RawValue {
	switch n.kind {
	case bson.TypeInt32:
		return i32(int32(n.i))
	case bson.TypeInt64:
		return i64(n.i)
	}
	return f64(n.f)
}

// widest returns the wider of two numeric kinds: int32 < int64 < double.
func widest(a, b bson.Type) bson.Type {
	rank := func(t bson.Type) int {
		switch t {
		case bson.TypeInt32:
			return 0
		case bson.TypeInt64:
			return 1
		}
		return 2
	}
	if rank(a) >= rank(b) {
		return a
	}
	return b
}

// fitInt returns an integer result of kind k, widened when it does not fit:
// an int32 result that overflows becomes an int64, as MongoDB's $add and
// $sum do.
func fitInt(k bson.Type, v int64) num {
	if k == bson.TypeInt32 && (v < math.MinInt32 || v > math.MaxInt32) {
		k = bson.TypeInt64
	}
	return num{kind: k, i: v}
}

// addInt64 and mulInt64 report overflow instead of wrapping.
func addInt64(a, b int64) (int64, bool) {
	s := a + b
	return s, (a >= 0) == (b >= 0) && (s >= 0) != (a >= 0)
}

func mulInt64(a, b int64) (int64, bool) {
	if a == 0 || b == 0 {
		return 0, false
	}
	p := a * b
	if (a == -1 && b == math.MinInt64) || (b == -1 && a == math.MinInt64) || p/b != a {
		return p, true
	}
	return p, false
}
