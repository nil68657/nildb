package keyenc

import (
	"encoding/binary"
	"math"
)

// Numeric class bytes. Every int32, int64 and double becomes TNumber, one
// class byte, and a payload of 0 or 8 bytes. The class boundaries sit at
// ±2^53 and ±2^63 so that each class holds values one Go type represents
// exactly: below 2^53 in magnitude an int64 converts to float64 without
// rounding, and from 2^53 up to 2^63 every double is an integer that fits an
// int64. Equal numbers of different BSON types therefore encode to the same
// bytes, and no value is rounded on the way in.
const (
	numNaN      byte = 0x00 // NaN, no payload
	numNegHuge  byte = 0x01 // x < -2^63 (doubles only, includes -Inf): dbl(x)
	numNegLarge byte = 0x02 // -2^63 <= x <= -2^53, integral: SortableInt64
	numNegSmall byte = 0x03 // -2^53 < x < 0: dbl(x)
	numZero     byte = 0x04 // 0, -0, 0.0, no payload
	numPosSmall byte = 0x05 // 0 < x < 2^53: dbl(x)
	numPosLarge byte = 0x06 // 2^53 <= x < 2^63: uint64(x)
	numPosHuge  byte = 0x07 // x >= 2^63 (doubles only, includes +Inf): dbl(x)
)

const (
	two53  = 1 << 53
	two63f = float64(1 << 63) // 9223372036854775808, exact as a double
)

// SortableDouble maps f to a uint64 whose unsigned order is the numeric
// order of the doubles: the sign bit of a non-negative double is set, every
// bit of a negative double is flipped. It is the dbl() of architecture.md
// section 3. It does not canonicalise -0 (dbl(-0) < dbl(+0)); callers that
// need -0 == 0, such as sorted-set scores, canonicalise first. NaNs map
// outside the range of every ordered double.
func SortableDouble(f float64) uint64 {
	b := math.Float64bits(f)
	if b>>63 == 1 {
		return ^b
	}
	return b | 1<<63
}

// FromSortableDouble inverts SortableDouble.
func FromSortableDouble(u uint64) float64 {
	if u>>63 == 1 {
		return math.Float64frombits(u &^ (1 << 63))
	}
	return math.Float64frombits(^u)
}

// SortableInt64 maps i to a uint64 whose unsigned order is the signed order
// of the int64s by flipping the sign bit.
func SortableInt64(i int64) uint64 { return uint64(i) ^ 1<<63 }

// FromSortableInt64 inverts SortableInt64.
func FromSortableInt64(u uint64) int64 { return int64(u ^ 1<<63) }

// AppendInt64 appends the full encoding (TNumber, class, payload) of i.
// int32 values go through here too.
func AppendInt64(dst []byte, i int64) []byte {
	dst = append(dst, TNumber)
	return appendInt64Payload(dst, i)
}

// AppendDouble appends the full encoding (TNumber, class, payload) of f.
// Integral doubles encode exactly like the equal int64.
func AppendDouble(dst []byte, f float64) []byte {
	dst = append(dst, TNumber)
	return appendDoublePayload(dst, f)
}

func appendInt64Payload(dst []byte, i int64) []byte {
	switch {
	case i == 0:
		return append(dst, numZero)
	case i > 0 && i < two53:
		dst = append(dst, numPosSmall)
		return binary.BigEndian.AppendUint64(dst, SortableDouble(float64(i)))
	case i > 0:
		dst = append(dst, numPosLarge)
		return binary.BigEndian.AppendUint64(dst, uint64(i))
	case i > -two53:
		dst = append(dst, numNegSmall)
		return binary.BigEndian.AppendUint64(dst, SortableDouble(float64(i)))
	default:
		dst = append(dst, numNegLarge)
		return binary.BigEndian.AppendUint64(dst, SortableInt64(i))
	}
}

func appendDoublePayload(dst []byte, f float64) []byte {
	switch {
	case f != f:
		return append(dst, numNaN)
	case f == 0:
		return append(dst, numZero)
	case f > 0 && f < two53:
		dst = append(dst, numPosSmall)
		return binary.BigEndian.AppendUint64(dst, SortableDouble(f))
	case f > 0 && f < two63f:
		dst = append(dst, numPosLarge)
		return binary.BigEndian.AppendUint64(dst, uint64(f))
	case f > 0:
		dst = append(dst, numPosHuge)
		return binary.BigEndian.AppendUint64(dst, SortableDouble(f))
	case f > -two53:
		dst = append(dst, numNegSmall)
		return binary.BigEndian.AppendUint64(dst, SortableDouble(f))
	case f >= -two63f:
		dst = append(dst, numNegLarge)
		return binary.BigEndian.AppendUint64(dst, SortableInt64(int64(f)))
	default:
		dst = append(dst, numNegHuge)
		return binary.BigEndian.AppendUint64(dst, SortableDouble(f))
	}
}

// number is a decoded numeric payload. isInt reports that i holds the value;
// otherwise f does.
type number struct {
	isInt bool
	i     int64
	f     float64
}

// decodeNumber turns a class byte and its payload back into a number and
// rejects payloads that fall outside their class, which only a corrupt key
// can contain.
func decodeNumber(class byte, p uint64) (number, bool) {
	switch class {
	case numNaN:
		return number{f: math.NaN()}, true
	case numZero:
		return number{isInt: true}, true
	case numNegHuge:
		f := FromSortableDouble(p)
		return number{f: f}, f < -two63f
	case numNegLarge:
		i := FromSortableInt64(p)
		return number{isInt: true, i: i}, i <= -two53
	case numNegSmall:
		f := FromSortableDouble(p)
		return fromSmall(f), f > -two53 && f < 0
	case numPosSmall:
		f := FromSortableDouble(p)
		return fromSmall(f), f > 0 && f < two53
	case numPosLarge:
		return number{isInt: true, i: int64(p)}, p >= two53 && p < 1<<63
	case numPosHuge:
		f := FromSortableDouble(p)
		return number{f: f}, f >= two63f
	}
	return number{}, false
}

// fromSmall picks the integer form for an integral value below 2^53 in
// magnitude, where the conversion is exact.
func fromSmall(f float64) number {
	if f == math.Trunc(f) {
		return number{isInt: true, i: int64(f)}
	}
	return number{f: f}
}

// numberPayloadLen is the payload size after a class byte, or -1 for an
// unknown class.
func numberPayloadLen(class byte) int {
	switch class {
	case numNaN, numZero:
		return 0
	case numNegHuge, numNegLarge, numNegSmall, numPosSmall, numPosLarge, numPosHuge:
		return 8
	}
	return -1
}
