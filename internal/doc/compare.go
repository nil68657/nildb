package doc

import (
	"bytes"
	"cmp"
	"encoding/binary"
	"math"
	"math/big"

	"github.com/nil68657/nildb/internal/keyenc"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// Compare returns -1, 0 or +1 as a sorts before, equal to, or after b in
// MongoDB's comparison order, without collation. It agrees with
// bytes.Compare of the keyenc encodings for every value keyenc encodes:
//
//   - type classes order MinKey < Undefined < Null (and missing, the zero
//     RawValue) < numbers < String/Symbol < Object < Array < BinData <
//     ObjectId < Bool < Date < Timestamp < Regex < DBPointer < JavaScript <
//     CodeWScope < MaxKey;
//   - int32, int64, double and Decimal128 compare by exact value; NaN equals
//     NaN and sorts below every other number; -0 equals 0;
//   - strings compare bytewise; objects field by field (value type class,
//     then name, then value); arrays element by element; a prefix first;
//   - BinData by length, subtype, bytes; Regex by pattern then options;
//     DBPointer by namespace then ObjectId; CodeWScope by code then scope.
//
// Decimal128 values are compared exactly through big.Rat, since big.Float
// cannot hold a decimal fraction such as 0.1. Malformed values never panic;
// they compare by their raw bytes.
func Compare(a, b bson.RawValue) int {
	ra, rb := keyenc.TypeOrder(a.Type), keyenc.TypeOrder(b.Type)
	if ra != rb {
		return cmp.Compare(ra, rb)
	}
	switch ra {
	case keyenc.TNumber:
		na, oka := numberOf(a)
		nb, okb := numberOf(b)
		if !oka || !okb {
			return bytes.Compare(a.Value, b.Value)
		}
		return compareNumbers(na, nb)
	case keyenc.TString:
		sa, _ := cstr32(a.Value)
		sb, _ := cstr32(b.Value)
		return bytes.Compare(sa, sb)
	case keyenc.TObject:
		return compareDocs(a.Value, b.Value, true)
	case keyenc.TArray:
		return compareDocs(a.Value, b.Value, false)
	case keyenc.TBinData:
		if len(a.Value) < 5 || len(b.Value) < 5 {
			return bytes.Compare(a.Value, b.Value)
		}
		la := int32(binary.LittleEndian.Uint32(a.Value))
		lb := int32(binary.LittleEndian.Uint32(b.Value))
		if c := cmp.Compare(la, lb); c != 0 {
			return c
		}
		if c := cmp.Compare(a.Value[4], b.Value[4]); c != 0 {
			return c
		}
		return bytes.Compare(a.Value[5:], b.Value[5:])
	case keyenc.TOID, keyenc.TBool:
		return bytes.Compare(a.Value, b.Value)
	case keyenc.TDate:
		if len(a.Value) != 8 || len(b.Value) != 8 {
			return bytes.Compare(a.Value, b.Value)
		}
		return cmp.Compare(int64(binary.LittleEndian.Uint64(a.Value)), int64(binary.LittleEndian.Uint64(b.Value)))
	case keyenc.TTimestamp:
		if len(a.Value) != 8 || len(b.Value) != 8 {
			return bytes.Compare(a.Value, b.Value)
		}
		// t is the high and i the low half of the little-endian uint64.
		return cmp.Compare(binary.LittleEndian.Uint64(a.Value), binary.LittleEndian.Uint64(b.Value))
	case keyenc.TRegex:
		pa, oa, _ := regexParts(a)
		pb, ob, _ := regexParts(b)
		if c := cmp.Compare(pa, pb); c != 0 {
			return c
		}
		return cmp.Compare(oa, ob)
	case keyenc.TDBPointer:
		na, _ := cstr32(a.Value)
		nb, _ := cstr32(b.Value)
		if c := bytes.Compare(na, nb); c != 0 {
			return c
		}
		return bytes.Compare(tail(a.Value, 12), tail(b.Value, 12))
	case keyenc.TCode:
		sa, _ := cstr32(a.Value)
		sb, _ := cstr32(b.Value)
		return bytes.Compare(sa, sb)
	case keyenc.TCodeWScope:
		ca, sa, oka := codeWScopeParts(a.Value)
		cb, sb, okb := codeWScopeParts(b.Value)
		if !oka || !okb {
			return bytes.Compare(a.Value, b.Value)
		}
		if c := bytes.Compare(ca, cb); c != 0 {
			return c
		}
		return compareDocs(sa, sb, true)
	}
	return 0 // MinKey, Undefined, Null, MaxKey: one value per class
}

// Equal reports whether a and b compare equal under Compare: the same type
// class and the same value, so 1, int64(1) and 1.0 are equal and a missing
// value equals Null.
func Equal(a, b bson.RawValue) bool { return Compare(a, b) == 0 }

func tail(b []byte, n int) []byte {
	if len(b) < n {
		return b
	}
	return b[len(b)-n:]
}

func codeWScopeParts(v []byte) (code, scope []byte, ok bool) {
	if len(v) < 4 {
		return nil, nil, false
	}
	code, ok = cstr32(v[4:])
	if !ok {
		return nil, nil, false
	}
	return code, v[4+4+len(code)+1:], true
}

// compareDocs compares two documents (names=true) or two arrays element by
// element.
func compareDocs(a, b []byte, names bool) int {
	ia, ib := newIter(a), newIter(b)
	for {
		ea, oka := ia.next()
		eb, okb := ib.next()
		switch {
		case !oka && !okb:
			if ia.bad || ib.bad {
				return bytes.Compare(a, b)
			}
			return 0
		case !oka:
			return -1
		case !okb:
			return 1
		}
		if c := cmp.Compare(keyenc.TypeOrder(ea.typ), keyenc.TypeOrder(eb.typ)); c != 0 {
			return c
		}
		if names {
			if c := bytes.Compare(ea.name, eb.name); c != 0 {
				return c
			}
		}
		if c := Compare(ea.value(), eb.value()); c != 0 {
			return c
		}
	}
}

// number is one numeric BSON value.
type number struct {
	kind bson.Type // TypeInt32, TypeInt64, TypeDouble or TypeDecimal128
	i    int64     // int32 and int64
	f    float64   // double
	d    bson.Decimal128
}

func numberOf(v bson.RawValue) (number, bool) {
	switch v.Type {
	case bson.TypeInt32:
		if len(v.Value) != 4 {
			return number{}, false
		}
		return number{kind: v.Type, i: int64(int32(binary.LittleEndian.Uint32(v.Value)))}, true
	case bson.TypeInt64:
		if len(v.Value) != 8 {
			return number{}, false
		}
		return number{kind: v.Type, i: int64(binary.LittleEndian.Uint64(v.Value))}, true
	case bson.TypeDouble:
		if len(v.Value) != 8 {
			return number{}, false
		}
		return number{kind: v.Type, f: math.Float64frombits(binary.LittleEndian.Uint64(v.Value))}, true
	case bson.TypeDecimal128:
		if len(v.Value) != 16 {
			return number{}, false
		}
		l := binary.LittleEndian.Uint64(v.Value)
		h := binary.LittleEndian.Uint64(v.Value[8:])
		return number{kind: v.Type, d: bson.NewDecimal128(h, l)}, true
	}
	return number{}, false
}

func (n number) isInt() bool { return n.kind == bson.TypeInt32 || n.kind == bson.TypeInt64 }

func (n number) isNaN() bool {
	switch n.kind {
	case bson.TypeDouble:
		return math.IsNaN(n.f)
	case bson.TypeDecimal128:
		return n.d.IsNaN()
	}
	return false
}

func (n number) isZero() bool {
	switch n.kind {
	case bson.TypeDouble:
		return n.f == 0
	case bson.TypeDecimal128:
		bi, _, err := n.d.BigInt()
		return err == nil && bi.Sign() == 0
	}
	return n.i == 0
}

// value returns n as a BSON value of its own type.
func (n number) value() bson.RawValue {
	switch n.kind {
	case bson.TypeInt32:
		return int32Value(int32(n.i))
	case bson.TypeInt64:
		return int64Value(n.i)
	case bson.TypeDouble:
		return doubleValue(n.f)
	}
	h, l := n.d.GetBytes()
	v := binary.LittleEndian.AppendUint64(nil, l)
	return bson.RawValue{Type: bson.TypeDecimal128, Value: binary.LittleEndian.AppendUint64(v, h)}
}

// float returns n as the nearest double.
func (n number) float() float64 {
	switch n.kind {
	case bson.TypeDouble:
		return n.f
	case bson.TypeDecimal128:
		r, special := n.rat()
		switch special {
		case specNaN:
			return math.NaN()
		case specPosInf:
			return math.Inf(1)
		case specNegInf:
			return math.Inf(-1)
		}
		f, _ := r.Float64()
		return f
	}
	return float64(n.i)
}

func compareNumbers(a, b number) int {
	if a.kind == bson.TypeDecimal128 || b.kind == bson.TypeDecimal128 {
		return compareRat(a, b)
	}
	switch {
	case a.isInt() && b.isInt():
		return cmp.Compare(a.i, b.i)
	case !a.isInt() && !b.isInt():
		return compareFloats(a.f, b.f)
	case a.isInt():
		return compareIntFloat(a.i, b.f)
	}
	return -compareIntFloat(b.i, a.f)
}

// compareFloats orders NaN below every number and equal to itself.
func compareFloats(x, y float64) int {
	xn, yn := math.IsNaN(x), math.IsNaN(y)
	switch {
	case xn && yn:
		return 0
	case xn:
		return -1
	case yn:
		return 1
	case x < y:
		return -1
	case x > y:
		return 1
	}
	return 0
}

// compareIntFloat compares an int64 with a double exactly.
func compareIntFloat(i int64, f float64) int {
	switch {
	case math.IsNaN(f):
		return 1
	case f >= 9223372036854775808.0: // 2^63, above every int64
		return -1
	case f < -9223372036854775808.0:
		return 1
	}
	t := math.Trunc(f) // exact, and within the int64 range
	ti := int64(t)
	if c := cmp.Compare(i, ti); c != 0 {
		return c
	}
	frac := f - t
	switch {
	case frac > 0:
		return -1
	case frac < 0:
		return 1
	}
	return 0
}

const (
	specFinite = iota
	specNaN
	specPosInf
	specNegInf
)

// rat returns n as an exact rational, or the special class it belongs to.
func (n number) rat() (*big.Rat, int) {
	switch n.kind {
	case bson.TypeInt32, bson.TypeInt64:
		return new(big.Rat).SetInt64(n.i), specFinite
	case bson.TypeDouble:
		switch {
		case math.IsNaN(n.f):
			return nil, specNaN
		case math.IsInf(n.f, 1):
			return nil, specPosInf
		case math.IsInf(n.f, -1):
			return nil, specNegInf
		}
		return new(big.Rat).SetFloat64(n.f), specFinite
	}
	if n.d.IsNaN() {
		return nil, specNaN
	}
	switch n.d.IsInf() {
	case 1:
		return nil, specPosInf
	case -1:
		return nil, specNegInf
	}
	bi, exp, err := n.d.BigInt()
	if err != nil {
		return nil, specNaN
	}
	r := new(big.Rat).SetInt(bi)
	if exp == 0 {
		return r, specFinite
	}
	e := exp
	if e < 0 {
		e = -e
	}
	p := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(e)), nil)
	if exp > 0 {
		return r.Mul(r, new(big.Rat).SetInt(p)), specFinite
	}
	return r.Quo(r, new(big.Rat).SetInt(p)), specFinite
}

func specRank(s int) int {
	switch s {
	case specNaN:
		return 0
	case specNegInf:
		return 1
	case specPosInf:
		return 3
	}
	return 2
}

func compareRat(a, b number) int {
	ra, sa := a.rat()
	rb, sb := b.rat()
	if sa != specFinite || sb != specFinite {
		return cmp.Compare(specRank(sa), specRank(sb))
	}
	return ra.Cmp(rb)
}

// truncInt64 truncates a number toward zero into an int64. ok is false for
// NaN, infinities and values outside the int64 range.
func truncInt64(n number) (int64, bool) {
	switch n.kind {
	case bson.TypeInt32, bson.TypeInt64:
		return n.i, true
	case bson.TypeDouble:
		if math.IsNaN(n.f) || n.f >= 9223372036854775808.0 || n.f < -9223372036854775808.0 {
			return 0, false
		}
		return int64(math.Trunc(n.f)), true
	}
	r, s := n.rat()
	if s != specFinite {
		return 0, false
	}
	q := new(big.Int).Quo(r.Num(), r.Denom())
	if !q.IsInt64() {
		return 0, false
	}
	return q.Int64(), true
}
