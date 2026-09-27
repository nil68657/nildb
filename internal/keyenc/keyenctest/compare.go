// Package keyenctest holds test helpers for code that orders BSON values:
// Compare, a reference comparator written from MongoDB's documented
// comparison order without reference to keyenc's byte format; builders for
// every BSON type; and Gen, a seeded generator of values rich in ties,
// prefixes and numeric boundary cases. keyenc's property test checks
// bytes.Compare of encodings against Compare, and internal/doc checks its
// matcher comparison against the same function.
package keyenctest

import (
	"bytes"
	"cmp"
	"encoding/binary"
	"math"
	"math/big"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// Rank of each BSON type class in MongoDB's comparison order. Undefined sits
// between MinKey and Null; Symbol ranks with String; the zero Type (a
// missing field) ranks with Null.
func rank(t bson.Type) int {
	switch t {
	case bson.TypeMinKey:
		return 0
	case bson.TypeUndefined:
		return 1
	case 0, bson.TypeNull:
		return 2
	case bson.TypeDouble, bson.TypeInt32, bson.TypeInt64, bson.TypeDecimal128:
		return 3
	case bson.TypeString, bson.TypeSymbol:
		return 4
	case bson.TypeEmbeddedDocument:
		return 5
	case bson.TypeArray:
		return 6
	case bson.TypeBinary:
		return 7
	case bson.TypeObjectID:
		return 8
	case bson.TypeBoolean:
		return 9
	case bson.TypeDateTime:
		return 10
	case bson.TypeTimestamp:
		return 11
	case bson.TypeRegex:
		return 12
	case bson.TypeDBPointer:
		return 13
	case bson.TypeJavaScript:
		return 14
	case bson.TypeCodeWithScope:
		return 15
	case bson.TypeMaxKey:
		return 16
	}
	panic("keyenctest: unknown BSON type " + t.String())
}

// Compare returns -1, 0 or +1 as a sorts before, equal to, or after b in
// MongoDB's comparison order, with no collation:
//
//   - different type classes compare by class: MinKey < Undefined < Null
//     (and missing, the zero RawValue) < numbers < String/Symbol < Object <
//     Array < BinData < ObjectId < Bool < Date < Timestamp < Regex <
//     DBPointer < JavaScript < CodeWScope < MaxKey;
//   - numbers compare by exact value across int32, int64, double and
//     Decimal128; NaN equals NaN and sorts below every other number; -0
//     equals 0;
//   - strings compare bytewise;
//   - objects compare field by field: the type class of the values, then the
//     field names bytewise, then the values; a prefix sorts first;
//   - arrays compare element by element; a prefix sorts first;
//   - BinData compares length, then subtype, then bytes;
//   - Timestamp compares t then i as unsigned integers; Date as signed ms;
//   - Regex compares pattern then options; DBPointer namespace then ObjectId
//     (the order architecture.md fixes); CodeWScope code then scope.
//
// It panics on malformed BSON, which only a broken test can supply.
func Compare(a, b bson.RawValue) int {
	ra, rb := rank(a.Type), rank(b.Type)
	if ra != rb {
		return cmp.Compare(ra, rb)
	}
	switch ra {
	case 3:
		return compareNumbers(a, b)
	case 4:
		return bytes.Compare(strBytes(a), strBytes(b))
	case 5:
		return compareDocs(a.Value, b.Value, true)
	case 6:
		return compareDocs(a.Value, b.Value, false)
	case 7:
		// Length, subtype, then the bytes the length counts. For subtype 2
		// those include the inner length prefix that Binary() strips.
		if c := cmp.Compare(binLen(a), binLen(b)); c != 0 {
			return c
		}
		if c := cmp.Compare(a.Value[4], b.Value[4]); c != 0 {
			return c
		}
		return bytes.Compare(a.Value[5:], b.Value[5:])
	case 8:
		oa, ob := a.ObjectID(), b.ObjectID()
		return bytes.Compare(oa[:], ob[:])
	case 9:
		return cmp.Compare(b2i(a.Boolean()), b2i(b.Boolean()))
	case 10:
		return cmp.Compare(a.DateTime(), b.DateTime())
	case 11:
		ta, ia := a.Timestamp()
		tb, ib := b.Timestamp()
		if c := cmp.Compare(ta, tb); c != 0 {
			return c
		}
		return cmp.Compare(ia, ib)
	case 12:
		pa, oa := a.Regex()
		pb, ob := b.Regex()
		if c := cmp.Compare(pa, pb); c != 0 {
			return c
		}
		return cmp.Compare(oa, ob)
	case 13:
		na, oa := a.DBPointer()
		nb, ob := b.DBPointer()
		if c := cmp.Compare(na, nb); c != 0 {
			return c
		}
		return bytes.Compare(oa[:], ob[:])
	case 14:
		return cmp.Compare(a.JavaScript(), b.JavaScript())
	case 15:
		ca, sa := a.CodeWithScope()
		cb, sb := b.CodeWithScope()
		if c := cmp.Compare(ca, cb); c != 0 {
			return c
		}
		return compareDocs(sa, sb, true)
	}
	return 0 // MinKey, Undefined, Null, MaxKey: one value per class
}

func b2i(b bool) int {
	if b {
		return 1
	}
	return 0
}

func binLen(v bson.RawValue) int32 {
	return int32(binary.LittleEndian.Uint32(v.Value))
}

func strBytes(v bson.RawValue) []byte {
	n := int32(binary.LittleEndian.Uint32(v.Value))
	return v.Value[4 : 4+n-1]
}

// compareDocs compares two documents (names=true) or two arrays field by
// field.
func compareDocs(a, b []byte, names bool) int {
	ea, err := bson.Raw(a).Elements()
	if err != nil {
		panic(err)
	}
	eb, err := bson.Raw(b).Elements()
	if err != nil {
		panic(err)
	}
	for i := 0; ; i++ {
		switch {
		case i == len(ea) && i == len(eb):
			return 0
		case i == len(ea):
			return -1
		case i == len(eb):
			return 1
		}
		va, vb := ea[i].Value(), eb[i].Value()
		if c := cmp.Compare(rank(va.Type), rank(vb.Type)); c != 0 {
			return c
		}
		if names {
			if c := cmp.Compare(ea[i].Key(), eb[i].Key()); c != 0 {
				return c
			}
		}
		if c := Compare(va, vb); c != 0 {
			return c
		}
	}
}

// num is an exact numeric value: NaN, ±Inf, or a rational.
type num struct {
	nan bool
	inf int
	r   *big.Rat
}

func numOf(v bson.RawValue) num {
	switch v.Type {
	case bson.TypeInt32:
		return num{r: new(big.Rat).SetInt64(int64(v.Int32()))}
	case bson.TypeInt64:
		return num{r: new(big.Rat).SetInt64(v.Int64())}
	case bson.TypeDouble:
		f := v.Double()
		switch {
		case math.IsNaN(f):
			return num{nan: true}
		case math.IsInf(f, 1):
			return num{inf: 1}
		case math.IsInf(f, -1):
			return num{inf: -1}
		}
		return num{r: new(big.Rat).SetFloat64(f)}
	case bson.TypeDecimal128:
		d := v.Decimal128()
		if d.IsNaN() {
			return num{nan: true}
		}
		if inf := d.IsInf(); inf != 0 {
			return num{inf: inf}
		}
		bi, exp, err := d.BigInt()
		if err != nil {
			panic(err)
		}
		r := new(big.Rat).SetInt(bi)
		p := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(abs(exp))), nil)
		if exp >= 0 {
			return num{r: r.Mul(r, new(big.Rat).SetInt(p))}
		}
		return num{r: r.Quo(r, new(big.Rat).SetInt(p))}
	}
	panic("keyenctest: not a number: " + v.Type.String())
}

func abs(i int) int {
	if i < 0 {
		return -i
	}
	return i
}

func compareNumbers(a, b bson.RawValue) int {
	na, nb := numOf(a), numOf(b)
	switch {
	case na.nan && nb.nan:
		return 0
	case na.nan:
		return -1
	case nb.nan:
		return 1
	case na.inf != 0 || nb.inf != 0:
		return cmp.Compare(na.inf, nb.inf)
	}
	return na.r.Cmp(nb.r)
}
