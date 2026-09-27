package keyenctest

import (
	"math"
	"math/rand/v2"
	"strings"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// Gen produces random BSON values from small alphabets, so that a few
// thousand draws hold many equal pairs, many string prefixes, and every
// numeric boundary keyenc's class table has (±2^53, ±2^63, zero, the
// smallest subnormal, the infinities, NaN) in all three numeric types.
type Gen struct {
	r *rand.Rand
	// MaxDepth bounds object, array and scope nesting. NewGen sets 3.
	MaxDepth int
	// Decimal lets Number return Decimal128 values, which keyenc refuses.
	Decimal bool
}

// NewGen returns a generator whose output is a function of seed.
func NewGen(seed uint64) *Gen {
	return &Gen{r: rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15)), MaxDepth: 3}
}

// Rand exposes the generator's source for tests that need more draws.
func (g *Gen) Rand() *rand.Rand { return g.r }

// SpecialInts are the int64 boundary cases the generator favours.
var SpecialInts = []int64{
	0, 1, -1, 2, -2, 42,
	math.MaxInt32, math.MinInt32, math.MaxInt32 + 1, math.MinInt32 - 1,
	1<<53 - 1, 1 << 53, 1<<53 + 1, 1<<53 + 2,
	-(1<<53 - 1), -(1 << 53), -(1<<53 + 1), -(1<<53 + 2),
	1 << 62, -(1 << 62),
	1<<63 - 1024, 1<<63 - 1025, -(1<<63 - 1024),
	math.MaxInt64, math.MaxInt64 - 1, math.MinInt64, math.MinInt64 + 1,
}

// SpecialFloats are the double boundary cases the generator favours.
var SpecialFloats = []float64{
	0, math.Copysign(0, -1), 1, -1, 0.5, -0.5, 1.5, -1.5,
	math.SmallestNonzeroFloat64, -math.SmallestNonzeroFloat64,
	2.2250738585072014e-308, -2.2250738585072014e-308, // smallest normal
	math.MaxFloat64, -math.MaxFloat64,
	math.Inf(1), math.Inf(-1), math.NaN(),
	1<<53 - 1, 1 << 53, 1<<53 + 2, 1<<53 + 4,
	-(1<<53 - 1), -(1 << 53), -(1<<53 + 2), -(1<<53 + 4),
	1<<52 - 0.5, -(1<<52 - 0.5), 1<<51 + 0.5, -(1<<51 + 0.5),
	9223372036854775808, -9223372036854775808, // ±2^63
	9223372036854777856, -9223372036854777856, // next double past ±2^63
	9223372036854774784, -9223372036854774784, // last double below 2^63 in magnitude
	1 << 64, -(1 << 64), 1e300, -1e300, 1e-300, -1e-300,
	2147483647, -2147483648, 2147483648.5,
}

var (
	stringPieces = []string{"", "a", "b", "z", "ab", "\x00", "\x01", "\xff", "a\x00", "\x00\xff", "é"}
	cstrPieces   = []string{"", "a", "b", "ab", ".", "*", "\x01", "\xff"}
	fieldNames   = []string{"a", "b", "ab", "_id", "x", ""}
	oidBytes     = []byte{0x00, 0x01, 0x7f, 0xff}
	decimalLits  = []string{"0", "-0", "1", "1.0", "1.5", "-1", "0.1", "9007199254740993", "-9007199254740993", "1E+400", "NaN", "Infinity", "-Infinity", "9223372036854775808"}
)

// Value returns a random value of any BSON type except Decimal128 (unless
// Decimal is set), occasionally Missing.
func (g *Gen) Value() bson.RawValue {
	if g.r.IntN(50) == 0 {
		return Missing()
	}
	return g.value(0)
}

func (g *Gen) value(depth int) bson.RawValue {
	deep := depth < g.MaxDepth
	for {
		k := g.r.IntN(100)
		switch {
		case k < 30:
			return g.Number()
		case k < 44:
			return String(g.str(stringPieces))
		case k < 47:
			return Symbol(g.str(stringPieces))
		case k < 55:
			if deep {
				return Doc(g.fields(depth + 1)...)
			}
		case k < 63:
			if deep {
				n := g.r.IntN(4)
				vals := make([]bson.RawValue, n)
				for i := range vals {
					vals[i] = g.value(depth + 1)
				}
				return Array(vals...)
			}
		case k < 67:
			return g.binary()
		case k < 71:
			return OID(g.oid())
		case k < 74:
			return Bool(g.r.IntN(2) == 1)
		case k < 78:
			return Date(g.pickInt64([]int64{0, 1, -1, math.MaxInt64, math.MinInt64, 1700000000000, -62135596800000}))
		case k < 82:
			ts := []uint32{0, 1, 2, math.MaxUint32, 1 << 31}
			return Timestamp(ts[g.r.IntN(len(ts))], ts[g.r.IntN(len(ts))])
		case k < 85:
			return Regex(g.str(cstrPieces), g.str([]string{"", "i", "m", "s", "x"}))
		case k < 87:
			return DBPointer(g.str(cstrPieces), g.oid())
		case k < 89:
			return JS(g.str(stringPieces))
		case k < 91:
			if deep {
				return CodeWScope(g.str(stringPieces), DocRaw(g.fields(depth+1)...))
			}
		case k < 93:
			return MinKey()
		case k < 95:
			return MaxKey()
		case k < 98:
			return Null()
		default:
			return Undefined()
		}
	}
}

// Number returns an int32, int64 or double (or Decimal128 when Decimal is
// set), drawn mostly from the boundary tables and rendered in whichever
// type can hold it, so equal values of different types are common.
func (g *Gen) Number() bson.RawValue {
	if g.Decimal && g.r.IntN(10) == 0 {
		d, err := bson.ParseDecimal128(decimalLits[g.r.IntN(len(decimalLits))])
		if err != nil {
			panic(err)
		}
		return Decimal(d)
	}
	switch k := g.r.IntN(100); {
	case k < 25:
		return g.renderInt(SpecialInts[g.r.IntN(len(SpecialInts))])
	case k < 50:
		f := SpecialFloats[g.r.IntN(len(SpecialFloats))]
		if g.r.IntN(4) == 0 {
			f = math.Nextafter(f, math.Inf(1-2*g.r.IntN(2)))
		}
		return g.renderFloat(f)
	case k < 70:
		return g.renderInt(int64(g.r.IntN(7) - 3))
	case k < 85:
		i := int64(g.r.Uint64() >> g.r.IntN(64))
		if g.r.IntN(2) == 0 {
			i = -i
		}
		return g.renderInt(i)
	default:
		var f float64
		if g.r.IntN(2) == 0 {
			f = math.Float64frombits(g.r.Uint64())
		} else {
			f = g.r.NormFloat64() * math.Pow(10, float64(g.r.IntN(40)-20))
		}
		return g.renderFloat(f)
	}
}

func (g *Gen) renderInt(i int64) bson.RawValue {
	switch g.r.IntN(3) {
	case 0:
		if i == int64(int32(i)) {
			return Int32(int32(i))
		}
		return Int64(i)
	case 1:
		return Int64(i)
	default:
		return Double(float64(i))
	}
}

func (g *Gen) renderFloat(f float64) bson.RawValue {
	// An integral double inside the int64 range is sometimes sent as an
	// integer so that cross-type ties show up.
	if g.r.IntN(3) == 0 && f == math.Trunc(f) && f >= -9223372036854775808 && f < 9223372036854775808 {
		return g.renderInt(int64(f))
	}
	return Double(f)
}

func (g *Gen) pickInt64(pool []int64) int64 {
	if g.r.IntN(3) == 0 {
		return int64(g.r.Uint64())
	}
	return pool[g.r.IntN(len(pool))]
}

func (g *Gen) str(pieces []string) string {
	var b strings.Builder
	for n := g.r.IntN(4); n > 0; n-- {
		b.WriteString(pieces[g.r.IntN(len(pieces))])
	}
	return b.String()
}

func (g *Gen) fields(depth int) []Field {
	fs := make([]Field, g.r.IntN(4))
	for i := range fs {
		fs[i] = Field{Name: fieldNames[g.r.IntN(len(fieldNames))], Value: g.value(depth)}
	}
	return fs
}

func (g *Gen) oid() bson.ObjectID {
	var id bson.ObjectID
	for i := range id {
		id[i] = oidBytes[g.r.IntN(len(oidBytes))]
	}
	if g.r.IntN(2) == 0 {
		id = bson.ObjectID{} // many ties on the zero id
		id[11] = oidBytes[g.r.IntN(len(oidBytes))]
	}
	return id
}

func (g *Gen) binary() bson.RawValue {
	subtypes := []byte{0x00, 0x01, 0x02, 0x04, 0x80}
	st := subtypes[g.r.IntN(len(subtypes))]
	data := make([]byte, g.r.IntN(4))
	for i := range data {
		data[i] = oidBytes[g.r.IntN(len(oidBytes))]
	}
	if st == 0x02 {
		inner := []byte{byte(len(data)), 0, 0, 0}
		data = append(inner, data...)
	}
	return Binary(st, data)
}
