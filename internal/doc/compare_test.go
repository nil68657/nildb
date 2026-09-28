package doc

import (
	"bytes"
	"math"
	"testing"

	"github.com/nil68657/nildb/internal/keyenc"
	kt "github.com/nil68657/nildb/internal/keyenc/keyenctest"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func sign(c int) int {
	switch {
	case c < 0:
		return -1
	case c > 0:
		return 1
	}
	return 0
}

// TestCompareAgreesWithKeyenc draws 20,000 random pairs and checks that
// Compare orders them exactly as bytes.Compare orders their keyenc
// encodings, the property the planner and SortKey rely on.
func TestCompareAgreesWithKeyenc(t *testing.T) {
	g := kt.NewGen(20260927)
	const pairs = 20000
	for i := 0; i < pairs; i++ {
		a, b := g.Value(), g.Value()
		if i%7 == 0 {
			b = a // exercise ties
		}
		ea, err := keyenc.Encode(nil, a)
		if err != nil {
			t.Fatalf("Encode(%v): %v", a, err)
		}
		eb, err := keyenc.Encode(nil, b)
		if err != nil {
			t.Fatalf("Encode(%v): %v", b, err)
		}
		want := bytes.Compare(ea, eb)
		if got := sign(Compare(a, b)); got != want {
			t.Fatalf("pair %d: Compare(%s, %s) = %d, keyenc says %d", i, a.DebugString(), b.DebugString(), got, want)
		}
		if got := sign(Compare(b, a)); got != -want {
			t.Fatalf("pair %d: Compare is not antisymmetric for %s, %s", i, a.DebugString(), b.DebugString())
		}
	}
}

// TestCompareAgreesWithReference covers Decimal128, which keyenc refuses,
// against keyenctest's big.Rat reference comparator.
func TestCompareAgreesWithReference(t *testing.T) {
	g := kt.NewGen(7)
	g.Decimal = true
	for i := 0; i < 20000; i++ {
		a, b := g.Value(), g.Value()
		want := kt.Compare(a, b)
		if got := sign(Compare(a, b)); got != want {
			t.Fatalf("pair %d: Compare(%s, %s) = %d, reference says %d", i, a.DebugString(), b.DebugString(), got, want)
		}
	}
}

func dec(t *testing.T, s string) bson.RawValue {
	t.Helper()
	d, err := bson.ParseDecimal128(s)
	if err != nil {
		t.Fatal(err)
	}
	return kt.Decimal(d)
}

func TestCompareNumbers(t *testing.T) {
	cases := []struct {
		a, b bson.RawValue
		want int
	}{
		{kt.Int32(1), kt.Double(1), 0},
		{kt.Int64(1), kt.Double(1), 0},
		{kt.Double(math.Copysign(0, -1)), kt.Int32(0), 0},
		{kt.Double(math.NaN()), kt.Double(math.NaN()), 0},
		{kt.Double(math.NaN()), kt.Double(math.Inf(-1)), -1},
		{kt.Double(math.NaN()), kt.Int64(math.MinInt64), -1},
		{kt.Int64(1<<53 + 1), kt.Double(1 << 53), 1},
		{kt.Int64(1<<53 + 1), kt.Double(1<<53 + 2), -1},
		{kt.Int64(math.MaxInt64), kt.Double(9223372036854775808.0), -1},
		{kt.Int64(math.MinInt64), kt.Double(-9223372036854775808.0), 0},
		{kt.Int64(math.MinInt64), kt.Double(math.Inf(-1)), 1},
		{kt.Int32(2), kt.Double(2.5), -1},
		{kt.Int32(-2), kt.Double(-2.5), 1},
		{dec(t, "0.1"), kt.Double(0.1), -1}, // double 0.1 is slightly above 1/10
		{dec(t, "0.10"), dec(t, "0.1"), 0},
		{dec(t, "1E+400"), kt.Double(math.MaxFloat64), 1},
		{dec(t, "-Infinity"), kt.Double(math.Inf(-1)), 0},
		{dec(t, "NaN"), kt.Double(math.NaN()), 0},
		{dec(t, "NaN"), dec(t, "-Infinity"), -1},
		{dec(t, "9223372036854775808"), kt.Int64(math.MaxInt64), 1},
		{dec(t, "-0"), kt.Int32(0), 0},
	}
	for _, c := range cases {
		if got := sign(Compare(c.a, c.b)); got != c.want {
			t.Errorf("Compare(%s, %s) = %d, want %d", c.a.DebugString(), c.b.DebugString(), got, c.want)
		}
		if got := sign(Compare(c.b, c.a)); got != -c.want {
			t.Errorf("Compare(%s, %s) = %d, want %d", c.b.DebugString(), c.a.DebugString(), got, -c.want)
		}
	}
}

func TestCompareTypeOrder(t *testing.T) {
	oid := bson.ObjectID{1}
	order := []bson.RawValue{
		kt.MinKey(), kt.Undefined(), kt.Null(), kt.Int32(-5), kt.String("a"),
		kt.Doc(kt.F("a", kt.Int32(1))), kt.Array(kt.Int32(1)), kt.Binary(0, []byte{1}),
		kt.OID(oid), kt.Bool(false), kt.Date(0), kt.Timestamp(1, 1), kt.Regex("a", ""),
		kt.DBPointer("db.c", oid), kt.JS("x"), kt.CodeWScope("x", kt.DocRaw()), kt.MaxKey(),
	}
	for i := range order {
		for j := range order {
			want := sign(i - j)
			if got := sign(Compare(order[i], order[j])); got != want {
				t.Errorf("Compare(%s, %s) = %d, want %d", order[i].DebugString(), order[j].DebugString(), got, want)
			}
		}
	}
	if Compare(kt.Missing(), kt.Null()) != 0 {
		t.Error("missing must compare equal to null")
	}
	if !Equal(kt.Symbol("x"), kt.String("x")) {
		t.Error("symbol and string must compare equal")
	}
}

// TestCompareMalformed checks that truncated values compare without a
// panic.
func TestCompareMalformed(t *testing.T) {
	bad := []bson.RawValue{
		{Type: bson.TypeInt32, Value: []byte{1}},
		{Type: bson.TypeDouble, Value: nil},
		{Type: bson.TypeEmbeddedDocument, Value: []byte{9, 0, 0, 0}},
		{Type: bson.TypeString, Value: []byte{1}},
		{Type: bson.TypeBinary, Value: []byte{1}},
		{Type: bson.TypeCodeWithScope, Value: []byte{1}},
		{Type: bson.TypeDateTime, Value: []byte{1, 2}},
	}
	for _, a := range bad {
		for _, b := range bad {
			_ = Compare(a, b)
		}
		_ = Compare(a, kt.Int32(1))
	}
}
