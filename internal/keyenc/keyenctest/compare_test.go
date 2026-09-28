package keyenctest

import (
	"math"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
)

func dec(t *testing.T, s string) bson.RawValue {
	t.Helper()
	d, err := bson.ParseDecimal128(s)
	if err != nil {
		t.Fatal(err)
	}
	return Decimal(d)
}

func TestDecimalCompare(t *testing.T) {
	cases := []struct {
		a, b bson.RawValue
		want int
	}{
		{dec(t, "1.0"), Int32(1), 0},
		{dec(t, "-0"), Double(0), 0},
		{dec(t, "0.1"), Double(0.1), -1}, // the double 0.1 is slightly above 1/10
		{dec(t, "9007199254740993"), Int64(1<<53 + 1), 0},
		{dec(t, "9007199254740993"), Double(1 << 53), 1},
		{dec(t, "1E+400"), Double(math.MaxFloat64), 1},
		{dec(t, "1E+400"), Double(math.Inf(1)), -1},
		{dec(t, "Infinity"), Double(math.Inf(1)), 0},
		{dec(t, "-Infinity"), Int64(math.MinInt64), -1},
		{dec(t, "NaN"), Double(math.NaN()), 0},
		{dec(t, "NaN"), Double(math.Inf(-1)), -1},
		{dec(t, "-1.5"), Double(-1.5), 0},
		{dec(t, "2"), String(""), -1},
	}
	for _, c := range cases {
		if got := Compare(c.a, c.b); got != c.want {
			t.Errorf("Compare(%v, %v) = %d, want %d", c.a, c.b, got, c.want)
		}
		if got := Compare(c.b, c.a); got != -c.want {
			t.Errorf("Compare(%v, %v) = %d, want %d", c.b, c.a, got, -c.want)
		}
	}
}

// TestGenValid checks that every generated value is valid BSON and that the
// generator is a function of its seed.
func TestGenValid(t *testing.T) {
	g1, g2 := NewGen(9), NewGen(9)
	g1.Decimal, g2.Decimal = true, true
	for i := 0; i < 5000; i++ {
		v, w := g1.Value(), g2.Value()
		if v.Type != w.Type || string(v.Value) != string(w.Value) {
			t.Fatalf("draw %d differs between two generators with one seed", i)
		}
		if v.Type == 0 {
			continue // Missing
		}
		if err := v.Validate(); err != nil {
			t.Fatalf("draw %d: invalid %s: %v", i, v.Type, err)
		}
		if Compare(v, v) != 0 {
			t.Fatalf("draw %d: %v not equal to itself", i, v)
		}
	}
}
