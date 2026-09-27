package keyenc

import (
	"bytes"
	"encoding/binary"
	"math"
	"math/rand/v2"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"

	kt "github.com/nil68657/nildb/internal/keyenc/keyenctest"
)

// numericLadder lists numbers in strictly ascending groups; the values
// inside one group are equal and must encode to identical bytes. class is
// the numeric class byte every member of the group must carry.
var numericLadder = []struct {
	class byte
	vals  []bson.RawValue
}{
	{numNaN, []bson.RawValue{kt.Double(math.NaN()), kt.Double(math.Float64frombits(0x7FF0000000000001)), kt.Double(math.Float64frombits(0xFFF8000000000000))}},
	{numNegHuge, []bson.RawValue{kt.Double(math.Inf(-1))}},
	{numNegHuge, []bson.RawValue{kt.Double(-math.MaxFloat64)}},
	{numNegHuge, []bson.RawValue{kt.Double(-(1 << 64))}},
	{numNegHuge, []bson.RawValue{kt.Double(-9223372036854777856)}}, // next double below -2^63
	{numNegLarge, []bson.RawValue{kt.Int64(math.MinInt64), kt.Double(-9223372036854775808)}},
	{numNegLarge, []bson.RawValue{kt.Int64(math.MinInt64 + 1)}},
	{numNegLarge, []bson.RawValue{kt.Int64(-(1<<63 - 1024)), kt.Double(-(1<<63 - 1024))}},
	{numNegLarge, []bson.RawValue{kt.Int64(-(1<<53 + 2)), kt.Double(-(1<<53 + 2))}},
	{numNegLarge, []bson.RawValue{kt.Int64(-(1<<53 + 1))}},
	{numNegLarge, []bson.RawValue{kt.Int64(-(1 << 53)), kt.Double(-(1 << 53))}},
	{numNegSmall, []bson.RawValue{kt.Int64(-(1<<53 - 1)), kt.Double(-(1<<53 - 1))}},
	{numNegSmall, []bson.RawValue{kt.Double(-(1<<52 - 0.5))}},
	{numNegSmall, []bson.RawValue{kt.Int32(math.MinInt32), kt.Int64(math.MinInt32), kt.Double(math.MinInt32)}},
	{numNegSmall, []bson.RawValue{kt.Double(-1.5)}},
	{numNegSmall, []bson.RawValue{kt.Int32(-1), kt.Int64(-1), kt.Double(-1)}},
	{numNegSmall, []bson.RawValue{kt.Double(-0.5)}},
	{numNegSmall, []bson.RawValue{kt.Double(-math.SmallestNonzeroFloat64)}},
	{numZero, []bson.RawValue{kt.Int32(0), kt.Int64(0), kt.Double(0), kt.Double(math.Copysign(0, -1))}},
	{numPosSmall, []bson.RawValue{kt.Double(math.SmallestNonzeroFloat64)}},
	{numPosSmall, []bson.RawValue{kt.Double(2.2250738585072014e-308)}},
	{numPosSmall, []bson.RawValue{kt.Double(0.5)}},
	{numPosSmall, []bson.RawValue{kt.Int32(1), kt.Int64(1), kt.Double(1)}},
	{numPosSmall, []bson.RawValue{kt.Double(1.5)}},
	{numPosSmall, []bson.RawValue{kt.Int32(math.MaxInt32), kt.Int64(math.MaxInt32), kt.Double(math.MaxInt32)}},
	{numPosSmall, []bson.RawValue{kt.Double(1<<52 - 0.5)}},
	{numPosSmall, []bson.RawValue{kt.Int64(1<<53 - 1), kt.Double(1<<53 - 1)}},
	{numPosLarge, []bson.RawValue{kt.Int64(1 << 53), kt.Double(1 << 53)}},
	{numPosLarge, []bson.RawValue{kt.Int64(1<<53 + 1)}},
	{numPosLarge, []bson.RawValue{kt.Int64(1<<53 + 2), kt.Double(1<<53 + 2)}},
	{numPosLarge, []bson.RawValue{kt.Int64(1<<53 + 3)}},
	{numPosLarge, []bson.RawValue{kt.Int64(1<<63 - 1024), kt.Double(1<<63 - 1024)}},
	{numPosLarge, []bson.RawValue{kt.Int64(1<<63 - 1023)}},
	{numPosLarge, []bson.RawValue{kt.Int64(math.MaxInt64)}},
	{numPosHuge, []bson.RawValue{kt.Double(9223372036854775808)}}, // 2^63
	{numPosHuge, []bson.RawValue{kt.Double(9223372036854777856)}}, // next double above 2^63
	{numPosHuge, []bson.RawValue{kt.Double(1 << 64)}},
	{numPosHuge, []bson.RawValue{kt.Double(math.MaxFloat64)}},
	{numPosHuge, []bson.RawValue{kt.Double(math.Inf(1))}},
}

func TestNumericLadder(t *testing.T) {
	type flat struct {
		group int
		v     bson.RawValue
		enc   []byte
	}
	var all []flat
	for g, step := range numericLadder {
		first := mustEncode(t, step.vals[0])
		for _, v := range step.vals {
			enc := mustEncode(t, v)
			if !bytes.Equal(enc, first) {
				t.Errorf("group %d: %s encodes %x, %s encodes %x", g, show(step.vals[0]), first, show(v), enc)
			}
			if enc[0] != TNumber || enc[1] != step.class {
				t.Errorf("%s: type/class %x %x, want %x %x", show(v), enc[0], enc[1], TNumber, step.class)
			}
			wantLen := 10
			if step.class == numNaN || step.class == numZero {
				wantLen = 2
			}
			if len(enc) != wantLen {
				t.Errorf("%s: %d bytes, want %d", show(v), len(enc), wantLen)
			}
			all = append(all, flat{g, v, enc})
		}
	}
	for _, a := range all {
		for _, b := range all {
			want := sign(a.group - b.group)
			if got := sign(bytes.Compare(a.enc, b.enc)); got != want {
				t.Errorf("%s vs %s: bytes order %d, want %d", show(a.v), show(b.v), got, want)
			}
			if ref := sign(kt.Compare(a.v, b.v)); ref != want {
				t.Errorf("reference disagrees with the ladder on %s vs %s: %d, want %d", show(a.v), show(b.v), ref, want)
			}
		}
	}
}

// TestAppendHelpers checks that AppendInt64, AppendDouble and AppendString
// write what Encode writes for the matching BSON value.
func TestAppendHelpers(t *testing.T) {
	for _, step := range numericLadder {
		for _, v := range step.vals {
			want := mustEncode(t, v)
			var got []byte
			switch v.Type {
			case bson.TypeInt32:
				got = AppendInt64([]byte("p"), int64(v.Int32()))
			case bson.TypeInt64:
				got = AppendInt64([]byte("p"), v.Int64())
			default:
				got = AppendDouble([]byte("p"), v.Double())
			}
			if !bytes.Equal(got[1:], want) || got[0] != 'p' {
				t.Errorf("append helper for %s: %x, want p+%x", show(v), got, want)
			}
		}
	}
	for _, s := range []string{"", "a", "\x00", "a\x00b\x00", "\xff\x00\xff"} {
		want := mustEncode(t, kt.String(s))
		if got := AppendString(nil, s); !bytes.Equal(got, want) {
			t.Errorf("AppendString(%q) = %x, want %x", s, got, want)
		}
	}
}

func TestSortableDouble(t *testing.T) {
	r := rand.New(rand.NewPCG(5, 6))
	vals := []float64{math.Inf(-1), -math.MaxFloat64, -1, -math.SmallestNonzeroFloat64, math.Copysign(0, -1), 0, math.SmallestNonzeroFloat64, 1, math.MaxFloat64, math.Inf(1)}
	for i := 0; i < 2000; i++ {
		f := math.Float64frombits(r.Uint64())
		if !math.IsNaN(f) {
			vals = append(vals, f)
		}
	}
	for _, a := range vals {
		if back := FromSortableDouble(SortableDouble(a)); math.Float64bits(back) != math.Float64bits(a) {
			t.Fatalf("FromSortableDouble(SortableDouble(%v)) = %v", a, back)
		}
	}
	for i := 0; i < 20000; i++ {
		a, b := vals[r.IntN(len(vals))], vals[r.IntN(len(vals))]
		ua, ub := SortableDouble(a), SortableDouble(b)
		switch {
		case a < b && ua >= ub, a > b && ua <= ub:
			t.Fatalf("SortableDouble order: %v %v -> %x %x", a, b, ua, ub)
		case a == b && a != 0 && ua != ub:
			t.Fatalf("SortableDouble equal: %v -> %x %x", a, ua, ub)
		}
	}
	// dbl() itself keeps -0 below +0; the numeric class table is what
	// makes them equal in keys.
	if SortableDouble(math.Copysign(0, -1)) >= SortableDouble(0) {
		t.Errorf("SortableDouble(-0) should sort just below SortableDouble(+0)")
	}
	if got := SortableDouble(1); got != 0xBFF0000000000000 {
		t.Errorf("SortableDouble(1) = %x", got)
	}
}

func TestSortableInt64(t *testing.T) {
	r := rand.New(rand.NewPCG(7, 8))
	vals := []int64{math.MinInt64, math.MinInt64 + 1, -1, 0, 1, math.MaxInt64 - 1, math.MaxInt64}
	for i := 0; i < 2000; i++ {
		vals = append(vals, int64(r.Uint64()))
	}
	for _, a := range vals {
		if FromSortableInt64(SortableInt64(a)) != a {
			t.Fatalf("round trip %d", a)
		}
		for j := 0; j < 10; j++ {
			b := vals[r.IntN(len(vals))]
			if (a < b) != (SortableInt64(a) < SortableInt64(b)) {
				t.Fatalf("order %d %d", a, b)
			}
		}
	}
	if SortableInt64(math.MinInt64) != 0 || SortableInt64(math.MaxInt64) != math.MaxUint64 {
		t.Errorf("SortableInt64 endpoints wrong")
	}
}

// TestDecodeNumberTypes pins the BSON type Decode chooses for numbers.
func TestDecodeNumberTypes(t *testing.T) {
	cases := []struct {
		in   bson.RawValue
		want bson.RawValue
	}{
		{kt.Double(1), kt.Int32(1)},
		{kt.Int64(1), kt.Int32(1)},
		{kt.Double(math.Copysign(0, -1)), kt.Int32(0)},
		{kt.Double(-1.5), kt.Double(-1.5)},
		{kt.Int64(math.MaxInt32 + 1), kt.Int64(math.MaxInt32 + 1)},
		{kt.Int32(math.MinInt32), kt.Int32(math.MinInt32)},
		{kt.Double(1 << 53), kt.Int64(1 << 53)},
		{kt.Int64(math.MinInt64), kt.Int64(math.MinInt64)},
		{kt.Double(-9223372036854775808), kt.Int64(math.MinInt64)},
		{kt.Double(9223372036854775808), kt.Double(9223372036854775808)},
		{kt.Double(math.Inf(-1)), kt.Double(math.Inf(-1))},
		{kt.Double(math.SmallestNonzeroFloat64), kt.Double(math.SmallestNonzeroFloat64)},
	}
	for _, c := range cases {
		got, _, err := Decode(mustEncode(t, c.in))
		if err != nil {
			t.Fatal(err)
		}
		if got.Type != c.want.Type || !bytes.Equal(got.Value, c.want.Value) {
			t.Errorf("Decode(Encode(%s)) = %s, want %s", show(c.in), show(got), show(c.want))
		}
	}
	got, _, err := Decode(mustEncode(t, kt.Double(math.NaN())))
	if err != nil || got.Type != bson.TypeDouble || !math.IsNaN(math.Float64frombits(binary.LittleEndian.Uint64(got.Value))) {
		t.Errorf("Decode NaN = %s, %v", show(got), err)
	}
}

// TestDecodeRejectsOutOfClass feeds payloads that sit in the wrong numeric
// class, which Encode never writes.
func TestDecodeRejectsOutOfClass(t *testing.T) {
	bad := [][]byte{
		binary.BigEndian.AppendUint64([]byte{TNumber, numPosSmall}, SortableDouble(-1)),
		binary.BigEndian.AppendUint64([]byte{TNumber, numPosSmall}, SortableDouble(1<<53)),
		binary.BigEndian.AppendUint64([]byte{TNumber, numNegSmall}, SortableDouble(-(1 << 53))),
		binary.BigEndian.AppendUint64([]byte{TNumber, numPosLarge}, 1<<53-1),
		binary.BigEndian.AppendUint64([]byte{TNumber, numPosLarge}, 1<<63),
		binary.BigEndian.AppendUint64([]byte{TNumber, numNegLarge}, SortableInt64(-(1<<53 - 1))),
		binary.BigEndian.AppendUint64([]byte{TNumber, numPosHuge}, SortableDouble(1<<62)),
		binary.BigEndian.AppendUint64([]byte{TNumber, numNegHuge}, SortableDouble(-(1 << 62))),
		binary.BigEndian.AppendUint64([]byte{TNumber, numPosSmall}, SortableDouble(math.NaN())),
		{TNumber, 0x08},
		{TNumber, numPosSmall, 0x01},
		{TNumber},
	}
	for _, b := range bad {
		if _, _, err := Decode(b); err != ErrCorrupt {
			t.Errorf("Decode(%x) err = %v, want ErrCorrupt", b, err)
		}
	}
}
