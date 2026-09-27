package keyenc

import (
	"bytes"
	"math"
	"slices"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	kt "github.com/nil68657/nildb/internal/keyenc/keyenctest"
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

func mustEncode(t testing.TB, v bson.RawValue) []byte {
	t.Helper()
	b, err := Encode(nil, v)
	if err != nil {
		t.Fatalf("Encode(%v): %v", v, err)
	}
	return b
}

func mustEncodeDesc(t testing.TB, v bson.RawValue) []byte {
	t.Helper()
	b, err := EncodeDesc(nil, v)
	if err != nil {
		t.Fatalf("EncodeDesc(%v): %v", v, err)
	}
	return b
}

func show(v bson.RawValue) string {
	if v.Type == 0 {
		return "<missing>"
	}
	return v.Type.String() + " " + v.String()
}

type encoded struct {
	v    bson.RawValue
	asc  []byte
	desc []byte
}

// pool draws n values from the generator and encodes each both ways.
func pool(t testing.TB, seed uint64, n int) []encoded {
	g := kt.NewGen(seed)
	out := make([]encoded, n)
	for i := range out {
		v := g.Value()
		out[i] = encoded{v: v, asc: mustEncode(t, v), desc: mustEncodeDesc(t, v)}
	}
	return out
}

// TestPropertyOrder checks 100,000 pairs: bytes.Compare of the encodings
// must agree with the reference comparator in both directions, and for
// compound keys whose fields mix directions.
func TestPropertyOrder(t *testing.T) {
	const pairs = 100_000
	start := time.Now()
	vals := pool(t, 1, 4000)
	byType := map[byte][]int{}
	for i, e := range vals {
		byType[e.asc[0]] = append(byType[e.asc[0]], i)
	}
	g := kt.NewGen(2)
	r := g.Rand()
	pick := func() (int, int) {
		i := r.IntN(len(vals))
		if r.IntN(2) == 0 {
			same := byType[vals[i].asc[0]]
			return i, same[r.IntN(len(same))]
		}
		return i, r.IntN(len(vals))
	}
	var ties, compound int
	for n := 0; n < pairs; n++ {
		i, j := pick()
		a, b := vals[i], vals[j]
		want := sign(kt.Compare(a.v, b.v))
		if want == 0 {
			ties++
		}
		if got := sign(bytes.Compare(a.asc, b.asc)); got != want {
			t.Fatalf("ascending: Compare(%s, %s) = %d, encodings order %d\n%x\n%x", show(a.v), show(b.v), want, got, a.asc, b.asc)
		}
		if want == 0 && !bytes.Equal(a.asc, b.asc) {
			t.Fatalf("equal values %s and %s encode differently:\n%x\n%x", show(a.v), show(b.v), a.asc, b.asc)
		}
		// A descending value followed by another value, as in every
		// non-unique index entry (see TestDescStringAtKeyEnd for why).
		tail := AppendInt64(nil, 7)
		da := append(slices.Clip(a.desc), tail...)
		db := append(slices.Clip(b.desc), tail...)
		if got := sign(bytes.Compare(da, db)); got != -want {
			t.Fatalf("descending: Compare(%s, %s) = %d, encodings order %d\n%x\n%x", show(a.v), show(b.v), want, got, da, db)
		}

		// Compound key: (a, x) against (b, y), each field in a random
		// direction, closed by an ascending ks(_id)-like value.
		k, l := pick()
		x, y := vals[k], vals[l]
		dir1, dir2 := 1-2*r.IntN(2), 1-2*r.IntN(2)
		key := func(first, second encoded) []byte {
			var out []byte
			if dir1 > 0 {
				out = append(out, first.asc...)
			} else {
				out = append(out, first.desc...)
			}
			if dir2 > 0 {
				out = append(out, second.asc...)
			} else {
				out = append(out, second.desc...)
			}
			return append(out, tail...)
		}
		wantC := dir1 * want
		if want == 0 {
			wantC = dir2 * sign(kt.Compare(x.v, y.v))
			compound++
		}
		if got := sign(bytes.Compare(key(a, x), key(b, y))); got != wantC {
			t.Fatalf("compound dirs (%d,%d): (%s, %s) vs (%s, %s): want %d, got %d", dir1, dir2, show(a.v), show(x.v), show(b.v), show(y.v), wantC, got)
		}
	}
	elapsed := time.Since(start)
	t.Logf("%d pairs, %d ties, %d compound ties decided by the second field, %v", pairs, ties, compound, elapsed)
	if ties < pairs/50 {
		t.Errorf("only %d ties in %d pairs; the generator no longer exercises equality", ties, pairs)
	}
	if !raceEnabled && elapsed > 5*time.Second {
		t.Errorf("property test took %v, budget is 5s", elapsed)
	}
}

// TestSortedPool sorts values with the reference comparator and checks that
// neighbouring encodings are in the same order, which also exercises
// transitivity across long chains of ties.
func TestSortedPool(t *testing.T) {
	vals := pool(t, 3, 5000)
	slices.SortStableFunc(vals, func(a, b encoded) int { return kt.Compare(a.v, b.v) })
	for i := 1; i < len(vals); i++ {
		a, b := vals[i-1], vals[i]
		want := sign(kt.Compare(a.v, b.v))
		if got := sign(bytes.Compare(a.asc, b.asc)); got != want {
			t.Fatalf("sorted neighbours %s, %s: reference %d, bytes %d", show(a.v), show(b.v), want, got)
		}
	}
}

// TestDecodeRoundTrip checks that Decode returns a value equal under the
// reference comparator that re-encodes to the same bytes, with the input's
// remaining bytes returned untouched, in both directions; and that Skip
// agrees with Decode on where the encoding ends.
func TestDecodeRoundTrip(t *testing.T) {
	vals := pool(t, 4, 5000)
	tail := []byte{0x01, 0x02, 0xFF}
	exact := map[bson.Type]bool{
		bson.TypeString: true, bson.TypeBinary: true, bson.TypeObjectID: true,
		bson.TypeBoolean: true, bson.TypeDateTime: true, bson.TypeTimestamp: true,
		bson.TypeRegex: true, bson.TypeDBPointer: true, bson.TypeJavaScript: true,
		bson.TypeMinKey: true, bson.TypeMaxKey: true, bson.TypeNull: true,
		bson.TypeUndefined: true, bson.TypeInt32: true,
	}
	for _, e := range vals {
		for _, dir := range []struct {
			name   string
			enc    []byte
			decode func([]byte) (bson.RawValue, []byte, error)
			skip   func([]byte) ([]byte, error)
			encode func([]byte, bson.RawValue) ([]byte, error)
		}{
			{"asc", e.asc, Decode, Skip, Encode},
			{"desc", e.desc, DecodeDesc, SkipDesc, EncodeDesc},
		} {
			in := append(slices.Clip(dir.enc), tail...)
			got, rest, err := dir.decode(in)
			if err != nil {
				t.Fatalf("%s decode %s from %x: %v", dir.name, show(e.v), in, err)
			}
			if !bytes.Equal(rest, tail) {
				t.Fatalf("%s decode %s: rest %x, want %x", dir.name, show(e.v), rest, tail)
			}
			if err := got.Validate(); err != nil {
				t.Fatalf("%s decode %s gave invalid BSON %v: %v", dir.name, show(e.v), got, err)
			}
			if kt.Compare(got, e.v) != 0 {
				t.Fatalf("%s decode %s gave unequal %s", dir.name, show(e.v), show(got))
			}
			again, err := dir.encode(nil, got)
			if err != nil || !bytes.Equal(again, dir.enc) {
				t.Fatalf("%s re-encode of %s: %x (%v), want %x", dir.name, show(got), again, err, dir.enc)
			}
			if exact[e.v.Type] && (got.Type != e.v.Type || !bytes.Equal(got.Value, e.v.Value)) {
				t.Fatalf("%s decode %s gave %s", dir.name, show(e.v), show(got))
			}
			skipped, err := dir.skip(in)
			if err != nil || !bytes.Equal(skipped, tail) {
				t.Fatalf("%s skip %s: %x (%v), want %x", dir.name, show(e.v), skipped, err, tail)
			}
		}
	}
}

// TestDescStringAtKeyEnd pins the one place EncodeDesc is not
// order-preserving on its own: a descending string that ends the key is a
// byte prefix of the same string extended by 0x00, so it sorts first even
// though it is the smaller string. Any value after it fixes the order.
func TestDescStringAtKeyEnd(t *testing.T) {
	short := mustEncodeDesc(t, kt.String("a"))
	long := mustEncodeDesc(t, kt.String("a\x00"))
	if !bytes.HasPrefix(long, short) {
		t.Fatalf("expected %x to be a prefix of %x", short, long)
	}
	tail := AppendInt64(nil, 1)
	a := append(slices.Clip(short), tail...)
	b := append(slices.Clip(long), tail...)
	if bytes.Compare(a, b) <= 0 {
		t.Fatalf("with a following value, descending \"a\" must sort after \"a\\x00\": %x vs %x", a, b)
	}
}

// TestReferenceComparator spot-checks the reference against MongoDB's
// documented order, so a bug there cannot hide a matching bug in keyenc.
func TestReferenceComparator(t *testing.T) {
	oid := bson.ObjectID{}
	ascending := []bson.RawValue{
		kt.MinKey(),
		kt.Undefined(),
		kt.Null(),
		kt.Double(math.NaN()),
		kt.Double(-1e300),
		kt.Int64(-1 << 63),
		kt.Int32(-1),
		kt.Int32(0),
		kt.Double(0.5),
		kt.Int64(1<<53 + 1),
		kt.Double(1e300),
		kt.String(""),
		kt.Symbol("a"),
		kt.String("a\x00"),
		kt.String("b"),
		kt.Doc(),
		kt.Doc(kt.F("a", kt.Int32(5))),
		kt.Doc(kt.F("b", kt.Int32(1))),    // same value type, name b > a
		kt.Doc(kt.F("a", kt.String("x"))), // value type string > number wins over names
		kt.Array(),
		kt.Array(kt.Int32(1)),
		kt.Array(kt.Int32(1), kt.Int32(0)),
		kt.Array(kt.Int32(2)),
		kt.Binary(0x80, nil),
		kt.Binary(0x00, []byte{9}), // longer wins before subtype
		kt.Binary(0x01, []byte{0}),
		kt.OID(oid),
		kt.Bool(false),
		kt.Bool(true),
		kt.Date(-1),
		kt.Date(0),
		kt.Timestamp(0, 5),
		kt.Timestamp(1, 0),
		kt.Regex("a", "i"),
		kt.Regex("a", "m"),
		kt.Regex("ab", ""),
		kt.DBPointer("a", oid),
		kt.JS("f"),
		kt.CodeWScope("f", kt.DocRaw()),
		kt.MaxKey(),
	}
	for i := range ascending {
		for j := range ascending {
			if got, want := sign(kt.Compare(ascending[i], ascending[j])), sign(i-j); got != want {
				t.Errorf("Compare(%s, %s) = %d, want %d", show(ascending[i]), show(ascending[j]), got, want)
			}
		}
	}
	equal := [][2]bson.RawValue{
		{kt.Int32(1), kt.Double(1)},
		{kt.Int64(1 << 53), kt.Double(1 << 53)},
		{kt.Double(0), kt.Double(math.Copysign(0, -1))},
		{kt.Missing(), kt.Null()},
		{kt.String("x"), kt.Symbol("x")},
		{kt.Double(math.NaN()), kt.Double(math.Float64frombits(0xFFF8000000000001))},
	}
	for _, p := range equal {
		if c := kt.Compare(p[0], p[1]); c != 0 {
			t.Errorf("Compare(%s, %s) = %d, want 0", show(p[0]), show(p[1]), c)
		}
	}
}
