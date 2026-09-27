package keyenc

import (
	"bytes"
	"encoding/hex"
	"errors"
	"math"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"

	kt "github.com/nil68657/nildb/internal/keyenc/keyenctest"
)

var oid012 = bson.ObjectID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}

// goldens pins the byte format of architecture.md section 3. Each hex string
// is split at field boundaries.
var goldens = []struct {
	name string
	v    bson.RawValue
	hex  string
}{
	{"minkey", kt.MinKey(), "0a"},
	{"undefined", kt.Undefined(), "0f"},
	{"null", kt.Null(), "14"},
	{"missing", kt.Missing(), "14"},
	{"int32 0", kt.Int32(0), "1e" + "04"},
	{"double -0", kt.Double(math.Copysign(0, -1)), "1e" + "04"},
	{"NaN", kt.Double(math.NaN()), "1e" + "00"},
	{"int32 1", kt.Int32(1), "1e" + "05" + "bff0000000000000"},
	{"double 1.0", kt.Double(1), "1e" + "05" + "bff0000000000000"},
	{"int64 -1", kt.Int64(-1), "1e" + "03" + "400fffffffffffff"},
	{"smallest subnormal", kt.Double(math.SmallestNonzeroFloat64), "1e" + "05" + "8000000000000001"},
	{"int64 2^53", kt.Int64(1 << 53), "1e" + "06" + "0020000000000000"},
	{"double 2^53", kt.Double(1 << 53), "1e" + "06" + "0020000000000000"},
	{"int64 max", kt.Int64(math.MaxInt64), "1e" + "06" + "7fffffffffffffff"},
	{"int64 min", kt.Int64(math.MinInt64), "1e" + "02" + "0000000000000000"},
	{"double -2^53", kt.Double(-(1 << 53)), "1e" + "02" + "7fe0000000000000"},
	{"double 2^63", kt.Double(9223372036854775808), "1e" + "07" + "c3e0000000000000"},
	{"+Inf", kt.Double(math.Inf(1)), "1e" + "07" + "fff0000000000000"},
	{"-Inf", kt.Double(math.Inf(-1)), "1e" + "01" + "000fffffffffffff"},
	{"empty string", kt.String(""), "3c" + "00"},
	{"string with NUL", kt.String("ab\x00c"), "3c" + "6162" + "00ff" + "63" + "00"},
	{"symbol", kt.Symbol("a"), "3c" + "61" + "00"},
	{"empty object", kt.Doc(), "46" + "00"},
	{"object", kt.Doc(kt.F("a", kt.Int32(1))), "46" + "1e" + "6100" + "05bff0000000000000" + "00"},
	{"object empty name", kt.Doc(kt.F("", kt.Null())), "46" + "14" + "00" + "00"},
	{"nested", kt.Doc(kt.F("a", kt.Doc(kt.F("b", kt.Array(kt.Null()))))), "46" + "46" + "6100" + "50" + "6200" + "14" + "00" + "00" + "00"},
	{"empty array", kt.Array(), "50" + "00"},
	{"array", kt.Array(kt.Int32(1), kt.String("a")), "50" + "1e05bff0000000000000" + "3c6100" + "00"},
	{"bindata", kt.Binary(0x80, []byte{1, 2}), "5a" + "00000002" + "80" + "0102"},
	{"oid", kt.OID(oid012), "64" + "000102030405060708090a0b"},
	{"false", kt.Bool(false), "6e" + "00"},
	{"true", kt.Bool(true), "6e" + "01"},
	{"date 0", kt.Date(0), "78" + "8000000000000000"},
	{"date -1", kt.Date(-1), "78" + "7fffffffffffffff"},
	{"timestamp", kt.Timestamp(1, 2), "82" + "00000001" + "00000002"},
	{"regex", kt.Regex("a.*", "i"), "8c" + "612e2a00" + "6900"},
	{"dbpointer", kt.DBPointer("db.c", oid012), "96" + "3c" + "64622e6300" + "000102030405060708090a0b"},
	{"javascript", kt.JS("f"), "a0" + "3c" + "6600"},
	{"codewscope", kt.CodeWScope("f", kt.DocRaw(kt.F("x", kt.Bool(true)))), "aa" + "3c6600" + "46" + "6e" + "7800" + "01" + "00"},
	{"maxkey", kt.MaxKey(), "f0"},
}

func TestGoldens(t *testing.T) {
	for _, g := range goldens {
		got, err := Encode([]byte{0xAB}, g.v)
		if err != nil {
			t.Errorf("%s: %v", g.name, err)
			continue
		}
		if got[0] != 0xAB {
			t.Errorf("%s: Encode overwrote dst", g.name)
		}
		if h := hex.EncodeToString(got[1:]); h != g.hex {
			t.Errorf("%s: got %s, want %s", g.name, h, g.hex)
		}
		desc, err := EncodeDesc(nil, g.v)
		if err != nil {
			t.Fatal(err)
		}
		want, _ := hex.DecodeString(g.hex)
		invert(want)
		if !bytes.Equal(desc, want) {
			t.Errorf("%s: desc %x, want %x", g.name, desc, want)
		}
		if b := TypeOrder(g.v.Type); b != got[1] {
			t.Errorf("%s: TypeOrder = %d, encoding starts with %d", g.name, b, got[1])
		}
	}
	if got := hex.EncodeToString(EncodeMissing([]byte{1})); got != "0114" {
		t.Errorf("EncodeMissing = %s", got)
	}
}

// TestDecodeExact checks that values without numbers of other types, and
// without symbols, come back byte for byte.
func TestDecodeExact(t *testing.T) {
	for _, g := range goldens {
		switch g.name {
		case "missing", "double -0", "NaN", "double 1.0", "int64 -1", "int64 2^53", "double 2^53", "double -2^53", "symbol":
			continue
		}
		enc := mustEncode(t, g.v)
		got, rest, err := Decode(enc)
		if err != nil || len(rest) != 0 {
			t.Errorf("%s: Decode err %v rest %x", g.name, err, rest)
			continue
		}
		if got.Type != g.v.Type || !bytes.Equal(got.Value, g.v.Value) {
			t.Errorf("%s: Decode = %s %x, want %s %x", g.name, got.Type, got.Value, g.v.Type, g.v.Value)
		}
	}
}

// TestDecodeTerminatorBeforeFF covers cstrings whose 0x00 terminator is
// followed by a 0xFF byte, which must not be read as an escaped 0x00.
func TestDecodeTerminatorBeforeFF(t *testing.T) {
	ffOID := bson.ObjectID{0xFF, 0, 0xFF, 0, 0, 0, 0, 0, 0, 0, 0, 0xFF}
	for _, v := range []bson.RawValue{
		kt.Doc(kt.F("_id", kt.Timestamp(0xFFFFFFFF, 1))),
		kt.Doc(kt.F("a", kt.OID(ffOID)), kt.F("b", kt.Date(-1))),
		kt.Doc(kt.F("x", kt.Binary(0xFF, nil))),
		kt.Regex("a", "\xff"),
		kt.DBPointer("db.c", ffOID),
		kt.CodeWScope("f", kt.DocRaw(kt.F("k", kt.OID(ffOID)))),
	} {
		for _, desc := range []bool{false, true} {
			enc, dec := mustEncode(t, v), Decode
			if desc {
				enc, dec = mustEncodeDesc(t, v), DecodeDesc
			}
			got, rest, err := dec(enc)
			if err != nil || len(rest) != 0 || got.Type != v.Type || !bytes.Equal(got.Value, v.Value) {
				t.Errorf("desc=%v round trip of %s: got %s rest %x err %v", desc, show(v), show(got), rest, err)
			}
		}
	}
}

func TestDecodeSequence(t *testing.T) {
	var key []byte
	key = AppendString(key, "a\x00")
	key, _ = EncodeDesc(key, kt.Int32(5))
	key = AppendInt64(key, 1<<60)
	v, rest, err := Decode(key)
	if err != nil || v.StringValue() != "a\x00" {
		t.Fatalf("first: %v %v", v, err)
	}
	v, rest, err = DecodeDesc(rest)
	if err != nil || v.Int32() != 5 {
		t.Fatalf("second: %v %v", v, err)
	}
	v, rest, err = Decode(rest)
	if err != nil || v.Int64() != 1<<60 || len(rest) != 0 {
		t.Fatalf("third: %v %v %x", v, err, rest)
	}
	rest, err = Skip(key)
	if err != nil {
		t.Fatal(err)
	}
	if rest, err = SkipDesc(rest); err != nil {
		t.Fatal(err)
	}
	if rest, err = Skip(rest); err != nil || len(rest) != 0 {
		t.Fatalf("skip chain: %x %v", rest, err)
	}
}

func TestEncodeErrors(t *testing.T) {
	dec := kt.Decimal(bson.NewDecimal128(0x3040000000000000, 1))
	unsupported := []bson.RawValue{
		dec,
		kt.Doc(kt.F("a", kt.Int32(1)), kt.F("b", dec)),
		kt.Array(kt.Int32(1), dec),
		kt.CodeWScope("f", kt.DocRaw(kt.F("x", dec))),
		kt.Doc(kt.F("a", kt.Array(kt.Doc(kt.F("d", dec))))),
	}
	for _, v := range unsupported {
		out, err := Encode([]byte("pre"), v)
		if !errors.Is(err, ErrUnsupported) {
			t.Errorf("Encode(%v) err = %v, want ErrUnsupported", v, err)
		}
		if string(out) != "pre" {
			t.Errorf("Encode error left %q in dst", out)
		}
		if _, err := EncodeDesc(nil, v); !errors.Is(err, ErrUnsupported) {
			t.Errorf("EncodeDesc(%v) err = %v", v, err)
		}
	}
	if ErrUnsupported.Error() != "Decimal128 is not supported in indexes or arithmetic in v1" {
		t.Errorf("ErrUnsupported text changed: %q", ErrUnsupported)
	}

	deep := kt.Int32(1)
	for i := 0; i < maxDepth+1; i++ {
		deep = kt.Array(deep)
	}
	malformed := []bson.RawValue{
		{Type: bson.TypeDouble, Value: make([]byte, 7)},
		{Type: bson.TypeInt32, Value: make([]byte, 8)},
		{Type: bson.TypeString, Value: []byte{5, 0, 0, 0, 'a', 0}},
		{Type: bson.TypeString, Value: []byte{2, 0, 0, 0, 'a', 'b'}},
		{Type: bson.TypeString, Value: []byte{0, 0, 0, 0}},
		{Type: bson.TypeEmbeddedDocument, Value: []byte{6, 0, 0, 0, 0}},
		{Type: bson.TypeEmbeddedDocument, Value: []byte{5, 0, 0, 0, 1}},
		{Type: bson.TypeEmbeddedDocument, Value: []byte{8, 0, 0, 0, 0x10, 'a', 0, 0}}, // truncated int32
		{Type: bson.TypeEmbeddedDocument, Value: []byte{8, 0, 0, 0, 0x14, 'a', 0, 0}}, // unknown type 0x14
		{Type: bson.TypeEmbeddedDocument, Value: []byte{7, 0, 0, 0, 0x0A, 'a', 0}},    // missing terminator
		{Type: bson.TypeBoolean, Value: []byte{2}},
		{Type: bson.TypeBinary, Value: []byte{3, 0, 0, 0, 0, 1}},
		{Type: bson.TypeRegex, Value: []byte{'a', 0}},
		{Type: bson.TypeNull, Value: []byte{0}},
		{Type: 0, Value: []byte{1}},
		{Type: 0x14, Value: nil},
		{Type: bson.TypeObjectID, Value: make([]byte, 11)},
		{Type: bson.TypeCodeWithScope, Value: []byte{5, 0, 0, 0, 0}},
		kt.DBPointer("db\x00c", oid012),
		deep,
	}
	for _, v := range malformed {
		if _, err := Encode(nil, v); !errors.Is(err, ErrMalformed) {
			t.Errorf("Encode(type %#x %x) err = %v, want ErrMalformed", byte(v.Type), v.Value, err)
		}
	}
}

func TestDecodeCorrupt(t *testing.T) {
	bad := []string{
		"",
		"01",                   // not a type byte
		"3c61",                 // string without terminator
		"3c6100ff",             // escaped NUL then end of input
		"46",                   // object without terminator
		"461e6100",             // field without payload
		"46146100ff0000",       // field type byte 0xff after a null field
		"50",                   // array without terminator
		"5a00000005" + "80",    // BinData shorter than its length
		"64000102",             // short ObjectId
		"6e02",                 // bool other than 0 or 1
		"78000000",             // short date
		"8c6100",               // regex without its options
		"96" + "1e04",          // DBPointer without its string
		"a0" + "14",            // JavaScript without its string
		"aa" + "3c6600" + "14", // CodeWScope without its scope object
		"f1",                   // not a type byte
	}
	for _, h := range bad {
		b, err := hex.DecodeString(h)
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := Decode(b); !errors.Is(err, ErrCorrupt) {
			t.Errorf("Decode(%s) err = %v, want ErrCorrupt", h, err)
		}
	}
	for _, h := range []string{"", "01", "3c61", "46", "50", "5a00000005", "96", "aa3c6600"} {
		b, _ := hex.DecodeString(h)
		if _, err := Skip(b); !errors.Is(err, ErrCorrupt) {
			t.Errorf("Skip(%s) err = %v, want ErrCorrupt", h, err)
		}
	}
}

func TestNext(t *testing.T) {
	cases := []struct{ in, want string }{
		{"", ""},
		{"ff", ""},
		{"ffff", ""},
		{"01", "02"},
		{"01ff", "02"},
		{"01fe", "01ff"},
		{"00ffff", "01"},
		{"0000", "0001"},
		{"7fffffff", "80"},
	}
	for _, c := range cases {
		in, _ := hex.DecodeString(c.in)
		orig := bytes.Clone(in)
		got := Next(in)
		if c.want == "" {
			if got != nil {
				t.Errorf("Next(%s) = %x, want nil", c.in, got)
			}
			continue
		}
		if h := hex.EncodeToString(got); h != c.want {
			t.Errorf("Next(%s) = %s, want %s", c.in, h, c.want)
		}
		if !bytes.Equal(in, orig) {
			t.Errorf("Next modified its input")
		}
		if bytes.Compare(got, in) <= 0 {
			t.Errorf("Next(%s) not greater than its input", c.in)
		}
		ext := append(bytes.Clone(in), 0xFF, 0xFF)
		if bytes.Compare(ext, got) >= 0 {
			t.Errorf("Next(%s) = %s does not bound %x", c.in, c.want, ext)
		}
	}
}

// TestValueRange checks that the range for "leading field equals v" holds
// every key whose first value equals v and nothing else, for strings whose
// NUL extensions share v's bytes as a prefix.
func TestValueRange(t *testing.T) {
	tails := [][]byte{AppendInt64(nil, 1), mustEncode(t, kt.MinKey()), mustEncode(t, kt.MaxKey()), mustEncodeDesc(t, kt.MinKey()), mustEncodeDesc(t, kt.MaxKey())}
	strs := []string{"", "a", "a\x00", "a\x00\x00", "a\x00b", "a\x01", "b", "\x00"}
	for _, desc := range []bool{false, true} {
		enc := func(s string) []byte {
			if desc {
				return mustEncodeDesc(t, kt.String(s))
			}
			return mustEncode(t, kt.String(s))
		}
		for _, v := range strs {
			lo, hi := ValueRange(enc(v))
			for _, other := range strs {
				for _, tail := range tails {
					key := append(enc(other), tail...)
					in := bytes.Compare(key, lo) >= 0 && bytes.Compare(key, hi) < 0
					if in != (other == v) {
						t.Errorf("desc=%v ValueRange(%q) contains %q+%x = %v", desc, v, other, tail, in)
					}
				}
			}
		}
	}
}
