package doc

import (
	"bytes"
	"math"
	"strings"
	"testing"

	kt "github.com/nil68657/nildb/internal/keyenc/keyenctest"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// allTypes returns one document holding every BSON type, several with
// awkward values (-0, NaN, NUL in strings, old binary subtype 2).
func allTypes(t *testing.T) bson.Raw {
	oid, err := bson.ObjectIDFromHex("5f1d7a0b8c9d0e1f2a3b4c5d")
	if err != nil {
		t.Fatal(err)
	}
	d128, err := bson.ParseDecimal128("-1.2345E-10")
	if err != nil {
		t.Fatal(err)
	}
	return kt.DocRaw(
		kt.F("_id", kt.OID(oid)),
		kt.F("double", kt.Double(1.5)),
		kt.F("negzero", kt.Double(math.Copysign(0, -1))),
		kt.F("nan", kt.Double(math.NaN())),
		kt.F("inf", kt.Double(math.Inf(-1))),
		kt.F("string", kt.String("héllo\x00world")),
		kt.F("doc", kt.Doc(kt.F("b", kt.Int32(1)), kt.F("a", kt.Null()))),
		kt.F("array", kt.Array(kt.Int32(1), kt.String("x"), kt.Array())),
		kt.F("bin0", kt.Binary(0x00, []byte{0, 1, 2, 255})),
		kt.F("bin2", kt.Binary(0x02, []byte{2, 0, 0, 0, 7, 8})),
		kt.F("bin4", kt.Binary(0x04, bytes.Repeat([]byte{0xab}, 16))),
		kt.F("bin80", kt.Binary(0x80, nil)),
		kt.F("undefined", kt.Undefined()),
		kt.F("oid", kt.OID(oid)),
		kt.F("bool", kt.Bool(true)),
		kt.F("date", kt.Date(-62135596800000)),
		kt.F("null", kt.Null()),
		kt.F("regex", kt.Regex("^a.*b$", "imsx")),
		kt.F("dbptr", kt.DBPointer("db.coll", oid)),
		kt.F("js", kt.JS("function() { return 1; }")),
		kt.F("symbol", kt.Symbol("sym")),
		kt.F("jsscope", kt.CodeWScope("x + y", kt.DocRaw(kt.F("x", kt.Int32(1))))),
		kt.F("int32", kt.Int32(math.MinInt32)),
		kt.F("timestamp", kt.Timestamp(math.MaxUint32, 7)),
		kt.F("int64", kt.Int64(math.MaxInt64)),
		kt.F("decimal", kt.Decimal(d128)),
		kt.F("minkey", kt.MinKey()),
		kt.F("maxkey", kt.MaxKey()),
	)
}

func TestEJSONRoundTripEveryType(t *testing.T) {
	d := allTypes(t)
	if err := d.Validate(); err != nil {
		t.Fatal(err)
	}
	text := FormatEJSON(d)
	if text == nil {
		t.Fatal("FormatEJSON failed")
	}
	back, err := ParseEJSON(text)
	if err != nil {
		t.Fatalf("ParseEJSON(%s): %v", text, err)
	}
	if !bytes.Equal(back, d) {
		t.Fatalf("round trip changed the bytes\n in  %x\n out %x\n text %s", []byte(d), []byte(back), text)
	}
	// Each field on its own too, so a failure names the type.
	it := newIter(d)
	for {
		e, ok := it.next()
		if !ok {
			break
		}
		one := bson.Raw(buildDoc([]string{string(e.name)}, []bson.RawValue{e.value()}))
		back, err := ParseEJSON(FormatEJSON(one))
		if err != nil || !bytes.Equal(back, one) {
			t.Errorf("%s (%s) does not round trip: %s, %v", e.name, typeName(e.typ), FormatEJSON(one), err)
		}
	}
}

func TestEJSONCanonicalShapes(t *testing.T) {
	cases := []struct {
		v    bson.RawValue
		want string
	}{
		{kt.Int32(1), `{"$numberInt":"1"}`},
		{kt.Int64(1), `{"$numberLong":"1"}`},
		{kt.Double(1), `{"$numberDouble":"1.0"}`},
		{kt.Double(math.Copysign(0, -1)), `{"$numberDouble":"-0.0"}`},
		{kt.String("x"), `"x"`},
		{kt.Date(0), `{"$date":{"$numberLong":"0"}}`},
		{kt.Null(), `null`},
	}
	for _, c := range cases {
		if got := string(FormatEJSONValue(c.v)); got != c.want {
			t.Errorf("FormatEJSONValue(%s) = %s, want %s", c.v.DebugString(), got, c.want)
		}
	}
}

func TestParseEJSONRelaxed(t *testing.T) {
	d, err := ParseEJSON([]byte(`{"b": 1, "a": 2.5, "big": 3000000000, "d": {"$date": "2020-01-01T00:00:00Z"}, "b": true, "o": {"$oid": "5f1d7a0b8c9d0e1f2a3b4c5d"}}`))
	if err != nil {
		t.Fatal(err)
	}
	want := []struct {
		name string
		t    bson.Type
	}{{"b", bson.TypeInt32}, {"a", bson.TypeDouble}, {"big", bson.TypeInt64}, {"d", bson.TypeDateTime}, {"b", bson.TypeBoolean}, {"o", bson.TypeObjectID}}
	it := newIter(d)
	for i := 0; ; i++ {
		e, ok := it.next()
		if !ok {
			if i != len(want) {
				t.Fatalf("got %d fields, want %d", i, len(want))
			}
			break
		}
		if string(e.name) != want[i].name || e.typ != want[i].t {
			t.Errorf("field %d = %s %s, want %s %s", i, e.name, e.typ, want[i].name, want[i].t)
		}
	}
	date, _ := lookupField(d, "d")
	if date.DateTime() != 1577836800000 {
		t.Errorf("date = %d", date.DateTime())
	}
}

func TestParseEJSONErrors(t *testing.T) {
	for _, in := range []string{
		``, `   `, `{`, `{"a": 1} trailing`, `[1, 2]`, `"x"`, `5`, `{"a": {"$numberInt": "x"}}`,
		`{"a": {"$oid": "short"}}`, `{a: 1}`,
	} {
		_, err := ParseEJSON([]byte(in))
		if err == nil {
			t.Errorf("ParseEJSON(%q) succeeded", in)
			continue
		}
		if !strings.HasPrefix(err.Error(), "ERR invalid Extended JSON: ") {
			t.Errorf("ParseEJSON(%q) error %q lacks the prefix", in, err)
		}
	}
}

func TestFormatEJSONMalformed(t *testing.T) {
	if FormatEJSON(bson.Raw{1, 2, 3}) != nil {
		t.Error("FormatEJSON of garbage should be nil")
	}
	if FormatEJSONValue(bson.RawValue{Type: bson.TypeInt32, Value: []byte{1}}) != nil {
		t.Error("FormatEJSONValue of a truncated int32 should be nil")
	}
}
