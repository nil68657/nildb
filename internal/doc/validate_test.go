package doc

import (
	"bytes"
	"strconv"
	"strings"
	"testing"

	kt "github.com/nil68657/nildb/internal/keyenc/keyenctest"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestValidateInsertGeneratesID(t *testing.T) {
	in := kt.DocRaw(kt.F("a", kt.Int32(1)), kt.F("b", kt.String("x")))
	out, id, err := ValidateInsert(in)
	if err != nil {
		t.Fatal(err)
	}
	if id.Type != bson.TypeObjectID {
		t.Fatalf("generated _id has type %s", id.Type)
	}
	want := kt.DocRaw(kt.F("_id", id), kt.F("a", kt.Int32(1)), kt.F("b", kt.String("x")))
	if !bytes.Equal(out, want) {
		t.Fatalf("got %s, want %s", FormatEJSON(out), FormatEJSON(want))
	}
	_, id2, _ := ValidateInsert(in)
	if bytes.Equal(id.Value, id2.Value) {
		t.Fatal("two inserts got the same ObjectId")
	}
}

func TestValidateInsertMovesID(t *testing.T) {
	in := kt.DocRaw(kt.F("a", kt.Int32(1)), kt.F("_id", kt.String("k")), kt.F("b", kt.Null()))
	out, id, err := ValidateInsert(in)
	if err != nil {
		t.Fatal(err)
	}
	want := kt.DocRaw(kt.F("_id", kt.String("k")), kt.F("a", kt.Int32(1)), kt.F("b", kt.Null()))
	if !bytes.Equal(out, want) || !Equal(id, kt.String("k")) {
		t.Fatalf("got %s id %s", FormatEJSON(out), id.DebugString())
	}
}

func TestValidateInsertKeepsLeadingID(t *testing.T) {
	in := kt.DocRaw(kt.F("_id", kt.Int32(5)), kt.F("a", kt.Int32(1)))
	out, _, err := ValidateInsert(in)
	if err != nil {
		t.Fatal(err)
	}
	if &out[0] != &in[0] {
		t.Fatal("a document that already starts with _id should be returned as is")
	}
}

func nested(depth int) bson.Raw {
	v := kt.Doc()
	for i := 2; i < depth; i++ {
		v = kt.Doc(kt.F("x", v))
	}
	return kt.DocRaw(kt.F("_id", kt.Int32(1)), kt.F("x", v))
}

func TestValidateInsertAccepts(t *testing.T) {
	for name, d := range map[string]bson.Raw{
		"depth 100":             nested(100),
		"nested dollar field":   kt.DocRaw(kt.F("a", kt.Doc(kt.F("$x", kt.Int32(1))))),
		"empty field name":      kt.DocRaw(kt.F("", kt.Int32(1))),
		"document _id":          kt.DocRaw(kt.F("_id", kt.Doc(kt.F("a", kt.Int32(1)), kt.F("b", kt.Array(kt.Int32(2)))))),
		"null _id":              kt.DocRaw(kt.F("_id", kt.Null())),
		"same name at 2 levels": kt.DocRaw(kt.F("a", kt.Doc(kt.F("a", kt.Int32(1))))),
		"repeated array values": kt.DocRaw(kt.F("a", kt.Array(kt.Int32(1), kt.Int32(1)))),
	} {
		if _, _, err := ValidateInsert(d); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
}

func manyFields(n int, dup string) bson.Raw {
	var fs []kt.Field
	for i := 0; i < n; i++ {
		fs = append(fs, kt.F("f"+strconv.Itoa(i), kt.Int32(int32(i))))
	}
	fs = append(fs, kt.F(dup, kt.Int32(0)))
	return kt.DocRaw(fs...)
}

func TestValidateInsertRefuses(t *testing.T) {
	d128, _ := bson.ParseDecimal128("1")
	big := kt.DocRaw(kt.F("s", kt.String(strings.Repeat("x", MaxDocSize))))
	cases := []struct {
		name string
		doc  bson.Raw
		want string
		code int
	}{
		{"array _id", kt.DocRaw(kt.F("_id", kt.Array())), "ERR can't use an array for _id", CodeInvalidIDField},
		{"regex _id", kt.DocRaw(kt.F("_id", kt.Regex("a", ""))), "ERR can't use a regex for _id", CodeInvalidIDField},
		{"undefined _id", kt.DocRaw(kt.F("_id", kt.Undefined())), "ERR can't use a undefined for _id", CodeInvalidIDField},
		{"minKey _id", kt.DocRaw(kt.F("_id", kt.MinKey())), "ERR can't use a minKey for _id", CodeInvalidIDField},
		{"maxKey _id", kt.DocRaw(kt.F("_id", kt.MaxKey())), "ERR can't use a maxKey for _id", CodeInvalidIDField},
		{"decimal _id", kt.DocRaw(kt.F("_id", kt.Decimal(d128))), "ERR Decimal128 is not supported in indexes or arithmetic in v1", CodeBadValue},
		{"decimal inside _id", kt.DocRaw(kt.F("_id", kt.Doc(kt.F("a", kt.Decimal(d128))))), "ERR Decimal128 is not supported", CodeBadValue},
		{"dollar inside _id", kt.DocRaw(kt.F("_id", kt.Doc(kt.F("$a", kt.Int32(1))))), "ERR _id fields may not contain '$'-prefixed fields: $a is not valid for storage.", 52},
		{"top-level dollar", kt.DocRaw(kt.F("$set", kt.Int32(1))), "ERR Document can't have $ prefixed field names: $set", CodeBadValue},
		{"dotted name", kt.DocRaw(kt.F("a.b", kt.Int32(1))), "ERR The dotted field 'a.b' in 'a.b' is not valid for storage.", 57},
		{"nested dotted name", kt.DocRaw(kt.F("a", kt.Array(kt.Doc(kt.F("x.y", kt.Int32(1)))))), "ERR The dotted field 'x.y'", 57},
		{"duplicate name", kt.DocRaw(kt.F("a", kt.Int32(1)), kt.F("a", kt.Int32(2))), "ERR duplicate field name 'a'", CodeBadValue},
		{"duplicate name past 16 fields", manyFields(40, "f3"), "ERR duplicate field name 'f3'", CodeBadValue},
		{"duplicate _id", kt.DocRaw(kt.F("_id", kt.Int32(1)), kt.F("_id", kt.Int32(2))), "ERR can't have multiple _id fields in one document", CodeInvalidIDField},
		{"depth 101", nested(101), "ERR Document exceeds maximum nesting depth of 100", 15},
		{"array depth 101", kt.DocRaw(kt.F("a", nestedArray(100))), "ERR Document exceeds maximum nesting depth of 100", 15},
		{"too large", big, "ERR object to insert too large. size in bytes: ", CodeBSONObjectTooLarge},
		{"malformed", bson.Raw{5, 0, 0, 0, 1}, "ERR malformed BSON document", CodeBadValue},
		{"truncated element", bson.Raw{9, 0, 0, 0, 0x10, 'a', 0, 1, 0}, "ERR malformed BSON document", CodeBadValue},
	}
	for _, c := range cases {
		_, _, err := ValidateInsert(c.doc)
		if err == nil || !strings.HasPrefix(err.Error(), c.want) {
			t.Errorf("%s: got %v, want %q", c.name, err, c.want)
			continue
		}
		if Code(err) != c.code {
			t.Errorf("%s: code %d, want %d", c.name, Code(err), c.code)
		}
	}
}

func nestedArray(depth int) bson.RawValue {
	v := kt.Array()
	for i := 1; i < depth; i++ {
		v = kt.Array(v)
	}
	return v
}
