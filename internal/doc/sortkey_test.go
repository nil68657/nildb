package doc

import (
	"bytes"
	"sort"
	"strings"
	"testing"

	kt "github.com/nil68657/nildb/internal/keyenc/keyenctest"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func ejson(t *testing.T, s string) bson.Raw {
	t.Helper()
	d, err := ParseEJSON([]byte(s))
	if err != nil {
		t.Fatalf("%s: %v", s, err)
	}
	return d
}

func TestParseSort(t *testing.T) {
	s, err := ParseSort(ejson(t, `{"a": 1, "b.c": -1, "d": {"$numberLong": "1"}, "e": -1.0}`))
	if err != nil {
		t.Fatal(err)
	}
	want := SortSpec{{"a", false}, {"b.c", true}, {"d", false}, {"e", true}}
	if len(s) != len(want) {
		t.Fatalf("got %v", s)
	}
	for i := range s {
		if s[i] != want[i] {
			t.Errorf("field %d = %v, want %v", i, s[i], want[i])
		}
	}
	if s, err := ParseSort(ejson(t, `{}`)); err != nil || len(s) != 0 {
		t.Errorf("empty sort: %v %v", s, err)
	}
	for in, want := range map[string]string{
		`{"a": 2}`:                      "ERR $sort key ordering must be 1 (for ascending) or -1 (for descending)",
		`{"a": "asc"}`:                  "ERR $sort key ordering must be 1",
		`{"a": {"$meta": "textScore"}}`: "ERR unsupported sort value {$meta: ...} in v1",
		`{"a..b": 1}`:                   "ERR FieldPath field names may not be empty strings.",
		`{"$a": 1}`:                     "ERR FieldPath field names may not start with '$'.",
	} {
		if _, err := ParseSort(ejson(t, in)); err == nil || !strings.HasPrefix(err.Error(), want) {
			t.Errorf("ParseSort(%s) = %v, want %q", in, err, want)
		}
	}
	var fs []string
	for i := 0; i <= MaxSortKeys; i++ {
		fs = append(fs, `"f`+string(rune('a'+i%26))+strings.Repeat("x", i)+`": 1`)
	}
	if _, err := ParseSort(ejson(t, "{"+strings.Join(fs, ",")+"}")); err == nil {
		t.Error("33 sort keys accepted")
	}
}

// sortDocs orders docs by SortKey under spec and returns their "n" fields.
func sortDocs(t *testing.T, spec string, docs []string) []int32 {
	t.Helper()
	s, err := ParseSort(ejson(t, spec))
	if err != nil {
		t.Fatal(err)
	}
	type row struct {
		key []byte
		n   int32
	}
	rows := make([]row, len(docs))
	for i, d := range docs {
		raw := ejson(t, d)
		n, _ := lookupField(raw, "n")
		rows[i] = row{SortKey(nil, raw, s), n.Int32()}
	}
	sort.SliceStable(rows, func(i, j int) bool { return bytes.Compare(rows[i].key, rows[j].key) < 0 })
	out := make([]int32, len(rows))
	for i, r := range rows {
		out[i] = r.n
	}
	return out
}

func TestSortKeyOrder(t *testing.T) {
	docs := []string{
		`{"n": 0, "a": 5}`,
		`{"n": 1, "a": [7, 2]}`,
		`{"n": 2}`,
		`{"n": 3, "a": null}`,
		`{"n": 4, "a": []}`,
		`{"n": 5, "a": "x"}`,
		`{"n": 6, "a": [9, "z"]}`,
		`{"n": 7, "a": 3.5}`,
	}
	cases := []struct {
		spec string
		want []int32
	}{
		// Ascending uses each array's smallest element; [] is Undefined,
		// below null and missing, which tie.
		{`{"a": 1}`, []int32{4, 2, 3, 1, 7, 0, 6, 5}},
		// Descending uses each array's largest element.
		{`{"a": -1}`, []int32{6, 5, 1, 0, 7, 2, 3, 4}},
	}
	for _, c := range cases {
		got := sortDocs(t, c.spec, docs)
		for i := range got {
			if got[i] != c.want[i] {
				t.Errorf("%s: got %v, want %v", c.spec, got, c.want)
				break
			}
		}
	}
}

func TestSortKeyCompound(t *testing.T) {
	docs := []string{
		`{"n": 0, "a": 1, "b": "y"}`,
		`{"n": 1, "a": 1, "b": "z"}`,
		`{"n": 2, "a": 0, "b": "a"}`,
		`{"n": 3, "a": {"$numberLong": "1"}, "b": "x"}`,
		`{"n": 4, "x": [{"y": 3}, {"y": 1}]}`,
	}
	got := sortDocs(t, `{"a": 1, "b": -1}`, docs)
	want := []int32{4, 2, 1, 0, 3}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("got %v, want %v", got, want)
		}
	}
	got = sortDocs(t, `{"x.y": 1}`, []string{`{"n": 0, "x": [{"y": 3}, {"y": 1}]}`, `{"n": 1, "x": {"y": 2}}`})
	if got[0] != 0 || got[1] != 1 {
		t.Fatalf("dotted sort through an array: %v", got)
	}
}

// TestSortKeyScalarsMatchCompare checks that for non-array values the key
// order is Compare's order, ascending and descending.
func TestSortKeyScalarsMatchCompare(t *testing.T) {
	g := kt.NewGen(99)
	g.Decimal = false
	asc := SortSpec{{"v", false}}
	desc := SortSpec{{"v", true}}
	for i := 0; i < 5000; i++ {
		a, b := g.Value(), g.Value()
		if a.Type == bson.TypeArray || b.Type == bson.TypeArray || a.Type == 0 || b.Type == 0 {
			continue
		}
		da := kt.DocRaw(kt.F("v", a))
		db := kt.DocRaw(kt.F("v", b))
		want := sign(Compare(a, b))
		if got := bytes.Compare(SortKey(nil, da, asc), SortKey(nil, db, asc)); got != want {
			t.Fatalf("asc %s vs %s: %d, want %d", a.DebugString(), b.DebugString(), got, want)
		}
		if got := bytes.Compare(SortKey(nil, da, desc), SortKey(nil, db, desc)); got != -want {
			t.Fatalf("desc %s vs %s: %d, want %d", a.DebugString(), b.DebugString(), got, -want)
		}
	}
}

func TestSortKeyDecimal(t *testing.T) {
	s := SortSpec{{"a", false}}
	got := sortDocs(t, `{"a": 1}`, []string{
		`{"n": 0, "a": {"$numberDecimal": "2.5"}}`,
		`{"n": 1, "a": 2}`,
		`{"n": 2, "a": {"$numberDecimal": "-1E+400"}}`,
		`{"n": 3, "a": {"x": {"$numberDecimal": "1"}}}`,
	})
	want := []int32{2, 1, 0, 3}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("got %v, want %v", got, want)
		}
	}
	if len(SortKey(nil, ejson(t, `{"a": {"$numberDecimal": "NaN"}}`), s)) == 0 {
		t.Fatal("empty key")
	}
	// SortKey appends to dst.
	pre := []byte{0xAA}
	if k := SortKey(pre, ejson(t, `{"a": 1}`), s); k[0] != 0xAA || len(k) < 2 {
		t.Fatalf("SortKey did not append: %x", k)
	}
}
