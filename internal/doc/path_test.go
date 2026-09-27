package doc

import (
	"strings"
	"testing"
)

func TestLookup(t *testing.T) {
	d := ejson(t, `{"a": {"b": [10, {"c": 5}]}, "x": [1, 2]}`)
	cases := []struct {
		path string
		want string // canonical EJSON of the value, "" when absent
	}{
		{"a.b.0", `{"$numberInt":"10"}`},
		{"a.b.1.c", `{"$numberInt":"5"}`},
		{"x.1", `{"$numberInt":"2"}`},
		{"x", `[{"$numberInt":"1"},{"$numberInt":"2"}]`},
		{"a.b.c", ""}, // would need array traversal
		{"a.b.2", ""},
		{"a.z", ""},
		{"x.01", ""},
		{"x.0.y", ""},
	}
	for _, c := range cases {
		v, ok := Lookup(d, c.path)
		got := ""
		if ok {
			got = string(FormatEJSONValue(v))
		}
		if got != c.want {
			t.Errorf("Lookup(%q) = %s, want %s", c.path, got, c.want)
		}
	}
}

func TestIndexValues(t *testing.T) {
	cases := []struct {
		doc, path string
		want      string // canonical EJSON values joined by " "; "missing" for the zero value
		multikey  bool
	}{
		{`{"a": 1}`, "a", `{"$numberInt":"1"}`, false},
		{`{}`, "a", `missing`, false},
		{`{"a": [1, 2, 1.0]}`, "a", `{"$numberInt":"1"} {"$numberInt":"2"}`, true},
		{`{"a": []}`, "a", `{"$undefined":true}`, true},
		{`{"a": [[1], 2]}`, "a", `[{"$numberInt":"1"}] {"$numberInt":"2"}`, true},
		{`{"a": [{"b": 1}, {"b": [2, 3]}, {"c": 0}]}`, "a.b", `{"$numberInt":"1"} {"$numberInt":"2"} {"$numberInt":"3"} missing`, true},
		{`{"a": {"b": null}}`, "a.b", `null`, false},
		{`{"a": 5}`, "a.b", `missing`, false},
		{`{"a": [1, 2]}`, "a.b", `missing`, true},
		{`{"a": [{"b": 1}, {"b": 2}]}`, "a.1.b", `{"$numberInt":"2"}`, true},
		{`{"a": [7]}`, "a.3", `missing`, true},
	}
	for _, c := range cases {
		vals, mk := IndexValues(ejson(t, c.doc), c.path)
		var parts []string
		for _, v := range vals {
			if v.Type == 0 {
				parts = append(parts, "missing")
				continue
			}
			parts = append(parts, string(FormatEJSONValue(v)))
		}
		if got := strings.Join(parts, " "); got != c.want || mk != c.multikey {
			t.Errorf("IndexValues(%s, %q) = %s multikey=%v, want %s multikey=%v", c.doc, c.path, got, mk, c.want, c.multikey)
		}
	}
}

func TestArrayIndex(t *testing.T) {
	for s, want := range map[string]int{"0": 0, "7": 7, "123": 123} {
		if n, ok := arrayIndex(s); !ok || n != want {
			t.Errorf("arrayIndex(%q) = %d %v", s, n, ok)
		}
	}
	for _, s := range []string{"", "01", "-1", "a", "1a", "1234567890"} {
		if _, ok := arrayIndex(s); ok {
			t.Errorf("arrayIndex(%q) accepted", s)
		}
	}
}
