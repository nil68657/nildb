package doc

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
)

func predString(p Predicate) string {
	var vals []string
	for _, v := range p.Values {
		vals = append(vals, string(FormatEJSONValue(v)))
	}
	s := fmt.Sprintf("%s %s [%s]", p.Field, p.Op, strings.Join(vals, ","))
	if p.Prefix != "" {
		s += " prefix=" + p.Prefix
	}
	return s
}

func TestIndexablePredicates(t *testing.T) {
	cases := []struct {
		filter string
		want   []string
	}{
		{`{"a": 1}`, []string{`a $eq [{"$numberInt":"1"}]`}},
		{`{"a": null}`, []string{`a $eq [null]`}},
		{`{"a": {"b": 1}}`, []string{`a $eq [{"b":{"$numberInt":"1"}}]`}},
		{`{"a": [1, 2]}`, nil},
		{`{"a": {"$numberDecimal": "1"}}`, nil},
		{`{"a": {"$gt": 1, "$lte": 5}}`, []string{`a $gt [{"$numberInt":"1"}]`, `a $lte [{"$numberInt":"5"}]`}},
		{`{"a": {"$in": [1, "x"]}}`, []string{`a $in [{"$numberInt":"1"},"x"]`}},
		{`{"a": {"$in": [1, {"$regularExpression": {"pattern": "x", "options": ""}}]}}`, nil},
		{`{"a": {"$in": [[1]]}}`, nil},
		{`{"a": {"$regex": "^abc.*"}}`, []string{`a $regex [{"$regularExpression":{"pattern":"^abc.*","options":""}}] prefix=abc`}},
		{`{"a": {"$regex": "^abc", "$options": "i"}}`, nil},
		{`{"a": {"$regex": "abc"}}`, nil},
		{`{"a": {"$regularExpression": {"pattern": "^x\\.y", "options": ""}}}`, []string{`a $regex [{"$regularExpression":{"pattern":"^x\\.y","options":""}}] prefix=x.y`}},
		{`{"a": {"$all": [1, 2]}}`, []string{`a $eq [{"$numberInt":"1"}]`, `a $eq [{"$numberInt":"2"}]`}},
		{`{"$and": [{"a": 1}, {"$and": [{"b": {"$lt": "z"}}]}]}`, []string{`a $eq [{"$numberInt":"1"}]`, `b $lt ["z"]`}},
		{`{"$or": [{"a": 1}, {"a": 2}]}`, nil},
		{`{"a": {"$ne": 1}, "b": {"$nin": [1]}, "c": {"$not": {"$gt": 1}}, "d": {"$exists": true}}`, nil},
		{`{"a": {"$elemMatch": {"b": 1}}}`, nil},
		{`{"x.y": {"$gte": {"$date": "2020-01-01T00:00:00Z"}}, "_id": 5}`, []string{`x.y $gte [{"$date":{"$numberLong":"1577836800000"}}]`, `_id $eq [{"$numberInt":"5"}]`}},
	}
	for _, c := range cases {
		m, err := Compile(ejson(t, c.filter), nil)
		if err != nil {
			t.Fatalf("Compile(%s): %v", c.filter, err)
		}
		var got []string
		for _, p := range m.IndexablePredicates() {
			got = append(got, predString(p))
		}
		if strings.Join(got, "\n") != strings.Join(c.want, "\n") {
			t.Errorf("%s:\n got %q\nwant %q", c.filter, got, c.want)
		}
	}
}

func TestMatchAll(t *testing.T) {
	for _, f := range []string{`{}`, `{"$comment": "x"}`} {
		m, err := Compile(ejson(t, f), nil)
		if err != nil || !m.MatchAll() {
			t.Errorf("%s: MatchAll false (%v)", f, err)
		}
	}
	m, _ := Compile(nil, nil)
	if !m.MatchAll() {
		t.Error("nil filter should match everything")
	}
	m, _ = Compile(ejson(t, `{"a": 1}`), nil)
	if m.MatchAll() {
		t.Error("a real filter is not MatchAll")
	}
}

// fakeGeo records every Compile call and matches documents whose field
// holds the string "in".
type fakeGeo struct {
	mu    sync.Mutex
	calls []string
	fail  error
}

func (g *fakeGeo) Compile(field, op string, spec bson.Raw) (func(bson.Raw) (bool, error), error) {
	g.mu.Lock()
	g.calls = append(g.calls, field+" "+op+" "+string(FormatEJSON(spec)))
	g.mu.Unlock()
	if g.fail != nil {
		return nil, g.fail
	}
	return func(d bson.Raw) (bool, error) {
		if v, ok := Lookup(d, field); ok {
			if s, ok := strValue(v); ok {
				if string(s) == "boom" {
					return false, errors.New("ERR hook failed")
				}
				return string(s) == "in", nil
			}
		}
		return false, nil
	}, nil
}

func TestGeoHooks(t *testing.T) {
	g := &fakeGeo{}
	m, err := Compile(ejson(t, `{"loc": {"$near": {"$geometry": {"type": "Point", "coordinates": [1, 2]}}, "$maxDistance": 10}, "k": 1}`), g)
	if err != nil {
		t.Fatal(err)
	}
	wantCall := `loc $near {"$near":{"$geometry":{"type":"Point","coordinates":[{"$numberInt":"1"},{"$numberInt":"2"}]}},"$maxDistance":{"$numberInt":"10"}}`
	if len(g.calls) != 1 || g.calls[0] != wantCall {
		t.Fatalf("calls %q\nwant %q", g.calls, wantCall)
	}
	preds := m.IndexablePredicates()
	if len(preds) != 2 || preds[0].Op != OpNear || preds[0].Field != "loc" || preds[1].Op != OpEq {
		t.Fatalf("predicates %v", preds)
	}
	for doc, want := range map[string]bool{`{"loc": "in", "k": 1}`: true, `{"loc": "out", "k": 1}`: false, `{"loc": "in", "k": 2}`: false} {
		if got, err := m.Match(ejson(t, doc)); err != nil || got != want {
			t.Errorf("%s: %v %v", doc, got, err)
		}
	}
	if _, err := m.Match(ejson(t, `{"loc": "boom", "k": 1}`)); err == nil || err.Error() != "ERR hook failed" {
		t.Errorf("hook error not propagated: %v", err)
	}

	// $geoWithin works under $or, $not and $elemMatch, and is indexable
	// only at the top level.
	g = &fakeGeo{}
	m, err = Compile(ejson(t, `{"$or": [{"loc": {"$geoWithin": {"$centerSphere": [[0, 0], 1]}}}, {"x": 1}], "arr": {"$elemMatch": {"p": {"$geoIntersects": {"$geometry": {"type": "Point", "coordinates": [0, 0]}}}}}}`), g)
	if err != nil {
		t.Fatal(err)
	}
	if len(g.calls) != 2 || !strings.HasPrefix(g.calls[1], "p $geoIntersects ") {
		t.Fatalf("calls %q", g.calls)
	}
	if len(m.IndexablePredicates()) != 0 {
		t.Fatalf("nested geo reported as indexable: %v", m.IndexablePredicates())
	}
	if ok, _ := m.Match(ejson(t, `{"loc": "in", "arr": [{"p": "out"}, {"p": "in"}]}`)); !ok {
		t.Error("nested geo match failed")
	}
	m, err = Compile(ejson(t, `{"loc": {"$geoWithin": {"$box": [[0, 0], [1, 1]]}}}`), g)
	if err != nil || len(m.IndexablePredicates()) != 1 || m.IndexablePredicates()[0].Op != OpGeoWithin {
		t.Fatalf("top-level $geoWithin: %v %v", err, m.IndexablePredicates())
	}

	for filter, want := range map[string]string{
		`{"$or": [{"loc": {"$near": [0, 0]}}]}`:                                                 "ERR geoNear must be top-level expr",
		`{"loc": {"$not": {"$nearSphere": [0, 0]}}}`:                                            "ERR geoNear must be top-level expr",
		`{"a": {"$near": [0, 0]}, "b": {"$nearSphere": [0, 0]}}`:                                "ERR Too many geoNear expressions",
		`{"a": {"$elemMatch": {"$geoWithin": {}}}}`:                                             "ERR $geoWithin is not allowed inside $elemMatch value conditions",
		`{"a": {"$geoWithin": {}, "$geoIntersects": {}}}`:                                       "ERR $geoWithin and $geoIntersects cannot be combined on one field",
		`{"$and": [{"a": {"$near": [0, 0]}}, {"b": {"$elemMatch": {"c": {"$near": [1, 1]}}}}]}`: "ERR geoNear must be top-level expr",
	} {
		if _, err := Compile(ejson(t, filter), &fakeGeo{}); err == nil || !strings.HasPrefix(err.Error(), want) {
			t.Errorf("%s: %v, want %q", filter, err, want)
		}
	}
	if _, err := Compile(ejson(t, `{"a": {"$geoWithin": {}}}`), &fakeGeo{fail: errors.New("ERR Loop is not valid: x")}); err == nil || err.Error() != "ERR Loop is not valid: x" {
		t.Errorf("hook compile error not returned: %v", err)
	}
	if _, err := Compile(ejson(t, `{"a": {"$geoWithin": {}}}`), nil); !errors.Is(err, ErrNoGeoHooks) {
		t.Errorf("nil hooks: %v", err)
	}
}

// TestMatcherConcurrent shares one compiled Matcher across goroutines; run
// under -race.
func TestMatcherConcurrent(t *testing.T) {
	m, err := Compile(ejson(t, `{"a": {"$regex": "^x"}, "b": {"$elemMatch": {"c": {"$gt": 1}}}}`), nil)
	if err != nil {
		t.Fatal(err)
	}
	yes := ejson(t, `{"a": "xy", "b": [{"c": 0}, {"c": 2}]}`)
	no := ejson(t, `{"a": "yx", "b": [{"c": 2}]}`)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 500; j++ {
				if ok, _ := m.Match(yes); !ok {
					t.Error("yes did not match")
					return
				}
				if ok, _ := m.Match(no); ok {
					t.Error("no matched")
					return
				}
			}
		}()
	}
	wg.Wait()
}

func TestErrorCodes(t *testing.T) {
	_, err := Compile(ejson(t, `{"a": {"$foo": 1}}`), nil)
	var de *Error
	if !errors.As(err, &de) || de.Code != CodeBadValue || de.Msg != "unknown operator: $foo" {
		t.Fatalf("got %#v", err)
	}
	if Code(errors.New("other")) != CodeBadValue {
		t.Fatal("Code of a foreign error")
	}
	u, _ := CompileUpdate(ejson(t, `{"$set": {"_id": 2}}`))
	_, _, err = u.Apply(ejson(t, `{"_id": 1}`), nil, fixtureNow)
	if Code(err) != CodeImmutableField {
		t.Fatalf("immutable _id code %d", Code(err))
	}
}
