package doc

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// Fixture files hold Extended JSON inside plain JSON, in a shape that a
// differential run against mongod can replay: each case names its input
// documents and the expected outcome.

type matcherCase struct {
	Name    string            `json:"name"`
	Filter  json.RawMessage   `json:"filter"`
	Match   []json.RawMessage `json:"match"`
	NoMatch []json.RawMessage `json:"nomatch"`
	Error   string            `json:"error"`
}

type updateCase struct {
	Name     string          `json:"name"`
	Doc      json.RawMessage `json:"doc"`
	Filter   json.RawMessage `json:"filter"`
	Update   json.RawMessage `json:"update"`
	Upsert   bool            `json:"upsert"`
	Result   json.RawMessage `json:"result"`
	Modified *bool           `json:"modified"`
	GenID    bool            `json:"genId"`
	Error    string          `json:"error"`
}

type projectCase struct {
	Name       string          `json:"name"`
	Projection json.RawMessage `json:"projection"`
	Doc        json.RawMessage `json:"doc"`
	Result     json.RawMessage `json:"result"`
	Error      string          `json:"error"`
}

// fixtureNow is the clock every update fixture runs at.
var fixtureNow = time.Date(2026, 1, 2, 3, 4, 5, 678_000_000, time.UTC)

func loadFixtures[T any](t *testing.T, dir string) map[string][]T {
	t.Helper()
	files, err := filepath.Glob(filepath.Join("testdata", dir, "*.json"))
	if err != nil || len(files) == 0 {
		t.Fatalf("no fixtures in testdata/%s: %v", dir, err)
	}
	out := map[string][]T{}
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		var cases []T
		if err := json.Unmarshal(b, &cases); err != nil {
			t.Fatalf("%s: %v", f, err)
		}
		out[filepath.Base(f)] = cases
	}
	return out
}

func mustEJSON(t *testing.T, raw json.RawMessage) bson.Raw {
	t.Helper()
	d, err := ParseEJSON(raw)
	if err != nil {
		t.Fatalf("fixture %s: %v", raw, err)
	}
	return d
}

func checkErr(t *testing.T, err error, want string) {
	t.Helper()
	if err == nil {
		t.Fatalf("no error, want %q", want)
	}
	if !strings.HasPrefix(err.Error(), want) {
		t.Fatalf("error %q, want prefix %q", err.Error(), want)
	}
}

func TestMatcherFixtures(t *testing.T) {
	total := 0
	for file, cases := range loadFixtures[matcherCase](t, "matcher") {
		for _, c := range cases {
			t.Run(file+"/"+c.Name, func(t *testing.T) {
				filter := mustEJSON(t, c.Filter)
				m, err := Compile(filter, nil)
				if c.Error != "" {
					checkErr(t, err, c.Error)
					return
				}
				if err != nil {
					t.Fatalf("Compile(%s): %v", c.Filter, err)
				}
				for _, want := range []bool{true, false} {
					docs := c.Match
					if !want {
						docs = c.NoMatch
					}
					for _, raw := range docs {
						got, err := m.Match(mustEJSON(t, raw))
						if err != nil {
							t.Fatalf("Match(%s): %v", raw, err)
						}
						if got != want {
							t.Errorf("filter %s on %s: got %v, want %v", c.Filter, raw, got, want)
						}
					}
				}
			})
			total += len(c.Match) + len(c.NoMatch)
			if c.Error != "" {
				total++
			}
		}
	}
	if total < 150 {
		t.Errorf("only %d matcher cases, want at least 150", total)
	}
	t.Logf("%d matcher cases", total)
}

func TestUpdateFixtures(t *testing.T) {
	total := 0
	for file, cases := range loadFixtures[updateCase](t, "update") {
		total += len(cases)
		for _, c := range cases {
			t.Run(file+"/"+c.Name, func(t *testing.T) { runUpdateCase(t, c) })
		}
	}
	if total < 60 {
		t.Errorf("only %d update cases, want at least 60", total)
	}
	t.Logf("%d update cases", total)
}

func runUpdateCase(t *testing.T, c updateCase) {
	u, err := CompileUpdate(mustEJSON(t, c.Update))
	if err != nil {
		if c.Error == "" {
			t.Fatalf("CompileUpdate(%s): %v", c.Update, err)
		}
		checkErr(t, err, c.Error)
		return
	}
	var doc, filter bson.Raw
	if !c.Upsert {
		doc = mustEJSON(t, c.Doc)
	}
	if c.Filter != nil {
		filter = mustEJSON(t, c.Filter)
	}
	out, modified, err := u.Apply(doc, filter, fixtureNow)
	if c.Error != "" {
		checkErr(t, err, c.Error)
		return
	}
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	want := mustEJSON(t, c.Result)
	if c.GenID {
		id, ok := lookupField(out, "_id")
		it := newIter(out)
		first, _ := it.next()
		if !ok || id.Type != bson.TypeObjectID || string(first.name) != "_id" {
			t.Fatalf("upsert did not generate a leading ObjectId _id: %s", FormatEJSON(out))
		}
		out = bson.Raw(buildDocWithout(out, "_id"))
	}
	if !bytes.Equal(out, want) {
		t.Fatalf("result\n got %s\nwant %s", FormatEJSON(out), FormatEJSON(want))
	}
	wantMod := c.Upsert || !bytes.Equal(want, doc)
	if c.Modified != nil {
		wantMod = *c.Modified
	}
	if modified != wantMod {
		t.Fatalf("modified = %v, want %v", modified, wantMod)
	}
	if !modified && &out[0] != &doc[0] {
		t.Fatalf("a no-op update must return the input document")
	}
}

func buildDocWithout(d []byte, name string) []byte {
	var w builder
	w.begin()
	it := newIter(d)
	for {
		e, ok := it.next()
		if !ok {
			break
		}
		if string(e.name) != name {
			w.rawElem(e.raw)
		}
	}
	return w.end()
}

func TestProjectionFixtures(t *testing.T) {
	total := 0
	for file, cases := range loadFixtures[projectCase](t, "project") {
		total += len(cases)
		for _, c := range cases {
			t.Run(file+"/"+c.Name, func(t *testing.T) {
				p, err := CompileProjection(mustEJSON(t, c.Projection))
				if c.Error != "" {
					checkErr(t, err, c.Error)
					return
				}
				if err != nil {
					t.Fatalf("CompileProjection(%s): %v", c.Projection, err)
				}
				out, err := p.Apply(mustEJSON(t, c.Doc))
				if err != nil {
					t.Fatalf("Apply: %v", err)
				}
				want := mustEJSON(t, c.Result)
				if !bytes.Equal(out, want) {
					t.Fatalf("result\n got %s\nwant %s", FormatEJSON(out), FormatEJSON(want))
				}
			})
		}
	}
	if total < 20 {
		t.Errorf("only %d projection cases, want at least 20", total)
	}
	t.Logf("%d projection cases", total)
}
