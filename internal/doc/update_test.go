package doc

import (
	"bytes"
	"strings"
	"sync"
	"testing"

	kt "github.com/nil68657/nildb/internal/keyenc/keyenctest"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// TestUpdateConcurrent applies one compiled update (with $rename, whose
// source values are per-call state) from several goroutines; run under
// -race.
func TestUpdateConcurrent(t *testing.T) {
	u, err := CompileUpdate(ejson(t, `{"$rename": {"a": "b"}, "$inc": {"n": 1}, "$push": {"l": {"$each": [3, 1], "$sort": 1}}}`))
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				in := kt.DocRaw(kt.F("_id", kt.Int32(int32(g))), kt.F("a", kt.Int32(int32(i))), kt.F("l", kt.Array(kt.Int32(2))))
				out, mod, err := u.Apply(in, nil, fixtureNow)
				if err != nil || !mod {
					t.Errorf("apply: %v %v", mod, err)
					return
				}
				want := kt.DocRaw(kt.F("_id", kt.Int32(int32(g))), kt.F("l", kt.Array(kt.Int32(1), kt.Int32(2), kt.Int32(3))), kt.F("b", kt.Int32(int32(i))), kt.F("n", kt.Int32(1)))
				if !bytes.Equal(out, want) {
					t.Errorf("got %s want %s", FormatEJSON(out), FormatEJSON(want))
					return
				}
			}
		}(g)
	}
	wg.Wait()
}

func TestUpdateSizeLimit(t *testing.T) {
	half := strings.Repeat("x", MaxDocSize/2)
	u, err := CompileUpdate(kt.DocRaw(kt.F("$set", kt.Doc(kt.F("b", kt.String(half))))))
	if err != nil {
		t.Fatal(err)
	}
	in := kt.DocRaw(kt.F("_id", kt.Int32(1)), kt.F("a", kt.String(half)))
	_, _, err = u.Apply(in, nil, fixtureNow)
	if err == nil || !strings.HasPrefix(err.Error(), "ERR Resulting document after update is larger than 16777216") || Code(err) != CodeDocumentTooLarge {
		t.Fatalf("got %v", err)
	}
}

func TestUpdateDepthLimit(t *testing.T) {
	path := strings.TrimSuffix(strings.Repeat("x.", 101), ".")
	u, err := CompileUpdate(kt.DocRaw(kt.F("$set", kt.Doc(kt.F(path, kt.Int32(1))))))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := u.Apply(kt.DocRaw(kt.F("_id", kt.Int32(1))), nil, fixtureNow); err == nil || !strings.Contains(err.Error(), "nesting depth") {
		t.Fatalf("got %v", err)
	}
	u, _ = CompileUpdate(kt.DocRaw(kt.F("$set", kt.Doc(kt.F(path[2:], kt.Int32(1))))))
	if _, _, err := u.Apply(kt.DocRaw(kt.F("_id", kt.Int32(1))), nil, fixtureNow); err != nil {
		t.Fatalf("depth 100 refused: %v", err)
	}
}

func TestCompileUpdateMalformed(t *testing.T) {
	for _, in := range []bson.Raw{nil, {1, 2}, {9, 0, 0, 0, 0x10, 'a', 0, 1, 0}} {
		if _, err := CompileUpdate(in); err == nil {
			t.Errorf("CompileUpdate(%x) accepted", []byte(in))
		}
	}
	u, _ := CompileUpdate(ejson(t, `{"$set": {"a": 1}}`))
	if _, _, err := u.Apply(bson.Raw{1, 2, 3}, nil, fixtureNow); err == nil {
		t.Error("Apply to malformed document accepted")
	}
	if u.IsReplacement() {
		t.Error("operator update reported as replacement")
	}
	r, _ := CompileUpdate(ejson(t, `{"a": 1}`))
	if !r.IsReplacement() {
		t.Error("replacement not reported")
	}
}

func TestUpdateArrayPadLimit(t *testing.T) {
	u, err := CompileUpdate(ejson(t, `{"$set": {"a.2000000": 1}}`))
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = u.Apply(ejson(t, `{"_id": 1, "a": []}`), nil, fixtureNow)
	if err == nil || !strings.HasPrefix(err.Error(), "ERR can't backfill array to larger than 1500000 elements") {
		t.Fatalf("got %v", err)
	}
}

// TestUpsertGeneratedIDIsFresh checks that each upsert without an _id gets
// its own ObjectId.
func TestUpsertGeneratedIDIsFresh(t *testing.T) {
	u, _ := CompileUpdate(ejson(t, `{"$set": {"a": 1}}`))
	a, _, err := u.Apply(nil, nil, fixtureNow)
	if err != nil {
		t.Fatal(err)
	}
	b, _, _ := u.Apply(nil, nil, fixtureNow)
	ida, _ := lookupField(a, "_id")
	idb, _ := lookupField(b, "_id")
	if ida.Type != bson.TypeObjectID || bytes.Equal(ida.Value, idb.Value) {
		t.Fatalf("ids %s %s", ida.DebugString(), idb.DebugString())
	}
}
