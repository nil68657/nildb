package query

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"

	"github.com/nil68657/nildb/internal/catalog"
	"github.com/nil68657/nildb/internal/doc"
	"go.mongodb.org/mongo-driver/v2/bson"
)

var (
	colors  = []string{"red", "green", "blue", "black", "brown"}
	tprefix = []string{"ab", "ac", "b"}
)

// findDoc is document i of the find fixture. n, k, m and u are always
// present and never arrays; a is an array for every fourth document, so
// a_1 is multikey; s is present for every third document (the sparse
// index); v mixes types.
func findDoc(i int) bson.D {
	d := bson.D{
		{Key: "_id", Value: int32(i)},
		{Key: "n", Value: int32(i % 100)},
		{Key: "k", Value: colors[i%5]},
		{Key: "m", Value: float64(i)*0.5 - 50},
		{Key: "u", Value: int32(i)},
	}
	if i%3 == 0 {
		d = append(d, bson.E{Key: "s", Value: int32(i)})
	}
	switch {
	case i%4 == 0:
		d = append(d, bson.E{Key: "a", Value: bson.A{int32(i % 10), int32((i + 1) % 10)}})
	case i%7 != 0:
		d = append(d, bson.E{Key: "a", Value: int32(i % 10)})
	}
	d = append(d, bson.E{Key: "t", Value: fmt.Sprintf("%s%03d", tprefix[i%3], i)})
	if i%5 != 0 {
		d = append(d, bson.E{Key: "z", Value: int32(i % 13)})
	}
	d = append(d, bson.E{Key: "d", Value: bson.D{{Key: "x", Value: int32(i % 7)}, {Key: "y", Value: fmt.Sprintf("y%d", i%4)}}})
	switch i % 6 {
	case 0:
		d = append(d, bson.E{Key: "v", Value: int32(i % 9)})
	case 1:
		d = append(d, bson.E{Key: "v", Value: fmt.Sprintf("v%d", i%9)})
	case 2:
		d = append(d, bson.E{Key: "v", Value: nil})
	case 4:
		d = append(d, bson.E{Key: "v", Value: float64(i%9) + 0.5})
	case 5:
		d = append(d, bson.E{Key: "v", Value: bson.A{int32(i % 3), "x"}})
	}
	return d
}

const findN = 600

// findEnv loads the find fixture plus four edge documents: a string with
// a NUL after an indexed prefix, a stored Regex value, a document without
// n and m, and a string _id.
func findEnv(t *testing.T) (*tenv, *catalog.Collection) {
	e := newEnv(t)
	c := e.coll("test.find")
	for _, spec := range []catalog.Index{btree("n"), btree("k", "n"), btree("-m"), btree("a"), btree("t"), btree("d.x")} {
		e.index(c, spec)
	}
	uniq := btree("u")
	uniq.Unique = true
	e.index(c, uniq)
	sparse := btree("s")
	sparse.Sparse = true
	e.index(c, sparse)
	var docs []bson.Raw
	for i := range findN {
		docs = append(docs, mustMarshal(t, findDoc(i)))
	}
	docs = append(docs,
		mustMarshal(t, bson.D{{Key: "_id", Value: int32(1000)}, {Key: "n", Value: int32(5)}, {Key: "k", Value: "red"}, {Key: "m", Value: 0.25}, {Key: "u", Value: int32(1000)}, {Key: "t", Value: "ab\x00z"}}),
		mustMarshal(t, bson.D{{Key: "_id", Value: int32(1001)}, {Key: "n", Value: int32(6)}, {Key: "k", Value: "blue"}, {Key: "m", Value: -0.25}, {Key: "u", Value: int32(1001)}, {Key: "t", Value: bson.Regex{Pattern: "^ab"}}}),
		mustMarshal(t, bson.D{{Key: "_id", Value: int32(1002)}, {Key: "k", Value: "green"}, {Key: "u", Value: int32(1002)}}),
		mustMarshal(t, bson.D{{Key: "_id", Value: "str-id"}, {Key: "n", Value: int32(7)}, {Key: "k", Value: "red"}, {Key: "m", Value: 1.0}, {Key: "u", Value: int32(1003)}}),
	)
	e.insert(c, docs...)
	return e, c
}

// checkFind compares a find's result with the brute-force oracle. Without
// a sort the order is the plan's, so only the sets are compared; with a
// sort the sequence of sort keys must match (index order may break ties
// differently from _id order) and, without a limit, the sets as well.
func checkFind(t *testing.T, e *tenv, c *catalog.Collection, f Find, got []bson.Raw) {
	t.Helper()
	want := brute(t, e.all(c), f)
	spec, _ := doc.ParseSort(f.Sort)
	label := fmt.Sprintf("filter %s sort %s skip %d limit %d", ejsonOf(f.Filter), ejsonOf(f.Sort), f.Skip, f.Limit)
	if len(got) != len(want) {
		t.Fatalf("%s: got %d documents, want %d\n got %v\nwant %v", label, len(got), len(want), ejsonList(got), ejsonList(want))
	}
	if len(spec) == 0 {
		if f.Limit > 0 || f.Skip > 0 {
			return
		}
		if g, w := sortedEJSON(got), sortedEJSON(want); !slices.Equal(g, w) {
			t.Fatalf("%s:\n got %v\nwant %v", label, g, w)
		}
		return
	}
	if g, w := sortKeys(got, spec), sortKeys(want, spec); !slices.Equal(g, w) {
		t.Fatalf("%s: sort order differs\n got %v\nwant %v", label, ejsonList(got), ejsonList(want))
	}
	if f.Limit == 0 && f.Skip == 0 {
		if g, w := sortedEJSON(got), sortedEJSON(want); !slices.Equal(g, w) {
			t.Fatalf("%s:\n got %v\nwant %v", label, g, w)
		}
	}
}

func TestPlannerGoldens(t *testing.T) {
	e, c := findEnv(t)
	re := `{"$regularExpression":{"pattern":"^ab","options":""}}`
	cases := []struct {
		filter, sort, hint, hintKey string
		limit                       int64
		plan, index                 string
		bounds                      []string
		sortKept                    bool // the plan still sorts in memory
	}{
		{filter: `{}`, plan: "rowscan"},
		{filter: `{"_id": 5}`, plan: "index", index: "_id_", bounds: []string{"_id: [5, 5]"}},
		{filter: `{"_id": {"$in": [3, 1]}}`, plan: "index", index: "_id_", bounds: []string{"_id: [3, 3], [1, 1]"}},
		{filter: `{"_id": {"$gt": 10}}`, plan: "index", index: "_id_", bounds: []string{"_id: (10, inf]"}},
		{filter: `{"n": 5}`, plan: "index", index: "n_1", bounds: []string{"n: [5, 5]"}},
		{filter: `{"n": {"$gte": 10, "$lt": 20}}`, plan: "index", index: "n_1", bounds: []string{"n: [10, 20)"}},
		{filter: `{"n": {"$in": [1, 2, 3]}}`, plan: "index", index: "n_1", bounds: []string{"n: [1, 1], [2, 2], [3, 3]"}},
		{filter: `{"u": 7}`, plan: "index", index: "u_1", bounds: []string{"u: [7, 7]"}},
		{filter: `{"u": 7, "n": 7}`, plan: "index", index: "u_1", bounds: []string{"u: [7, 7]"}},
		{filter: `{"k": "red"}`, plan: "index", index: "k_1_n_1", bounds: []string{`k: ["red", "red"]`}},
		{filter: `{"k": "red", "n": {"$gt": 5}}`, plan: "index", index: "k_1_n_1", bounds: []string{`k: ["red", "red"]`, "n: (5, inf]"}},
		{filter: `{"k": "red", "n": 5}`, plan: "index", index: "k_1_n_1", bounds: []string{`k: ["red", "red"]`, "n: [5, 5]"}},
		{filter: `{"n": 5}`, sort: `{"k": 1}`, plan: "index", index: "n_1", bounds: []string{"n: [5, 5]"}, sortKept: true},
		{filter: `{"k": "red"}`, sort: `{"n": 1}`, plan: "index", index: "k_1_n_1", bounds: []string{`k: ["red", "red"]`}},
		{filter: `{"k": "red"}`, sort: `{"n": -1}`, plan: "index", index: "k_1_n_1", bounds: []string{`k: ["red", "red"]`}},
		{filter: `{"k": {"$in": ["red", "blue"]}}`, sort: `{"n": 1}`, plan: "index", index: "k_1_n_1", bounds: []string{`k: ["red", "red"], ["blue", "blue"]`}, sortKept: true},
		{filter: `{"m": {"$gt": 0}}`, sort: `{"m": -1}`, plan: "index", index: "m_-1", bounds: []string{"m: (0, inf]"}},
		{filter: `{"m": {"$gt": 0}}`, sort: `{"m": 1}`, plan: "index", index: "m_-1", bounds: []string{"m: (0, inf]"}},
		{filter: `{"m": {"$lte": -10.5}}`, plan: "index", index: "m_-1", bounds: []string{"m: [-inf, -10.5]"}},
		{filter: `{"s": 3}`, plan: "index", index: "s_1", bounds: []string{"s: [3, 3]"}},
		{filter: `{"s": null}`, plan: "rowscan"},
		{filter: `{"a": {"$gt": 2, "$lt": 5}}`, plan: "index", index: "a_1", bounds: []string{"a: (2, inf]"}},
		{filter: `{"a": 3}`, sort: `{"a": 1}`, plan: "index", index: "a_1", bounds: []string{"a: [3, 3]"}, sortKept: true},
		{filter: `{"t": {"$regex": "^ab"}}`, plan: "index", index: "t_1", bounds: []string{`t: prefix "ab", [` + re + `, ` + re + `]`}},
		{filter: `{"t": {"$regex": "ab"}}`, plan: "rowscan"},
		{filter: `{"t": {"$regex": "^ab", "$options": "i"}}`, plan: "rowscan"},
		{filter: `{"d.x": 3}`, plan: "index", index: "d.x_1", bounds: []string{"d.x: [3, 3]"}},
		{filter: `{"z": 5}`, plan: "rowscan"},
		{filter: `{}`, sort: `{"n": 1}`, plan: "index", index: "n_1", bounds: []string{"n: [MinKey, MaxKey]"}},
		{filter: `{}`, sort: `{"_id": -1}`, plan: "rowscan"},
		{filter: `{}`, sort: `{"z": 1}`, plan: "rowscan", sortKept: true},
		{filter: `{"n": {"$in": []}}`, plan: "index", index: "n_1", bounds: []string{"n: []"}},
		{filter: `{"n": 5, "$or": [{"k": "red"}, {"k": "blue"}]}`, plan: "index", index: "n_1", bounds: []string{"n: [5, 5]"}},
		{filter: `{"n": 5}`, hint: "_id_", plan: "index", index: "_id_", bounds: []string{"_id: [MinKey, MaxKey]"}},
		{filter: `{"n": 5}`, hint: "k_1_n_1", plan: "index", index: "k_1_n_1", bounds: []string{"k: [MinKey, MaxKey]", "n: [MinKey, MaxKey]"}},
		{filter: `{"n": 5}`, hintKey: `{"n": 1}`, plan: "index", index: "n_1", bounds: []string{"n: [5, 5]"}},
		{filter: `{"n": {"$gt": "x"}}`, plan: "index", index: "n_1", bounds: []string{`n: ("x", inf]`}},
		{filter: `{"n": 5, "k": "red"}`, sort: `{"n": 1}`, plan: "index", index: "k_1_n_1", bounds: []string{`k: ["red", "red"]`, "n: [5, 5]"}},
		{filter: `{"n": {"$lt": 3}}`, sort: `{"n": -1}`, limit: 2, plan: "index", index: "n_1", bounds: []string{"n: [-inf, 3)"}},
	}
	if len(cases) < 25 {
		t.Fatalf("only %d golden cases", len(cases))
	}
	for i, tc := range cases {
		t.Run(fmt.Sprintf("%02d", i), func(t *testing.T) {
			f := Find{Filter: ej(t, tc.filter), Sort: ej(t, tc.sort), Limit: tc.limit, Hint: tc.hint, HintKey: ej(t, tc.hintKey)}
			p, got := e.find(c, f, Options{})
			if p.Source != tc.plan || p.Index != tc.index || !slices.Equal(p.Bounds, tc.bounds) {
				t.Errorf("plan = %s %q %q, want %s %q %q", p.Source, p.Index, p.Bounds, tc.plan, tc.index, tc.bounds)
			}
			sorted := slices.ContainsFunc(p.Stages, func(s string) bool { return strings.HasPrefix(s, "$sort") })
			if sorted != tc.sortKept {
				t.Errorf("stages %q: in-memory sort = %v, want %v", p.Stages, sorted, tc.sortKept)
			}
			checkFind(t, e, c, f, got)
		})
	}
}

func TestPlannerEstimates(t *testing.T) {
	e, c := findEnv(t)
	p, _ := e.find(c, Find{}, Options{})
	if p.EstimatedRows != findN+4 {
		t.Errorf("rowscan estimate = %d, want %d", p.EstimatedRows, findN+4)
	}
	p, _ = e.find(c, Find{Filter: ej(t, `{"_id": {"$in": [1, 2, 3]}}`)}, Options{})
	if p.EstimatedRows != 3 {
		t.Errorf("_id point estimate = %d, want 3", p.EstimatedRows)
	}
	p, _ = e.find(c, Find{Filter: ej(t, `{"n": {"$in": []}}`)}, Options{})
	if p.EstimatedRows != 0 {
		t.Errorf("empty bounds estimate = %d, want 0", p.EstimatedRows)
	}
}

func TestPlannerErrors(t *testing.T) {
	e, c := findEnv(t)
	cases := []struct {
		f    Find
		want string
	}{
		{Find{Filter: ej(t, `{"n": 1}`), Hint: "nope_1"}, "ERR hint provided does not correspond to an existing index"},
		{Find{Filter: ej(t, `{"loc": {"$near": {"$geometry": {"type": "Point", "coordinates": [0, 0]}}}}`)}, "ERR unable to find index for $geoNear query"},
		{Find{Filter: ej(t, `{"n": {"$bad": 1}}`)}, "ERR unknown operator: $bad"},
		{Find{Sort: ej(t, `{"n": 2}`)}, "ERR $sort key ordering must be 1 (for ascending) or -1 (for descending)"},
	}
	for _, tc := range cases {
		_, err := e.q.PlanFind(c, tc.f, Options{})
		if err == nil || err.Error() != tc.want {
			t.Errorf("PlanFind(%s) error = %v, want %q", ejsonOf(tc.f.Filter), err, tc.want)
		}
	}
}

// randomFilter draws filters over the fixture's fields: equality, ranges,
// $in, $nin, $ne, $exists and anchored regexes, with cross-type operands,
// nested in $and and $or.
func randomFilter(r *rand.Rand, depth int) bson.D {
	var d bson.D
	for range 1 + r.IntN(2) {
		switch x := r.IntN(10); {
		case x == 8 && depth < 2:
			d = append(d, bson.E{Key: "$or", Value: bson.A{randomFilter(r, depth+1), randomFilter(r, depth+1)}})
		case x == 9 && depth < 2:
			d = append(d, bson.E{Key: "$and", Value: bson.A{randomFilter(r, depth+1), randomFilter(r, depth+1)}})
		default:
			d = append(d, randomClause(r))
		}
	}
	return d
}

var findFields = []string{"_id", "n", "k", "m", "u", "s", "a", "t", "z", "d.x", "v"}

func randomClause(r *rand.Rand) bson.E {
	f := findFields[r.IntN(len(findFields))]
	v := randomValue(r, f)
	switch r.IntN(12) {
	case 0, 1, 2:
		return bson.E{Key: f, Value: v}
	case 3:
		return bson.E{Key: f, Value: bson.D{{Key: "$ne", Value: v}}}
	case 4:
		return bson.E{Key: f, Value: bson.D{{Key: "$gt", Value: v}}}
	case 5:
		return bson.E{Key: f, Value: bson.D{{Key: "$gte", Value: v}, {Key: "$lt", Value: randomValue(r, f)}}}
	case 6:
		return bson.E{Key: f, Value: bson.D{{Key: "$lte", Value: v}}}
	case 7:
		return bson.E{Key: f, Value: bson.D{{Key: "$in", Value: bson.A{v, randomValue(r, f), randomValue(r, f)}}}}
	case 8:
		return bson.E{Key: f, Value: bson.D{{Key: "$nin", Value: bson.A{v, randomValue(r, f)}}}}
	case 9:
		return bson.E{Key: f, Value: bson.D{{Key: "$exists", Value: r.IntN(2) == 0}}}
	case 10:
		return bson.E{Key: f, Value: bson.D{{Key: "$lt", Value: v}}}
	}
	prefixes := []string{"a", "ab", "ac", "b", "bl", "re", "y"}
	return bson.E{Key: []string{"t", "k", "d.y"}[r.IntN(3)], Value: bson.Regex{Pattern: "^" + prefixes[r.IntN(len(prefixes))]}}
}

func randomValue(r *rand.Rand, f string) any {
	if r.IntN(10) == 0 {
		return []any{"x", nil, 2.5, true, bson.MinKey{}, bson.MaxKey{}}[r.IntN(6)]
	}
	switch f {
	case "m":
		return float64(r.IntN(260))*0.5 - 60
	case "k":
		return []string{"red", "green", "blue", "black", "brown", "zz"}[r.IntN(6)]
	case "t":
		return fmt.Sprintf("%s%03d", tprefix[r.IntN(3)], r.IntN(findN))
	case "v":
		return []any{int32(r.IntN(9)), fmt.Sprintf("v%d", r.IntN(9)), nil, float64(r.IntN(9)) + 0.5}[r.IntN(4)]
	}
	return int32(r.IntN(115) - 5)
}

var randomSorts = []string{"", "", `{"n": 1}`, `{"n": -1}`, `{"k": 1, "n": 1}`, `{"m": -1}`, `{"_id": -1}`, `{"a": 1}`, `{"t": 1}`, `{"v": -1, "u": 1}`}

func TestFindMatchesBruteForce(t *testing.T) {
	e, c := findEnv(t)
	r := rand.New(rand.NewPCG(1, 2))
	indexes := map[string]int{}
	for i := range 400 {
		f := Find{Filter: mustMarshal(t, randomFilter(r, 0)), Sort: ej(t, randomSorts[r.IntN(len(randomSorts))])}
		if r.IntN(3) == 0 {
			f.Limit = int64(1 + r.IntN(20))
		}
		if r.IntN(5) == 0 {
			f.Skip = int64(r.IntN(10))
		}
		p, err := e.q.PlanFind(c, f, Options{})
		if err != nil {
			t.Fatalf("case %d: PlanFind(%s): %v", i, ejsonOf(f.Filter), err)
		}
		indexes[p.Index]++
		checkFind(t, e, c, f, e.drain(p))
	}
	t.Logf("plans by index: %v", indexes)
	if used := 400 - indexes[""]; used < 150 || len(indexes) < 8 {
		t.Fatalf("only %d index plans over %d indexes: %v", used, len(indexes)-1, indexes)
	}
}

func TestFindProjection(t *testing.T) {
	e, c := findEnv(t)
	f := Find{Filter: ej(t, `{"k": "blue"}`), Sort: ej(t, `{"n": 1}`), Project: ej(t, `{"n": 1, "d.x": 1, "_id": 0}`), Limit: 5}
	_, got := e.find(c, f, Options{})
	want := brute(t, e.all(c), f)
	if g, w := ejsonList(got), ejsonList(want); !slices.Equal(g, w) {
		t.Fatalf("got %v\nwant %v", g, w)
	}
}

// TestTopKDeterministic checks that an in-memory top-k breaks ties by _id
// whatever the insertion order, and gives the same answer every run.
func TestTopKDeterministic(t *testing.T) {
	e := newEnv(t)
	c := e.coll("test.topk")
	r := rand.New(rand.NewPCG(3, 4))
	ids := r.Perm(300)
	var docs []bson.Raw
	for _, id := range ids {
		docs = append(docs, mustMarshal(t, bson.D{{Key: "_id", Value: int32(id)}, {Key: "g", Value: int32(id % 3)}, {Key: "h", Value: int32(id % 2)}}))
	}
	e.insert(c, docs...)
	f := Find{Sort: ej(t, `{"g": 1, "h": -1}`), Skip: 3, Limit: 10}
	var first []string
	for run := range 3 {
		p, got := e.find(c, f, Options{})
		if !slices.Contains(p.Stages, "$sort (top 13)") {
			t.Fatalf("stages = %q, want a top-13 sort", p.Stages)
		}
		ids := ejsonList(got)
		if run == 0 {
			first = ids
			continue
		}
		if !slices.Equal(ids, first) {
			t.Fatalf("run %d = %v, first run %v", run, ids, first)
		}
	}
	var want []string
	for id := 0; len(want) < 13; id += 6 {
		want = append(want, fmt.Sprintf(`{"_id":{"$numberInt":"%d"},"g":{"$numberInt":"0"},"h":{"$numberInt":"1"}}`, id+3))
	}
	if !slices.Equal(first, want[3:]) {
		t.Fatalf("top-k = %v\nwant %v", first, want[3:])
	}
}

func TestSortMemoryLimit(t *testing.T) {
	e, c := findEnv(t)
	p, err := e.q.PlanFind(c, Find{Sort: ej(t, `{"z": 1}`)}, Options{MaxMem: 4096})
	if err != nil {
		t.Fatal(err)
	}
	run, err := p.Start()
	if err != nil {
		t.Fatal(err)
	}
	defer run.Close()
	_, _, err = run.Next(-1, 0)
	const want = "ERR $sort exceeded memory limit of 4096 bytes (allowDiskUse is not supported)"
	if err == nil || err.Error() != want {
		t.Fatalf("error = %v, want %q", err, want)
	}
	// A top-k within the limit runs.
	_, got := e.find(c, Find{Sort: ej(t, `{"z": 1}`), Limit: 3}, Options{MaxMem: 4096})
	if len(got) != 3 {
		t.Fatalf("top-3 under the limit returned %d documents", len(got))
	}
}
