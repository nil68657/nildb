package query

import (
	"fmt"
	"math"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/nil68657/nildb/internal/catalog"
	"github.com/nil68657/nildb/internal/doc"
	"go.mongodb.org/mongo-driver/v2/bson"
)

const aggN = 2000

// aggDoc is document i of the aggregation fixture.
func aggDoc(i int) bson.D {
	d := bson.D{
		{Key: "_id", Value: int32(i)},
		{Key: "cat", Value: fmt.Sprintf("c%d", i%7)},
		{Key: "qty", Value: int32(i % 50)},
		{Key: "price", Value: float64(i%20) + 0.5},
		{Key: "big", Value: int64(i) * 1_000_000_000},
		{Key: "when", Value: bson.DateTime(time.Date(2020+i%3, time.Month(1+i%12), 1+i%28, 0, 0, 0, 0, time.UTC).UnixMilli())},
		{Key: "sub", Value: bson.D{{Key: "x", Value: int32(i % 3)}, {Key: "y", Value: fmt.Sprintf("y%d", i%4)}}},
		{Key: "flag", Value: i%2 == 0},
	}
	switch {
	case i%9 == 0:
		d = append(d, bson.E{Key: "tags", Value: bson.A{}})
	case i%5 == 0:
	default:
		d = append(d, bson.E{Key: "tags", Value: bson.A{fmt.Sprintf("t%d", i%4), fmt.Sprintf("t%d", (i+1)%4)}})
	}
	if i%3 == 0 {
		d = append(d, bson.E{Key: "opt", Value: fmt.Sprintf("o%d", i%2)})
	}
	return d
}

func aggEnv(t *testing.T) (*tenv, *catalog.Collection) {
	e := newEnv(t)
	c := e.coll("test.agg")
	e.index(c, btree("cat"))
	e.index(c, btree("qty"))
	var docs []bson.Raw
	for i := range aggN {
		docs = append(docs, mustMarshal(t, aggDoc(i)))
	}
	e.insert(c, docs...)
	return e, c
}

// The brute-force evaluator below works on decoded bson.D values and
// shares no code with the operators it checks, apart from the reference
// matcher for $match.

type missingT struct{}

var bfMissing any = missingT{}

func isMissing(v any) bool { _, ok := v.(missingT); return ok }

func bfPath(v any, path []string) any {
	if len(path) == 0 {
		return v
	}
	switch x := v.(type) {
	case bson.D:
		for _, e := range x {
			if e.Key == path[0] {
				return bfPath(e.Value, path[1:])
			}
		}
	case bson.A:
		out := bson.A{}
		for _, el := range x {
			switch el.(type) {
			case bson.D, bson.A:
				if r := bfPath(el, path); !isMissing(r) {
					out = append(out, r)
				}
			}
		}
		return out
	}
	return bfMissing
}

// bfNum returns a number as (int64, float64, kind): kind 0 int32, 1
// int64, 2 double.
func bfNum(v any) (int64, float64, int, bool) {
	switch x := v.(type) {
	case int32:
		return int64(x), float64(x), 0, true
	case int64:
		return x, float64(x), 1, true
	case float64:
		return 0, x, 2, true
	}
	return 0, 0, 0, false
}

func bfInt(kind int, v int64) any {
	if kind == 0 && v >= math.MinInt32 && v <= math.MaxInt32 {
		return int32(v)
	}
	return v
}

func bfNullish(v any) bool { return v == nil || isMissing(v) }

func bfTruthy(v any) bool {
	switch x := v.(type) {
	case nil, missingT:
		return false
	case bool:
		return x
	}
	if _, f, _, ok := bfNum(v); ok {
		return f != 0
	}
	return true
}

func bfRank(v any) int {
	switch v.(type) {
	case nil, missingT:
		return 0
	case int32, int64, float64:
		return 1
	case string:
		return 2
	case bson.D:
		return 3
	case bson.A:
		return 4
	case bool:
		return 5
	case bson.DateTime:
		return 6
	}
	return 7
}

func bfCmp(a, b any) int {
	if ra, rb := bfRank(a), bfRank(b); ra != rb {
		return ra - rb
	}
	switch x := a.(type) {
	case string:
		return strings.Compare(x, b.(string))
	case bool:
		switch {
		case x == b.(bool):
			return 0
		case !x:
			return -1
		}
		return 1
	case bson.DateTime:
		return int(x - b.(bson.DateTime))
	}
	if _, fa, _, ok := bfNum(a); ok {
		_, fb, _, _ := bfNum(b)
		switch {
		case fa < fb:
			return -1
		case fa > fb:
			return 1
		}
	}
	return 0
}

func bfEval(e any, root bson.D) any {
	switch x := e.(type) {
	case string:
		if strings.HasPrefix(x, "$") {
			return bfPath(root, strings.Split(x[1:], "."))
		}
		return x
	case bson.D:
		if len(x) == 1 && strings.HasPrefix(x[0].Key, "$") {
			return bfOp(x[0].Key, x[0].Value, root)
		}
		out := bson.D{}
		for _, f := range x {
			if v := bfEval(f.Value, root); !isMissing(v) {
				out = append(out, bson.E{Key: f.Key, Value: v})
			}
		}
		return out
	case bson.A:
		out := bson.A{}
		for _, el := range x {
			v := bfEval(el, root)
			if isMissing(v) {
				v = nil
			}
			out = append(out, v)
		}
		return out
	}
	return e
}

func bfOp(op string, arg any, root bson.D) any {
	var vs []any
	if a, ok := arg.(bson.A); ok {
		for _, el := range a {
			vs = append(vs, bfEval(el, root))
		}
	} else {
		vs = []any{bfEval(arg, root)}
	}
	arith := func(unit int64, iop func(a, b int64) int64, fop func(a, b float64) float64) any {
		kind, acc, facc := 0, unit, float64(unit)
		for _, v := range vs {
			if bfNullish(v) {
				return nil
			}
			i, f, k, _ := bfNum(v)
			kind = max(kind, k)
			acc, facc = iop(acc, i), fop(facc, f)
		}
		if kind == 2 {
			return facc
		}
		return bfInt(kind, acc)
	}
	switch op {
	case "$add":
		return arith(0, func(a, b int64) int64 { return a + b }, func(a, b float64) float64 { return a + b })
	case "$multiply":
		return arith(1, func(a, b int64) int64 { return a * b }, func(a, b float64) float64 { return a * b })
	case "$divide":
		_, a, _, _ := bfNum(vs[0])
		_, b, _, _ := bfNum(vs[1])
		return a / b
	case "$mod":
		ia, fa, ka, _ := bfNum(vs[0])
		ib, fb, kb, _ := bfNum(vs[1])
		if max(ka, kb) == 2 {
			return math.Mod(fa, fb)
		}
		return bfInt(max(ka, kb), ia%ib)
	case "$gt":
		return bfCmp(vs[0], vs[1]) > 0
	case "$eq":
		return bfCmp(vs[0], vs[1]) == 0
	case "$cond":
		if bfTruthy(vs[0]) {
			return vs[1]
		}
		return vs[2]
	case "$ifNull":
		if bfNullish(vs[0]) {
			return vs[1]
		}
		return vs[0]
	case "$concat":
		var b strings.Builder
		for _, v := range vs {
			if bfNullish(v) {
				return nil
			}
			b.WriteString(v.(string))
		}
		return b.String()
	case "$toUpper":
		if bfNullish(vs[0]) {
			return ""
		}
		return strings.ToUpper(vs[0].(string))
	case "$size":
		return int32(len(vs[0].(bson.A)))
	case "$year":
		return int32(vs[0].(bson.DateTime).Time().UTC().Year())
	case "$month":
		return int32(vs[0].(bson.DateTime).Time().UTC().Month())
	}
	panic("brute force: no operator " + op)
}

type bfAcc struct {
	op        string
	kind      int
	isum      int64
	fsum      float64
	n         int64
	v         any
	set       bool
	vals      bson.A
	seen      map[string]bool
	anyDouble bool
}

func (a *bfAcc) add(v any) {
	switch a.op {
	case "$sum", "$avg":
		if i, f, k, ok := bfNum(v); ok {
			a.n++
			a.kind = max(a.kind, k)
			if k == 2 {
				a.fsum += f
				a.anyDouble = true
			} else {
				a.isum += i
			}
		}
	case "$min", "$max":
		if bfNullish(v) {
			return
		}
		if !a.set || (a.op == "$min" && bfCmp(v, a.v) < 0) || (a.op == "$max" && bfCmp(v, a.v) > 0) {
			a.v, a.set = v, true
		}
	case "$count":
		a.n++
	case "$first":
		if !a.set {
			if isMissing(v) {
				v = nil
			}
			a.v, a.set = v, true
		}
	case "$last":
		if isMissing(v) {
			v = nil
		}
		a.v = v
	case "$push":
		if !isMissing(v) {
			a.vals = append(a.vals, v)
		}
	case "$addToSet":
		if isMissing(v) {
			return
		}
		k := ejsonOf(mustMarshalValue(v))
		if !a.seen[k] {
			a.seen[k] = true
			a.vals = append(a.vals, v)
		}
	}
}

func mustMarshalValue(v any) bson.Raw {
	b, err := bson.Marshal(bson.D{{Key: "v", Value: v}})
	if err != nil {
		panic(err)
	}
	return b
}

func (a *bfAcc) result() any {
	switch a.op {
	case "$sum":
		switch {
		case a.n == 0:
			return int32(0)
		case a.anyDouble:
			return float64(a.isum) + a.fsum
		}
		return bfInt(a.kind, a.isum)
	case "$avg":
		if a.n == 0 {
			return nil
		}
		return (float64(a.isum) + a.fsum) / float64(a.n)
	case "$min", "$max", "$first", "$last":
		return a.v
	case "$count":
		return int32(a.n)
	}
	if a.vals == nil {
		return bson.A{}
	}
	return a.vals
}

func bfSetPath(d bson.D, path []string, v any) bson.D {
	for i, e := range d {
		if e.Key != path[0] {
			continue
		}
		if len(path) == 1 {
			d[i].Value = v
			return d
		}
		sub, _ := e.Value.(bson.D)
		d[i].Value = bfSetPath(slices.Clone(sub), path[1:], v)
		return d
	}
	if len(path) == 1 {
		return append(d, bson.E{Key: path[0], Value: v})
	}
	return append(d, bson.E{Key: path[0], Value: bfSetPath(bson.D{}, path[1:], v)})
}

// bruteAgg runs a pipeline over docs, decoded, stage by stage.
func bruteAgg(t *testing.T, docs []bson.Raw, pipeline string) []bson.D {
	t.Helper()
	var rows []bson.D
	for _, d := range docs {
		var x bson.D
		if err := bson.Unmarshal(d, &x); err != nil {
			t.Fatal(err)
		}
		rows = append(rows, x)
	}
	var wrap struct {
		P bson.A `bson:"p"`
	}
	if err := bson.Unmarshal(ej(t, `{"p":`+pipeline+`}`), &wrap); err != nil {
		t.Fatal(err)
	}
	for _, s := range wrap.P {
		st := s.(bson.D)[0]
		switch st.Key {
		case "$match":
			m, err := doc.Compile(mustMarshal(t, st.Value), nil)
			if err != nil {
				t.Fatal(err)
			}
			var out []bson.D
			for _, r := range rows {
				if ok, _ := m.Match(mustMarshal(t, r)); ok {
					out = append(out, r)
				}
			}
			rows = out
		case "$group":
			spec := st.Value.(bson.D)
			var order []string
			groups := map[string]bson.D{}
			accs := map[string][]*bfAcc{}
			for _, r := range rows {
				id := bfEval(spec[0].Value, r)
				if isMissing(id) {
					id = nil
				}
				k := ejsonOf(mustMarshalValue(id))
				if _, ok := groups[k]; !ok {
					order = append(order, k)
					groups[k] = bson.D{{Key: "_id", Value: id}}
					for _, f := range spec[1:] {
						a := f.Value.(bson.D)[0]
						accs[k] = append(accs[k], &bfAcc{op: a.Key, seen: map[string]bool{}})
					}
				}
				for i, f := range spec[1:] {
					a := f.Value.(bson.D)[0]
					v := bfMissing
					if a.Key != "$count" {
						v = bfEval(a.Value, r)
					}
					accs[k][i].add(v)
				}
			}
			var out []bson.D
			for _, k := range order {
				g := groups[k]
				for i, f := range spec[1:] {
					g = append(g, bson.E{Key: f.Key, Value: accs[k][i].result()})
				}
				out = append(out, g)
			}
			rows = out
		case "$project":
			spec := st.Value.(bson.D)
			keepID, include := true, map[string]bool{}
			var computed []bson.E
			for _, f := range spec {
				switch v := f.Value.(type) {
				case int32, bool:
					if bfTruthy(v) {
						include[f.Key] = true
					} else if f.Key == "_id" {
						keepID = false
					}
				default:
					computed = append(computed, f)
				}
			}
			for i, r := range rows {
				out := bson.D{}
				for _, e := range r {
					if (e.Key == "_id" && keepID) || include[e.Key] {
						out = append(out, e)
					}
				}
				for _, c := range computed {
					if v := bfEval(c.Value, r); !isMissing(v) {
						out = append(out, bson.E{Key: c.Key, Value: v})
					}
				}
				rows[i] = out
			}
		case "$addFields", "$set":
			for i, r := range rows {
				out := slices.Clone(r)
				for _, f := range st.Value.(bson.D) {
					out = bfSetPath(out, strings.Split(f.Key, "."), bfEval(f.Value, r))
				}
				rows[i] = out
			}
		case "$unset":
			drop := map[string]bool{}
			for _, f := range st.Value.(bson.A) {
				drop[f.(string)] = true
			}
			for i, r := range rows {
				rows[i] = slices.DeleteFunc(slices.Clone(r), func(e bson.E) bool { return drop[e.Key] })
			}
		case "$sort":
			spec := st.Value.(bson.D)
			slices.SortStableFunc(rows, func(a, b bson.D) int {
				for _, f := range spec {
					c := bfCmp(bfPath(a, strings.Split(f.Key, ".")), bfPath(b, strings.Split(f.Key, ".")))
					if f.Value.(int32) < 0 {
						c = -c
					}
					if c != 0 {
						return c
					}
				}
				return bfCmp(bfPath(a, []string{"_id"}), bfPath(b, []string{"_id"}))
			})
		case "$limit":
			rows = rows[:min(int(st.Value.(int32)), len(rows))]
		case "$skip":
			rows = rows[min(int(st.Value.(int32)), len(rows)):]
		case "$count":
			if len(rows) == 0 {
				rows = nil
			} else {
				rows = []bson.D{{{Key: st.Value.(string), Value: int32(len(rows))}}}
			}
		case "$unwind":
			path, index, preserve := "", "", false
			switch v := st.Value.(type) {
			case string:
				path = v
			case bson.D:
				for _, f := range v {
					switch f.Key {
					case "path":
						path = f.Value.(string)
					case "includeArrayIndex":
						index = f.Value.(string)
					case "preserveNullAndEmptyArrays":
						preserve = f.Value.(bool)
					}
				}
			}
			field := path[1:]
			var out []bson.D
			for _, r := range rows {
				v := bfPath(r, []string{field})
				arr, isArr := v.(bson.A)
				switch {
				case isArr && len(arr) > 0:
					for i, el := range arr {
						nd := bfSetPath(slices.Clone(r), []string{field}, el)
						if index != "" {
							nd = bfSetPath(nd, []string{index}, int64(i))
						}
						out = append(out, nd)
					}
				case isArr || bfNullish(v):
					if preserve {
						nd := slices.Clone(r)
						if isArr {
							nd = slices.DeleteFunc(nd, func(e bson.E) bool { return e.Key == field })
						}
						if index != "" {
							nd = bfSetPath(nd, []string{index}, nil)
						}
						out = append(out, nd)
					}
				default:
					nd := slices.Clone(r)
					if index != "" {
						nd = bfSetPath(nd, []string{index}, nil)
					}
					out = append(out, nd)
				}
			}
			rows = out
		default:
			t.Fatalf("brute force has no stage %s", st.Key)
		}
	}
	return rows
}

func bfEJSON(t *testing.T, rows []bson.D) []string {
	out := make([]string, len(rows))
	for i, r := range rows {
		out[i] = ejsonOf(mustMarshal(t, r))
	}
	return out
}

var aggPipelines = []struct {
	pipeline string
	ordered  bool
}{
	{`[{"$match": {"cat": "c1"}}]`, false},
	{`[{"$group": {"_id": "$cat", "n": {"$sum": 1}}}]`, false},
	{`[{"$group": {"_id": "$cat", "total": {"$sum": "$qty"}, "avg": {"$avg": "$price"}}}]`, false},
	{`[{"$group": {"_id": null, "mn": {"$min": "$qty"}, "mx": {"$max": "$price"}, "c": {"$count": {}}}}]`, false},
	{`[{"$match": {"qty": {"$gt": 10}}}, {"$group": {"_id": "$sub.x", "c": {"$count": {}}}}]`, false},
	{`[{"$sort": {"qty": -1, "_id": 1}}, {"$limit": 5}]`, true},
	{`[{"$sort": {"price": 1}}, {"$skip": 10}, {"$limit": 7}]`, true},
	{`[{"$project": {"cat": 1, "qty": 1}}]`, false},
	{`[{"$project": {"_id": 0, "cat": 1, "double": {"$multiply": ["$qty", 2]}}}, {"$limit": 50}]`, true},
	{`[{"$addFields": {"total": {"$multiply": ["$qty", "$price"]}}}, {"$match": {"total": {"$gt": 100}}}, {"$count": "n"}]`, false},
	{`[{"$unwind": "$tags"}, {"$group": {"_id": "$tags", "n": {"$sum": 1}}}]`, false},
	{`[{"$unwind": {"path": "$tags", "includeArrayIndex": "i", "preserveNullAndEmptyArrays": true}}, {"$match": {"_id": {"$lt": 30}}}]`, true},
	{`[{"$group": {"_id": {"c": "$cat", "x": "$sub.x"}, "q": {"$sum": "$qty"}}}]`, false},
	{`[{"$group": {"_id": "$cat", "first": {"$first": "$qty"}, "last": {"$last": "$qty"}}}]`, false},
	{`[{"$match": {"cat": "c2"}}, {"$group": {"_id": "$flag", "ids": {"$push": "$_id"}}}]`, false},
	{`[{"$group": {"_id": "$cat", "ys": {"$addToSet": "$sub.y"}}}]`, false},
	{`[{"$set": {"yr": {"$year": "$when"}, "mo": {"$month": "$when"}}}, {"$group": {"_id": "$yr", "n": {"$sum": 1}, "mo": {"$max": "$mo"}}}]`, false},
	{`[{"$project": {"up": {"$toUpper": "$cat"}, "cat": 1}}, {"$match": {"up": "C3"}}]`, false},
	{`[{"$project": {"label": {"$concat": ["$cat", "-", "$sub.y"]}}}]`, false},
	{`[{"$project": {"q": {"$cond": [{"$gt": ["$qty", 25]}, "high", "low"]}}}, {"$group": {"_id": "$q", "n": {"$sum": 1}}}]`, false},
	{`[{"$project": {"o": {"$ifNull": ["$opt", "none"]}}}, {"$group": {"_id": "$o", "n": {"$sum": 1}}}]`, false},
	{`[{"$unset": ["tags", "sub"]}, {"$limit": 3}]`, true},
	{`[{"$group": {"_id": "$cat", "big": {"$sum": "$big"}}}]`, false},
	{`[{"$project": {"r": {"$divide": ["$qty", 4]}, "m": {"$mod": ["$qty", 7]}}}, {"$limit": 20}]`, true},
	{`[{"$project": {"n": {"$size": {"$ifNull": ["$tags", []]}}}}, {"$group": {"_id": "$n", "c": {"$sum": 1}}}]`, false},
	{`[{"$match": {"qty": {"$gte": 5, "$lt": 9}}}, {"$sort": {"qty": 1}}]`, true},
	{`[{"$count": "all"}]`, false},
	{`[{"$match": {"cat": {"$in": ["c1", "c4"]}}}, {"$group": {"_id": "$cat", "s": {"$sum": "$price"}}}, {"$sort": {"s": -1}}]`, true},
	{`[{"$group": {"_id": "$cat", "n": {"$sum": 1}}}, {"$match": {"n": {"$gt": 285}}}]`, false},
	{`[{"$addFields": {"sub.z": {"$add": ["$sub.x", 10]}}}, {"$project": {"sub": 1}}, {"$limit": 4}]`, true},
	{`[{"$sort": {"cat": 1, "qty": -1}}, {"$group": {"_id": "$cat", "top": {"$first": "$qty"}}}]`, false},
	{`[{"$match": {"tags": "t1"}}, {"$project": {"tags": 1}}]`, false},
	{`[{"$match": {"flag": true, "qty": {"$lt": 3}}}, {"$sort": {"price": -1, "_id": -1}}, {"$project": {"price": 1, "flag": 1}}]`, true},
}

func TestPipelinesMatchBruteForce(t *testing.T) {
	e, c := aggEnv(t)
	if len(aggPipelines) < 30 {
		t.Fatalf("only %d pipelines", len(aggPipelines))
	}
	all := e.all(c)
	for i, tc := range aggPipelines {
		t.Run(fmt.Sprintf("%02d", i), func(t *testing.T) {
			_, got, err := e.aggregate(c, tc.pipeline, Options{})
			if err != nil {
				t.Fatalf("%s: %v", tc.pipeline, err)
			}
			g, w := ejsonList(got), bfEJSON(t, bruteAgg(t, all, tc.pipeline))
			if !tc.ordered {
				slices.Sort(g)
				slices.Sort(w)
			}
			if !slices.Equal(g, w) {
				t.Fatalf("%s:\n got %d %v\nwant %d %v", tc.pipeline, len(g), head(g), len(w), head(w))
			}
		})
	}
}

func head(s []string) []string { return s[:min(len(s), 6)] }

func TestPipelineExplain(t *testing.T) {
	e, c := aggEnv(t)
	cases := []struct {
		pipeline, plan, index string
		stages                []string
	}{
		{`[{"$sort": {"qty": 1}}, {"$match": {"cat": "c2"}}]`, "index", "cat_1", []string{"$match", "$sort"}},
		{`[{"$match": {"qty": {"$gt": 40}}}, {"$sort": {"qty": -1}}, {"$limit": 3}]`, "index", "qty_1", []string{"$match", "$limit 3"}},
		{`[{"$project": {"cat": 1, "qty": 1}}, {"$match": {"qty": 7}}]`, "index", "qty_1", []string{"$match", "$project"}},
		{`[{"$addFields": {"qty": 1}}, {"$match": {"qty": 1}}]`, "rowscan", "", []string{"$addFields", "$match"}},
		{`[{"$sort": {"price": 1}}, {"$skip": 5}, {"$limit": 5}]`, "rowscan", "", []string{"$sort (top 10)", "$skip 5", "$limit 5"}},
		{`[{"$match": {"cat": "c1"}}, {"$match": {"qty": 3}}, {"$limit": 9}, {"$limit": 4}]`, "index", "cat_1", []string{"$match", "$limit 4"}},
		{`[{"$group": {"_id": "$cat"}}, {"$match": {"_id": "c1"}}]`, "rowscan", "", []string{"$group", "$match"}},
		{`[{"$unwind": "$tags"}, {"$count": "n"}]`, "rowscan", "", []string{"$unwind $tags", "$count"}},
	}
	for _, tc := range cases {
		p, err := e.q.PlanPipeline(c, pipe(t, tc.pipeline), Options{})
		if err != nil {
			t.Fatalf("%s: %v", tc.pipeline, err)
		}
		if p.Source != tc.plan || p.Index != tc.index || !slices.Equal(p.Stages, tc.stages) {
			t.Errorf("%s: plan %s %q %q, want %s %q %q", tc.pipeline, p.Source, p.Index, p.Stages, tc.plan, tc.index, tc.stages)
		}
	}
}

func TestPipelineErrors(t *testing.T) {
	e, c := aggEnv(t)
	cases := []struct{ pipeline, want string }{
		{`[{"$lookup": {"from": "x"}}]`, "ERR unsupported pipeline stage '$lookup' in v1"},
		{`[{"$project": {"x": {"$sqrt": 4}}}]`, "ERR unsupported expression operator '$sqrt' in v1"},
		{`[{"$group": {"_id": null, "s": {"$stdDevPop": "$qty"}}}]`, "ERR unsupported accumulator '$stdDevPop' in v1"},
		{`[{"$match": {}}, {"$geoNear": {"near": [0, 0], "distanceField": "d", "spherical": true}}]`, "ERR $geoNear is only valid as the first stage in a pipeline."},
		{`[{"$group": {"n": {"$sum": 1}}}]`, "ERR a group specification must include an _id"},
		{`[{"$limit": 0}]`, "ERR the limit must be positive"},
		{`[{"$match": {"loc": {"$near": [0, 0]}}}]`, "ERR $geoNear, $near, and $nearSphere are not allowed in this context"},
		{`[{"$unwind": "tags"}]`, "ERR path option to $unwind stage should be prefixed with a '$': tags"},
		{`[{"$project": {"a": 0, "b": {"$literal": 1}}}]`, "ERR Invalid $project :: caused by :: Cannot do exclusion on field a in inclusion projection"},
	}
	for _, tc := range cases {
		_, err := e.q.PlanPipeline(c, pipe(t, tc.pipeline), Options{})
		if err == nil || err.Error() != tc.want {
			t.Errorf("%s: error %v, want %q", tc.pipeline, err, tc.want)
		}
	}
	_, _, err := e.aggregate(c, `[{"$project": {"x": {"$divide": [1, 0]}}}]`, Options{})
	if err == nil || err.Error() != "ERR can't $divide by zero" {
		t.Errorf("divide by zero: %v", err)
	}
}

func TestGroupMemoryCap(t *testing.T) {
	e, c := aggEnv(t)
	const limit = 50 << 10
	_, _, err := e.aggregate(c, `[{"$group": {"_id": "$_id", "all": {"$push": "$$ROOT"}}}]`, Options{MaxMem: limit})
	want := fmt.Sprintf("ERR $group exceeded memory limit of %d bytes (allowDiskUse is not supported)", limit)
	if err == nil || err.Error() != want {
		t.Fatalf("error %v, want %q", err, want)
	}
	// Seven groups fit easily.
	if _, got, err := e.aggregate(c, `[{"$group": {"_id": "$cat", "n": {"$sum": 1}}}]`, Options{MaxMem: limit}); err != nil || len(got) != 7 {
		t.Fatalf("small group: %d groups, err %v", len(got), err)
	}
}

// TestGroupNumericKeys checks that 1, int64(1) and 1.0 fall in one group.
func TestGroupNumericKeys(t *testing.T) {
	e := newEnv(t)
	c := e.coll("test.keys")
	e.insert(c,
		mustMarshal(t, bson.D{{Key: "_id", Value: int32(1)}, {Key: "k", Value: int32(1)}}),
		mustMarshal(t, bson.D{{Key: "_id", Value: int32(2)}, {Key: "k", Value: int64(1)}}),
		mustMarshal(t, bson.D{{Key: "_id", Value: int32(3)}, {Key: "k", Value: 1.0}}),
		mustMarshal(t, bson.D{{Key: "_id", Value: int32(4)}, {Key: "k", Value: "1"}}),
	)
	_, got, err := e.aggregate(c, `[{"$group": {"_id": "$k", "n": {"$sum": 1}}}]`, Options{})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{`{"_id":{"$numberInt":"1"},"n":{"$numberInt":"3"}}`, `{"_id":"1","n":{"$numberInt":"1"}}`}
	if g := ejsonList(got); !slices.Equal(g, want) {
		t.Fatalf("groups = %v, want %v", g, want)
	}
}
