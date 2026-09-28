package query

import (
	"fmt"
	"math"
	"strings"

	"github.com/nil68657/nildb/internal/catalog"
	"github.com/nil68657/nildb/internal/doc"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// maxStages is MongoDB's limit on the stages of one pipeline.
const maxStages = 1000

// stage is one parsed pipeline stage.
type stage struct {
	name  string
	label string // EXPLAIN text

	filter bson.Raw     // $match
	sort   doc.SortSpec // $sort
	topK   int64        // $sort: keep only the first topK rows (0 = all)
	n      int64        // $limit, $skip

	// keeps reports whether the stage leaves the value at a dotted path
	// unchanged, which lets a later $match move ahead of it. nil means
	// nothing may move past the stage.
	keeps   func(path string) bool
	build   func(x *Exec, in Operator) (Operator, error)
	geoNear *geoNearSpec // a leading $geoNear, planned as the source
}

type stageParser func(arg bson.RawValue) (*stage, error)

// stageParsers is the stage table. Each operator file registers its stages
// from init, so a new stage is a new file.
var stageParsers = map[string]stageParser{}

func registerStage(name string, p stageParser) { stageParsers[name] = p }

func init() {
	registerStage("$match", parseMatchStage)
	registerStage("$project", parseProjectStage)
	registerStage("$sort", parseSortStage)
	registerStage("$limit", parseLimitStage)
	registerStage("$skip", parseSkipStage)
	registerStage("$count", parseCountStage)
}

// PlanPipeline plans an aggregation pipeline, a BSON array of stage
// documents. After parsing it moves each $match ahead of the stages that
// leave its fields alone ($sort always; $project, $addFields and $unset
// when they keep every path the filter names), merges adjacent $match,
// $limit and $skip stages, and turns $sort followed by $limit (or by $skip
// then $limit) into a top-k sort. A leading $match picks the source
// through the find planner and is applied by the source itself; a $sort
// right after it is dropped when the chosen index yields its order. A
// leading $geoNear becomes the kNN source, with a following $limit pushed
// into it.
func (e *Engine) PlanPipeline(coll *catalog.Collection, pipeline bson.Raw, o Options) (*Plan, error) {
	stages, err := parseStages(pipeline)
	if err != nil {
		return nil, err
	}
	stages = optimize(stages)
	var sp *srcPlan
	var filter *doc.Matcher
	var lead []string
	rest := stages
	if len(rest) > 0 && rest[0].geoNear != nil {
		var limit int64
		switch {
		case len(rest) > 1 && rest[1].name == "$limit":
			limit = rest[1].n
		case len(rest) > 2 && rest[1].name == "$skip" && rest[2].name == "$limit":
			limit = rest[1].n + rest[2].n
		}
		if sp, err = e.geoNearSource(coll, rest[0].geoNear, limit, o); err != nil {
			return nil, err
		}
		rest, lead = rest[1:], []string{"$geoNear"}
	} else {
		var m *doc.Matcher
		if len(rest) > 0 && rest[0].name == "$match" {
			if m, err = doc.Compile(rest[0].filter, geoHooks{}); err != nil {
				return nil, err
			}
			rest = rest[1:]
		}
		var sortSpec doc.SortSpec
		if len(rest) > 0 && rest[0].name == "$sort" {
			sortSpec = rest[0].sort
		}
		switch {
		case coll == nil:
			sp = emptySource()
		case m != nil:
			sp, err = e.source(coll, m.IndexablePredicates(), sortSpec, hintSpec{}, o)
		default:
			sp, err = e.source(coll, nil, sortSpec, hintSpec{}, o)
		}
		if err != nil {
			return nil, err
		}
		if sortSpec != nil && sp.sortOK {
			rest = rest[1:]
		}
		if m != nil && !m.MatchAll() {
			filter, lead = m, []string{"$match"}
		}
	}
	p := &Plan{e: e, opts: o, Source: sp.kind, Index: sp.index, Bounds: sp.bounds, EstimatedRows: sp.est, Stages: lead}
	for _, st := range rest {
		p.Stages = append(p.Stages, st.label)
	}
	p.build = func(x *Exec) (Operator, error) {
		op := sp.open(x, filter)
		for _, st := range rest {
			next, err := st.build(x, op)
			if err != nil {
				op.Close()
				return nil, err
			}
			op = next
		}
		return op, nil
	}
	return p, nil
}

func parseStages(pipeline bson.Raw) ([]*stage, error) {
	var out []*stage
	it := iterDoc(pipeline)
	for e, ok := it.next(); ok; e, ok = it.next() {
		if e.typ != bson.TypeEmbeddedDocument {
			return nil, exprErr(14, "Each element of the 'pipeline' array must be an object")
		}
		sit := iterDoc(e.val)
		f, ok := sit.next()
		if _, more := sit.next(); !ok || more {
			return nil, exprErr(40323, "A pipeline stage specification object must contain exactly one field.")
		}
		name := string(f.name)
		parse := stageParsers[name]
		if parse == nil {
			return nil, exprErr(40324, "unsupported pipeline stage '%s' in v1", name)
		}
		if name == "$geoNear" && len(out) > 0 {
			return nil, exprErr(40602, "$geoNear is only valid as the first stage in a pipeline.")
		}
		st, err := parse(f.value())
		if err != nil {
			return nil, err
		}
		st.name = name
		out = append(out, st)
		if len(out) > maxStages {
			return nil, exprErr(7749501, "Pipeline length must be no longer than %d stages", maxStages)
		}
	}
	if it.bad {
		return nil, exprErr(doc.CodeBadValue, "malformed pipeline")
	}
	return out, nil
}

func optimize(s []*stage) []*stage {
	for i := 1; i < len(s); i++ {
		if s[i].name != "$match" {
			continue
		}
		paths := filterPaths(s[i].filter)
		for j := i; j > 0 && s[j-1].name != "$match" && passes(s[j-1], paths); j-- {
			s[j-1], s[j] = s[j], s[j-1]
		}
	}
	var out []*stage
	for _, st := range s {
		if n := len(out); n > 0 {
			prev := out[n-1]
			switch {
			case st.name == "$match" && prev.name == "$match":
				var w builder
				w.begin()
				w.elem("$and", arrV([]bson.RawValue{docV(prev.filter), docV(st.filter)}))
				prev.filter = w.end()
				continue
			case st.name == "$limit" && prev.name == "$limit":
				prev.n = min(prev.n, st.n)
				prev.label = fmt.Sprintf("$limit %d", prev.n)
				continue
			case st.name == "$skip" && prev.name == "$skip":
				prev.n += st.n
				prev.label = fmt.Sprintf("$skip %d", prev.n)
				continue
			}
		}
		out = append(out, st)
	}
	for i, st := range out {
		if st.name != "$sort" {
			continue
		}
		switch {
		case i+1 < len(out) && out[i+1].name == "$limit":
			st.topK = out[i+1].n
		case i+2 < len(out) && out[i+1].name == "$skip" && out[i+2].name == "$limit":
			st.topK = out[i+1].n + out[i+2].n
		}
		st.label = sortLabel(st.topK)
	}
	return out
}

func passes(prev *stage, paths []string) bool {
	if prev.keeps == nil {
		return false
	}
	for _, p := range paths {
		if !prev.keeps(p) {
			return false
		}
	}
	return true
}

// filterPaths returns the field paths a filter tests, through $and, $or
// and $nor.
func filterPaths(d []byte) []string {
	var out []string
	it := iterDoc(d)
	for e, ok := it.next(); ok; e, ok = it.next() {
		name := string(e.name)
		if !strings.HasPrefix(name, "$") {
			out = append(out, name)
			continue
		}
		if (name == "$and" || name == "$or" || name == "$nor") && e.typ == bson.TypeArray {
			for _, v := range arrayValues(e.val) {
				if v.Type == bson.TypeEmbeddedDocument {
					out = append(out, filterPaths(v.Value)...)
				}
			}
		}
	}
	return out
}

func parseMatchStage(arg bson.RawValue) (*stage, error) {
	if arg.Type != bson.TypeEmbeddedDocument {
		return nil, exprErr(15959, "the match filter must be an expression in an object")
	}
	if _, err := doc.Compile(arg.Value, geoHooks{}); err != nil {
		return nil, err
	}
	st := &stage{label: "$match", filter: arg.Value}
	st.build = func(x *Exec, in Operator) (Operator, error) {
		m, err := doc.Compile(st.filter, geoHooks{})
		if err != nil {
			return nil, err
		}
		return &matchOp{x: x, in: in, m: m}, nil
	}
	return st, nil
}

func parseSortStage(arg bson.RawValue) (*stage, error) {
	if arg.Type != bson.TypeEmbeddedDocument {
		return nil, exprErr(15973, "the $sort key specification must be an object")
	}
	spec, err := doc.ParseSort(arg.Value)
	if err != nil {
		return nil, err
	}
	if len(spec) == 0 {
		return nil, exprErr(15976, "$sort stage must have at least one sort key")
	}
	st := &stage{label: "$sort", sort: spec, keeps: func(string) bool { return true }}
	st.build = func(x *Exec, in Operator) (Operator, error) {
		return newSortOp(x, in, st.sort, st.topK), nil
	}
	return st, nil
}

// wholeCount parses the argument of $limit or $skip.
func wholeCount(arg bson.RawValue) (int64, bool) {
	n, ok := numOf(arg)
	if !ok {
		return 0, false
	}
	f := n.float()
	if n.isInt() {
		return n.i, true
	}
	if f != math.Trunc(f) || math.IsNaN(f) {
		return 0, false
	}
	if f >= math.MaxInt64 {
		return math.MaxInt64, true
	}
	return int64(f), true
}

func parseLimitStage(arg bson.RawValue) (*stage, error) {
	if _, ok := numOf(arg); !ok {
		return nil, exprErr(15957, "the limit must be specified as a number")
	}
	n, ok := wholeCount(arg)
	if !ok || n <= 0 {
		return nil, exprErr(15958, "the limit must be positive")
	}
	st := &stage{label: fmt.Sprintf("$limit %d", n), n: n}
	st.build = func(x *Exec, in Operator) (Operator, error) { return &limitOp{in: in, n: st.n}, nil }
	return st, nil
}

func parseSkipStage(arg bson.RawValue) (*stage, error) {
	if _, ok := numOf(arg); !ok {
		return nil, exprErr(15972, "the skip must be specified as a number")
	}
	n, ok := wholeCount(arg)
	if !ok || n < 0 {
		return nil, exprErr(15956, "the number to skip cannot be negative")
	}
	st := &stage{label: fmt.Sprintf("$skip %d", n), n: n}
	st.build = func(x *Exec, in Operator) (Operator, error) { return &skipOp{in: in, n: st.n}, nil }
	return st, nil
}

func parseCountStage(arg bson.RawValue) (*stage, error) {
	name, err := parseCountName(arg)
	if err != nil {
		return nil, err
	}
	return &stage{label: "$count", build: func(x *Exec, in Operator) (Operator, error) {
		return &countOp{x: x, in: in, name: name}, nil
	}}, nil
}
