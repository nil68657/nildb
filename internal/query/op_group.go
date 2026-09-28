package query

import (
	"errors"
	"math"
	"strings"

	"github.com/nil68657/nildb/internal/doc"
	"github.com/nil68657/nildb/internal/keyenc"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func init() { registerStage("$group", parseGroupStage) }

// groupSpec is a parsed $group: the _id expression and the accumulators.
type groupSpec struct {
	id     Expr
	fields []groupField
}

type groupField struct {
	name string
	op   string // "$sum", "$avg", ...
	expr Expr   // nil for $count
}

// accumulators lists the v1 accumulators; mongoAccumulators the ones
// MongoDB has that v1 does not.
var (
	accumulators      = map[string]bool{"$sum": true, "$avg": true, "$min": true, "$max": true, "$count": true, "$first": true, "$last": true, "$push": true, "$addToSet": true}
	mongoAccumulators = map[string]bool{"$stdDevPop": true, "$stdDevSamp": true, "$mergeObjects": true, "$top": true, "$topN": true, "$bottom": true, "$bottomN": true, "$firstN": true, "$lastN": true, "$maxN": true, "$minN": true, "$median": true, "$percentile": true, "$accumulator": true, "$concatArrays": true, "$setUnion": true}
)

func parseGroupStage(arg bson.RawValue) (*stage, error) {
	g, err := parseGroup(arg)
	if err != nil {
		return nil, err
	}
	return &stage{label: "$group", build: func(x *Exec, in Operator) (Operator, error) {
		return &groupOp{x: x, in: in, spec: g, index: make(map[string]int)}, nil
	}}, nil
}

func parseGroup(arg bson.RawValue) (*groupSpec, error) {
	if arg.Type != bson.TypeEmbeddedDocument {
		return nil, exprErr(15947, "a group's fields must be specified in an object")
	}
	g := &groupSpec{}
	it := iterDoc(arg.Value)
	for e, ok := it.next(); ok; e, ok = it.next() {
		name := string(e.name)
		if name == "_id" {
			id, err := compileExpr(e.value())
			if err != nil {
				return nil, err
			}
			g.id = id
			continue
		}
		switch {
		case strings.Contains(name, "."):
			return nil, exprErr(40235, "The field name '%s' cannot contain '.'", name)
		case strings.HasPrefix(name, "$"):
			return nil, exprErr(40236, "The field name '%s' cannot be an operator name", name)
		case e.typ != bson.TypeEmbeddedDocument:
			return nil, exprErr(40234, "The field '%s' must be an accumulator object", name)
		}
		ait := iterDoc(e.val)
		acc, ok := ait.next()
		if !ok {
			return nil, exprErr(40238, "The field '%s' must specify one accumulator", name)
		}
		if _, more := ait.next(); more {
			return nil, exprErr(40238, "The field '%s' must specify one accumulator", name)
		}
		op := string(acc.name)
		switch {
		case mongoAccumulators[op]:
			return nil, exprErr(15952, "unsupported accumulator '%s' in v1", op)
		case !accumulators[op]:
			return nil, exprErr(15952, "unknown group operator '%s'", op)
		}
		f := groupField{name: name, op: op}
		if op == "$count" {
			if acc.typ != bson.TypeEmbeddedDocument || !emptyDoc(acc.val) {
				return nil, exprErr(doc.CodeBadValue, "$count takes no arguments, i.e. $count:{}")
			}
		} else {
			if acc.typ == bson.TypeArray {
				return nil, exprErr(40237, "The %s accumulator is a unary operator", op)
			}
			ex, err := compileExpr(acc.value())
			if err != nil {
				return nil, err
			}
			f.expr = ex
		}
		g.fields = append(g.fields, f)
	}
	if it.bad {
		return nil, exprErr(doc.CodeBadValue, "malformed $group specification")
	}
	if g.id == nil {
		return nil, exprErr(15955, "a group specification must include an _id")
	}
	return g, nil
}

// groupOp is the hash aggregate: groups keyed by keyenc of the group _id,
// so 1, int64(1) and 1.0 fall in one group as MongoDB requires, emitted in
// first-seen order. Every byte of state (keys, _id values, accumulators)
// is charged to the run's Accountant under "$group", and each group's
// share is returned as it is emitted.
type groupOp struct {
	x     *Exec
	in    Operator
	spec  *groupSpec
	index map[string]int

	groups  []*group
	pos     int
	ready   bool
	charged int64
}

type group struct {
	key  []byte
	id   bson.RawValue
	accs []accumulator
	cost int64
}

const groupOverhead = 96

func (o *groupOp) grow(g *group, n int64) error {
	g.cost += n
	o.charged += n
	return o.x.mem.Grow("$group", n)
}

func (o *groupOp) fill() error {
	defer o.in.Close()
	n := 0
	for {
		r, ok, err := o.in.Next()
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
		if n++; n&1023 == 0 {
			if err := o.x.checkDeadline(); err != nil {
				return err
			}
		}
		idv, err := o.spec.id.eval(r.Doc)
		if err != nil {
			return err
		}
		if idv.Type == 0 {
			idv = nullV
		}
		key, err := keyenc.Encode(nil, idv)
		if err != nil {
			if errors.Is(err, keyenc.ErrUnsupported) {
				return errDecimal
			}
			return exprErr(doc.CodeBadValue, "cannot group by a malformed value")
		}
		gi, seen := o.index[string(key)]
		if !seen {
			g := &group{key: key, id: bson.RawValue{Type: idv.Type, Value: clone(idv.Value)}, accs: make([]accumulator, len(o.spec.fields))}
			for i, f := range o.spec.fields {
				g.accs[i] = newAccumulator(f.op)
			}
			gi = len(o.groups)
			o.index[string(key)] = gi
			o.groups = append(o.groups, g)
			if err := o.grow(g, int64(2*len(key)+len(idv.Value)+groupOverhead+48*len(o.spec.fields))); err != nil {
				return err
			}
		}
		g := o.groups[gi]
		for i, f := range o.spec.fields {
			var v bson.RawValue
			if f.expr != nil {
				if v, err = f.expr.eval(r.Doc); err != nil {
					return err
				}
			}
			delta, err := g.accs[i].add(v)
			if err != nil {
				return err
			}
			if delta != 0 {
				if err := o.grow(g, delta); err != nil {
					return err
				}
			}
		}
	}
}

func (o *groupOp) Next() (Row, bool, error) {
	if !o.ready {
		o.ready = true
		if err := o.fill(); err != nil {
			return Row{}, false, err
		}
		o.index = nil
	}
	if o.pos >= len(o.groups) {
		o.release()
		return Row{}, false, nil
	}
	g := o.groups[o.pos]
	o.groups[o.pos] = nil
	o.pos++
	var w builder
	w.begin()
	w.elem("_id", g.id)
	for i, f := range o.spec.fields {
		w.elem(f.name, g.accs[i].result())
	}
	o.x.mem.Shrink(g.cost)
	o.charged -= g.cost
	return Row{Doc: w.end(), ID: g.key}, true, nil
}

func (o *groupOp) release() {
	o.x.mem.Shrink(o.charged)
	o.charged = 0
	o.groups = nil
}

func (o *groupOp) Suspend() {
	if !o.ready {
		o.in.Suspend()
	}
}

func (o *groupOp) Close() {
	o.in.Close()
	o.release()
	o.ready = true
}

// accumulator is the state of one accumulator in one group. add returns
// how many bytes the state grew by (negative when it shrank).
type accumulator interface {
	add(v bson.RawValue) (int64, error)
	result() bson.RawValue
}

func newAccumulator(op string) accumulator {
	switch op {
	case "$sum":
		return &sumAcc{}
	case "$avg":
		return &avgAcc{}
	case "$min":
		return &minMaxAcc{}
	case "$max":
		return &minMaxAcc{max: true}
	case "$count":
		return &countAcc{}
	case "$first":
		return &firstAcc{}
	case "$last":
		return &lastAcc{}
	case "$push":
		return &pushAcc{}
	}
	return &addToSetAcc{seen: make(map[string]struct{})}
}

// sumAcc is $sum: numbers only (anything else is ignored), exact in int64
// while every input is an integer, int32 when the total fits, int64 past
// that, and double once a double appears or the int64 total overflows.
// Doubles add with Neumaier's compensated summation, in input order.
type sumAcc struct {
	kind     bson.Type
	isum     int64
	fsum     float64
	comp     float64
	overflow bool
	n        int64
}

func (a *sumAcc) add(v bson.RawValue) (int64, error) {
	if v.Type == bson.TypeDecimal128 {
		return 0, errDecimal
	}
	x, ok := numOf(v)
	if !ok {
		return 0, nil
	}
	if a.n == 0 {
		a.kind = bson.TypeInt32
	}
	a.n++
	a.kind = widest(a.kind, x.kind)
	switch {
	case !x.isInt():
		a.addFloat(x.f)
	case a.overflow:
		a.addFloat(float64(x.i))
	default:
		s, over := addInt64(a.isum, x.i)
		if over {
			a.overflow = true
			a.addFloat(float64(a.isum))
			a.addFloat(float64(x.i))
			a.isum = 0
		} else {
			a.isum = s
		}
	}
	return 0, nil
}

func (a *sumAcc) addFloat(f float64) {
	t := a.fsum + f
	if math.IsInf(t, 0) || math.IsNaN(t) {
		a.fsum, a.comp = t, 0
		return
	}
	if math.Abs(a.fsum) >= math.Abs(f) {
		a.comp += (a.fsum - t) + f
	} else {
		a.comp += (f - t) + a.fsum
	}
	a.fsum = t
}

func (a *sumAcc) total() float64 { return float64(a.isum) + (a.fsum + a.comp) }

func (a *sumAcc) result() bson.RawValue {
	switch {
	case a.n == 0:
		return i32(0)
	case a.kind == bson.TypeDouble || a.overflow:
		return f64(a.total())
	}
	return fitInt(a.kind, a.isum).value()
}

// avgAcc is $avg: the mean of the numeric inputs as a double, null when
// there were none.
type avgAcc struct{ sumAcc }

func (a *avgAcc) result() bson.RawValue {
	if a.n == 0 {
		return nullV
	}
	return f64(a.total() / float64(a.n))
}

// minMaxAcc is $min or $max in BSON order; null, undefined and missing
// inputs are ignored, and the result is null when nothing else came.
type minMaxAcc struct {
	max bool
	v   bson.RawValue
	set bool
}

func (a *minMaxAcc) add(v bson.RawValue) (int64, error) {
	if nullArg(v) {
		return 0, nil
	}
	if a.set {
		c := exprCompare(v, a.v)
		if (a.max && c <= 0) || (!a.max && c >= 0) {
			return 0, nil
		}
	}
	old := len(a.v.Value)
	a.v, a.set = bson.RawValue{Type: v.Type, Value: clone(v.Value)}, true
	return int64(len(v.Value) - old), nil
}

func (a *minMaxAcc) result() bson.RawValue {
	if !a.set {
		return nullV
	}
	return a.v
}

type countAcc struct{ n int64 }

func (a *countAcc) add(bson.RawValue) (int64, error) { a.n++; return 0, nil }
func (a *countAcc) result() bson.RawValue            { return intValue(a.n) }

// firstAcc and lastAcc keep the value of the first or last document of the
// group; a missing value counts as null.
type firstAcc struct {
	v   bson.RawValue
	set bool
}

func (a *firstAcc) add(v bson.RawValue) (int64, error) {
	if a.set {
		return 0, nil
	}
	if v.Type == 0 {
		v = nullV
	}
	a.v, a.set = bson.RawValue{Type: v.Type, Value: clone(v.Value)}, true
	return int64(len(v.Value)), nil
}

func (a *firstAcc) result() bson.RawValue {
	if !a.set {
		return nullV
	}
	return a.v
}

type lastAcc struct{ v bson.RawValue }

func (a *lastAcc) add(v bson.RawValue) (int64, error) {
	if v.Type == 0 {
		v = nullV
	}
	old := len(a.v.Value)
	a.v = bson.RawValue{Type: v.Type, Value: clone(v.Value)}
	return int64(len(v.Value) - old), nil
}

func (a *lastAcc) result() bson.RawValue {
	if a.v.Type == 0 {
		return nullV
	}
	return a.v
}

// pushAcc is $push: every non-missing value, in input order.
type pushAcc struct{ vals []bson.RawValue }

func (a *pushAcc) add(v bson.RawValue) (int64, error) {
	if v.Type == 0 {
		return 0, nil
	}
	a.vals = append(a.vals, bson.RawValue{Type: v.Type, Value: clone(v.Value)})
	return int64(len(v.Value)) + 32, nil
}

func (a *pushAcc) result() bson.RawValue { return arrV(a.vals) }

// addToSetAcc is $addToSet: distinct non-missing values by keyenc bytes
// (so 1 and 1.0 are one value), in first-seen order.
type addToSetAcc struct {
	seen map[string]struct{}
	vals []bson.RawValue
}

func (a *addToSetAcc) add(v bson.RawValue) (int64, error) {
	if v.Type == 0 {
		return 0, nil
	}
	key, err := keyenc.Encode(nil, v)
	if err != nil {
		key = append([]byte{0xFE, byte(v.Type)}, v.Value...)
	}
	if _, dup := a.seen[string(key)]; dup {
		return 0, nil
	}
	a.seen[string(key)] = struct{}{}
	a.vals = append(a.vals, bson.RawValue{Type: v.Type, Value: clone(v.Value)})
	return int64(len(key)+len(v.Value)) + 64, nil
}

func (a *addToSetAcc) result() bson.RawValue { return arrV(a.vals) }
