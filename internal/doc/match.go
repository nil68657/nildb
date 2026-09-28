package doc

import (
	"regexp"
	"strings"

	"github.com/nil68657/nildb/internal/keyenc"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// GeoHooks compiles the geo query operators ($geoWithin, $geoIntersects,
// $near, $nearSphere). field is the dotted path the operator applies to,
// relative to the document the returned function receives (the element
// document inside $elemMatch). spec is a document holding the operator and
// its argument, for example {"$geoWithin": {"$geometry": {...}}}; for $near
// and $nearSphere it also holds the sibling $minDistance and $maxDistance
// when the query gives them there. The returned function reports whether a
// document satisfies the operator.
type GeoHooks interface {
	Compile(field string, op string, spec bson.Raw) (func(doc bson.Raw) (bool, error), error)
}

// Op names the kind of an index-usable predicate.
type Op uint8

const (
	OpEq            Op = iota + 1 // Values[0]; Null also matches a missing field
	OpGt                          // Values[0]
	OpGte                         // Values[0]
	OpLt                          // Values[0]
	OpLte                         // Values[0]
	OpIn                          // Values, any of them; Null also matches missing
	OpRegex                       // Values[0] is the Regex; Prefix is its anchored literal prefix
	OpGeoWithin                   // Values[0] is the spec document given to GeoHooks
	OpGeoIntersects               // Values[0] is the spec document
	OpNear                        // Values[0] is the spec document
	OpNearSphere                  // Values[0] is the spec document
)

var opNames = map[Op]string{
	OpEq: "$eq", OpGt: "$gt", OpGte: "$gte", OpLt: "$lt", OpLte: "$lte", OpIn: "$in",
	OpRegex: "$regex", OpGeoWithin: "$geoWithin", OpGeoIntersects: "$geoIntersects",
	OpNear: "$near", OpNearSphere: "$nearSphere",
}

func (o Op) String() string {
	if s, ok := opNames[o]; ok {
		return s
	}
	return "unknown"
}

// Predicate is one condition a planner can turn into index bounds. Every
// predicate IndexablePredicates returns is necessary on its own: a document
// that fails it cannot match the filter.
type Predicate struct {
	Field  string
	Op     Op
	Values []bson.RawValue
	Prefix string // OpRegex only
}

// Matcher is a compiled query filter.
type Matcher struct {
	root  node
	preds []Predicate
	all   bool
}

// MatchAll reports whether the filter was empty (or only $comment), so it
// matches every document.
func (m *Matcher) MatchAll() bool { return m.all }

// Match reports whether doc satisfies the filter. Errors come only from
// GeoHooks functions.
func (m *Matcher) Match(doc bson.Raw) (bool, error) {
	if m.root == nil {
		return true, nil
	}
	return m.root.match(doc)
}

// IndexablePredicates returns the predicates of the filter's top-level
// conjunction (the filter document itself and top-level $and) that index
// bounds can serve: equality and ranges with a scalar or document operand
// keyenc can encode, $in over such values, anchored regexes with a literal
// prefix, the values listed in $all (each is an equality the document must
// satisfy), and the geo operators. Nothing under $or, $nor, $not or
// $elemMatch is returned.
func (m *Matcher) IndexablePredicates() []Predicate {
	out := make([]Predicate, len(m.preds))
	copy(out, m.preds)
	return out
}

// Compile parses a filter document. geo may be nil when the filter uses no
// geo operator.
func Compile(filter bson.Raw, geo GeoHooks) (*Matcher, error) {
	c := &compiler{geo: geo}
	if len(filter) == 0 {
		return &Matcher{all: true}, nil
	}
	root, err := c.doc(filter, ctxTop)
	if err != nil {
		return nil, err
	}
	m := &Matcher{root: root, preds: c.preds}
	if a, ok := root.(andNode); ok && len(a) == 0 {
		m.root, m.all = nil, true
	}
	return m, nil
}

// compile contexts: where an expression sits decides which operators it
// may use and whether its predicates are indexable.
type ctx uint8

const (
	ctxTop       ctx = iota // the filter document or top-level $and
	ctxNested               // under $or, $nor or $not
	ctxElemMatch            // inside an $elemMatch
)

type compiler struct {
	geo   GeoHooks
	preds []Predicate
	nears int
}

func (c *compiler) doc(d []byte, cx ctx) (node, error) {
	var out andNode
	it := newIter(d)
	for {
		e, ok := it.next()
		if !ok {
			break
		}
		name := string(e.name)
		if !strings.HasPrefix(name, "$") {
			n, err := c.field(name, e.value(), cx)
			if err != nil {
				return nil, err
			}
			out = append(out, n)
			continue
		}
		switch name {
		case "$and", "$or", "$nor":
			sub := cx
			if name != "$and" {
				sub = nestedCtx(cx)
			}
			kids, err := c.logical(name, e.value(), sub)
			if err != nil {
				return nil, err
			}
			switch name {
			case "$and":
				out = append(out, andNode(kids))
			case "$or":
				out = append(out, orNode(kids))
			default:
				out = append(out, norNode(kids))
			}
		case "$comment":
		case "$alwaysTrue":
			out = append(out, constNode(true))
		case "$alwaysFalse":
			out = append(out, constNode(false))
		case "$where", "$expr", "$text", "$jsonSchema", "$sampleRate":
			return nil, badValue("unsupported query operator '%s' in v1", name)
		default:
			return nil, badValue("unknown top level operator: %s", name)
		}
	}
	if err := it.err(); err != nil {
		return nil, err
	}
	if len(out) == 1 {
		return out[0], nil
	}
	return out, nil
}

func nestedCtx(cx ctx) ctx {
	if cx == ctxElemMatch {
		return cx
	}
	return ctxNested
}

func (c *compiler) logical(op string, v bson.RawValue, cx ctx) ([]node, error) {
	if v.Type != bson.TypeArray {
		return nil, badValue("%s must be an array", op)
	}
	var kids []node
	it := newIter(v.Value)
	for {
		e, ok := it.next()
		if !ok {
			break
		}
		if e.typ != bson.TypeEmbeddedDocument {
			return nil, badValue("$or/$and/$nor entries need to be full objects")
		}
		n, err := c.doc(e.val, cx)
		if err != nil {
			return nil, err
		}
		kids = append(kids, n)
	}
	if len(kids) == 0 {
		return nil, badValue("$and/$or/$nor must be a nonempty array")
	}
	return kids, nil
}

// isOperatorDoc reports whether an object value is an operator expression:
// its first field starts with '$' and is not a DBRef field.
func isOperatorDoc(v bson.RawValue) bool {
	if v.Type != bson.TypeEmbeddedDocument {
		return false
	}
	it := newIter(v.Value)
	e, ok := it.next()
	if !ok {
		return false
	}
	name := string(e.name)
	return strings.HasPrefix(name, "$") && name != "$ref" && name != "$id" && name != "$db"
}

func (c *compiler) field(name string, v bson.RawValue, cx ctx) (node, error) {
	path := splitPath(name)
	switch {
	case v.Type == bson.TypeRegex:
		t, err := newRegexTest(v)
		if err != nil {
			return nil, err
		}
		c.addRegexPred(name, t, cx)
		return &fieldNode{path: path, t: t}, nil
	case isOperatorDoc(v):
		return c.ops(name, path, v.Value, cx)
	}
	c.addEqPred(name, v, cx)
	return &fieldNode{path: path, t: eqTest{v}}, nil
}

// ops compiles an operator document {$op: arg, ...} for one field. Each
// operator becomes its own node and all must hold.
func (c *compiler) ops(name string, path []string, d []byte, cx ctx) (node, error) {
	var out andNode
	var regex, options bson.RawValue
	var hasRegex, hasOptions bool
	var geoOp string
	it := newIter(d)
	for {
		e, ok := it.next()
		if !ok {
			break
		}
		op := string(e.name)
		v := e.value()
		switch op {
		case "$regex":
			regex, hasRegex = v, true
			continue
		case "$options":
			options, hasOptions = v, true
			continue
		case "$geoWithin", "$geoIntersects", "$near", "$nearSphere":
			if geoOp != "" {
				return nil, badValue("%s and %s cannot be combined on one field", geoOp, op)
			}
			geoOp = op
			continue
		case "$maxDistance", "$minDistance":
			continue
		}
		n, err := c.op(name, path, op, v, cx)
		if err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	if err := it.err(); err != nil {
		return nil, err
	}
	if hasOptions && !hasRegex {
		return nil, badValue("$options needs a $regex")
	}
	if hasRegex {
		t, err := regexFromOperator(regex, options, hasOptions)
		if err != nil {
			return nil, err
		}
		c.addRegexPred(name, t, cx)
		out = append(out, &fieldNode{path: path, t: t})
	}
	if geoOp != "" {
		n, err := c.geoField(name, geoOp, d, cx)
		if err != nil {
			return nil, err
		}
		out = append(out, n)
	} else if _, ok := lookupField(d, "$maxDistance"); ok {
		return nil, badValue("$maxDistance and $minDistance are only valid with $near or $nearSphere")
	} else if _, ok := lookupField(d, "$minDistance"); ok {
		return nil, badValue("$maxDistance and $minDistance are only valid with $near or $nearSphere")
	}
	if len(out) == 1 {
		return out[0], nil
	}
	return out, nil
}

func (c *compiler) op(name string, path []string, op string, v bson.RawValue, cx ctx) (node, error) {
	switch op {
	case "$eq":
		c.addEqPred(name, v, cx)
		return &fieldNode{path: path, t: eqTest{v}}, nil
	case "$ne":
		if v.Type == bson.TypeRegex {
			return nil, badValue("Can't have regex as arg to $ne.")
		}
		return notNode{&fieldNode{path: path, t: eqTest{v}}}, nil
	case "$gt", "$gte", "$lt", "$lte":
		t := cmpTest{op: cmpOps[op], v: v}
		c.addRangePred(name, t, cx)
		return &fieldNode{path: path, t: t}, nil
	case "$in", "$nin":
		t, err := newInTest(op, v)
		if err != nil {
			return nil, err
		}
		if op == "$nin" {
			return notNode{&fieldNode{path: path, t: t}}, nil
		}
		c.addInPred(name, t, cx)
		return &fieldNode{path: path, t: t}, nil
	case "$exists":
		n := &fieldNode{path: path, t: existsTest{}}
		if truthy(v) {
			return n, nil
		}
		return notNode{n}, nil
	case "$type":
		t, err := newTypeTest(v)
		if err != nil {
			return nil, err
		}
		return &fieldNode{path: path, t: t}, nil
	case "$size":
		t, err := newSizeTest(v)
		if err != nil {
			return nil, err
		}
		return &fieldNode{path: path, t: t}, nil
	case "$mod":
		t, err := newModTest(v)
		if err != nil {
			return nil, err
		}
		return &fieldNode{path: path, t: t}, nil
	case "$all":
		return c.all(name, path, v, cx)
	case "$elemMatch":
		t, err := c.elemMatch(v)
		if err != nil {
			return nil, err
		}
		return &fieldNode{path: path, t: t}, nil
	case "$not":
		return c.not(name, path, v, cx)
	case "$bitsAllSet", "$bitsAllClear", "$bitsAnySet", "$bitsAnyClear", "$where", "$expr", "$text", "$jsonSchema":
		return nil, badValue("unsupported query operator '%s' in v1", op)
	}
	return nil, badValue("unknown operator: %s", op)
}

func (c *compiler) not(name string, path []string, v bson.RawValue, cx ctx) (node, error) {
	switch v.Type {
	case bson.TypeRegex:
		t, err := newRegexTest(v)
		if err != nil {
			return nil, err
		}
		return notNode{&fieldNode{path: path, t: t}}, nil
	case bson.TypeEmbeddedDocument:
		if docEmpty(v.Value) {
			return nil, badValue("$not cannot be empty")
		}
		if !isOperatorDoc(v) {
			return nil, badValue("$not needs a regex or a document")
		}
		n, err := c.ops(name, path, v.Value, nestedCtx(cx))
		if err != nil {
			return nil, err
		}
		return notNode{n}, nil
	}
	return nil, badValue("$not needs a regex or a document")
}

func (c *compiler) all(name string, path []string, v bson.RawValue, cx ctx) (node, error) {
	if v.Type != bson.TypeArray {
		return nil, badValue("$all needs an array")
	}
	vals := arrayValues(v.Value)
	if len(vals) == 0 {
		return constNode(false), nil
	}
	var out andNode
	elemForm := isElemMatchDoc(vals[0])
	for _, x := range vals {
		if isElemMatchDoc(x) != elemForm {
			return nil, badValue("$all/$elemMatch has to be consistent")
		}
		switch {
		case elemForm:
			em, _ := lookupField(x.Value, "$elemMatch")
			t, err := c.elemMatch(em)
			if err != nil {
				return nil, err
			}
			out = append(out, &fieldNode{path: path, t: t})
		case x.Type == bson.TypeRegex:
			t, err := newRegexTest(x)
			if err != nil {
				return nil, err
			}
			c.addRegexPred(name, t, cx)
			out = append(out, &fieldNode{path: path, t: t})
		case isOperatorDoc(x):
			return nil, badValue("no $ expressions in $all")
		default:
			c.addEqPred(name, x, cx)
			out = append(out, &fieldNode{path: path, t: eqTest{x}})
		}
	}
	return out, nil
}

func isElemMatchDoc(v bson.RawValue) bool {
	if v.Type != bson.TypeEmbeddedDocument {
		return false
	}
	it := newIter(v.Value)
	e, ok := it.next()
	return ok && string(e.name) == "$elemMatch"
}

// elemMatch compiles the argument of $elemMatch. An argument whose first
// field is an operator other than $and, $or and $nor applies its operators
// to each element as a value; any other document is a query each element
// document (or array, read as a document keyed "0", "1", ...) must match.
func (c *compiler) elemMatch(v bson.RawValue) (test, error) {
	if v.Type != bson.TypeEmbeddedDocument {
		return nil, badValue("$elemMatch needs an Object")
	}
	it := newIter(v.Value)
	first, _ := it.next()
	fn := string(first.name)
	if strings.HasPrefix(fn, "$") && fn != "$and" && fn != "$or" && fn != "$nor" && fn != "$ref" && fn != "$id" && fn != "$db" {
		t, err := c.valueOps(v.Value)
		if err != nil {
			return nil, err
		}
		return elemMatchValueTest{t}, nil
	}
	n, err := c.doc(v.Value, ctxElemMatch)
	if err != nil {
		return nil, err
	}
	return elemMatchObjectTest{n}, nil
}

// valueOps compiles an operator document into one test applied to a single
// value with no array traversal: the $elemMatch value form and $pull
// conditions.
func (c *compiler) valueOps(d []byte) (test, error) {
	var out andTest
	var regex, options bson.RawValue
	var hasRegex, hasOptions bool
	it := newIter(d)
	for {
		e, ok := it.next()
		if !ok {
			break
		}
		op := string(e.name)
		v := e.value()
		var t test
		var err error
		switch op {
		case "$regex":
			regex, hasRegex = v, true
			continue
		case "$options":
			options, hasOptions = v, true
			continue
		case "$eq":
			t = eqTest{v}
		case "$ne":
			if v.Type == bson.TypeRegex {
				return nil, badValue("Can't have regex as arg to $ne.")
			}
			t = notTest{eqTest{v}}
		case "$gt", "$gte", "$lt", "$lte":
			t = cmpTest{op: cmpOps[op], v: v}
		case "$in":
			t, err = newInTest(op, v)
		case "$nin":
			var in test
			in, err = newInTest(op, v)
			t = notTest{in}
		case "$exists":
			t = existsTest{}
			if !truthy(v) {
				t = notTest{t}
			}
		case "$type":
			t, err = newTypeTest(v)
		case "$size":
			t, err = newSizeTest(v)
		case "$mod":
			t, err = newModTest(v)
		case "$elemMatch":
			t, err = c.elemMatch(v)
		case "$all":
			if v.Type != bson.TypeArray {
				return nil, badValue("$all needs an array")
			}
			vals := arrayValues(v.Value)
			if len(vals) == 0 {
				t = constTest(false)
				break
			}
			var at andTest
			for _, x := range vals {
				if x.Type == bson.TypeRegex {
					rt, err := newRegexTest(x)
					if err != nil {
						return nil, err
					}
					at = append(at, rt)
					continue
				}
				at = append(at, eqTest{x})
			}
			t = at
		case "$not":
			switch v.Type {
			case bson.TypeRegex:
				var rt test
				rt, err = newRegexTest(v)
				t = notTest{rt}
			case bson.TypeEmbeddedDocument:
				if docEmpty(v.Value) {
					return nil, badValue("$not cannot be empty")
				}
				var sub test
				sub, err = c.valueOps(v.Value)
				t = notTest{sub}
			default:
				return nil, badValue("$not needs a regex or a document")
			}
		case "$geoWithin", "$geoIntersects", "$near", "$nearSphere":
			return nil, badValue("%s is not allowed inside $elemMatch value conditions", op)
		case "$and", "$or", "$nor":
			return nil, badValue("unknown operator: %s", op)
		default:
			_, err = c.op("", nil, op, v, ctxElemMatch)
			if err == nil {
				err = badValue("unknown operator: %s", op)
			}
		}
		if err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	if err := it.err(); err != nil {
		return nil, err
	}
	if hasOptions && !hasRegex {
		return nil, badValue("$options needs a $regex")
	}
	if hasRegex {
		t, err := regexFromOperator(regex, options, hasOptions)
		if err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	if len(out) == 1 {
		return out[0], nil
	}
	return out, nil
}

func (c *compiler) geoField(name, op string, d []byte, cx ctx) (node, error) {
	if c.geo == nil {
		return nil, ErrNoGeoHooks
	}
	near := op == "$near" || op == "$nearSphere"
	if near {
		if cx != ctxTop {
			return nil, badValue("geoNear must be top-level expr")
		}
		c.nears++
		if c.nears > 1 {
			return nil, badValue("Too many geoNear expressions")
		}
	}
	var w builder
	w.begin()
	it := newIter(d)
	for {
		e, ok := it.next()
		if !ok {
			break
		}
		n := string(e.name)
		if n == op || (near && (n == "$maxDistance" || n == "$minDistance")) {
			w.rawElem(e.raw)
		}
	}
	spec := bson.Raw(w.end())
	fn, err := c.geo.Compile(name, op, spec)
	if err != nil {
		return nil, err
	}
	if cx == ctxTop {
		var o Op
		switch op {
		case "$geoWithin":
			o = OpGeoWithin
		case "$geoIntersects":
			o = OpGeoIntersects
		case "$near":
			o = OpNear
		default:
			o = OpNearSphere
		}
		c.preds = append(c.preds, Predicate{Field: name, Op: o, Values: []bson.RawValue{docValue(spec)}})
	}
	return geoNode{fn}, nil
}

// keyable reports whether v may bound an index scan: keyenc encodes it and
// it is not an array (a multikey index holds elements, not arrays).
func keyable(v bson.RawValue) bool {
	if v.Type == bson.TypeArray || v.Type == bson.TypeRegex {
		return false
	}
	_, err := keyenc.Encode(nil, v)
	return err == nil
}

func (c *compiler) addEqPred(name string, v bson.RawValue, cx ctx) {
	if cx == ctxTop && keyable(v) {
		c.preds = append(c.preds, Predicate{Field: name, Op: OpEq, Values: []bson.RawValue{v}})
	}
}

func (c *compiler) addRangePred(name string, t cmpTest, cx ctx) {
	if cx == ctxTop && keyable(t.v) {
		c.preds = append(c.preds, Predicate{Field: name, Op: t.op.indexOp(), Values: []bson.RawValue{t.v}})
	}
}

func (c *compiler) addInPred(name string, t *inTest, cx ctx) {
	if cx != ctxTop || len(t.regexes) > 0 {
		return
	}
	for _, v := range t.vals {
		if !keyable(v) {
			return
		}
	}
	vals := make([]bson.RawValue, len(t.vals))
	copy(vals, t.vals)
	c.preds = append(c.preds, Predicate{Field: name, Op: OpIn, Values: vals})
}

func (c *compiler) addRegexPred(name string, t *regexTest, cx ctx) {
	if cx != ctxTop {
		return
	}
	if p := regexPrefix(t.pattern, t.options); p != "" {
		c.preds = append(c.preds, Predicate{Field: name, Op: OpRegex, Values: []bson.RawValue{regexValue(t.pattern, t.options)}, Prefix: p})
	}
}

// Nodes evaluate against a whole document.

type node interface {
	match(doc []byte) (bool, error)
}

type andNode []node

func (a andNode) match(doc []byte) (bool, error) {
	for _, n := range a {
		ok, err := n.match(doc)
		if err != nil || !ok {
			return false, err
		}
	}
	return true, nil
}

type orNode []node

func (o orNode) match(doc []byte) (bool, error) {
	for _, n := range o {
		ok, err := n.match(doc)
		if err != nil || ok {
			return ok, err
		}
	}
	return false, nil
}

type norNode []node

func (o norNode) match(doc []byte) (bool, error) {
	ok, err := orNode(o).match(doc)
	return !ok && err == nil, err
}

type notNode struct{ n node }

func (n notNode) match(doc []byte) (bool, error) {
	ok, err := n.n.match(doc)
	return !ok && err == nil, err
}

type constNode bool

func (c constNode) match([]byte) (bool, error) { return bool(c), nil }

type geoNode struct {
	fn func(bson.Raw) (bool, error)
}

func (g geoNode) match(doc []byte) (bool, error) { return g.fn(doc) }

// fieldNode matches when any value its path reaches passes the test.
type fieldNode struct {
	path []string
	t    test
}

func (f *fieldNode) match(doc []byte) (bool, error) {
	var res bool
	var err error
	walkDoc(doc, f.path, f.t.expand(), func(v bson.RawValue) bool {
		res, err = f.t.test(v)
		return res || err != nil
	})
	return res && err == nil, err
}

// Tests evaluate one value, which may be the zero RawValue for a missing
// field. expand reports whether an array at the end of the path also offers
// its elements.

type test interface {
	test(v bson.RawValue) (bool, error)
	expand() bool
}

type eqTest struct{ v bson.RawValue }

func (t eqTest) expand() bool { return true }

func (t eqTest) test(x bson.RawValue) (bool, error) {
	if t.v.Type == bson.TypeNull {
		return x.Type == 0 || x.Type == bson.TypeNull, nil
	}
	if x.Type == 0 {
		return false, nil
	}
	return Compare(x, t.v) == 0, nil
}

type cmpOp uint8

const (
	cmpGt cmpOp = iota
	cmpGte
	cmpLt
	cmpLte
)

var cmpOps = map[string]cmpOp{"$gt": cmpGt, "$gte": cmpGte, "$lt": cmpLt, "$lte": cmpLte}

func (o cmpOp) indexOp() Op {
	switch o {
	case cmpGt:
		return OpGt
	case cmpGte:
		return OpGte
	case cmpLt:
		return OpLt
	}
	return OpLte
}

func (o cmpOp) holds(c int) bool {
	switch o {
	case cmpGt:
		return c > 0
	case cmpGte:
		return c >= 0
	case cmpLt:
		return c < 0
	}
	return c <= 0
}

// cmpTest is $gt, $gte, $lt or $lte with type bracketing: only values of
// the operand's type class compare, except that anything is above MinKey
// and below MaxKey, and a missing field counts as Null.
type cmpTest struct {
	op cmpOp
	v  bson.RawValue
}

func (t cmpTest) expand() bool { return true }

func (t cmpTest) test(x bson.RawValue) (bool, error) {
	if x.Type == 0 {
		x = nullValue
	}
	cx, cv := keyenc.TypeOrder(x.Type), keyenc.TypeOrder(t.v.Type)
	if cx != cv {
		switch t.v.Type {
		case bson.TypeMinKey:
			return t.op == cmpGt || t.op == cmpGte, nil
		case bson.TypeMaxKey:
			return t.op == cmpLt || t.op == cmpLte, nil
		}
		return false, nil
	}
	if cx == keyenc.TNumber {
		nx, okx := numberOf(x)
		nv, okv := numberOf(t.v)
		if okx && okv && (nx.isNaN() || nv.isNaN()) {
			// NaN equals NaN and is neither above nor below anything.
			return nx.isNaN() && nv.isNaN() && (t.op == cmpGte || t.op == cmpLte), nil
		}
	}
	return t.op.holds(Compare(x, t.v)), nil
}

type inTest struct {
	vals    []bson.RawValue
	regexes []*regexTest
}

func newInTest(op string, v bson.RawValue) (*inTest, error) {
	if v.Type != bson.TypeArray {
		return nil, badValue("%s needs an array", op)
	}
	t := &inTest{}
	it := newIter(v.Value)
	for {
		e, ok := it.next()
		if !ok {
			break
		}
		x := e.value()
		switch {
		case x.Type == bson.TypeRegex:
			rt, err := newRegexTest(x)
			if err != nil {
				return nil, err
			}
			t.regexes = append(t.regexes, rt)
		case isOperatorDoc(x):
			return nil, badValue("cannot nest $ under %s", op)
		default:
			t.vals = append(t.vals, x)
		}
	}
	return t, it.err()
}

func (t *inTest) expand() bool { return true }

func (t *inTest) test(x bson.RawValue) (bool, error) {
	for _, v := range t.vals {
		if ok, _ := (eqTest{v}).test(x); ok {
			return true, nil
		}
	}
	for _, r := range t.regexes {
		if ok, _ := r.test(x); ok {
			return true, nil
		}
	}
	return false, nil
}

type existsTest struct{}

func (existsTest) expand() bool                       { return false }
func (existsTest) test(x bson.RawValue) (bool, error) { return x.Type != 0, nil }

// typeTest is $type: one or more BSON types by alias or number, plus the
// "number" alias for the four numeric types.
type typeTest struct {
	types  map[bson.Type]bool
	number bool
}

var typeAliases = map[string]bson.Type{
	"double": bson.TypeDouble, "string": bson.TypeString, "object": bson.TypeEmbeddedDocument,
	"array": bson.TypeArray, "binData": bson.TypeBinary, "undefined": bson.TypeUndefined,
	"objectId": bson.TypeObjectID, "bool": bson.TypeBoolean, "date": bson.TypeDateTime,
	"null": bson.TypeNull, "regex": bson.TypeRegex, "dbPointer": bson.TypeDBPointer,
	"javascript": bson.TypeJavaScript, "symbol": bson.TypeSymbol,
	"javascriptWithScope": bson.TypeCodeWithScope, "int": bson.TypeInt32,
	"timestamp": bson.TypeTimestamp, "long": bson.TypeInt64, "decimal": bson.TypeDecimal128,
	"minKey": bson.TypeMinKey, "maxKey": bson.TypeMaxKey,
}

func newTypeTest(v bson.RawValue) (test, error) {
	t := typeTest{types: map[bson.Type]bool{}}
	vals := []bson.RawValue{v}
	if v.Type == bson.TypeArray {
		vals = arrayValues(v.Value)
		if len(vals) == 0 {
			return nil, badValue("$type must match at least one type")
		}
	}
	for _, x := range vals {
		if s, ok := strValue(x); ok && x.Type == bson.TypeString {
			if string(s) == "number" {
				t.number = true
				continue
			}
			bt, ok := typeAliases[string(s)]
			if !ok {
				return nil, badValue("Unknown type name alias: %s", s)
			}
			t.types[bt] = true
			continue
		}
		n, ok := numberOf(x)
		if !ok {
			return nil, badValue("type must be represented as a number or a string")
		}
		code, ok := truncInt64(n)
		if !ok || n.float() != float64(code) {
			return nil, badValue("Invalid numerical type code: %s", showValue(x))
		}
		bt := bson.Type(byte(code))
		switch code {
		case -1:
			bt = bson.TypeMinKey
		case 127:
			bt = bson.TypeMaxKey
		default:
			if code < 1 || code > 19 {
				return nil, badValue("Invalid numerical type code: %d", code)
			}
		}
		t.types[bt] = true
	}
	return t, nil
}

func (t typeTest) expand() bool { return true }

func (t typeTest) test(x bson.RawValue) (bool, error) {
	if x.Type == 0 {
		return false, nil
	}
	return t.types[x.Type] || (t.number && isNumberType(x.Type)), nil
}

type sizeTest struct{ n int }

func newSizeTest(v bson.RawValue) (test, error) {
	n, ok := numberOf(v)
	if !ok {
		return nil, badValue("$size needs a number")
	}
	i, ok := truncInt64(n)
	if !ok || n.float() != float64(i) {
		return nil, badValue("$size must be a whole number")
	}
	if i < 0 {
		return nil, badValue("$size may not be negative")
	}
	if i > 1<<31-1 {
		return constTest(false), nil
	}
	return sizeTest{int(i)}, nil
}

func (t sizeTest) expand() bool { return false }

func (t sizeTest) test(x bson.RawValue) (bool, error) {
	return x.Type == bson.TypeArray && arrayLen(x.Value) == t.n, nil
}

type modTest struct{ div, rem int64 }

func newModTest(v bson.RawValue) (test, error) {
	if v.Type != bson.TypeArray {
		return nil, badValue("malformed mod, needs to be an array")
	}
	vals := arrayValues(v.Value)
	switch {
	case len(vals) < 2:
		return nil, badValue("malformed mod, not enough elements")
	case len(vals) > 2:
		return nil, badValue("malformed mod, too many elements")
	}
	d, ok := numberOf(vals[0])
	if !ok {
		return nil, badValue("malformed mod, divisor not a number")
	}
	r, ok := numberOf(vals[1])
	if !ok {
		return nil, badValue("malformed mod, remainder not a number")
	}
	div, ok := truncInt64(d)
	if !ok {
		return nil, badValue("malformed mod, divisor value is invalid :: caused by :: %s", showValue(vals[0]))
	}
	rem, ok := truncInt64(r)
	if !ok {
		return nil, badValue("malformed mod, remainder value is invalid :: caused by :: %s", showValue(vals[1]))
	}
	if div == 0 {
		return nil, badValue("divisor cannot be 0")
	}
	return modTest{div, rem}, nil
}

func (t modTest) expand() bool { return true }

func (t modTest) test(x bson.RawValue) (bool, error) {
	n, ok := numberOf(x)
	if !ok {
		return false, nil
	}
	i, ok := truncInt64(n)
	if !ok {
		return false, nil
	}
	return i%t.div == t.rem, nil
}

// regexTest matches strings and symbols against a regular expression, and
// a stored Regex value with the same pattern and options.
type regexTest struct {
	re               *regexp.Regexp
	pattern, options string
}

func newRegexTest(v bson.RawValue) (*regexTest, error) {
	p, o, ok := regexParts(v)
	if !ok {
		return nil, errMalformed
	}
	re, err := compileRegex(p, o)
	if err != nil {
		return nil, err
	}
	return &regexTest{re: re, pattern: p, options: o}, nil
}

// regexFromOperator builds the regex of {$regex: p, $options: o}. p is a
// string or a Regex value; options may appear in only one of the two.
func regexFromOperator(regex, options bson.RawValue, hasOptions bool) (*regexTest, error) {
	var p, o string
	switch regex.Type {
	case bson.TypeString, bson.TypeSymbol:
		s, _ := strValue(regex)
		p = string(s)
	case bson.TypeRegex:
		p, o, _ = regexParts(regex)
	default:
		return nil, badValue("$regex has to be a string")
	}
	if hasOptions {
		s, ok := strValue(options)
		if !ok || options.Type != bson.TypeString {
			return nil, badValue("$options has to be a string")
		}
		if o != "" && len(s) > 0 {
			return nil, badValue("options set in both $regex and $options")
		}
		if len(s) > 0 {
			o = string(s)
		}
	}
	return newRegexTest(regexValue(p, o))
}

func (t *regexTest) expand() bool { return true }

func (t *regexTest) test(x bson.RawValue) (bool, error) {
	switch x.Type {
	case bson.TypeString, bson.TypeSymbol:
		s, ok := strValue(x)
		return ok && t.re.Match(s), nil
	case bson.TypeRegex:
		p, o, ok := regexParts(x)
		return ok && p == t.pattern && o == t.options, nil
	}
	return false, nil
}

// elemMatchObjectTest matches an array with an element that, read as a
// document, matches the sub-query.
type elemMatchObjectTest struct{ n node }

func (t elemMatchObjectTest) expand() bool { return false }

func (t elemMatchObjectTest) test(x bson.RawValue) (bool, error) {
	if x.Type != bson.TypeArray {
		return false, nil
	}
	it := newIter(x.Value)
	for {
		e, ok := it.next()
		if !ok {
			return false, nil
		}
		if e.typ != bson.TypeEmbeddedDocument && e.typ != bson.TypeArray {
			continue
		}
		ok, err := t.n.match(e.val)
		if err != nil || ok {
			return ok, err
		}
	}
}

// elemMatchValueTest matches an array with an element that passes every
// operator of the value form.
type elemMatchValueTest struct{ t test }

func (t elemMatchValueTest) expand() bool { return false }

func (t elemMatchValueTest) test(x bson.RawValue) (bool, error) {
	if x.Type != bson.TypeArray {
		return false, nil
	}
	it := newIter(x.Value)
	for {
		e, ok := it.next()
		if !ok {
			return false, nil
		}
		ok, err := t.t.test(e.value())
		if err != nil || ok {
			return ok, err
		}
	}
}

type andTest []test

func (a andTest) expand() bool { return true }

func (a andTest) test(x bson.RawValue) (bool, error) {
	for _, t := range a {
		ok, err := t.test(x)
		if err != nil || !ok {
			return false, err
		}
	}
	return true, nil
}

type notTest struct{ t test }

func (n notTest) expand() bool { return true }

func (n notTest) test(x bson.RawValue) (bool, error) {
	ok, err := n.t.test(x)
	return !ok && err == nil, err
}

type constTest bool

func (c constTest) expand() bool                     { return false }
func (c constTest) test(bson.RawValue) (bool, error) { return bool(c), nil }
