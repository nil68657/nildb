package query

import (
	"strconv"
	"strings"

	"github.com/nil68657/nildb/internal/doc"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func init() {
	registerStage("$addFields", parseAddFieldsStage)
	registerStage("$set", parseAddFieldsStage)
	registerStage("$unset", parseUnsetStage)
}

// ptree is the field tree of an $addFields/$set stage or of a $project
// with computed fields. Dotted names and nested specifications ({a: {b:
// ...}}) both become child nodes.
type ptree struct {
	kids     []*pkid
	index    map[string]int
	computed bool // the subtree holds a computed field
}

type pkid struct {
	name    string
	include bool // a $project inclusion leaf
	expr    Expr // a computed leaf
	sub     *ptree
}

func newPtree() *ptree { return &ptree{index: make(map[string]int)} }

func (t *ptree) lookup(name string) *pkid {
	if i, ok := t.index[name]; ok {
		return t.kids[i]
	}
	return nil
}

func (t *ptree) add(k *pkid) {
	t.index[k.name] = len(t.kids)
	t.kids = append(t.kids, k)
}

// insert places leaf at the dotted path parts.
func (t *ptree) insert(stageName string, parts []string, leaf *pkid) error {
	cur := t
	for i, p := range parts[:len(parts)-1] {
		k := cur.lookup(p)
		switch {
		case k == nil:
			k = &pkid{name: p, sub: newPtree()}
			cur.add(k)
		case k.sub == nil:
			return exprErr(31250, "Invalid %s :: caused by :: Path collision at %s", stageName, strings.Join(parts[:i+1], "."))
		}
		cur = k.sub
	}
	last := parts[len(parts)-1]
	if cur.lookup(last) != nil {
		return exprErr(31250, "Invalid %s :: caused by :: Path collision at %s", stageName, strings.Join(parts, "."))
	}
	leaf.name = last
	cur.add(leaf)
	return nil
}

// finish sets the computed flags bottom up.
func (t *ptree) finish() bool {
	for _, k := range t.kids {
		switch {
		case k.expr != nil:
			t.computed = true
		case k.sub != nil && k.sub.finish():
			t.computed = true
		}
	}
	return t.computed
}

var emptyDocBytes = []byte{5, 0, 0, 0, 0}

// project applies the tree to d. add selects $addFields: every input field
// stays, a computed field replaces an existing one in place, and new
// fields follow in specification order; a scalar in the way of a nested
// path becomes a document of the computed fields. Otherwise the tree is
// an inclusion $project: included fields keep their input order, computed
// fields follow in specification order, _id stays unless keepID is false,
// and scalars inside projected arrays are dropped.
func (t *ptree) project(d bson.Raw, add, keepID bool) (bson.Raw, error) {
	var w builder
	w.begin()
	if err := t.apply(&w, d, d, add, keepID); err != nil {
		return nil, err
	}
	out := w.end()
	if len(out) > doc.MaxDocSize {
		return nil, exprErr(10334, "document of %d bytes exceeds the 16 MiB limit", len(out))
	}
	return out, nil
}

func (t *ptree) apply(w *builder, d []byte, root bson.Raw, add, keepID bool) error {
	done := make([]bool, len(t.kids))
	it := iterDoc(d)
	for e, ok := it.next(); ok; e, ok = it.next() {
		name := string(e.name)
		ki, found := t.index[name]
		if !found {
			if add || (keepID && name == "_id") {
				w.rawElem(e.raw)
			}
			continue
		}
		if done[ki] {
			continue
		}
		k := t.kids[ki]
		switch {
		case k.include:
			w.rawElem(e.raw)
			done[ki] = true
		case k.expr != nil:
			if !add {
				continue
			}
			v, err := k.expr.eval(root)
			if err != nil {
				return err
			}
			if v.Type != 0 {
				w.elem(name, v)
			}
			done[ki] = true
		default:
			done[ki] = true
			var err error
			switch e.typ {
			case bson.TypeEmbeddedDocument:
				w.beginElem(bson.TypeEmbeddedDocument, name)
				err = k.sub.apply(w, e.val, root, add, false)
				w.end()
			case bson.TypeArray:
				w.beginElem(bson.TypeArray, name)
				err = k.sub.applyArray(w, e.val, root, add)
				w.end()
			default:
				if k.sub.computed {
					w.beginElem(bson.TypeEmbeddedDocument, name)
					err = k.sub.apply(w, emptyDocBytes, root, add, false)
					w.end()
				} else if add {
					w.rawElem(e.raw)
				}
			}
			if err != nil {
				return err
			}
		}
	}
	for ki, k := range t.kids {
		if done[ki] {
			continue
		}
		switch {
		case k.expr != nil:
			v, err := k.expr.eval(root)
			if err != nil {
				return err
			}
			if v.Type != 0 {
				w.elem(k.name, v)
			}
		case k.sub != nil && k.sub.computed:
			w.beginElem(bson.TypeEmbeddedDocument, k.name)
			err := k.sub.apply(w, emptyDocBytes, root, add, false)
			w.end()
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (t *ptree) applyArray(w *builder, arr []byte, root bson.Raw, add bool) error {
	i := 0
	it := iterDoc(arr)
	for e, ok := it.next(); ok; e, ok = it.next() {
		key := strconv.Itoa(i)
		var err error
		switch {
		case e.typ == bson.TypeEmbeddedDocument:
			w.beginElem(bson.TypeEmbeddedDocument, key)
			err = t.apply(w, e.val, root, add, false)
			w.end()
		case e.typ == bson.TypeArray:
			w.beginElem(bson.TypeArray, key)
			err = t.applyArray(w, e.val, root, add)
			w.end()
		case add:
			w.beginElem(bson.TypeEmbeddedDocument, key)
			err = t.apply(w, emptyDocBytes, root, add, false)
			w.end()
		default:
			continue
		}
		if err != nil {
			return err
		}
		i++
	}
	return nil
}

// treeOp runs a ptree over every row.
type treeOp struct {
	in     Operator
	t      *ptree
	add    bool
	keepID bool
}

func (o *treeOp) Next() (Row, bool, error) {
	r, ok, err := o.in.Next()
	if err != nil || !ok {
		return r, ok, err
	}
	out, err := o.t.project(r.Doc, o.add, o.keepID)
	if err != nil {
		return Row{}, false, err
	}
	r.Doc = out
	return r, true, nil
}

func (o *treeOp) Suspend() { o.in.Suspend() }
func (o *treeOp) Close()   { o.in.Close() }

// fieldSetter sets one dotted field with $addFields semantics, for the
// fields $unwind and $geoNear add. The tree is built once; set swaps the
// value.
type fieldSetter struct {
	t   *ptree
	val *bson.RawValue
}

type slotExpr struct{ v *bson.RawValue }

func (e slotExpr) eval(bson.Raw) (bson.RawValue, error) { return *e.v, nil }

func newFieldSetter(path string) *fieldSetter {
	s := &fieldSetter{t: newPtree(), val: new(bson.RawValue)}
	_ = s.t.insert("$addFields", strings.Split(path, "."), &pkid{expr: slotExpr{s.val}})
	s.t.finish()
	return s
}

func (s *fieldSetter) set(d bson.Raw, v bson.RawValue) bson.Raw {
	*s.val = v
	out, err := s.t.project(d, true, true)
	if err != nil {
		return d
	}
	return out
}

// checkSpecName validates one field name of a stage specification.
func checkSpecName(stageName, name string) error {
	if name == "" {
		return exprErr(40352, "Invalid %s :: caused by :: FieldPath cannot be constructed with empty string", stageName)
	}
	for _, c := range strings.Split(name, ".") {
		if c == "" {
			return exprErr(15998, "Invalid %s :: caused by :: FieldPath field names may not be empty strings.", stageName)
		}
		if c[0] == '$' {
			return exprErr(16410, "Invalid %s :: caused by :: FieldPath field names may not start with '$'.", stageName)
		}
	}
	return nil
}

// isSubSpec reports a nested specification: a non-empty document whose
// first field is not an operator.
func isSubSpec(v bson.RawValue) bool {
	if v.Type != bson.TypeEmbeddedDocument || emptyDoc(v.Value) {
		return false
	}
	first, _ := lookupFirst(v.Value)
	return !strings.HasPrefix(first, "$")
}

func parseAddFieldsStage(arg bson.RawValue) (*stage, error) {
	const name = "$addFields"
	if arg.Type != bson.TypeEmbeddedDocument {
		return nil, exprErr(40272, "$addFields specification stage must be an object, got %s", typeName(arg.Type))
	}
	t := newPtree()
	var paths []string
	var walk func(prefix string, d []byte) error
	walk = func(prefix string, d []byte) error {
		it := iterDoc(d)
		for e, ok := it.next(); ok; e, ok = it.next() {
			field := string(e.name)
			if err := checkSpecName(name, field); err != nil {
				return err
			}
			path := prefix + field
			v := e.value()
			if isSubSpec(v) {
				if err := walk(path+".", v.Value); err != nil {
					return err
				}
				continue
			}
			ex, err := compileExpr(v)
			if err != nil {
				return err
			}
			if err := t.insert(name, strings.Split(path, "."), &pkid{expr: ex}); err != nil {
				return err
			}
			paths = append(paths, path)
		}
		return nil
	}
	if err := walk("", arg.Value); err != nil {
		return nil, err
	}
	t.finish()
	return &stage{label: "$addFields", keeps: keepsExcept(paths), build: func(x *Exec, in Operator) (Operator, error) {
		return &treeOp{in: in, t: t, add: true, keepID: true}, nil
	}}, nil
}

func parseUnsetStage(arg bson.RawValue) (*stage, error) {
	var paths []string
	switch arg.Type {
	case bson.TypeString:
		s, _ := strOf(arg)
		paths = []string{s}
	case bson.TypeArray:
		for _, v := range arrayValues(arg.Value) {
			s, ok := strOf(v)
			if !ok {
				return nil, exprErr(31120, "$unset specification must be a string or an array containing only string values")
			}
			paths = append(paths, s)
		}
		if len(paths) == 0 {
			return nil, exprErr(31119, "$unset specification must be a string or an array with at least one field")
		}
	default:
		return nil, exprErr(31002, "$unset specification must be a string or an array")
	}
	var w builder
	w.begin()
	for _, p := range paths {
		if err := checkSpecName("$unset", p); err != nil {
			return nil, err
		}
		w.elem(p, i32(0))
	}
	proj, err := doc.CompileProjection(w.end())
	if err != nil {
		return nil, err
	}
	return &stage{label: "$unset", keeps: keepsExcept(paths), build: func(x *Exec, in Operator) (Operator, error) {
		return &projectOp{in: in, p: proj}, nil
	}}, nil
}

// parseProjectStage parses $project. Inclusion and exclusion without
// computed fields follow find's projection rules (doc.CompileProjection);
// a projection with computed fields is an inclusion ptree.
func parseProjectStage(arg bson.RawValue) (*stage, error) {
	const name = "$project"
	if arg.Type != bson.TypeEmbeddedDocument {
		return nil, exprErr(15969, "$project specification must be an object")
	}
	if emptyDoc(arg.Value) {
		return nil, exprErr(51272, "Invalid $project :: caused by :: projection specification must have at least one field")
	}
	type entry struct {
		path string
		v    bson.RawValue
	}
	var entries []entry
	var walk func(prefix string, d []byte) error
	walk = func(prefix string, d []byte) error {
		it := iterDoc(d)
		for e, ok := it.next(); ok; e, ok = it.next() {
			field := string(e.name)
			if err := checkSpecName(name, field); err != nil {
				return err
			}
			path, v := prefix+field, e.value()
			switch {
			case isSubSpec(v):
				if err := walk(path+".", v.Value); err != nil {
					return err
				}
			case v.Type == bson.TypeEmbeddedDocument && emptyDoc(v.Value):
				return exprErr(51270, "Invalid $project :: caused by :: An empty sub-projection is not a valid value. Found empty object at path %s", path)
			default:
				entries = append(entries, entry{path, v})
			}
		}
		return nil
	}
	if err := walk("", arg.Value); err != nil {
		return nil, err
	}
	isFlag := func(v bson.RawValue) bool { _, n := numOf(v); return v.Type == bson.TypeBoolean || n }
	computed := false
	for _, e := range entries {
		if !isFlag(e.v) {
			computed = true
		}
	}
	keepID := true
	var included, excluded []string
	for _, e := range entries {
		if !isFlag(e.v) {
			continue
		}
		switch {
		case truthy(e.v):
			included = append(included, e.path)
		case e.path == "_id":
			keepID = false
		default:
			excluded = append(excluded, e.path)
		}
	}
	if !computed {
		proj, err := doc.CompileProjection(arg.Value)
		if err != nil {
			return nil, err
		}
		keeps := keepsExcept(excluded)
		if len(included) > 0 {
			keeps = keepsOnly(included, keepID)
		} else if !keepID {
			keeps = keepsExcept(append(excluded, "_id"))
		}
		return &stage{label: "$project", keeps: keeps, build: func(x *Exec, in Operator) (Operator, error) {
			return &projectOp{in: in, p: proj}, nil
		}}, nil
	}
	if len(excluded) > 0 {
		return nil, exprErr(31254, "Invalid $project :: caused by :: Cannot do exclusion on field %s in inclusion projection", excluded[0])
	}
	t := newPtree()
	for _, e := range entries {
		leaf := &pkid{}
		switch {
		case isFlag(e.v) && truthy(e.v):
			leaf.include = true
		case isFlag(e.v):
			continue // _id: 0
		default:
			ex, err := compileExpr(e.v)
			if err != nil {
				return nil, err
			}
			leaf.expr = ex
		}
		if err := t.insert(name, strings.Split(e.path, "."), leaf); err != nil {
			return nil, err
		}
	}
	t.finish()
	if t.lookup("_id") != nil {
		keepID = false // the tree names _id itself, as an inclusion or a computed field
	}
	return &stage{label: "$project", keeps: keepsOnly(included, keepID), build: func(x *Exec, in Operator) (Operator, error) {
		return &treeOp{in: in, t: t, keepID: keepID}, nil
	}}, nil
}

// keepsOnly is the path test of an inclusion projection: a path survives
// unchanged when an included path is it or its ancestor.
func keepsOnly(included []string, keepID bool) func(string) bool {
	return func(p string) bool {
		if keepID && (p == "_id" || strings.HasPrefix(p, "_id.")) {
			return true
		}
		for _, q := range included {
			if p == q || strings.HasPrefix(p, q+".") {
				return true
			}
		}
		return false
	}
}

// keepsExcept is the path test of a stage that changes the given paths:
// a path survives when it neither is nor overlaps one of them.
func keepsExcept(changed []string) func(string) bool {
	return func(p string) bool {
		for _, q := range changed {
			if p == q || strings.HasPrefix(p, q+".") || strings.HasPrefix(q, p+".") {
				return false
			}
		}
		return true
	}
}
