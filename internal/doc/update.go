package doc

import (
	"bytes"
	"encoding/binary"
	"math"
	"math/bits"
	"sort"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// Update is a compiled update: either operator form ($set, $inc, ...) or a
// replacement document.
type Update struct {
	replacement bson.Raw // non-nil for a replacement update
	root        *unode
	renames     int // number of $rename pairs
}

// IsReplacement reports whether the update is a replacement document.
func (u *Update) IsReplacement() bool { return u.replacement != nil }

type opKind uint8

const (
	opSet opKind = iota
	opSetOnInsert
	opUnset
	opInc
	opMul
	opMin
	opMax
	opCurrentDate
	opPush
	opAddToSet
	opPop
	opPull
	opPullAll
	opRenameFrom
	opRenameTo
)

var updateOps = map[string]opKind{
	"$set": opSet, "$setOnInsert": opSetOnInsert, "$unset": opUnset, "$inc": opInc,
	"$mul": opMul, "$min": opMin, "$max": opMax, "$currentDate": opCurrentDate,
	"$push": opPush, "$addToSet": opAddToSet, "$pop": opPop, "$pull": opPull,
	"$pullAll": opPullAll, "$rename": opRenameFrom,
}

// uop is one field operation of an operator update.
type uop struct {
	kind opKind
	path string // full dotted path, for messages
	val  bson.RawValue

	each     []bson.RawValue // $push and $addToSet values
	slice    *int64
	position *int64
	sortDir  int      // $push $sort 1 or -1 on whole elements
	sortSpec SortSpec // $push $sort on element fields
	popFirst bool
	dateTS   bool // $currentDate as a Timestamp
	pull     pullCond
	rename   int      // index of the $rename pair
	from     []string // $rename source path, on the opRenameTo side
}

// unode is a node of the update tree: one path component. A node holds an
// operation or children, never both; two operations on overlapping paths
// are a conflict at compile time.
type unode struct {
	name string
	kids []*unode
	idx  map[string]int
	op   *uop
}

func (n *unode) child(name string) *unode {
	for _, k := range n.kids {
		if k.name == name {
			return k
		}
	}
	k := &unode{name: name}
	n.kids = append(n.kids, k)
	return k
}

// finish orders children as MongoDB 5.0 applies them (numeric names
// numerically, others bytewise) and indexes them by name.
func (n *unode) finish() {
	sort.SliceStable(n.kids, func(i, j int) bool { return fieldLess(n.kids[i].name, n.kids[j].name) })
	n.idx = make(map[string]int, len(n.kids))
	for i, k := range n.kids {
		n.idx[k.name] = i
		k.finish()
	}
}

func fieldLess(a, b string) bool {
	ia, oka := arrayIndex(a)
	ib, okb := arrayIndex(b)
	if oka && okb {
		return ia < ib
	}
	return a < b
}

// CompileUpdate parses an update document. A document whose first field
// starts with '$' is an operator update; any other document, including {},
// is a replacement.
func CompileUpdate(u bson.Raw) (*Update, error) {
	it := newIter(u)
	first, ok := it.next()
	if err := it.err(); err != nil {
		return nil, err
	}
	if !ok || !strings.HasPrefix(string(first.name), "$") {
		return compileReplacement(u)
	}
	up := &Update{root: &unode{}}
	it = newIter(u)
	for {
		e, ok := it.next()
		if !ok {
			break
		}
		name := string(e.name)
		kind, known := updateOps[name]
		if !known {
			if name == "$bit" {
				return nil, badValue("unsupported update operator '$bit' in v1")
			}
			return nil, errf(CodeFailedToParse, "Unknown modifier: %s. Expected a valid update modifier or pipeline-style update specified as an array", name)
		}
		if e.typ != bson.TypeEmbeddedDocument {
			return nil, errf(CodeFailedToParse, "Modifiers operate on fields but we found type %s instead. For example: {$mod: {<field>: ...}} not {%s: %s}", typeName(e.typ), name, showValue(e.value()))
		}
		fit := newIter(e.val)
		for {
			f, ok := fit.next()
			if !ok {
				break
			}
			if err := up.addOp(kind, name, string(f.name), f.value()); err != nil {
				return nil, err
			}
		}
		if err := fit.err(); err != nil {
			return nil, err
		}
	}
	if err := it.err(); err != nil {
		return nil, err
	}
	up.root.finish()
	return up, nil
}

func compileReplacement(u bson.Raw) (*Update, error) {
	it := newIter(u)
	for {
		e, ok := it.next()
		if !ok {
			break
		}
		if strings.HasPrefix(string(e.name), "$") {
			return nil, errf(52, "The dollar ($) prefixed field '%s' in '%s' is not valid for storage.", e.name, e.name)
		}
	}
	if err := it.err(); err != nil {
		return nil, err
	}
	r := append(bson.Raw(nil), u...)
	if len(r) == 0 {
		r = bson.Raw(buildDoc(nil, nil))
	}
	return &Update{replacement: r}, nil
}

// checkUpdatePath validates the field path of one update operation.
func checkUpdatePath(path string) error {
	if path == "" {
		return errf(CodeEmptyFieldName, "An empty update path is not valid.")
	}
	for _, c := range splitPath(path) {
		switch {
		case c == "":
			return errf(CodeEmptyFieldName, "The update path '%s' contains an empty field name, which is not allowed.", path)
		case c == "$" || strings.HasPrefix(c, "$["):
			return badValue("positional update operators are not supported in v1: '%s'", path)
		case strings.HasPrefix(c, "$"):
			return errf(52, "The dollar ($) prefixed field '%s' in '%s' is not valid for storage.", c, path)
		}
	}
	return nil
}

func (u *Update) addOp(kind opKind, opName, path string, v bson.RawValue) error {
	if err := checkUpdatePath(path); err != nil {
		return err
	}
	op := &uop{kind: kind, path: path, val: v}
	switch kind {
	case opInc, opMul:
		if !isNumberType(v.Type) {
			verb := "increment"
			if kind == opMul {
				verb = "multiply"
			}
			return errf(CodeTypeMismatch, "Cannot %s with non-numeric argument: %s", verb, showField(path, v))
		}
		if v.Type == bson.TypeDecimal128 {
			return errDecimal
		}
	case opCurrentDate:
		ts, err := currentDateType(v)
		if err != nil {
			return err
		}
		op.dateTS = ts
	case opPush:
		if err := compilePush(op, v); err != nil {
			return err
		}
	case opAddToSet:
		if err := compileAddToSet(op, v); err != nil {
			return err
		}
	case opPop:
		n, ok := numberOf(v)
		f := 0.0
		if ok {
			f = n.float()
		}
		if f != 1 && f != -1 {
			return errf(CodeFailedToParse, "$pop expects 1 or -1, found: %s", showValue(v))
		}
		op.popFirst = f == -1
	case opPull:
		pc, err := compilePull(v)
		if err != nil {
			return err
		}
		op.pull = pc
	case opPullAll:
		if v.Type != bson.TypeArray {
			return errf(CodeFailedToParse, "$pullAll requires an array argument but was given a %s", typeName(v.Type))
		}
		op.each = arrayValues(v.Value)
	case opRenameFrom:
		return u.addRename(path, v)
	}
	return u.insert(path, op)
}

func (u *Update) addRename(from string, v bson.RawValue) error {
	to, ok := strValue(v)
	if !ok || v.Type != bson.TypeString {
		return badValue("The 'to' field for $rename must be a string: %s", showField(from, v))
	}
	target := string(to)
	if err := checkUpdatePath(target); err != nil {
		return err
	}
	if from == target {
		return badValue("The source and target field for $rename must differ: %s", showField(from, v))
	}
	if strings.HasPrefix(target, from+".") || strings.HasPrefix(from, target+".") {
		return badValue("The source and target field for $rename must not be on the same path: %s", showField(from, v))
	}
	id := u.renames
	u.renames++
	if err := u.insert(from, &uop{kind: opRenameFrom, path: from, rename: id}); err != nil {
		return err
	}
	return u.insert(target, &uop{kind: opRenameTo, path: target, rename: id, from: splitPath(from)})
}

// insert places op at path, refusing overlap with an operation already in
// the tree.
func (u *Update) insert(path string, op *uop) error {
	parts := splitPath(path)
	n := u.root
	for i, p := range parts {
		if n.op != nil {
			return errf(CodeConflictingUpdateOperators, "Updating the path '%s' would create a conflict at '%s'", path, strings.Join(parts[:i], "."))
		}
		n = n.child(p)
	}
	if n.op != nil || len(n.kids) > 0 {
		return errf(CodeConflictingUpdateOperators, "Updating the path '%s' would create a conflict at '%s'", path, path)
	}
	n.op = op
	return nil
}

func currentDateType(v bson.RawValue) (timestamp bool, err error) {
	switch v.Type {
	case bson.TypeBoolean:
		return false, nil
	case bson.TypeEmbeddedDocument:
		t, ok := lookupField(v.Value, "$type")
		s, sok := strValue(t)
		if ok && sok && t.Type == bson.TypeString && arrayLen(v.Value) == 1 {
			switch string(s) {
			case "date":
				return false, nil
			case "timestamp":
				return true, nil
			}
		}
		return false, badValue("The '$type' string field is required to be 'date' or 'timestamp': {$currentDate: {field : {$type: 'date'}}}")
	}
	return false, badValue("%s is not valid type for $currentDate. Please use a boolean ('true') or a $type expression ({$type: 'timestamp/date'}).", typeName(v.Type))
}

// hasEach reports whether v is a modifier document: an object with $each.
func hasEach(v bson.RawValue) bool {
	if v.Type != bson.TypeEmbeddedDocument {
		return false
	}
	_, ok := lookupField(v.Value, "$each")
	return ok
}

func compilePush(op *uop, v bson.RawValue) error {
	if !hasEach(v) {
		op.each = []bson.RawValue{v}
		return nil
	}
	it := newIter(v.Value)
	for {
		e, ok := it.next()
		if !ok {
			break
		}
		x := e.value()
		switch string(e.name) {
		case "$each":
			if x.Type != bson.TypeArray {
				return badValue("The argument to $each in $push must be an array but it was of type: %s", typeName(x.Type))
			}
			op.each = arrayValues(x.Value)
		case "$slice":
			n, err := wholeNumber(x, "The value for $slice must be an integer value but was given type: %s")
			if err != nil {
				return err
			}
			op.slice = &n
		case "$position":
			n, err := wholeNumber(x, "The value for $position must be an integer value, not of type: %s")
			if err != nil {
				return err
			}
			op.position = &n
		case "$sort":
			if err := compilePushSort(op, x); err != nil {
				return err
			}
		default:
			return badValue("Unrecognized clause in $push: %s", e.name)
		}
	}
	return it.err()
}

func wholeNumber(v bson.RawValue, format string) (int64, error) {
	n, ok := numberOf(v)
	if !ok {
		return 0, badValue(format, typeName(v.Type))
	}
	i, ok := truncInt64(n)
	if !ok || n.float() != float64(i) {
		return 0, badValue(format, typeName(v.Type))
	}
	return i, nil
}

const pushSortErr = "The $sort is invalid: use 1/-1 to sort the whole element, or {field:1/-1} to sort embedded fields"

func compilePushSort(op *uop, v bson.RawValue) error {
	if n, ok := numberOf(v); ok {
		switch n.float() {
		case 1:
			op.sortDir = 1
			return nil
		case -1:
			op.sortDir = -1
			return nil
		}
		return badValue(pushSortErr)
	}
	if v.Type != bson.TypeEmbeddedDocument || docEmpty(v.Value) {
		return badValue(pushSortErr)
	}
	spec, err := ParseSort(v.Value)
	if err != nil {
		return badValue(pushSortErr)
	}
	op.sortSpec = spec
	return nil
}

func compileAddToSet(op *uop, v bson.RawValue) error {
	if !hasEach(v) {
		op.each = []bson.RawValue{v}
		return nil
	}
	if arrayLen(v.Value) != 1 {
		return badValue("Found unexpected fields after $each in $addToSet: %s", showValue(v))
	}
	x, _ := lookupField(v.Value, "$each")
	if x.Type != bson.TypeArray {
		return badValue("The argument to $each in $addToSet must be an array but it was of type %s", typeName(x.Type))
	}
	op.each = arrayValues(x.Value)
	return nil
}

// pullCond is the condition of $pull: an equality, a regex, operators
// applied to each element as a value, or a query applied to each element
// document.
type pullCond struct {
	eq    *bson.RawValue
	t     test
	query node
}

func compilePull(v bson.RawValue) (pullCond, error) {
	switch {
	case v.Type == bson.TypeRegex:
		t, err := newRegexTest(v)
		return pullCond{t: t}, err
	case isOperatorDoc(v):
		it := newIter(v.Value)
		first, _ := it.next()
		switch string(first.name) {
		case "$and", "$or", "$nor":
		default:
			c := &compiler{}
			t, err := c.valueOps(v.Value)
			return pullCond{t: t}, err
		}
		fallthrough
	case v.Type == bson.TypeEmbeddedDocument:
		c := &compiler{}
		n, err := c.doc(v.Value, ctxElemMatch)
		return pullCond{query: n}, err
	}
	return pullCond{eq: &v}, nil
}

func (p pullCond) match(e bson.RawValue) (bool, error) {
	switch {
	case p.eq != nil:
		return Equal(e, *p.eq), nil
	case p.t != nil:
		return p.t.test(e)
	}
	if e.Type != bson.TypeEmbeddedDocument {
		return false, nil
	}
	return p.query.match(e.Value)
}

// Apply runs the update against doc and returns the new document. With doc
// nil it builds the document an upsert inserts: the equality predicates of
// upsertFilter seed it (for a replacement, only an _id equality), the
// operators run with $setOnInsert enabled, and an ObjectId _id is generated
// when none results; the output always has _id first. For an existing
// document, modified is false and out is doc itself when the update changes
// no byte. _id is immutable: changing it fails with MongoDB's code 66 text.
func (u *Update) Apply(doc bson.Raw, upsertFilter bson.Raw, now time.Time) (out bson.Raw, modified bool, err error) {
	if u.replacement != nil {
		return u.applyReplacement(doc, upsertFilter)
	}
	insert := doc == nil
	base := []byte(doc)
	if insert {
		base, err = seedFromFilter(upsertFilter)
		if err != nil {
			return nil, false, err
		}
	} else if err := newIter(base).err(); err != nil {
		return nil, false, err
	}
	oldID, hadID := lookupField(base, "_id")
	a := &applier{u: u, now: now, insert: insert, id: oldID}
	if err := a.resolveRenames(base); err != nil {
		return nil, false, err
	}
	res, err := a.applyObject(u.root, base)
	if err != nil {
		return nil, false, err
	}
	newID, hasID := lookupField(res, "_id")
	if hadID && (!hasID || !sameValue(oldID, newID)) {
		return nil, false, errf(CodeImmutableField, "Performing an update on the path '_id' would modify the immutable field '_id'")
	}
	if insert {
		res = idFirst(res, newID, hasID)
	}
	if len(res) > MaxDocSize {
		return nil, false, errf(CodeDocumentTooLarge, "Resulting document after update is larger than %d", MaxDocSize)
	}
	if err := checkStored(res, false); err != nil {
		return nil, false, err
	}
	if !insert && bytes.Equal(res, doc) {
		return doc, false, nil
	}
	return res, true, nil
}

func sameValue(a, b bson.RawValue) bool { return a.Type == b.Type && bytes.Equal(a.Value, b.Value) }

// idFirst returns d with its _id element first, generating an ObjectId when
// d has none.
func idFirst(d []byte, id bson.RawValue, hasID bool) []byte {
	if !hasID {
		oid := bson.NewObjectID()
		id = bson.RawValue{Type: bson.TypeObjectID, Value: oid[:]}
	}
	var w builder
	w.begin()
	w.elem("_id", id)
	it := newIter(d)
	for {
		e, ok := it.next()
		if !ok {
			break
		}
		if string(e.name) != "_id" {
			w.rawElem(e.raw)
		}
	}
	return w.end()
}

func (u *Update) applyReplacement(doc, filter bson.Raw) (bson.Raw, bool, error) {
	repID, hasRepID := lookupField(u.replacement, "_id")
	var id bson.RawValue
	hasID := false
	if doc != nil {
		oldID, ok := lookupField(doc, "_id")
		if ok && hasRepID && !sameValue(oldID, repID) {
			return nil, false, errf(CodeImmutableField, "After applying the update, the (immutable) field '_id' was found to have been altered to _id: %s", showValue(repID))
		}
		id, hasID = oldID, ok
		if !ok {
			id, hasID = repID, hasRepID
		}
	} else {
		id, hasID = repID, hasRepID
		if !hasID {
			pairs, err := equalityPairs(filter)
			if err != nil {
				return nil, false, err
			}
			for _, p := range pairs {
				if p.path == "_id" {
					id, hasID = p.val, true
				}
			}
		}
	}
	out := idFirst(u.replacement, id, hasID)
	if len(out) > MaxDocSize {
		return nil, false, errf(CodeDocumentTooLarge, "Resulting document after update is larger than %d", MaxDocSize)
	}
	if err := checkStored(out, false); err != nil {
		return nil, false, err
	}
	if doc != nil && bytes.Equal(out, doc) {
		return doc, false, nil
	}
	return out, true, nil
}

type eqPair struct {
	path string
	val  bson.RawValue
}

// equalityPairs collects the equality predicates an upsert copies into the
// new document: field: value, field: {$eq: value}, field: {$in: [value]}
// with one element, recursively through top-level $and. Regexes and
// operator expressions are skipped.
func equalityPairs(filter bson.Raw) ([]eqPair, error) {
	var out []eqPair
	var collect func(d []byte) error
	collect = func(d []byte) error {
		it := newIter(d)
		for {
			e, ok := it.next()
			if !ok {
				break
			}
			name := string(e.name)
			v := e.value()
			if strings.HasPrefix(name, "$") {
				if name != "$and" || v.Type != bson.TypeArray {
					continue
				}
				for _, sub := range arrayValues(v.Value) {
					if sub.Type == bson.TypeEmbeddedDocument {
						if err := collect(sub.Value); err != nil {
							return err
						}
					}
				}
				continue
			}
			switch {
			case v.Type == bson.TypeRegex:
			case isOperatorDoc(v):
				if eq, ok := lookupField(v.Value, "$eq"); ok {
					out = append(out, eqPair{name, eq})
				}
				if in, ok := lookupField(v.Value, "$in"); ok && in.Type == bson.TypeArray {
					if vals := arrayValues(in.Value); len(vals) == 1 && vals[0].Type != bson.TypeRegex {
						out = append(out, eqPair{name, vals[0]})
					}
				}
			default:
				out = append(out, eqPair{name, v})
			}
		}
		return it.err()
	}
	if len(filter) == 0 {
		return nil, nil
	}
	if err := collect(filter); err != nil {
		return nil, err
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].path < out[j].path })
	for i := 1; i < len(out); i++ {
		a, b := out[i-1].path, out[i].path
		if a == b {
			return nil, errf(CodeNotSingleValueField, "cannot infer query fields to set, path '%s' is matched twice", a)
		}
	}
	for i := range out {
		for j := range out {
			if i != j && strings.HasPrefix(out[j].path, out[i].path+".") {
				return nil, errf(CodeNotSingleValueField, "cannot infer query fields to set, both paths '%s' and '%s' are matched", out[j].path, out[i].path)
			}
		}
	}
	return out, nil
}

// seedFromFilter builds the starting document of an upsert from the
// filter's equality predicates.
func seedFromFilter(filter bson.Raw) ([]byte, error) {
	pairs, err := equalityPairs(filter)
	if err != nil {
		return nil, err
	}
	empty := buildDoc(nil, nil)
	if len(pairs) == 0 {
		return empty, nil
	}
	seed := &Update{root: &unode{}}
	for _, p := range pairs {
		if err := checkUpdatePath(p.path); err != nil {
			return nil, err
		}
		if err := seed.insert(p.path, &uop{kind: opSet, path: p.path, val: p.val}); err != nil {
			return nil, err
		}
	}
	seed.root.finish()
	a := &applier{u: seed, insert: true}
	return a.applyObject(seed.root, empty)
}

// applier holds the state of one Apply call, so a compiled Update can be
// shared across goroutines.
type applier struct {
	u         *Update
	now       time.Time
	insert    bool
	id        bson.RawValue // _id of the document, for messages
	renameVal []bson.RawValue
	renameOK  []bool
}

// resolveRenames reads every $rename source from the document before any
// operation runs. A source or target path that crosses an array is an
// error, as in MongoDB.
func (a *applier) resolveRenames(d []byte) error {
	if a.u.renames == 0 {
		return nil
	}
	a.renameVal = make([]bson.RawValue, a.u.renames)
	a.renameOK = make([]bool, a.u.renames)
	var visit func(n *unode) error
	visit = func(n *unode) error {
		if n.op != nil && n.op.kind == opRenameTo {
			v, ok, err := renamePath(d, n.op.from, "source")
			if err != nil {
				return err
			}
			if _, _, err := renamePath(d, splitPath(n.op.path), "destination"); err != nil {
				return err
			}
			a.renameVal[n.op.rename], a.renameOK[n.op.rename] = v, ok
		}
		for _, k := range n.kids {
			if err := visit(k); err != nil {
				return err
			}
		}
		return nil
	}
	return visit(a.u.root)
}

func renamePath(d []byte, parts []string, role string) (bson.RawValue, bool, error) {
	cur := docValue(d)
	for i, p := range parts {
		switch cur.Type {
		case bson.TypeEmbeddedDocument:
			v, ok := lookupField(cur.Value, p)
			if !ok {
				return missing, false, nil
			}
			cur = v
		case bson.TypeArray:
			return missing, false, badValue("The %s field cannot be an array element, '%s' in doc with %s", role, strings.Join(parts, "."), showField(parts[i-1], cur))
		default:
			return missing, false, nil
		}
	}
	return cur, true, nil
}

func (a *applier) applyObject(n *unode, obj []byte) ([]byte, error) {
	var w builder
	w.begin()
	matched := make([]bool, len(n.kids))
	it := newIter(obj)
	for {
		e, ok := it.next()
		if !ok {
			break
		}
		k, hit := n.idx[string(e.name)]
		if !hit {
			w.rawElem(e.raw)
			continue
		}
		matched[k] = true
		nv, present, err := a.applyChild(n.kids[k], e.value(), true)
		if err != nil {
			return nil, err
		}
		if present {
			w.elem(n.kids[k].name, nv)
		}
	}
	if err := it.err(); err != nil {
		return nil, err
	}
	for k, kid := range n.kids {
		if matched[k] {
			continue
		}
		nv, present, err := a.applyChild(kid, missing, false)
		if err != nil {
			return nil, err
		}
		if present {
			w.elem(kid.name, nv)
		}
	}
	return w.end(), nil
}

func (a *applier) applyChild(n *unode, cur bson.RawValue, exists bool) (bson.RawValue, bool, error) {
	if n.op != nil {
		return a.applyLeaf(n.op, cur, exists)
	}
	if !exists {
		sub, err := a.applyObject(n, buildDoc(nil, nil))
		if err != nil || docEmpty(sub) {
			return missing, false, err
		}
		return docValue(sub), true, nil
	}
	switch cur.Type {
	case bson.TypeEmbeddedDocument:
		sub, err := a.applyObject(n, cur.Value)
		if err != nil {
			return missing, false, err
		}
		return docValue(sub), true, nil
	case bson.TypeArray:
		return a.applyArray(n, cur)
	}
	if k := a.creator(n); k != nil {
		return missing, false, errf(CodePathNotViable, "Cannot create field '%s' in element %s", k.name, showField(n.name, cur))
	}
	return cur, true, nil
}

// maxPad bounds how far an update may extend an array with nulls.
const maxPad = 1500000

func (a *applier) applyArray(n *unode, cur bson.RawValue) (bson.RawValue, bool, error) {
	elems := arrayValues(cur.Value)
	for _, kid := range n.kids {
		idx, ok := arrayIndex(kid.name)
		if !ok {
			if a.creator(kid) != nil {
				return missing, false, errf(CodePathNotViable, "Cannot create field '%s' in element %s", kid.name, showField(n.name, cur))
			}
			continue
		}
		if idx < len(elems) {
			nv, present, err := a.applyChild(kid, elems[idx], true)
			if err != nil {
				return missing, false, err
			}
			if !present {
				nv = nullValue // $unset of an array element leaves null
			}
			elems[idx] = nv
			continue
		}
		nv, present, err := a.applyChild(kid, missing, false)
		if err != nil {
			return missing, false, err
		}
		if !present {
			continue
		}
		if idx-len(elems) > maxPad {
			return missing, false, badValue("can't backfill array to larger than %d elements", maxPad)
		}
		for len(elems) < idx {
			elems = append(elems, nullValue)
		}
		elems = append(elems, nv)
	}
	return arrayValue(elems), true, nil
}

// creator returns the first node under n whose operation would create a
// field, or nil when every operation under n leaves a missing path alone.
func (a *applier) creator(n *unode) *unode {
	if n.op != nil {
		switch n.op.kind {
		case opSet, opInc, opMul, opMin, opMax, opCurrentDate, opPush, opAddToSet:
			return n
		case opSetOnInsert:
			if a.insert {
				return n
			}
		case opRenameTo:
			if a.renameOK[n.op.rename] {
				return n
			}
		}
		return nil
	}
	for _, k := range n.kids {
		if a.creator(k) != nil {
			return k
		}
	}
	return nil
}

func (a *applier) applyLeaf(op *uop, cur bson.RawValue, exists bool) (bson.RawValue, bool, error) {
	switch op.kind {
	case opSet:
		return op.val, true, nil
	case opSetOnInsert:
		if a.insert {
			return op.val, true, nil
		}
		return cur, exists, nil
	case opUnset, opRenameFrom:
		return missing, false, nil
	case opRenameTo:
		if a.renameOK[op.rename] {
			return a.renameVal[op.rename], true, nil
		}
		return cur, exists, nil
	case opInc, opMul:
		return a.arith(op, cur, exists)
	case opMin, opMax:
		if !exists {
			return op.val, true, nil
		}
		c := Compare(op.val, cur)
		if (op.kind == opMin && c < 0) || (op.kind == opMax && c > 0) {
			return op.val, true, nil
		}
		return cur, true, nil
	case opCurrentDate:
		if op.dateTS {
			ts := uint64(uint32(a.now.Unix()))<<32 | 1
			return bson.RawValue{Type: bson.TypeTimestamp, Value: binary.LittleEndian.AppendUint64(nil, ts)}, true, nil
		}
		return bson.RawValue{Type: bson.TypeDateTime, Value: binary.LittleEndian.AppendUint64(nil, uint64(a.now.UnixMilli()))}, true, nil
	case opPush:
		return a.push(op, cur, exists)
	case opAddToSet:
		return a.addToSet(op, cur, exists)
	case opPop:
		if !exists {
			return missing, false, nil
		}
		if cur.Type != bson.TypeArray {
			return missing, false, badValue("Path '%s' contains an element of non-array type '%s'", op.path, typeName(cur.Type))
		}
		elems := arrayValues(cur.Value)
		if len(elems) == 0 {
			return cur, true, nil
		}
		if op.popFirst {
			elems = elems[1:]
		} else {
			elems = elems[:len(elems)-1]
		}
		return arrayValue(elems), true, nil
	case opPull, opPullAll:
		if !exists {
			return missing, false, nil
		}
		if cur.Type != bson.TypeArray {
			name := "$pull"
			if op.kind == opPullAll {
				name = "$pullAll"
			}
			return missing, false, badValue("Cannot apply %s to a non-array value", name)
		}
		elems := arrayValues(cur.Value)
		kept := elems[:0:0]
		for _, e := range elems {
			var drop bool
			if op.kind == opPull {
				var err error
				if drop, err = op.pull.match(e); err != nil {
					return missing, false, err
				}
			} else {
				for _, v := range op.each {
					if Equal(e, v) {
						drop = true
						break
					}
				}
			}
			if !drop {
				kept = append(kept, e)
			}
		}
		if len(kept) == len(elems) {
			return cur, true, nil
		}
		return arrayValue(kept), true, nil
	}
	return cur, exists, nil
}

func (a *applier) idText() string { return showField("_id", a.id) }

func (a *applier) arith(op *uop, cur bson.RawValue, exists bool) (bson.RawValue, bool, error) {
	operand, _ := numberOf(op.val)
	if !exists {
		if op.kind == opInc {
			return op.val, true, nil
		}
		switch operand.kind {
		case bson.TypeInt32:
			return int32Value(0), true, nil
		case bson.TypeInt64:
			return int64Value(0), true, nil
		}
		return doubleValue(0), true, nil
	}
	name := "$inc"
	if op.kind == opMul {
		name = "$mul"
	}
	x, ok := numberOf(cur)
	if !ok {
		return missing, false, errf(CodeTypeMismatch, "Cannot apply %s to a value of non-numeric type. %s has the field '%s' of non-numeric type %s", name, a.idText(), op.path, typeName(cur.Type))
	}
	if x.kind == bson.TypeDecimal128 {
		return missing, false, errDecimal
	}
	if x.kind == bson.TypeDouble || operand.kind == bson.TypeDouble {
		if op.kind == opInc {
			return doubleValue(x.float() + operand.float()), true, nil
		}
		return doubleValue(x.float() * operand.float()), true, nil
	}
	var r int64
	var overflow bool
	if op.kind == opInc {
		r = x.i + operand.i
		overflow = (x.i >= 0) == (operand.i >= 0) && (r >= 0) != (x.i >= 0)
	} else {
		r, overflow = mulInt64(x.i, operand.i)
	}
	if x.kind == bson.TypeInt32 && operand.kind == bson.TypeInt32 {
		// Two int32s never overflow int64; the result stays int32 when it
		// fits, as MongoDB's SafeNum promotes only on overflow.
		if r >= math.MinInt32 && r <= math.MaxInt32 {
			return int32Value(int32(r)), true, nil
		}
		return int64Value(r), true, nil
	}
	if overflow {
		return missing, false, badValue("Failed to apply %s operations to current value (%s) for document %s", name, showValue(cur), a.idText())
	}
	return int64Value(r), true, nil
}

func mulInt64(x, y int64) (int64, bool) {
	if x == 0 || y == 0 {
		return 0, false
	}
	neg := (x < 0) != (y < 0)
	ux, uy := absU64(x), absU64(y)
	hi, lo := bits.Mul64(ux, uy)
	if hi != 0 {
		return 0, true
	}
	if neg {
		if lo > 1<<63 {
			return 0, true
		}
		return int64(-lo), false
	}
	if lo > math.MaxInt64 {
		return 0, true
	}
	return int64(lo), false
}

func absU64(x int64) uint64 {
	if x < 0 {
		return uint64(-x)
	}
	return uint64(x)
}

func (a *applier) push(op *uop, cur bson.RawValue, exists bool) (bson.RawValue, bool, error) {
	var elems []bson.RawValue
	if exists {
		if cur.Type != bson.TypeArray {
			return missing, false, badValue("The field '%s' must be an array but is of type %s in document %s", op.path, typeName(cur.Type), a.idText())
		}
		elems = arrayValues(cur.Value)
	}
	pos := len(elems)
	if op.position != nil {
		p := *op.position
		if p < 0 {
			p += int64(len(elems))
			if p < 0 {
				p = 0
			}
		}
		if p < int64(len(elems)) {
			pos = int(p)
		}
	}
	out := make([]bson.RawValue, 0, len(elems)+len(op.each))
	out = append(out, elems[:pos]...)
	out = append(out, op.each...)
	out = append(out, elems[pos:]...)
	switch {
	case op.sortDir != 0:
		sort.SliceStable(out, func(i, j int) bool { return Compare(out[i], out[j])*op.sortDir < 0 })
	case op.sortSpec != nil:
		keys := make([][]byte, len(out))
		empty := buildDoc(nil, nil)
		for i, e := range out {
			d := empty
			if e.Type == bson.TypeEmbeddedDocument {
				d = e.Value
			}
			keys[i] = SortKey(nil, d, op.sortSpec)
		}
		idx := make([]int, len(out))
		for i := range idx {
			idx[i] = i
		}
		sort.SliceStable(idx, func(i, j int) bool { return bytes.Compare(keys[idx[i]], keys[idx[j]]) < 0 })
		sorted := make([]bson.RawValue, len(out))
		for i, k := range idx {
			sorted[i] = out[k]
		}
		out = sorted
	}
	if op.slice != nil {
		n := *op.slice
		switch {
		case n >= 0 && n < int64(len(out)):
			out = out[:n]
		case n < 0 && -n < int64(len(out)):
			out = out[int64(len(out))+n:]
		}
	}
	return arrayValue(out), true, nil
}

func (a *applier) addToSet(op *uop, cur bson.RawValue, exists bool) (bson.RawValue, bool, error) {
	var elems []bson.RawValue
	if exists {
		if cur.Type != bson.TypeArray {
			return missing, false, badValue("Cannot apply $addToSet to non-array field. Field named '%s' has non-array type %s", op.path, typeName(cur.Type))
		}
		elems = arrayValues(cur.Value)
	}
	added := false
	for _, v := range op.each {
		dup := false
		for _, e := range elems {
			if Equal(e, v) {
				dup = true
				break
			}
		}
		if !dup {
			elems = append(elems, v)
			added = true
		}
	}
	if exists && !added {
		return cur, true, nil
	}
	return arrayValue(elems), true, nil
}
