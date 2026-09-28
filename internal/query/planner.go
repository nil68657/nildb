package query

import (
	"bytes"
	"fmt"
	"slices"
	"strings"

	"github.com/nil68657/nildb/internal/catalog"
	"github.com/nil68657/nildb/internal/doc"
	"github.com/nil68657/nildb/internal/keyenc"
	"github.com/nil68657/nildb/internal/layout"
	"github.com/nil68657/nildb/internal/store"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// Planner errors.
var (
	errBadHint   = &doc.Error{Code: doc.CodeBadValue, Msg: "hint provided does not correspond to an existing index"}
	errHintUsage = &doc.Error{Code: doc.CodeBadValue, Msg: "hint provided does not correspond to a usable index for this query"}
	errNearSort  = &doc.Error{Code: doc.CodeBadValue, Msg: "$near and $nearSphere sort by distance and cannot be combined with SORT in v1"}
)

// maxIntervals bounds the index ranges one plan builds from $in lists over
// a compound index; past it the planner bounds fewer fields.
const maxIntervals = 4096

// PlanFind plans a find. The filter is compiled here with the geo hooks,
// so $geoWithin and $geoIntersects work with or without a 2dsphere index
// and $near needs one. A nil collection plans an empty result, as MongoDB
// does for a namespace that does not exist.
func (e *Engine) PlanFind(coll *catalog.Collection, f Find, o Options) (*Plan, error) {
	m, err := doc.Compile(f.Filter, geoHooks{near: true})
	if err != nil {
		return nil, err
	}
	sortSpec, err := doc.ParseSort(f.Sort)
	if err != nil {
		return nil, err
	}
	proj, err := doc.CompileProjection(f.Project)
	if err != nil {
		return nil, err
	}
	if f.Skip < 0 || f.Limit < 0 {
		return nil, &doc.Error{Code: doc.CodeBadValue, Msg: "skip and limit must be non-negative"}
	}
	filter := m
	if m.MatchAll() {
		filter = nil
	}
	preds := m.IndexablePredicates()
	var sp *srcPlan
	switch near := nearPredicate(preds); {
	case near != nil:
		if len(sortSpec) > 0 {
			return nil, errNearSort
		}
		limit := int64(0)
		if f.Limit > 0 {
			limit = f.Skip + f.Limit
		}
		sp, err = e.nearFindSource(coll, f.Filter, *near, limit, o)
		filter = nil // the near source applies the rest of the filter itself
	case coll == nil:
		sp = emptySource()
	default:
		sp, err = e.source(coll, preds, sortSpec, hintSpec{name: f.Hint, key: f.HintKey}, o)
	}
	if err != nil {
		return nil, err
	}
	p := &Plan{e: e, opts: o, Source: sp.kind, Index: sp.index, Bounds: sp.bounds, EstimatedRows: sp.est}
	if filter != nil {
		p.Stages = append(p.Stages, "$match")
	}
	needSort := len(sortSpec) > 0 && !sp.sortOK
	var topK int64
	if needSort && f.Limit > 0 {
		topK = f.Skip + f.Limit
	}
	if needSort {
		p.Stages = append(p.Stages, sortLabel(topK))
	}
	if f.Skip > 0 {
		p.Stages = append(p.Stages, fmt.Sprintf("$skip %d", f.Skip))
	}
	if f.Limit > 0 {
		p.Stages = append(p.Stages, fmt.Sprintf("$limit %d", f.Limit))
	}
	project := !emptyDoc(f.Project)
	if project {
		p.Stages = append(p.Stages, "$project")
	}
	p.build = func(x *Exec) (Operator, error) {
		op := sp.open(x, filter)
		if needSort {
			op = newSortOp(x, op, sortSpec, topK)
		}
		if f.Skip > 0 {
			op = &skipOp{in: op, n: f.Skip}
		}
		if f.Limit > 0 {
			op = &limitOp{in: op, n: f.Limit}
		}
		if project {
			op = &projectOp{in: op, p: proj}
		}
		return op, nil
	}
	return p, nil
}

func sortLabel(topK int64) string {
	if topK > 0 {
		return fmt.Sprintf("$sort (top %d)", topK)
	}
	return "$sort"
}

func emptyDoc(d bson.Raw) bool { return len(d) <= 5 }

// srcPlan is a chosen source: how to read the collection, what EXPLAIN
// says about it, and whether it already yields rows in the requested sort
// order.
type srcPlan struct {
	kind   string // "index" or "rowscan"
	index  string
	bounds []string
	sortOK bool
	est    int64
	open   func(x *Exec, filter *doc.Matcher) Operator
}

func emptySource() *srcPlan {
	return &srcPlan{kind: "rowscan", open: func(*Exec, *doc.Matcher) Operator { return emptyOp{} }}
}

type hintSpec struct {
	name string
	key  bson.Raw
}

func (h hintSpec) set() bool { return h.name != "" || !emptyDoc(h.key) }

// source picks how to read coll for the indexable predicates of a filter.
// Candidates are the _id index (the doc column family itself) and every
// ready btree index whose leading fields the predicates bound; the best
// one wins by: bounds that hold no key at all, exact lookups on a unique
// index, more leading equality fields, a range after them, providing the
// sort, fewer key fields, older index. A $geoWithin or $geoIntersects on a
// field with a ready 2dsphere index beats any btree candidate without an
// equality field. With no bounded candidate, an index that yields the
// requested sort order is scanned whole; otherwise the plan is a rowscan.
func (e *Engine) source(coll *catalog.Collection, preds []doc.Predicate, sort doc.SortSpec, hint hintSpec, o Options) (*srcPlan, error) {
	byField := predsByField(preds)
	if hint.set() {
		return e.hinted(coll, preds, byField, sort, hint, o)
	}
	var best *candidate
	var bestSort, bestRev bool
	consider := func(c *candidate) {
		if c == nil {
			return
		}
		ok, rev := c.sortOrder(sort)
		if best == nil || c.better(ok, best, bestSort) {
			best, bestSort, bestRev = c, ok, rev
		}
	}
	consider(idCandidate(coll.ID, byField["_id"]))
	for _, ix := range coll.Indexes() {
		if ix.Kind == catalog.Btree && ix.State == catalog.Ready {
			consider(btreeCandidate(ix, byField))
		}
	}
	if best == nil || (best.eq == 0 && !best.empty) {
		g, err := e.geoWithinSource(coll, preds, nil, o)
		if err != nil || g != nil {
			return g, err
		}
	}
	if best != nil {
		return e.fromCandidate(coll, best, bestSort, bestRev, o), nil
	}
	if len(sort) > 0 {
		if ok, rev := sortOrder(idKeys, nil, false, sort); ok {
			return e.rowscan(coll, true, rev, o), nil
		}
		for _, ix := range coll.Indexes() {
			if ix.Kind != catalog.Btree || ix.State != catalog.Ready || ix.Sparse {
				continue
			}
			c := fullCandidate(ix)
			if ok, rev := c.sortOrder(sort); ok {
				return e.fromCandidate(coll, c, true, rev, o), nil
			}
		}
	}
	return e.rowscan(coll, false, false, o), nil
}

// hinted plans with the index the query names.
func (e *Engine) hinted(coll *catalog.Collection, preds []doc.Predicate, byField map[string][]doc.Predicate, sort doc.SortSpec, hint hintSpec, o Options) (*srcPlan, error) {
	if hint.name == "_id_" || (hint.name == "" && keyPatternIs(hint.key, idKeys, catalog.Btree)) {
		c := idCandidate(coll.ID, byField["_id"])
		if c == nil {
			c = fullIDCandidate(coll.ID)
		}
		ok, rev := c.sortOrder(sort)
		return e.fromCandidate(coll, c, ok, rev, o), nil
	}
	var ix *catalog.Index
	for _, cand := range coll.Indexes() {
		if cand.State != catalog.Ready {
			continue
		}
		if (hint.name != "" && cand.Name == hint.name) || (hint.name == "" && keyPatternIs(hint.key, cand.Keys, cand.Kind)) {
			ix = cand
			break
		}
	}
	if ix == nil {
		return nil, errBadHint
	}
	if ix.Kind == catalog.TwoDSphere {
		g, err := e.geoWithinSource(coll, preds, ix, o)
		if err == nil && g == nil {
			err = errHintUsage
		}
		return g, err
	}
	c := btreeCandidate(ix, byField)
	if c == nil {
		c = fullCandidate(ix)
	}
	ok, rev := c.sortOrder(sort)
	return e.fromCandidate(coll, c, ok, rev, o), nil
}

// keyPatternIs reports whether the key pattern document key describes keys
// of the given kind: the same fields in order, 1 or -1 for btree fields
// and "2dsphere" for a 2dsphere field.
func keyPatternIs(key bson.Raw, keys []catalog.Key, kind catalog.IndexKind) bool {
	if emptyDoc(key) {
		return false
	}
	it := iterDoc(key)
	for _, k := range keys {
		e, ok := it.next()
		if !ok || string(e.name) != k.Field {
			return false
		}
		v := e.value()
		if kind == catalog.TwoDSphere {
			if s, ok := strOf(v); !ok || s != "2dsphere" {
				return false
			}
			continue
		}
		n, ok := numOf(v)
		if !ok || (n.float() != 1 && n.float() != -1) || (n.float() == -1) != k.Desc {
			return false
		}
	}
	_, more := it.next()
	return !more
}

func (e *Engine) rowscan(coll *catalog.Collection, sortOK, reverse bool, o Options) *srcPlan {
	id := coll.ID
	return &srcPlan{
		kind:   "rowscan",
		sortOK: sortOK,
		est:    e.count(o, coll),
		open: func(x *Exec, f *doc.Matcher) Operator {
			return newDocScan(x, id, nil, reverse, f)
		},
	}
}

func (e *Engine) fromCandidate(coll *catalog.Collection, c *candidate, sortOK, rev bool, o Options) *srcPlan {
	sp := &srcPlan{kind: "index", index: c.name, bounds: c.bounds, sortOK: sortOK, est: e.estimate(o, coll, c)}
	id := coll.ID
	if !sortOK {
		rev = false
	}
	switch {
	case c.empty:
		sp.open = func(*Exec, *doc.Matcher) Operator { return emptyOp{} }
	case c.ix == nil && c.ids != nil:
		ids := c.ids
		sp.open = func(x *Exec, f *doc.Matcher) Operator { return newDocGet(x, id, ids, rev, f) }
	case c.ix == nil:
		ranges := c.ranges
		sp.open = func(x *Exec, f *doc.Matcher) Operator { return newDocScan(x, id, ranges, rev, f) }
	default:
		ix, ranges := c.ix, c.ranges
		sp.open = func(x *Exec, f *doc.Matcher) Operator { return newIdxScan(x, id, ix, ranges, rev, f) }
	}
	return sp
}

// count reads the collection's document count as the plan's reader sees
// it; the planner's estimates start from it.
func (e *Engine) count(o Options, coll *catalog.Collection) int64 {
	r := o.Reader
	if r == nil {
		r = e.st
	}
	n, err := catalog.CountAt(r, coll)
	if err != nil {
		return 0
	}
	return n
}

// estimate is the count scaled by the share of the index's on-disk bytes
// the bounds cover, which RocksDB's ApproximateSizes reports; data still in
// memtables has no size yet, so the estimate falls back to the count.
func (e *Engine) estimate(o Options, coll *catalog.Collection, c *candidate) int64 {
	total := e.count(o, coll)
	switch {
	case c.empty:
		return 0
	case c.ids != nil:
		return min(int64(len(c.ids)), total)
	case c.point:
		return min(int64(len(c.ranges)), total)
	case c.full:
		return total
	}
	cf, id := store.CFIdx, uint32(0)
	if c.ix == nil {
		cf, id = store.CFDoc, coll.ID
	} else {
		id = c.ix.ID
	}
	lo, hi := layout.IDBounds(id)
	ranges := [][2][]byte{{lo, hi}}
	for _, r := range c.ranges {
		ranges = append(ranges, [2][]byte{r.lo, r.hi})
	}
	sizes, err := e.st.ApproxSizes(cf, ranges)
	if err != nil || len(sizes) != len(ranges) || sizes[0] == 0 {
		return total
	}
	var sum uint64
	for _, s := range sizes[1:] {
		sum += s
	}
	est := int64(float64(total) * float64(sum) / float64(sizes[0]))
	if total > 0 {
		est = max(est, 1)
	}
	return min(est, total)
}

// predsByField groups the predicates btree bounds can use by field.
func predsByField(preds []doc.Predicate) map[string][]doc.Predicate {
	out := make(map[string][]doc.Predicate)
	for _, p := range preds {
		switch p.Op {
		case doc.OpEq, doc.OpIn, doc.OpGt, doc.OpGte, doc.OpLt, doc.OpLte, doc.OpRegex:
			out[p.Field] = append(out[p.Field], p)
		}
	}
	return out
}

func nearPredicate(preds []doc.Predicate) *doc.Predicate {
	for i := range preds {
		if preds[i].Op == doc.OpNear || preds[i].Op == doc.OpNearSphere {
			return &preds[i]
		}
	}
	return nil
}

// candidate is one way to read a collection through an index.
type candidate struct {
	name   string
	ix     *catalog.Index // nil for the _id index, which is the doc CF itself
	keys   []catalog.Key
	ranges []keyRange // absolute keys, sorted and disjoint
	ids    [][]byte   // _id point lookups (ks(_id)); ranges unused
	eq     int        // leading fields bound by equality
	rng    bool       // a range bound follows the equality fields
	point  bool       // exact lookups: unique and every field bound by equality
	full   bool       // no bounds: every entry
	empty  bool       // the bounds hold no key
	fixed  map[string]bool
	bounds []string
}

var idKeys = []catalog.Key{{Field: "_id"}}

func (c *candidate) multikey() bool { return c.ix != nil && c.ix.Multikey() }

func (c *candidate) sortOrder(sort doc.SortSpec) (ok, reverse bool) {
	return sortOrder(c.keys, c.fixed, c.multikey(), sort)
}

// better reports whether c (providing the sort when cs) beats b (when bs).
func (c *candidate) better(cs bool, b *candidate, bs bool) bool {
	score := func(x *candidate, s bool) [5]int {
		var r [5]int
		switch {
		case x.empty:
			r[0] = 2
		case x.point:
			r[0] = 1
		}
		r[1] = x.eq
		if x.rng {
			r[2] = 1
		}
		if s {
			r[3] = 1
		}
		r[4] = -len(x.keys)
		return r
	}
	rc, rb := score(c, cs), score(b, bs)
	if rc != rb {
		return slices.Compare(rc[:], rb[:]) > 0
	}
	return c.ix != nil && b.ix != nil && c.ix.ID < b.ix.ID
}

// sortOrder reports whether reading an index whose key is keys, with the
// fields in fixed bound to one value each, yields documents in sort order,
// and whether it must be read backwards. A multikey index never does: a
// document sorts by its smallest (or largest) element, while its entries
// sit at every element.
func sortOrder(keys []catalog.Key, fixed map[string]bool, multikey bool, sort doc.SortSpec) (ok, reverse bool) {
	if len(sort) == 0 {
		return true, false
	}
	if multikey {
		return false, false
	}
	var order []catalog.Key
	for _, k := range keys {
		if !fixed[k.Field] {
			order = append(order, k)
		}
	}
	var want doc.SortSpec
	for _, s := range sort {
		if !fixed[s.Field] {
			want = append(want, s)
		}
	}
	if len(want) == 0 {
		return true, false
	}
	if len(want) > len(order) {
		return false, false
	}
	rev := want[0].Desc != order[0].Desc
	for j, s := range want {
		if s.Field != order[j].Field || (s.Desc != order[j].Desc) != rev {
			return false, false
		}
	}
	return true, rev
}

// idCandidate bounds the doc column family by predicates on _id.
func idCandidate(coll uint32, ps []doc.Predicate) *candidate {
	if len(ps) == 0 {
		return nil
	}
	fb, ok := boundField(ps, false, true, false)
	if !ok {
		return nil
	}
	c := &candidate{name: "_id_", keys: idKeys, fixed: map[string]bool{}, bounds: []string{"_id: " + fb.text}}
	switch {
	case fb.empty:
		c.empty = true
	case fb.points != nil:
		c.ids, c.eq, c.point = fb.points, 1, true
		if len(fb.points) == 1 {
			c.fixed["_id"] = true
		}
	default:
		c.rng = true
		c.ranges = absRanges(layout.IDPrefix(nil, coll), fb.ivls)
	}
	return c
}

func fullIDCandidate(coll uint32) *candidate {
	lo, hi := layout.IDBounds(coll)
	return &candidate{name: "_id_", keys: idKeys, full: true, ranges: []keyRange{{lo, hi}},
		bounds: []string{"_id: [MinKey, MaxKey]"}}
}

// fullCandidate reads every entry of ix.
func fullCandidate(ix *catalog.Index) *candidate {
	lo, hi := layout.IDBounds(ix.ID)
	c := &candidate{name: ix.Name, ix: ix, keys: ix.Keys, full: true, ranges: []keyRange{{lo, hi}}}
	for _, k := range ix.Keys {
		c.bounds = append(c.bounds, k.Field+": [MinKey, MaxKey]")
	}
	return c
}

// btreeCandidate bounds ix by the predicates on its leading fields:
// equality and $in values extend a key prefix field by field, and a range
// or regex ends it. It returns nil when no leading field is bound, or when
// ix is sparse and every bound allows a missing field (a sparse index
// skips documents missing every indexed field).
func btreeCandidate(ix *catalog.Index, byField map[string][]doc.Predicate) *candidate {
	c := &candidate{name: ix.Name, ix: ix, keys: ix.Keys, fixed: map[string]bool{}}
	prefixes := [][]byte{nil}
	var rel []fieldIvl
	nonNull := false
	n := len(ix.Keys)
	for i, k := range ix.Keys {
		ps := byField[k.Field]
		if len(ps) == 0 {
			break
		}
		fb, ok := boundField(ps, k.Desc, ix.Unique && i == n-1, ix.Multikey())
		if !ok {
			break
		}
		if fb.empty {
			c.empty = true
			c.bounds = append(c.bounds, k.Field+": "+fb.text)
			return c
		}
		if fb.points != nil {
			if len(prefixes)*len(fb.points) > maxIntervals {
				break
			}
			c.bounds = append(c.bounds, k.Field+": "+fb.text)
			next := make([][]byte, 0, len(prefixes)*len(fb.points))
			for _, p := range prefixes {
				for _, pt := range fb.points {
					next = append(next, concat(p, pt))
				}
			}
			prefixes = next
			c.eq++
			if len(fb.points) == 1 {
				c.fixed[k.Field] = true
			}
			nonNull = nonNull || !fb.null
			continue
		}
		if len(prefixes)*len(fb.ivls) > maxIntervals {
			break
		}
		c.bounds = append(c.bounds, k.Field+": "+fb.text)
		for _, p := range prefixes {
			for _, iv := range fb.ivls {
				rel = append(rel, fieldIvl{concat(p, iv.lo), concat(p, iv.hi)})
			}
		}
		c.rng = true
		nonNull = nonNull || !fb.null
		break
	}
	if c.eq == 0 && !c.rng {
		return nil
	}
	if ix.Sparse && !nonNull {
		return nil
	}
	if !c.rng {
		for _, p := range prefixes {
			rel = append(rel, fieldIvl{p, plusByte(p, 0xFF)})
		}
	}
	c.point = ix.Unique && c.eq == n && !c.rng
	c.ranges = absRanges(layout.IDPrefix(nil, ix.ID), rel)
	return c
}

// absRanges prefixes relative intervals, drops empty ones, sorts them and
// merges overlaps, so a non-multikey scan meets each entry once.
func absRanges(prefix []byte, rel []fieldIvl) []keyRange {
	out := make([]keyRange, 0, len(rel))
	for _, iv := range rel {
		lo, hi := concat(prefix, iv.lo), concat(prefix, iv.hi)
		if bytes.Compare(lo, hi) < 0 {
			out = append(out, keyRange{lo, hi})
		}
	}
	slices.SortFunc(out, func(a, b keyRange) int { return bytes.Compare(a.lo, b.lo) })
	merged := out[:0]
	for _, r := range out {
		if n := len(merged); n > 0 && bytes.Compare(r.lo, merged[n-1].hi) <= 0 {
			if bytes.Compare(r.hi, merged[n-1].hi) > 0 {
				merged[n-1].hi = r.hi
			}
			continue
		}
		merged = append(merged, r)
	}
	return merged
}

// fieldIvl is [lo, hi) over the encoded bytes of one index field and
// whatever follows it in the key.
type fieldIvl struct{ lo, hi []byte }

// fullIvl holds every encoded value: type bytes run from 0x0A to 0xF5.
var fullIvl = fieldIvl{lo: nil, hi: []byte{0xFF}}

// fieldBound is what the predicates on one index field allow.
type fieldBound struct {
	points [][]byte   // equality: distinct encodings, sorted; later fields extend them
	ivls   []fieldIvl // otherwise a range, sorted and disjoint
	text   string     // EXPLAIN text
	null   bool       // a missing field can satisfy the bound
	empty  bool       // no value can
}

func encode(v bson.RawValue, desc bool) ([]byte, bool) {
	var b []byte
	var err error
	if desc {
		b, err = keyenc.EncodeDesc(nil, v)
	} else {
		b, err = keyenc.Encode(nil, v)
	}
	return b, err == nil
}

// bracket returns the interval holding every encoding of v's type class:
// the other end of a one-sided range under MongoDB's type bracketing.
func bracket(v bson.RawValue, desc bool) (lo, hi []byte) {
	t := keyenc.TypeOrder(v.Type)
	if desc {
		return []byte{0xFF - t}, []byte{0xFF - t + 1}
	}
	return []byte{t}, []byte{t + 1}
}

// nullish reports an operand under which a missing field (indexed as null)
// can match.
func nullish(v bson.RawValue) bool {
	return v.Type == bson.TypeNull || v.Type == bson.TypeMinKey || v.Type == bson.TypeMaxKey
}

func isRangeOp(op doc.Op) bool {
	return op == doc.OpGt || op == doc.OpGte || op == doc.OpLt || op == doc.OpLte
}

// boundField turns the predicates on one field into index bounds. Equality
// wins over $in, which wins over ranges, which win over an anchored regex;
// each is necessary on its own, so choosing one keeps the bounds a
// superset. Ranges on a non-multikey field intersect; on a multikey field
// only the first is used, because {a: {$gt: 1, $lt: 5}} matches
// {a: [0, 10]}. uniqueLast is set for the last field of a unique index,
// where no encoding follows the value in the key.
//
// Interval ends follow keyenc's rules: a value's entries followed by more
// encodings lie in [enc, enc+0xFF), since every type byte is below 0xFF,
// and a string's NUL extensions ("a\x00...") encode as enc+0xFF..., so
// they fall outside equality and inside "greater than". A descending
// string that ends a unique key breaks that order (keyenc's documented
// corner), and its upper-bounded ranges widen to the whole string bracket.
func boundField(ps []doc.Predicate, desc, uniqueLast, multikey bool) (fieldBound, bool) {
	for _, p := range ps {
		if p.Op != doc.OpEq {
			continue
		}
		if e, ok := encode(p.Values[0], desc); ok {
			return fieldBound{points: [][]byte{e}, text: pointText(p.Values), null: nullish(p.Values[0])}, true
		}
	}
	for _, p := range ps {
		if p.Op != doc.OpIn {
			continue
		}
		fb := fieldBound{text: pointText(p.Values)}
		usable := true
		for _, v := range p.Values {
			e, ok := encode(v, desc)
			if !ok {
				usable = false
				break
			}
			fb.points = append(fb.points, e)
			fb.null = fb.null || nullish(v)
		}
		if !usable {
			continue
		}
		if len(fb.points) == 0 {
			fb.empty = true
			return fb, true
		}
		slices.SortFunc(fb.points, bytes.Compare)
		fb.points = slices.CompactFunc(fb.points, bytes.Equal)
		return fb, true
	}
	if fb, ok := rangeBound(ps, desc, uniqueLast, multikey); ok {
		return fb, true
	}
	for _, p := range ps {
		if p.Op != doc.OpRegex {
			continue
		}
		prefix := keyenc.AppendString(nil, p.Prefix)
		prefix = prefix[:len(prefix)-1] // every string starting with Prefix encodes with these bytes first
		if desc {
			for i := range prefix {
				prefix[i] ^= 0xFF
			}
		}
		ivls := []fieldIvl{{prefix, keyenc.Next(prefix)}}
		text := fmt.Sprintf("prefix %q", p.Prefix)
		// A regex also matches a stored Regex value with the same pattern
		// and options, which sits in its own type bracket.
		if e, ok := encode(p.Values[0], desc); ok {
			ivls = append(ivls, fieldIvl{e, plusByte(e, 0xFF)})
			re := relaxed(p.Values[0])
			text += ", [" + re + ", " + re + "]"
		}
		slices.SortFunc(ivls, func(a, b fieldIvl) int { return bytes.Compare(a.lo, b.lo) })
		return fieldBound{ivls: ivls, text: text}, true
	}
	return fieldBound{}, false
}

// rangeBound bounds a field by its $gt, $gte, $lt and $lte predicates.
func rangeBound(ps []doc.Predicate, desc, uniqueLast, multikey bool) (fieldBound, bool) {
	var iv fieldIvl
	var lower, upper *doc.Predicate
	used := 0
	fb := fieldBound{}
	for i := range ps {
		p := &ps[i]
		if !isRangeOp(p.Op) || (used > 0 && multikey) {
			continue
		}
		pi, ok := rangeIvl(*p, desc, uniqueLast)
		if !ok {
			continue
		}
		if used == 0 {
			iv = pi
		} else {
			iv = fieldIvl{maxBytes(iv.lo, pi.lo), minBytes(iv.hi, pi.hi)}
		}
		used++
		fb.null = fb.null || nullish(p.Values[0])
		if p.Op == doc.OpGt || p.Op == doc.OpGte {
			if lower == nil || tighter(*p, *lower, 1) {
				lower = p
			}
		} else if upper == nil || tighter(*p, *upper, -1) {
			upper = p
		}
	}
	if used == 0 {
		return fb, false
	}
	switch {
	case bytes.Compare(iv.lo, iv.hi) >= 0:
		fb.empty = true
	default:
		fb.ivls = []fieldIvl{iv}
	}
	if iv.lo == nil && bytes.Equal(iv.hi, fullIvl.hi) {
		fb.text = "[MinKey, MaxKey]"
		return fb, true
	}
	lb, lo, hi, rb := "[", "-inf", "inf", "]"
	if lower != nil {
		lo = relaxed(lower.Values[0])
		if lower.Op == doc.OpGt {
			lb = "("
		}
	}
	if upper != nil {
		hi = relaxed(upper.Values[0])
		if upper.Op == doc.OpLt {
			rb = ")"
		}
	}
	fb.text = lb + lo + ", " + hi + rb
	return fb, true
}

// tighter reports whether bound p is tighter than cur in direction dir (+1
// for lower bounds, -1 for upper bounds), by BSON value order; at equal
// values an exclusive bound is the tighter one.
func tighter(p, cur doc.Predicate, dir int) bool {
	c := doc.Compare(p.Values[0], cur.Values[0]) * dir
	excl := p.Op == doc.OpGt || p.Op == doc.OpLt
	curExcl := cur.Op == doc.OpGt || cur.Op == doc.OpLt
	return c > 0 || (c == 0 && excl && !curExcl)
}

// rangeIvl is the interval of one comparison, bracketed by the operand's
// type. Exclusive ends skip the operand's own entries where the encoding
// allows; an over-wide end is still a superset, which the full filter
// narrows.
func rangeIvl(p doc.Predicate, desc, uniqueLast bool) (fieldIvl, bool) {
	v := p.Values[0]
	e, ok := encode(v, desc)
	if !ok {
		return fieldIvl{}, false
	}
	blo, bhi := bracket(v, desc)
	switch p.Op {
	case doc.OpGt, doc.OpGte:
		if v.Type == bson.TypeMinKey {
			return fullIvl, true
		}
		if !desc {
			if p.Op == doc.OpGt {
				return fieldIvl{plusByte(e, 0xFF), bhi}, true
			}
			return fieldIvl{e, bhi}, true
		}
		if p.Op == doc.OpGt {
			return fieldIvl{blo, plusByte(e, 0x01)}, true
		}
		return fieldIvl{blo, plusByte(e, 0xFF)}, true
	case doc.OpLt, doc.OpLte:
		if v.Type == bson.TypeMaxKey {
			return fullIvl, true
		}
		if !desc {
			if p.Op == doc.OpLt {
				return fieldIvl{blo, e}, true
			}
			return fieldIvl{blo, plusByte(e, 0xFF)}, true
		}
		lo := e
		if p.Op == doc.OpLt {
			lo = plusByte(e, 0xFF)
		}
		if uniqueLast && keyenc.TypeOrder(v.Type) == keyenc.TString {
			lo = blo
		}
		return fieldIvl{lo, bhi}, true
	}
	return fieldIvl{}, false
}

func maxBytes(a, b []byte) []byte {
	if bytes.Compare(a, b) >= 0 {
		return a
	}
	return b
}

func minBytes(a, b []byte) []byte {
	if bytes.Compare(a, b) <= 0 {
		return a
	}
	return b
}

func pointText(vals []bson.RawValue) string {
	if len(vals) == 0 {
		return "[]"
	}
	parts := make([]string, len(vals))
	for i, v := range vals {
		s := relaxed(v)
		parts[i] = "[" + s + ", " + s + "]"
	}
	return strings.Join(parts, ", ")
}
