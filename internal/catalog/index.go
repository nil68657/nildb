package catalog

import (
	"encoding/binary"
	"fmt"
	"strings"
	"sync/atomic"

	"github.com/nil68657/nildb/internal/doc"
	"github.com/nil68657/nildb/internal/geo"
	"github.com/nil68657/nildb/internal/layout"
	"github.com/nil68657/nildb/internal/store"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// IndexKind is the kind of a secondary index.
type IndexKind uint8

// Index kinds. Columnar is recorded in the layout but refused by AddIndex
// in v1.
const (
	Btree IndexKind = iota
	TwoDSphere
	Columnar
)

var kindNames = [...]string{"btree", "2dsphere", "columnar"}

// String returns the kind as the catalog record spells it.
func (k IndexKind) String() string {
	if int(k) < len(kindNames) {
		return kindNames[k]
	}
	return fmt.Sprintf("IndexKind(%d)", uint8(k))
}

// ParseIndexKind parses "btree", "2dsphere" or "columnar".
func ParseIndexKind(s string) (IndexKind, bool) {
	for i, n := range kindNames {
		if n == s {
			return IndexKind(i), true
		}
	}
	return 0, false
}

// IndexState is an index's build state. In v1 every build is synchronous,
// so a Building index is visible only inside DOC.CREATEINDEX or after a
// crash in the middle of a build (docstore.ResumeBuilds redoes it).
type IndexState uint8

// Index states.
const (
	Building IndexState = iota
	Ready
)

// String returns "building" or "ready".
func (s IndexState) String() string {
	if s == Ready {
		return "ready"
	}
	return "building"
}

// Key is one field of an index key pattern. Its type is the anonymous
// struct build-plan.md fixes, so []Key converts to doc.SortSpec directly:
// doc.SortSpec(ix.Keys).
type Key = struct {
	Field string
	Desc  bool
}

// NoExpire is the ExpireAfter value of an index without a TTL.
const NoExpire = -1

// MaxIndexes is MongoDB's limit of 64 indexes per collection; the implicit
// _id index takes one slot.
const MaxIndexes = 64

// MaxKeyFields is MongoDB's limit on the fields of one compound index.
const MaxKeyFields = 32

// Index is one secondary index record. Values are immutable once
// published: SetIndexState installs a new *Index, so compare indexes by
// ID. Only the multikey flag changes in place, and only from false to
// true.
type Index struct {
	ID     uint32
	Name   string
	Kind   IndexKind
	Keys   []Key // btree: one or more fields; 2dsphere and columnar: one field
	Unique bool
	Sparse bool
	// ExpireAfter is expireAfterSeconds, NoExpire (-1) for none. The zero
	// value means a TTL of 0 s, so set NoExpire explicitly. v1 records
	// the value and runs no sweeper.
	ExpireAfter int64
	S2          geo.Params // 2dsphere only; frozen at creation
	State       IndexState

	multikey *atomic.Bool // shared by every copy of the record
}

// Multikey reports whether some document has put more than one entry in a
// btree index for one field, which is when a planner must not intersect
// two range predicates on that field. It never goes back to false.
func (ix *Index) Multikey() bool { return ix.multikey != nil && ix.multikey.Load() }

// Fields returns the indexed field paths in key order.
func (ix *Index) Fields() []string {
	out := make([]string, len(ix.Keys))
	for i, k := range ix.Keys {
		out[i] = k.Field
	}
	return out
}

// KeyDoc returns the key pattern as MongoDB shows it: {f: 1, g: -1},
// {loc: "2dsphere"} or {f: "columnar"}.
func (ix *Index) KeyDoc() bson.Raw {
	d := make(bson.D, 0, len(ix.Keys))
	for _, k := range ix.Keys {
		var v any
		switch {
		case ix.Kind != Btree:
			v = ix.Kind.String()
		case k.Desc:
			v = int32(-1)
		default:
			v = int32(1)
		}
		d = append(d, bson.E{Key: k.Field, Value: v})
	}
	raw, err := bson.Marshal(d)
	if err != nil {
		panic(err) // strings and int32s always marshal
	}
	return raw
}

// sameSpec reports whether two records describe the same index apart from
// id, state and name.
func (ix *Index) sameSpec(o *Index) bool {
	return ix.sameKeys(o) && ix.Unique == o.Unique && ix.Sparse == o.Sparse &&
		ix.ExpireAfter == o.ExpireAfter && (ix.Kind != TwoDSphere || ix.S2 == o.S2)
}

func (ix *Index) sameKeys(o *Index) bool {
	if ix.Kind != o.Kind || len(ix.Keys) != len(o.Keys) {
		return false
	}
	for i := range ix.Keys {
		if ix.Keys[i] != o.Keys[i] {
			return false
		}
	}
	return true
}

func (ix *Index) clone() *Index {
	cp := *ix
	cp.Keys = append([]Key(nil), ix.Keys...)
	return &cp
}

// DefaultIndexName returns MongoDB's generated name for a key pattern:
// each field and its value joined by '_', such as "a_1_b_-1" or
// "loc_2dsphere".
func DefaultIndexName(kind IndexKind, keys []Key) string {
	var b strings.Builder
	for i, k := range keys {
		if i > 0 {
			b.WriteByte('_')
		}
		b.WriteString(k.Field)
		b.WriteByte('_')
		switch {
		case kind != Btree:
			b.WriteString(kind.String())
		case k.Desc:
			b.WriteString("-1")
		default:
			b.WriteString("1")
		}
	}
	return b.String()
}

func badIndex(format string, a ...any) error {
	return &doc.Error{Code: CodeCannotCreateIndex, Msg: fmt.Sprintf(format, a...)}
}

// normalise validates spec and fills its defaults (name, S2 params).
func normalise(spec Index) (*Index, error) {
	ix := spec.clone()
	ix.ID, ix.State, ix.multikey = 0, Building, new(atomic.Bool)
	switch ix.Kind {
	case Btree, TwoDSphere:
	case Columnar:
		return nil, ErrColumnar
	default:
		return nil, badIndex("unknown index kind %d", ix.Kind)
	}
	if len(ix.Keys) == 0 {
		return nil, badIndex("Index keys cannot be empty.")
	}
	if len(ix.Keys) > MaxKeyFields {
		return nil, badIndex("Index key pattern has %d fields, the limit is %d", len(ix.Keys), MaxKeyFields)
	}
	seen := make(map[string]bool, len(ix.Keys))
	for _, k := range ix.Keys {
		if k.Field == "" || strings.HasPrefix(k.Field, "$") || strings.Contains(k.Field, "..") ||
			strings.HasPrefix(k.Field, ".") || strings.HasSuffix(k.Field, ".") || strings.IndexByte(k.Field, 0) >= 0 {
			return nil, badIndex("Index key field name is invalid: '%s'", k.Field)
		}
		if seen[k.Field] {
			return nil, badIndex("Index key pattern repeats the field '%s'", k.Field)
		}
		seen[k.Field] = true
	}
	if ix.Kind == TwoDSphere {
		if len(ix.Keys) != 1 {
			return nil, badIndex("compound 2dsphere indexes are not supported in v1")
		}
		if ix.Keys[0].Desc {
			return nil, badIndex("a 2dsphere index field has no direction")
		}
		if ix.Unique {
			return nil, badIndex("unique 2dsphere indexes are not supported")
		}
		if ix.S2 == (geo.Params{}) {
			ix.S2 = geo.DefaultParams()
		}
		p := ix.S2
		if p.Coarsest < 0 || p.Finest > 30 || p.Coarsest > p.Finest || p.MaxCells < 1 {
			return nil, badIndex("invalid 2dsphere parameters: coarsest %d, finest %d, maxCells %d", p.Coarsest, p.Finest, p.MaxCells)
		}
	} else {
		ix.S2 = geo.Params{}
	}
	if ix.ExpireAfter < NoExpire {
		return nil, badIndex("expireAfterSeconds must be a non-negative number")
	}
	if ix.ExpireAfter != NoExpire && (ix.Kind != Btree || len(ix.Keys) != 1) {
		return nil, badIndex("TTL indexes are single-field indexes, compound indexes do not support TTL")
	}
	if ix.Name == "" {
		ix.Name = DefaultIndexName(ix.Kind, ix.Keys)
	}
	if ix.Name == "_id_" {
		return nil, badIndex("The index name '_id_' is reserved for the _id index")
	}
	if strings.IndexByte(ix.Name, 0) >= 0 {
		return nil, badIndex("index names cannot contain NUL bytes")
	}
	return ix, nil
}

// AddIndex records a new index on coll with State Building and adds it to
// the collection's list, so every later write maintains it. The caller
// holds coll's exclusive lock, then builds the index (docstore.Build) and
// marks it Ready.
//
// When an index of the same name and specification exists, AddIndex
// returns it with ErrIndexExists. It refuses a columnar index
// (ErrColumnar), {_id: 1} (ErrIDIndex), a name clash with a different
// specification (code 86), the same key pattern under another name (code
// 85) and a 64th index (code 67).
func (c *Catalog) AddIndex(coll *Collection, spec Index) (*Index, error) {
	ix, err := normalise(spec)
	if err != nil {
		return nil, err
	}
	if ix.Kind == Btree && len(ix.Keys) == 1 && ix.Keys[0].Field == "_id" && !ix.Keys[0].Desc &&
		!ix.Sparse && ix.ExpireAfter == NoExpire {
		return nil, ErrIDIndex
	}
	c.ddl.Lock()
	defer c.ddl.Unlock()
	if err := c.checkLive(coll); err != nil {
		return nil, err
	}
	list := coll.Indexes()
	for _, o := range list {
		switch {
		case o.Name == ix.Name && o.sameSpec(ix):
			return o, ErrIndexExists
		case o.Name == ix.Name:
			return nil, &doc.Error{Code: CodeIndexKeySpecsConflict,
				Msg: "An existing index has the same name as the requested index. Requested index name: " + ix.Name}
		case o.sameKeys(ix) && o.Unique == ix.Unique && o.Sparse == ix.Sparse:
			return nil, &doc.Error{Code: CodeIndexOptionsConflict, Msg: "Index already exists with a different name: " + o.Name}
		}
	}
	if len(list)+1 >= MaxIndexes {
		return nil, badIndex("add index fails, too many indexes for %s key:%s", coll.NS, ix.KeyDoc().String())
	}
	id, err := c.allocID()
	if err != nil {
		return nil, err
	}
	ix.ID = id
	c.publish(c.live.Load().with(id))
	txn := c.st.Begin()
	txn.Put(store.CFDefault, layout.IndexKey(coll.ID, ix.Name), encodeIndex(ix))
	txn.Put(store.CFDefault, layout.SeqKey(layout.SeqIdx), binary.BigEndian.AppendUint32(nil, id+1))
	if err := txn.Commit(); err != nil {
		c.publish(c.live.Load().without(id))
		return nil, err
	}
	c.nextIdx = id + 1
	coll.setIndexes(append(append([]*Index(nil), list...), ix))
	return ix, nil
}

// checkLive fails unless coll is the registered collection of its id. The
// caller holds ddl.
func (c *Catalog) checkLive(coll *Collection) error {
	if cur, ok := c.ByID(coll.ID); !ok || cur != coll || coll.Dropped() {
		return ErrCollectionDropped
	}
	return nil
}

// SetIndexState writes ix's record with state s and installs the updated
// record in coll's list. It returns the new *Index; ix itself is not
// changed. The caller holds coll's exclusive lock.
func (c *Catalog) SetIndexState(coll *Collection, ix *Index, s IndexState) (*Index, error) {
	c.ddl.Lock()
	defer c.ddl.Unlock()
	if err := c.checkLive(coll); err != nil {
		return nil, err
	}
	list := coll.Indexes()
	pos := -1
	for i, o := range list {
		if o.ID == ix.ID {
			pos = i
		}
	}
	if pos < 0 {
		return nil, &doc.Error{Code: CodeIndexNotFound, Msg: "index not found with name [" + ix.Name + "]"}
	}
	nix := list[pos].clone()
	nix.State = s
	txn := c.st.Begin()
	txn.Put(store.CFDefault, layout.IndexKey(coll.ID, nix.Name), encodeIndex(nix))
	if err := txn.Commit(); err != nil {
		return nil, err
	}
	nl := append([]*Index(nil), list...)
	nl[pos] = nix
	coll.setIndexes(nl)
	return nix, nil
}

// MarkMultikey persists that ix is multikey, in a catalog write of its own
// committed before it returns, then sets the in-memory flag. A writer
// calls it before committing its first multikey document, so the flag is
// on disk whenever such a document is, even if the writer's own Txn is
// later discarded (an early flag is only conservative). It is a no-op once
// the flag is set, so it costs one commit per index lifetime. The caller
// holds coll's lock, shared or exclusive, so no DDL rewrites the record
// concurrently.
func (c *Catalog) MarkMultikey(coll *Collection, ix *Index) error {
	if ix.multikey == nil || ix.Multikey() || ix.Kind != Btree {
		return nil
	}
	c.ddl.Lock()
	defer c.ddl.Unlock()
	if ix.Multikey() {
		return nil
	}
	// Encode the collection's current record, not ix, which may be an
	// older copy with a stale state.
	cur, ok := coll.IndexByID(ix.ID)
	if !ok {
		return nil
	}
	cp := cur.clone()
	cp.multikey = new(atomic.Bool)
	cp.multikey.Store(true)
	txn := c.st.Begin()
	txn.Put(store.CFDefault, layout.IndexKey(coll.ID, ix.Name), encodeIndex(cp))
	if err := txn.Commit(); err != nil {
		return err
	}
	ix.multikey.Store(true)
	return nil
}

// SetMultikey sets ix's in-memory multikey flag without writing; the next
// record write (SetIndexState after a build) persists it.
func (c *Catalog) SetMultikey(ix *Index) {
	if ix.Kind == Btree && ix.multikey != nil {
		ix.multikey.Store(true)
	}
}

// Drop describes a planned drop: the collection (or the owner of the
// dropped index), the indexes whose entries go, and the catalog records
// the drop batch deletes. docstore turns it into one store.DeleteRanges
// call, then calls CommitDrop, or AbortDrop if the batch failed.
type Drop struct {
	Coll    *Collection
	Whole   bool     // the collection itself is dropped
	Indexes []*Index // indexes whose idx/geo/col keyspaces go
	Records []store.PointDelete
}

// IDs returns the ids the drop removes from the LiveSet: the coll_id for a
// collection drop, then every dropped idx_id.
func (d Drop) IDs() []uint32 {
	var ids []uint32
	if d.Whole {
		ids = append(ids, d.Coll.ID)
	}
	for _, ix := range d.Indexes {
		ids = append(ids, ix.ID)
	}
	return ids
}

// DropCollection hides coll from Resolve, marks it dropped and returns the
// drop plan: coll's record, its count and every index record, plus its
// indexes. Nothing is written and the ids stay live until CommitDrop. The
// caller holds coll's exclusive lock.
func (c *Catalog) DropCollection(coll *Collection) (Drop, error) {
	c.ddl.Lock()
	defer c.ddl.Unlock()
	if err := c.checkLive(coll); err != nil {
		return Drop{}, &doc.Error{Code: CodeNamespaceNotFound, Msg: "ns not found"}
	}
	d := Drop{Coll: coll, Whole: true, Indexes: coll.Indexes()}
	d.Records = append(d.Records,
		store.PointDelete{CF: store.CFDefault, Key: layout.CollKey(coll.NS)},
		store.PointDelete{CF: store.CFDefault, Key: layout.CountKey(coll.ID)})
	for _, ix := range d.Indexes {
		d.Records = append(d.Records, store.PointDelete{CF: store.CFDefault, Key: layout.IndexKey(coll.ID, ix.Name)})
	}
	coll.dropped.Store(true)
	c.mu.Lock()
	delete(c.byNS, coll.NS)
	delete(c.byID, coll.ID)
	c.mu.Unlock()
	return d, nil
}

// DropIndex removes the index called name from coll's list and returns it
// with the drop plan. Nothing is written and its id stays live until
// CommitDrop. "_id_" fails with code 72 and an unknown name with code 27.
// The caller holds coll's exclusive lock.
func (c *Catalog) DropIndex(coll *Collection, name string) (*Index, Drop, error) {
	if name == "_id_" {
		return nil, Drop{}, &doc.Error{Code: CodeInvalidOptions, Msg: "cannot drop _id index"}
	}
	c.ddl.Lock()
	defer c.ddl.Unlock()
	if err := c.checkLive(coll); err != nil {
		return nil, Drop{}, err
	}
	list := coll.Indexes()
	for i, ix := range list {
		if ix.Name != name {
			continue
		}
		nl := append(append([]*Index(nil), list[:i]...), list[i+1:]...)
		coll.setIndexes(nl)
		return ix, Drop{
			Coll:    coll,
			Indexes: []*Index{ix},
			Records: []store.PointDelete{{CF: store.CFDefault, Key: layout.IndexKey(coll.ID, ix.Name)}},
		}, nil
	}
	return nil, Drop{}, &doc.Error{Code: CodeIndexNotFound, Msg: "index not found with name [" + name + "]"}
}

// CommitDrop finishes a drop whose batch has committed: it removes the
// drop's ids from the LiveSet.
func (c *Catalog) CommitDrop(d Drop) {
	c.Unpublish(d.IDs()...)
}

// AbortDrop undoes the in-memory part of a drop whose batch failed: the
// collection becomes resolvable again, or the index returns to its
// collection's list. If another collection took the namespace in the
// meantime, the old one stays hidden (its ids stay live, so nothing is
// lost; a restart shows it again).
func (c *Catalog) AbortDrop(d Drop) {
	c.ddl.Lock()
	defer c.ddl.Unlock()
	if d.Whole {
		c.mu.Lock()
		if _, taken := c.byNS[d.Coll.NS]; !taken {
			c.byNS[d.Coll.NS] = d.Coll
			c.byID[d.Coll.ID] = d.Coll
			d.Coll.dropped.Store(false)
		}
		c.mu.Unlock()
		return
	}
	list := append([]*Index(nil), d.Coll.Indexes()...)
	for _, ix := range d.Indexes {
		pos := len(list)
		for i, o := range list {
			if o.ID > ix.ID {
				pos = i
				break
			}
		}
		list = append(list[:pos], append([]*Index{ix}, list[pos:]...)...)
	}
	d.Coll.setIndexes(list)
}
