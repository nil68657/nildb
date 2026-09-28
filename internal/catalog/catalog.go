// Package catalog keeps NilDB's document collections and their indexes.
//
// Records live in the default column family under the keys of
// architecture.md section 3 (layout.CollKey, layout.IndexKey,
// layout.CountKey, layout.SeqKey). Open loads every record into an
// in-memory cache, and every change writes the record first and then
// updates the cache.
//
// Ids. Collections and indexes draw their ids from one space: a new id is
// the larger of the two counters s/coll and s/idx, and only the counter of
// the kind that took it advances. No id is ever reused, and a coll_id never
// equals an idx_id, so the single LiveSet that store's idFilter reads for
// doc, idx, geo and col never mistakes one kind for the other.
//
// LiveSet. A new id is published to store.SetLiveSet before the batch that
// records it commits, so it is live before any write under it. A dropped
// id leaves the set only in CommitDrop, which runs after the drop batch has
// committed (architecture.md section 5).
//
// Locking. Collection locks live in store (store.LockColl, keyed by
// coll_id); use CollLock to build the key. DDL (AddIndex, SetIndexState,
// DropIndex, DropCollection and the drop's commit) expects the caller to
// hold the collection's exclusive lock; the catalog's own mutexes only
// protect its cache.
//
// Namespaces are single strings such as "shop.orders" or "orders". The
// catalog stores them as given; DOC.* commands take one namespace string
// and never split it into database and collection, except that
// DOC.COLLECTIONS lists by prefix.
package catalog

import (
	"encoding/binary"
	"errors"
	"math"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"unicode/utf8"

	"github.com/nil68657/nildb/internal/doc"
	"github.com/nil68657/nildb/internal/layout"
	"github.com/nil68657/nildb/internal/store"
)

// MongoDB error codes that catalog errors carry (ErrorCodes values).
const (
	CodeNamespaceNotFound     = 26
	CodeIndexNotFound         = 27
	CodeNamespaceExists       = 48
	CodeCannotCreateIndex     = 67
	CodeInvalidOptions        = 72
	CodeInvalidNamespace      = 73
	CodeIndexOptionsConflict  = 85
	CodeIndexKeySpecsConflict = 86
)

// MaxNamespaceLen is MongoDB's limit on a namespace in bytes.
const MaxNamespaceLen = 255

// Errors returned by the catalog. User-facing ones are *doc.Error, so a
// handler replies resp.Err(err.Error()) and fills writeErrors from
// doc.Code(err) and the Msg field.
var (
	// ErrCollectionDropped is returned by docstore writes that acquired
	// their locks after the collection was dropped. Resolve (or Ensure)
	// the namespace again and retry.
	ErrCollectionDropped = &doc.Error{Code: CodeNamespaceNotFound, Msg: "collection was dropped"}
	// ErrColumnar refuses columnar indexes, which v1 does not maintain.
	ErrColumnar = &doc.Error{Code: CodeCannotCreateIndex, Msg: "columnar indexes are not supported in v1"}
	// ErrIndexExists is returned by AddIndex, together with the existing
	// index, when an index with the same name and specification exists.
	// MongoDB's createIndexes treats that as success.
	ErrIndexExists = errors.New("catalog: an identical index already exists")
	// ErrIDIndex is returned by AddIndex for a key pattern of {_id: 1},
	// the index every collection already has.
	ErrIDIndex = errors.New("catalog: the _id index always exists")
	// ErrIDSpace is returned when the id space is used up.
	ErrIDSpace = &doc.Error{Code: doc.CodeBadValue, Msg: "catalog id space exhausted"}
)

// Collection is one document collection. Its identity (ID, NS, Created)
// never changes; the index list is replaced wholesale on DDL, so a slice
// returned by Indexes is safe to read without a lock and never changes.
type Collection struct {
	ID      uint32
	NS      string
	Created int64 // unix milliseconds

	indexes atomic.Pointer[[]*Index]
	dropped atomic.Bool
}

// Indexes returns the collection's secondary indexes in creation order.
// The _id index is implicit (the doc CF key) and is not listed. The slice
// and the Index values it holds must not be modified.
func (c *Collection) Indexes() []*Index {
	if p := c.indexes.Load(); p != nil {
		return *p
	}
	return nil
}

// Index returns the index called name.
func (c *Collection) Index(name string) (*Index, bool) {
	for _, ix := range c.Indexes() {
		if ix.Name == name {
			return ix, true
		}
	}
	return nil, false
}

// IndexByID returns the index with idx_id id.
func (c *Collection) IndexByID(id uint32) (*Index, bool) {
	for _, ix := range c.Indexes() {
		if ix.ID == id {
			return ix, true
		}
	}
	return nil, false
}

// Dropped reports whether the collection has been dropped. A writer that
// resolved the collection before a drop checks it after taking its locks.
func (c *Collection) Dropped() bool { return c.dropped.Load() }

func (c *Collection) setIndexes(list []*Index) { c.indexes.Store(&list) }

// CollLock returns the store.LockColl key of collection id: shared for
// point writes, exclusive for DDL and filter-driven multi-document writes.
func CollLock(id uint32, exclusive bool) store.LockKey {
	mode := byte('r')
	if exclusive {
		mode = 'w'
	}
	return store.LockKey{Kind: store.LockColl, NS: id, Key: []byte{mode}}
}

// Catalog is the cache of collections and indexes over one store. It is
// safe for concurrent use.
type Catalog struct {
	st *store.Store

	// ddl serialises id allocation, catalog writes and LiveSet updates.
	ddl      sync.Mutex
	nextColl uint32 // guarded by ddl
	nextIdx  uint32 // guarded by ddl
	live     atomic.Pointer[LiveSet]
	hook     func(*LiveSet) // guarded by ddl

	// mu guards the maps for readers.
	mu   sync.RWMutex
	byNS map[string]*Collection
	byID map[uint32]*Collection
}

// Open loads every collection and index record from st, restores the id
// counters and publishes the LiveSet of every recorded id. Index records
// whose collection record is missing are ignored and their ids are not
// live, so compaction reclaims their entries.
func Open(st *store.Store) (*Catalog, error) {
	c := &Catalog{
		st:       st,
		nextColl: 1,
		nextIdx:  1,
		byNS:     make(map[string]*Collection),
		byID:     make(map[uint32]*Collection),
	}
	for _, seq := range []struct {
		name string
		dst  *uint32
	}{{layout.SeqColl, &c.nextColl}, {layout.SeqIdx, &c.nextIdx}} {
		v, ok, err := st.Get(store.CFDefault, layout.SeqKey(seq.name))
		if err != nil {
			return nil, err
		}
		if ok {
			if len(v) != 4 {
				return nil, corrupt("s/%s holds %d bytes", seq.name, len(v))
			}
			*seq.dst = max(binary.BigEndian.Uint32(v), 1)
		}
	}
	if err := c.loadCollections(); err != nil {
		return nil, err
	}
	if err := c.loadIndexes(); err != nil {
		return nil, err
	}
	var ids []uint32
	for id, coll := range c.byID {
		ids = append(ids, id)
		for _, ix := range coll.Indexes() {
			ids = append(ids, ix.ID)
		}
		// Counters must stay above every recorded id, even if a record
		// outlived a lost counter write.
		c.nextColl = max(c.nextColl, id+1)
		for _, ix := range coll.Indexes() {
			c.nextIdx = max(c.nextIdx, ix.ID+1)
		}
	}
	c.ddl.Lock()
	c.publish(newLiveSet(ids))
	c.ddl.Unlock()
	return c, nil
}

func (c *Catalog) loadCollections() error {
	lo := []byte{layout.CollTag}
	it := c.st.Iter(store.CFDefault, lo, []byte{layout.CollTag + 1}, store.IterOpts{FillCache: true})
	defer it.Close()
	for it.SeekToFirst(); it.Valid(); it.Next() {
		ns, ok := layout.SplitCollKey(it.Key())
		if !ok {
			continue
		}
		coll, err := decodeCollection(ns, it.Value())
		if err != nil {
			return err
		}
		if prev, dup := c.byID[coll.ID]; dup {
			return corrupt("collections %q and %q share coll_id %d", prev.NS, ns, coll.ID)
		}
		c.byNS[ns] = coll
		c.byID[coll.ID] = coll
	}
	return it.Err()
}

func (c *Catalog) loadIndexes() error {
	lists := make(map[uint32][]*Index)
	it := c.st.Iter(store.CFDefault, []byte{layout.IndexTag}, []byte{layout.IndexTag + 1}, store.IterOpts{FillCache: true})
	defer it.Close()
	for it.SeekToFirst(); it.Valid(); it.Next() {
		collID, name, ok := layout.SplitIndexKey(it.Key())
		if !ok {
			continue
		}
		if _, known := c.byID[collID]; !known {
			continue
		}
		ix, err := decodeIndex(name, it.Value())
		if err != nil {
			return err
		}
		lists[collID] = append(lists[collID], ix)
	}
	if err := it.Err(); err != nil {
		return err
	}
	for collID, list := range lists {
		slices.SortFunc(list, func(a, b *Index) int {
			switch {
			case a.ID < b.ID:
				return -1
			case a.ID > b.ID:
				return 1
			}
			return 0
		})
		c.byID[collID].setIndexes(list)
	}
	return nil
}

// Resolve returns the live collection called ns.
func (c *Catalog) Resolve(ns string) (*Collection, bool) {
	c.mu.RLock()
	coll, ok := c.byNS[ns]
	c.mu.RUnlock()
	return coll, ok
}

// ByID returns the live collection with coll_id id.
func (c *Catalog) ByID(id uint32) (*Collection, bool) {
	c.mu.RLock()
	coll, ok := c.byID[id]
	c.mu.RUnlock()
	return coll, ok
}

// ResolveNS returns the coll_id of ns. With UniqueIndexes it is the
// catalog side of command.KeyCtx.Catalog.
func (c *Catalog) ResolveNS(ns string) (collID uint32, ok bool) {
	coll, ok := c.Resolve(ns)
	if !ok {
		return 0, false
	}
	return coll.ID, true
}

// UniqueIndexes returns the idx_ids of the unique indexes of collection
// collID, in creation order.
func (c *Catalog) UniqueIndexes(collID uint32) []uint32 {
	coll, ok := c.ByID(collID)
	if !ok {
		return nil
	}
	var out []uint32
	for _, ix := range coll.Indexes() {
		if ix.Unique {
			out = append(out, ix.ID)
		}
	}
	return out
}

// List returns the namespaces that start with prefix, sorted bytewise.
// DOC.COLLECTIONS db passes db + ".".
func (c *Catalog) List(prefix string) []string {
	c.mu.RLock()
	out := make([]string, 0, len(c.byNS))
	for ns := range c.byNS {
		if strings.HasPrefix(ns, prefix) {
			out = append(out, ns)
		}
	}
	c.mu.RUnlock()
	sort.Strings(out)
	return out
}

// Collections returns every live collection, sorted by namespace.
func (c *Catalog) Collections() []*Collection {
	c.mu.RLock()
	out := make([]*Collection, 0, len(c.byNS))
	for _, coll := range c.byNS {
		out = append(out, coll)
	}
	c.mu.RUnlock()
	slices.SortFunc(out, func(a, b *Collection) int { return strings.Compare(a.NS, b.NS) })
	return out
}

// ValidateNS checks a namespace string: non-empty, valid UTF-8, no NUL
// byte, at most MaxNamespaceLen bytes, and not starting or ending with '.'.
func ValidateNS(ns string) error {
	if ns == "" || len(ns) > MaxNamespaceLen || !utf8.ValidString(ns) ||
		strings.IndexByte(ns, 0) >= 0 || ns[0] == '.' || ns[len(ns)-1] == '.' {
		return &doc.Error{Code: CodeInvalidNamespace, Msg: "Invalid namespace specified '" + ns + "'"}
	}
	return nil
}

// Create records a new collection. It fails with code 48 when ns exists.
func (c *Catalog) Create(ns string) (*Collection, error) {
	coll, created, err := c.ensure(ns)
	if err != nil {
		return nil, err
	}
	if !created {
		return nil, &doc.Error{Code: CodeNamespaceExists, Msg: "Collection already exists. NS: " + ns}
	}
	return coll, nil
}

// Ensure returns the collection called ns, creating it when it does not
// exist, which is what an insert into a new namespace does. created
// reports whether this call created it.
func (c *Catalog) Ensure(ns string) (coll *Collection, created bool, err error) {
	if coll, ok := c.Resolve(ns); ok {
		return coll, false, nil
	}
	return c.ensure(ns)
}

func (c *Catalog) ensure(ns string) (*Collection, bool, error) {
	if err := ValidateNS(ns); err != nil {
		return nil, false, err
	}
	c.ddl.Lock()
	defer c.ddl.Unlock()
	if coll, ok := c.Resolve(ns); ok {
		return coll, false, nil
	}
	id, err := c.allocID()
	if err != nil {
		return nil, false, err
	}
	coll := &Collection{ID: id, NS: ns, Created: c.st.Clock()().UnixMilli()}
	// Publish before the batch that records the id commits.
	c.publish(c.live.Load().with(id))
	txn := c.st.Begin()
	txn.Put(store.CFDefault, layout.CollKey(ns), encodeCollection(coll))
	txn.Put(store.CFDefault, layout.SeqKey(layout.SeqColl), binary.BigEndian.AppendUint32(nil, id+1))
	if err := txn.Commit(); err != nil {
		c.publish(c.live.Load().without(id))
		return nil, false, err
	}
	c.nextColl = id + 1
	c.mu.Lock()
	c.byNS[ns] = coll
	c.byID[id] = coll
	c.mu.Unlock()
	return coll, true, nil
}

// allocID returns the next unused id of the shared id space. The caller
// holds ddl and advances nextColl or nextIdx after its record commits.
func (c *Catalog) allocID() (uint32, error) {
	id := max(c.nextColl, c.nextIdx)
	if id == math.MaxUint32 {
		// IDBounds(MaxUint32) has no upper bound, so its keyspace could
		// never be range-deleted.
		return 0, ErrIDSpace
	}
	return id, nil
}

// Count returns the document count of coll from n/<coll_id> on live state.
func (c *Catalog) Count(coll *Collection) (int64, error) {
	return CountAt(c.st, coll)
}

// CountAt returns the document count of coll as r sees it; pass
// st.At(lease) to count inside a snapshot.
func CountAt(r store.Reader, coll *Collection) (int64, error) {
	v, ok, err := r.Get(store.CFDefault, layout.CountKey(coll.ID))
	if err != nil || !ok {
		return 0, err
	}
	return layout.DecodeCount(v)
}

// NextVersionSeed returns s/version + 1, the lowest Redis collection
// version a restart may issue. store.Open seeds store.VersionGen from the
// same record and from the version map, and Close saves it, so NilDB does
// not need to call this; it exists for tools that inspect a store.
func (c *Catalog) NextVersionSeed() (uint64, error) {
	v, ok, err := c.st.Get(store.CFDefault, layout.SeqKey(layout.SeqVersion))
	if err != nil || !ok {
		return 1, err
	}
	if len(v) != 8 {
		return 0, corrupt("s/version holds %d bytes", len(v))
	}
	return binary.BigEndian.Uint64(v) + 1, nil
}

// SaveVersion writes v to s/version unless the stored value is already
// larger, so it can never move the seed backwards. store.Close saves the
// generator itself; this is for tools.
func (c *Catalog) SaveVersion(v uint64) error {
	c.ddl.Lock()
	defer c.ddl.Unlock()
	next, err := c.NextVersionSeed()
	if err != nil {
		return err
	}
	if next > v {
		return nil
	}
	txn := c.st.Begin()
	txn.Put(store.CFDefault, layout.SeqKey(layout.SeqVersion), binary.BigEndian.AppendUint64(nil, v))
	return txn.Commit()
}
