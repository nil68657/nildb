package redis

import (
	"fmt"
	"math/rand/v2"

	"github.com/nil68657/nildb/internal/command"
	"github.com/nil68657/nildb/internal/layout"
	"github.com/nil68657/nildb/internal/resp"
	"github.com/nil68657/nildb/internal/store"
)

// maxStringLen is proto-max-bulk-len at its Redis default (512 MiB), the
// longest string APPEND and SETRANGE may build.
const maxStringLen = 512 << 20

// Replies several files share.
var (
	zero             = resp.Int(0)
	one              = resp.Int(1)
	emptyBulk        = resp.Str("")
	errOffsetRange   = resp.Err("ERR offset is out of range")
	errStringTooLong = resp.Err("ERR string exceeds maximum allowed size (proto-max-bulk-len)")
	errSameObject    = resp.Err("ERR source and destination objects are the same")
)

// storeErr is the reply for a storage failure.
func storeErr(err error) resp.Reply { return resp.Err("ERR " + err.Error()) }

// errOutOfRange is getRangeLongFromObjectOrReply's reply for a parsed
// integer outside [min, max].
func errOutOfRange(min, max int64) resp.Reply {
	return resp.Err(fmt.Sprintf("ERR value is out of range, value must between %d and %d", min, max))
}

// parseIntRange parses an integer argument and checks it against
// [min, max], with the replies Redis's getRangeLongFromObjectOrReply
// sends.
func parseIntRange(b []byte, min, max int64) (int64, resp.Reply) {
	n, ok := resp.ParseInt(b)
	if !ok {
		return 0, resp.ErrNotInteger
	}
	if n < min || n > max {
		return 0, errOutOfRange(min, max)
	}
	return n, nil
}

// entry is the metadata stored under one key, as a command read it.
type entry struct {
	m     layout.Meta // decoded entry; zero unless found
	found bool        // a meta entry exists, expired or not
	live  bool        // found and not expired at the command's clock
}

// readEntry reads the metadata of key in db through r. An expired entry
// is returned with live false; nothing is written.
func readEntry(r store.Reader, db uint8, key []byte, nowMS int64) (entry, error) {
	v, ok, err := r.Get(store.CFMeta, layout.MetaKey(nil, db, key))
	if err != nil || !ok {
		return entry{}, err
	}
	m, err := layout.DecodeMeta(v)
	if err != nil {
		return entry{}, err
	}
	return entry{m: m, found: true, live: !m.Expired(nowMS)}, nil
}

// getEntry reads key in the command's database through c.Reader().
func getEntry(c *command.Ctx, key []byte) (entry, error) {
	return readEntry(c.Reader(), c.DB, key, c.NowMS())
}

// loadMeta returns key's metadata with lazy expiry applied: an expired
// key reads as absent (ok false) and the read writes nothing.
func loadMeta(c *command.Ctx, key []byte) (layout.Meta, bool, error) {
	e, err := getEntry(c, key)
	if err != nil || !e.live {
		return layout.Meta{}, false, err
	}
	return e.m, true, nil
}

// requireType returns WRONGTYPE when the key exists with a type other
// than t, and nil otherwise.
func requireType(m layout.Meta, ok bool, t layout.Type) resp.Reply {
	if ok && m.Type != t {
		return resp.ErrWrongType
	}
	return nil
}

// newVersion issues a collection version from the store's generator.
func newVersion(c *command.Ctx) uint64 { return c.Store.VersionGen().Next(c.Now) }

// writeMeta puts m under key in db. For a collection it also puts the
// version-map entry (expiry, db, key) that the sub-key compaction filter
// reads, unless prev shows the same live version with the same expiry.
// When prev holds a collection whose version m replaces, its version-map
// entry is deleted in the same batch, so the filter reclaims its elements.
func writeMeta(c *command.Ctx, db uint8, key []byte, m layout.Meta, prev entry) {
	c.Txn.Put(store.CFMeta, layout.MetaKey(nil, db, key), m.Encode(nil))
	if m.Type.IsCollection() && !(prev.live && prev.m.Type == m.Type &&
		prev.m.Version == m.Version && prev.m.ExpireMS == m.ExpireMS) {
		c.Txn.Put(store.CFMeta, layout.VersionKey(nil, m.Version),
			layout.VersionEntry{ExpireMS: m.ExpireMS, DB: db, Key: key}.Encode(nil))
	}
	if prev.found && prev.m.Type.IsCollection() && (!m.Type.IsCollection() || prev.m.Version != m.Version) {
		c.Txn.Delete(store.CFMeta, layout.VersionKey(nil, prev.m.Version))
	}
}

// deleteKey deletes the metadata of key in db and, for a collection, its
// version-map entry. The elements stay until the sub-key compaction
// filter drops them: readers only reach elements through the metadata.
func deleteKey(c *command.Ctx, db uint8, key []byte, e entry) {
	c.Txn.Delete(store.CFMeta, layout.MetaKey(nil, db, key))
	if e.found && e.m.Type.IsCollection() {
		c.Txn.Delete(store.CFMeta, layout.VersionKey(nil, e.m.Version))
	}
}

// coll is a hash, list, set or sorted set that a command reads or
// changes.
type coll struct {
	key    []byte
	prev   entry       // what was stored under key when the command read it
	m      layout.Meta // current metadata; a new collection has a fresh version
	exists bool        // the key held a live collection of this type
	dirty  bool        // m changed and save must write it
}

// openColl reads key as a collection of type t. A live key of another
// type replies WRONGTYPE. An absent or expired key gives exists false;
// with create set it also gets a fresh version, so the command can write
// elements under it right away.
func openColl(c *command.Ctx, key []byte, t layout.Type, create bool) (*coll, resp.Reply) {
	e, err := getEntry(c, key)
	if err != nil {
		return nil, storeErr(err)
	}
	k := &coll{key: key, prev: e}
	if e.live {
		if e.m.Type != t {
			return nil, resp.ErrWrongType
		}
		k.m, k.exists = e.m, true
		return k, nil
	}
	if create {
		k.m = layout.Meta{Type: t, Version: newVersion(c)}
		if t == layout.TList {
			k.m.Head, k.m.Tail = layout.ListStart, layout.ListStart
		}
	}
	return k, nil
}

// grow adds n (possibly negative) to the element count.
func (k *coll) grow(n int64) {
	if n != 0 {
		k.m.Size += n
		k.dirty = true
	}
}

// save writes the metadata after the command changed the collection. An
// emptied collection loses its key, as Redis deletes empty aggregates; a
// new collection that received nothing writes nothing.
func (k *coll) save(c *command.Ctx) {
	switch {
	case k.m.Size <= 0:
		if k.exists {
			deleteKey(c, c.DB, k.key, k.prev)
		}
	case k.dirty:
		writeMeta(c, c.DB, k.key, k.m, k.prev)
	}
}

// iterColl opens an OLTP iterator over every key one collection version
// has in cf (store.CFSub or store.CFZScore). Forward scans from the lower
// bound stay in prefix mode. Callers that seek backwards or to random
// places set totalOrder: SeekForPrev and SeekToLast would otherwise
// consult the prefix bloom filter of the upper bound, which is the next
// version's prefix.
func iterColl(r store.Reader, cf store.CF, version uint64, totalOrder bool) store.Iterator {
	lo, hi := layout.VersionBounds(version)
	return r.Iter(cf, lo, hi, store.IterOpts{FillCache: true, TotalOrderSeek: totalOrder})
}

// randomSeek positions it on a pseudo-random entry of its range and
// reports false when the range is empty. It reads the first and last
// keys, builds a key that shares their common prefix and continues with
// random bytes (the first bounded by the two keys), and seeks to it,
// wrapping to the first entry past the end. Entries that follow a wide
// gap in key space come up more often, so the choice is not uniform;
// RANDOMKEY, SPOP, SRANDMEMBER, HRANDFIELD and ZRANDMEMBER document that.
func randomSeek(it store.Iterator) bool {
	p, ok := newPicker(it)
	return ok && p.pick()
}

// picker repeats randomSeek's choice on one iterator without re-reading
// the first and last keys.
type picker struct {
	it          store.Iterator
	first, last []byte
}

func newPicker(it store.Iterator) (*picker, bool) {
	it.SeekToFirst()
	if !it.Valid() {
		return nil, false
	}
	p := &picker{it: it, first: clone(it.Key())}
	it.SeekToLast()
	if !it.Valid() {
		return nil, false
	}
	p.last = clone(it.Key())
	return p, true
}

// pick positions the iterator on a pseudo-random entry.
func (p *picker) pick() bool {
	p.it.Seek(randomKeyBetween(p.first, p.last))
	if !p.it.Valid() {
		p.it.SeekToFirst()
	}
	return p.it.Valid()
}

// elem is one element of a hash, set or sorted set as sub stores it: the
// field or member, and the value (hash value, encoded score, or empty).
type elem struct{ sub, val []byte }

// readElems returns every element of a collection version in key order.
// It fails once more than limit elements have been read (limit <= 0 means
// no limit), so set operations stay inside nildb.setop-max-members.
func readElems(r store.Reader, version uint64, limit int) ([]elem, error) {
	it := iterColl(r, store.CFSub, version, false)
	defer it.Close()
	var out []elem
	for it.SeekToFirst(); it.Valid(); it.Next() {
		if limit > 0 && len(out) >= limit {
			return nil, errTooManyMembers(limit)
		}
		out = append(out, elem{sub: clone(it.Key()[8:]), val: clone(it.Value())})
	}
	return out, it.Err()
}

// errTooManyMembers is the error of a set operation that would hold more
// members than nildb.setop-max-members allows.
func errTooManyMembers(limit int) error {
	return fmt.Errorf("set operation exceeds nildb.setop-max-members (%d)", limit)
}

// sampleLoadAll is the collection size up to which sample loads every
// element and draws uniformly; larger collections are sampled by random
// seeks, which are not uniform.
const sampleLoadAll = 1 << 16

// sample draws count elements at random from a collection of about size
// elements. With unique set no element repeats, so at most size come
// back; otherwise exactly count come back and repeats are allowed.
func sample(r store.Reader, version uint64, size, count int64, unique bool) ([]elem, error) {
	if count <= 0 || size <= 0 {
		return nil, nil
	}
	if (unique && count >= size) || size <= sampleLoadAll {
		all, err := readElems(r, version, 0)
		if err != nil || len(all) == 0 {
			return nil, err
		}
		n := int64(len(all))
		if unique {
			count = min(count, n)
			for i := range count {
				j := i + rand.Int64N(n-i)
				all[i], all[j] = all[j], all[i]
			}
			return all[:count], nil
		}
		out := make([]elem, count)
		for i := range out {
			out[i] = all[rand.Int64N(n)]
		}
		return out, nil
	}
	it := iterColl(r, store.CFSub, version, true)
	defer it.Close()
	p, ok := newPicker(it)
	if !ok {
		return nil, it.Err()
	}
	out := make([]elem, 0, count)
	seen := map[string]struct{}{}
	add := func() {
		if unique {
			if _, dup := seen[string(it.Key()[8:])]; dup {
				return
			}
			seen[string(it.Key()[8:])] = struct{}{}
		}
		out = append(out, elem{sub: clone(it.Key()[8:]), val: clone(it.Value())})
	}
	for tries := int64(0); int64(len(out)) < count && tries < count*10+100 && p.pick(); tries++ {
		add()
	}
	// Unique draws that kept hitting seen elements finish with a walk
	// forward from a random position, wrapping once.
	if unique && int64(len(out)) < count && p.pick() {
		for ; it.Valid() && int64(len(out)) < count; it.Next() {
			add()
		}
		for it.SeekToFirst(); it.Valid() && int64(len(out)) < count; it.Next() {
			add()
		}
	}
	return out, it.Err()
}

// subValues reads the value of each sub of a collection version with one
// batched read; a nil entry means absent.
func subValues(r store.Reader, version uint64, subs [][]byte) ([][]byte, error) {
	keys := make([][]byte, len(subs))
	for i, s := range subs {
		keys[i] = layout.SubKey(nil, version, s)
	}
	return r.MultiGet(store.CFSub, keys)
}

// distinct returns subs without repeats, keeping first occurrences.
func distinct(subs [][]byte) [][]byte {
	seen := make(map[string]struct{}, len(subs))
	out := make([][]byte, 0, len(subs))
	for _, s := range subs {
		if _, dup := seen[string(s)]; !dup {
			seen[string(s)] = struct{}{}
			out = append(out, s)
		}
	}
	return out
}

// randomKeyBetween returns a random key that starts with the common
// prefix of a and b (a <= b) and whose next byte lies between theirs.
func randomKeyBetween(a, b []byte) []byte {
	n := 0
	for n < len(a) && n < len(b) && a[n] == b[n] {
		n++
	}
	if n == len(b) {
		return clone(a)
	}
	lo, hi := 0, int(b[n])
	if n < len(a) {
		lo = int(a[n])
	}
	out := make([]byte, n, n+8)
	copy(out, a[:n])
	out = append(out, byte(lo+rand.IntN(hi-lo+1)))
	for range 7 {
		out = append(out, byte(rand.IntN(256)))
	}
	return out
}

// clone returns a Go-owned copy of b, non-nil even when b is empty, so
// an empty value stays distinguishable from a missing one.
func clone(b []byte) []byte {
	out := make([]byte, len(b))
	copy(out, b)
	return out
}

// bulkArray replies with an array of bulk strings, a nil element being a
// null, without building one Reply per element. The items must not change
// after the handler returns.
func bulkArray(items [][]byte) resp.Reply {
	return resp.Stream(func(w *resp.Writer) {
		w.ArrayHeader(len(items))
		for _, b := range items {
			if b == nil {
				w.Null()
			} else {
				w.Bulk(b)
			}
		}
	})
}

// bulkMap replies with alternating keys and values: a map in RESP3, a
// flat array in RESP2.
func bulkMap(kv [][]byte) resp.Reply {
	return resp.Stream(func(w *resp.Writer) {
		w.MapHeader(len(kv) / 2)
		for _, b := range kv {
			w.Bulk(b)
		}
	})
}

// bulkSet replies with a set in RESP3 and an array in RESP2.
func bulkSet(items [][]byte) resp.Reply {
	return resp.Stream(func(w *resp.Writer) {
		w.SetHeader(len(items))
		for _, b := range items {
			w.Bulk(b)
		}
	})
}

// equalFold reports whether b equals the lowercase ASCII word w, ignoring
// ASCII case in b (strcasecmp).
func equalFold(b []byte, w string) bool {
	if len(b) != len(w) {
		return false
	}
	for i := range len(b) {
		if lower(b[i]) != w[i] {
			return false
		}
	}
	return true
}
