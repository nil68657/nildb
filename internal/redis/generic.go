package redis

import (
	"bytes"
	"encoding/binary"
	"math"

	"github.com/nil68657/nildb/internal/command"
	"github.com/nil68657/nildb/internal/layout"
	"github.com/nil68657/nildb/internal/resp"
	"github.com/nil68657/nildb/internal/store"
)

func registerGeneric(r *command.Registry) {
	const g = "generic"
	w, ro, f := command.Write, command.ReadOnly, command.Fast
	r.Register(
		command.Spec{Name: "del", Arity: -2, Flags: w, FirstKey: 1, LastKey: -1, KeyStep: 1, Group: g, Since: "1.0.0",
			Summary: "Deletes one or more keys.", Run: cmdDel},
		command.Spec{Name: "unlink", Arity: -2, Flags: w | f, FirstKey: 1, LastKey: -1, KeyStep: 1, Group: g, Since: "4.0.0",
			Summary: "Asynchronously deletes one or more keys.", Run: cmdDel},
		command.Spec{Name: "exists", Arity: -2, Flags: ro | f | command.MultiRead, FirstKey: 1, LastKey: -1, KeyStep: 1, Group: g, Since: "1.0.0",
			Summary: "Determines whether one or more keys exist.", Run: cmdExists},
		command.Spec{Name: "touch", Arity: -2, Flags: ro | f | command.MultiRead, FirstKey: 1, LastKey: -1, KeyStep: 1, Group: g, Since: "3.2.1",
			Summary: "Returns the number of existing keys out of those specified after updating the time they were last accessed.", Run: cmdExists},
		command.Spec{Name: "type", Arity: 2, Flags: ro | f, FirstKey: 1, LastKey: 1, KeyStep: 1, Group: g, Since: "1.0.0",
			Summary: "Determines the type of value stored at a key.", Run: cmdType},
		command.Spec{Name: "rename", Arity: 3, Flags: w, FirstKey: 1, LastKey: 2, KeyStep: 1, Group: g, Since: "1.0.0",
			Summary: "Renames a key and overwrites the destination.", Run: cmdRename},
		command.Spec{Name: "renamenx", Arity: 3, Flags: w | f, FirstKey: 1, LastKey: 2, KeyStep: 1, Group: g, Since: "1.0.0",
			Summary: "Renames a key only when the target key name doesn't exist.", Run: cmdRenamenx},
		command.Spec{Name: "keys", Arity: 2, Flags: ro, Group: g, Since: "1.0.0",
			Summary: "Returns all key names that match a pattern.", Run: cmdKeys},
		command.Spec{Name: "randomkey", Arity: 1, Flags: ro, Group: g, Since: "1.0.0",
			Summary: "Returns a random key name from the database.", Run: cmdRandomkey},
		command.Spec{Name: "move", Arity: 3, Flags: w | f, FirstKey: 1, LastKey: 1, KeyStep: 1, Keys: moveKeys, Group: g, Since: "1.0.0",
			Summary: "Moves a key to another database.", Run: cmdMove},
		command.Spec{Name: "dbsize", Arity: 1, Flags: ro | f, Group: "server", Since: "1.0.0",
			Summary: "Returns the number of keys in the database.", Run: cmdDbsize},
		command.Spec{Name: "flushdb", Arity: -1, Flags: w | command.NoMulti, Group: "server", Since: "1.0.0",
			Summary: "Remove all keys from the current database.", Run: cmdFlushdb},
		command.Spec{Name: "flushall", Arity: -1, Flags: w | command.NoMulti, Group: "server", Since: "1.0.0",
			Summary: "Removes all keys from all databases.", Run: cmdFlushall},
	)
}

// rawExpired reads expire_ms from the header of a raw meta value.
// Values too short to hold a header count as gone.
func rawExpired(v []byte, nowMS int64) bool {
	if len(v) < layout.StringHeaderLen {
		return true
	}
	exp := int64(binary.BigEndian.Uint64(v[1:layout.StringHeaderLen]))
	return exp != 0 && exp <= nowMS
}

// dbBounds returns the meta range [db, db+1) holding database db's keys.
func dbBounds(db uint8) (lo, hi []byte) {
	return layout.MetaKey(nil, db, nil), layout.MetaKey(nil, db+1, nil)
}

// cmdDel is DEL and UNLINK (NilDB deletes a collection in O(1), so both
// are synchronous). Expired keys are removed too but neither counted nor
// touched, as Redis expires them before deleting.
func cmdDel(c *command.Ctx, args [][]byte) resp.Reply {
	var deleted [][]byte
	seen := make(map[string]struct{}, len(args)-1)
	for _, k := range args[1:] {
		if _, dup := seen[string(k)]; dup {
			continue
		}
		seen[string(k)] = struct{}{}
		e, err := getEntry(c, k)
		if err != nil {
			return storeErr(err)
		}
		if !e.found {
			continue
		}
		deleteKey(c, c.DB, k, e)
		if e.live {
			deleted = append(deleted, k)
		}
	}
	c.Touch(deleted...)
	return resp.Int(int64(len(deleted)))
}

// cmdExists is EXISTS and TOUCH: the count of live keys, a key named
// twice counting twice.
func cmdExists(c *command.Ctx, args [][]byte) resp.Reply {
	keys := make([][]byte, len(args)-1)
	for i, k := range args[1:] {
		keys[i] = layout.MetaKey(nil, c.DB, k)
	}
	vals, err := c.Reader().MultiGet(store.CFMeta, keys)
	if err != nil {
		return storeErr(err)
	}
	n, now := int64(0), c.NowMS()
	for _, v := range vals {
		if v != nil && !rawExpired(v, now) {
			n++
		}
	}
	return resp.Int(n)
}

func cmdType(c *command.Ctx, args [][]byte) resp.Reply {
	m, ok, err := loadMeta(c, args[1])
	if err != nil {
		return storeErr(err)
	}
	if !ok {
		return resp.Status("none")
	}
	return resp.Status(m.Type.String())
}

func cmdRename(c *command.Ctx, args [][]byte) resp.Reply   { return renameGeneric(c, args, false) }
func cmdRenamenx(c *command.Ctx, args [][]byte) resp.Reply { return renameGeneric(c, args, true) }

// renameGeneric moves the metadata of src to dst, TTL included. A
// collection keeps its version, so its elements stay where they are and
// only the version-map entry is rewritten with the new key.
func renameGeneric(c *command.Ctx, args [][]byte, nx bool) resp.Reply {
	src, dst := args[1], args[2]
	se, err := getEntry(c, src)
	if err != nil {
		return storeErr(err)
	}
	if !se.live {
		return resp.ErrNoSuchKey
	}
	if bytes.Equal(src, dst) {
		if nx {
			return zero
		}
		return resp.OK()
	}
	de, err := getEntry(c, dst)
	if err != nil {
		return storeErr(err)
	}
	if de.live && nx {
		return zero
	}
	writeMeta(c, c.DB, dst, se.m, de)
	c.Txn.Delete(store.CFMeta, layout.MetaKey(nil, c.DB, src))
	if nx {
		return one
	}
	return resp.OK()
}

// moveKeys locks the key in the current database and, when the target
// database parses, in that one too.
func moveKeys(kc *command.KeyCtx, args [][]byte) ([]store.LockKey, any, error) {
	keys := []store.LockKey{{Kind: store.LockRedis, NS: uint32(kc.DB), Key: args[1]}}
	if db, ok := resp.ParseInt(args[2]); ok && db >= 0 && db < layout.NumDBs && uint8(db) != kc.DB {
		keys = append(keys, store.LockKey{Kind: store.LockRedis, NS: uint32(db), Key: args[1]})
	}
	return keys, nil, nil
}

// cmdMove rewrites the metadata under the target database's key byte; a
// collection keeps its version and elements.
func cmdMove(c *command.Ctx, args [][]byte) resp.Reply {
	key := args[1]
	db, rep := parseIntRange(args[2], math.MinInt32, math.MaxInt32)
	if rep != nil {
		return rep
	}
	if db < 0 || db >= layout.NumDBs {
		return resp.ErrDBIndex
	}
	dst := uint8(db)
	if dst == c.DB {
		return errSameObject
	}
	se, err := getEntry(c, key)
	if err != nil {
		return storeErr(err)
	}
	if !se.live {
		return zero
	}
	de, err := readEntry(c.Reader(), dst, key, c.NowMS())
	if err != nil {
		return storeErr(err)
	}
	if de.live {
		return zero
	}
	writeMeta(c, dst, key, se.m, de)
	c.Txn.Delete(store.CFMeta, layout.MetaKey(nil, c.DB, key))
	return one
}

// cmdKeys walks the database in key order from the pattern's literal
// prefix. It reads through one iterator, which sees one consistent
// state, and skips expired keys.
func cmdKeys(c *command.Ctx, args [][]byte) resp.Reply {
	pattern := args[1]
	all := len(pattern) == 1 && pattern[0] == '*'
	lo := layout.MetaKey(nil, c.DB, globPrefix(pattern))
	hi := prefixEnd(lo)
	it := c.Reader().Iter(store.CFMeta, lo, hi, store.IterOpts{})
	defer it.Close()
	var out [][]byte
	now := c.NowMS()
	for it.SeekToFirst(); it.Valid(); it.Next() {
		k := it.Key()[1:]
		if (all || globMatch(pattern, k, false)) && !rawExpired(it.Value(), now) {
			out = append(out, clone(k))
		}
	}
	if err := it.Err(); err != nil {
		return storeErr(err)
	}
	return bulkArray(out)
}

// prefixEnd returns the smallest key above every key that starts with p,
// or nil when there is none. p starts with a database byte below 16, so
// the result is never nil for a meta key.
func prefixEnd(p []byte) []byte {
	end := clone(p)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] != 0xFF {
			end[i]++
			return end[:i+1]
		}
	}
	return nil
}

// cmdDbsize counts the live keys of the database with one O(n) scan of
// its meta range; expired keys waiting for compaction are skipped.
func cmdDbsize(c *command.Ctx, _ [][]byte) resp.Reply {
	lo, hi := dbBounds(c.DB)
	it := c.Reader().Iter(store.CFMeta, lo, hi, store.IterOpts{})
	defer it.Close()
	n, now := int64(0), c.NowMS()
	for it.SeekToFirst(); it.Valid(); it.Next() {
		if !rawExpired(it.Value(), now) {
			n++
		}
	}
	if err := it.Err(); err != nil {
		return storeErr(err)
	}
	return resp.Int(n)
}

// randomKeyTries bounds how many expired keys RANDOMKEY steps over.
const randomKeyTries = 100

// cmdRandomkey seeks a random position in the database (see randomSeek;
// the choice is not uniform) and returns the first live key from there,
// wrapping once. After 100 expired keys in a row it replies a null.
func cmdRandomkey(c *command.Ctx, _ [][]byte) resp.Reply {
	lo, hi := dbBounds(c.DB)
	it := c.Reader().Iter(store.CFMeta, lo, hi, store.IterOpts{FillCache: true})
	defer it.Close()
	if !randomSeek(it) {
		if err := it.Err(); err != nil {
			return storeErr(err)
		}
		return resp.Null()
	}
	now := c.NowMS()
	wrapped := false
	for range randomKeyTries {
		if !it.Valid() {
			if wrapped {
				break
			}
			wrapped = true
			it.SeekToFirst()
			continue
		}
		if !rawExpired(it.Value(), now) {
			return resp.Bulk(clone(it.Key()[1:]))
		}
		it.Next()
	}
	if err := it.Err(); err != nil {
		return storeErr(err)
	}
	return resp.Null()
}

// flushArgs checks FLUSHDB and FLUSHALL's optional SYNC or ASYNC. Both
// run synchronously: a drop is a range tombstone.
func flushArgs(args [][]byte) resp.Reply {
	if len(args) == 1 || (len(args) == 2 && (equalFold(args[1], "sync") || equalFold(args[1], "async"))) {
		return nil
	}
	return resp.ErrSyntax
}

// touchFlushed marks dirty the watchers of every key of db that has a
// meta entry, before the flush deletes it.
func touchFlushed(c *command.Ctx, db uint8) {
	if c.Watch == nil {
		return
	}
	c.Watch.TouchExisting(db, func(key []byte) bool {
		_, ok, err := c.Store.Get(store.CFMeta, layout.MetaKey(nil, db, key))
		return ok || err != nil
	})
}

// cmdFlushdb drops the database with one range tombstone over its meta
// range plus point deletes of the version-map entries of its collections,
// found by a scan of that range. A collection created by another client
// between the scan and the drop keeps a version-map entry for a key that
// no longer exists, which holds its elements until FLUSHALL.
func cmdFlushdb(c *command.Ctx, args [][]byte) resp.Reply {
	if rep := flushArgs(args); rep != nil {
		return rep
	}
	touchFlushed(c, c.DB)
	c.Touch()
	lo, hi := dbBounds(c.DB)
	var vkeys []store.PointDelete
	it := c.Store.Iter(store.CFMeta, lo, hi, store.IterOpts{})
	for it.SeekToFirst(); it.Valid(); it.Next() {
		v := it.Value()
		if len(v) >= layout.CollMetaLen && layout.Type(v[0]&0x0F).IsCollection() {
			ver := binary.BigEndian.Uint64(v[layout.StringHeaderLen:])
			vkeys = append(vkeys, store.PointDelete{CF: store.CFMeta, Key: layout.VersionKey(nil, ver)})
		}
	}
	err := it.Err()
	it.Close()
	if err != nil {
		return storeErr(err)
	}
	if err := c.Store.DeleteRanges([]store.RangeDelete{{CF: store.CFMeta, Lo: lo, Hi: hi}}, vkeys); err != nil {
		return storeErr(err)
	}
	c.Store.ScheduleCompact(store.CFMeta, lo, hi)
	return resp.OK()
}

// cmdFlushall drops every database and the whole version map with two
// range tombstones; nothing else lives in meta.
func cmdFlushall(c *command.Ctx, args [][]byte) resp.Reply {
	if rep := flushArgs(args); rep != nil {
		return rep
	}
	for db := range uint8(layout.NumDBs) {
		touchFlushed(c, db)
	}
	c.Touch()
	ranges := []store.RangeDelete{
		{CF: store.CFMeta, Lo: []byte{0}, Hi: []byte{layout.NumDBs}},
		{CF: store.CFMeta, Lo: []byte{layout.VersionMapPrefix}, Hi: []byte{layout.VersionMapPrefix + 1}},
	}
	if err := c.Store.DeleteRanges(ranges, nil); err != nil {
		return storeErr(err)
	}
	for _, r := range ranges {
		c.Store.ScheduleCompact(r.CF, r.Lo, r.Hi)
	}
	return resp.OK()
}
