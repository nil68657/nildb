package redis

import (
	"bytes"
	"math"
	"slices"

	"github.com/nil68657/nildb/internal/command"
	"github.com/nil68657/nildb/internal/layout"
	"github.com/nil68657/nildb/internal/resp"
	"github.com/nil68657/nildb/internal/store"
)

// A set keeps each member in sub under | version | member | with an empty
// value. Members come back in byte order; Redis's order is arbitrary.
// SINTER, SUNION, SDIFF and their STORE forms read every input through the
// command's snapshot and hold at most nildb.setop-max-members members.

func init() { groups = append(groups, registerSet) }

func registerSet(r *command.Registry) {
	const g = "set"
	w, ro, f, mr := command.Write, command.ReadOnly, command.Fast, command.MultiRead
	dstOnly := command.KeysFirstLastStep(1, 1, 1)
	r.Register(
		command.Spec{Name: "sadd", Arity: -3, Flags: w | f, FirstKey: 1, LastKey: 1, KeyStep: 1, Group: g, Since: "1.0.0",
			Summary: "Adds one or more members to a set. Creates the key if it doesn't exist.", Run: cmdSadd},
		command.Spec{Name: "srem", Arity: -3, Flags: w | f, FirstKey: 1, LastKey: 1, KeyStep: 1, Group: g, Since: "1.0.0",
			Summary: "Removes one or more members from a set. Deletes the set if the last member was removed.", Run: cmdSrem},
		command.Spec{Name: "smembers", Arity: 2, Flags: ro, FirstKey: 1, LastKey: 1, KeyStep: 1, Group: g, Since: "1.0.0",
			Summary: "Returns all members of a set.", Run: cmdSmembers},
		command.Spec{Name: "sismember", Arity: 3, Flags: ro | f, FirstKey: 1, LastKey: 1, KeyStep: 1, Group: g, Since: "1.0.0",
			Summary: "Determines whether a member belongs to a set.", Run: cmdSismember},
		command.Spec{Name: "smismember", Arity: -3, Flags: ro | f, FirstKey: 1, LastKey: 1, KeyStep: 1, Group: g, Since: "6.2.0",
			Summary: "Determines whether multiple members belong to a set.", Run: cmdSmismember},
		command.Spec{Name: "scard", Arity: 2, Flags: ro | f, FirstKey: 1, LastKey: 1, KeyStep: 1, Group: g, Since: "1.0.0",
			Summary: "Returns the number of members in a set.", Run: cmdScard},
		command.Spec{Name: "spop", Arity: -2, Flags: w | f, FirstKey: 1, LastKey: 1, KeyStep: 1, Group: g, Since: "1.0.0",
			Summary: "Returns one or more random members from a set after removing them. Deletes the set if the last member was popped.", Run: cmdSpop},
		command.Spec{Name: "srandmember", Arity: -2, Flags: ro, FirstKey: 1, LastKey: 1, KeyStep: 1, Group: g, Since: "1.0.0",
			Summary: "Get one or multiple random members from a set", Run: cmdSrandmember},
		command.Spec{Name: "smove", Arity: 4, Flags: w | f, FirstKey: 1, LastKey: 2, KeyStep: 1, Group: g, Since: "1.0.0",
			Summary: "Moves a member from one set to another.", Run: cmdSmove},
		command.Spec{Name: "sinter", Arity: -2, Flags: ro | mr, FirstKey: 1, LastKey: -1, KeyStep: 1, Group: g, Since: "1.0.0",
			Summary: "Returns the intersect of multiple sets.", Run: cmdSinter},
		command.Spec{Name: "sunion", Arity: -2, Flags: ro | mr, FirstKey: 1, LastKey: -1, KeyStep: 1, Group: g, Since: "1.0.0",
			Summary: "Returns the union of multiple sets.", Run: cmdSunion},
		command.Spec{Name: "sdiff", Arity: -2, Flags: ro | mr, FirstKey: 1, LastKey: -1, KeyStep: 1, Group: g, Since: "1.0.0",
			Summary: "Returns the difference of multiple sets.", Run: cmdSdiff},
		command.Spec{Name: "sinterstore", Arity: -3, Flags: w | mr, FirstKey: 1, LastKey: -1, KeyStep: 1, Keys: dstOnly, Group: g, Since: "1.0.0",
			Summary: "Stores the intersect of multiple sets in a key.", Run: cmdSinterstore},
		command.Spec{Name: "sunionstore", Arity: -3, Flags: w | mr, FirstKey: 1, LastKey: -1, KeyStep: 1, Keys: dstOnly, Group: g, Since: "1.0.0",
			Summary: "Stores the union of multiple sets in a key.", Run: cmdSunionstore},
		command.Spec{Name: "sdiffstore", Arity: -3, Flags: w | mr, FirstKey: 1, LastKey: -1, KeyStep: 1, Keys: dstOnly, Group: g, Since: "1.0.0",
			Summary: "Stores the difference of multiple sets in a key.", Run: cmdSdiffstore},
		command.Spec{Name: "sintercard", Arity: -3, Flags: ro | mr, FirstKey: 2, LastKey: -1, KeyStep: 1, Group: g, Since: "7.0.0",
			Summary: "Returns the number of members of the intersect of multiple sets.", Run: cmdSintercard},
	)
}

func cmdSadd(c *command.Ctx, args [][]byte) resp.Reply {
	k, rep := openColl(c, args[1], layout.TSet, true)
	if rep != nil {
		return rep
	}
	members := distinct(args[2:])
	if k.exists {
		vals, err := subValues(c.Reader(), k.m.Version, members)
		if err != nil {
			return storeErr(err)
		}
		fresh := members[:0:0]
		for i, v := range vals {
			if v == nil {
				fresh = append(fresh, members[i])
			}
		}
		members = fresh
	}
	for _, m := range members {
		c.Txn.Put(store.CFSub, layout.SubKey(nil, k.m.Version, m), nil)
	}
	k.grow(int64(len(members)))
	k.save(c)
	return resp.Int(int64(len(members)))
}

func cmdSrem(c *command.Ctx, args [][]byte) resp.Reply {
	k, rep := openColl(c, args[1], layout.TSet, false)
	if rep != nil {
		return rep
	}
	if !k.exists {
		return zero
	}
	members := distinct(args[2:])
	vals, err := subValues(c.Reader(), k.m.Version, members)
	if err != nil {
		return storeErr(err)
	}
	var gone [][]byte
	for i, v := range vals {
		if v != nil {
			gone = append(gone, members[i])
		}
	}
	removeSubs(c, k, gone)
	k.save(c)
	return resp.Int(int64(len(gone)))
}

// removeSubs drops subs from a collection and its count. When they were
// the last elements the key goes (in save) and the sub-keys are left to
// the compaction filter.
func removeSubs(c *command.Ctx, k *coll, subs [][]byte) {
	k.grow(-int64(len(subs)))
	if k.m.Size > 0 {
		for _, s := range subs {
			c.Txn.Delete(store.CFSub, layout.SubKey(nil, k.m.Version, s))
		}
	}
}

func cmdSmembers(c *command.Ctx, args [][]byte) resp.Reply {
	k, rep := openColl(c, args[1], layout.TSet, false)
	if rep != nil {
		return rep
	}
	if !k.exists {
		return bulkSet(nil)
	}
	els, err := readElems(c.Reader(), k.m.Version, 0)
	if err != nil {
		return storeErr(err)
	}
	return bulkSet(subsOf(els))
}

func subsOf(els []elem) [][]byte {
	out := make([][]byte, len(els))
	for i, e := range els {
		out[i] = e.sub
	}
	return out
}

func cmdSismember(c *command.Ctx, args [][]byte) resp.Reply {
	k, rep := openColl(c, args[1], layout.TSet, false)
	if rep != nil || !k.exists {
		return orZero(rep)
	}
	_, ok, err := c.Reader().Get(store.CFSub, layout.SubKey(nil, k.m.Version, args[2]))
	switch {
	case err != nil:
		return storeErr(err)
	case ok:
		return one
	}
	return zero
}

// orZero returns rep, or 0 when rep is nil.
func orZero(rep resp.Reply) resp.Reply {
	if rep != nil {
		return rep
	}
	return zero
}

// cmdSmismember answers 0 for every member of an absent set.
func cmdSmismember(c *command.Ctx, args [][]byte) resp.Reply {
	k, rep := openColl(c, args[1], layout.TSet, false)
	if rep != nil {
		return rep
	}
	out := make([]resp.Reply, len(args)-2)
	var vals [][]byte
	if k.exists {
		var err error
		if vals, err = subValues(c.Reader(), k.m.Version, args[2:]); err != nil {
			return storeErr(err)
		}
	}
	for i := range out {
		out[i] = zero
		if vals != nil && vals[i] != nil {
			out[i] = one
		}
	}
	return resp.Array(out...)
}

func cmdScard(c *command.Ctx, args [][]byte) resp.Reply {
	k, rep := openColl(c, args[1], layout.TSet, false)
	if rep != nil {
		return rep
	}
	return resp.Int(k.m.Size)
}

// errPositive is getPositiveLongFromObjectOrReply's reply, sent for both
// a negative and an unparsable count.
var errPositive = resp.Err("ERR value is out of range, must be positive")

// cmdSpop removes and returns random members (see sample; not uniform
// for sets above 65536 members).
func cmdSpop(c *command.Ctx, args [][]byte) resp.Reply {
	if len(args) > 3 {
		return resp.ErrSyntax
	}
	var count int64 = 1
	if len(args) == 3 {
		n, ok := resp.ParseInt(args[2])
		if !ok || n < 0 {
			return errPositive
		}
		count = n
	}
	k, rep := openColl(c, args[1], layout.TSet, false)
	if rep != nil {
		return rep
	}
	if !k.exists || count == 0 {
		if len(args) == 3 {
			return bulkSet(nil)
		}
		return resp.Null()
	}
	els, err := sample(c.Reader(), k.m.Version, k.m.Size, count, true)
	if err != nil {
		return storeErr(err)
	}
	subs := subsOf(els)
	removeSubs(c, k, subs)
	k.save(c)
	if len(args) == 3 {
		return bulkSet(subs)
	}
	if len(subs) == 0 {
		return resp.Null()
	}
	return resp.Bulk(subs[0])
}

// cmdSrandmember returns random members without removing them: distinct
// ones for a positive count, -count with repeats for a negative one.
func cmdSrandmember(c *command.Ctx, args [][]byte) resp.Reply {
	switch {
	case len(args) > 3:
		return resp.ErrSyntax
	case len(args) == 2:
		return randOne(c, args[1], layout.TSet)
	}
	l, rep := parseIntRange(args[2], -math.MaxInt64, math.MaxInt64)
	if rep != nil {
		return rep
	}
	k, rep := openColl(c, args[1], layout.TSet, false)
	if rep != nil {
		return rep
	}
	count, unique := l, true
	if l < 0 {
		count, unique = -l, false
	}
	if !k.exists || count == 0 {
		return resp.Array()
	}
	els, err := sample(c.Reader(), k.m.Version, k.m.Size, count, unique)
	if err != nil {
		return storeErr(err)
	}
	return bulkArray(subsOf(els))
}

// cmdSmove is smoveCommand. An absent source replies 0 before the
// destination's type is checked. The source is touched when the member
// leaves it, the destination only when the member was new there.
func cmdSmove(c *command.Ctx, args [][]byte) resp.Reply {
	srcKey, dstKey, member := args[1], args[2], args[3]
	src, rep := openColl(c, srcKey, layout.TSet, false)
	if rep != nil {
		return rep
	}
	if !src.exists {
		return zero
	}
	dst, rep := openColl(c, dstKey, layout.TSet, true)
	if rep != nil {
		return rep
	}
	r := c.Reader()
	_, inSrc, err := r.Get(store.CFSub, layout.SubKey(nil, src.m.Version, member))
	if err != nil {
		return storeErr(err)
	}
	if bytes.Equal(srcKey, dstKey) || !inSrc {
		c.Touch()
		if inSrc {
			return one
		}
		return zero
	}
	inDst := false
	if dst.exists {
		if _, inDst, err = r.Get(store.CFSub, layout.SubKey(nil, dst.m.Version, member)); err != nil {
			return storeErr(err)
		}
	}
	removeSubs(c, src, [][]byte{member})
	src.save(c)
	touched := [][]byte{srcKey}
	if !inDst {
		c.Txn.Put(store.CFSub, layout.SubKey(nil, dst.m.Version, member), nil)
		dst.grow(1)
		dst.save(c)
		touched = append(touched, dstKey)
	}
	c.Touch(touched...)
	return one
}

// setIn is one input of a set operation; an absent key is an empty set.
type setIn struct {
	version uint64
	size    int64
	exists  bool
}

// openSets reads every input set, replying WRONGTYPE for the first key of
// another type, as Redis checks them all before computing.
func openSets(c *command.Ctx, keys [][]byte) ([]setIn, resp.Reply) {
	out := make([]setIn, len(keys))
	for i, key := range keys {
		m, ok, err := loadMeta(c, key)
		if err != nil {
			return nil, storeErr(err)
		}
		if rep := requireType(m, ok, layout.TSet); rep != nil {
			return nil, rep
		}
		out[i] = setIn{version: m.Version, size: m.Size, exists: ok}
	}
	return out, nil
}

// setBatch is how many members a set operation checks per MultiGet.
const setBatch = 256

// interSets returns the members common to every input, scanning the
// smallest set and probing the others in batches. It stops after limit
// members when limit > 0 (SINTERCARD's LIMIT) and keeps no members when
// countOnly is set. keep caps the members held.
func interSets(r store.Reader, sets []setIn, limit int64, countOnly bool, keep int) ([][]byte, int64, error) {
	for _, s := range sets {
		if !s.exists {
			return nil, 0, nil
		}
	}
	sorted := slices.Clone(sets)
	slices.SortStableFunc(sorted, func(a, b setIn) int {
		switch {
		case a.size < b.size:
			return -1
		case a.size > b.size:
			return 1
		}
		return 0
	})
	first := sorted[0]
	var out [][]byte
	var n int64
	it := iterColl(r, store.CFSub, first.version, false)
	defer it.Close()
	batch := make([][]byte, 0, setBatch)
	flush := func() (bool, error) {
		alive := batch
		for _, s := range sorted[1:] {
			if s.version == first.version || len(alive) == 0 {
				continue
			}
			vals, err := subValues(r, s.version, alive)
			if err != nil {
				return false, err
			}
			next := alive[:0:0]
			for i, v := range vals {
				if v != nil {
					next = append(next, alive[i])
				}
			}
			alive = next
		}
		for _, m := range alive {
			n++
			if !countOnly {
				if keep > 0 && len(out) >= keep {
					return false, errTooManyMembers(keep)
				}
				out = append(out, m)
			}
			if limit > 0 && n >= limit {
				return true, nil
			}
		}
		batch = batch[:0]
		return false, nil
	}
	for it.SeekToFirst(); it.Valid(); it.Next() {
		batch = append(batch, clone(it.Key()[8:]))
		if len(batch) == setBatch {
			if done, err := flush(); done || err != nil {
				return out, n, err
			}
		}
	}
	if err := it.Err(); err != nil {
		return nil, 0, err
	}
	if len(batch) > 0 {
		if _, err := flush(); err != nil {
			return nil, 0, err
		}
	}
	return out, n, nil
}

// unionSets merges the inputs' sorted member lists, dropping repeats.
func unionSets(r store.Reader, sets []setIn, keep int) ([][]byte, error) {
	var its []store.Iterator
	seen := map[uint64]bool{}
	for _, s := range sets {
		if !s.exists || seen[s.version] {
			continue
		}
		seen[s.version] = true
		it := iterColl(r, store.CFSub, s.version, false)
		defer it.Close()
		it.SeekToFirst()
		its = append(its, it)
	}
	var out [][]byte
	for {
		var low []byte
		for _, it := range its {
			if it.Valid() && (low == nil || bytes.Compare(it.Key()[8:], low) < 0) {
				low = it.Key()[8:]
			}
		}
		if low == nil {
			break
		}
		if keep > 0 && len(out) >= keep {
			return nil, errTooManyMembers(keep)
		}
		m := clone(low)
		out = append(out, m)
		for _, it := range its {
			if it.Valid() && bytes.Equal(it.Key()[8:], m) {
				it.Next()
			}
		}
	}
	for _, it := range its {
		if err := it.Err(); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// diffSets returns the members of the first input that no other input
// holds.
func diffSets(r store.Reader, sets []setIn, keep int) ([][]byte, error) {
	if !sets[0].exists {
		return nil, nil
	}
	first := sets[0].version
	var out [][]byte
	it := iterColl(r, store.CFSub, first, false)
	defer it.Close()
	batch := make([][]byte, 0, setBatch)
	flush := func() error {
		alive := batch
		for _, s := range sets[1:] {
			if !s.exists || len(alive) == 0 {
				continue
			}
			if s.version == first {
				alive = nil
				break
			}
			vals, err := subValues(r, s.version, alive)
			if err != nil {
				return err
			}
			next := alive[:0:0]
			for i, v := range vals {
				if v == nil {
					next = append(next, alive[i])
				}
			}
			alive = next
		}
		if keep > 0 && len(out)+len(alive) > keep {
			return errTooManyMembers(keep)
		}
		out = append(out, alive...)
		batch = batch[:0]
		return nil
	}
	for it.SeekToFirst(); it.Valid(); it.Next() {
		batch = append(batch, clone(it.Key()[8:]))
		if len(batch) == setBatch {
			if err := flush(); err != nil {
				return nil, err
			}
		}
	}
	if err := it.Err(); err != nil {
		return nil, err
	}
	if len(batch) > 0 {
		if err := flush(); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// Set operation kinds.
const (
	opInter = iota
	opUnion
	opDiff
)

// setOp computes SINTER, SUNION or SDIFF over keys.
func setOp(c *command.Ctx, keys [][]byte, op int) ([][]byte, resp.Reply) {
	sets, rep := openSets(c, keys)
	if rep != nil {
		return nil, rep
	}
	keep := c.Cfg.Knobs().SetOpMaxMembers
	var out [][]byte
	var err error
	switch op {
	case opInter:
		out, _, err = interSets(c.Reader(), sets, 0, false, keep)
	case opUnion:
		out, err = unionSets(c.Reader(), sets, keep)
	default:
		out, err = diffSets(c.Reader(), sets, keep)
	}
	if err != nil {
		return nil, storeErr(err)
	}
	return out, nil
}

func cmdSinter(c *command.Ctx, args [][]byte) resp.Reply { return setOpReply(c, args[1:], opInter) }
func cmdSunion(c *command.Ctx, args [][]byte) resp.Reply { return setOpReply(c, args[1:], opUnion) }
func cmdSdiff(c *command.Ctx, args [][]byte) resp.Reply  { return setOpReply(c, args[1:], opDiff) }

func setOpReply(c *command.Ctx, keys [][]byte, op int) resp.Reply {
	out, rep := setOp(c, keys, op)
	if rep != nil {
		return rep
	}
	return bulkSet(out)
}

func cmdSinterstore(c *command.Ctx, args [][]byte) resp.Reply {
	return setOpStore(c, args[1], args[2:], opInter)
}

func cmdSunionstore(c *command.Ctx, args [][]byte) resp.Reply {
	return setOpStore(c, args[1], args[2:], opUnion)
}

func cmdSdiffstore(c *command.Ctx, args [][]byte) resp.Reply {
	return setOpStore(c, args[1], args[2:], opDiff)
}

// setOpStore writes the result to dst as a new set without TTL; an empty
// result deletes dst.
func setOpStore(c *command.Ctx, dst []byte, keys [][]byte, op int) resp.Reply {
	out, rep := setOp(c, keys, op)
	if rep != nil {
		return rep
	}
	return storeSet(c, dst, out)
}

// storeSet replaces dst with a set holding members.
func storeSet(c *command.Ctx, dst []byte, members [][]byte) resp.Reply {
	e, err := getEntry(c, dst)
	if err != nil {
		return storeErr(err)
	}
	if len(members) == 0 {
		if e.found {
			deleteKey(c, c.DB, dst, e)
		}
		return zero
	}
	ver := newVersion(c)
	for _, m := range members {
		c.Txn.Put(store.CFSub, layout.SubKey(nil, ver, m), nil)
	}
	writeMeta(c, c.DB, dst, layout.Meta{Type: layout.TSet, Version: ver, Size: int64(len(members))}, e)
	return resp.Int(int64(len(members)))
}

// cmdSintercard is sinterCardCommand: numkeys and LIMIT carry their own
// error texts, which Redis also sends for unparsable values.
func cmdSintercard(c *command.Ctx, args [][]byte) resp.Reply {
	numkeys, ok := resp.ParseInt(args[1])
	if !ok || numkeys < 1 {
		return resp.Err("ERR numkeys should be greater than 0")
	}
	if numkeys > int64(len(args)-2) {
		return resp.Err("ERR Number of keys can't be greater than number of args")
	}
	nk := int(numkeys)
	var limit int64
	for j := 2 + nk; j < len(args); j++ {
		if equalFold(args[j], "limit") && j+1 < len(args) {
			j++
			n, ok := resp.ParseInt(args[j])
			if !ok || n < 0 {
				return resp.Err("ERR LIMIT can't be negative")
			}
			limit = n
			continue
		}
		return resp.ErrSyntax
	}
	sets, rep := openSets(c, args[2:2+nk])
	if rep != nil {
		return rep
	}
	_, n, err := interSets(c.Reader(), sets, limit, true, 0)
	if err != nil {
		return storeErr(err)
	}
	return resp.Int(n)
}
