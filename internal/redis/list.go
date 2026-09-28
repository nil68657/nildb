package redis

import (
	"bytes"
	"math"

	"github.com/nil68657/nildb/internal/command"
	"github.com/nil68657/nildb/internal/layout"
	"github.com/nil68657/nildb/internal/resp"
	"github.com/nil68657/nildb/internal/store"
)

// A list keeps element i (0 = head) in sub under | version | Head+i | with
// the 8-byte index big-endian; the meta holds Head and Tail (both start at
// 1<<63) and live indexes are [Head, Tail). Pushes and pops move Head or
// Tail. LINSERT and LREM rewrite the shorter side of the change and cost
// O(n). An emptied list deletes its key. Blocking variants are not in v1.

func init() { groups = append(groups, registerList) }

func registerList(r *command.Registry) {
	const g = "list"
	w, ro, f := command.Write, command.ReadOnly, command.Fast
	r.Register(
		command.Spec{Name: "lpush", Arity: -3, Flags: w | f, FirstKey: 1, LastKey: 1, KeyStep: 1, Group: g, Since: "1.0.0",
			Summary: "Prepends one or more elements to a list. Creates the key if it doesn't exist.", Run: cmdLpush},
		command.Spec{Name: "rpush", Arity: -3, Flags: w | f, FirstKey: 1, LastKey: 1, KeyStep: 1, Group: g, Since: "1.0.0",
			Summary: "Appends one or more elements to a list. Creates the key if it doesn't exist.", Run: cmdRpush},
		command.Spec{Name: "lpushx", Arity: -3, Flags: w | f, FirstKey: 1, LastKey: 1, KeyStep: 1, Group: g, Since: "2.2.0",
			Summary: "Prepends one or more elements to a list only when the list exists.", Run: cmdLpushx},
		command.Spec{Name: "rpushx", Arity: -3, Flags: w | f, FirstKey: 1, LastKey: 1, KeyStep: 1, Group: g, Since: "2.2.0",
			Summary: "Appends an element to a list only when the list exists.", Run: cmdRpushx},
		command.Spec{Name: "lpop", Arity: -2, Flags: w | f, FirstKey: 1, LastKey: 1, KeyStep: 1, Group: g, Since: "1.0.0",
			Summary: "Returns the first elements in a list after removing it. Deletes the list if the last element was popped.", Run: cmdLpop},
		command.Spec{Name: "rpop", Arity: -2, Flags: w | f, FirstKey: 1, LastKey: 1, KeyStep: 1, Group: g, Since: "1.0.0",
			Summary: "Returns and removes the last elements of a list. Deletes the list if the last element was popped.", Run: cmdRpop},
		command.Spec{Name: "llen", Arity: 2, Flags: ro | f, FirstKey: 1, LastKey: 1, KeyStep: 1, Group: g, Since: "1.0.0",
			Summary: "Returns the length of a list.", Run: cmdLlen},
		command.Spec{Name: "lrange", Arity: 4, Flags: ro, FirstKey: 1, LastKey: 1, KeyStep: 1, Group: g, Since: "1.0.0",
			Summary: "Returns a range of elements from a list.", Run: cmdLrange},
		command.Spec{Name: "lindex", Arity: 3, Flags: ro, FirstKey: 1, LastKey: 1, KeyStep: 1, Group: g, Since: "1.0.0",
			Summary: "Returns an element from a list by its index.", Run: cmdLindex},
		command.Spec{Name: "lset", Arity: 4, Flags: w, FirstKey: 1, LastKey: 1, KeyStep: 1, Group: g, Since: "1.0.0",
			Summary: "Sets the value of an element in a list by its index.", Run: cmdLset},
		command.Spec{Name: "lrem", Arity: 4, Flags: w, FirstKey: 1, LastKey: 1, KeyStep: 1, Group: g, Since: "1.0.0",
			Summary: "Removes elements from a list. Deletes the list if the last element was removed.", Run: cmdLrem},
		command.Spec{Name: "ltrim", Arity: 4, Flags: w, FirstKey: 1, LastKey: 1, KeyStep: 1, Group: g, Since: "1.0.0",
			Summary: "Removes elements from both ends a list. Deletes the list if all elements were trimmed.", Run: cmdLtrim},
		command.Spec{Name: "linsert", Arity: 5, Flags: w, FirstKey: 1, LastKey: 1, KeyStep: 1, Group: g, Since: "2.2.0",
			Summary: "Inserts an element before or after another element in a list.", Run: cmdLinsert},
		command.Spec{Name: "lpos", Arity: -3, Flags: ro, FirstKey: 1, LastKey: 1, KeyStep: 1, Group: g, Since: "6.0.6",
			Summary: "Returns the index of matching elements in a list.", Run: cmdLpos},
		command.Spec{Name: "lmove", Arity: 5, Flags: w, FirstKey: 1, LastKey: 2, KeyStep: 1, Group: g, Since: "6.2.0",
			Summary: "Returns an element after popping it from one list and pushing it to another. Deletes the list if the last element was moved.", Run: cmdLmove},
		command.Spec{Name: "rpoplpush", Arity: 3, Flags: w, FirstKey: 1, LastKey: 2, KeyStep: 1, Group: g, Since: "1.2.0",
			Summary: "Returns the last element of a list after removing and pushing it to another list. Deletes the list if the last element was popped.", Run: cmdRpoplpush},
	)
}

// lkey is the sub key of the element stored at index idx.
func lkey(k *coll, idx uint64) []byte { return layout.ListKey(nil, k.m.Version, idx) }

// size is the element count, Tail - Head.
func (k *coll) size() int64 { return int64(k.m.Tail - k.m.Head) }

// setBounds moves Head and Tail and keeps Size in step.
func (k *coll) setBounds(head, tail uint64) {
	if head != k.m.Head || tail != k.m.Tail {
		k.m.Head, k.m.Tail = head, tail
		k.m.Size = int64(tail - head)
		k.dirty = true
	}
}

// pushLeft and pushRight add one element at an end.
func pushLeft(c *command.Ctx, k *coll, v []byte) {
	k.setBounds(k.m.Head-1, k.m.Tail)
	c.Txn.Put(store.CFSub, lkey(k, k.m.Head), v)
}

func pushRight(c *command.Ctx, k *coll, v []byte) {
	c.Txn.Put(store.CFSub, lkey(k, k.m.Tail), v)
	k.setBounds(k.m.Head, k.m.Tail+1)
}

// listRange reads the elements at indexes [from, to) relative to Head,
// in order.
func listRange(r store.Reader, k *coll, from, to int64) ([][]byte, error) {
	if from >= to {
		return nil, nil
	}
	it := r.Iter(store.CFSub, lkey(k, k.m.Head+uint64(from)), lkey(k, k.m.Head+uint64(to)), store.IterOpts{FillCache: true})
	defer it.Close()
	out := make([][]byte, 0, to-from)
	for it.SeekToFirst(); it.Valid(); it.Next() {
		out = append(out, clone(it.Value()))
	}
	return out, it.Err()
}

func cmdLpush(c *command.Ctx, args [][]byte) resp.Reply  { return pushGeneric(c, args, true, false) }
func cmdRpush(c *command.Ctx, args [][]byte) resp.Reply  { return pushGeneric(c, args, false, false) }
func cmdLpushx(c *command.Ctx, args [][]byte) resp.Reply { return pushGeneric(c, args, true, true) }
func cmdRpushx(c *command.Ctx, args [][]byte) resp.Reply { return pushGeneric(c, args, false, true) }

// pushGeneric is pushGenericCommand: elements go in argument order, so
// LPUSH a b c leaves c at the head. The X forms reply 0 for an absent key.
func pushGeneric(c *command.Ctx, args [][]byte, left, xx bool) resp.Reply {
	k, rep := openColl(c, args[1], layout.TList, !xx)
	if rep != nil {
		return rep
	}
	if !k.exists && xx {
		return zero
	}
	for _, v := range args[2:] {
		if left {
			pushLeft(c, k, v)
		} else {
			pushRight(c, k, v)
		}
	}
	k.save(c)
	return resp.Int(k.size())
}

func cmdLpop(c *command.Ctx, args [][]byte) resp.Reply { return popGeneric(c, args, true) }
func cmdRpop(c *command.Ctx, args [][]byte) resp.Reply { return popGeneric(c, args, false) }

// popGeneric is popGenericCommand: a count replies an array (a null array
// for an absent key), no count replies one bulk string or a null.
func popGeneric(c *command.Ctx, args [][]byte, left bool) resp.Reply {
	if len(args) > 3 {
		return resp.ErrArity(c.Spec.FullName())
	}
	counted := len(args) == 3
	var count int64 = 1
	if counted {
		n, ok := resp.ParseInt(args[2])
		if !ok || n < 0 {
			return errPositive
		}
		count = n
	}
	k, rep := openColl(c, args[1], layout.TList, false)
	switch {
	case rep != nil:
		return rep
	case !k.exists && counted:
		return resp.NullArray()
	case !k.exists:
		return resp.Null()
	case counted && count == 0:
		return resp.Array()
	}
	n := min(count, k.size())
	var els [][]byte
	var err error
	if left {
		els, err = listRange(c.Reader(), k, 0, n)
	} else {
		els, err = listRange(c.Reader(), k, k.size()-n, k.size())
		reverse(els)
	}
	if err != nil {
		return storeErr(err)
	}
	if len(els) == 0 {
		return resp.Null()
	}
	popEnds(c, k, n, left)
	k.save(c)
	if !counted {
		return resp.Bulk(els[0])
	}
	return bulkArray(els)
}

// popEnds removes n elements from the head (left) or the tail. When they
// are all the elements, the key goes (in save) and the sub-keys are left
// to the compaction filter.
func popEnds(c *command.Ctx, k *coll, n int64, left bool) {
	head, tail := k.m.Head, k.m.Tail
	lo, hi := head, head+uint64(n)
	if !left {
		lo, hi = tail-uint64(n), tail
	}
	if n < k.size() {
		deleteIndexes(c, k, lo, hi)
	}
	if left {
		k.setBounds(hi, tail)
	} else {
		k.setBounds(head, lo)
	}
}

// deleteIndexes deletes the elements at absolute indexes [lo, hi): one
// range tombstone for a large span outside EXEC, point deletes otherwise.
func deleteIndexes(c *command.Ctx, k *coll, lo, hi uint64) {
	if hi-lo > 1024 && !c.InExec {
		if err := c.Txn.DeleteRange(store.CFSub, lkey(k, lo), lkey(k, hi)); err == nil {
			return
		}
	}
	for i := lo; i < hi; i++ {
		c.Txn.Delete(store.CFSub, lkey(k, i))
	}
}

func reverse(s [][]byte) {
	for i, j := 0, len(s)-1; i < j; i, j = i+1, j-1 {
		s[i], s[j] = s[j], s[i]
	}
}

func cmdLlen(c *command.Ctx, args [][]byte) resp.Reply {
	k, rep := openColl(c, args[1], layout.TList, false)
	if rep != nil || !k.exists {
		return orZero(rep)
	}
	return resp.Int(k.size())
}

// listIndexes normalises a start/end pair as addListRangeReply does and
// returns [from, to) relative to Head, empty when from >= to.
func listIndexes(start, end, n int64) (int64, int64) {
	if start < 0 {
		start += n
	}
	if end < 0 {
		end += n
	}
	start = max(start, 0)
	if start > end || start >= n {
		return 0, 0
	}
	end = min(end, n-1)
	return start, end + 1
}

func cmdLrange(c *command.Ctx, args [][]byte) resp.Reply {
	start, ok1 := resp.ParseInt(args[2])
	end, ok2 := resp.ParseInt(args[3])
	if !ok1 || !ok2 {
		return resp.ErrNotInteger
	}
	k, rep := openColl(c, args[1], layout.TList, false)
	switch {
	case rep != nil:
		return rep
	case !k.exists:
		return resp.Array()
	}
	from, to := listIndexes(start, end, k.size())
	els, err := listRange(c.Reader(), k, from, to)
	if err != nil {
		return storeErr(err)
	}
	return bulkArray(els)
}

// resolveIndex turns a LINDEX/LSET index into an offset from Head.
func resolveIndex(idx, n int64) (int64, bool) {
	if idx < 0 {
		idx += n
	}
	return idx, idx >= 0 && idx < n
}

// cmdLindex reads the key before it parses the index, as lindexCommand
// does, so an absent key replies a null even for a bad index.
func cmdLindex(c *command.Ctx, args [][]byte) resp.Reply {
	k, rep := openColl(c, args[1], layout.TList, false)
	switch {
	case rep != nil:
		return rep
	case !k.exists:
		return resp.Null()
	}
	idx, ok := resp.ParseInt(args[2])
	if !ok {
		return resp.ErrNotInteger
	}
	off, in := resolveIndex(idx, k.size())
	if !in {
		return resp.Null()
	}
	v, found, err := c.Reader().Get(store.CFSub, lkey(k, k.m.Head+uint64(off)))
	switch {
	case err != nil:
		return storeErr(err)
	case !found:
		return resp.Null()
	}
	return resp.Bulk(v)
}

func cmdLset(c *command.Ctx, args [][]byte) resp.Reply {
	k, rep := openColl(c, args[1], layout.TList, false)
	switch {
	case rep != nil:
		return rep
	case !k.exists:
		return resp.ErrNoSuchKey
	}
	idx, ok := resp.ParseInt(args[2])
	if !ok {
		return resp.ErrNotInteger
	}
	off, in := resolveIndex(idx, k.size())
	if !in {
		return resp.ErrOutOfRange
	}
	c.Txn.Put(store.CFSub, lkey(k, k.m.Head+uint64(off)), args[3])
	return resp.OK()
}

// cmdLtrim keeps [start, end] and deletes both outer parts; an absent key
// replies +OK.
func cmdLtrim(c *command.Ctx, args [][]byte) resp.Reply {
	start, ok1 := resp.ParseInt(args[2])
	end, ok2 := resp.ParseInt(args[3])
	if !ok1 || !ok2 {
		return resp.ErrNotInteger
	}
	k, rep := openColl(c, args[1], layout.TList, false)
	switch {
	case rep != nil:
		return rep
	case !k.exists:
		return resp.OK()
	}
	from, to := listIndexes(start, end, k.size())
	head := k.m.Head
	if to > from {
		deleteIndexes(c, k, head, head+uint64(from))
		deleteIndexes(c, k, head+uint64(to), k.m.Tail)
		k.setBounds(head+uint64(from), head+uint64(to))
	} else {
		k.setBounds(head, head)
	}
	k.save(c)
	return resp.OK()
}

// cmdLrem removes up to |count| elements equal to the argument, from the
// head for a positive count and from the tail for a negative one, 0
// meaning all, then closes the gaps.
func cmdLrem(c *command.Ctx, args [][]byte) resp.Reply {
	toRemove, ok := resp.ParseInt(args[2])
	if !ok {
		return resp.ErrNotInteger
	}
	k, rep := openColl(c, args[1], layout.TList, false)
	if rep != nil || !k.exists {
		return orZero(rep)
	}
	els, err := listRange(c.Reader(), k, 0, k.size())
	if err != nil {
		return storeErr(err)
	}
	drop := make([]bool, len(els))
	removed := int64(0)
	limit := toRemove
	if limit < 0 {
		limit = -limit
	}
	match := func(i int) bool {
		if bytes.Equal(els[i], args[3]) {
			drop[i] = true
			removed++
		}
		return limit != 0 && removed == limit
	}
	if toRemove < 0 {
		for i := len(els) - 1; i >= 0 && !match(i); i-- {
		}
	} else {
		for i := 0; i < len(els) && !match(i); i++ {
		}
	}
	if removed == 0 {
		return zero
	}
	rewriteList(c, k, els, drop)
	k.save(c)
	return resp.Int(removed)
}

// rewriteList stores els minus the dropped ones contiguously, moving
// whichever side of the dropped span is shorter: the part left of the
// last drop shifts right, or the part right of the first drop shifts
// left.
func rewriteList(c *command.Ctx, k *coll, els [][]byte, drop []bool) {
	first, last := -1, -1
	kept := 0
	for i, d := range drop {
		switch {
		case d && first < 0:
			first, last = i, i
		case d:
			last = i
		default:
			kept++
		}
	}
	gone := uint64(len(els) - kept)
	head, tail := k.m.Head, k.m.Tail
	if kept == 0 {
		k.setBounds(head, head)
		return
	}
	if last+1 <= len(els)-first {
		// Shift the left part [0, last] right by the number of drops.
		for i := uint64(0); i < gone; i++ {
			c.Txn.Delete(store.CFSub, lkey(k, head+i))
		}
		j := head + gone
		for i := 0; i <= last; i++ {
			if !drop[i] {
				c.Txn.Put(store.CFSub, lkey(k, j), els[i])
				j++
			}
		}
		k.setBounds(head+gone, tail)
		return
	}
	// Shift the right part [first, n) left.
	j := head + uint64(first)
	for i := first; i < len(els); i++ {
		if !drop[i] {
			c.Txn.Put(store.CFSub, lkey(k, j), els[i])
			j++
		}
	}
	for i := uint64(0); i < gone; i++ {
		c.Txn.Delete(store.CFSub, lkey(k, tail-1-i))
	}
	k.setBounds(head, tail-gone)
}

// cmdLinsert inserts before or after the first element equal to the
// pivot, shifting the shorter side by one. It replies the new length, -1
// when the pivot is absent, 0 when the key is.
func cmdLinsert(c *command.Ctx, args [][]byte) resp.Reply {
	var after bool
	switch {
	case equalFold(args[2], "after"):
		after = true
	case equalFold(args[2], "before"):
	default:
		return resp.ErrSyntax
	}
	k, rep := openColl(c, args[1], layout.TList, false)
	if rep != nil || !k.exists {
		return orZero(rep)
	}
	els, err := listRange(c.Reader(), k, 0, k.size())
	if err != nil {
		return storeErr(err)
	}
	pos := -1
	for i, v := range els {
		if bytes.Equal(v, args[3]) {
			pos = i
			break
		}
	}
	if pos < 0 {
		return resp.Int(-1)
	}
	if after {
		pos++
	}
	head, tail := k.m.Head, k.m.Tail
	if pos <= len(els)-pos {
		// Move [0, pos) one step left and put the new element at pos-1.
		for i := 0; i < pos; i++ {
			c.Txn.Put(store.CFSub, lkey(k, head-1+uint64(i)), els[i])
		}
		c.Txn.Put(store.CFSub, lkey(k, head-1+uint64(pos)), args[4])
		k.setBounds(head-1, tail)
	} else {
		// Move [pos, n) one step right.
		c.Txn.Put(store.CFSub, lkey(k, head+uint64(pos)), args[4])
		for i := pos; i < len(els); i++ {
			c.Txn.Put(store.CFSub, lkey(k, head+uint64(i)+1), els[i])
		}
		k.setBounds(head, tail+1)
	}
	k.save(c)
	return resp.Int(k.size())
}

// cmdLpos is lposCommand: RANK picks the n-th match (negative from the
// tail), COUNT returns up to n matches (0 for all) as an array, MAXLEN
// bounds how many elements are compared.
func cmdLpos(c *command.Ctx, args [][]byte) resp.Reply {
	rank, count, maxlen := int64(1), int64(-1), int64(0)
	for j := 3; j < len(args); j++ {
		opt, more := args[j], len(args)-1-j
		switch {
		case equalFold(opt, "rank") && more > 0:
			j++
			var rep resp.Reply
			if rank, rep = parseIntRange(args[j], -math.MaxInt64, math.MaxInt64); rep != nil {
				return rep
			}
			if rank == 0 {
				return resp.Err("ERR RANK can't be zero: use 1 to start from the first match, 2 from the second ... or use negative to start from the end of the list")
			}
		case equalFold(opt, "count") && more > 0:
			j++
			n, ok := resp.ParseInt(args[j])
			if !ok || n < 0 {
				return resp.Err("ERR COUNT can't be negative")
			}
			count = n
		case equalFold(opt, "maxlen") && more > 0:
			j++
			n, ok := resp.ParseInt(args[j])
			if !ok || n < 0 {
				return resp.Err("ERR MAXLEN can't be negative")
			}
			maxlen = n
		default:
			return resp.ErrSyntax
		}
	}
	fromTail := rank < 0
	if fromTail {
		rank = -rank
	}
	k, rep := openColl(c, args[1], layout.TList, false)
	switch {
	case rep != nil:
		return rep
	case !k.exists && count != -1:
		return resp.Array()
	case !k.exists:
		return resp.Null()
	}
	n := k.size()
	it := c.Reader().Iter(store.CFSub, lkey(k, k.m.Head), lkey(k, k.m.Tail), store.IterOpts{FillCache: true, TotalOrderSeek: fromTail})
	defer it.Close()
	if fromTail {
		it.SeekToLast()
	} else {
		it.SeekToFirst()
	}
	var found []resp.Reply
	matches := int64(0)
	for i := int64(0); it.Valid() && (maxlen == 0 || i < maxlen); i++ {
		if bytes.Equal(it.Value(), args[2]) {
			matches++
			if matches >= rank {
				idx := i
				if fromTail {
					idx = n - i - 1
				}
				found = append(found, resp.Int(idx))
				if count == -1 || (count != 0 && matches-rank+1 >= count) {
					break
				}
			}
		}
		step(it, fromTail)
	}
	if err := it.Err(); err != nil {
		return storeErr(err)
	}
	if count != -1 {
		return resp.Array(found...)
	}
	if len(found) == 0 {
		return resp.Null()
	}
	return found[0]
}

func cmdLmove(c *command.Ctx, args [][]byte) resp.Reply {
	fromLeft, ok1 := listSide(args[3])
	toLeft, ok2 := listSide(args[4])
	if !ok1 || !ok2 {
		return resp.ErrSyntax
	}
	return lmoveGeneric(c, args[1], args[2], fromLeft, toLeft)
}

func cmdRpoplpush(c *command.Ctx, args [][]byte) resp.Reply {
	return lmoveGeneric(c, args[1], args[2], false, true)
}

// listSide parses LEFT or RIGHT.
func listSide(b []byte) (left, ok bool) {
	switch {
	case equalFold(b, "left"):
		return true, true
	case equalFold(b, "right"):
		return false, true
	}
	return false, false
}

// lmoveGeneric pops from src and pushes onto dst, which may be the same
// list. An absent source replies a null before dst's type is checked.
func lmoveGeneric(c *command.Ctx, srcKey, dstKey []byte, fromLeft, toLeft bool) resp.Reply {
	src, rep := openColl(c, srcKey, layout.TList, false)
	switch {
	case rep != nil:
		return rep
	case !src.exists:
		return resp.Null()
	}
	dst := src
	if !bytes.Equal(srcKey, dstKey) {
		if dst, rep = openColl(c, dstKey, layout.TList, true); rep != nil {
			return rep
		}
	}
	var els [][]byte
	var err error
	if fromLeft {
		els, err = listRange(c.Reader(), src, 0, 1)
	} else {
		els, err = listRange(c.Reader(), src, src.size()-1, src.size())
	}
	if err != nil {
		return storeErr(err)
	}
	if len(els) == 0 {
		return resp.Null()
	}
	v := els[0]
	if dst == src {
		// The pop deletes the element's key and the push may write the
		// same key again; the later Put in the batch wins.
		if fromLeft {
			c.Txn.Delete(store.CFSub, lkey(src, src.m.Head))
			src.setBounds(src.m.Head+1, src.m.Tail)
		} else {
			c.Txn.Delete(store.CFSub, lkey(src, src.m.Tail-1))
			src.setBounds(src.m.Head, src.m.Tail-1)
		}
	} else {
		popEnds(c, src, 1, fromLeft)
	}
	if toLeft {
		pushLeft(c, dst, v)
	} else {
		pushRight(c, dst, v)
	}
	src.save(c)
	if dst != src {
		dst.save(c)
	}
	return resp.Bulk(v)
}
