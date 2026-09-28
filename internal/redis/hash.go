package redis

import (
	"math"
	"math/big"
	"strconv"

	"github.com/nil68657/nildb/internal/command"
	"github.com/nil68657/nildb/internal/layout"
	"github.com/nil68657/nildb/internal/resp"
	"github.com/nil68657/nildb/internal/store"
)

// A hash keeps each field in sub under | version | field | with the value
// as the RocksDB value; Size in the meta counts the fields, and deleting
// the last field deletes the key.

func init() { groups = append(groups, registerHash) }

func registerHash(r *command.Registry) {
	const g = "hash"
	w, ro, f := command.Write, command.ReadOnly, command.Fast
	r.Register(
		command.Spec{Name: "hset", Arity: -4, Flags: w | f, FirstKey: 1, LastKey: 1, KeyStep: 1, Group: g, Since: "2.0.0",
			Summary: "Creates or modifies the value of a field in a hash.", Run: cmdHset},
		command.Spec{Name: "hsetnx", Arity: 4, Flags: w | f, FirstKey: 1, LastKey: 1, KeyStep: 1, Group: g, Since: "2.0.0",
			Summary: "Sets the value of a field in a hash only when the field doesn't exist.", Run: cmdHsetnx},
		command.Spec{Name: "hget", Arity: 3, Flags: ro | f, FirstKey: 1, LastKey: 1, KeyStep: 1, Group: g, Since: "2.0.0",
			Summary: "Returns the value of a field in a hash.", Run: cmdHget},
		command.Spec{Name: "hmget", Arity: -3, Flags: ro | f, FirstKey: 1, LastKey: 1, KeyStep: 1, Group: g, Since: "2.0.0",
			Summary: "Returns the values of all fields in a hash.", Run: cmdHmget},
		command.Spec{Name: "hmset", Arity: -4, Flags: w | f, FirstKey: 1, LastKey: 1, KeyStep: 1, Group: g, Since: "2.0.0",
			Summary: "Sets the values of multiple fields.", Run: cmdHmset},
		command.Spec{Name: "hdel", Arity: -3, Flags: w | f, FirstKey: 1, LastKey: 1, KeyStep: 1, Group: g, Since: "2.0.0",
			Summary: "Deletes one or more fields and their values from a hash. Deletes the hash if no fields remain.", Run: cmdHdel},
		command.Spec{Name: "hlen", Arity: 2, Flags: ro | f, FirstKey: 1, LastKey: 1, KeyStep: 1, Group: g, Since: "2.0.0",
			Summary: "Returns the number of fields in a hash.", Run: cmdHlen},
		command.Spec{Name: "hexists", Arity: 3, Flags: ro | f, FirstKey: 1, LastKey: 1, KeyStep: 1, Group: g, Since: "2.0.0",
			Summary: "Determines whether a field exists in a hash.", Run: cmdHexists},
		command.Spec{Name: "hstrlen", Arity: 3, Flags: ro | f, FirstKey: 1, LastKey: 1, KeyStep: 1, Group: g, Since: "3.2.0",
			Summary: "Returns the length of the value of a field.", Run: cmdHstrlen},
		command.Spec{Name: "hkeys", Arity: 2, Flags: ro, FirstKey: 1, LastKey: 1, KeyStep: 1, Group: g, Since: "2.0.0",
			Summary: "Returns all fields in a hash.", Run: cmdHkeys},
		command.Spec{Name: "hvals", Arity: 2, Flags: ro, FirstKey: 1, LastKey: 1, KeyStep: 1, Group: g, Since: "2.0.0",
			Summary: "Returns all values in a hash.", Run: cmdHvals},
		command.Spec{Name: "hgetall", Arity: 2, Flags: ro, FirstKey: 1, LastKey: 1, KeyStep: 1, Group: g, Since: "2.0.0",
			Summary: "Returns all fields and values in a hash.", Run: cmdHgetall},
		command.Spec{Name: "hincrby", Arity: 4, Flags: w | f, FirstKey: 1, LastKey: 1, KeyStep: 1, Group: g, Since: "2.0.0",
			Summary: "Increments the integer value of a field in a hash by a number. Uses 0 as initial value if the field doesn't exist.", Run: cmdHincrby},
		command.Spec{Name: "hincrbyfloat", Arity: 4, Flags: w | f, FirstKey: 1, LastKey: 1, KeyStep: 1, Group: g, Since: "2.6.0",
			Summary: "Increments the floating point value of a field by a number. Uses 0 as initial value if the field doesn't exist.", Run: cmdHincrbyfloat},
		command.Spec{Name: "hrandfield", Arity: -2, Flags: ro, FirstKey: 1, LastKey: 1, KeyStep: 1, Group: g, Since: "6.2.0",
			Summary: "Returns one or more random fields from a hash.", Run: cmdHrandfield},
	)
}

func cmdHset(c *command.Ctx, args [][]byte) resp.Reply  { return hsetGeneric(c, args, false) }
func cmdHmset(c *command.Ctx, args [][]byte) resp.Reply { return hsetGeneric(c, args, true) }

// hsetGeneric is HSET (replying the number of new fields) and HMSET
// (replying +OK). A field named twice counts once; its last value wins.
func hsetGeneric(c *command.Ctx, args [][]byte, hmset bool) resp.Reply {
	if len(args)%2 == 1 {
		return resp.ErrArity(c.Spec.FullName())
	}
	k, rep := openColl(c, args[1], layout.THash, true)
	if rep != nil {
		return rep
	}
	fields := make([][]byte, 0, (len(args)-2)/2)
	for i := 2; i < len(args); i += 2 {
		fields = append(fields, args[i])
	}
	fields = distinct(fields)
	created := int64(len(fields))
	if k.exists {
		vals, err := subValues(c.Reader(), k.m.Version, fields)
		if err != nil {
			return storeErr(err)
		}
		for _, v := range vals {
			if v != nil {
				created--
			}
		}
	}
	for i := 2; i < len(args); i += 2 {
		c.Txn.Put(store.CFSub, layout.SubKey(nil, k.m.Version, args[i]), args[i+1])
	}
	k.grow(created)
	k.save(c)
	if hmset {
		return resp.OK()
	}
	return resp.Int(created)
}

func cmdHsetnx(c *command.Ctx, args [][]byte) resp.Reply {
	k, rep := openColl(c, args[1], layout.THash, true)
	if rep != nil {
		return rep
	}
	sk := layout.SubKey(nil, k.m.Version, args[2])
	if k.exists {
		_, ok, err := c.Reader().Get(store.CFSub, sk)
		if err != nil {
			return storeErr(err)
		}
		if ok {
			return zero
		}
	}
	c.Txn.Put(store.CFSub, sk, args[3])
	k.grow(1)
	k.save(c)
	return one
}

// hashField reads one field: the hash, the value and whether it is there.
func hashField(c *command.Ctx, key, field []byte) (*coll, []byte, bool, resp.Reply) {
	k, rep := openColl(c, key, layout.THash, false)
	if rep != nil || !k.exists {
		return k, nil, false, rep
	}
	v, ok, err := c.Reader().Get(store.CFSub, layout.SubKey(nil, k.m.Version, field))
	if err != nil {
		return k, nil, false, storeErr(err)
	}
	return k, v, ok, nil
}

func cmdHget(c *command.Ctx, args [][]byte) resp.Reply {
	_, v, ok, rep := hashField(c, args[1], args[2])
	switch {
	case rep != nil:
		return rep
	case !ok:
		return resp.Null()
	}
	return resp.Bulk(v)
}

func cmdHexists(c *command.Ctx, args [][]byte) resp.Reply {
	_, _, ok, rep := hashField(c, args[1], args[2])
	switch {
	case rep != nil:
		return rep
	case !ok:
		return zero
	}
	return one
}

func cmdHstrlen(c *command.Ctx, args [][]byte) resp.Reply {
	_, v, _, rep := hashField(c, args[1], args[2])
	if rep != nil {
		return rep
	}
	return resp.Int(int64(len(v)))
}

// cmdHmget replies a null for every field of an absent hash, as Redis
// treats a missing key as an empty hash here.
func cmdHmget(c *command.Ctx, args [][]byte) resp.Reply {
	k, rep := openColl(c, args[1], layout.THash, false)
	if rep != nil {
		return rep
	}
	out := make([][]byte, len(args)-2)
	if k.exists {
		vals, err := subValues(c.Reader(), k.m.Version, args[2:])
		if err != nil {
			return storeErr(err)
		}
		out = vals
	}
	return bulkArray(out)
}

func cmdHdel(c *command.Ctx, args [][]byte) resp.Reply {
	k, rep := openColl(c, args[1], layout.THash, false)
	if rep != nil {
		return rep
	}
	if !k.exists {
		return zero
	}
	fields := distinct(args[2:])
	vals, err := subValues(c.Reader(), k.m.Version, fields)
	if err != nil {
		return storeErr(err)
	}
	var gone [][]byte
	for i, v := range vals {
		if v != nil {
			gone = append(gone, fields[i])
		}
	}
	k.grow(-int64(len(gone)))
	if k.m.Size > 0 {
		for _, f := range gone {
			c.Txn.Delete(store.CFSub, layout.SubKey(nil, k.m.Version, f))
		}
	}
	k.save(c)
	return resp.Int(int64(len(gone)))
}

func cmdHlen(c *command.Ctx, args [][]byte) resp.Reply {
	k, rep := openColl(c, args[1], layout.THash, false)
	if rep != nil {
		return rep
	}
	return resp.Int(k.m.Size)
}

func cmdHkeys(c *command.Ctx, args [][]byte) resp.Reply   { return hashAll(c, args[1], true, false) }
func cmdHvals(c *command.Ctx, args [][]byte) resp.Reply   { return hashAll(c, args[1], false, true) }
func cmdHgetall(c *command.Ctx, args [][]byte) resp.Reply { return hashAll(c, args[1], true, true) }

// hashAll is genericHgetallCommand: HGETALL replies a map (a flat array
// in RESP2), HKEYS and HVALS an array.
func hashAll(c *command.Ctx, key []byte, fields, values bool) resp.Reply {
	k, rep := openColl(c, key, layout.THash, false)
	if rep != nil {
		return rep
	}
	var out [][]byte
	if k.exists {
		it := iterColl(c.Reader(), store.CFSub, k.m.Version, false)
		for it.SeekToFirst(); it.Valid(); it.Next() {
			if fields {
				out = append(out, clone(it.Key()[8:]))
			}
			if values {
				out = append(out, clone(it.Value()))
			}
		}
		err := it.Err()
		it.Close()
		if err != nil {
			return storeErr(err)
		}
	}
	if fields && values {
		return bulkMap(out)
	}
	return bulkArray(out)
}

// cmdHincrby is hincrbyCommand: the increment is parsed first, then the
// field; a value that is not an integer replies "hash value is not an
// integer".
func cmdHincrby(c *command.Ctx, args [][]byte) resp.Reply {
	incr, ok := resp.ParseInt(args[3])
	if !ok {
		return resp.ErrNotInteger
	}
	k, v, found, rep := hashFieldForWrite(c, args[1], args[2])
	if rep != nil {
		return rep
	}
	var value int64
	if found {
		n, ok := resp.ParseInt(v)
		if !ok {
			return resp.Err("ERR hash value is not an integer")
		}
		value = n
	}
	if (incr < 0 && value < 0 && incr < math.MinInt64-value) ||
		(incr > 0 && value > 0 && incr > math.MaxInt64-value) {
		return resp.ErrOverflow
	}
	value += incr
	c.Txn.Put(store.CFSub, layout.SubKey(nil, k.m.Version, args[2]), strconv.AppendInt(nil, value, 10))
	if !found {
		k.grow(1)
	}
	k.save(c)
	return resp.Int(value)
}

// cmdHincrbyfloat is hincrbyfloatCommand, with the long double arithmetic
// of incrByFloat. Its errors differ from INCRBYFLOAT's: an infinite
// increment replies "value is NaN or Infinity" before the key is read,
// and a stored value that is not a number replies "hash value is not a
// float".
func cmdHincrbyfloat(c *command.Ctx, args [][]byte) resp.Reply {
	by, ok := parseLongDouble(args[3])
	if !ok {
		return resp.ErrNotFloat
	}
	if by.IsInf() {
		return resp.Err("ERR value is NaN or Infinity")
	}
	k, v, found, rep := hashFieldForWrite(c, args[1], args[2])
	if rep != nil {
		return rep
	}
	value := new(big.Float).SetPrec(ldPrec)
	if found {
		if value, ok = parseLongDouble(v); !ok {
			return resp.Err("ERR hash value is not a float")
		}
	}
	s, rep := addLongDouble(value, by)
	if rep != nil {
		return rep
	}
	c.Txn.Put(store.CFSub, layout.SubKey(nil, k.m.Version, args[2]), []byte(s))
	if !found {
		k.grow(1)
	}
	k.save(c)
	return resp.Str(s)
}

// hashFieldForWrite opens the hash for a write (creating it when absent)
// and reads one field.
func hashFieldForWrite(c *command.Ctx, key, field []byte) (*coll, []byte, bool, resp.Reply) {
	k, rep := openColl(c, key, layout.THash, true)
	if rep != nil {
		return nil, nil, false, rep
	}
	if !k.exists {
		return k, nil, false, nil
	}
	v, ok, err := c.Reader().Get(store.CFSub, layout.SubKey(nil, k.m.Version, field))
	if err != nil {
		return nil, nil, false, storeErr(err)
	}
	return k, v, ok, nil
}

// cmdHrandfield is hrandfieldCommand. Without a count it replies one
// field or a null; with a count see randWithCount. The count range and
// WITHVALUES checks come before the key is read.
func cmdHrandfield(c *command.Ctx, args [][]byte) resp.Reply {
	if len(args) >= 3 {
		l, rep := parseIntRange(args[2], -math.MaxInt64, math.MaxInt64)
		if rep != nil {
			return rep
		}
		withValues := false
		if len(args) > 4 || (len(args) == 4 && !equalFold(args[3], "withvalues")) {
			return resp.ErrSyntax
		}
		if len(args) == 4 {
			withValues = true
			if l < -math.MaxInt64/2 || l > math.MaxInt64/2 {
				return resp.Err("ERR value is out of range")
			}
		}
		return randWithCount(c, args[1], layout.THash, l, withValues)
	}
	return randOne(c, args[1], layout.THash)
}

// randOne replies one random field or member, or a null for an absent
// key.
func randOne(c *command.Ctx, key []byte, t layout.Type) resp.Reply {
	k, rep := openColl(c, key, t, false)
	if rep != nil {
		return rep
	}
	if !k.exists {
		return resp.Null()
	}
	els, err := sample(c.Reader(), k.m.Version, k.m.Size, 1, true)
	if err != nil {
		return storeErr(err)
	}
	if len(els) == 0 {
		return resp.Null()
	}
	return resp.Bulk(els[0].sub)
}

// randWithCount serves HRANDFIELD and ZRANDMEMBER with a count: a
// positive count returns distinct elements (all of them when count
// exceeds the size), a negative one returns -count elements with repeats.
// WITHVALUES/WITHSCORES pairs are a flat array in RESP2 and an array of
// two-element arrays in RESP3; ZRANDMEMBER's scores are doubles.
func randWithCount(c *command.Ctx, key []byte, t layout.Type, l int64, withValues bool) resp.Reply {
	k, rep := openColl(c, key, t, false)
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
	scores := t == layout.TZSet
	vals := make([]float64, len(els))
	if scores && withValues {
		for i, e := range els {
			if vals[i], err = layout.DecodeScoreValue(e.val); err != nil {
				return storeErr(err)
			}
		}
	}
	return resp.Stream(func(w *resp.Writer) {
		switch {
		case !withValues:
			w.ArrayHeader(len(els))
			for _, e := range els {
				w.Bulk(e.sub)
			}
		case w.Proto() == 3:
			w.ArrayHeader(len(els))
			for i, e := range els {
				w.ArrayHeader(2)
				w.Bulk(e.sub)
				if scores {
					w.Double(vals[i])
				} else {
					w.Bulk(e.val)
				}
			}
		default:
			w.ArrayHeader(2 * len(els))
			for i, e := range els {
				w.Bulk(e.sub)
				if scores {
					w.Double(vals[i])
				} else {
					w.Bulk(e.val)
				}
			}
		}
	})
}
