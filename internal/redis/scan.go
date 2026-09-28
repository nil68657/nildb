package redis

import (
	"bytes"
	"math"
	"strconv"

	"github.com/nil68657/nildb/internal/command"
	"github.com/nil68657/nildb/internal/layout"
	"github.com/nil68657/nildb/internal/resp"
	"github.com/nil68657/nildb/internal/store"
)

// A SCAN cursor names a position kept in the server's cursor ring (see
// command.Cursors): the last key, field or member examined. Each call
// resumes just after it in key order, so every element present for the
// whole iteration comes back exactly once. A cursor evicted from the ring,
// issued before a restart, for another database or for another SCAN
// family replies ERR invalid cursor.

func registerScan(r *command.Registry) {
	ro := command.ReadOnly
	r.Register(
		command.Spec{Name: "scan", Arity: -2, Flags: ro, Group: "generic", Since: "2.8.0",
			Summary: "Iterates over the key names in the database.", Run: cmdScan},
		command.Spec{Name: "hscan", Arity: -3, Flags: ro, FirstKey: 1, LastKey: 1, KeyStep: 1, Group: "hash", Since: "2.8.0",
			Summary: "Iterates over fields and values of a hash.", Run: cmdHscan},
		command.Spec{Name: "sscan", Arity: -3, Flags: ro, FirstKey: 1, LastKey: 1, KeyStep: 1, Group: "set", Since: "2.8.0",
			Summary: "Iterates over members of a set.", Run: cmdSscan},
		command.Spec{Name: "zscan", Arity: -3, Flags: ro, FirstKey: 1, LastKey: 1, KeyStep: 1, Group: "sorted-set", Since: "2.8.0",
			Summary: "Iterates over members and scores of a sorted set.", Run: cmdZscan},
	)
}

// scanOpts is a parsed SCAN-family option list.
type scanOpts struct {
	count    int64
	pattern  []byte // nil when absent or "*"
	typeName []byte // SCAN TYPE; nil when absent
	novalues bool   // HSCAN NOVALUES
}

// parseScanOpts reads the options from args[i]. TYPE is accepted only by
// SCAN (forKeys) and NOVALUES only by HSCAN (forHash); anything else,
// and a COUNT below 1, is a syntax error.
func parseScanOpts(args [][]byte, i int, forKeys, forHash bool) (scanOpts, resp.Reply) {
	o := scanOpts{count: 10}
	for i < len(args) {
		more := len(args) - i
		switch {
		case equalFold(args[i], "count") && more >= 2:
			n, ok := resp.ParseInt(args[i+1])
			if !ok {
				return o, resp.ErrNotInteger
			}
			if n < 1 {
				return o, resp.ErrSyntax
			}
			o.count = n
			i += 2
		case equalFold(args[i], "match") && more >= 2:
			o.pattern = args[i+1]
			if len(o.pattern) == 1 && o.pattern[0] == '*' {
				o.pattern = nil
			}
			i += 2
		case forKeys && equalFold(args[i], "type") && more >= 2:
			o.typeName = args[i+1]
			i += 2
		case forHash && equalFold(args[i], "novalues"):
			o.novalues = true
			i++
		default:
			return o, resp.ErrSyntax
		}
	}
	return o, nil
}

// limit is the most entries one call examines: COUNT*10, Redis's cap on
// dictScan iterations.
func (o scanOpts) limit() int64 {
	if o.count > math.MaxInt64/10 {
		return math.MaxInt64
	}
	return o.count * 10
}

// parseCursor reads a cursor the way parseScanCursorOrReply does
// (strtoul, base 10): an optional sign, digits, nothing else. The empty
// string reads as 0; leading space, a bare sign and overflow fail. A
// leading '-' negates modulo 2^64, as strtoul does.
func parseCursor(b []byte) (uint64, bool) {
	if len(b) == 0 {
		return 0, true
	}
	neg, i := false, 0
	switch b[0] {
	case '-':
		neg, i = true, 1
	case '+':
		i = 1
	}
	if i == len(b) {
		return 0, false
	}
	var v uint64
	for ; i < len(b); i++ {
		d := b[i]
		if d < '0' || d > '9' {
			return 0, false
		}
		if v > (math.MaxUint64-uint64(d-'0'))/10 {
			return 0, false
		}
		v = v*10 + uint64(d-'0')
	}
	if neg {
		v = -v
	}
	return v, true
}

// scanReply is the two-element SCAN reply: the cursor as a bulk string,
// then the items.
func scanReply(cursor uint64, items [][]byte) resp.Reply {
	return resp.Array(resp.Str(strconv.FormatUint(cursor, 10)), bulkArray(items))
}

// emptyScan is shared.emptyscan: cursor 0 and no items.
func emptyScan() resp.Reply { return scanReply(0, nil) }

// cmdScan walks the database's meta range from the cursor's position,
// skipping expired keys and applying MATCH and TYPE. It stops after COUNT
// keys or COUNT*10 examined entries and replies cursor 0 once the range
// is exhausted. One iterator gives the call a consistent view.
func cmdScan(c *command.Ctx, args [][]byte) resp.Reply {
	cur, ok := parseCursor(args[1])
	if !ok {
		return resp.ErrInvalidCursor
	}
	o, rep := parseScanOpts(args, 2, true, false)
	if rep != nil {
		return rep
	}
	var after []byte
	if cur != 0 {
		pos, ok := c.Cursors.Get(cur, c.DB, command.CursorScan)
		if !ok {
			return resp.ErrInvalidCursor
		}
		after = pos.Key
	}
	lo, hi := dbBounds(c.DB)
	it := c.Reader().Iter(store.CFMeta, lo, hi, store.IterOpts{FillCache: true})
	defer it.Close()
	if after == nil {
		it.SeekToFirst()
	} else {
		it.Seek(append(layout.MetaKey(nil, c.DB, after), 0))
	}
	var out [][]byte
	var last []byte
	now, limit := c.NowMS(), o.limit()
	for n := int64(0); it.Valid() && int64(len(out)) < o.count && n < limit; it.Next() {
		n++
		key, v := it.Key()[1:], it.Value()
		last = append(last[:0], key...)
		if rawExpired(v, now) || (o.pattern != nil && !globMatch(o.pattern, key, false)) {
			continue
		}
		if o.typeName != nil && !equalFold(o.typeName, layout.Type(v[0]&0x0F).String()) {
			continue
		}
		out = append(out, clone(key))
	}
	if err := it.Err(); err != nil {
		return storeErr(err)
	}
	var next uint64
	if it.Valid() {
		next = c.Cursors.Put(command.Position{DB: c.DB, Kind: command.CursorScan, Key: last})
	}
	return scanReply(next, out)
}

func cmdHscan(c *command.Ctx, args [][]byte) resp.Reply {
	return collScan(c, args, layout.THash, command.CursorHScan)
}

func cmdSscan(c *command.Ctx, args [][]byte) resp.Reply {
	return collScan(c, args, layout.TSet, command.CursorSScan)
}

func cmdZscan(c *command.Ctx, args [][]byte) resp.Reply {
	return collScan(c, args, layout.TZSet, command.CursorZScan)
}

// collScan is HSCAN, SSCAN and ZSCAN over the collection's sub-keys,
// which sort by field or member. The checks run in hscanCommand's order:
// cursor syntax, key (absent replies an empty scan), type, options, and
// only then the ring lookup. A cursor issued for an older version of the
// key (it was deleted and recreated since) replies an empty scan. ZSCAN
// prints scores with d2string, as Redis does for small sorted sets.
func collScan(c *command.Ctx, args [][]byte, t layout.Type, kind command.CursorKind) resp.Reply {
	key := args[1]
	cur, ok := parseCursor(args[2])
	if !ok {
		return resp.ErrInvalidCursor
	}
	k, rep := openColl(c, key, t, false)
	if rep != nil {
		return rep
	}
	if !k.exists {
		return emptyScan()
	}
	o, rep := parseScanOpts(args, 3, false, t == layout.THash)
	if rep != nil {
		return rep
	}
	ver := k.m.Version
	var after []byte
	if cur != 0 {
		pos, ok := c.Cursors.Get(cur, c.DB, kind)
		if !ok || !bytes.Equal(pos.Key, key) {
			return resp.ErrInvalidCursor
		}
		if pos.Version != ver {
			return emptyScan()
		}
		after = pos.Sub
	}
	it := iterColl(c.Reader(), store.CFSub, ver, false)
	defer it.Close()
	if after == nil {
		it.SeekToFirst()
	} else {
		it.Seek(append(layout.SubKey(nil, ver, after), 0))
	}
	var out [][]byte
	var last []byte
	got, limit := int64(0), o.limit()
	for n := int64(0); it.Valid() && got < o.count && n < limit; it.Next() {
		n++
		sub := it.Key()[8:]
		last = append(last[:0], sub...)
		if o.pattern != nil && !globMatch(o.pattern, sub, false) {
			continue
		}
		got++
		out = append(out, clone(sub))
		switch {
		case t == layout.THash && !o.novalues:
			out = append(out, clone(it.Value()))
		case t == layout.TZSet:
			score, err := layout.DecodeScoreValue(it.Value())
			if err != nil {
				return storeErr(err)
			}
			out = append(out, []byte(resp.FormatFloat(score)))
		}
	}
	if err := it.Err(); err != nil {
		return storeErr(err)
	}
	var next uint64
	if it.Valid() {
		next = c.Cursors.Put(command.Position{DB: c.DB, Kind: kind, Version: ver, Key: key, Sub: last})
	}
	return scanReply(next, out)
}
