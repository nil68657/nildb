package redis

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"slices"
	"strconv"

	"github.com/nil68657/nildb/internal/command"
	"github.com/nil68657/nildb/internal/layout"
	"github.com/nil68657/nildb/internal/resp"
	"github.com/nil68657/nildb/internal/store"
)

// A sorted set keeps two entries per member (architecture.md section 3):
// sub | version | member | -> dbl(score) for lookups by member, and
// zscore | version | dbl(score) | member | for order by score. Scores are
// stored with -0 turned into 0, so ZSCORE of a member added with -0
// replies 0 where Redis replies -0. Rank lookups (ZRANK, ZRANGE by index)
// walk the score index and cost O(rank).

func init() { groups = append(groups, registerZset) }

func registerZset(r *command.Registry) {
	const g = "sorted-set"
	w, ro, f, mr := command.Write, command.ReadOnly, command.Fast, command.MultiRead
	dstOnly := command.KeysFirstLastStep(1, 1, 1)
	r.Register(
		command.Spec{Name: "zadd", Arity: -4, Flags: w | f, FirstKey: 1, LastKey: 1, KeyStep: 1, Group: g, Since: "1.2.0",
			Summary: "Adds one or more members to a sorted set, or updates their scores. Creates the key if it doesn't exist.", Run: cmdZadd},
		command.Spec{Name: "zincrby", Arity: 4, Flags: w | f, FirstKey: 1, LastKey: 1, KeyStep: 1, Group: g, Since: "1.2.0",
			Summary: "Increments the score of a member in a sorted set.", Run: cmdZincrby},
		command.Spec{Name: "zrem", Arity: -3, Flags: w | f, FirstKey: 1, LastKey: 1, KeyStep: 1, Group: g, Since: "1.2.0",
			Summary: "Removes one or more members from a sorted set. Deletes the sorted set if all members were removed.", Run: cmdZrem},
		command.Spec{Name: "zscore", Arity: 3, Flags: ro | f, FirstKey: 1, LastKey: 1, KeyStep: 1, Group: g, Since: "1.2.0",
			Summary: "Returns the score of a member in a sorted set.", Run: cmdZscore},
		command.Spec{Name: "zmscore", Arity: -3, Flags: ro | f, FirstKey: 1, LastKey: 1, KeyStep: 1, Group: g, Since: "6.2.0",
			Summary: "Returns the score of one or more members in a sorted set.", Run: cmdZmscore},
		command.Spec{Name: "zcard", Arity: 2, Flags: ro | f, FirstKey: 1, LastKey: 1, KeyStep: 1, Group: g, Since: "1.2.0",
			Summary: "Returns the number of members in a sorted set.", Run: cmdZcard},
		command.Spec{Name: "zcount", Arity: 4, Flags: ro | f, FirstKey: 1, LastKey: 1, KeyStep: 1, Group: g, Since: "2.0.0",
			Summary: "Returns the count of members in a sorted set that have scores within a range.", Run: cmdZcount},
		command.Spec{Name: "zlexcount", Arity: 4, Flags: ro | f, FirstKey: 1, LastKey: 1, KeyStep: 1, Group: g, Since: "2.8.9",
			Summary: "Returns the number of members in a sorted set within a lexicographical range.", Run: cmdZlexcount},
		command.Spec{Name: "zrange", Arity: -4, Flags: ro, FirstKey: 1, LastKey: 1, KeyStep: 1, Group: g, Since: "1.2.0",
			Summary: "Returns members in a sorted set within a range of indexes.", Run: cmdZrange},
		command.Spec{Name: "zrangestore", Arity: -5, Flags: w | mr, FirstKey: 1, LastKey: 2, KeyStep: 1, Keys: dstOnly, Group: g, Since: "6.2.0",
			Summary: "Stores a range of members from sorted set in a key.", Run: cmdZrangestore},
		command.Spec{Name: "zrevrange", Arity: -4, Flags: ro, FirstKey: 1, LastKey: 1, KeyStep: 1, Group: g, Since: "1.2.0",
			Summary: "Returns members in a sorted set within a range of indexes in reverse order.", Run: cmdZrevrange},
		command.Spec{Name: "zrangebyscore", Arity: -4, Flags: ro, FirstKey: 1, LastKey: 1, KeyStep: 1, Group: g, Since: "1.0.5",
			Summary: "Returns members in a sorted set within a range of scores.", Run: cmdZrangebyscore},
		command.Spec{Name: "zrevrangebyscore", Arity: -4, Flags: ro, FirstKey: 1, LastKey: 1, KeyStep: 1, Group: g, Since: "2.2.0",
			Summary: "Returns members in a sorted set within a range of scores in reverse order.", Run: cmdZrevrangebyscore},
		command.Spec{Name: "zrangebylex", Arity: -4, Flags: ro, FirstKey: 1, LastKey: 1, KeyStep: 1, Group: g, Since: "2.8.9",
			Summary: "Returns members in a sorted set within a lexicographical range.", Run: cmdZrangebylex},
		command.Spec{Name: "zrevrangebylex", Arity: -4, Flags: ro, FirstKey: 1, LastKey: 1, KeyStep: 1, Group: g, Since: "2.8.9",
			Summary: "Returns members in a sorted set within a lexicographical range in reverse order.", Run: cmdZrevrangebylex},
		command.Spec{Name: "zrank", Arity: -3, Flags: ro | f, FirstKey: 1, LastKey: 1, KeyStep: 1, Group: g, Since: "2.0.0",
			Summary: "Returns the index of a member in a sorted set ordered by ascending scores.", Run: cmdZrank},
		command.Spec{Name: "zrevrank", Arity: -3, Flags: ro | f, FirstKey: 1, LastKey: 1, KeyStep: 1, Group: g, Since: "2.0.0",
			Summary: "Returns the index of a member in a sorted set ordered by descending scores.", Run: cmdZrevrank},
		command.Spec{Name: "zpopmin", Arity: -2, Flags: w | f, FirstKey: 1, LastKey: 1, KeyStep: 1, Group: g, Since: "5.0.0",
			Summary: "Returns the lowest-scoring members from a sorted set after removing them. Deletes the sorted set if the last member was popped.", Run: cmdZpopmin},
		command.Spec{Name: "zpopmax", Arity: -2, Flags: w | f, FirstKey: 1, LastKey: 1, KeyStep: 1, Group: g, Since: "5.0.0",
			Summary: "Returns the highest-scoring members from a sorted set after removing them. Deletes the sorted set if the last member was popped.", Run: cmdZpopmax},
		command.Spec{Name: "zrandmember", Arity: -2, Flags: ro, FirstKey: 1, LastKey: 1, KeyStep: 1, Group: g, Since: "6.2.0",
			Summary: "Returns one or more random members from a sorted set.", Run: cmdZrandmember},
		command.Spec{Name: "zremrangebyrank", Arity: 4, Flags: w, FirstKey: 1, LastKey: 1, KeyStep: 1, Group: g, Since: "2.0.0",
			Summary: "Removes members in a sorted set within a range of indexes. Deletes the sorted set if all members were removed.", Run: cmdZremrangebyrank},
		command.Spec{Name: "zremrangebyscore", Arity: 4, Flags: w, FirstKey: 1, LastKey: 1, KeyStep: 1, Group: g, Since: "1.2.0",
			Summary: "Removes members in a sorted set within a range of scores. Deletes the sorted set if all members were removed.", Run: cmdZremrangebyscore},
		command.Spec{Name: "zremrangebylex", Arity: 4, Flags: w, FirstKey: 1, LastKey: 1, KeyStep: 1, Group: g, Since: "2.8.9",
			Summary: "Removes members in a sorted set within a lexicographical range. Deletes the sorted set if all members were removed.", Run: cmdZremrangebylex},
		command.Spec{Name: "zunionstore", Arity: -4, Flags: w | mr, FirstKey: 1, LastKey: 1, KeyStep: 1, Keys: dstOnly, Group: g, Since: "2.0.0",
			Summary: "Stores the union of multiple sorted sets in a key.", Run: cmdZunionstore},
		command.Spec{Name: "zinterstore", Arity: -4, Flags: w | mr, FirstKey: 1, LastKey: 1, KeyStep: 1, Keys: dstOnly, Group: g, Since: "2.0.0",
			Summary: "Stores the intersect of multiple sorted sets in a key.", Run: cmdZinterstore},
		command.Spec{Name: "zdiffstore", Arity: -4, Flags: w | mr, FirstKey: 1, LastKey: 1, KeyStep: 1, Keys: dstOnly, Group: g, Since: "6.2.0",
			Summary: "Stores the difference of multiple sorted sets in a key.", Run: cmdZdiffstore},
		command.Spec{Name: "zunion", Arity: -3, Flags: ro | mr, Group: g, Since: "6.2.0",
			Summary: "Returns the union of multiple sorted sets.", Run: cmdZunion},
		command.Spec{Name: "zinter", Arity: -3, Flags: ro | mr, Group: g, Since: "6.2.0",
			Summary: "Returns the intersect of multiple sorted sets.", Run: cmdZinter},
		command.Spec{Name: "zdiff", Arity: -3, Flags: ro | mr, Group: g, Since: "6.2.0",
			Summary: "Returns the difference between multiple sorted sets.", Run: cmdZdiff},
		command.Spec{Name: "zintercard", Arity: -3, Flags: ro | mr, Group: g, Since: "7.0.0",
			Summary: "Returns the number of members of the intersect of multiple sorted sets.", Run: cmdZintercard},
	)
}

// zentry is one sorted-set member with its score.
type zentry struct {
	member []byte
	score  float64
}

// zsetReply replies members, with their scores when withScores is set: a
// flat array in RESP2, [member, score] pairs in RESP3 (zrangeResultBegin).
func zsetReply(es []zentry, withScores bool) resp.Reply {
	return resp.Stream(func(w *resp.Writer) {
		switch {
		case !withScores:
			w.ArrayHeader(len(es))
			for _, e := range es {
				w.Bulk(e.member)
			}
		case w.Proto() == 3:
			w.ArrayHeader(len(es))
			for _, e := range es {
				w.ArrayHeader(2)
				w.Bulk(e.member)
				w.Double(e.score)
			}
		default:
			w.ArrayHeader(2 * len(es))
			for _, e := range es {
				w.Bulk(e.member)
				w.Double(e.score)
			}
		}
	})
}

// zput writes member with score; old is its current score when it is
// already a member, whose index entry is then replaced.
func zput(c *command.Ctx, ver uint64, member []byte, score float64, old *float64) {
	if old != nil {
		if *old == score {
			return
		}
		c.Txn.Delete(store.CFZScore, layout.ScoreKey(nil, ver, *old, member))
	}
	c.Txn.Put(store.CFSub, layout.SubKey(nil, ver, member), layout.ScoreValue(nil, score))
	c.Txn.Put(store.CFZScore, layout.ScoreKey(nil, ver, score, member), nil)
}

// zremove drops entries from a sorted set and its count; when they were
// the last members the key goes (in save) and nothing else is written.
func zremove(c *command.Ctx, k *coll, es []zentry) {
	k.grow(-int64(len(es)))
	if k.m.Size > 0 {
		for _, e := range es {
			c.Txn.Delete(store.CFSub, layout.SubKey(nil, k.m.Version, e.member))
			c.Txn.Delete(store.CFZScore, layout.ScoreKey(nil, k.m.Version, e.score, e.member))
		}
	}
}

// zscores reads the score of each member, nil where absent.
func zscores(r store.Reader, ver uint64, members [][]byte) ([]*float64, error) {
	vals, err := subValues(r, ver, members)
	if err != nil {
		return nil, err
	}
	out := make([]*float64, len(vals))
	for i, v := range vals {
		if v == nil {
			continue
		}
		s, err := layout.DecodeScoreValue(v)
		if err != nil {
			return nil, err
		}
		out[i] = &s
	}
	return out, nil
}

// Error replies of the sorted-set commands.
var (
	errNotFloatRange = resp.Err("ERR min or max is not a float")
	errNotLexRange   = resp.Err("ERR min or max not valid string range item")
	errZaddNaN       = resp.Err("ERR resulting score is not a number (NaN)")
)

// zaddFlags are ZADD's options.
type zaddFlags struct{ nx, xx, gt, lt, ch, incr bool }

// cmdZadd is zaddGenericCommand: options, then shape checks, then every
// score, then the key; nothing is written when any of these fail.
func cmdZadd(c *command.Ctx, args [][]byte) resp.Reply {
	var f zaddFlags
	i := 2
scan:
	for ; i < len(args); i++ {
		switch opt := args[i]; {
		case equalFold(opt, "nx"):
			f.nx = true
		case equalFold(opt, "xx"):
			f.xx = true
		case equalFold(opt, "ch"):
			f.ch = true
		case equalFold(opt, "incr"):
			f.incr = true
		case equalFold(opt, "gt"):
			f.gt = true
		case equalFold(opt, "lt"):
			f.lt = true
		default:
			break scan
		}
	}
	n := len(args) - i
	if n%2 != 0 || n == 0 {
		return resp.ErrSyntax
	}
	if rep := checkZaddFlags(f, n/2); rep != nil {
		return rep
	}
	scores := make([]float64, n/2)
	members := make([][]byte, n/2)
	for j := range scores {
		s, ok := resp.ParseFloat(args[i+2*j])
		if !ok {
			return resp.ErrNotFloat
		}
		scores[j], members[j] = s, args[i+2*j+1]
	}
	return zaddApply(c, args[1], f, scores, members)
}

// checkZaddFlags applies ZADD's option compatibility rules.
func checkZaddFlags(f zaddFlags, elements int) resp.Reply {
	switch {
	case f.nx && f.xx:
		return resp.Err("ERR XX and NX options at the same time are not compatible")
	case (f.gt && f.nx) || (f.lt && f.nx) || (f.gt && f.lt):
		return resp.Err("ERR GT, LT, and/or NX options at the same time are not compatible")
	case f.incr && elements > 1:
		return resp.Err("ERR INCR option supports a single increment-element pair")
	}
	return nil
}

func cmdZincrby(c *command.Ctx, args [][]byte) resp.Reply {
	s, ok := resp.ParseFloat(args[2])
	if !ok {
		return resp.ErrNotFloat
	}
	return zaddApply(c, args[1], zaddFlags{incr: true}, []float64{s}, [][]byte{args[3]})
}

// zaddApply adds or updates members with zsetAdd's rules per element. A
// member named twice sees the score the first occurrence gave it. The
// reply is the added count (added plus updated with CH), or with INCR the
// new score, or a null when INCR changed nothing.
func zaddApply(c *command.Ctx, key []byte, f zaddFlags, scores []float64, members [][]byte) resp.Reply {
	k, rep := openColl(c, key, layout.TZSet, !f.xx)
	if rep != nil {
		return rep
	}
	if !k.exists && f.xx {
		if f.incr {
			return resp.Null()
		}
		return zero
	}
	orig := map[string]*float64{}
	if k.exists {
		uniq := distinct(members)
		cur, err := zscores(c.Reader(), k.m.Version, uniq)
		if err != nil {
			return storeErr(err)
		}
		for i, m := range uniq {
			orig[string(m)] = cur[i]
		}
	}
	next := map[string]float64{}
	var order [][]byte
	var added, updated, processed int64
	var last float64
	for j, member := range members {
		score := scores[j]
		old, had := next[string(member)]
		if !had {
			if p := orig[string(member)]; p != nil {
				old, had = *p, true
			}
		}
		switch {
		case had:
			if f.nx {
				continue
			}
			if f.incr {
				score += old
				if math.IsNaN(score) {
					return errZaddNaN
				}
			}
			if (f.lt && score >= old) || (f.gt && score <= old) {
				continue
			}
			processed++
			last = score
			if score != old {
				updated++
				if _, seen := next[string(member)]; !seen {
					order = append(order, member)
				}
				next[string(member)] = score
			}
		case !f.xx:
			processed++
			added++
			last = score
			order = append(order, member)
			next[string(member)] = score
		}
	}
	for _, m := range order {
		zput(c, k.m.Version, m, next[string(m)], orig[string(m)])
	}
	k.grow(added)
	k.save(c)
	switch {
	case f.incr && processed == 0:
		return resp.Null()
	case f.incr:
		return resp.Double(last + 0) // -0 + 0 is 0, the score stored
	case f.ch:
		return resp.Int(added + updated)
	}
	return resp.Int(added)
}

func cmdZrem(c *command.Ctx, args [][]byte) resp.Reply {
	k, rep := openColl(c, args[1], layout.TZSet, false)
	if rep != nil || !k.exists {
		return orZero(rep)
	}
	members := distinct(args[2:])
	cur, err := zscores(c.Reader(), k.m.Version, members)
	if err != nil {
		return storeErr(err)
	}
	var gone []zentry
	for i, s := range cur {
		if s != nil {
			gone = append(gone, zentry{members[i], *s})
		}
	}
	zremove(c, k, gone)
	k.save(c)
	return resp.Int(int64(len(gone)))
}

func cmdZscore(c *command.Ctx, args [][]byte) resp.Reply {
	k, rep := openColl(c, args[1], layout.TZSet, false)
	switch {
	case rep != nil:
		return rep
	case !k.exists:
		return resp.Null()
	}
	cur, err := zscores(c.Reader(), k.m.Version, args[2:3])
	switch {
	case err != nil:
		return storeErr(err)
	case cur[0] == nil:
		return resp.Null()
	}
	return resp.Double(*cur[0])
}

// cmdZmscore treats an absent key as an empty sorted set.
func cmdZmscore(c *command.Ctx, args [][]byte) resp.Reply {
	k, rep := openColl(c, args[1], layout.TZSet, false)
	if rep != nil {
		return rep
	}
	out := make([]resp.Reply, len(args)-2)
	for i := range out {
		out[i] = resp.Null()
	}
	if k.exists {
		cur, err := zscores(c.Reader(), k.m.Version, args[2:])
		if err != nil {
			return storeErr(err)
		}
		for i, s := range cur {
			if s != nil {
				out[i] = resp.Double(*s)
			}
		}
	}
	return resp.Array(out...)
}

func cmdZcard(c *command.Ctx, args [][]byte) resp.Reply {
	k, rep := openColl(c, args[1], layout.TZSet, false)
	if rep != nil {
		return rep
	}
	return resp.Int(k.m.Size)
}

// scoreRange is a parsed ZRANGEBYSCORE interval.
type scoreRange struct {
	min, max     float64
	minex, maxex bool
}

// parseScoreRange is zslParseRange: "(" makes a bound exclusive and the
// rest is read with strtod, which skips leading space, reads "" as 0 and
// turns overflow into infinity. NaN fails.
func parseScoreRange(minArg, maxArg []byte) (scoreRange, bool) {
	var r scoreRange
	var ok bool
	if r.min, r.minex, ok = parseScoreBound(minArg); !ok {
		return r, false
	}
	if r.max, r.maxex, ok = parseScoreBound(maxArg); !ok {
		return r, false
	}
	return r, true
}

func parseScoreBound(b []byte) (float64, bool, bool) {
	ex := len(b) > 0 && b[0] == '('
	if ex {
		b = b[1:]
	}
	v, ok := strtodFull(b)
	return v, ex, ok
}

// strtodFull reports what strtod reads when it must consume all of b: C
// whitespace first, then a number; "" is 0; overflow and underflow are
// kept (±Inf, 0); NaN is refused.
func strtodFull(b []byte) (float64, bool) {
	if len(b) == 0 {
		return 0, true
	}
	i := 0
	for i < len(b) && isCSpace(b[i]) {
		i++
	}
	rest := b[i:]
	if len(rest) == 0 {
		return 0, false
	}
	if v, ok := resp.ParseFloat(rest); ok {
		return v, true
	}
	if !plainDecimal(rest) {
		return 0, false
	}
	v, err := strconv.ParseFloat(string(rest), 64)
	if err != nil && !errors.Is(err, strconv.ErrRange) {
		return 0, false
	}
	return v, true
}

func isCSpace(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\v' || c == '\f' || c == '\r'
}

// scoreFloor returns the first score-index key of version ver with score
// s or, with past set, the first key past every key with score s.
func scoreFloor(ver uint64, s float64, past bool) []byte {
	k := layout.ScoreKey(nil, ver, s, nil)
	if past {
		n := binary.BigEndian.Uint64(k[8:])
		if n == math.MaxUint64 {
			_, hi := layout.VersionBounds(ver)
			return hi
		}
		binary.BigEndian.PutUint64(k[8:], n+1)
	}
	return k
}

// bounds returns the score-index key range [lo, hi) holding the interval
// and whether it is empty.
func (r scoreRange) bounds(ver uint64) (lo, hi []byte, empty bool) {
	lo = scoreFloor(ver, r.min, r.minex)
	hi = scoreFloor(ver, r.max, !r.maxex)
	return lo, hi, bytes.Compare(lo, hi) >= 0
}

// lexBound is one end of a ZRANGEBYLEX interval.
type lexBound struct {
	inf   int // -1 for "-", +1 for "+", 0 for a value
	value []byte
	ex    bool
}

// parseLexBound is zslParseLexRangeItem: "-", "+", "(value" or "[value".
func parseLexBound(b []byte) (lexBound, bool) {
	switch {
	case len(b) == 1 && b[0] == '-':
		return lexBound{inf: -1}, true
	case len(b) == 1 && b[0] == '+':
		return lexBound{inf: 1}, true
	case len(b) > 0 && b[0] == '(':
		return lexBound{value: b[1:], ex: true}, true
	case len(b) > 0 && b[0] == '[':
		return lexBound{value: b[1:]}, true
	}
	return lexBound{}, false
}

// lexBounds returns the sub key range [lo, hi) of a lex interval over one
// version. Members sort by bytes in sub, which is the order ZRANGEBYLEX
// defines for members of equal score.
func lexBounds(ver uint64, min, max lexBound) (lo, hi []byte, empty bool) {
	vlo, vhi := layout.VersionBounds(ver)
	switch {
	case min.inf < 0:
		lo = vlo
	case min.inf > 0:
		return nil, nil, true
	case min.ex:
		lo = append(layout.SubKey(nil, ver, min.value), 0)
	default:
		lo = layout.SubKey(nil, ver, min.value)
	}
	switch {
	case max.inf > 0:
		hi = vhi
	case max.inf < 0:
		return nil, nil, true
	case max.ex:
		hi = layout.SubKey(nil, ver, max.value)
	default:
		hi = append(layout.SubKey(nil, ver, max.value), 0)
	}
	return lo, hi, bytes.Compare(lo, hi) >= 0
}

func cmdZcount(c *command.Ctx, args [][]byte) resp.Reply {
	r, ok := parseScoreRange(args[2], args[3])
	if !ok {
		return errNotFloatRange
	}
	k, rep := openColl(c, args[1], layout.TZSet, false)
	if rep != nil || !k.exists {
		return orZero(rep)
	}
	lo, hi, empty := r.bounds(k.m.Version)
	if empty {
		return zero
	}
	return countRange(c.Reader(), store.CFZScore, lo, hi)
}

func cmdZlexcount(c *command.Ctx, args [][]byte) resp.Reply {
	min, ok1 := parseLexBound(args[2])
	max, ok2 := parseLexBound(args[3])
	if !ok1 || !ok2 {
		return errNotLexRange
	}
	k, rep := openColl(c, args[1], layout.TZSet, false)
	if rep != nil || !k.exists {
		return orZero(rep)
	}
	lo, hi, empty := lexBounds(k.m.Version, min, max)
	if empty {
		return zero
	}
	return countRange(c.Reader(), store.CFSub, lo, hi)
}

// countRange counts the keys of cf in [lo, hi), both inside one version.
func countRange(r store.Reader, cf store.CF, lo, hi []byte) resp.Reply {
	it := r.Iter(cf, lo, hi, store.IterOpts{FillCache: true})
	defer it.Close()
	var n int64
	for it.SeekToFirst(); it.Valid(); it.Next() {
		n++
	}
	if err := it.Err(); err != nil {
		return storeErr(err)
	}
	return resp.Int(n)
}

// Range kinds of zrangeGenericCommand.
const (
	byAuto = iota
	byRank
	byScore
	byLex
)

// zrangeReq is a parsed ZRANGE-family request.
type zrangeReq struct {
	key            []byte
	by             int
	rev            bool
	withScores     bool
	offset, limit  int64 // limit -1: none
	start, end     int64
	scores         scoreRange
	lexMin, lexMax lexBound
}

// parseZrange is zrangeGenericCommand's argument parsing. argStart is the
// index of the source key; by and rev preset the legacy forms
// (ZREVRANGE, ZRANGEBYSCORE, ...), whose direction is part of the name
// (revFixed), so they refuse REV.
func parseZrange(args [][]byte, argStart int, store bool, by int, rev, revFixed bool) (zrangeReq, resp.Reply) {
	q := zrangeReq{key: args[argStart], by: by, rev: rev, limit: -1}
	minIdx, maxIdx := argStart+1, argStart+2
	for j := argStart + 3; j < len(args); j++ {
		left := len(args) - j - 1
		switch opt := args[j]; {
		case !store && equalFold(opt, "withscores"):
			q.withScores = true
		case equalFold(opt, "limit") && left >= 2:
			var ok bool
			if q.offset, ok = resp.ParseInt(args[j+1]); !ok {
				return q, resp.ErrNotInteger
			}
			if q.limit, ok = resp.ParseInt(args[j+2]); !ok {
				return q, resp.ErrNotInteger
			}
			j += 2
		case !revFixed && !q.rev && equalFold(opt, "rev"):
			q.rev = true
		case q.by == byAuto && equalFold(opt, "bylex"):
			q.by = byLex
		case q.by == byAuto && equalFold(opt, "byscore"):
			q.by = byScore
		default:
			return q, resp.ErrSyntax
		}
	}
	if q.by == byAuto {
		q.by = byRank
	}
	if q.limit != -1 && q.by == byRank {
		return q, resp.Err("ERR syntax error, LIMIT is only supported in combination with either BYSCORE or BYLEX")
	}
	if q.withScores && q.by == byLex {
		return q, resp.Err("ERR syntax error, WITHSCORES not supported in combination with BYLEX")
	}
	if q.rev && q.by != byRank {
		minIdx, maxIdx = maxIdx, minIdx
	}
	switch q.by {
	case byRank:
		var ok bool
		if q.start, ok = resp.ParseInt(args[minIdx]); !ok {
			return q, resp.ErrNotInteger
		}
		if q.end, ok = resp.ParseInt(args[maxIdx]); !ok {
			return q, resp.ErrNotInteger
		}
	case byScore:
		var ok bool
		if q.scores, ok = parseScoreRange(args[minIdx], args[maxIdx]); !ok {
			return q, errNotFloatRange
		}
	case byLex:
		var ok1, ok2 bool
		q.lexMin, ok1 = parseLexBound(args[minIdx])
		q.lexMax, ok2 = parseLexBound(args[maxIdx])
		if !ok1 || !ok2 {
			return q, errNotLexRange
		}
	}
	return q, nil
}

// zrangeRun reads the entries a parsed request selects from sorted set k.
// A negative LIMIT offset selects nothing and a negative count means no
// limit, as Redis's skip and emit loops behave.
func zrangeRun(r store.Reader, k *coll, q zrangeReq) ([]zentry, error) {
	switch q.by {
	case byRank:
		n := k.m.Size
		start, end := q.start, q.end
		if start < 0 {
			start += n
		}
		if end < 0 {
			end += n
		}
		start = max(start, 0)
		if start > end || start >= n {
			return nil, nil
		}
		end = min(end, n-1)
		lo, hi := layout.VersionBounds(k.m.Version)
		return walkScores(r, lo, hi, q.rev, start, end-start+1)
	case byScore:
		if q.offset < 0 || (q.offset > 0 && q.offset >= k.m.Size) {
			return nil, nil
		}
		lo, hi, empty := q.scores.bounds(k.m.Version)
		if empty {
			return nil, nil
		}
		return walkScores(r, lo, hi, q.rev, q.offset, q.limit)
	default:
		if q.offset < 0 {
			return nil, nil
		}
		lo, hi, empty := lexBounds(k.m.Version, q.lexMin, q.lexMax)
		if empty {
			return nil, nil
		}
		return walkMembers(r, lo, hi, q.rev, q.offset, q.limit)
	}
}

// walkScores reads up to limit entries (limit < 0: all) of the score
// index in [lo, hi), forwards or backwards, after skipping skip entries.
func walkScores(r store.Reader, lo, hi []byte, rev bool, skip, limit int64) ([]zentry, error) {
	it := r.Iter(store.CFZScore, lo, hi, store.IterOpts{FillCache: true, TotalOrderSeek: rev})
	defer it.Close()
	var out []zentry
	if rev {
		it.SeekToLast()
	} else {
		it.SeekToFirst()
	}
	for ; it.Valid() && (limit < 0 || int64(len(out)) < limit); step(it, rev) {
		if skip > 0 {
			skip--
			continue
		}
		_, score, member := layout.SplitScoreKey(it.Key())
		out = append(out, zentry{clone(member), score})
	}
	return out, it.Err()
}

// walkMembers is walkScores over the sub keys of a lex range; scores come
// from the values.
func walkMembers(r store.Reader, lo, hi []byte, rev bool, skip, limit int64) ([]zentry, error) {
	it := r.Iter(store.CFSub, lo, hi, store.IterOpts{FillCache: true, TotalOrderSeek: rev})
	defer it.Close()
	var out []zentry
	if rev {
		it.SeekToLast()
	} else {
		it.SeekToFirst()
	}
	for ; it.Valid() && (limit < 0 || int64(len(out)) < limit); step(it, rev) {
		if skip > 0 {
			skip--
			continue
		}
		score, err := layout.DecodeScoreValue(it.Value())
		if err != nil {
			return nil, err
		}
		out = append(out, zentry{clone(it.Key()[8:]), score})
	}
	return out, it.Err()
}

func step(it store.Iterator, rev bool) {
	if rev {
		it.Prev()
	} else {
		it.Next()
	}
}

func cmdZrange(c *command.Ctx, args [][]byte) resp.Reply {
	return zrangeCmd(c, args, byAuto, false, false)
}

func cmdZrevrange(c *command.Ctx, args [][]byte) resp.Reply {
	return zrangeCmd(c, args, byRank, true, true)
}

func cmdZrangebyscore(c *command.Ctx, args [][]byte) resp.Reply {
	return zrangeCmd(c, args, byScore, false, true)
}

func cmdZrevrangebyscore(c *command.Ctx, args [][]byte) resp.Reply {
	return zrangeCmd(c, args, byScore, true, true)
}

func cmdZrangebylex(c *command.Ctx, args [][]byte) resp.Reply {
	return zrangeCmd(c, args, byLex, false, true)
}

func cmdZrevrangebylex(c *command.Ctx, args [][]byte) resp.Reply {
	return zrangeCmd(c, args, byLex, true, true)
}

// zrangeCmd serves ZRANGE and its legacy forms.
func zrangeCmd(c *command.Ctx, args [][]byte, by int, rev, revFixed bool) resp.Reply {
	q, rep := parseZrange(args, 1, false, by, rev, revFixed)
	if rep != nil {
		return rep
	}
	k, rep := openColl(c, q.key, layout.TZSet, false)
	switch {
	case rep != nil:
		return rep
	case !k.exists:
		return resp.Array()
	}
	es, err := zrangeRun(c.Reader(), k, q)
	if err != nil {
		return storeErr(err)
	}
	return zsetReply(es, q.withScores)
}

// cmdZrangestore stores the selected range, with scores, as a new sorted
// set at dst; an empty range deletes dst.
func cmdZrangestore(c *command.Ctx, args [][]byte) resp.Reply {
	q, rep := parseZrange(args, 2, true, byAuto, false, false)
	if rep != nil {
		return rep
	}
	k, rep := openColl(c, q.key, layout.TZSet, false)
	if rep != nil {
		return rep
	}
	var es []zentry
	if k.exists {
		var err error
		if es, err = zrangeRun(c.Reader(), k, q); err != nil {
			return storeErr(err)
		}
	}
	return storeZset(c, args[1], es)
}

// storeZset replaces dst with a sorted set holding es (distinct members)
// and replies its size; an empty es deletes dst.
func storeZset(c *command.Ctx, dst []byte, es []zentry) resp.Reply {
	e, err := getEntry(c, dst)
	if err != nil {
		return storeErr(err)
	}
	if len(es) == 0 {
		if e.found {
			deleteKey(c, c.DB, dst, e)
		}
		return zero
	}
	ver := newVersion(c)
	for _, z := range es {
		zput(c, ver, z.member, z.score, nil)
	}
	writeMeta(c, c.DB, dst, layout.Meta{Type: layout.TZSet, Version: ver, Size: int64(len(es))}, e)
	return resp.Int(int64(len(es)))
}

func cmdZrank(c *command.Ctx, args [][]byte) resp.Reply    { return zrankGeneric(c, args, false) }
func cmdZrevrank(c *command.Ctx, args [][]byte) resp.Reply { return zrankGeneric(c, args, true) }

// zrankGeneric counts the members ordered before the member in the score
// index, an O(rank) walk.
func zrankGeneric(c *command.Ctx, args [][]byte, rev bool) resp.Reply {
	if len(args) > 4 {
		return resp.ErrArity(c.Spec.FullName())
	}
	withScore := false
	if len(args) == 4 {
		if !equalFold(args[3], "withscore") {
			return resp.ErrSyntax
		}
		withScore = true
	}
	missing := resp.Null()
	if withScore {
		missing = resp.NullArray()
	}
	k, rep := openColl(c, args[1], layout.TZSet, false)
	switch {
	case rep != nil:
		return rep
	case !k.exists:
		return missing
	}
	r := c.Reader()
	cur, err := zscores(r, k.m.Version, args[2:3])
	switch {
	case err != nil:
		return storeErr(err)
	case cur[0] == nil:
		return missing
	}
	score := *cur[0]
	lo, _ := layout.VersionBounds(k.m.Version)
	it := r.Iter(store.CFZScore, lo, layout.ScoreKey(nil, k.m.Version, score, args[2]), store.IterOpts{FillCache: true})
	var rank int64
	for it.SeekToFirst(); it.Valid(); it.Next() {
		rank++
	}
	err = it.Err()
	it.Close()
	if err != nil {
		return storeErr(err)
	}
	if rev {
		rank = k.m.Size - 1 - rank
	}
	if withScore {
		return resp.Array(resp.Int(rank), resp.Double(score))
	}
	return resp.Int(rank)
}

func cmdZpopmin(c *command.Ctx, args [][]byte) resp.Reply { return zpop(c, args, false) }
func cmdZpopmax(c *command.Ctx, args [][]byte) resp.Reply { return zpop(c, args, true) }

// zpop is zpopMinMaxCommand. Without a count RESP3 still gets a flat
// [member, score]; with a count it gets pairs.
func zpop(c *command.Ctx, args [][]byte, fromMax bool) resp.Reply {
	if len(args) > 3 {
		return resp.ErrSyntax
	}
	count, counted := int64(1), len(args) == 3
	if counted {
		n, ok := resp.ParseInt(args[2])
		if !ok || n < 0 {
			return errPositive
		}
		count = n
	}
	k, rep := openColl(c, args[1], layout.TZSet, false)
	switch {
	case rep != nil:
		return rep
	case !k.exists || count == 0:
		return resp.Array()
	}
	lo, hi := layout.VersionBounds(k.m.Version)
	es, err := walkScores(c.Reader(), lo, hi, fromMax, 0, count)
	if err != nil {
		return storeErr(err)
	}
	zremove(c, k, es)
	k.save(c)
	if counted {
		return zsetReply(es, true)
	}
	return resp.Stream(func(w *resp.Writer) {
		w.ArrayHeader(2 * len(es))
		for _, e := range es {
			w.Bulk(e.member)
			w.Double(e.score)
		}
	})
}

// cmdZrandmember is zrandmemberCommand; see randWithCount.
func cmdZrandmember(c *command.Ctx, args [][]byte) resp.Reply {
	if len(args) < 3 {
		return randOne(c, args[1], layout.TZSet)
	}
	l, rep := parseIntRange(args[2], -math.MaxInt64, math.MaxInt64)
	if rep != nil {
		return rep
	}
	if len(args) > 4 || (len(args) == 4 && !equalFold(args[3], "withscores")) {
		return resp.ErrSyntax
	}
	withScores := len(args) == 4
	if withScores && (l < -math.MaxInt64/2 || l > math.MaxInt64/2) {
		return resp.Err("ERR value is out of range")
	}
	return randWithCount(c, args[1], layout.TZSet, l, withScores)
}

func cmdZremrangebyrank(c *command.Ctx, args [][]byte) resp.Reply {
	start, ok1 := resp.ParseInt(args[2])
	end, ok2 := resp.ParseInt(args[3])
	if !ok1 || !ok2 {
		return resp.ErrNotInteger
	}
	return zremRange(c, args[1], zrangeReq{by: byRank, start: start, end: end, limit: -1})
}

func cmdZremrangebyscore(c *command.Ctx, args [][]byte) resp.Reply {
	r, ok := parseScoreRange(args[2], args[3])
	if !ok {
		return errNotFloatRange
	}
	return zremRange(c, args[1], zrangeReq{by: byScore, scores: r, limit: -1})
}

func cmdZremrangebylex(c *command.Ctx, args [][]byte) resp.Reply {
	min, ok1 := parseLexBound(args[2])
	max, ok2 := parseLexBound(args[3])
	if !ok1 || !ok2 {
		return errNotLexRange
	}
	return zremRange(c, args[1], zrangeReq{by: byLex, lexMin: min, lexMax: max, limit: -1})
}

// zremRange removes the entries a range selects and replies the count.
func zremRange(c *command.Ctx, key []byte, q zrangeReq) resp.Reply {
	k, rep := openColl(c, key, layout.TZSet, false)
	if rep != nil || !k.exists {
		return orZero(rep)
	}
	es, err := zrangeRun(c.Reader(), k, q)
	if err != nil {
		return storeErr(err)
	}
	zremove(c, k, es)
	k.save(c)
	return resp.Int(int64(len(es)))
}

// Aggregates of ZUNION and ZINTER.
const (
	aggSum = iota
	aggMin
	aggMax
)

// zsrc is one input of ZUNION, ZINTER or ZDIFF: a sorted set, or a plain
// set whose members score 1.
type zsrc struct {
	t       layout.Type
	version uint64
	size    int64
	exists  bool
	weight  float64
}

// zopReq is a parsed ZUNION/ZINTER/ZDIFF request.
type zopReq struct {
	srcs       []zsrc
	agg        int
	withScores bool
	limit      int64
}

// parseZop is zunionInterDiffGenericCommand's parsing: numkeys, the keys
// (their types are checked here, before the options), then WEIGHTS,
// AGGREGATE, WITHSCORES and, for ZINTERCARD, LIMIT.
func parseZop(c *command.Ctx, args [][]byte, numIdx, op int, store, cardOnly bool) (zopReq, resp.Reply) {
	var q zopReq
	n, ok := resp.ParseInt(args[numIdx])
	if !ok {
		return q, resp.ErrNotInteger
	}
	if n < 1 {
		return q, resp.Err("ERR at least 1 input key is needed for '" + c.Spec.FullName() + "' command")
	}
	if n > int64(len(args)-numIdx-1) {
		return q, resp.ErrSyntax
	}
	q.srcs = make([]zsrc, n)
	j := numIdx + 1
	for i := range q.srcs {
		m, ok, err := loadMeta(c, args[j])
		if err != nil {
			return q, storeErr(err)
		}
		if ok && m.Type != layout.TZSet && m.Type != layout.TSet {
			return q, resp.ErrWrongType
		}
		q.srcs[i] = zsrc{t: m.Type, version: m.Version, size: m.Size, exists: ok, weight: 1}
		j++
	}
	for j < len(args) {
		left := len(args) - j
		switch opt := args[j]; {
		case op != opDiff && !cardOnly && left >= len(q.srcs)+1 && equalFold(opt, "weights"):
			j++
			for i := range q.srcs {
				w, ok := resp.ParseFloat(args[j])
				if !ok {
					return q, resp.Err("ERR weight value is not a float")
				}
				q.srcs[i].weight = w
				j++
			}
		case op != opDiff && !cardOnly && left >= 2 && equalFold(opt, "aggregate"):
			switch a := args[j+1]; {
			case equalFold(a, "sum"):
				q.agg = aggSum
			case equalFold(a, "min"):
				q.agg = aggMin
			case equalFold(a, "max"):
				q.agg = aggMax
			default:
				return q, resp.ErrSyntax
			}
			j += 2
		case !store && !cardOnly && equalFold(opt, "withscores"):
			q.withScores = true
			j++
		case cardOnly && left >= 2 && equalFold(opt, "limit"):
			l, ok := resp.ParseInt(args[j+1])
			if !ok || l < 0 {
				return q, resp.Err("ERR LIMIT can't be negative")
			}
			q.limit = l
			j += 2
		default:
			return q, resp.ErrSyntax
		}
	}
	return q, nil
}

// aggregate is zunionInterAggregate: a SUM that yields NaN (+inf plus
// -inf) becomes 0.
func aggregate(target *float64, v float64, agg int) {
	switch agg {
	case aggSum:
		*target += v
		if math.IsNaN(*target) {
			*target = 0
		}
	case aggMin:
		if v < *target {
			*target = v
		}
	case aggMax:
		if v > *target {
			*target = v
		}
	}
}

// srcScore decodes the score an input holds for a member value.
func srcScore(s zsrc, val []byte) (float64, error) {
	if s.t == layout.TSet {
		return 1, nil
	}
	return layout.DecodeScoreValue(val)
}

// weighted is weight*score with NaN (0 times infinity) read as 0.
func weighted(w, score float64) float64 {
	v := w * score
	if math.IsNaN(v) {
		return 0
	}
	return v
}

// sortedBySize orders inputs by size, smallest first, as Redis does
// before a union or intersection; ties keep their order.
func sortedBySize(srcs []zsrc) []zsrc {
	out := slices.Clone(srcs)
	slices.SortStableFunc(out, func(a, b zsrc) int {
		switch {
		case a.size < b.size:
			return -1
		case a.size > b.size:
			return 1
		}
		return 0
	})
	return out
}

// zinter computes the intersection: members of the smallest input found
// in every other, scores weighted and aggregated input by input. With
// cardOnly it only counts, stopping at the LIMIT.
func zinter(r store.Reader, q zopReq, cardOnly bool, keep int) ([]zentry, int64, error) {
	srcs := sortedBySize(q.srcs)
	for _, s := range srcs {
		if !s.exists {
			return nil, 0, nil
		}
	}
	first := srcs[0]
	var out []zentry
	var n int64
	it := iterColl(r, store.CFSub, first.version, false)
	defer it.Close()
	type cand struct {
		member []byte
		raw    float64
		score  float64
	}
	batch := make([]cand, 0, setBatch)
	flush := func() (bool, error) {
		alive := batch
		for _, s := range srcs[1:] {
			if len(alive) == 0 {
				break
			}
			if s.version == first.version {
				for i := range alive {
					aggregate(&alive[i].score, alive[i].raw*s.weight, q.agg)
				}
				continue
			}
			subs := make([][]byte, len(alive))
			for i, a := range alive {
				subs[i] = a.member
			}
			vals, err := subValues(r, s.version, subs)
			if err != nil {
				return false, err
			}
			next := alive[:0:0]
			for i, v := range vals {
				if v == nil {
					continue
				}
				sc, err := srcScore(s, v)
				if err != nil {
					return false, err
				}
				a := alive[i]
				aggregate(&a.score, sc*s.weight, q.agg)
				next = append(next, a)
			}
			alive = next
		}
		for _, a := range alive {
			n++
			if !cardOnly {
				if keep > 0 && len(out) >= keep {
					return false, errTooManyMembers(keep)
				}
				out = append(out, zentry{a.member, a.score})
			}
			if cardOnly && q.limit > 0 && n >= q.limit {
				return true, nil
			}
		}
		batch = batch[:0]
		return false, nil
	}
	for it.SeekToFirst(); it.Valid(); it.Next() {
		raw, err := srcScore(first, it.Value())
		if err != nil {
			return nil, 0, err
		}
		batch = append(batch, cand{clone(it.Key()[8:]), raw, weighted(first.weight, raw)})
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

// zunion accumulates every input, smallest first: a member's first score
// is weighted, later ones are aggregated into it.
func zunion(r store.Reader, q zopReq, keep int) ([]zentry, error) {
	acc := map[string]float64{}
	var order [][]byte
	for _, s := range sortedBySize(q.srcs) {
		if !s.exists {
			continue
		}
		it := iterColl(r, store.CFSub, s.version, false)
		for it.SeekToFirst(); it.Valid(); it.Next() {
			raw, err := srcScore(s, it.Value())
			if err != nil {
				it.Close()
				return nil, err
			}
			v := weighted(s.weight, raw)
			m := it.Key()[8:]
			if cur, ok := acc[string(m)]; ok {
				aggregate(&cur, v, q.agg)
				acc[string(m)] = cur
				continue
			}
			if keep > 0 && len(acc) >= keep {
				it.Close()
				return nil, errTooManyMembers(keep)
			}
			acc[string(m)] = v
			order = append(order, clone(m))
		}
		err := it.Err()
		it.Close()
		if err != nil {
			return nil, err
		}
	}
	out := make([]zentry, len(order))
	for i, m := range order {
		out[i] = zentry{m, acc[string(m)]}
	}
	return out, nil
}

// zdiff returns the members of the first input found in no other, with
// the first input's scores.
func zdiff(r store.Reader, q zopReq, keep int) ([]zentry, error) {
	first := q.srcs[0]
	if !first.exists {
		return nil, nil
	}
	var out []zentry
	it := iterColl(r, store.CFSub, first.version, false)
	defer it.Close()
	var batch []zentry
	flush := func() error {
		alive := batch
		for _, s := range q.srcs[1:] {
			if !s.exists || len(alive) == 0 {
				continue
			}
			if s.version == first.version {
				alive = nil
				break
			}
			subs := make([][]byte, len(alive))
			for i, a := range alive {
				subs[i] = a.member
			}
			vals, err := subValues(r, s.version, subs)
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
		raw, err := srcScore(first, it.Value())
		if err != nil {
			return nil, err
		}
		batch = append(batch, zentry{clone(it.Key()[8:]), raw})
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

// sortEntries orders a result the way Redis's skiplist does: by score,
// then by member bytes.
func sortEntries(es []zentry) {
	slices.SortFunc(es, func(a, b zentry) int {
		switch {
		case a.score < b.score:
			return -1
		case a.score > b.score:
			return 1
		}
		return bytes.Compare(a.member, b.member)
	})
}

// zop runs ZUNION, ZINTER or ZDIFF and returns the result sorted.
func zop(c *command.Ctx, args [][]byte, numIdx, op int, store bool) ([]zentry, bool, resp.Reply) {
	q, rep := parseZop(c, args, numIdx, op, store, false)
	if rep != nil {
		return nil, false, rep
	}
	keep := c.Cfg.Knobs().SetOpMaxMembers
	var es []zentry
	var err error
	switch op {
	case opInter:
		es, _, err = zinter(c.Reader(), q, false, keep)
	case opUnion:
		es, err = zunion(c.Reader(), q, keep)
	default:
		es, err = zdiff(c.Reader(), q, keep)
	}
	if err != nil {
		return nil, false, storeErr(err)
	}
	sortEntries(es)
	return es, q.withScores, nil
}

func cmdZunion(c *command.Ctx, args [][]byte) resp.Reply { return zopReply(c, args, opUnion) }
func cmdZinter(c *command.Ctx, args [][]byte) resp.Reply { return zopReply(c, args, opInter) }
func cmdZdiff(c *command.Ctx, args [][]byte) resp.Reply  { return zopReply(c, args, opDiff) }

func zopReply(c *command.Ctx, args [][]byte, op int) resp.Reply {
	es, withScores, rep := zop(c, args, 1, op, false)
	if rep != nil {
		return rep
	}
	return zsetReply(es, withScores)
}

func cmdZunionstore(c *command.Ctx, args [][]byte) resp.Reply { return zopStore(c, args, opUnion) }
func cmdZinterstore(c *command.Ctx, args [][]byte) resp.Reply { return zopStore(c, args, opInter) }
func cmdZdiffstore(c *command.Ctx, args [][]byte) resp.Reply  { return zopStore(c, args, opDiff) }

func zopStore(c *command.Ctx, args [][]byte, op int) resp.Reply {
	es, _, rep := zop(c, args, 2, op, true)
	if rep != nil {
		return rep
	}
	return storeZset(c, args[1], es)
}

func cmdZintercard(c *command.Ctx, args [][]byte) resp.Reply {
	q, rep := parseZop(c, args, 1, opInter, false, true)
	if rep != nil {
		return rep
	}
	_, n, err := zinter(c.Reader(), q, true, 0)
	if err != nil {
		return storeErr(err)
	}
	return resp.Int(n)
}
