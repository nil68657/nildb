package redis

import (
	"bytes"
	"math"
	"math/big"
	"strconv"
	"strings"

	"github.com/nil68657/nildb/internal/command"
	"github.com/nil68657/nildb/internal/layout"
	"github.com/nil68657/nildb/internal/resp"
	"github.com/nil68657/nildb/internal/store"
)

// Strings keep their payload inline in the meta entry
// (| flags | expire_ms | payload |), so every string command is one Get
// and at most one Put. Counters and APPEND read, compute and Put under
// the key lock; they reply their error before writing anything.

func registerString(r *command.Registry) {
	const g = "string"
	w, ro, f := command.Write, command.ReadOnly, command.Fast
	r.Register(
		command.Spec{Name: "set", Arity: -3, Flags: w, FirstKey: 1, LastKey: 1, KeyStep: 1, Group: g, Since: "1.0.0",
			Summary: "Sets the string value of a key, ignoring its type. The key is created if it doesn't exist.", Run: cmdSet},
		command.Spec{Name: "get", Arity: 2, Flags: ro | f, FirstKey: 1, LastKey: 1, KeyStep: 1, Group: g, Since: "1.0.0",
			Summary: "Returns the string value of a key.", Run: cmdGet},
		command.Spec{Name: "getdel", Arity: 2, Flags: w | f, FirstKey: 1, LastKey: 1, KeyStep: 1, Group: g, Since: "6.2.0",
			Summary: "Returns the string value of a key after deleting the key.", Run: cmdGetdel},
		command.Spec{Name: "getex", Arity: -2, Flags: w | f, FirstKey: 1, LastKey: 1, KeyStep: 1, Group: g, Since: "6.2.0",
			Summary: "Returns the string value of a key after setting its expiration time.", Run: cmdGetex},
		command.Spec{Name: "getset", Arity: 3, Flags: w | f, FirstKey: 1, LastKey: 1, KeyStep: 1, Group: g, Since: "1.0.0",
			Summary: "Returns the previous string value of a key after setting it to a new value.", Run: cmdGetset},
		command.Spec{Name: "mget", Arity: -2, Flags: ro | f | command.MultiRead, FirstKey: 1, LastKey: -1, KeyStep: 1, Group: g, Since: "1.0.0",
			Summary: "Atomically returns the string values of one or more keys.", Run: cmdMget},
		command.Spec{Name: "mset", Arity: -3, Flags: w, FirstKey: 1, LastKey: -1, KeyStep: 2, Group: g, Since: "1.0.1",
			Summary: "Atomically creates or modifies the string values of one or more keys.", Run: cmdMset},
		command.Spec{Name: "msetnx", Arity: -3, Flags: w, FirstKey: 1, LastKey: -1, KeyStep: 2, Group: g, Since: "1.0.1",
			Summary: "Atomically modifies the string values of one or more keys only when all keys don't exist.", Run: cmdMsetnx},
		command.Spec{Name: "setnx", Arity: 3, Flags: w | f, FirstKey: 1, LastKey: 1, KeyStep: 1, Group: g, Since: "1.0.0",
			Summary: "Set the string value of a key only when the key doesn't exist.", Run: cmdSetnx},
		command.Spec{Name: "setex", Arity: 4, Flags: w, FirstKey: 1, LastKey: 1, KeyStep: 1, Group: g, Since: "2.0.0",
			Summary: "Sets the string value and expiration time of a key. Creates the key if it doesn't exist.", Run: cmdSetex},
		command.Spec{Name: "psetex", Arity: 4, Flags: w, FirstKey: 1, LastKey: 1, KeyStep: 1, Group: g, Since: "2.6.0",
			Summary: "Sets both string value and expiration time in milliseconds of a key. The key is created if it doesn't exist.", Run: cmdPsetex},
		command.Spec{Name: "incr", Arity: 2, Flags: w | f, FirstKey: 1, LastKey: 1, KeyStep: 1, Group: g, Since: "1.0.0",
			Summary: "Increments the integer value of a key by one. Uses 0 as initial value if the key doesn't exist.", Run: cmdIncr},
		command.Spec{Name: "decr", Arity: 2, Flags: w | f, FirstKey: 1, LastKey: 1, KeyStep: 1, Group: g, Since: "1.0.0",
			Summary: "Decrements the integer value of a key by one. Uses 0 as initial value if the key doesn't exist.", Run: cmdDecr},
		command.Spec{Name: "incrby", Arity: 3, Flags: w | f, FirstKey: 1, LastKey: 1, KeyStep: 1, Group: g, Since: "1.0.0",
			Summary: "Increments the integer value of a key by a number. Uses 0 as initial value if the key doesn't exist.", Run: cmdIncrby},
		command.Spec{Name: "decrby", Arity: 3, Flags: w | f, FirstKey: 1, LastKey: 1, KeyStep: 1, Group: g, Since: "1.0.0",
			Summary: "Decrements a number from the integer value of a key. Uses 0 as initial value if the key doesn't exist.", Run: cmdDecrby},
		command.Spec{Name: "incrbyfloat", Arity: 3, Flags: w | f, FirstKey: 1, LastKey: 1, KeyStep: 1, Group: g, Since: "2.6.0",
			Summary: "Increment the floating point value of a key by a number. Uses 0 as initial value if the key doesn't exist.", Run: cmdIncrbyfloat},
		command.Spec{Name: "append", Arity: 3, Flags: w | f, FirstKey: 1, LastKey: 1, KeyStep: 1, Group: g, Since: "2.0.0",
			Summary: "Appends a string to the value of a key. Creates the key if it doesn't exist.", Run: cmdAppend},
		command.Spec{Name: "strlen", Arity: 2, Flags: ro | f, FirstKey: 1, LastKey: 1, KeyStep: 1, Group: g, Since: "2.2.0",
			Summary: "Returns the length of a string value.", Run: cmdStrlen},
		command.Spec{Name: "getrange", Arity: 4, Flags: ro, FirstKey: 1, LastKey: 1, KeyStep: 1, Group: g, Since: "2.4.0",
			Summary: "Returns a substring of the string stored at a key.", Run: cmdGetrange},
		command.Spec{Name: "substr", Arity: 4, Flags: ro, FirstKey: 1, LastKey: 1, KeyStep: 1, Group: g, Since: "1.0.0",
			Summary: "Returns a substring from a string value.", Run: cmdGetrange},
		command.Spec{Name: "setrange", Arity: 4, Flags: w, FirstKey: 1, LastKey: 1, KeyStep: 1, Group: g, Since: "2.2.0",
			Summary: "Overwrites a part of a string value with another by an offset. Creates the key if it doesn't exist.", Run: cmdSetrange},
	)
}

// Option bits of SET and GETEX, as in t_string.c.
const (
	optNX = 1 << iota
	optXX
	optEX
	optPX
	optKeepTTL
	optGet
	optEXAT
	optPXAT
	optPersist
)

// strOpts is a parsed SET or GETEX option list.
type strOpts struct {
	flags  int
	expire []byte // the EX, PX, EXAT or PXAT argument; nil when none
	ms     bool   // expire counts milliseconds (PX, PXAT)
}

// parseStringOpts is parseExtendedStringArgumentsOrReply: the SET options
// from args[3] when forSet, the GETEX options from args[2] otherwise. The
// exclusion rules are Redis's, including what it lets repeat (GET,
// KEEPTTL, a second EX).
func parseStringOpts(args [][]byte, forSet bool) (strOpts, resp.Reply) {
	var o strOpts
	j := 2
	if forSet {
		j = 3
	}
	has := func(bits int) bool { return o.flags&bits != 0 }
	for ; j < len(args); j++ {
		opt := args[j]
		next := j+1 < len(args)
		switch {
		case forSet && equalFold(opt, "nx") && !has(optXX):
			o.flags |= optNX
		case forSet && equalFold(opt, "xx") && !has(optNX):
			o.flags |= optXX
		case forSet && equalFold(opt, "get"):
			o.flags |= optGet
		case forSet && equalFold(opt, "keepttl") && !has(optPersist|optEX|optEXAT|optPX|optPXAT):
			o.flags |= optKeepTTL
		case !forSet && equalFold(opt, "persist") && !has(optEX|optEXAT|optPX|optPXAT|optKeepTTL):
			o.flags |= optPersist
		case equalFold(opt, "ex") && !has(optKeepTTL|optPersist|optEXAT|optPX|optPXAT) && next:
			o.flags |= optEX
			o.expire, o.ms = args[j+1], false
			j++
		case equalFold(opt, "px") && !has(optKeepTTL|optPersist|optEX|optEXAT|optPXAT) && next:
			o.flags |= optPX
			o.expire, o.ms = args[j+1], true
			j++
		case equalFold(opt, "exat") && !has(optKeepTTL|optPersist|optEX|optPX|optPXAT) && next:
			o.flags |= optEXAT
			o.expire, o.ms = args[j+1], false
			j++
		case equalFold(opt, "pxat") && !has(optKeepTTL|optPersist|optEX|optEXAT|optPX) && next:
			o.flags |= optPXAT
			o.expire, o.ms = args[j+1], true
			j++
		default:
			return o, resp.ErrSyntax
		}
	}
	return o, nil
}

// errExpireTime is addReplyErrorExpireTime for the running command.
func errExpireTime(c *command.Ctx) resp.Reply {
	return resp.Err("ERR invalid expire time in '" + c.Spec.FullName() + "' command")
}

// expireAt is getExpireMillisecondsOrReply: it turns an EX, PX, EXAT or
// PXAT argument into an absolute Unix time in milliseconds. Values that
// are not positive, or that overflow, reply "invalid expire time".
func expireAt(c *command.Ctx, o strOpts) (int64, resp.Reply) {
	v, ok := resp.ParseInt(o.expire)
	if !ok {
		return 0, resp.ErrNotInteger
	}
	if v <= 0 || (!o.ms && v > math.MaxInt64/1000) {
		return 0, errExpireTime(c)
	}
	if !o.ms {
		v *= 1000
	}
	if o.flags&(optEX|optPX) != 0 {
		v += c.NowMS() // wraps on overflow, caught just below as in Redis
	}
	if v <= 0 {
		return 0, errExpireTime(c)
	}
	return v, nil
}

func cmdSet(c *command.Ctx, args [][]byte) resp.Reply {
	o, rep := parseStringOpts(args, true)
	if rep != nil {
		return rep
	}
	return setGeneric(c, args[1], args[2], o, nil, nil)
}

func cmdSetnx(c *command.Ctx, args [][]byte) resp.Reply {
	return setGeneric(c, args[1], args[2], strOpts{flags: optNX}, one, zero)
}

func cmdSetex(c *command.Ctx, args [][]byte) resp.Reply {
	return setGeneric(c, args[1], args[3], strOpts{flags: optEX, expire: args[2]}, nil, nil)
}

func cmdPsetex(c *command.Ctx, args [][]byte) resp.Reply {
	return setGeneric(c, args[1], args[3], strOpts{flags: optPX, expire: args[2], ms: true}, nil, nil)
}

// setGeneric is setGenericCommand: expire validation, then GET's WRONGTYPE
// check, then NX/XX, then the write. okReply and abortReply default to +OK
// and a null; with GET the reply is always the old value.
func setGeneric(c *command.Ctx, key, val []byte, o strOpts, okReply, abortReply resp.Reply) resp.Reply {
	var when int64
	if o.expire != nil {
		var rep resp.Reply
		if when, rep = expireAt(c, o); rep != nil {
			return rep
		}
	}
	e, err := getEntry(c, key)
	if err != nil {
		return storeErr(err)
	}
	var old resp.Reply
	if o.flags&optGet != 0 {
		switch {
		case !e.live:
			old = resp.Null()
		case e.m.Type != layout.TString:
			return resp.ErrWrongType
		default:
			old = resp.Bulk(e.m.Payload)
		}
	}
	if (o.flags&optNX != 0 && e.live) || (o.flags&optXX != 0 && !e.live) {
		switch {
		case old != nil:
			return old
		case abortReply != nil:
			return abortReply
		}
		return resp.Null()
	}
	m := layout.Meta{Type: layout.TString, Payload: val}
	switch {
	case o.expire != nil:
		m.ExpireMS = when
	case o.flags&optKeepTTL != 0 && e.live:
		m.ExpireMS = e.m.ExpireMS
	}
	writeMeta(c, c.DB, key, m, e)
	switch {
	case old != nil:
		return old
	case okReply != nil:
		return okReply
	}
	return resp.OK()
}

// getString reads key as a string: live false when it is absent or
// expired, WRONGTYPE for another type.
func getString(c *command.Ctx, key []byte) (entry, resp.Reply) {
	e, err := getEntry(c, key)
	if err != nil {
		return e, storeErr(err)
	}
	if e.live && e.m.Type != layout.TString {
		return e, resp.ErrWrongType
	}
	return e, nil
}

func cmdGet(c *command.Ctx, args [][]byte) resp.Reply {
	e, rep := getString(c, args[1])
	switch {
	case rep != nil:
		return rep
	case !e.live:
		return resp.Null()
	}
	return resp.Bulk(e.m.Payload)
}

func cmdGetdel(c *command.Ctx, args [][]byte) resp.Reply {
	e, rep := getString(c, args[1])
	switch {
	case rep != nil:
		return rep
	case !e.live:
		return resp.Null()
	}
	deleteKey(c, c.DB, args[1], e)
	return resp.Bulk(e.m.Payload)
}

func cmdGetset(c *command.Ctx, args [][]byte) resp.Reply {
	e, rep := getString(c, args[1])
	if rep != nil {
		return rep
	}
	old := resp.Null()
	if e.live {
		old = resp.Bulk(e.m.Payload)
	}
	writeMeta(c, c.DB, args[1], layout.Meta{Type: layout.TString, Payload: args[2]}, e)
	return old
}

// cmdGetex is GETEX: the option syntax is checked first, then the key,
// then the expire value, as in getexCommand. An EXAT or PXAT time that has
// already passed deletes the key.
func cmdGetex(c *command.Ctx, args [][]byte) resp.Reply {
	o, rep := parseStringOpts(args, false)
	if rep != nil {
		return rep
	}
	key := args[1]
	e, rep := getString(c, key)
	switch {
	case rep != nil:
		return rep
	case !e.live:
		return resp.Null()
	}
	var when int64
	if o.expire != nil {
		if when, rep = expireAt(c, o); rep != nil {
			return rep
		}
	}
	m := e.m
	switch {
	case o.flags&(optEXAT|optPXAT) != 0 && when <= c.NowMS():
		deleteKey(c, c.DB, key, e)
	case o.expire != nil:
		m.ExpireMS = when
		writeMeta(c, c.DB, key, m, e)
	case o.flags&optPersist != 0 && m.ExpireMS != 0:
		m.ExpireMS = 0
		writeMeta(c, c.DB, key, m, e)
	}
	return resp.Bulk(e.m.Payload)
}

func cmdMget(c *command.Ctx, args [][]byte) resp.Reply {
	keys := make([][]byte, len(args)-1)
	for i, k := range args[1:] {
		keys[i] = layout.MetaKey(nil, c.DB, k)
	}
	vals, err := c.Reader().MultiGet(store.CFMeta, keys)
	if err != nil {
		return storeErr(err)
	}
	now := c.NowMS()
	out := make([][]byte, len(vals))
	for i, v := range vals {
		if v == nil {
			continue
		}
		m, err := layout.DecodeMeta(v)
		if err != nil {
			return storeErr(err)
		}
		if m.Type == layout.TString && !m.Expired(now) {
			out[i] = m.Payload
		}
	}
	return bulkArray(out)
}

func cmdMset(c *command.Ctx, args [][]byte) resp.Reply {
	if len(args)%2 == 0 {
		return resp.ErrArity(c.Spec.FullName())
	}
	if rep := msetAll(c, args); rep != nil {
		return rep
	}
	return resp.OK()
}

func cmdMsetnx(c *command.Ctx, args [][]byte) resp.Reply {
	if len(args)%2 == 0 {
		return resp.ErrArity(c.Spec.FullName())
	}
	for j := 1; j < len(args); j += 2 {
		_, ok, err := loadMeta(c, args[j])
		if err != nil {
			return storeErr(err)
		}
		if ok {
			return zero
		}
	}
	if rep := msetAll(c, args); rep != nil {
		return rep
	}
	return one
}

// msetAll writes every key/value pair of MSET without TTL. A key named
// twice is written twice; the later Put wins inside the batch.
func msetAll(c *command.Ctx, args [][]byte) resp.Reply {
	for j := 1; j < len(args); j += 2 {
		e, err := getEntry(c, args[j])
		if err != nil {
			return storeErr(err)
		}
		writeMeta(c, c.DB, args[j], layout.Meta{Type: layout.TString, Payload: args[j+1]}, e)
	}
	return nil
}

func cmdIncr(c *command.Ctx, args [][]byte) resp.Reply { return incrDecr(c, args[1], 1) }
func cmdDecr(c *command.Ctx, args [][]byte) resp.Reply { return incrDecr(c, args[1], -1) }

func cmdIncrby(c *command.Ctx, args [][]byte) resp.Reply {
	n, ok := resp.ParseInt(args[2])
	if !ok {
		return resp.ErrNotInteger
	}
	return incrDecr(c, args[1], n)
}

func cmdDecrby(c *command.Ctx, args [][]byte) resp.Reply {
	n, ok := resp.ParseInt(args[2])
	if !ok {
		return resp.ErrNotInteger
	}
	if n == math.MinInt64 {
		return resp.Err("ERR decrement would overflow")
	}
	return incrDecr(c, args[1], -n)
}

// incrDecr is incrDecrCommand. The TTL survives; an absent or expired
// key counts from 0 and gets none.
func incrDecr(c *command.Ctx, key []byte, incr int64) resp.Reply {
	e, rep := getString(c, key)
	if rep != nil {
		return rep
	}
	var value int64
	if e.live {
		v, ok := resp.ParseInt(e.m.Payload)
		if !ok {
			return resp.ErrNotInteger
		}
		value = v
	}
	if (incr < 0 && value < 0 && incr < math.MinInt64-value) ||
		(incr > 0 && value > 0 && incr > math.MaxInt64-value) {
		return resp.ErrOverflow
	}
	value += incr
	m := layout.Meta{Type: layout.TString, Payload: strconv.AppendInt(nil, value, 10)}
	if e.live {
		m.ExpireMS = e.m.ExpireMS
	}
	writeMeta(c, c.DB, key, m, e)
	return resp.Int(value)
}

// cmdIncrbyfloat stores and replies the sum as incrByFloat prints it.
func cmdIncrbyfloat(c *command.Ctx, args [][]byte) resp.Reply {
	key := args[1]
	e, rep := getString(c, key)
	if rep != nil {
		return rep
	}
	s, rep := incrByFloat(e.m.Payload, e.live, args[2])
	if rep != nil {
		return rep
	}
	m := layout.Meta{Type: layout.TString, Payload: []byte(s)}
	if e.live {
		m.ExpireMS = e.m.ExpireMS
	}
	writeMeta(c, c.DB, key, m, e)
	return resp.Str(s)
}

// Redis adds INCRBYFLOAT and HINCRBYFLOAT operands in long double and
// prints the sum with "%.17Lf" (ld2string, LD_STR_HUMAN). On x86-64 Linux,
// the platform Redis's replies are defined on, long double is x87 extended
// precision: a 64-bit significand, rounded to nearest with ties to even.
// float64 arithmetic gives -0.10000000000000009 for 1 plus -1.1 where
// Redis prints -0.1, so these helpers redo the arithmetic in big.Float at
// that precision: strtold's correctly rounded parse, one rounded addition,
// and the exact decimal expansion rounded to 17 places.
const ldPrec = 64

var (
	// ldMax is LDBL_MAX, (2 - 2^-63) * 2^16383.
	ldMax = new(big.Float).SetMantExp(
		new(big.Float).SetPrec(ldPrec).Sub(big.NewFloat(2), new(big.Float).SetMantExp(big.NewFloat(1), -63)), 16383)
	// ldTrueMin is the smallest positive long double, 2^-16445.
	ldTrueMin = new(big.Float).SetMantExp(big.NewFloat(1), -16445)
)

// incrByFloat adds incr to the stored number cur (0 when !have) and
// returns the text Redis stores and replies.
func incrByFloat(cur []byte, have bool, incr []byte) (string, resp.Reply) {
	value := new(big.Float).SetPrec(ldPrec)
	if have {
		v, ok := parseLongDouble(cur)
		if !ok {
			return "", resp.ErrNotFloat
		}
		value = v
	}
	by, ok := parseLongDouble(incr)
	if !ok {
		return "", resp.ErrNotFloat
	}
	return addLongDouble(value, by)
}

// addLongDouble adds two long doubles as x87 does and prints the sum; an
// infinite operand or a sum past LDBL_MAX replies ERR increment would
// produce NaN or Infinity.
func addLongDouble(value, by *big.Float) (string, resp.Reply) {
	if value.IsInf() || by.IsInf() {
		return "", resp.ErrNaNOrInf
	}
	sum := new(big.Float).SetPrec(ldPrec).SetMode(big.ToNearestEven).Add(value, by)
	if new(big.Float).Abs(sum).Cmp(ldMax) > 0 {
		return "", resp.ErrNaNOrInf
	}
	return formatLongDoubleHuman(sum), nil
}

// parseLongDouble is string2ld on x86-64: it accepts what resp.ParseFloat
// accepts and rounds the exact value to a 64-bit significand. Decimal
// values beyond float64's range but inside long double's are accepted
// too, as strtold accepts them.
func parseLongDouble(b []byte) (*big.Float, bool) {
	f := new(big.Float).SetPrec(ldPrec).SetMode(big.ToNearestEven)
	v, ok := resp.ParseFloat(b)
	switch {
	case ok && math.IsInf(v, 0):
		return f.SetInf(v < 0), true
	case !ok && !plainDecimal(b):
		return nil, false
	}
	var r big.Rat
	if !smallExponent(b) {
		if ok {
			return f.SetFloat64(v), true
		}
		return nil, false
	}
	if _, parsed := r.SetString(string(b)); !parsed {
		if ok {
			return f.SetFloat64(v), true
		}
		return nil, false
	}
	f.SetRat(&r)
	if !ok {
		a := new(big.Float).Abs(f)
		if a.Cmp(ldMax) > 0 || (a.Sign() != 0 && a.Cmp(ldTrueMin) < 0) {
			return nil, false
		}
	}
	return f, true
}

// plainDecimal reports whether b is [sign] digits [. digits] [e [sign]
// digits] with at least one mantissa digit and nothing else.
func plainDecimal(b []byte) bool {
	i := 0
	if i < len(b) && (b[i] == '+' || b[i] == '-') {
		i++
	}
	digits := 0
	for ; i < len(b) && b[i] >= '0' && b[i] <= '9'; i++ {
		digits++
	}
	if i < len(b) && b[i] == '.' {
		for i++; i < len(b) && b[i] >= '0' && b[i] <= '9'; i++ {
			digits++
		}
	}
	if digits == 0 {
		return false
	}
	if i < len(b) && (b[i] == 'e' || b[i] == 'E') {
		i++
		if i < len(b) && (b[i] == '+' || b[i] == '-') {
			i++
		}
		start := i
		for i < len(b) && b[i] >= '0' && b[i] <= '9' {
			i++
		}
		if i == start {
			return false
		}
	}
	return i == len(b)
}

// smallExponent reports whether the exponent part, if any, has at most
// five significant digits, so big.Rat never builds a power of ten with
// millions of digits for input such as "1e999999999".
func smallExponent(b []byte) bool {
	s := b
	if len(s) > 0 && (s[0] == '+' || s[0] == '-') {
		s = s[1:]
	}
	expChars := "eE"
	if len(s) >= 2 && s[0] == '0' && (s[1] == 'x' || s[1] == 'X') {
		expChars = "pP"
	}
	i := bytes.IndexAny(s, expChars)
	if i < 0 {
		return true
	}
	exp := s[i+1:]
	if len(exp) > 0 && (exp[0] == '+' || exp[0] == '-') {
		exp = exp[1:]
	}
	return len(bytes.TrimLeft(exp, "0")) <= 5
}

// formatLongDoubleHuman is ld2string's LD_STR_HUMAN mode: "%.17Lf", then
// trailing zeros and a trailing '.' removed, and "-0" printed as "0".
func formatLongDoubleHuman(f *big.Float) string {
	s := f.Text('f', 17)
	if strings.IndexByte(s, '.') >= 0 {
		s = strings.TrimRight(s, "0")
		s = strings.TrimSuffix(s, ".")
	}
	if s == "-0" {
		s = "0"
	}
	return s
}

func cmdAppend(c *command.Ctx, args [][]byte) resp.Reply {
	key, add := args[1], args[2]
	e, rep := getString(c, key)
	if rep != nil {
		return rep
	}
	if !e.live {
		writeMeta(c, c.DB, key, layout.Meta{Type: layout.TString, Payload: add}, e)
		return resp.Int(int64(len(add)))
	}
	if int64(len(e.m.Payload))+int64(len(add)) > maxStringLen {
		return errStringTooLong
	}
	m := e.m
	m.Payload = append(e.m.Payload[:len(e.m.Payload):len(e.m.Payload)], add...)
	writeMeta(c, c.DB, key, m, e)
	return resp.Int(int64(len(m.Payload)))
}

func cmdStrlen(c *command.Ctx, args [][]byte) resp.Reply {
	e, rep := getString(c, args[1])
	switch {
	case rep != nil:
		return rep
	case !e.live:
		return zero
	}
	return resp.Int(int64(len(e.m.Payload)))
}

// cmdGetrange is GETRANGE and SUBSTR: both indexes are parsed before the
// key is read, and an absent key replies an empty bulk string.
func cmdGetrange(c *command.Ctx, args [][]byte) resp.Reply {
	start, ok := resp.ParseInt(args[2])
	if !ok {
		return resp.ErrNotInteger
	}
	end, ok := resp.ParseInt(args[3])
	if !ok {
		return resp.ErrNotInteger
	}
	e, rep := getString(c, args[1])
	switch {
	case rep != nil:
		return rep
	case !e.live:
		return emptyBulk
	}
	s := e.m.Payload
	n := int64(len(s))
	if start < 0 && end < 0 && start > end {
		return emptyBulk
	}
	if start < 0 {
		start += n
	}
	if end < 0 {
		end += n
	}
	start, end = max(start, 0), max(end, 0)
	if end >= n {
		end = n - 1
	}
	if start > end || n == 0 {
		return emptyBulk
	}
	return resp.Bulk(s[start : end+1])
}

// cmdSetrange is setrangeCommand. An empty value on an absent key writes
// nothing and replies 0; on an existing string it replies the length.
func cmdSetrange(c *command.Ctx, args [][]byte) resp.Reply {
	key, val := args[1], args[3]
	offset, ok := resp.ParseInt(args[2])
	if !ok {
		return resp.ErrNotInteger
	}
	if offset < 0 {
		return errOffsetRange
	}
	e, rep := getString(c, key)
	if rep != nil {
		return rep
	}
	var old []byte
	if e.live {
		old = e.m.Payload
	}
	if len(val) == 0 {
		return resp.Int(int64(len(old)))
	}
	if offset > maxStringLen-int64(len(val)) {
		return errStringTooLong
	}
	end := int(offset) + len(val)
	buf := make([]byte, max(len(old), end))
	copy(buf, old)
	copy(buf[offset:], val)
	m := layout.Meta{Type: layout.TString, Payload: buf}
	if e.live {
		m.ExpireMS = e.m.ExpireMS
	}
	writeMeta(c, c.DB, key, m, e)
	return resp.Int(int64(len(buf)))
}
