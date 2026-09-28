package redis

import (
	"math"

	"github.com/nil68657/nildb/internal/command"
	"github.com/nil68657/nildb/internal/resp"
)

func registerExpire(r *command.Registry) {
	const g = "generic"
	w, ro, f := command.Write, command.ReadOnly, command.Fast
	r.Register(
		command.Spec{Name: "expire", Arity: -3, Flags: w | f, FirstKey: 1, LastKey: 1, KeyStep: 1, Group: g, Since: "1.0.0",
			Summary: "Sets the expiration time of a key in seconds.", Run: cmdExpire},
		command.Spec{Name: "pexpire", Arity: -3, Flags: w | f, FirstKey: 1, LastKey: 1, KeyStep: 1, Group: g, Since: "2.6.0",
			Summary: "Sets the expiration time of a key in milliseconds.", Run: cmdPexpire},
		command.Spec{Name: "expireat", Arity: -3, Flags: w | f, FirstKey: 1, LastKey: 1, KeyStep: 1, Group: g, Since: "1.2.0",
			Summary: "Sets the expiration time of a key to a Unix timestamp.", Run: cmdExpireat},
		command.Spec{Name: "pexpireat", Arity: -3, Flags: w | f, FirstKey: 1, LastKey: 1, KeyStep: 1, Group: g, Since: "2.6.0",
			Summary: "Sets the expiration time of a key to a Unix milliseconds timestamp.", Run: cmdPexpireat},
		command.Spec{Name: "ttl", Arity: 2, Flags: ro | f, FirstKey: 1, LastKey: 1, KeyStep: 1, Group: g, Since: "1.0.0",
			Summary: "Returns the expiration time in seconds of a key.", Run: cmdTTL},
		command.Spec{Name: "pttl", Arity: 2, Flags: ro | f, FirstKey: 1, LastKey: 1, KeyStep: 1, Group: g, Since: "2.6.0",
			Summary: "Returns the expiration time in milliseconds of a key.", Run: cmdPTTL},
		command.Spec{Name: "expiretime", Arity: 2, Flags: ro | f, FirstKey: 1, LastKey: 1, KeyStep: 1, Group: g, Since: "7.0.0",
			Summary: "Returns the expiration time of a key as a Unix timestamp.", Run: cmdExpiretime},
		command.Spec{Name: "pexpiretime", Arity: 2, Flags: ro | f, FirstKey: 1, LastKey: 1, KeyStep: 1, Group: g, Since: "7.0.0",
			Summary: "Returns the expiration time of a key as a Unix milliseconds timestamp.", Run: cmdPexpiretime},
		command.Spec{Name: "persist", Arity: 2, Flags: w | f, FirstKey: 1, LastKey: 1, KeyStep: 1, Group: g, Since: "2.2.0",
			Summary: "Removes the expiration time of a key.", Run: cmdPersist},
	)
}

func cmdExpire(c *command.Ctx, args [][]byte) resp.Reply {
	return expireGeneric(c, args, c.NowMS(), false)
}

func cmdPexpire(c *command.Ctx, args [][]byte) resp.Reply {
	return expireGeneric(c, args, c.NowMS(), true)
}

func cmdExpireat(c *command.Ctx, args [][]byte) resp.Reply { return expireGeneric(c, args, 0, false) }

func cmdPexpireat(c *command.Ctx, args [][]byte) resp.Reply { return expireGeneric(c, args, 0, true) }

// Condition bits of EXPIRE and friends.
const (
	expNX = 1 << iota
	expXX
	expGT
	expLT
)

// parseExpireFlags is parseExtendedExpireArgumentsOrReply.
func parseExpireFlags(args [][]byte) (int, resp.Reply) {
	flags := 0
	for _, opt := range args {
		switch {
		case equalFold(opt, "nx"):
			flags |= expNX
		case equalFold(opt, "xx"):
			flags |= expXX
		case equalFold(opt, "gt"):
			flags |= expGT
		case equalFold(opt, "lt"):
			flags |= expLT
		default:
			return 0, resp.Err("ERR Unsupported option " + string(opt))
		}
	}
	if flags&expNX != 0 && flags&(expXX|expGT|expLT) != 0 {
		return 0, resp.Err("ERR NX and XX, GT or LT options at the same time are not compatible")
	}
	if flags&expGT != 0 && flags&expLT != 0 {
		return 0, resp.Err("ERR GT and LT options at the same time are not compatible")
	}
	return flags, nil
}

// expireGeneric is expireGenericCommand. basetime is the command's clock
// in milliseconds for the relative forms and 0 for the *AT forms. A time
// that is not in the future deletes the key and replies 1, as Redis does.
func expireGeneric(c *command.Ctx, args [][]byte, basetime int64, ms bool) resp.Reply {
	key := args[1]
	flags, rep := parseExpireFlags(args[3:])
	if rep != nil {
		return rep
	}
	when, ok := resp.ParseInt(args[2])
	if !ok {
		return resp.ErrNotInteger
	}
	if !ms {
		if when > math.MaxInt64/1000 || when < math.MinInt64/1000 {
			return errExpireTime(c)
		}
		when *= 1000
	}
	if when > math.MaxInt64-basetime {
		return errExpireTime(c)
	}
	when += basetime
	e, err := getEntry(c, key)
	if err != nil {
		return storeErr(err)
	}
	if !e.live {
		return zero
	}
	cur := e.m.ExpireMS // 0 is "no expiry", Redis's -1
	switch {
	case flags&expNX != 0 && cur != 0,
		flags&expXX != 0 && cur == 0,
		flags&expGT != 0 && (cur == 0 || when <= cur),
		flags&expLT != 0 && cur != 0 && when >= cur:
		return zero
	}
	if when <= c.NowMS() {
		deleteKey(c, c.DB, key, e)
		return one
	}
	m := e.m
	m.ExpireMS = when
	writeMeta(c, c.DB, key, m, e)
	return one
}

func cmdTTL(c *command.Ctx, args [][]byte) resp.Reply  { return ttlGeneric(c, args[1], false, false) }
func cmdPTTL(c *command.Ctx, args [][]byte) resp.Reply { return ttlGeneric(c, args[1], true, false) }
func cmdExpiretime(c *command.Ctx, args [][]byte) resp.Reply {
	return ttlGeneric(c, args[1], false, true)
}
func cmdPexpiretime(c *command.Ctx, args [][]byte) resp.Reply {
	return ttlGeneric(c, args[1], true, true)
}

// ttlGeneric is ttlGenericCommand: -2 for an absent key, -1 for a key
// without expiry, otherwise the remaining time (or the absolute time with
// abs), seconds rounded to nearest.
func ttlGeneric(c *command.Ctx, key []byte, ms, abs bool) resp.Reply {
	m, ok, err := loadMeta(c, key)
	switch {
	case err != nil:
		return storeErr(err)
	case !ok:
		return resp.Int(-2)
	case m.ExpireMS == 0:
		return resp.Int(-1)
	}
	ttl := m.ExpireMS
	if !abs {
		ttl -= c.NowMS()
	}
	ttl = max(ttl, 0)
	if !ms {
		ttl = (ttl + 500) / 1000
	}
	return resp.Int(ttl)
}

func cmdPersist(c *command.Ctx, args [][]byte) resp.Reply {
	e, err := getEntry(c, args[1])
	switch {
	case err != nil:
		return storeErr(err)
	case !e.live || e.m.ExpireMS == 0:
		return zero
	}
	m := e.m
	m.ExpireMS = 0
	writeMeta(c, c.DB, args[1], m, e)
	return one
}
