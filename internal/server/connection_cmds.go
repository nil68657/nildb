package server

import (
	"crypto/subtle"
	"errors"
	"fmt"
	"math"
	"net"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/nil68657/nildb/internal/command"
	"github.com/nil68657/nildb/internal/config"
	"github.com/nil68657/nildb/internal/resp"
	"github.com/nil68657/nildb/internal/store"
)

// Error texts of the connection commands, as Redis 7.2 words them.
var (
	errAuthNoPass = resp.Err("ERR AUTH <password> called without any password configured for the default user. Are you sure your configuration is correct?")
	errHelloAuth  = resp.Err("NOAUTH HELLO must be called with the client already authenticated, otherwise the HELLO <proto> AUTH <user> <pass> option can be used to authenticate the client and select the RESP protocol version at the same time")
	errHelloProto = resp.Err("ERR Protocol version is not an integer or out of range")
	errClientName = resp.Err("ERR Client names cannot contain spaces, newlines or special characters.")
)

func (s *Server) connectionSpecs() []command.Spec {
	const conn = "connection"
	return []command.Spec{
		{Name: "ping", Arity: -1, Flags: command.Fast, Group: conn, Since: "1.0.0",
			Summary: "Returns the server's liveliness response.", Run: cmdPing},
		{Name: "echo", Arity: 2, Flags: command.Fast, Group: conn, Since: "1.0.0",
			Summary: "Returns the given string.", Run: cmdEcho},
		{Name: "quit", Arity: -1, Flags: command.NoAuth | command.Fast, Group: conn, Since: "1.0.0",
			Summary: "Closes the connection.", Run: cmdQuit},
		{Name: "select", Arity: 2, Flags: command.Fast, Group: conn, Since: "1.0.0",
			Summary: "Changes the selected database.", Run: cmdSelect},
		{Name: "auth", Arity: -2, Flags: command.NoAuth | command.Fast, Group: conn, Since: "1.0.0",
			Summary: "Authenticates the connection.", Run: s.cmdAuth},
		{Name: "hello", Arity: -1, Flags: command.NoAuth | command.Fast, Group: conn, Since: "6.0.0",
			Summary: "Handshakes with the Redis server.", Run: s.cmdHello},
		s.clientSpec(),
		s.commandSpec(),
		s.configSpec(),
		{Name: "time", Arity: 1, Flags: command.ReadOnly | command.Fast, Group: "server", Since: "2.6.0",
			Summary: "Returns the server time.", Run: cmdTime},
		{Name: "shutdown", Arity: -1, Flags: command.Admin | command.NoMulti, Group: "server", Since: "1.0.0",
			Summary: "Stops the server.", Run: s.cmdShutdown},
	}
}

func cmdPing(_ *command.Ctx, args [][]byte) resp.Reply {
	if len(args) > 2 {
		return resp.ErrArity("ping")
	}
	if len(args) == 2 {
		return resp.Bulk(args[1])
	}
	return resp.Status("PONG")
}

func cmdEcho(_ *command.Ctx, args [][]byte) resp.Reply { return resp.Bulk(args[1]) }

func cmdQuit(ctx *command.Ctx, _ [][]byte) resp.Reply {
	ctx.Conn.Close()
	return resp.OK()
}

// parseDB parses a SELECT argument into a database number.
func parseDB(b []byte) (uint8, bool) {
	n, ok := resp.ParseInt(b)
	if !ok || n < 0 || n >= config.Databases {
		return 0, false
	}
	return uint8(n), true
}

func cmdSelect(ctx *command.Ctx, args [][]byte) resp.Reply {
	n, ok := resp.ParseInt(args[1])
	if !ok || n < math.MinInt32 || n > math.MaxInt32 {
		return resp.ErrNotInteger
	}
	if n < 0 || n >= config.Databases {
		return resp.ErrDBIndex
	}
	ctx.Conn.SetDB(uint8(n))
	return resp.OK()
}

// checkPass reports whether user and pass log in. The only user is
// "default"; without --requirepass it accepts any password.
func (s *Server) checkPass(user string, pass []byte) bool {
	if user != "default" {
		return false
	}
	if s.cfg.RequirePass == "" {
		return true
	}
	return subtle.ConstantTimeCompare(pass, []byte(s.cfg.RequirePass)) == 1
}

// cmdAuth is AUTH [username] password.
func (s *Server) cmdAuth(ctx *command.Ctx, args [][]byte) resp.Reply {
	if len(args) > 3 {
		return resp.ErrSyntax
	}
	user, pass := "default", args[1]
	if len(args) == 2 {
		if s.cfg.RequirePass == "" {
			return errAuthNoPass
		}
	} else {
		user, pass = string(args[1]), args[2]
	}
	if !s.checkPass(user, pass) {
		return resp.ErrWrongPass
	}
	ctx.Conn.SetAuthenticated(true)
	return resp.OK()
}

// validClientAttr is Redis's validateClientAttr: every byte in '!'..'~'.
func validClientAttr(b []byte) bool {
	for _, c := range b {
		if c < '!' || c > '~' {
			return false
		}
	}
	return true
}

func equalFold(b []byte, s string) bool { return strings.EqualFold(string(b), s) }

// cmdHello is HELLO [protover [AUTH username password] [SETNAME name]].
func (s *Server) cmdHello(ctx *command.Ctx, args [][]byte) resp.Reply {
	c := ctx.Conn
	var ver int64
	next := 1
	if len(args) >= 2 {
		v, ok := resp.ParseInt(args[1])
		if !ok {
			return errHelloProto
		}
		if v < 2 || v > 3 {
			return resp.ErrNoProto
		}
		ver, next = v, 2
	}
	var user, pass, name []byte
	auth, setName := false, false
	for j := next; j < len(args); j++ {
		more := len(args) - 1 - j
		switch opt := args[j]; {
		case equalFold(opt, "auth") && more >= 2:
			user, pass, auth = args[j+1], args[j+2], true
			j += 2
		case equalFold(opt, "setname") && more >= 1:
			name, setName = args[j+1], true
			if !validClientAttr(name) {
				return errClientName
			}
			j++
		default:
			return resp.Errorf("ERR Syntax error in HELLO option '%s'", cStr(opt))
		}
	}
	if auth {
		if !s.checkPass(string(user), pass) {
			return resp.ErrWrongPass
		}
		c.SetAuthenticated(true)
	}
	if !c.Authenticated() {
		return errHelloAuth
	}
	if setName {
		c.SetName(string(name))
	}
	if ver != 0 {
		c.SetProto(int(ver))
	}
	return resp.Map(
		resp.Str("server"), resp.Str("redis"),
		resp.Str("version"), resp.Str(s.cfg.RedisVersion),
		resp.Str("proto"), resp.Int(int64(c.Proto())),
		resp.Str("id"), resp.Int(int64(c.ID())),
		resp.Str("mode"), resp.Str("standalone"),
		resp.Str("role"), resp.Str("master"),
		resp.Str("modules"), resp.Array(),
	)
}

// cStr is b as C's %s prints it: up to the first NUL byte.
func cStr(b []byte) string {
	for i, c := range b {
		if c == 0 {
			return string(b[:i])
		}
	}
	return string(b)
}

// helpReply is Redis's addReplyHelp: a title line, the given lines, and
// the HELP entry, each a status reply.
func helpReply(cmd string, lines ...string) resp.Reply {
	out := []resp.Reply{resp.Status(cmd + " <subcommand> [<arg> [value] [opt] ...]. Subcommands are:")}
	for _, l := range lines {
		out = append(out, resp.Status(l))
	}
	out = append(out, resp.Status("HELP"), resp.Status("    Print this help."))
	return resp.Array(out...)
}

func (s *Server) clientSpec() command.Spec {
	sub := func(name string, arity int, flags command.Flags, summary string, run command.Handler) command.Spec {
		return command.Spec{Name: name, Arity: arity, Flags: flags, Summary: summary, Run: run}
	}
	return command.Spec{
		Name: "client", Group: "connection", Since: "2.4.0",
		Summary: "A container for client connection commands.",
		Subcommands: []command.Spec{
			sub("id", 2, command.Fast, "Returns the unique client ID of the connection.", func(ctx *command.Ctx, _ [][]byte) resp.Reply {
				return resp.Int(int64(ctx.Conn.ID()))
			}),
			sub("setname", 3, command.Fast, "Sets the connection name.", func(ctx *command.Ctx, args [][]byte) resp.Reply {
				if !validClientAttr(args[2]) {
					return errClientName
				}
				ctx.Conn.SetName(string(args[2]))
				return resp.OK()
			}),
			sub("getname", 2, command.Fast, "Returns the name of the connection.", func(ctx *command.Ctx, _ [][]byte) resp.Reply {
				if n := ctx.Conn.Name(); n != "" {
					return resp.Str(n)
				}
				return resp.Null()
			}),
			sub("info", 2, 0, "Returns information about the connection.", func(ctx *command.Ctx, _ [][]byte) resp.Reply {
				return resp.Verbatim("txt", s.clientLine(ctx.Conn.(*conn), time.Now())+"\n")
			}),
			sub("list", -2, command.Admin, "Lists open connections.", s.cmdClientList),
			sub("setinfo", 4, 0, "Sets information specific to the client or connection.", cmdClientSetInfo),
			sub("no-evict", 3, command.Admin, "Accepted for compatibility; NilDB never evicts clients.", cmdOnOff),
			sub("no-touch", 3, 0, "Accepted for compatibility; NilDB keeps no LRU clock.", cmdOnOff),
			sub("help", 2, 0, "Returns helpful text about the different subcommands.", func(*command.Ctx, [][]byte) resp.Reply {
				return helpReply("CLIENT",
					"GETNAME", "    Return the name of the current connection.",
					"ID", "    Return the ID of the current connection.",
					"INFO", "    Return information about the current client connection.",
					"LIST [TYPE (NORMAL|MASTER|REPLICA|PUBSUB)] [ID <id> [<id> ...]]", "    Return information about client connections.",
					"SETINFO <option> <value>", "    Set client meta attr. Options are: LIB-NAME, LIB-VER.",
					"SETNAME <name>", "    Assign the name <name> to the current connection.",
					"NO-EVICT (ON|OFF)", "    Accepted and ignored.",
					"NO-TOUCH (ON|OFF)", "    Accepted and ignored.")
			}),
		},
	}
}

func cmdOnOff(_ *command.Ctx, args [][]byte) resp.Reply {
	if equalFold(args[2], "on") || equalFold(args[2], "off") {
		return resp.OK()
	}
	return resp.ErrSyntax
}

func cmdClientSetInfo(ctx *command.Ctx, args [][]byte) resp.Reply {
	attr, val := args[2], args[3]
	isName := equalFold(attr, "lib-name")
	if !isName && !equalFold(attr, "lib-ver") {
		return resp.Errorf("ERR Unrecognized option '%s'", cStr(attr))
	}
	if !validClientAttr(val) {
		return resp.Errorf("ERR %s cannot contain spaces, newlines or special characters.", cStr(attr))
	}
	name, ver := ctx.Conn.LibInfo()
	if isName {
		name = string(val)
	} else {
		ver = string(val)
	}
	ctx.Conn.SetLibInfo(name, ver)
	return resp.OK()
}

// cmdClientList is CLIENT LIST [TYPE type] [ID id ...].
func (s *Server) cmdClientList(_ *command.Ctx, args [][]byte) resp.Reply {
	var ids map[uint64]bool
	typeNone := false
	for j := 2; j < len(args); j++ {
		switch {
		case equalFold(args[j], "type") && j+1 < len(args):
			t := strings.ToLower(string(args[j+1]))
			switch t {
			case "normal":
			case "master", "replica", "slave", "pubsub":
				typeNone = true
			default:
				return resp.Errorf("ERR Unknown client type '%s'", cStr(args[j+1]))
			}
			j++
		case equalFold(args[j], "id") && j+1 < len(args):
			ids = make(map[uint64]bool)
			for j++; j < len(args); j++ {
				n, ok := resp.ParseInt(args[j])
				if !ok || n <= 0 {
					return resp.Err("ERR Invalid client ID")
				}
				ids[uint64(n)] = true
			}
		default:
			return resp.ErrSyntax
		}
	}
	var b strings.Builder
	if !typeNone {
		now := time.Now()
		for _, c := range s.connList() {
			if ids != nil && !ids[c.id] {
				continue
			}
			b.WriteString(s.clientLine(c, now))
			b.WriteByte('\n')
		}
	}
	return resp.Verbatim("txt", b.String())
}

// clientLine is one CLIENT LIST line in Redis 7.2's field order.
func (s *Server) clientLine(c *conn, now time.Time) string {
	name := c.Name()
	lib, ver := c.LibInfo()
	flags := ""
	multi := c.multiLen.Load()
	if multi >= 0 {
		flags += "x"
	}
	if c.watchDirty.Load() {
		flags += "d"
	}
	if flags == "" {
		flags = "N"
	}
	cmd := "NULL"
	if sp := c.lastSpec.Load(); sp != nil {
		cmd = sp.FullName()
	}
	idle := now.Sub(time.Unix(0, c.lastActive.Load())) / time.Second
	return fmt.Sprintf("id=%d addr=%s laddr=%s fd=0 name=%s age=%d idle=%d flags=%s db=%d sub=0 psub=0 ssub=0 multi=%d qbuf=0 qbuf-free=0 argv-mem=0 multi-mem=0 rbs=16384 rbp=0 obl=0 oll=0 omem=0 tot-mem=0 events=r cmd=%s user=default redir=-1 resp=%d lib-name=%s lib-ver=%s",
		c.id, c.addr, c.laddr, name, int64(now.Sub(c.created)/time.Second), int64(idle), flags, c.DB(), multi, cmd, c.Proto(), lib, ver)
}

func (s *Server) commandSpec() command.Spec {
	return command.Spec{
		Name: "command", Arity: -1, Group: "server", Since: "2.8.13",
		Summary: "Returns detailed information about all commands.",
		Run: func(*command.Ctx, [][]byte) resp.Reply {
			return command.CommandInfoReply(s.reg, nil)
		},
		Subcommands: []command.Spec{
			{Name: "count", Arity: 2, Summary: "Returns a count of commands.", Run: func(*command.Ctx, [][]byte) resp.Reply {
				return command.CommandCountReply(s.reg)
			}},
			{Name: "info", Arity: -2, Summary: "Returns information about one, multiple or all commands.", Run: func(_ *command.Ctx, args [][]byte) resp.Reply {
				return command.CommandInfoReply(s.reg, byteStrings(args[2:]))
			}},
			{Name: "docs", Arity: -2, Summary: "Returns documentary information about one, multiple or all commands.", Run: func(_ *command.Ctx, args [][]byte) resp.Reply {
				return command.CommandDocsReply(s.reg, byteStrings(args[2:]))
			}},
			{Name: "list", Arity: -2, Summary: "Returns a list of command names.", Run: s.cmdCommandList},
			{Name: "getkeys", Arity: -3, Summary: "Extracts the key names from an arbitrary command.", Run: s.cmdCommandGetKeys},
			{Name: "help", Arity: 2, Summary: "Returns helpful text about the different subcommands.", Run: func(*command.Ctx, [][]byte) resp.Reply {
				return helpReply("COMMAND",
					"(no subcommand)", "    Return details about all commands.",
					"COUNT", "    Return the total number of commands.",
					"DOCS [<command-name> ...]", "    Return documentation details about multiple commands.",
					"GETKEYS <full-command>", "    Return the keys from a full command.",
					"INFO [<command-name> ...]", "    Return details about multiple commands.",
					"LIST [FILTERBY (PATTERN <pattern>)]", "    Return a list of all commands.")
			}},
		},
	}
}

func byteStrings(bs [][]byte) []string {
	out := make([]string, len(bs))
	for i, b := range bs {
		out[i] = string(b)
	}
	return out
}

// cmdCommandList is COMMAND LIST [FILTERBY PATTERN pattern].
func (s *Server) cmdCommandList(_ *command.Ctx, args [][]byte) resp.Reply {
	switch {
	case len(args) == 2:
		return command.CommandListReply(s.reg)
	case len(args) == 5 && equalFold(args[2], "filterby") && equalFold(args[3], "pattern"):
		var out []resp.Reply
		for _, sp := range s.reg.Specs() {
			names := []string{sp.FullName()}
			for _, c := range sp.Subs() {
				names = append(names, c.FullName())
			}
			for _, n := range names {
				if globMatch(string(args[4]), n, true) {
					out = append(out, resp.Str(n))
				}
			}
		}
		return resp.Array(out...)
	}
	return resp.ErrSyntax
}

// cmdCommandGetKeys is COMMAND GETKEYS command [arg ...]: the Redis keys
// its KeysFunc would lock.
func (s *Server) cmdCommandGetKeys(ctx *command.Ctx, args [][]byte) resp.Reply {
	sub := args[2:]
	spec, rej := s.reg.Resolve(sub)
	switch {
	case spec == nil:
		return resp.Err("ERR Invalid command specified")
	case rej != nil:
		return resp.Err("ERR Invalid number of arguments specified for command")
	}
	keys, _, kr := s.keysOf(spec, ctx.DB, sub)
	if kr != nil {
		return kr
	}
	var out []resp.Reply
	for _, k := range keys {
		if k.Kind == store.LockRedis {
			out = append(out, resp.Bulk(k.Key))
		}
	}
	if len(out) == 0 {
		return resp.Err("ERR The command has no key arguments")
	}
	return resp.Array(out...)
}

func (s *Server) configSpec() command.Spec {
	return command.Spec{
		Name: "config", Group: "server", Since: "2.0.0",
		Summary: "A container for server configuration commands.",
		Subcommands: []command.Spec{
			{Name: "get", Arity: -3, Flags: command.Admin, Summary: "Returns the effective values of configuration parameters.", Run: s.cmdConfigGet},
			{Name: "set", Arity: -4, Flags: command.Admin, Summary: "Sets configuration parameters in-flight.", Run: s.cmdConfigSet},
			{Name: "resetstat", Arity: 2, Flags: command.Admin, Summary: "Resets the server's statistics.", Run: func(*command.Ctx, [][]byte) resp.Reply {
				s.stats.reset()
				return resp.OK()
			}},
			{Name: "help", Arity: 2, Summary: "Returns helpful text about the different subcommands.", Run: func(*command.Ctx, [][]byte) resp.Reply {
				return helpReply("CONFIG",
					"GET <pattern>", "    Return parameters matching the glob-like <pattern> and their values.",
					"SET <directive> <value>", "    Set the configuration <directive> to <value>. Only nildb.* runtime knobs can be set.",
					"RESETSTAT", "    Reset statistics reported by the INFO command.")
			}},
		},
	}
}

// redisConfig is what CONFIG GET reports for the Redis parameters clients
// and redis-benchmark ask about. NilDB has no RDB or AOF: durability is
// the RocksDB WAL, reported under nildb.fsync.
func (s *Server) redisConfig() [][2]string {
	host, port, err := net.SplitHostPort(s.cfg.Addr)
	if a := s.Addr(); a != nil {
		host, port, err = net.SplitHostPort(a.String())
	}
	if err != nil {
		host, port = "", "0"
	}
	return [][2]string{
		{"appendonly", "no"},
		{"bind", host},
		{"databases", strconv.Itoa(config.Databases)},
		{"dir", s.cfg.Dir},
		{"maxclients", "10000"},
		{"maxmemory", "0"},
		{"maxmemory-policy", "noeviction"},
		{"port", port},
		{"proto-max-bulk-len", strconv.FormatInt(resp.DefaultLimits().BulkMax, 10)},
		{"save", ""},
		{"timeout", "0"},
	}
}

// cmdConfigGet is CONFIG GET pattern [pattern ...]: the fixed Redis
// parameters plus the nildb.* knobs, as a map (a flat array in RESP2).
func (s *Server) cmdConfigGet(_ *command.Ctx, args [][]byte) resp.Reply {
	type kv struct{ k, v string }
	var found []kv
	seen := make(map[string]bool)
	fixed := s.redisConfig()
	knobs := config.KnobNames()
	for _, p := range args[2:] {
		pat := string(p)
		for _, f := range fixed {
			if !seen[f[0]] && globMatch(pat, f[0], true) {
				seen[f[0]] = true
				found = append(found, kv{f[0], f[1]})
			}
		}
		for _, n := range knobs {
			if !seen[n] && globMatch(pat, n, true) {
				if v, ok := s.cfg.Get(n); ok {
					seen[n] = true
					found = append(found, kv{n, v})
				}
			}
		}
	}
	sort.SliceStable(found, func(i, j int) bool { return found[i].k < found[j].k })
	out := make([]resp.Reply, 0, 2*len(found))
	for _, f := range found {
		out = append(out, resp.Str(f.k), resp.Str(f.v))
	}
	return resp.Map(out...)
}

// cmdConfigSet is CONFIG SET name value [name value ...]. Only nildb.*
// runtime knobs are settable. All pairs apply or none do.
func (s *Server) cmdConfigSet(_ *command.Ctx, args [][]byte) resp.Reply {
	pairs := args[2:]
	if len(pairs)%2 != 0 {
		return resp.ErrArity("config|set")
	}
	type undo struct{ name, old string }
	var done []undo
	rollback := func() {
		for i := len(done) - 1; i >= 0; i-- {
			_ = s.cfg.Set(done[i].name, done[i].old)
		}
	}
	for i := 0; i < len(pairs); i += 2 {
		name, val := string(pairs[i]), string(pairs[i+1])
		old, _ := s.cfg.Get(name)
		if err := s.cfg.Set(name, val); err != nil {
			rollback()
			if errors.Is(err, config.ErrUnknownOption) {
				return resp.Errorf("ERR Unknown option or number of arguments for CONFIG SET - '%s'", name)
			}
			return resp.Errorf("ERR CONFIG SET failed (possibly related to argument '%s') - %s", name, err.Error())
		}
		done = append(done, undo{name, old})
	}
	return resp.OK()
}

// cmdTime is TIME: seconds and microseconds of the configured clock.
func cmdTime(ctx *command.Ctx, _ [][]byte) resp.Reply {
	us := ctx.Now.UnixMicro()
	return resp.Array(
		resp.Str(strconv.FormatInt(us/1_000_000, 10)),
		resp.Str(strconv.FormatInt(us%1_000_000, 10)),
	)
}

// cmdShutdown is SHUTDOWN [NOSAVE|SAVE] [NOW] [FORCE]. It starts a
// graceful shutdown and closes the connection without a reply, as Redis
// does when it exits.
func (s *Server) cmdShutdown(ctx *command.Ctx, args [][]byte) resp.Reply {
	for _, a := range args[1:] {
		switch {
		case equalFold(a, "nosave"), equalFold(a, "save"), equalFold(a, "now"), equalFold(a, "force"):
		case equalFold(a, "abort"):
			return resp.Err("ERR No shutdown in progress.")
		default:
			return resp.ErrSyntax
		}
	}
	c := ctx.Conn.(*conn)
	c.noReply = true
	c.Close()
	s.Shutdown()
	return nil
}
