package resp

import "strings"

// ProtocolError is a malformed request. Text is the exact message Redis
// 7.2 sends, such as "Protocol error: invalid multibulk length"; the server
// replies "-ERR " followed by Text and closes the connection.
type ProtocolError struct {
	Text string
}

func (e *ProtocolError) Error() string { return e.Text }

// Reply returns the error reply the server sends before closing.
func (e *ProtocolError) Reply() Reply { return Err("ERR " + e.Text) }

// The shared Redis error replies. Each is an Err value, so a handler
// returns it as is and a test compares a reply against it with ==.
var (
	ErrWrongType      = Err("WRONGTYPE Operation against a key holding the wrong kind of value")
	ErrSyntax         = Err("ERR syntax error")
	ErrNoSuchKey      = Err("ERR no such key")
	ErrOutOfRange     = Err("ERR index out of range")
	ErrNotInteger     = Err("ERR value is not an integer or out of range")
	ErrNotFloat       = Err("ERR value is not a valid float")
	ErrOverflow       = Err("ERR increment or decrement would overflow")
	ErrNaNOrInf       = Err("ERR increment would produce NaN or Infinity")
	ErrNoAuth         = Err("NOAUTH Authentication required.")
	ErrWrongPass      = Err("WRONGPASS invalid username-password pair or user is disabled.")
	ErrExecAbort      = Err("EXECABORT Transaction discarded because of previous errors.")
	ErrNestedMulti    = Err("ERR MULTI calls can not be nested")
	ErrExecNoMulti    = Err("ERR EXEC without MULTI")
	ErrDiscardNoMulti = Err("ERR DISCARD without MULTI")
	ErrWatchInMulti   = Err("ERR WATCH inside MULTI is not allowed")
	ErrDBIndex        = Err("ERR DB index is out of range")
	ErrInvalidCursor  = Err("ERR invalid cursor")
	ErrNoProto        = Err("NOPROTO unsupported protocol version")
)

// ErrArity is commandCheckArity's reply: "ERR wrong number of arguments
// for '<name>' command". Pass the command's full name ("get", or
// "client|setname" for a subcommand); ASCII letters are lowercased, as
// Redis prints the declared name rather than what the client typed.
func ErrArity(name string) Reply {
	return Err("ERR wrong number of arguments for '" + strings.ToLower(name) + "' command")
}

// ErrUnknown is commandCheckExistence's reply for a name that is not a
// command: "ERR unknown command '<name>', with args beginning with: 'a'
// 'b' ". name is printed as the client sent it; args are the arguments
// after the name (argv[1:]). As in Redis, the name is cut to 128 bytes,
// the quoted arguments stop once 128 bytes of them are written (the last
// one cut to fit), each string also stops at a NUL byte as a C %s does,
// and the text keeps its trailing space.
func ErrUnknown(name string, args [][]byte) Reply {
	var b strings.Builder
	b.WriteString("ERR unknown command '")
	b.WriteString(cString(name, 128))
	b.WriteString("', with args beginning with: ")
	n := 0
	for _, a := range args {
		if n >= 128 {
			break
		}
		s := cString(string(a), 128-n)
		b.WriteByte('\'')
		b.WriteString(s)
		b.WriteString("' ")
		n += len(s) + 3
	}
	return Err(b.String())
}

// ErrUnknownSubcommand is the reply for a known container command with an
// unknown subcommand: "ERR unknown subcommand '<sub>'. Try <CMD> HELP.",
// with sub cut to 128 bytes and cmd uppercased.
func ErrUnknownSubcommand(sub, cmd string) Reply {
	return Err("ERR unknown subcommand '" + cString(sub, 128) + "'. Try " + strings.ToUpper(cmd) + " HELP.")
}

// cString returns s as C's "%.<n>s" prints it: at most n bytes, ending
// early at a NUL byte.
func cString(s string, n int) string {
	if i := strings.IndexByte(s, 0); i >= 0 {
		s = s[:i]
	}
	if len(s) > n {
		s = s[:n]
	}
	return s
}
