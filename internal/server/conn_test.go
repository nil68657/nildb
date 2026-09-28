package server

import (
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/nil68657/nildb/internal/config"
)

func TestPingAndEcho(t *testing.T) {
	s := newTestServer(t, nil)
	p := s.pipe(t)
	p.send([]byte("PING\r\n"))
	p.expect("+PONG\r\n")
	p.do("+PONG\r\n", "ping")
	p.do("$5\r\nhello\r\n", "PING", "hello")
	p.do("-ERR wrong number of arguments for 'ping' command\r\n", "PING", "a", "b")
	p.do("$3\r\nabc\r\n", "echo", "abc")
	p.do("-ERR wrong number of arguments for 'echo' command\r\n", "ECHO")
}

func TestPipelinedRepliesLeaveInOneWrite(t *testing.T) {
	s := newTestServer(t, nil)
	p := s.pipe(t)
	p.do("+PONG\r\n", "PING") // settle the connection
	before := p.server.writes.Load()
	var batch []byte
	for i := range 50 {
		batch = append(batch, encodeArgs("ECHO", strconv.Itoa(i))...)
	}
	p.send(batch)
	var want strings.Builder
	for i := range 50 {
		s := strconv.Itoa(i)
		fmt.Fprintf(&want, "$%d\r\n%s\r\n", len(s), s)
	}
	p.expect(want.String())
	if n := p.server.writes.Load() - before; n != 1 {
		t.Errorf("50 pipelined replies took %d writes, want 1", n)
	}
	p.send([]byte("PING\r\nPING\r\nPING\r\n"))
	p.expect("+PONG\r\n+PONG\r\n+PONG\r\n")
}

func TestUnknownCommandAndArityTexts(t *testing.T) {
	s := newTestServer(t, nil)
	p := s.pipe(t)
	p.do("-ERR unknown command 'foobar', with args beginning with: 'a' 'b c' \r\n", "foobar", "a", "b c")
	p.do("-ERR unknown command 'NOPE', with args beginning with: \r\n", "NOPE")
	p.do("-ERR wrong number of arguments for 'get' command\r\n", "GET")
	p.do("-ERR wrong number of arguments for 'client|setname' command\r\n", "CLIENT", "SETNAME")
	p.do("-ERR wrong number of arguments for 'client' command\r\n", "client")
	p.do("-ERR unknown subcommand 'nope'. Try CLIENT HELP.\r\n", "client", "nope")
	p.do("-ERR unknown subcommand 'x'. Try CONFIG HELP.\r\n", "Config", "x")
}

func TestProtocolErrorClosesConnection(t *testing.T) {
	s := newTestServer(t, nil)
	p := s.pipe(t)
	p.send([]byte("*1\r\n$x\r\n"))
	p.expect("-ERR Protocol error: invalid bulk length\r\n")
	p.expectClosed()

	p2 := s.pipe(t)
	p2.send([]byte("*3\r\n$3\r\nSET\r\n+x\r\n"))
	p2.expect("-ERR Protocol error: expected '$', got '+'\r\n")
	p2.expectClosed()
}

func TestQuit(t *testing.T) {
	s := newTestServer(t, nil)
	p := s.pipe(t)
	p.send([]byte("QUIT\r\nPING\r\n"))
	p.expect("+OK\r\n")
	p.expectClosed()
}

func TestSelect(t *testing.T) {
	s := newTestServer(t, nil)
	p := s.pipe(t)
	p.do("+OK\r\n", "SET", "k", "db0")
	p.do("+OK\r\n", "SELECT", "9")
	p.do("$-1\r\n", "GET", "k")
	p.do("+OK\r\n", "SET", "k", "db9")
	p.do("+OK\r\n", "SELECT", "0")
	p.do("$3\r\ndb0\r\n", "GET", "k")
	p.do("-ERR DB index is out of range\r\n", "SELECT", "16")
	p.do("-ERR DB index is out of range\r\n", "SELECT", "-1")
	p.do("-ERR value is not an integer or out of range\r\n", "SELECT", "x")
	p.do("-ERR value is not an integer or out of range\r\n", "SELECT", "99999999999")
}

func helloMap(proto, id int, ver string, resp3 bool) string {
	head := "*14\r\n"
	if resp3 {
		head = "%7\r\n"
	}
	return head + "$6\r\nserver\r\n$5\r\nredis\r\n" +
		"$7\r\nversion\r\n$" + strconv.Itoa(len(ver)) + "\r\n" + ver + "\r\n" +
		"$5\r\nproto\r\n:" + strconv.Itoa(proto) + "\r\n" +
		"$2\r\nid\r\n:" + strconv.Itoa(id) + "\r\n" +
		"$4\r\nmode\r\n$10\r\nstandalone\r\n" +
		"$4\r\nrole\r\n$6\r\nmaster\r\n" +
		"$7\r\nmodules\r\n*0\r\n"
}

func TestHelloBytes(t *testing.T) {
	s := newTestServer(t, nil)
	p := s.pipe(t)
	p.send(encodeArgs("CLIENT", "ID"))
	p.expect(":1\r\n")
	p.do(helloMap(2, 1, "7.2.0", false), "HELLO", "2")
	p.do(helloMap(3, 1, "7.2.0", true), "HELLO", "3")
	// The connection now speaks RESP3: nulls and maps change shape.
	p.do("_\r\n", "GET", "missing")
	p.do(helloMap(3, 1, "7.2.0", true), "HELLO")
	p.do(helloMap(2, 1, "7.2.0", false), "HELLO", "2")
	p.do("$-1\r\n", "GET", "missing")
	p.do("-NOPROTO unsupported protocol version\r\n", "HELLO", "4")
	p.do("-NOPROTO unsupported protocol version\r\n", "HELLO", "1")
	p.do("-ERR Protocol version is not an integer or out of range\r\n", "HELLO", "three")
	p.do("-ERR Syntax error in HELLO option 'bogus'\r\n", "HELLO", "3", "bogus")
	p.do(helloMap(2, 1, "7.2.0", false), "HELLO", "2", "SETNAME", "myname")
	p.do("$6\r\nmyname\r\n", "CLIENT", "GETNAME")
	p.do("-ERR Client names cannot contain spaces, newlines or special characters.\r\n", "HELLO", "2", "SETNAME", "my name")
}

func TestHelloReportsConfiguredVersion(t *testing.T) {
	s := newTestServer(t, func(c *config.Config) { c.RedisVersion = "7.2.4" })
	p := s.pipe(t)
	p.do(helloMap(2, 1, "7.2.4", false), "HELLO", "2")
}

func TestAuthFlowsWithPassword(t *testing.T) {
	s := newTestServer(t, func(c *config.Config) { c.RequirePass = "s3cret" })
	p := s.pipe(t)
	p.do("-NOAUTH Authentication required.\r\n", "PING")
	p.do("-NOAUTH Authentication required.\r\n", "GET", "k")
	p.do("-ERR unknown command 'nosuch', with args beginning with: \r\n", "nosuch")
	p.do("-WRONGPASS invalid username-password pair or user is disabled.\r\n", "AUTH", "wrong")
	p.do("-WRONGPASS invalid username-password pair or user is disabled.\r\n", "AUTH", "bob", "s3cret")
	p.do("-NOAUTH HELLO must be called with the client already authenticated, otherwise the HELLO <proto> AUTH <user> <pass> option can be used to authenticate the client and select the RESP protocol version at the same time\r\n", "HELLO", "3")
	p.do("-WRONGPASS invalid username-password pair or user is disabled.\r\n", "HELLO", "3", "AUTH", "default", "nope")
	p.do("-ERR syntax error\r\n", "AUTH", "a", "b", "c")
	p.do("+OK\r\n", "AUTH", "s3cret")
	p.do("+PONG\r\n", "PING")

	q := s.pipe(t)
	q.do(helloMap(3, 2, "7.2.0", true), "HELLO", "3", "AUTH", "default", "s3cret")
	q.do("+PONG\r\n", "PING")

	r := s.pipe(t)
	r.do("+OK\r\n", "AUTH", "default", "s3cret")
	r.do("+PONG\r\n", "PING")
	r.do("+OK\r\n", "QUIT")
}

func TestPreAuthLimits(t *testing.T) {
	s := newTestServer(t, func(c *config.Config) { c.RequirePass = "pw" })
	p := s.pipe(t)
	p.send([]byte("*20\r\n"))
	p.expect("-ERR Protocol error: unauthenticated multibulk length\r\n")
	p.expectClosed()

	q := s.pipe(t)
	q.do("+OK\r\n", "AUTH", "pw")
	args := []string{"MGET"}
	for i := range 19 {
		args = append(args, "k"+strconv.Itoa(i))
	}
	q.send(encodeArgs(args...))
	q.expect("*19\r\n" + strings.Repeat("$-1\r\n", 19))
}

func TestAuthWithoutPassword(t *testing.T) {
	s := newTestServer(t, nil)
	p := s.pipe(t)
	p.do("-ERR AUTH <password> called without any password configured for the default user. Are you sure your configuration is correct?\r\n", "AUTH", "x")
	p.do("+OK\r\n", "AUTH", "default", "anything")
	p.do("-WRONGPASS invalid username-password pair or user is disabled.\r\n", "AUTH", "bob", "anything")
	p.do(helloMap(3, 1, "7.2.0", true), "HELLO", "3", "AUTH", "default", "x")
}

func TestClientCommands(t *testing.T) {
	s := newTestServer(t, nil)
	p := s.pipe(t)
	p.do(":1\r\n", "CLIENT", "ID")
	p.do("$-1\r\n", "CLIENT", "GETNAME")
	p.do("+OK\r\n", "CLIENT", "SETNAME", "worker-1")
	p.do("$8\r\nworker-1\r\n", "client", "getname")
	p.do("-ERR Client names cannot contain spaces, newlines or special characters.\r\n", "CLIENT", "SETNAME", "a b")
	p.do("+OK\r\n", "CLIENT", "SETINFO", "LIB-NAME", "go-redis(,go1.27.1)")
	p.do("+OK\r\n", "CLIENT", "SETINFO", "lib-ver", "9.22.0")
	p.do("-ERR Unrecognized option 'lib-foo'\r\n", "CLIENT", "SETINFO", "lib-foo", "x")
	p.do("-ERR LIB-VER cannot contain spaces, newlines or special characters.\r\n", "CLIENT", "SETINFO", "LIB-VER", "1 2")
	p.do("+OK\r\n", "CLIENT", "NO-EVICT", "on")
	p.do("-ERR syntax error\r\n", "CLIENT", "NO-EVICT", "maybe")
	p.do("+OK\r\n", "SELECT", "3")

	p.send(encodeArgs("CLIENT", "INFO"))
	line := readBulk(t, p)
	for _, want := range []string{"id=1 ", " name=worker-1 ", " db=3 ", " flags=N ", " cmd=client|info ", " resp=2 ", " lib-name=go-redis(,go1.27.1) lib-ver=9.22.0\n"} {
		if !strings.Contains(line, want) {
			t.Errorf("CLIENT INFO %q lacks %q", line, want)
		}
	}
	q := s.pipe(t)
	q.do("+OK\r\n", "MULTI")
	p.send(encodeArgs("CLIENT", "LIST"))
	list := readBulk(t, p)
	if strings.Count(list, "\n") != 2 || !strings.Contains(list, "id=2 ") || !strings.Contains(list, " flags=x ") || !strings.Contains(list, " multi=0 ") {
		t.Errorf("CLIENT LIST %q", list)
	}
	p.send(encodeArgs("CLIENT", "LIST", "ID", "2"))
	if one := readBulk(t, p); strings.Count(one, "\n") != 1 || !strings.HasPrefix(one, "id=2 ") {
		t.Errorf("CLIENT LIST ID 2: %q", one)
	}
	p.do("$0\r\n\r\n", "CLIENT", "LIST", "TYPE", "pubsub")
	p.do("-ERR Unknown client type 'bogus'\r\n", "CLIENT", "LIST", "TYPE", "bogus")
	p.do("-ERR Invalid client ID\r\n", "CLIENT", "LIST", "ID", "x")
}

// readBulk reads one RESP2 bulk string reply.
func readBulk(t *testing.T, p *pipeClient) string {
	t.Helper()
	line, err := p.r.ReadString('\n')
	if err != nil || len(line) < 3 || line[0] != '$' {
		t.Fatalf("bulk header %q, %v", line, err)
	}
	n, _ := strconv.Atoi(strings.TrimSpace(line[1:]))
	buf := make([]byte, n+2)
	if _, err := io.ReadFull(p.r, buf); err != nil {
		t.Fatal(err)
	}
	return string(buf[:n])
}

func TestTimeUsesServerClock(t *testing.T) {
	s := newTestServer(t, nil)
	p := s.pipe(t)
	p.send(encodeArgs("TIME"))
	secs := readTimeSecs(t, p)
	p.do("+OK\r\n", "NIL.DEBUG", "CLOCK-ADVANCE", "3600000")
	p.send(encodeArgs("TIME"))
	later := readTimeSecs(t, p)
	if d := later - secs; d < 3599 || d > 3610 {
		t.Errorf("TIME moved %d s after a one-hour advance", d)
	}
	p.do("-ERR value is not an integer or out of range\r\n", "NIL.DEBUG", "CLOCK-ADVANCE", "soon")
	p.do("-ERR unknown subcommand 'nap'. Try NIL.DEBUG HELP.\r\n", "nil.debug", "nap")
}

func readTimeSecs(t *testing.T, p *pipeClient) int64 {
	t.Helper()
	head, _ := p.r.ReadString('\n')
	if head != "*2\r\n" {
		t.Fatalf("TIME header %q", head)
	}
	secs, _ := strconv.ParseInt(readBulk(t, p), 10, 64)
	usec, _ := strconv.ParseInt(readBulk(t, p), 10, 64)
	if usec < 0 || usec >= 1_000_000 {
		t.Fatalf("microseconds %d", usec)
	}
	return secs
}

func TestDebugCommandsNeedTheFlag(t *testing.T) {
	s := newTestServer(t, func(c *config.Config) { c.EnableDebugCommands = false })
	p := s.pipe(t)
	p.do("-ERR unknown command 'NIL.DEBUG', with args beginning with: 'CLOCK-ADVANCE' '1' \r\n", "NIL.DEBUG", "CLOCK-ADVANCE", "1")
}

func TestHandlerPanicIsContained(t *testing.T) {
	log.SetOutput(io.Discard)
	t.Cleanup(func() { log.SetOutput(os.Stderr) })
	s := newTestServer(t, nil)
	p := s.pipe(t)
	p.do("-ERR internal error in 'panic': test panic\r\n", "PANIC", "k")
	p.do("$-1\r\n", "GET", "k") // the half-written batch was discarded
	p.do("+PONG\r\n", "PING")
}
