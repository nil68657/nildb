package server

import (
	"strconv"
	"strings"
	"testing"

	"github.com/nil68657/nildb/internal/command"
	"github.com/nil68657/nildb/internal/resp"
)

func TestConfigGetSet(t *testing.T) {
	s := newTestServer(t, nil)
	p := s.pipe(t)
	p.do("*4\r\n$10\r\nappendonly\r\n$2\r\nno\r\n$4\r\nsave\r\n$0\r\n\r\n", "CONFIG", "GET", "save", "appendonly")
	p.do("*4\r\n$9\r\ndatabases\r\n$2\r\n16\r\n$9\r\nmaxmemory\r\n$1\r\n0\r\n", "CONFIG", "GET", "databases", "MAXMEMORY")
	p.do("*2\r\n$21\r\nnildb.multi-queue-max\r\n$6\r\n100000\r\n", "CONFIG", "GET", "nildb.multi-queue-max")
	p.do("*0\r\n", "CONFIG", "GET", "requirepass")
	p.do("*0\r\n", "CONFIG", "GET", "nildb.requirepass")
	p.do("+OK\r\n", "CONFIG", "SET", "nildb.multi-queue-max", "2", "nildb.lease-ttl", "30")
	p.do("*2\r\n$15\r\nnildb.lease-ttl\r\n$2\r\n30\r\n", "CONFIG", "GET", "nildb.lease-t*")
	p.do("-ERR Unknown option or number of arguments for CONFIG SET - 'maxmemory'\r\n", "CONFIG", "SET", "maxmemory", "1")
	p.do("-ERR CONFIG SET failed (possibly related to argument 'nildb.dir') - can't set immutable config\r\n", "CONFIG", "SET", "nildb.dir", "/tmp")
	// A failing pair rolls back the pairs before it.
	p.send(encodeArgs("CONFIG", "SET", "nildb.lease-ttl", "45", "nildb.multi-queue-max", "lots"))
	line, _ := p.r.ReadString('\n')
	if !strings.HasPrefix(line, "-ERR CONFIG SET failed (possibly related to argument 'nildb.multi-queue-max') - ") {
		t.Errorf("bad value reply %q", line)
	}
	p.do("*2\r\n$15\r\nnildb.lease-ttl\r\n$2\r\n30\r\n", "CONFIG", "GET", "nildb.lease-ttl")
	p.do("-ERR wrong number of arguments for 'config|set' command\r\n", "CONFIG", "SET", "a", "b", "c")
	p.do("-ERR wrong number of arguments for 'config|get' command\r\n", "CONFIG", "GET")
	if s.cfg.Knobs().MultiQueueMax != 2 {
		t.Errorf("multi-queue-max %d", s.cfg.Knobs().MultiQueueMax)
	}

	// RESP3 turns the reply into a map.
	p.do(helloMap(3, 1, "7.2.0", true), "HELLO", "3")
	p.do("%1\r\n$9\r\ndatabases\r\n$2\r\n16\r\n", "CONFIG", "GET", "databases")
}

func TestMultiQueueLimit(t *testing.T) {
	s := newTestServer(t, nil)
	p := s.pipe(t)
	p.do("+OK\r\n", "CONFIG", "SET", "nildb.multi-queue-max", "2")
	p.do("+OK\r\n", "MULTI")
	p.do("+QUEUED\r\n", "SET", "a", "1")
	p.do("+QUEUED\r\n", "SET", "b", "2")
	p.do("-ERR MULTI queue limit reached\r\n", "SET", "c", "3")
	p.do("-EXECABORT Transaction discarded because of previous errors.\r\n", "EXEC")
	p.do(":0\r\n", "EXISTS", "a", "b", "c")
}

func TestCommandCommand(t *testing.T) {
	s := newTestServer(t, nil)
	p := s.pipe(t)
	count := len(s.reg.Specs())
	p.do(string(resp.Encode(resp.Int(int64(count)), 2)), "COMMAND", "COUNT")
	p.do(string(resp.Encode(command.CommandInfoReply(s.reg, []string{"get"}), 2)), "COMMAND", "INFO", "get")
	p.do("*1\r\n$-1\r\n", "COMMAND", "INFO", "nosuch")
	p.do("*1\r\n$3\r\nget\r\n", "COMMAND", "GETKEYS", "SET", "get", "k")
	p.do("*2\r\n$1\r\na\r\n$1\r\nb\r\n", "COMMAND", "GETKEYS", "DEL", "a", "b")
	p.do("*1\r\n$3\r\nset\r\n", "COMMAND", "GETKEYS", "WATCH", "set")
	p.do("-ERR The command has no key arguments\r\n", "COMMAND", "GETKEYS", "PING")
	p.do("-ERR Invalid command specified\r\n", "COMMAND", "GETKEYS", "NOSUCH", "x")
	p.do("-ERR Invalid number of arguments specified for command\r\n", "COMMAND", "GETKEYS", "GET")
	p.do("*1\r\n$10\r\nconfig|get\r\n", "COMMAND", "LIST", "FILTERBY", "PATTERN", "config|g*")
	p.do("*2\r\n$4\r\ntime\r\n$5\r\nwatch\r\n", "COMMAND", "LIST", "FILTERBY", "PATTERN", "[tw]?[mt]*")

	p.send(encodeArgs("COMMAND"))
	head, _ := p.r.ReadString('\n')
	if head != "*"+itoa(count)+"\r\n" {
		t.Fatalf("COMMAND header %q, want %d entries", head, count)
	}
	p.nc.Close()
}

func itoa(n int) string { return strconv.Itoa(n) }

func TestInfoSections(t *testing.T) {
	s := newTestServer(t, nil)
	s.RegisterInfoSection("widgets", func(b *strings.Builder) { b.WriteString("widgets_open:3\nwidgets_closed:1\n") })
	s.RegisterInfoSection("persistence", func(b *strings.Builder) { b.WriteString("nildb_last_checkpoint:none\r\n") })
	p := s.pipe(t)
	p.do("+OK\r\n", "SET", "a", "1")
	p.do("+OK\r\n", "SET", "b", "2", "PX", "100000")
	p.do("+OK\r\n", "SET", "gone", "x", "PX", "1")
	p.do("+OK\r\n", "SELECT", "4")
	p.do("+OK\r\n", "SET", "c", "3")
	p.do("+OK\r\n", "NIL.DEBUG", "CLOCK-ADVANCE", "10")

	info := s.Info()
	for _, want := range []string{
		"# Server\r\nredis_version:7.2.0\r\n",
		"\r\n\r\n# Clients\r\nconnected_clients:1\r\n",
		"redis_mode:standalone\r\n",
		"# Persistence\r\nloading:0\r\n",
		"nildb_last_checkpoint:none\r\n\r\n# Stats\r\n",
		"# Keyspace\r\ndb0:keys=2,expires=1,avg_ttl=",
		"\r\ndb4:keys=1,expires=0,avg_ttl=0\r\n",
		"# Widgets\r\nwidgets_open:3\r\nwidgets_closed:1\r\n",
		"# Errorstats\r\n",
	} {
		if !strings.Contains(info, want) {
			t.Errorf("INFO lacks %q:\n%s", want, info)
		}
	}
	if strings.Contains(info, "# Commandstats") || strings.Contains(info, "# Latencystats") {
		t.Errorf("default INFO includes commandstats or latencystats:\n%s", info)
	}
	if strings.Contains(info, "\n\n") || strings.Contains(strings.ReplaceAll(info, "\r\n", ""), "\n") {
		t.Errorf("INFO has bare newlines:\n%q", info)
	}

	p.do("-ERR wrong number of arguments for 'get' command\r\n", "GET")
	cs := s.Info("commandstats", "errorstats")
	for _, want := range []string{
		"# Commandstats\r\n",
		"cmdstat_set:calls=4,usec=",
		",rejected_calls=0,failed_calls=0,p50=",
		"cmdstat_get:calls=0,usec=0,usec_per_call=0.00,rejected_calls=1,failed_calls=0,",
		"cmdstat_nil.debug|clock-advance:calls=1,",
		"errorstat_ERR:count=1\r\n",
	} {
		if !strings.Contains(cs, want) {
			t.Errorf("INFO commandstats lacks %q:\n%s", want, cs)
		}
	}
	if strings.Contains(cs, "# Server") {
		t.Error("section filter ignored")
	}
	if lat := s.Info("latencystats"); !strings.Contains(lat, "latency_percentiles_usec_set:p50=") {
		t.Errorf("latencystats:\n%s", lat)
	}
	if only := s.Info("WIDGETS"); only != "# Widgets\r\nwidgets_open:3\r\nwidgets_closed:1\r\n" {
		t.Errorf("INFO widgets = %q", only)
	}
	all := s.Info("all")
	if !strings.Contains(all, "# Commandstats") || !strings.Contains(all, "# Widgets") {
		t.Error("INFO all misses sections")
	}

	p.do("+OK\r\n", "CONFIG", "RESETSTAT")
	if cs := s.Info("commandstats"); strings.Contains(cs, "cmdstat_set") {
		t.Errorf("RESETSTAT kept stats:\n%s", cs)
	}

	// Over the wire: a bulk string in RESP2, verbatim text in RESP3.
	p.send(encodeArgs("INFO", "widgets"))
	body := "# Widgets\r\nwidgets_open:3\r\nwidgets_closed:1\r\n"
	p.expect("$" + itoa(len(body)) + "\r\n" + body + "\r\n")
	p.do(helloMap(3, 1, "7.2.0", true), "HELLO", "3")
	p.send(encodeArgs("INFO", "widgets"))
	p.expect("=" + itoa(len(body)+4) + "\r\ntxt:" + body + "\r\n")
}
