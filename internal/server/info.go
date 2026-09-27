package server

import (
	"encoding/binary"
	"fmt"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/nil68657/nildb/internal/command"
	"github.com/nil68657/nildb/internal/config"
	"github.com/nil68657/nildb/internal/layout"
	"github.com/nil68657/nildb/internal/resp"
	"github.com/nil68657/nildb/internal/store"
)

// builtinSections lists INFO's own sections in output order, with the
// header each prints and whether "INFO" with no argument includes it.
var builtinSections = []struct {
	name, title string
	dflt        bool
}{
	{"server", "Server", true},
	{"clients", "Clients", true},
	{"memory", "Memory", true},
	{"persistence", "Persistence", true},
	{"stats", "Stats", true},
	{"replication", "Replication", true},
	{"cpu", "CPU", true},
	{"commandstats", "Commandstats", false},
	{"errorstats", "Errorstats", true},
	{"latencystats", "Latencystats", false},
	{"keyspace", "Keyspace", true},
}

func (s *Server) infoSpec() command.Spec {
	return command.Spec{
		Name: "info", Arity: -1, Group: "server", Since: "1.0.0",
		Summary: "Returns information and statistics about the server.",
		Run: func(_ *command.Ctx, args [][]byte) resp.Reply {
			return resp.Verbatim("txt", s.Info(byteStrings(args[1:])...))
		},
	}
}

// Info renders INFO for the given section names ("default" when none,
// "all" or "everything" for every section). Unknown names are ignored.
func (s *Server) Info(sections ...string) string {
	want := make(map[string]bool)
	all, dflt := false, len(sections) == 0
	for _, sec := range sections {
		switch sec = strings.ToLower(sec); sec {
		case "all", "everything":
			all = true
		case "default":
			dflt = true
		default:
			want[sec] = true
		}
	}
	extra := s.reg.InfoSections()
	pick := func(name string, inDefault bool) bool {
		return all || want[name] || (dflt && inDefault)
	}

	var b strings.Builder
	first := true
	start := func(title string) {
		if !first {
			b.WriteString("\r\n")
		}
		first = false
		b.WriteString("# ")
		b.WriteString(title)
		b.WriteString("\r\n")
	}
	appendExtra := func(name string) {
		for _, e := range extra {
			if e.Name == name {
				writeLines(&b, e.Fn)
			}
		}
	}
	builtin := make(map[string]bool, len(builtinSections))
	for _, sec := range builtinSections {
		builtin[sec.name] = true
		if !pick(sec.name, sec.dflt) {
			continue
		}
		start(sec.title)
		s.writeSection(&b, sec.name)
		appendExtra(sec.name)
	}
	done := make(map[string]bool)
	for _, e := range extra {
		if builtin[e.Name] || done[e.Name] || !pick(e.Name, true) {
			continue
		}
		done[e.Name] = true
		start(title(e.Name))
		appendExtra(e.Name)
	}
	return b.String()
}

// writeLines runs a registered section writer and turns bare "\n" line
// ends into "\r\n".
func writeLines(b *strings.Builder, fn func(*strings.Builder)) {
	var tmp strings.Builder
	fn(&tmp)
	out := tmp.String()
	if out == "" {
		return
	}
	out = strings.ReplaceAll(strings.ReplaceAll(out, "\r\n", "\n"), "\n", "\r\n")
	if !strings.HasSuffix(out, "\r\n") {
		out += "\r\n"
	}
	b.WriteString(out)
}

func title(name string) string {
	if name == "" {
		return name
	}
	return strings.ToUpper(name[:1]) + name[1:]
}

func field(b *strings.Builder, k string, v any) {
	fmt.Fprintf(b, "%s:%v\r\n", k, v)
}

func (s *Server) writeSection(b *strings.Builder, name string) {
	switch name {
	case "server":
		s.infoServer(b)
	case "clients":
		s.mu.Lock()
		n := len(s.conns)
		s.mu.Unlock()
		field(b, "connected_clients", n)
		field(b, "cluster_connections", 0)
		field(b, "maxclients", 10000)
		field(b, "client_recent_max_input_buffer", 0)
		field(b, "client_recent_max_output_buffer", 0)
		field(b, "blocked_clients", 0)
		field(b, "tracking_clients", 0)
		field(b, "clients_in_timeout_table", 0)
		field(b, "total_blocking_keys", 0)
		field(b, "total_blocking_keys_on_nokey", 0)
		field(b, "watched_keys", s.watch.Len())
	case "memory":
		var ms runtime.MemStats
		runtime.ReadMemStats(&ms)
		field(b, "used_memory", ms.HeapAlloc)
		field(b, "used_memory_human", bytesHuman(ms.HeapAlloc))
		field(b, "used_memory_rss", ms.Sys)
		field(b, "used_memory_rss_human", bytesHuman(ms.Sys))
		field(b, "maxmemory", 0)
		field(b, "maxmemory_human", "0B")
		field(b, "maxmemory_policy", "noeviction")
		field(b, "mem_allocator", "go-"+runtime.Version())
		if v, ok := s.st.IntProperty(store.CFDefault, "rocksdb.block-cache-usage"); ok {
			field(b, "nildb_block_cache_usage", v)
		}
	case "persistence":
		field(b, "loading", 0)
		field(b, "async_loading", 0)
		field(b, "rdb_changes_since_last_save", 0)
		field(b, "rdb_bgsave_in_progress", 0)
		field(b, "rdb_last_save_time", s.started.Unix())
		field(b, "rdb_last_bgsave_status", "ok")
		field(b, "aof_enabled", 0)
		field(b, "aof_rewrite_in_progress", 0)
		field(b, "aof_last_write_status", "ok")
		field(b, "nildb_wal", "on")
		field(b, "nildb_fsync", s.cfg.Fsync)
	case "stats":
		st := s.stats
		field(b, "total_connections_received", st.connsTotal.Load())
		field(b, "total_commands_processed", st.commands.Load())
		field(b, "instantaneous_ops_per_sec", st.opsPerSec())
		field(b, "total_net_input_bytes", st.netIn.Load())
		field(b, "total_net_output_bytes", st.netOut.Load())
		field(b, "rejected_connections", 0)
		field(b, "expired_keys", 0)
		field(b, "evicted_keys", 0)
		field(b, "keyspace_hits", 0)
		field(b, "keyspace_misses", 0)
		field(b, "total_error_replies", st.errors.Load())
	case "replication":
		field(b, "role", "master")
		field(b, "connected_slaves", 0)
		field(b, "master_replid", s.runID)
		field(b, "master_repl_offset", 0)
	case "cpu":
		var ru syscall.Rusage
		if syscall.Getrusage(syscall.RUSAGE_SELF, &ru) == nil {
			field(b, "used_cpu_sys", fmt.Sprintf("%.6f", tvSeconds(ru.Stime)))
			field(b, "used_cpu_user", fmt.Sprintf("%.6f", tvSeconds(ru.Utime)))
		}
	case "commandstats":
		s.eachCmdStats(func(name string, cs *cmdStats) {
			calls := cs.calls.Load()
			usec := uint64(cs.hist.Sum() / time.Microsecond)
			per := 0.0
			if calls > 0 {
				per = float64(cs.hist.Sum()) / float64(time.Microsecond) / float64(calls)
			}
			fmt.Fprintf(b, "cmdstat_%s:calls=%d,usec=%d,usec_per_call=%.2f,rejected_calls=%d,failed_calls=%d,p50=%.3f,p99=%.3f,p999=%.3f\r\n",
				name, calls, usec, per, cs.rejected.Load(), cs.failed.Load(),
				usecs(cs.hist.Quantile(0.50)), usecs(cs.hist.Quantile(0.99)), usecs(cs.hist.Quantile(0.999)))
		})
	case "errorstats":
		st := s.stats
		st.errMu.Lock()
		codes := make([]string, 0, len(st.errCodes))
		for c := range st.errCodes {
			codes = append(codes, c)
		}
		sort.Strings(codes)
		for _, c := range codes {
			fmt.Fprintf(b, "errorstat_%s:count=%d\r\n", c, st.errCodes[c])
		}
		st.errMu.Unlock()
	case "latencystats":
		s.eachCmdStats(func(name string, cs *cmdStats) {
			fmt.Fprintf(b, "latency_percentiles_usec_%s:p50=%.3f,p99=%.3f,p99.9=%.3f\r\n", name,
				usecs(cs.hist.Quantile(0.50)), usecs(cs.hist.Quantile(0.99)), usecs(cs.hist.Quantile(0.999)))
		})
	case "keyspace":
		s.infoKeyspace(b)
	}
}

func (s *Server) infoServer(b *strings.Builder) {
	port := "0"
	if a := s.Addr(); a != nil {
		if i := strings.LastIndexByte(a.String(), ':'); i >= 0 {
			port = a.String()[i+1:]
		}
	}
	exe, _ := os.Executable()
	major, minor, patch := store.Version()
	up := time.Since(s.started)
	field(b, "redis_version", s.cfg.RedisVersion)
	field(b, "redis_git_sha1", "00000000")
	field(b, "redis_git_dirty", 0)
	field(b, "redis_build_id", "0")
	field(b, "redis_mode", "standalone")
	field(b, "os", runtime.GOOS+" "+runtime.GOARCH)
	field(b, "arch_bits", strconv.Itoa(strconv.IntSize))
	field(b, "multiplexing_api", "goroutines")
	field(b, "process_id", os.Getpid())
	field(b, "process_supervised", "no")
	field(b, "run_id", s.runID)
	field(b, "tcp_port", port)
	field(b, "server_time_usec", s.cfg.Now().UnixMicro())
	field(b, "uptime_in_seconds", int64(up/time.Second))
	field(b, "uptime_in_days", int64(up/(24*time.Hour)))
	field(b, "hz", 10)
	field(b, "configured_hz", 10)
	field(b, "executable", exe)
	field(b, "config_file", "")
	field(b, "io_threads_active", 0)
	field(b, "nildb_rocksdb_version", fmt.Sprintf("%d.%d.%d", major, minor, patch))
	field(b, "nildb_go_version", runtime.Version())
}

// eachCmdStats visits the commands that ran, by full name.
func (s *Server) eachCmdStats(fn func(name string, cs *cmdStats)) {
	type ent struct {
		name string
		cs   *cmdStats
	}
	var list []ent
	s.stats.cmds.Range(func(k, v any) bool {
		cs := v.(*cmdStats)
		if cs.calls.Load() > 0 || cs.rejected.Load() > 0 {
			list = append(list, ent{k.(*command.Spec).FullName(), cs})
		}
		return true
	})
	sort.Slice(list, func(i, j int) bool { return list[i].name < list[j].name })
	for _, e := range list {
		fn(e.name, e.cs)
	}
}

// infoKeyspace scans meta once and prints "dbN:keys=..,expires=..,avg_ttl=.."
// for every database holding live keys. Expired keys are skipped, so the
// counts match what DBSIZE reports.
func (s *Server) infoKeyspace(b *strings.Builder) {
	var keys, expires [config.Databases]int64
	var ttlSum [config.Databases]float64
	nowMS := s.cfg.Now().UnixMilli()
	it := s.st.Iter(store.CFMeta, []byte{0}, []byte{config.Databases}, store.IterOpts{})
	defer it.Close()
	for it.SeekToFirst(); it.Valid(); it.Next() {
		k, v := it.Key(), it.Value()
		if len(k) == 0 || k[0] >= config.Databases || len(v) < layout.StringHeaderLen {
			continue
		}
		exp := int64(binary.BigEndian.Uint64(v[1:layout.StringHeaderLen]))
		if exp != 0 && exp <= nowMS {
			continue
		}
		db := k[0]
		keys[db]++
		if exp != 0 {
			expires[db]++
			ttlSum[db] += float64(exp - nowMS)
		}
	}
	for db := range config.Databases {
		if keys[db] == 0 {
			continue
		}
		avg := int64(0)
		if expires[db] > 0 {
			avg = int64(ttlSum[db] / float64(expires[db]))
		}
		fmt.Fprintf(b, "db%d:keys=%d,expires=%d,avg_ttl=%d\r\n", db, keys[db], expires[db], avg)
	}
}

func usecs(d time.Duration) float64 { return float64(d) / float64(time.Microsecond) }

func tvSeconds(tv syscall.Timeval) float64 {
	return float64(tv.Sec) + float64(tv.Usec)/1e6
}

// bytesHuman is Redis's bytesToHuman: B, then K, M, G, T with two
// decimals.
func bytesHuman(n uint64) string {
	f := float64(n)
	switch {
	case n < 1<<10:
		return strconv.FormatUint(n, 10) + "B"
	case n < 1<<20:
		return fmt.Sprintf("%.2fK", f/(1<<10))
	case n < 1<<30:
		return fmt.Sprintf("%.2fM", f/(1<<20))
	case n < 1<<40:
		return fmt.Sprintf("%.2fG", f/(1<<30))
	default:
		return fmt.Sprintf("%.2fT", f/(1<<40))
	}
}
