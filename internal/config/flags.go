package config

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

type kind uint8

const (
	kString   kind = iota
	kBool          // yes/no
	kInt           // *int
	kInt64         // *int64
	kBytes         // *int64 with Redis memory units
	kDuration      // *time.Duration, whole seconds or a Go duration
)

// option describes one setting: its flag name (also the nildb.* knob name
// and the config file key), how to parse it, its bounds, and whether
// CONFIG SET may change it.
type option struct {
	name    string
	usage   string
	kind    kind
	mutable bool
	secret  bool // left out of Get and KnobNames
	min     int64
	max     int64 // nanoseconds for durations
	oneOf   []string
	check   func(string) error             // extra check for strings
	cross   func(c *Config, v int64) error // check against other fields on Set
	ptr     func(c *Config) any            // the field
}

// options lists every setting in the order Usage prints them.
var options = []option{
	{name: "dir", usage: "data directory", kind: kString,
		ptr: func(c *Config) any { return &c.Dir }},
	{name: "engine", usage: "storage engine: rocksdb, or lsm, btree or pgheap in a binary built with -tags nilengine", kind: kString,
		oneOf: []string{EngineRocksDB, EngineLSM, EngineBTree, EnginePgHeap},
		ptr:   func(c *Config) any { return &c.Engine }},
	{name: "addr", usage: "listen address, host:port", kind: kString, check: checkAddr,
		ptr: func(c *Config) any { return &c.Addr }},
	{name: "requirepass", usage: "password for AUTH; empty disables AUTH", kind: kString, secret: true,
		ptr: func(c *Config) any { return &c.RequirePass }},
	{name: "fsync", usage: "WAL sync policy: always, everysec or no", kind: kString,
		oneOf: []string{FsyncAlways, FsyncEverySec, FsyncNo},
		ptr:   func(c *Config) any { return &c.Fsync }},
	{name: "databases", usage: "number of Redis databases (fixed at 16 in v1)", kind: kInt, min: Databases, max: Databases,
		ptr: func(c *Config) any { return &c.Databases }},
	{name: "redis-version", usage: "Redis version reported by HELLO and INFO", kind: kString, check: checkVersion,
		ptr: func(c *Config) any { return &c.RedisVersion }},
	{name: "block-cache-mb", usage: "main block cache size in MiB", kind: kInt, min: 1, max: 1 << 20,
		ptr: func(c *Config) any { return &c.BlockCacheMB }},
	{name: "analytics-cache-mb", usage: "columnar block cache size in MiB", kind: kInt, min: 1, max: 1 << 20,
		ptr: func(c *Config) any { return &c.AnalyticsCacheMB }},
	{name: "write-buffer-mb", usage: "memtable budget across all column families in MiB", kind: kInt, min: 1, max: 1 << 20,
		ptr: func(c *Config) any { return &c.WriteBufferMB }},
	{name: "bg-io-mb-per-sec", usage: "RocksDB rate limiter in MiB/s", kind: kInt, min: 1, max: 1 << 20,
		ptr: func(c *Config) any { return &c.BgIOMBPerSec }},
	{name: "max-snapshots", usage: "open snapshot leases and cursors allowed", kind: kInt, min: 1, max: math.MaxInt32,
		ptr: func(c *Config) any { return &c.MaxSnapshots }},
	{name: "lease-ttl", usage: "default snapshot lease lifetime (seconds or a duration such as 90s)", kind: kDuration,
		mutable: true, min: int64(time.Second), max: int64(24 * time.Hour), cross: checkLeaseTTL,
		ptr: func(c *Config) any { return &c.LeaseTTL }},
	{name: "lease-max", usage: "longest lease NIL.SNAPSHOT CREATE TTL accepts", kind: kDuration,
		min: int64(time.Second), max: int64(24 * time.Hour),
		ptr: func(c *Config) any { return &c.LeaseMax }},
	{name: "analytics-max-concurrent", usage: "NIL.* commands allowed to run at once", kind: kInt,
		mutable: true, min: 1, max: 1024,
		ptr: func(c *Config) any { return &c.AnalyticsMaxConcurrent }},
	{name: "analytics-queue-timeout", usage: "how long a NIL.* command waits for a slot", kind: kDuration,
		mutable: true, min: 0, max: int64(time.Hour),
		ptr: func(c *Config) any { return &c.AnalyticsQueueTimeout }},
	{name: "analytics-read-bps", usage: "bytes per second one analytical scan may read, 0 for no limit (accepts kb, mb, gb)", kind: kBytes,
		mutable: true, min: 0, max: math.MaxInt64,
		ptr: func(c *Config) any { return &c.AnalyticsReadBPS }},
	{name: "analytics-max-rows", usage: "rows one analytical command may scan", kind: kInt64,
		mutable: true, min: 1, max: math.MaxInt64,
		ptr: func(c *Config) any { return &c.AnalyticsMaxRows }},
	{name: "analytics-group-mem", usage: "default MAXMEM of NIL.AGGREGATE in bytes (accepts kb, mb, gb)", kind: kBytes,
		mutable: true, min: 1, max: math.MaxInt64,
		ptr: func(c *Config) any { return &c.AnalyticsGroupMem }},
	{name: "doc-agg-mem", usage: "default MAXMEM of DOC.AGGREGATE in bytes (accepts kb, mb, gb)", kind: kBytes,
		mutable: true, min: 1, max: math.MaxInt64,
		ptr: func(c *Config) any { return &c.DocAggMem }},
	{name: "geo-query-max-cells", usage: "covering cells per 2dsphere query", kind: kInt,
		mutable: true, min: 1, max: 1000,
		ptr: func(c *Config) any { return &c.GeoQueryMaxCells }},
	{name: "setop-max-members", usage: "members an in-memory set operation may hold", kind: kInt,
		mutable: true, min: 1, max: math.MaxInt32,
		ptr: func(c *Config) any { return &c.SetOpMaxMembers }},
	{name: "multi-queue-max", usage: "commands one MULTI may queue", kind: kInt,
		mutable: true, min: 1, max: math.MaxInt32,
		ptr: func(c *Config) any { return &c.MultiQueueMax }},
	{name: "rocks-stats", usage: "collect RocksDB statistics (ROCKS.STATS)", kind: kBool,
		ptr: func(c *Config) any { return &c.RocksStats }},
	{name: "enable-debug-commands", usage: "enable NIL.DEBUG, including CLOCK-ADVANCE", kind: kBool,
		ptr: func(c *Config) any { return &c.EnableDebugCommands }},
	{name: "readonly", usage: "serve a ROCKS.CHECKPOINT directory read-only", kind: kString,
		ptr: func(c *Config) any { return &c.ReadOnly }},
}

func checkLeaseTTL(c *Config, v int64) error {
	if limit := loadDuration(&c.LeaseMax); time.Duration(v) > limit {
		return fmt.Errorf("argument must not exceed lease-max (%s)", formatDuration(limit))
	}
	return nil
}

// parse turns text into the option's value: a string, a bool, or an
// int64 (nanoseconds for durations). It checks bounds but not cross.
func (o *option) parse(s string) (str string, b bool, n int64, err error) {
	switch o.kind {
	case kString:
		if o.oneOf != nil {
			for _, v := range o.oneOf {
				if strings.EqualFold(s, v) {
					return v, false, 0, nil
				}
			}
			return "", false, 0, fmt.Errorf("argument(s) must be one of the following: %s", strings.Join(o.oneOf, ", "))
		}
		if o.check != nil {
			if err := o.check(s); err != nil {
				return "", false, 0, err
			}
		}
		return s, false, 0, nil
	case kBool:
		switch strings.ToLower(s) {
		case "yes", "true", "1":
			return "", true, 0, nil
		case "no", "false", "0":
			return "", false, 0, nil
		}
		return "", false, 0, errors.New("argument must be 'yes' or 'no'")
	case kInt, kInt64:
		v, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return "", false, 0, errors.New("argument couldn't be parsed into an integer")
		}
		n = v
	case kBytes:
		v, ok := parseMemory(s)
		if !ok {
			return "", false, 0, errors.New("argument must be a memory value")
		}
		n = v
	case kDuration:
		d, ok := parseDuration(s)
		if !ok {
			return "", false, 0, errors.New("argument must be whole seconds or a duration such as 90s or 1500ms")
		}
		n = int64(d)
	}
	if o.min == o.max && n != o.min {
		return "", false, 0, fmt.Errorf("argument must be %d", o.min)
	}
	if n < o.min || n > o.max {
		if o.kind == kDuration {
			return "", false, 0, fmt.Errorf("argument must be between %s and %s inclusive",
				formatDuration(time.Duration(o.min)), formatDuration(time.Duration(o.max)))
		}
		return "", false, 0, fmt.Errorf("argument must be between %d and %d inclusive", o.min, o.max)
	}
	return "", false, n, nil
}

// set parses s and stores it in c. Numbers are stored atomically; with
// runtime true the cross-field check runs first.
func (o *option) set(c *Config, s string, runtime bool) error {
	str, b, n, err := o.parse(s)
	if err != nil {
		return err
	}
	if runtime && o.cross != nil {
		if err := o.cross(c, n); err != nil {
			return err
		}
	}
	switch p := o.ptr(c).(type) {
	case *string:
		*p = str
	case *bool:
		*p = b
	case *int:
		storeInt(p, int(n))
	case *int64:
		atomic.StoreInt64(p, n)
	case *time.Duration:
		storeDuration(p, time.Duration(n))
	}
	return nil
}

// value returns the option's current value as an int64 for numeric kinds.
func (o *option) value(c *Config) int64 {
	switch p := o.ptr(c).(type) {
	case *int:
		return int64(loadInt(p))
	case *int64:
		return atomic.LoadInt64(p)
	case *time.Duration:
		return int64(loadDuration(p))
	}
	return 0
}

// format prints the current value the way Get and the flag defaults show it.
func (o *option) format(c *Config) string {
	switch p := o.ptr(c).(type) {
	case *string:
		return *p
	case *bool:
		if *p {
			return "yes"
		}
		return "no"
	case *time.Duration:
		return formatDuration(loadDuration(p))
	}
	return strconv.FormatInt(o.value(c), 10)
}

// validate checks the current value against the option's rules.
func (o *option) validate(c *Config) error {
	switch p := o.ptr(c).(type) {
	case *string:
		if o.oneOf != nil {
			// Exact match: parse would accept other cases, but code
			// comparing against the constants would not.
			if slices.Contains(o.oneOf, *p) {
				return nil
			}
			return fmt.Errorf("must be one of %s, got %q", strings.Join(o.oneOf, ", "), *p)
		}
		if o.check == nil {
			return nil
		}
		_, _, _, err := o.parse(*p)
		return err
	case *bool:
		return nil
	}
	n := o.value(c)
	if o.min == o.max && n != o.min {
		return fmt.Errorf("must be %d in v1, got %d", o.min, n)
	}
	if n < o.min || n > o.max {
		if o.kind == kDuration {
			return fmt.Errorf("must be between %s and %s, got %s",
				formatDuration(time.Duration(o.min)), formatDuration(time.Duration(o.max)), formatDuration(time.Duration(n)))
		}
		return fmt.Errorf("must be between %d and %d, got %d", o.min, o.max, n)
	}
	return nil
}

// parseMemory is Redis's memtoull: a non-negative integer with an
// optional unit, b, k (1000), kb (1024), m, mb, g or gb, in any case.
func parseMemory(s string) (int64, bool) {
	i := 0
	for i < len(s) && s[i] >= '0' && s[i] <= '9' {
		i++
	}
	if i == 0 {
		return 0, false
	}
	n, err := strconv.ParseInt(s[:i], 10, 64)
	if err != nil {
		return 0, false
	}
	var mul int64
	switch strings.ToLower(s[i:]) {
	case "", "b":
		mul = 1
	case "k":
		mul = 1000
	case "kb":
		mul = 1 << 10
	case "m":
		mul = 1000 * 1000
	case "mb":
		mul = 1 << 20
	case "g":
		mul = 1000 * 1000 * 1000
	case "gb":
		mul = 1 << 30
	default:
		return 0, false
	}
	if n > math.MaxInt64/mul {
		return 0, false
	}
	return n * mul, true
}

// parseDuration takes whole seconds ("90") or a Go duration ("1500ms").
func parseDuration(s string) (time.Duration, bool) {
	if s == "" {
		return 0, false
	}
	if strings.Trim(s, "0123456789") == "" {
		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil || n > int64(math.MaxInt64/time.Second) {
			return 0, false
		}
		return time.Duration(n) * time.Second, true
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0, false
	}
	return d, true
}

// formatDuration prints whole seconds as a bare number and anything else
// as Go does, so the output parses back with parseDuration.
func formatDuration(d time.Duration) string {
	if d%time.Second == 0 {
		return strconv.FormatInt(int64(d/time.Second), 10)
	}
	return d.String()
}

// flagValue adapts an option to flag.Value.
type flagValue struct {
	o *option
	c *Config
}

func (v flagValue) String() string {
	if v.o == nil || v.c == nil {
		return ""
	}
	return v.o.format(v.c)
}

func (v flagValue) Set(s string) error { return v.o.set(v.c, s, false) }

// IsBoolFlag lets "--rocks-stats" stand alone.
func (v flagValue) IsBoolFlag() bool { return v.o != nil && v.o.kind == kBool }

func newFlagSet(c *Config, file *string) *flag.FlagSet {
	fs := flag.NewFlagSet("nildb", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	for i := range options {
		o := &options[i]
		fs.Var(flagValue{o: o, c: c}, o.name, o.usage)
	}
	fs.StringVar(file, "config", "", "key=value file read after the flags; flags given on the command line win")
	return fs
}

// Parse reads command-line arguments (os.Args[1:]) over Default(): flags
// in -name value, --name value or --name=value form, then the --config
// file if one is named, then Validate. A setting given both as a flag and
// in the file takes the flag's value. -h or --help returns flag.ErrHelp;
// print Usage then.
//
// The file holds one key=value per line. Keys are flag names, optionally
// prefixed with "nildb."; values may be wrapped in double quotes; blank
// lines and lines starting with '#' are skipped.
func Parse(args []string) (Config, error) {
	c := Default()
	var file string
	fs := newFlagSet(&c, &file)
	if err := fs.Parse(args); err != nil {
		return Config{}, err
	}
	if fs.NArg() > 0 {
		return Config{}, fmt.Errorf("config: unexpected argument %q", fs.Arg(0))
	}
	if file != "" {
		onCommandLine := map[string]bool{}
		fs.Visit(func(f *flag.Flag) { onCommandLine[f.Name] = true })
		if err := loadFile(&c, file, onCommandLine); err != nil {
			return Config{}, err
		}
	}
	if err := c.Validate(); err != nil {
		return Config{}, err
	}
	return c, nil
}

func loadFile(c *Config, path string, skip map[string]bool) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for line := 1; sc.Scan(); line++ {
		text := strings.TrimSpace(sc.Text())
		if text == "" || text[0] == '#' {
			continue
		}
		key, val, ok := strings.Cut(text, "=")
		if !ok {
			return fmt.Errorf("config: %s:%d: expected key=value", path, line)
		}
		key = strings.ToLower(strings.TrimSpace(key))
		key = strings.TrimPrefix(key, KnobPrefix)
		val = strings.TrimSpace(val)
		if len(val) >= 2 && val[0] == '"' && val[len(val)-1] == '"' {
			val = val[1 : len(val)-1]
		}
		o := lookupOption(key)
		if o == nil {
			return fmt.Errorf("config: %s:%d: unknown option %q", path, line, key)
		}
		if skip[o.name] {
			continue
		}
		if err := o.set(c, val, false); err != nil {
			return fmt.Errorf("config: %s:%d: %s: %v", path, line, key, err)
		}
	}
	if err := sc.Err(); err != nil {
		return fmt.Errorf("config: %s: %w", path, err)
	}
	return nil
}

// Usage writes the flag list with defaults to w.
func Usage(w io.Writer) {
	c := Default()
	var file string
	fs := newFlagSet(&c, &file)
	fs.SetOutput(w)
	fmt.Fprintln(w, "Usage: nildb [flags]")
	fs.PrintDefaults()
}
