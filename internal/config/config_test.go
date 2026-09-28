package config

import (
	"bytes"
	"errors"
	"flag"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestDefault(t *testing.T) {
	c := Default()
	if err := c.Validate(); err != nil {
		t.Fatalf("Default() does not validate: %v", err)
	}
	checks := []struct {
		name      string
		got, want any
	}{
		{"Dir", c.Dir, "./data"},
		{"Addr", c.Addr, "127.0.0.1:6380"},
		{"RequirePass", c.RequirePass, ""},
		{"Fsync", c.Fsync, FsyncEverySec},
		{"Databases", c.Databases, 16},
		{"RedisVersion", c.RedisVersion, "7.2.0"},
		{"BlockCacheMB", c.BlockCacheMB, 512},
		{"AnalyticsCacheMB", c.AnalyticsCacheMB, 64},
		{"WriteBufferMB", c.WriteBufferMB, 256},
		{"BgIOMBPerSec", c.BgIOMBPerSec, 200},
		{"MaxSnapshots", c.MaxSnapshots, 32},
		{"LeaseTTL", c.LeaseTTL, 60 * time.Second},
		{"LeaseMax", c.LeaseMax, 600 * time.Second},
		{"AnalyticsMaxConcurrent", c.AnalyticsMaxConcurrent, 2},
		{"AnalyticsQueueTimeout", c.AnalyticsQueueTimeout, 5 * time.Second},
		{"AnalyticsReadBPS", c.AnalyticsReadBPS, int64(256 << 20)},
		{"AnalyticsMaxRows", c.AnalyticsMaxRows, int64(50_000_000)},
		{"AnalyticsGroupMem", c.AnalyticsGroupMem, int64(100 << 20)},
		{"DocAggMem", c.DocAggMem, int64(32 << 20)},
		{"GeoQueryMaxCells", c.GeoQueryMaxCells, 16},
		{"SetOpMaxMembers", c.SetOpMaxMembers, 1_000_000},
		{"MultiQueueMax", c.MultiQueueMax, 100_000},
		{"RocksStats", c.RocksStats, false},
		{"EnableDebugCommands", c.EnableDebugCommands, false},
		{"ReadOnly", c.ReadOnly, ""},
		{"BlockCacheBytes", c.BlockCacheBytes(), int64(512 << 20)},
		{"AnalyticsCacheBytes", c.AnalyticsCacheBytes(), int64(64 << 20)},
		{"WriteBufferBytes", c.WriteBufferBytes(), int64(256 << 20)},
		{"BgIOBytesPerSec", c.BgIOBytesPerSec(), int64(200 << 20)},
	}
	for _, ch := range checks {
		if ch.got != ch.want {
			t.Errorf("%s = %v, want %v", ch.name, ch.got, ch.want)
		}
	}
	if _, ok := c.Clock.(SystemClock); !ok {
		t.Errorf("Clock = %T, want SystemClock", c.Clock)
	}
	k := c.Knobs()
	if k.LeaseTTL != c.LeaseTTL || k.AnalyticsReadBPS != c.AnalyticsReadBPS || k.MultiQueueMax != c.MultiQueueMax ||
		k.GeoQueryMaxCells != c.GeoQueryMaxCells || k.DocAggMem != c.DocAggMem {
		t.Errorf("Knobs() = %+v does not match the fields", k)
	}
}

func TestParseFlags(t *testing.T) {
	c, err := Parse([]string{
		"--dir", "/tmp/nil", "--addr=0.0.0.0:7000", "-requirepass", "s3cret", "--fsync", "always",
		"--databases", "16", "--redis-version", "7.2.4", "--block-cache-mb", "1024", "--max-snapshots", "8",
		"--lease-ttl", "90", "--lease-max", "15m", "--analytics-max-concurrent", "4",
		"--analytics-read-bps", "64mb", "--analytics-queue-timeout", "1500ms",
		"--rocks-stats", "--enable-debug-commands=yes", "--readonly", "/tmp/ckpt",
	})
	if err != nil {
		t.Fatal(err)
	}
	if c.Dir != "/tmp/nil" || c.Addr != "0.0.0.0:7000" || c.RequirePass != "s3cret" || c.Fsync != FsyncAlways ||
		c.RedisVersion != "7.2.4" || c.BlockCacheMB != 1024 || c.MaxSnapshots != 8 {
		t.Errorf("string and int flags: %+v", c)
	}
	if c.LeaseTTL != 90*time.Second || c.LeaseMax != 15*time.Minute || c.AnalyticsQueueTimeout != 1500*time.Millisecond {
		t.Errorf("durations: ttl %v max %v queue %v", c.LeaseTTL, c.LeaseMax, c.AnalyticsQueueTimeout)
	}
	if c.AnalyticsMaxConcurrent != 4 || c.AnalyticsReadBPS != 64<<20 {
		t.Errorf("analytics: %d %d", c.AnalyticsMaxConcurrent, c.AnalyticsReadBPS)
	}
	if !c.RocksStats || !c.EnableDebugCommands || c.ReadOnly != "/tmp/ckpt" {
		t.Errorf("bools and readonly: %v %v %q", c.RocksStats, c.EnableDebugCommands, c.ReadOnly)
	}
}

func TestParseErrors(t *testing.T) {
	cases := []struct {
		args []string
		want string
	}{
		{[]string{"--nope"}, "flag provided but not defined"},
		{[]string{"extra"}, `unexpected argument "extra"`},
		{[]string{"--databases", "8"}, "argument must be 16"},
		{[]string{"--fsync", "sometimes"}, "must be one of the following: always, everysec, no"},
		{[]string{"--block-cache-mb", "0"}, "between 1 and 1048576"},
		{[]string{"--block-cache-mb", "lots"}, "couldn't be parsed into an integer"},
		{[]string{"--addr", "localhost"}, "must be host:port"},
		{[]string{"--addr", "localhost:99999"}, "port must be a number"},
		{[]string{"--redis-version", "7.2"}, "must look like 7.2.0"},
		{[]string{"--lease-ttl", "0"}, "between 1 and 86400"},
		{[]string{"--lease-ttl", "soon"}, "whole seconds or a duration"},
		{[]string{"--lease-ttl", "700"}, "lease-max (600) must not be shorter than lease-ttl (700)"},
		{[]string{"--analytics-read-bps", "-1"}, "memory value"},
		{[]string{"--analytics-read-bps", "1.5mb"}, "memory value"},
		{[]string{"--rocks-stats=maybe"}, "'yes' or 'no'"},
		{[]string{"--dir", "", "--readonly", ""}, "dir: must not be empty"},
		{[]string{"--config", "/nonexistent/nildb.conf"}, "no such file"},
	}
	for _, c := range cases {
		_, err := Parse(c.args)
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("Parse(%q) error = %v, want it to contain %q", c.args, err, c.want)
		}
	}
	if _, err := Parse([]string{"-h"}); !errors.Is(err, flag.ErrHelp) {
		t.Errorf("-h: %v, want flag.ErrHelp", err)
	}
	if _, err := Parse([]string{"--dir", "", "--readonly", "/ckpt"}); err != nil {
		t.Errorf("empty dir with --readonly: %v", err)
	}
}

func TestConfigFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nildb.conf")
	body := `# NilDB test config
dir = /var/lib/nildb
  nildb.max-snapshots=12
fsync="no"

ROCKS-STATS = yes
lease-ttl = 120
block-cache-mb = 128
`
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	c, err := Parse([]string{"--config", path, "--block-cache-mb", "256"})
	if err != nil {
		t.Fatal(err)
	}
	if c.Dir != "/var/lib/nildb" || c.MaxSnapshots != 12 || c.Fsync != FsyncNo || !c.RocksStats || c.LeaseTTL != 2*time.Minute {
		t.Errorf("file values not applied: %+v", c)
	}
	if c.BlockCacheMB != 256 {
		t.Errorf("BlockCacheMB = %d, want the command-line 256 over the file's 128", c.BlockCacheMB)
	}

	for body, want := range map[string]string{
		"dir = x\nbogus = 1\n":        "nildb.conf:2: unknown option \"bogus\"",
		"config = other.conf\n":       "unknown option \"config\"",
		"just words\n":                "nildb.conf:1: expected key=value",
		"max-snapshots = none\n":      "nildb.conf:1: max-snapshots: argument couldn't be parsed into an integer",
		"databases = 32\n":            "argument must be 16",
		"requirepass = \"a b\"\nx=\n": "unknown option \"x\"",
	} {
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		_, err := Parse([]string{"--config", path})
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("file %q: error = %v, want it to contain %q", body, err, want)
		}
	}
}

func TestValidate(t *testing.T) {
	cases := []struct {
		mutate func(*Config)
		want   string
	}{
		{func(c *Config) { c.Databases = 15 }, "databases: must be 16 in v1, got 15"},
		{func(c *Config) { c.Fsync = "" }, "fsync"},
		{func(c *Config) { c.Fsync = "EVERYSEC" }, `fsync: must be one of always, everysec, no, got "EVERYSEC"`},
		{func(c *Config) { c.Addr = "" }, "addr: must be host:port"},
		{func(c *Config) { c.RedisVersion = "" }, "redis-version"},
		{func(c *Config) { c.Dir = "" }, "dir: must not be empty"},
		{func(c *Config) { c.MaxSnapshots = 0 }, "max-snapshots: must be between 1"},
		{func(c *Config) { c.LeaseTTL = 0 }, "lease-ttl: must be between 1 and 86400, got 0"},
		{func(c *Config) { c.LeaseMax = 30 * time.Second }, "lease-max (30) must not be shorter than lease-ttl (60)"},
		{func(c *Config) { c.AnalyticsMaxConcurrent = 0 }, "analytics-max-concurrent"},
		{func(c *Config) { c.AnalyticsQueueTimeout = -time.Second }, "analytics-queue-timeout"},
		{func(c *Config) { c.AnalyticsReadBPS = -1 }, "analytics-read-bps"},
		{func(c *Config) { c.AnalyticsMaxRows = 0 }, "analytics-max-rows"},
		{func(c *Config) { c.GeoQueryMaxCells = 0 }, "geo-query-max-cells"},
		{func(c *Config) { c.SetOpMaxMembers = 0 }, "setop-max-members"},
		{func(c *Config) { c.MultiQueueMax = 0 }, "multi-queue-max"},
		{func(c *Config) { c.WriteBufferMB = 0 }, "write-buffer-mb"},
		{func(c *Config) { c.Clock = nil }, "clock: must not be nil"},
	}
	for _, tc := range cases {
		c := Default()
		tc.mutate(&c)
		err := c.Validate()
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("error = %v, want it to contain %q", err, tc.want)
		}
	}
	c := Default()
	c.Databases, c.MultiQueueMax = 1, 0
	if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "databases") || !strings.Contains(err.Error(), "multi-queue-max") {
		t.Errorf("Validate should report every problem: %v", err)
	}
}

func TestSetRoundTrips(t *testing.T) {
	cases := []struct {
		name, in, get string
		knob          func(Knobs) any
		want          any
	}{
		{"nildb.lease-ttl", "90", "90", func(k Knobs) any { return k.LeaseTTL }, 90 * time.Second},
		{"nildb.lease-ttl", "1500ms", "1.5s", func(k Knobs) any { return k.LeaseTTL }, 1500 * time.Millisecond},
		{"nildb.analytics-max-concurrent", "8", "8", func(k Knobs) any { return k.AnalyticsMaxConcurrent }, 8},
		{"nildb.analytics-queue-timeout", "0", "0", func(k Knobs) any { return k.AnalyticsQueueTimeout }, time.Duration(0)},
		{"nildb.analytics-queue-timeout", "2m", "120", func(k Knobs) any { return k.AnalyticsQueueTimeout }, 2 * time.Minute},
		{"nildb.analytics-read-bps", "0", "0", func(k Knobs) any { return k.AnalyticsReadBPS }, int64(0)},
		{"nildb.analytics-read-bps", "128MB", "134217728", func(k Knobs) any { return k.AnalyticsReadBPS }, int64(128 << 20)},
		{"nildb.analytics-read-bps", "5k", "5000", func(k Knobs) any { return k.AnalyticsReadBPS }, int64(5000)},
		{"nildb.analytics-max-rows", "1000", "1000", func(k Knobs) any { return k.AnalyticsMaxRows }, int64(1000)},
		{"nildb.analytics-group-mem", "1gb", "1073741824", func(k Knobs) any { return k.AnalyticsGroupMem }, int64(1 << 30)},
		{"nildb.doc-agg-mem", "4096", "4096", func(k Knobs) any { return k.DocAggMem }, int64(4096)},
		{"nildb.geo-query-max-cells", "32", "32", func(k Knobs) any { return k.GeoQueryMaxCells }, 32},
		{"nildb.setop-max-members", "10", "10", func(k Knobs) any { return k.SetOpMaxMembers }, 10},
		{"NILDB.Multi-Queue-Max", "7", "7", func(k Knobs) any { return k.MultiQueueMax }, 7},
	}
	for _, tc := range cases {
		c := Default()
		if err := c.Set(tc.name, tc.in); err != nil {
			t.Errorf("Set(%s, %s): %v", tc.name, tc.in, err)
			continue
		}
		if got, ok := c.Get(tc.name); !ok || got != tc.get {
			t.Errorf("Get(%s) after Set %s = %q, %v; want %q", tc.name, tc.in, got, ok, tc.get)
		}
		if got := tc.knob(c.Knobs()); got != tc.want {
			t.Errorf("Knobs after Set(%s, %s) = %v, want %v", tc.name, tc.in, got, tc.want)
		}
		// Get's output is accepted by Set and lands on the same value.
		again := Default()
		got, _ := c.Get(tc.name)
		if err := again.Set(tc.name, got); err != nil || tc.knob(again.Knobs()) != tc.want {
			t.Errorf("Set(%s, Get output %q): %v", tc.name, got, err)
		}
	}
}

func TestSetErrors(t *testing.T) {
	c := Default()
	before := c.Knobs()
	cases := []struct {
		name, value string
		want        error
		text        string
	}{
		{"nildb.nope", "1", ErrUnknownOption, ""},
		{"maxmemory", "1", ErrUnknownOption, ""},
		{"nildb.requirepass", "x", ErrUnknownOption, ""},
		{"nildb.dir", "/tmp", ErrImmutable, ""},
		{"nildb.databases", "16", ErrImmutable, ""},
		{"nildb.max-snapshots", "64", ErrImmutable, ""},
		{"nildb.lease-max", "900", ErrImmutable, ""},
		{"nildb.rocks-stats", "yes", ErrImmutable, ""},
		{"nildb.analytics-max-concurrent", "0", nil, "argument must be between 1 and 1024 inclusive"},
		{"nildb.analytics-max-concurrent", "two", nil, "argument couldn't be parsed into an integer"},
		{"nildb.analytics-read-bps", "-5", nil, "argument must be a memory value"},
		{"nildb.analytics-read-bps", "5tb", nil, "argument must be a memory value"},
		{"nildb.lease-ttl", "601", nil, "argument must not exceed lease-max (600)"},
		{"nildb.lease-ttl", "-1s", nil, "argument must be between 1 and 86400 inclusive"},
		{"nildb.geo-query-max-cells", "1001", nil, "argument must be between 1 and 1000 inclusive"},
	}
	for _, tc := range cases {
		err := c.Set(tc.name, tc.value)
		switch {
		case err == nil:
			t.Errorf("Set(%s, %s) succeeded", tc.name, tc.value)
		case tc.want != nil && err != tc.want:
			t.Errorf("Set(%s, %s) = %v, want %v", tc.name, tc.value, err, tc.want)
		case tc.want == nil && err.Error() != tc.text:
			t.Errorf("Set(%s, %s) = %q, want %q", tc.name, tc.value, err, tc.text)
		}
	}
	if c.Knobs() != before {
		t.Errorf("failed Sets changed the knobs: %+v", c.Knobs())
	}
	if ErrImmutable.Error() != "can't set immutable config" {
		t.Errorf("ErrImmutable text %q", ErrImmutable)
	}
}

func TestGetAndNames(t *testing.T) {
	c := Default()
	for name, want := range map[string]string{
		"nildb.dir":                   "./data",
		"nildb.fsync":                 "everysec",
		"nildb.databases":             "16",
		"nildb.redis-version":         "7.2.0",
		"nildb.lease-ttl":             "60",
		"nildb.analytics-read-bps":    "268435456",
		"nildb.rocks-stats":           "no",
		"nildb.readonly":              "",
		"NILDB.BLOCK-CACHE-MB":        "512",
		"nildb.enable-debug-commands": "no",
	} {
		if got, ok := c.Get(name); !ok || got != want {
			t.Errorf("Get(%s) = %q, %v; want %q", name, got, ok, want)
		}
	}
	for _, name := range []string{"nildb.requirepass", "nildb.config", "dir", "nildb.", "", "nildb.nope"} {
		if got, ok := c.Get(name); ok {
			t.Errorf("Get(%q) = %q, want not found", name, got)
		}
	}
	names := KnobNames()
	if !slices.IsSorted(names) {
		t.Errorf("KnobNames not sorted: %v", names)
	}
	if len(names) != len(options)-1 {
		t.Errorf("KnobNames has %d names, want %d", len(names), len(options)-1)
	}
	for _, n := range names {
		if !strings.HasPrefix(n, KnobPrefix) {
			t.Errorf("name %q lacks the prefix", n)
		}
		if _, ok := c.Get(n); !ok {
			t.Errorf("Get(%q) not found", n)
		}
		if n == "nildb.requirepass" {
			t.Error("KnobNames lists requirepass")
		}
	}
	if !Mutable("nildb.lease-ttl") || Mutable("nildb.lease-max") || Mutable("nildb.requirepass") || Mutable("nope") {
		t.Error("Mutable answers wrong")
	}
}

// TestConcurrentSet runs CONFIG SET against readers; -race checks that
// Knobs and Get read atomically.
func TestConcurrentSet(t *testing.T) {
	c := Default()
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				k := c.Knobs()
				if k.AnalyticsMaxConcurrent < 1 || k.MultiQueueMax < 1 {
					t.Error("read a value Set never wrote")
					return
				}
				c.Get("nildb.analytics-read-bps")
			}
		}()
	}
	for i := range 2000 {
		n := i%100 + 1
		if err := c.Set("nildb.analytics-max-concurrent", strconv.Itoa(n%1024+1)); err != nil {
			t.Fatal(err)
		}
		if err := c.Set("nildb.multi-queue-max", strconv.Itoa(n)); err != nil {
			t.Fatal(err)
		}
		if err := c.Set("nildb.analytics-read-bps", strconv.Itoa(n)+"mb"); err != nil {
			t.Fatal(err)
		}
		if err := c.Set("nildb.lease-ttl", strconv.Itoa(n)); err != nil {
			t.Fatal(err)
		}
	}
	close(stop)
	wg.Wait()
}

func TestOffsetClock(t *testing.T) {
	var c OffsetClock
	if d := c.Now().Sub(time.Now()); d > time.Second || d < -time.Second {
		t.Errorf("zero OffsetClock is %v off the system clock", d)
	}
	c.Advance(time.Hour)
	c.Advance(30 * time.Minute)
	if c.Offset() != 90*time.Minute {
		t.Errorf("Offset() = %v", c.Offset())
	}
	if d := c.Now().Sub(time.Now()); d < 89*time.Minute || d > 91*time.Minute {
		t.Errorf("after Advance(90m) the clock is %v ahead", d)
	}
	c.Advance(-90 * time.Minute)
	if c.Offset() != 0 {
		t.Errorf("Offset() after moving back = %v", c.Offset())
	}

	cfg := Default()
	cfg.Clock = &c
	c.Advance(time.Hour)
	if d := cfg.Now().Sub(time.Now()); d < 59*time.Minute {
		t.Errorf("Config.Now ignores the clock: %v", d)
	}
	if d := cfg.ClockFunc()().Sub(time.Now()); d < 59*time.Minute {
		t.Errorf("Config.ClockFunc ignores the clock: %v", d)
	}
	var nilClock Config
	if d := nilClock.Now().Sub(time.Now()); d > time.Second || d < -time.Second {
		t.Errorf("Now with no clock is %v off", d)
	}

	var wg sync.WaitGroup
	var shared OffsetClock
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 1000 {
				shared.Advance(time.Millisecond)
				shared.Now()
			}
		}()
	}
	wg.Wait()
	if shared.Offset() != 8*time.Second {
		t.Errorf("concurrent Advance lost updates: %v", shared.Offset())
	}
}

func TestParseMemory(t *testing.T) {
	cases := map[string]int64{
		"0": 0, "1": 1, "1b": 1, "1k": 1000, "1kb": 1024, "1K": 1000, "1KB": 1024,
		"2m": 2_000_000, "2mb": 2 << 20, "3g": 3_000_000_000, "3gb": 3 << 30,
	}
	for in, want := range cases {
		if got, ok := parseMemory(in); !ok || got != want {
			t.Errorf("parseMemory(%q) = %d, %v; want %d", in, got, ok, want)
		}
	}
	for _, in := range []string{"", "-1", "mb", "1.5mb", "1 mb", "1tb", "99999999999999999999", "9223372036854775807kb"} {
		if got, ok := parseMemory(in); ok {
			t.Errorf("parseMemory(%q) = %d, want an error", in, got)
		}
	}
}

func TestUsage(t *testing.T) {
	var buf bytes.Buffer
	Usage(&buf)
	out := buf.String()
	for _, want := range []string{"-dir", "-addr", "-requirepass", "-fsync", "-block-cache-mb", "-databases", "-redis-version",
		"-max-snapshots", "-lease-ttl", "-analytics-max-concurrent", "-analytics-read-bps", "-rocks-stats",
		"-enable-debug-commands", "-readonly", "-config", "(default 512)", "(default everysec)"} {
		if !strings.Contains(out, want) {
			t.Errorf("Usage lacks %q", want)
		}
	}
}
