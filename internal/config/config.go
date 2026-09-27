// Package config holds NilDB's configuration: the defaults, command-line
// and key=value file parsing (flags.go), validation, the nildb.* knobs
// that CONFIG GET and CONFIG SET reach, and the Clock every
// time-dependent component reads.
//
// Startup code reads Config fields directly. Once the server is serving,
// CONFIG SET may change the runtime knobs (the fields in Knobs) from
// another goroutine, so request-time code reads them through Knobs, which
// loads each one atomically; a plain field read would race with Set.
package config

import (
	"errors"
	"fmt"
	"net"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
	"unsafe"
)

// Fsync policies for Config.Fsync.
const (
	FsyncAlways   = "always"
	FsyncEverySec = "everysec"
	FsyncNo       = "no"
)

// Databases is the number of Redis databases. SELECT takes 0..15 and the
// value is fixed in v1.
const Databases = 16

// Config is the full server configuration.
type Config struct {
	Dir          string // RocksDB directory
	Addr         string // listen address, host:port
	RequirePass  string // password of the default user; empty means no AUTH
	Fsync        string // FsyncAlways, FsyncEverySec or FsyncNo
	Databases    int    // fixed at 16 in v1
	RedisVersion string // reported by HELLO and INFO, "7.2.0"

	BlockCacheMB     int // main HyperClockCache
	AnalyticsCacheMB int // col CF cache
	WriteBufferMB    int // WriteBufferManager cap across all CFs
	BgIOMBPerSec     int // RocksDB rate limiter

	MaxSnapshots int           // leases plus cursors
	LeaseTTL     time.Duration // default lease lifetime (runtime knob)
	LeaseMax     time.Duration // longest TTL NIL.SNAPSHOT CREATE accepts

	AnalyticsMaxConcurrent int           // analytics semaphore size (runtime knob)
	AnalyticsQueueTimeout  time.Duration // wait for a semaphore slot (runtime knob)
	AnalyticsReadBPS       int64         // byte bucket, bytes per second, 0 = off (runtime knob)
	AnalyticsMaxRows       int64         // row cap per analytical command (runtime knob)
	AnalyticsGroupMem      int64         // default MAXMEM of NIL.AGGREGATE, bytes (runtime knob)
	DocAggMem              int64         // default MAXMEM of DOC.AGGREGATE, bytes (runtime knob)
	GeoQueryMaxCells       int           // RegionCoverer MaxCells for 2dsphere queries (runtime knob)
	SetOpMaxMembers        int           // cap on in-memory set operations (runtime knob)
	MultiQueueMax          int           // commands queued in one MULTI (runtime knob)

	RocksStats          bool   // --rocks-stats: RocksDB statistics on
	EnableDebugCommands bool   // NIL.DEBUG, including CLOCK-ADVANCE
	ReadOnly            string // checkpoint directory opened read-only by --readonly; empty for normal mode

	Clock Clock
}

// Default returns the v1 defaults with the system clock.
func Default() Config {
	return Config{
		Dir:                    "./data",
		Addr:                   "127.0.0.1:6380",
		Fsync:                  FsyncEverySec,
		Databases:              Databases,
		RedisVersion:           "7.2.0",
		BlockCacheMB:           512,
		AnalyticsCacheMB:       64,
		WriteBufferMB:          256,
		BgIOMBPerSec:           200,
		MaxSnapshots:           32,
		LeaseTTL:               60 * time.Second,
		LeaseMax:               600 * time.Second,
		AnalyticsMaxConcurrent: 2,
		AnalyticsQueueTimeout:  5 * time.Second,
		AnalyticsReadBPS:       256 << 20,
		AnalyticsMaxRows:       50_000_000,
		AnalyticsGroupMem:      100 << 20,
		DocAggMem:              32 << 20,
		GeoQueryMaxCells:       16,
		SetOpMaxMembers:        1_000_000,
		MultiQueueMax:          100_000,
		Clock:                  SystemClock{},
	}
}

// BlockCacheBytes is BlockCacheMB in bytes, for store.Config.BlockCacheBytes.
func (c *Config) BlockCacheBytes() int64 { return int64(c.BlockCacheMB) << 20 }

// AnalyticsCacheBytes is AnalyticsCacheMB in bytes, for
// store.Config.AnalyticsCacheBytes.
func (c *Config) AnalyticsCacheBytes() int64 { return int64(c.AnalyticsCacheMB) << 20 }

// WriteBufferBytes is WriteBufferMB in bytes, for store.Config.WriteBufferBytes.
func (c *Config) WriteBufferBytes() int64 { return int64(c.WriteBufferMB) << 20 }

// BgIOBytesPerSec is BgIOMBPerSec in bytes, for store.Config.BgIOBytesPerSec.
func (c *Config) BgIOBytesPerSec() int64 { return int64(c.BgIOMBPerSec) << 20 }

// Now reads the configured clock, or the system clock when none is set.
func (c *Config) Now() time.Time {
	if c.Clock == nil {
		return time.Now()
	}
	return c.Clock.Now()
}

// ClockFunc returns the configured clock as a function, for
// store.Config.Clock.
func (c *Config) ClockFunc() func() time.Time {
	if c.Clock == nil {
		return time.Now
	}
	return c.Clock.Now
}

// Knobs holds the settings CONFIG SET can change while the server runs.
type Knobs struct {
	LeaseTTL               time.Duration
	AnalyticsMaxConcurrent int
	AnalyticsQueueTimeout  time.Duration
	AnalyticsReadBPS       int64
	AnalyticsMaxRows       int64
	AnalyticsGroupMem      int64
	DocAggMem              int64
	GeoQueryMaxCells       int
	SetOpMaxMembers        int
	MultiQueueMax          int
}

// Knobs returns the current runtime knobs. Each field is loaded
// atomically, so it is safe to call while Set runs on another goroutine;
// two knobs changed by two separate CONFIG SET calls may be seen one old,
// one new.
func (c *Config) Knobs() Knobs {
	return Knobs{
		LeaseTTL:               loadDuration(&c.LeaseTTL),
		AnalyticsMaxConcurrent: loadInt(&c.AnalyticsMaxConcurrent),
		AnalyticsQueueTimeout:  loadDuration(&c.AnalyticsQueueTimeout),
		AnalyticsReadBPS:       atomic.LoadInt64(&c.AnalyticsReadBPS),
		AnalyticsMaxRows:       atomic.LoadInt64(&c.AnalyticsMaxRows),
		AnalyticsGroupMem:      atomic.LoadInt64(&c.AnalyticsGroupMem),
		DocAggMem:              atomic.LoadInt64(&c.DocAggMem),
		GeoQueryMaxCells:       loadInt(&c.GeoQueryMaxCells),
		SetOpMaxMembers:        loadInt(&c.SetOpMaxMembers),
		MultiQueueMax:          loadInt(&c.MultiQueueMax),
	}
}

// The runtime knobs of type int are accessed as int64, which needs int to
// be 64 bits wide. Both lines fail to compile on any other platform.
var (
	_ [unsafe.Sizeof(int(0)) - 8]struct{}
	_ [8 - unsafe.Sizeof(int(0))]struct{}
)

func loadInt(p *int) int { return int(atomic.LoadInt64((*int64)(unsafe.Pointer(p)))) }

func storeInt(p *int, v int) { atomic.StoreInt64((*int64)(unsafe.Pointer(p)), int64(v)) }

func loadDuration(p *time.Duration) time.Duration {
	return time.Duration(atomic.LoadInt64((*int64)(p)))
}

func storeDuration(p *time.Duration, v time.Duration) {
	atomic.StoreInt64((*int64)(p), int64(v))
}

// Validate checks every field and returns all problems joined, or nil.
func (c *Config) Validate() error {
	var errs []error
	bad := func(format string, a ...any) {
		errs = append(errs, fmt.Errorf("config: "+format, a...))
	}
	for i := range options {
		o := &options[i]
		if err := o.validate(c); err != nil {
			bad("%s: %v", o.name, err)
		}
	}
	if c.Dir == "" && c.ReadOnly == "" {
		bad("dir: must not be empty")
	}
	if c.LeaseMax < c.LeaseTTL {
		bad("lease-max (%s) must not be shorter than lease-ttl (%s)", formatDuration(c.LeaseMax), formatDuration(c.LeaseTTL))
	}
	if c.Clock == nil {
		bad("clock: must not be nil")
	}
	return errors.Join(errs...)
}

// ErrUnknownOption is returned by Set for a name that is not a nildb.*
// knob. The server replies "ERR Unknown option or number of arguments for
// CONFIG SET - '<name>'".
var ErrUnknownOption = errors.New("unknown option")

// ErrImmutable is returned by Set for a knob that only a restart changes.
// Its text is Redis's reason for such a CONFIG SET.
var ErrImmutable = errors.New("can't set immutable config")

// KnobPrefix starts every name Get, Set and KnobNames use.
const KnobPrefix = "nildb."

// KnobNames returns the names CONFIG GET matches its pattern against,
// "nildb.<flag name>", sorted. requirepass is left out.
func KnobNames() []string {
	names := make([]string, 0, len(options))
	for i := range options {
		if !options[i].secret {
			names = append(names, KnobPrefix+options[i].name)
		}
	}
	slices.Sort(names)
	return names
}

// Mutable reports whether CONFIG SET may change the named knob.
func Mutable(name string) bool {
	o := lookupKnob(name)
	return o != nil && o.mutable
}

// Get returns the current value of a nildb.* knob as CONFIG GET prints
// it: integers in decimal, byte sizes in bytes, booleans as yes or no,
// durations in whole seconds or, when not whole, as Go prints them
// ("1.5s"). The name is matched without regard to case. It reports false
// for an unknown name and for requirepass.
func (c *Config) Get(name string) (string, bool) {
	o := lookupKnob(name)
	if o == nil {
		return "", false
	}
	return o.format(c), true
}

// Set changes a runtime knob, as CONFIG SET nildb.<name> does. It accepts
// what the matching flag accepts. It returns ErrUnknownOption for an
// unknown name, ErrImmutable for a knob that is not runtime, and
// otherwise an error whose text is the reason Redis gives after
// "ERR CONFIG SET failed (possibly related to argument '<name>') - ".
// A failed Set changes nothing.
func (c *Config) Set(name, value string) error {
	o := lookupKnob(name)
	if o == nil {
		return ErrUnknownOption
	}
	if !o.mutable {
		return ErrImmutable
	}
	return o.set(c, value, true)
}

// lookupKnob finds the option named "nildb.<name>", ignoring case.
func lookupKnob(name string) *option {
	if len(name) < len(KnobPrefix) || !strings.EqualFold(name[:len(KnobPrefix)], KnobPrefix) {
		return nil
	}
	o := lookupOption(strings.ToLower(name[len(KnobPrefix):]))
	if o == nil || o.secret {
		return nil
	}
	return o
}

func lookupOption(name string) *option {
	for i := range options {
		if options[i].name == name {
			return &options[i]
		}
	}
	return nil
}

func checkAddr(s string) error {
	_, port, err := net.SplitHostPort(s)
	if err != nil {
		return fmt.Errorf("must be host:port")
	}
	p, err := strconv.Atoi(port)
	if err != nil || p < 0 || p > 65535 {
		return fmt.Errorf("port must be a number from 0 to 65535")
	}
	return nil
}

func checkVersion(s string) error {
	parts := strings.Split(s, ".")
	if len(parts) != 3 {
		return fmt.Errorf("must look like 7.2.0")
	}
	for _, p := range parts {
		if p == "" || strings.Trim(p, "0123456789") != "" {
			return fmt.Errorf("must look like 7.2.0")
		}
	}
	return nil
}

// Clock is the time source for expiry, versions, leases and the fake
// clock of tests.
type Clock interface{ Now() time.Time }

// SystemClock reads time.Now.
type SystemClock struct{}

// Now returns time.Now().
func (SystemClock) Now() time.Time { return time.Now() }

// OffsetClock is the system clock shifted by an offset that Advance moves
// forward (or back, with a negative duration). The zero value reads the
// system time. testutil and NIL.DEBUG CLOCK-ADVANCE use it to expire keys
// without sleeping. Use it through a pointer; it is safe for concurrent
// use.
type OffsetClock struct {
	offset atomic.Int64
}

// Now returns the system time plus the offset.
func (c *OffsetClock) Now() time.Time {
	return time.Now().Add(time.Duration(c.offset.Load()))
}

// Advance adds d to the offset.
func (c *OffsetClock) Advance(d time.Duration) { c.offset.Add(int64(d)) }

// Offset returns the current offset.
func (c *OffsetClock) Offset() time.Duration { return time.Duration(c.offset.Load()) }
