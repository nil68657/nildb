// Package testutil runs an in-process NilDB server for tests: a store in
// a temporary directory, a registry with the server's own commands plus
// whatever the test registers, a listener on a free loopback port, and a
// clock tests can move. It also hands out go-redis clients and a raw TCP
// client for byte-exact checks.
package testutil

import (
	"context"
	"sync"
	"testing"

	"github.com/nil68657/nildb/internal/command"
	"github.com/nil68657/nildb/internal/config"
	"github.com/nil68657/nildb/internal/server"
	"github.com/nil68657/nildb/internal/store"
)

// RegisterFunc registers a command package into the registry. It runs
// after the store is open and the server is built, before the server
// accepts connections, so it may use e.Store, e.Cfg and e.Server (for
// example as a command.InfoRegistrar).
type RegisterFunc func(r *command.Registry, e *Env) error

// Env is one running server.
type Env struct {
	Addr   string              // "127.0.0.1:<port>"
	Clock  *config.OffsetClock // the server clock; nil if mutate installed another kind
	Cfg    *config.Config
	Store  *store.Store
	Server *server.Server
	// Stop shuts the server down, waits for its connections and closes the
	// store. Spawn registers it with t.Cleanup; calling it earlier is fine
	// and later calls do nothing.
	Stop func()
}

// Spawn starts a server for the test. The configuration starts from
// config.Default() with Dir in t.TempDir(), Addr 127.0.0.1:0, an
// OffsetClock, NIL.DEBUG enabled and small caches; mutate may change any
// of it before validation. regs register the command packages under
// test. Spawn fails the test on any error. Tests that use it open
// RocksDB, so they should skip under -short.
func Spawn(t testing.TB, mutate func(*config.Config), regs ...RegisterFunc) *Env {
	t.Helper()
	clock := &config.OffsetClock{}
	cfg := config.Default()
	cfg.Dir = t.TempDir()
	cfg.Addr = "127.0.0.1:0"
	cfg.Clock = clock
	cfg.EnableDebugCommands = true
	cfg.BlockCacheMB = 16
	cfg.AnalyticsCacheMB = 8
	cfg.WriteBufferMB = 64
	if mutate != nil {
		mutate(&cfg)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("testutil: config: %v", err)
	}
	env := &Env{Cfg: &cfg}
	if oc, ok := cfg.Clock.(*config.OffsetClock); ok {
		env.Clock = oc
	}

	st, err := store.Open(store.Config{
		Dir:                 cfg.Dir,
		BlockCacheBytes:     cfg.BlockCacheBytes(),
		AnalyticsCacheBytes: cfg.AnalyticsCacheBytes(),
		WriteBufferBytes:    cfg.WriteBufferBytes(),
		BgIOBytesPerSec:     cfg.BgIOBytesPerSec(),
		Fsync:               cfg.Fsync,
		MaxSnapshots:        cfg.MaxSnapshots,
		LeaseTTL:            cfg.LeaseTTL,
		LeaseMax:            cfg.LeaseMax,
		Statistics:          cfg.RocksStats,
		Clock:               cfg.ClockFunc(),
	})
	if err != nil {
		t.Fatalf("testutil: open store: %v", err)
	}
	env.Store = st

	reg := command.NewRegistry()
	srv := server.New(env.Cfg, st, reg)
	env.Server = srv
	for _, fn := range regs {
		if err := fn(reg, env); err != nil {
			st.Close()
			t.Fatalf("testutil: register: %v", err)
		}
	}
	addr, err := srv.Listen()
	if err != nil {
		st.Close()
		t.Fatalf("testutil: listen: %v", err)
	}
	env.Addr = addr.String()

	ctx, cancel := context.WithCancel(context.Background())
	served := make(chan error, 1)
	go func() { served <- srv.Serve(ctx) }()
	var once sync.Once
	env.Stop = func() {
		once.Do(func() {
			cancel()
			if err := <-served; err != nil {
				t.Errorf("testutil: serve: %v", err)
			}
			if err := st.Close(); err != nil {
				t.Errorf("testutil: close store: %v", err)
			}
		})
	}
	t.Cleanup(env.Stop)
	return env
}
