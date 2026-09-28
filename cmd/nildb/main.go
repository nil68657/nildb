// nildb is the NilDB server: Redis 7.2 data commands, ROCKS.* admin
// commands, and the DOC.* document and NIL.* analytics commands, over
// RESP2 and RESP3 on one port, stored in RocksDB. nildb -h
// lists the flags; --config names a key=value file (internal/config).
//
// SIGINT, SIGTERM or the SHUTDOWN command stop it: the listener closes,
// in-flight commands finish (sockets still busy after 5 s are closed),
// the WAL is fsynced and the store is closed.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nil68657/nildb/internal/admin"
	"github.com/nil68657/nildb/internal/analytics"
	"github.com/nil68657/nildb/internal/catalog"
	"github.com/nil68657/nildb/internal/cmddoc"
	"github.com/nil68657/nildb/internal/command"
	"github.com/nil68657/nildb/internal/config"
	"github.com/nil68657/nildb/internal/docstore"
	"github.com/nil68657/nildb/internal/query"
	"github.com/nil68657/nildb/internal/redis"
	"github.com/nil68657/nildb/internal/server"
	"github.com/nil68657/nildb/internal/store"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lmsgprefix)
	log.SetPrefix("nildb: ")
	if err := run(os.Args[1:]); err != nil {
		log.Print(err)
		os.Exit(1)
	}
}

// run serves until SIGINT, SIGTERM or SHUTDOWN. Cleanup runs in deferred
// calls, last registered first: the WAL ticker stops, then anything the
// document block defers, then the final WAL fsync and the store close.
func run(args []string) (err error) {
	cfg, err := config.Parse(args)
	if errors.Is(err, flag.ErrHelp) {
		config.Usage(os.Stdout)
		return nil
	}
	if err != nil {
		return err
	}

	st, err := openStore(&cfg)
	if err != nil {
		return err
	}
	defer func() {
		if !st.ReadOnly() {
			if serr := st.FlushWAL(true); serr != nil {
				log.Printf("final WAL sync: %v", serr)
			}
		}
		if cerr := st.Close(); cerr != nil && err == nil {
			err = fmt.Errorf("close store: %w", cerr)
		}
	}()

	cat, err := catalog.Open(st)
	if err != nil {
		return fmt.Errorf("open catalog: %w", err)
	}
	ds := docstore.New(st, cat)
	if !st.ReadOnly() {
		if err := ds.ResumeBuilds(); err != nil {
			return fmt.Errorf("resume index builds: %w", err)
		}
	}

	reg := command.NewRegistry()
	reg.SetCatalog(catalogView{cat})
	srv := server.New(&cfg, st, reg)
	if err := redis.Register(reg); err != nil {
		return err
	}
	if err := admin.Register(reg, srv, st); err != nil {
		return err
	}

	// DOC.* and NIL.* share one query engine. Its deferred Close runs
	// after Serve returns and before the store closes, and ends every open
	// cursor and the leases they hold.
	q := query.New(st, &cfg)
	defer q.Close()
	cmddoc.Register(reg, cat, ds, q)
	analytics.Register(reg, srv, &cfg, cat, q)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if cfg.Fsync == store.FsyncEverySec && !st.ReadOnly() {
		quit, done := make(chan struct{}), make(chan struct{})
		go syncWAL(st, quit, done)
		defer func() {
			close(quit)
			<-done
		}()
	}

	addr, err := srv.Listen()
	if err != nil {
		return err
	}
	major, minor, patch := store.Version()
	log.Printf("listening on %s, data in %s, fsync %s, RocksDB %d.%d.%d", addr, st.Config().Dir, cfg.Fsync, major, minor, patch)
	logged := make(chan struct{})
	go func() {
		defer close(logged)
		select {
		case <-ctx.Done():
		case <-srv.Done():
		}
		log.Print("shutting down")
	}()
	err = srv.Serve(ctx)
	srv.Shutdown() // a no-op after a normal stop; closes srv.Done if Serve failed first
	<-logged
	if err != nil {
		return err
	}
	log.Print("stopped")
	return nil
}

// openStore opens the data directory, creating it when missing, or with
// --readonly opens that checkpoint directory without write access.
func openStore(cfg *config.Config) (*store.Store, error) {
	sc := store.Config{
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
	}
	if cfg.ReadOnly != "" {
		sc.Dir = cfg.ReadOnly
		return store.OpenReadOnly(sc)
	}
	if err := os.MkdirAll(cfg.Dir, 0o750); err != nil {
		return nil, err
	}
	return store.Open(sc)
}

// syncWAL fsyncs the WAL once a second, the everysec policy, until quit
// closes; then it closes done.
func syncWAL(st *store.Store, quit <-chan struct{}, done chan<- struct{}) {
	defer close(done)
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-quit:
			return
		case <-t.C:
			if err := st.FlushWAL(true); err != nil {
				log.Printf("WAL sync: %v", err)
			}
		}
	}
}
