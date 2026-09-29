// nildb-ui is a web console for NilDB. It connects to a running server over
// RESP3, the way any client does, and serves a single-page console on
// localhost: server overview, key browser and editors, a redis-cli style
// REPL, the DOC.* document store, GEO sets on a world map, and NIL.*
// analytics on snapshot leases. The frontend is embedded and makes no
// request beyond this process.
//
//	nildb-ui --nildb 127.0.0.1:6380 --addr 127.0.0.1:8090 [--readonly]
//
// The API can run any command, so it listens on localhost by default and
// refuses requests from other origins; --readonly refuses every command
// whose COMMAND INFO flags say it writes.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

const (
	poolSize = 8
	maxReply = 64 << 20
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lmsgprefix)
	log.SetPrefix("nildb-ui: ")
	if err := run(os.Args[1:]); err != nil {
		log.Print(err)
		os.Exit(1)
	}
}

func run(args []string) error {
	fs := flag.NewFlagSet("nildb-ui", flag.ContinueOnError)
	nildb := fs.String("nildb", "127.0.0.1:6380", "NilDB address, host:port")
	addr := fs.String("addr", "127.0.0.1:8090", "address the console listens on")
	password := fs.String("password", "", "password for AUTH (NilDB's --requirepass); read from NILDB_PASSWORD when empty")
	readonly := fs.Bool("readonly", false, "refuse commands that write data or change the server")
	timeout := fs.Duration("timeout", time.Minute, "how long one command may run")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if fs.NArg() > 0 {
		return fmt.Errorf("unexpected argument %q", fs.Arg(0))
	}
	if *timeout <= 0 {
		return errors.New("--timeout must be positive")
	}
	if *password == "" {
		*password = os.Getenv("NILDB_PASSWORD")
	}

	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		return err
	}
	host, port, _ := net.SplitHostPort(ln.Addr().String())
	cfg := &clientConfig{addr: *nildb, password: *password, timeout: *timeout, maxReply: maxReply}
	srv, err := newServer(options{nildb: *nildb, readonly: *readonly}, cfg, poolSize, port)
	if err != nil {
		ln.Close()
		return err
	}
	defer srv.close()

	if ip := net.ParseIP(host); ip == nil || !ip.IsLoopback() {
		log.Printf("warning: listening on %s, so other machines can reach a console that runs any command; bind 127.0.0.1 or add --readonly", ln.Addr())
	}
	mode := ""
	if *readonly {
		mode = ", read-only"
	}
	log.Printf("console on http://%s/ for NilDB at %s%s", ln.Addr(), *nildb, mode)

	hs := &http.Server{
		Handler:           srv.handler(),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       time.Minute,
		WriteTimeout:      *timeout + 15*time.Second,
		IdleTimeout:       2 * time.Minute,
		ErrorLog:          log.Default(),
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		shut, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = hs.Shutdown(shut)
	}()
	if err := hs.Serve(ln); !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	log.Print("stopped")
	return nil
}
