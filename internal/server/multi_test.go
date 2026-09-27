package server_test

import (
	"context"
	"errors"
	"io"
	"log"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/nil68657/nildb/internal/command"
	"github.com/nil68657/nildb/internal/server"
	"github.com/nil68657/nildb/internal/testutil"
)

// spawn starts a server with the fake string commands of testcmds_test.go.
func spawn(t *testing.T) *testutil.Env {
	t.Helper()
	if testing.Short() {
		t.Skip("opens RocksDB")
	}
	return testutil.Spawn(t, nil, func(r *command.Registry, _ *testutil.Env) error {
		r.Register(server.FakeStringCommands()...)
		return nil
	})
}

// do sends one command and compares the raw reply.
func do(t *testing.T, c *testutil.TCP, want string, args ...string) {
	t.Helper()
	if got := string(c.Do(args...)); got != want {
		t.Fatalf("%q:\n got %q\nwant %q", args, got, want)
	}
}

const (
	ok      = "+OK\r\n"
	queued  = "+QUEUED\r\n"
	pong    = "+PONG\r\n"
	nullArr = "*-1\r\n"
)

func bulk(s string) string { return "$" + strconv.Itoa(len(s)) + "\r\n" + s + "\r\n" }

func TestMultiExecBasics(t *testing.T) {
	e := spawn(t)
	c := e.TCP(t)
	do(t, c, ok, "MULTI")
	do(t, c, queued, "SET", "a", "1")
	do(t, c, queued, "INCR", "a")
	do(t, c, queued, "GET", "a")
	do(t, c, queued, "PING")
	do(t, c, "*4\r\n"+ok+":2\r\n"+bulk("2")+pong, "EXEC")
	do(t, c, bulk("2"), "GET", "a")
	do(t, c, ok, "MULTI")
	do(t, c, "*0\r\n", "EXEC")
}

func TestDiscard(t *testing.T) {
	e := spawn(t)
	c := e.TCP(t)
	do(t, c, ok, "SET", "k", "v")
	do(t, c, ok, "MULTI")
	do(t, c, queued, "DEL", "k")
	do(t, c, ok, "DISCARD")
	do(t, c, bulk("v"), "GET", "k")
	do(t, c, "-ERR DISCARD without MULTI\r\n", "DISCARD")
	do(t, c, "-ERR EXEC without MULTI\r\n", "EXEC")
}

func TestNestedMultiAndWatchInsideMulti(t *testing.T) {
	e := spawn(t)
	c := e.TCP(t)
	do(t, c, ok, "MULTI")
	do(t, c, "-ERR MULTI calls can not be nested\r\n", "MULTI")
	do(t, c, "-ERR WATCH inside MULTI is not allowed\r\n", "WATCH", "x")
	do(t, c, queued, "SET", "x", "1")
	// Neither error aborts the transaction.
	do(t, c, "*1\r\n"+ok, "EXEC")
}

func TestExecAbortOnQueueErrors(t *testing.T) {
	e := spawn(t)
	c := e.TCP(t)
	cases := [][]string{
		{"non-existing-command"},
		{"SET", "foo1"},              // arity
		{"FLUSHDB"},                  // NoMulti
		{"CLIENT", "no-such-subcmd"}, // unknown subcommand
	}
	wantErr := []string{
		"-ERR unknown command 'non-existing-command', with args beginning with: \r\n",
		"-ERR wrong number of arguments for 'set' command\r\n",
		"-ERR Command not allowed inside a transaction\r\n",
		"-ERR unknown subcommand 'no-such-subcmd'. Try CLIENT HELP.\r\n",
	}
	for i, bad := range cases {
		do(t, c, ok, "MULTI")
		do(t, c, queued, "SET", "foo1", "bar1")
		do(t, c, wantErr[i], bad...)
		do(t, c, queued, "SET", "foo2", "bar2")
		do(t, c, "-EXECABORT Transaction discarded because of previous errors.\r\n", "EXEC")
		do(t, c, ":0\r\n", "EXISTS", "foo1", "foo2")
		do(t, c, pong, "PING") // the MULTI state is gone
	}
}

func TestExecRuntimeErrorsAreElements(t *testing.T) {
	e := spawn(t)
	c := e.TCP(t)
	do(t, c, ok, "SET", "s", "notanumber")
	do(t, c, ok, "MULTI")
	do(t, c, queued, "SET", "a", "1")
	do(t, c, queued, "INCR", "s")
	do(t, c, queued, "KEYSERR")
	do(t, c, queued, "INCR", "a")
	do(t, c, "*4\r\n"+ok+"-ERR value is not an integer or out of range\r\n-ERR syntax error\r\n:2\r\n", "EXEC")
	do(t, c, bulk("2"), "GET", "a")
}

func TestExecPanicDiscardsEverything(t *testing.T) {
	log.SetOutput(io.Discard)
	t.Cleanup(func() { log.SetOutput(os.Stderr) })
	e := spawn(t)
	c := e.TCP(t)
	do(t, c, ok, "MULTI")
	do(t, c, queued, "SET", "a", "1")
	do(t, c, queued, "PANIC", "b")
	do(t, c, "-ERR internal error in 'panic': test panic\r\n", "EXEC")
	do(t, c, ":0\r\n", "EXISTS", "a", "b")
}

func TestSelectInsideMulti(t *testing.T) {
	e := spawn(t)
	c := e.TCP(t)
	do(t, c, ok, "MULTI")
	do(t, c, queued, "SET", "k", "zero")
	do(t, c, queued, "SELECT", "2")
	do(t, c, queued, "SET", "k", "two")
	do(t, c, queued, "MGET", "k")
	do(t, c, "*4\r\n"+ok+ok+ok+"*1\r\n"+bulk("two"), "EXEC")
	do(t, c, bulk("two"), "GET", "k")
	do(t, c, ok, "SELECT", "0")
	do(t, c, bulk("zero"), "GET", "k")
}

func TestWatch(t *testing.T) {
	e := spawn(t)
	c := e.TCP(t)
	other := e.TCP(t)

	t.Run("not modified", func(t *testing.T) {
		do(t, c, ok, "WATCH", "x", "y", "z")
		do(t, c, ok, "WATCH", "k")
		do(t, c, ok, "MULTI")
		do(t, c, queued, "PING")
		do(t, c, "*1\r\n"+pong, "EXEC")
	})
	t.Run("1 of 1 modified", func(t *testing.T) {
		do(t, c, ok, "SET", "x", "30")
		do(t, c, ok, "WATCH", "x")
		do(t, c, ok, "SET", "x", "40")
		do(t, c, ok, "MULTI")
		do(t, c, queued, "PING")
		do(t, c, nullArr, "EXEC")
	})
	t.Run("1 of 5 modified by another client", func(t *testing.T) {
		do(t, c, ok, "SET", "x", "30")
		do(t, c, ok, "WATCH", "a", "b", "x", "k", "z")
		do(t, other, ok, "SET", "x", "40")
		do(t, c, ok, "MULTI")
		do(t, c, queued, "PING")
		do(t, c, nullArr, "EXEC")
	})
	t.Run("no longer watched after EXEC", func(t *testing.T) {
		do(t, c, ok, "SET", "x", "30")
		do(t, c, ok, "WATCH", "x")
		do(t, c, ok, "MULTI")
		do(t, c, queued, "PING")
		do(t, c, "*1\r\n"+pong, "EXEC")
		do(t, c, ok, "SET", "x", "40")
		do(t, c, ok, "MULTI")
		do(t, c, queued, "PING")
		do(t, c, "*1\r\n"+pong, "EXEC")
	})
	t.Run("no longer watched after failed EXEC", func(t *testing.T) {
		do(t, c, ok, "SET", "x", "30")
		do(t, c, ok, "WATCH", "x")
		do(t, c, ok, "SET", "x", "40")
		do(t, c, ok, "MULTI")
		do(t, c, queued, "PING")
		do(t, c, nullArr, "EXEC")
		do(t, c, ok, "SET", "x", "40")
		do(t, c, ok, "MULTI")
		do(t, c, queued, "PING")
		do(t, c, "*1\r\n"+pong, "EXEC")
	})
	t.Run("UNWATCH", func(t *testing.T) {
		do(t, c, ok, "SET", "x", "30")
		do(t, c, ok, "WATCH", "x")
		do(t, c, ok, "SET", "x", "40")
		do(t, c, ok, "UNWATCH")
		do(t, c, ok, "MULTI")
		do(t, c, queued, "PING")
		do(t, c, "*1\r\n"+pong, "EXEC")
		do(t, c, ok, "UNWATCH")
	})
	t.Run("failed SET NX touches nothing", func(t *testing.T) {
		do(t, c, ok, "SET", "x", "30")
		do(t, c, ok, "WATCH", "x")
		do(t, other, "$-1\r\n", "SET", "x", "99", "NX")
		do(t, c, ok, "MULTI")
		do(t, c, queued, "GET", "x")
		do(t, c, "*1\r\n"+bulk("30"), "EXEC")
	})
	t.Run("DEL touches only what it deleted", func(t *testing.T) {
		do(t, c, ok, "SET", "x", "30")
		do(t, c, ok, "WATCH", "missing")
		do(t, other, ":1\r\n", "DEL", "x", "missing")
		do(t, c, ok, "MULTI")
		do(t, c, queued, "PING")
		do(t, c, "*1\r\n"+pong, "EXEC")
	})
	t.Run("creating a missing watched key", func(t *testing.T) {
		do(t, c, ":0\r\n", "DEL", "fresh")
		do(t, c, ok, "WATCH", "fresh")
		do(t, other, ok, "SET", "fresh", "1")
		do(t, c, ok, "MULTI")
		do(t, c, queued, "PING")
		do(t, c, nullArr, "EXEC")
	})
	t.Run("RESP3 null", func(t *testing.T) {
		c3 := e.TCP(t)
		c3.Do("HELLO", "3")
		do(t, c3, ok, "WATCH", "x")
		do(t, other, ok, "SET", "x", "1")
		do(t, c3, ok, "MULTI")
		do(t, c3, queued, "PING")
		do(t, c3, "_\r\n", "EXEC")
	})
}

func TestDiscardClearsWatch(t *testing.T) {
	e := spawn(t)
	c := e.TCP(t)
	// DISCARD should clear the WATCH dirty flag on the client.
	do(t, c, ok, "WATCH", "x")
	do(t, c, ok, "SET", "x", "10")
	do(t, c, ok, "MULTI")
	do(t, c, ok, "DISCARD")
	do(t, c, ok, "MULTI")
	do(t, c, queued, "INCR", "x")
	do(t, c, "*1\r\n:11\r\n", "EXEC")
	// DISCARD should UNWATCH all the keys.
	do(t, c, ok, "WATCH", "x")
	do(t, c, ok, "SET", "x", "10")
	do(t, c, ok, "MULTI")
	do(t, c, ok, "DISCARD")
	do(t, c, ok, "SET", "x", "10")
	do(t, c, ok, "MULTI")
	do(t, c, queued, "INCR", "x")
	do(t, c, "*1\r\n:11\r\n", "EXEC")
}

func TestWatchRemembersDB(t *testing.T) {
	e := spawn(t)
	c := e.TCP(t)
	do(t, c, ok, "SELECT", "5")
	do(t, c, ok, "SET", "x", "30")
	do(t, c, ok, "WATCH", "x")
	do(t, c, ok, "SELECT", "1")
	do(t, c, ok, "SET", "x", "10")
	do(t, c, ok, "SELECT", "5")
	do(t, c, ok, "MULTI")
	do(t, c, queued, "PING")
	do(t, c, "*1\r\n"+pong, "EXEC")
}

func TestWatchAndFlushDB(t *testing.T) {
	e := spawn(t)
	c := e.TCP(t)
	// FLUSHDB is able to touch the watched keys.
	do(t, c, ok, "SET", "x", "30")
	do(t, c, ok, "WATCH", "x")
	do(t, c, ok, "FLUSHDB")
	do(t, c, ok, "MULTI")
	do(t, c, queued, "PING")
	do(t, c, nullArr, "EXEC")
	// FLUSHDB does not touch non affected keys.
	do(t, c, ":0\r\n", "DEL", "x")
	do(t, c, ok, "WATCH", "x")
	do(t, c, ok, "FLUSHDB")
	do(t, c, ok, "MULTI")
	do(t, c, queued, "PING")
	do(t, c, "*1\r\n"+pong, "EXEC")
	// FLUSHDB of another db leaves watchers of db 0 alone.
	do(t, c, ok, "SET", "x", "30")
	do(t, c, ok, "WATCH", "x")
	do(t, c, ok, "SELECT", "3")
	do(t, c, ok, "FLUSHDB")
	do(t, c, ok, "SELECT", "0")
	do(t, c, ok, "MULTI")
	do(t, c, queued, "PING")
	do(t, c, "*1\r\n"+pong, "EXEC")
}

func TestWatchAndExpiry(t *testing.T) {
	e := spawn(t)
	c := e.TCP(t)
	t.Run("EXPIRE touches", func(t *testing.T) {
		do(t, c, ok, "SET", "x", "foo")
		do(t, c, ok, "WATCH", "x")
		do(t, c, ":1\r\n", "PEXPIRE", "x", "10000")
		do(t, c, ok, "MULTI")
		do(t, c, queued, "PING")
		do(t, c, nullArr, "EXEC")
	})
	t.Run("lazy expired watched key fails EXEC", func(t *testing.T) {
		do(t, c, ok, "SET", "key", "1", "PX", "100")
		do(t, c, ok, "WATCH", "key")
		e.Clock.Advance(101 * time.Millisecond)
		do(t, c, ok, "MULTI")
		do(t, c, queued, "INCR", "key")
		do(t, c, nullArr, "EXEC")
		do(t, c, "$-1\r\n", "GET", "key")
	})
	t.Run("stale watched key does not fail EXEC", func(t *testing.T) {
		do(t, c, ok, "SET", "x", "foo", "PX", "1")
		e.Clock.Advance(2 * time.Millisecond)
		do(t, c, ok, "WATCH", "x")
		do(t, c, ok, "MULTI")
		do(t, c, queued, "PING")
		do(t, c, "*1\r\n"+pong, "EXEC")
	})
	t.Run("deleting a stale watched key does not fail EXEC", func(t *testing.T) {
		do(t, c, ok, "SET", "x", "foo", "PX", "1")
		e.Clock.Advance(2 * time.Millisecond)
		do(t, c, ok, "WATCH", "x")
		do(t, c, ":0\r\n", "EXISTS", "x")
		do(t, c, ":0\r\n", "DEL", "x")
		do(t, c, ok, "MULTI")
		do(t, c, queued, "PING")
		do(t, c, "*1\r\n"+pong, "EXEC")
	})
	t.Run("FLUSHDB while watching stale keys does not fail EXEC", func(t *testing.T) {
		do(t, c, ok, "SET", "x", "foo", "PX", "1")
		e.Clock.Advance(2 * time.Millisecond)
		do(t, c, ok, "WATCH", "x")
		do(t, c, ok, "FLUSHDB")
		do(t, c, ok, "MULTI")
		do(t, c, queued, "PING")
		do(t, c, "*1\r\n"+pong, "EXEC")
	})
	t.Run("expiry through the wire clock", func(t *testing.T) {
		do(t, c, ok, "SET", "w", "1", "PX", "50")
		do(t, c, ok, "WATCH", "w")
		do(t, c, ok, "NIL.DEBUG", "CLOCK-ADVANCE", "60")
		do(t, c, ok, "MULTI")
		do(t, c, queued, "PING")
		do(t, c, nullArr, "EXEC")
	})
}

// TestWatchOptimisticIncrement runs the classic WATCH/GET/MULTI/SET/EXEC
// retry loop from several clients at once; every increment must land
// exactly once.
func TestWatchOptimisticIncrement(t *testing.T) {
	e := spawn(t)
	const workers, each = 8, 40
	ctx := context.Background()
	var wg sync.WaitGroup
	for w := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rc := e.Client(t, 2+w%2)
			for range each {
				for {
					err := rc.Watch(ctx, func(tx *redis.Tx) error {
						n, err := tx.Get(ctx, "counter").Int()
						if err != nil && !errors.Is(err, redis.Nil) {
							return err
						}
						_, err = tx.TxPipelined(ctx, func(p redis.Pipeliner) error {
							p.Set(ctx, "counter", n+1, 0)
							return nil
						})
						return err
					}, "counter")
					if err == nil {
						break
					}
					if !errors.Is(err, redis.TxFailedErr) {
						t.Errorf("worker %d: %v", w, err)
						return
					}
				}
			}
		}()
	}
	wg.Wait()
	got, err := e.Client(t, 3).Get(ctx, "counter").Int()
	if err != nil || got != workers*each {
		t.Fatalf("counter = %d, %v; want %d", got, err, workers*each)
	}
	if n := e.Server.Watch().Len(); n != 0 {
		t.Errorf("%d watch entries left after every client finished", n)
	}
}

func TestDisconnectForgetsWatches(t *testing.T) {
	e := spawn(t)
	c := e.TCP(t)
	do(t, c, ok, "WATCH", "a", "b", "a")
	if n := e.Server.Watch().Len(); n != 2 {
		t.Fatalf("watch entries %d, want 2", n)
	}
	c.Close()
	deadline := time.Now().Add(5 * time.Second)
	for e.Server.Watch().Len() != 0 {
		if time.Now().After(deadline) {
			t.Fatal("watch entries survived the disconnect")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestShutdownCommand(t *testing.T) {
	e := spawn(t)
	c := e.TCP(t)
	idle := e.TCP(t)
	do(t, idle, pong, "PING")
	c.Send(testutil.Encode("SHUTDOWN", "NOSAVE"))
	if rest := c.ExpectClosed(t); len(rest) != 0 {
		t.Errorf("SHUTDOWN replied %q", rest)
	}
	idle.ExpectClosed(t)
	select {
	case <-e.Server.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("server did not start shutting down")
	}
	e.Stop()
}
