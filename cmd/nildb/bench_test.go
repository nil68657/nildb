package main

import (
	"context"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"

	goredis "github.com/redis/go-redis/v9"
)

// serverOp is one benchmark operation; i numbers it within its run.
type serverOp func(ctx context.Context, c *goredis.Client, run string, i int64) error

type serverWorkload struct {
	name  string
	setup func(ctx context.Context, c *goredis.Client, run string) error
	op    serverOp
}

// preloaded is how many keys, fields and documents the read workloads
// find in place.
const preloaded = 10_000

// runSeq numbers sub-benchmark invocations. -count repeats a sub-benchmark
// on the same server, and a repeat that reused the keys of the one before
// would measure overwrites and no-op ZADDs instead of inserts.
var runSeq atomic.Int64

var value100 = strings.Repeat("v", 100)

var serverWorkloads = []serverWorkload{
	{name: "SET", op: func(ctx context.Context, c *goredis.Client, run string, i int64) error {
		return c.Set(ctx, run+":k:"+strconv.FormatInt(i, 10), value100, 0).Err()
	}},
	{name: "GET", setup: func(ctx context.Context, c *goredis.Client, run string) error {
		p := c.Pipeline()
		for i := range preloaded {
			p.Set(ctx, run+":k:"+strconv.Itoa(i), value100, 0)
		}
		_, err := p.Exec(ctx)
		return err
	}, op: func(ctx context.Context, c *goredis.Client, run string, i int64) error {
		return c.Get(ctx, run+":k:"+strconv.FormatInt(i%preloaded, 10)).Err()
	}},
	{name: "INCR", op: func(ctx context.Context, c *goredis.Client, run string, i int64) error {
		return c.Incr(ctx, run+":n:"+strconv.FormatInt(i%1000, 10)).Err()
	}},
	{name: "HSET", op: func(ctx context.Context, c *goredis.Client, run string, i int64) error {
		return c.HSet(ctx, run+":h:"+strconv.FormatInt(i%1000, 10), "f"+strconv.FormatInt(i, 10), value100).Err()
	}},
	{name: "ZADD", op: func(ctx context.Context, c *goredis.Client, run string, i int64) error {
		return c.ZAdd(ctx, run+":z:"+strconv.FormatInt(i%100, 10), goredis.Z{Score: float64(i), Member: "m" + strconv.FormatInt(i, 10)}).Err()
	}},
	// Half DOC.INSERT of a new document, half DOC.FIND of a preloaded one
	// by _id.
	{name: "DOCMIX", setup: func(ctx context.Context, c *goredis.Client, run string) error {
		for i := 0; i < preloaded; i += 500 {
			args := []any{"DOC.INSERT", "bench." + run}
			for j := i; j < i+500; j++ {
				args = append(args, benchDoc(j))
			}
			if err := c.Do(ctx, args...).Err(); err != nil {
				return err
			}
		}
		return nil
	}, op: func(ctx context.Context, c *goredis.Client, run string, i int64) error {
		ns := "bench." + run
		if i%2 == 0 {
			return c.Do(ctx, "DOC.INSERT", ns, benchDoc(int(preloaded+i))).Err()
		}
		return c.Do(ctx, "DOC.FIND", ns, fmt.Sprintf(`{"_id": %d}`, i%preloaded)).Err()
	}},
}

func benchDoc(id int) string {
	return fmt.Sprintf(`{"_id": %d, "a": %d, "s": "%s"}`, id, id%100, value100[:60])
}

// BenchmarkServer measures the server end to end: it builds nildb, starts
// it with the test engine (store.TestEngine, set by make bench-engines) and
// drives it over TCP with go-redis v9 clients. Each sub-benchmark spreads
// b.N operations over conc goroutines sharing one client whose pool holds
// conc connections, and reports operations per second.
func BenchmarkServer(b *testing.B) {
	if testing.Short() {
		b.Skip("builds and runs the nildb binary")
	}
	bin := nildbBinary(b)
	addr := freeAddr(b)
	p := start(b, bin, addr, "--dir", filepath.Join(b.TempDir(), "data"))
	defer p.stop(b, syscall.SIGTERM)
	ctx := context.Background()
	for _, w := range serverWorkloads {
		for _, conc := range []int{1, 8, 32} {
			b.Run(fmt.Sprintf("%s/c%d", w.name, conc), func(b *testing.B) {
				c := goredis.NewClient(&goredis.Options{Addr: addr, Protocol: 2, PoolSize: conc, MinIdleConns: conc, MaxRetries: -1})
				defer c.Close()
				run := fmt.Sprintf("%s%d_%d_%d", strings.ToLower(w.name), conc, b.N, runSeq.Add(1))
				if w.setup != nil {
					if err := w.setup(ctx, c, run); err != nil {
						b.Fatal(err)
					}
				}
				var next atomic.Int64
				var failed atomic.Pointer[error]
				var wg sync.WaitGroup
				b.ResetTimer()
				for range conc {
					wg.Add(1)
					go func() {
						defer wg.Done()
						for {
							i := next.Add(1) - 1
							if i >= int64(b.N) || failed.Load() != nil {
								return
							}
							if err := w.op(ctx, c, run, i); err != nil {
								failed.CompareAndSwap(nil, &err)
								return
							}
						}
					}()
				}
				wg.Wait()
				b.StopTimer()
				if err := failed.Load(); err != nil {
					b.Fatal(*err)
				}
				b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "ops/s")
			})
		}
	}
}
