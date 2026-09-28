package analytics

import (
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nil68657/nildb/internal/catalog"
	"github.com/nil68657/nildb/internal/command"
	"github.com/nil68657/nildb/internal/config"
	"github.com/nil68657/nildb/internal/doc"
	"github.com/nil68657/nildb/internal/query"
	"github.com/nil68657/nildb/internal/resp"
	"github.com/nil68657/nildb/internal/store"
	"go.mongodb.org/mongo-driver/v2/bson"
)

const (
	// defaultBatch is MongoDB's first-batch size for aggregate.
	defaultBatch = 101
	// maxBatchBytes caps one batch at MongoDB's 16 MiB message size.
	maxBatchBytes = 16 << 20
)

var errNotArray = resp.Err("ERR the pipeline must be a JSON array of stage documents")

// errReply turns an error into its reply. doc and query errors carry
// their own "ERR " prefix; store errors take query's texts.
func errReply(err error) resp.Reply {
	switch {
	case errors.Is(err, store.ErrLeaseExpired):
		err = query.ErrLeaseExpired
	case errors.Is(err, store.ErrTooManySnapshots):
		err = query.ErrTooManySnapshots
	case errors.Is(err, store.ErrClosed):
		return resp.Err("ERR server is shutting down")
	}
	var de *doc.Error
	if errors.As(err, &de) {
		return resp.Err(err.Error())
	}
	return resp.Err("ERR " + err.Error())
}

func resp3(c *command.Ctx) bool { return c.Conn != nil && c.Conn.Proto() == 3 }

func connName(c *command.Ctx) string {
	if c.Conn == nil {
		return "local"
	}
	return "conn " + strconv.FormatUint(c.Conn.ID(), 10)
}

// parseArray parses an Extended JSON array, such as a pipeline.
func parseArray(arg []byte) (bson.Raw, resp.Reply) {
	wrapped := make([]byte, 0, len(arg)+6)
	wrapped = append(append(append(wrapped, `{"p":`...), arg...), '}')
	d, err := doc.ParseEJSON(wrapped)
	if err != nil {
		return nil, errReply(err)
	}
	v, err := d.LookupErr("p")
	if err != nil || v.Type != bson.TypeArray {
		return nil, errNotArray
	}
	return v.Value, nil
}

// leadingFilter splits an optional Extended JSON filter off the front of
// args; option keywords never start with '{'.
func leadingFilter(args [][]byte) (bson.Raw, [][]byte, resp.Reply) {
	if len(args) == 0 || !strings.HasPrefix(strings.TrimSpace(string(args[0])), "{") {
		return nil, args, nil
	}
	f, err := doc.ParseEJSON(args[0])
	if err != nil {
		return nil, nil, errReply(err)
	}
	return f, args[1:], nil
}

// opts are the trailing options of a NIL command.
type opts struct {
	batch   int
	maxMem  int64
	timeout time.Duration
	at      uint64
	hasAt   bool
	raw     bool
	explain bool
}

// parseOpts reads the options named in allowed, in any order.
func parseOpts(args [][]byte, allowed ...string) (opts, resp.Reply) {
	o := opts{batch: defaultBatch}
	for i := 0; i < len(args); i++ {
		kw := strings.ToUpper(string(args[i]))
		if !slices.Contains(allowed, kw) {
			return o, resp.ErrSyntax
		}
		if kw == "EXPLAIN" {
			o.explain = true
			continue
		}
		if i+1 >= len(args) {
			return o, resp.ErrSyntax
		}
		i++
		n, isInt := resp.ParseInt(args[i])
		switch kw {
		case "BATCH":
			if !isInt || n <= 0 {
				return o, resp.Err("ERR BATCH must be a positive integer")
			}
			o.batch = int(min(n, 1<<31))
		case "MAXMEM":
			if !isInt || n <= 0 {
				return o, resp.Err("ERR MAXMEM must be a positive number of bytes")
			}
			o.maxMem = n
		case "TIMEOUT":
			if !isInt || n <= 0 {
				return o, resp.Err("ERR TIMEOUT must be a positive number of milliseconds")
			}
			o.timeout = time.Duration(n) * time.Millisecond
		case "AT":
			if !isInt || n < 0 {
				return o, resp.ErrNotInteger
			}
			o.at, o.hasAt = uint64(n), true
		case "FORMAT":
			switch strings.ToUpper(string(args[i])) {
			case "EJSON":
				o.raw = false
			case "BSON":
				o.raw = true
			default:
				return o, resp.ErrSyntax
			}
		}
	}
	return o, nil
}

func (a *Analytics) limit() int { return a.cfg.Knobs().AnalyticsMaxConcurrent }

// acquire takes a semaphore slot. The returned release is idempotent.
func (a *Analytics) acquire(k config.Knobs) (func(), error) {
	waited, ok := a.sem.acquire(a.limit, k.AnalyticsQueueTimeout)
	if waited {
		a.waits.Add(1)
	}
	if !ok {
		a.rejections.Add(1)
		return nil, &doc.Error{Code: doc.CodeBadValue, Msg: fmt.Sprintf(
			"too many analytical queries running (nildb.analytics-max-concurrent is %d)", k.AnalyticsMaxConcurrent)}
	}
	var once sync.Once
	return func() { once.Do(func() { a.sem.release(a.limit) }) }, nil
}

// session is one admitted analytical command.
type session struct {
	a       *Analytics
	release func()
	lease   *store.Snapshot
	own     bool // the command took the lease and releases it
	bucket  *query.Bucket
	opts    query.Options
}

// begin admits an analytical command: it takes a semaphore slot, then the
// lease o.at names or a fresh one of nildb.lease-ttl, and builds the
// Analytic query options with a byte bucket of its own.
func (a *Analytics) begin(c *command.Ctx, name string, o opts) (*session, resp.Reply) {
	k := a.cfg.Knobs()
	release, err := a.acquire(k)
	if err != nil {
		return nil, errReply(err)
	}
	s := &session{a: a, release: release}
	if o.hasAt {
		lease, ok := a.st.Lookup(o.at)
		if !ok {
			release()
			return nil, errReply(query.ErrLeaseExpired)
		}
		s.lease = lease
	} else {
		lease, err := a.st.Lease(name+" "+connName(c), k.LeaseTTL)
		if err != nil {
			release()
			return nil, errReply(err)
		}
		s.lease, s.own = lease, true
	}
	maxMem := o.maxMem
	if maxMem <= 0 {
		maxMem = k.AnalyticsGroupMem
	}
	s.bucket = query.NewBucket(k.AnalyticsReadBPS)
	s.opts = query.Options{
		Mode:        query.Analytic,
		Reader:      a.st.At(s.lease),
		MaxMem:      maxMem,
		MaxRows:     k.AnalyticsMaxRows,
		Timeout:     o.timeout,
		Bucket:      s.bucket,
		GeoMaxCells: k.GeoQueryMaxCells,
		Stats:       &a.stats,
	}
	a.queries.Add(1)
	return s, nil
}

// end frees the slot and, unless keepLease hands it to a cursor, the
// lease the command took.
func (s *session) end(keepLease bool) {
	s.a.throttleNS.Add(int64(s.bucket.Slept()))
	if s.own && !keepLease {
		s.a.st.Release(s.lease)
	}
	s.release()
}

// gate is the cursor Gate of NIL.AGGREGATE: each DOC.CURSOR READ takes a
// semaphore slot, picks up the current nildb.analytics-read-bps, and adds
// its throttle sleep to INFO.
func (a *Analytics) gate(b *query.Bucket) func() (func(), error) {
	return func() (func(), error) {
		k := a.cfg.Knobs()
		release, err := a.acquire(k)
		if err != nil {
			return nil, err
		}
		b.SetRate(k.AnalyticsReadBPS)
		slept := b.Slept()
		return func() {
			a.throttleNS.Add(int64(b.Slept() - slept))
			release()
		}, nil
	}
}

// aggregate is NIL.AGGREGATE ns pipeline [BATCH n] [MAXMEM bytes] [TIMEOUT
// ms] [AT lease] [FORMAT EJSON|BSON] [EXPLAIN]. MAXMEM defaults to
// nildb.analytics-group-mem; TIMEOUT bounds each batch. A result larger
// than BATCH documents returns a cursor id for DOC.CURSOR READ that keeps
// reading the same lease.
func (a *Analytics) aggregate(c *command.Ctx, args [][]byte) resp.Reply {
	ns := string(args[1])
	pipeline, rep := parseArray(args[2])
	if rep != nil {
		return rep
	}
	o, rep := parseOpts(args[3:], "BATCH", "MAXMEM", "TIMEOUT", "AT", "FORMAT", "EXPLAIN")
	if rep != nil {
		return rep
	}
	coll, _ := a.cat.Resolve(ns)
	if o.explain {
		return a.explainPlan(coll, pipeline)
	}
	s, rep := a.begin(c, "NIL.AGGREGATE", o)
	if rep != nil {
		return rep
	}
	docs, id, err := a.firstBatch(s, coll, ns, pipeline, o)
	if err != nil {
		return errReply(err)
	}
	return query.CursorReply(resp3(c), id, ns, "firstBatch", docs, o.raw)
}

// firstBatch runs the pipeline for one batch and ends the session. When
// documents remain, a cursor takes over the run and the lease.
func (a *Analytics) firstBatch(s *session, coll *catalog.Collection, ns string, pipeline bson.Raw, o opts) ([]bson.Raw, uint64, error) {
	plan, err := a.q.PlanPipeline(coll, pipeline, s.opts)
	if err != nil {
		s.end(false)
		return nil, 0, err
	}
	run, err := plan.Start()
	if err != nil {
		s.end(false)
		return nil, 0, err
	}
	docs, done, err := run.Next(o.batch, maxBatchBytes)
	if err != nil || done {
		run.Close()
		s.end(false)
		return docs, 0, err
	}
	cur, err := a.q.Cursors.Open(run, query.CursorOptions{
		NS: ns, Kind: "nil.aggregate", Batch: o.batch,
		Lease: s.lease, Own: s.own, Gate: a.gate(s.bucket), RawBSON: o.raw,
	})
	if err != nil {
		run.Close()
		s.end(false)
		return nil, 0, err
	}
	s.end(true)
	return docs, cur.ID, nil
}

// count is NIL.COUNT ns [filter] [AT lease] [TIMEOUT ms]. Without a filter
// it reads the collection's document counter at the lease.
func (a *Analytics) count(c *command.Ctx, args [][]byte) resp.Reply {
	filter, rest, rep := leadingFilter(args[2:])
	if rep != nil {
		return rep
	}
	o, rep := parseOpts(rest, "AT", "TIMEOUT")
	if rep != nil {
		return rep
	}
	coll, _ := a.cat.Resolve(string(args[1]))
	s, rep := a.begin(c, "NIL.COUNT", o)
	if rep != nil {
		return rep
	}
	defer s.end(false)
	n, err := a.q.Count(coll, filter, s.opts)
	if err != nil {
		return errReply(err)
	}
	return resp.Int(n)
}

// distinct is NIL.DISTINCT ns field [filter] [AT lease] [TIMEOUT ms]
// [MAXMEM bytes]. Values come back in BSON order as Extended JSON.
func (a *Analytics) distinct(c *command.Ctx, args [][]byte) resp.Reply {
	filter, rest, rep := leadingFilter(args[3:])
	if rep != nil {
		return rep
	}
	o, rep := parseOpts(rest, "AT", "TIMEOUT", "MAXMEM")
	if rep != nil {
		return rep
	}
	coll, _ := a.cat.Resolve(string(args[1]))
	s, rep := a.begin(c, "NIL.DISTINCT", o)
	if rep != nil {
		return rep
	}
	defer s.end(false)
	vals, err := a.q.Distinct(coll, string(args[2]), filter, s.opts)
	if err != nil {
		return errReply(err)
	}
	items := make([]resp.Reply, len(vals))
	for i, v := range vals {
		items[i] = resp.Bulk(doc.FormatEJSONValue(v))
	}
	return resp.Array(items...)
}

// explain is NIL.EXPLAIN ns pipeline: the plan NIL.AGGREGATE would run,
// without running it or taking a semaphore slot.
func (a *Analytics) explain(c *command.Ctx, args [][]byte) resp.Reply {
	pipeline, rep := parseArray(args[2])
	if rep != nil {
		return rep
	}
	coll, _ := a.cat.Resolve(string(args[1]))
	return a.explainPlan(coll, pipeline)
}

func (a *Analytics) explainPlan(coll *catalog.Collection, pipeline bson.Raw) resp.Reply {
	k := a.cfg.Knobs()
	plan, err := a.q.PlanPipeline(coll, pipeline, query.Options{
		Mode: query.Analytic, Reader: a.st, MaxMem: k.AnalyticsGroupMem,
		MaxRows: k.AnalyticsMaxRows, GeoMaxCells: k.GeoQueryMaxCells,
	})
	if err != nil {
		return errReply(err)
	}
	return plan.Explain()
}
