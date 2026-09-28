// Package query is NilDB's document query engine: the planner that picks
// an index or a rowscan for a filter (architecture.md section 7, with
// scope-v1.md's cut to two sources, "index" and "rowscan"), pull-based
// operators over any store.Reader (live state, a command snapshot, a
// lease, or the EXEC transaction), the aggregation pipeline, the cursor
// table, and the memory and throughput limits analytical runs obey.
//
// A caller plans once and runs the plan:
//
//	p, err := q.PlanFind(coll, query.Find{Filter: f, Sort: s, Limit: 10}, opts)
//	run, err := p.Start()
//	docs, done, err := run.Next(101, 16<<20)
//
// and hands an unfinished run to Cursors.Open. Runs hold no RocksDB
// iterator between commands: a cursor's run is suspended after each batch
// and reopens its iterators, under the cursor's lease, on the next read.
package query

import (
	"time"

	"github.com/nil68657/nildb/internal/catalog"
	"github.com/nil68657/nildb/internal/config"
	"github.com/nil68657/nildb/internal/resp"
	"github.com/nil68657/nildb/internal/store"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// Options configure one plan and its run.
type Options struct {
	Mode Mode
	// Reader is what the run reads through; nil means the live store.
	Reader store.Reader
	// MaxMem bounds the bytes blocking stages hold (0 = unlimited).
	MaxMem int64
	// MaxRows bounds the documents the run reads (0 = unlimited).
	MaxRows int64
	// Timeout bounds each batch of the run (0 = none): analytical
	// iterators get it as their RocksDB deadline, and in-memory stages
	// check it as they work.
	Timeout time.Duration
	// Bucket throttles reads (nil = unthrottled).
	Bucket *Bucket
	// GeoMaxCells is the RegionCoverer budget of 2dsphere queries (0 =
	// geo.DefaultQueryMaxCells).
	GeoMaxCells int
	// Stats receives the run's row and byte counts (nil = not collected).
	Stats *Stats
}

// Find is one find: a filter plus the options of DOC.FIND.
type Find struct {
	Filter  bson.Raw // nil or {} matches everything
	Sort    bson.Raw // nil or {} for none
	Project bson.Raw // nil or {} for the whole document
	Skip    int64
	Limit   int64 // 0 = no limit
	// Hint names an index to use ("_id_" for the _id index); HintKey is
	// the same by key pattern, {"a": 1}.
	Hint    string
	HintKey bson.Raw
}

// Engine plans and runs queries over one store. It is safe for concurrent
// use.
type Engine struct {
	st      *store.Store
	Cursors *Cursors
}

// New returns the engine for st. cfg supplies the cursor idle timeout
// (nildb.lease-ttl, read at every use); nil uses the store's lease TTL.
func New(st *store.Store, cfg *config.Config) *Engine {
	idle := func() time.Duration { return st.Config().LeaseTTL }
	if cfg != nil {
		idle = func() time.Duration { return cfg.Knobs().LeaseTTL }
	}
	return &Engine{st: st, Cursors: newCursors(st, idle)}
}

// Store returns the store the engine reads.
func (e *Engine) Store() *store.Store { return e.st }

// Close closes every open cursor and releases their leases. Call it after
// the server stops serving and before the store closes.
func (e *Engine) Close() { e.Cursors.Close() }

// Plan is a planned query. The exported fields are what EXPLAIN shows.
type Plan struct {
	Source        string   // "index" or "rowscan"
	Index         string   // the index an "index" plan reads ("_id_" for _id bounds)
	Bounds        []string // one line per bounded index field
	Stages        []string // the operators after the source, in order
	EstimatedRows int64    // documents the source is expected to read

	e     *Engine
	opts  Options
	build func(x *Exec) (Operator, error)
}

// Explain returns the plan as NIL.EXPLAIN and DOC.* EXPLAIN reply it: a
// map {plan, index, bounds, stages, estimatedRows} (a flat array in RESP2).
func (p *Plan) Explain() resp.Reply {
	index := resp.Null()
	if p.Index != "" {
		index = resp.Str(p.Index)
	}
	bounds := make([]resp.Reply, len(p.Bounds))
	for i, b := range p.Bounds {
		bounds[i] = resp.Str(b)
	}
	stages := make([]resp.Reply, len(p.Stages))
	for i, s := range p.Stages {
		stages[i] = resp.Str(s)
	}
	return resp.Map(
		resp.Str("plan"), resp.Str(p.Source),
		resp.Str("index"), index,
		resp.Str("bounds"), resp.Array(bounds...),
		resp.Str("stages"), resp.Array(stages...),
		resp.Str("estimatedRows"), resp.Int(p.EstimatedRows),
	)
}

// Options returns the options the plan was made with.
func (p *Plan) Options() Options { return p.opts }

// Start builds the plan's operators for one run.
func (p *Plan) Start() (*Run, error) {
	x := newExec(p.opts, p.e.st)
	root, err := p.build(x)
	if err != nil {
		return nil, err
	}
	return &Run{x: x, root: root}, nil
}

// Run is one execution of a plan.
type Run struct {
	x       *Exec
	root    Operator
	pending *Row // read ahead to learn whether more rows follow a batch
	done    bool
	closed  bool
}

// Next returns up to n documents (n < 0 for every remaining one), stopping
// early once the batch reaches maxBytes (0 = no byte limit) but always
// returning at least one document when any is left. done reports that the
// run has no more documents; Next reads one document ahead to know it.
func (r *Run) Next(n, maxBytes int) (docs []bson.Raw, done bool, err error) {
	if r.done || r.closed {
		return nil, true, nil
	}
	r.x.startBatch()
	defer r.x.flush()
	size := 0
	for n < 0 || len(docs) < n {
		row, ok, err := r.pull()
		if err != nil {
			return nil, false, err
		}
		if !ok {
			r.done = true
			return docs, true, nil
		}
		if maxBytes > 0 && len(docs) > 0 && size+len(row.Doc) > maxBytes {
			r.pending = &row
			return docs, false, nil
		}
		docs = append(docs, row.Doc)
		size += len(row.Doc)
	}
	row, ok, err := r.pull()
	if err != nil {
		return nil, false, err
	}
	if !ok {
		r.done = true
		return docs, true, nil
	}
	r.pending = &row
	return docs, false, nil
}

func (r *Run) pull() (Row, bool, error) {
	if r.pending != nil {
		row := *r.pending
		r.pending = nil
		return row, true, nil
	}
	return r.root.Next()
}

// Done reports whether the run has returned its last document.
func (r *Run) Done() bool { return r.done }

// SetReader switches what the run reads through, for a cursor that moves
// from live state to its lease after the first batch. Call it only while
// the run is suspended.
func (r *Run) SetReader(rd store.Reader) { r.x.r = rd }

// Rows returns the number of documents the run has read.
func (r *Run) Rows() int64 { return r.x.rows }

// Bytes returns the key and value bytes the run has read.
func (r *Run) Bytes() int64 { return r.x.bytes }

// Suspend closes the run's iterators until the next Next call.
func (r *Run) Suspend() {
	if !r.closed {
		r.root.Suspend()
	}
}

// Close releases the run. It is idempotent.
func (r *Run) Close() {
	if !r.closed {
		r.closed = true
		r.root.Close()
		r.x.flush()
	}
}

// emptyOp is the source of a plan over a collection that does not exist or
// bounds that hold no key.
type emptyOp struct{}

func (emptyOp) Next() (Row, bool, error) { return Row{}, false, nil }
func (emptyOp) Suspend()                 {}
func (emptyOp) Close()                   {}

// collID returns coll's id, 0 for a missing collection.
func collID(coll *catalog.Collection) uint32 {
	if coll == nil {
		return 0
	}
	return coll.ID
}
