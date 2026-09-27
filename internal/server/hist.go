package server

import (
	"math/bits"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nil68657/nildb/internal/command"
	"github.com/nil68657/nildb/internal/resp"
)

// Histogram geometry: values below histSub get one bucket each; above
// that, every power of two is split into histSub linear buckets, so a
// bucket is at most 1/16 (6.25%) of its lower bound wide.
const (
	histSubBits = 4
	histSub     = 1 << histSubBits
	histBuckets = (64 - histSubBits + 1) * histSub
)

// Histogram is a log-linear histogram of durations in nanoseconds. Record
// and the readers are lock-free and safe for concurrent use; a reader
// running alongside Record sees a count that is off by the records in
// flight.
type Histogram struct {
	counts [histBuckets]atomic.Uint64
	n      atomic.Uint64
	sum    atomic.Uint64 // nanoseconds
}

func histBucket(v uint64) int {
	if v < histSub {
		return int(v)
	}
	e := bits.Len64(v) - 1
	sub := (v >> (e - histSubBits)) & (histSub - 1)
	return (e-histSubBits+1)*histSub + int(sub)
}

// histLow is the smallest value that falls in bucket i.
func histLow(i int) uint64 {
	if i < histSub {
		return uint64(i)
	}
	e := i/histSub + histSubBits - 1
	sub := uint64(i % histSub)
	return (histSub + sub) << (e - histSubBits)
}

// Record adds one observation; negative durations count as zero.
func (h *Histogram) Record(d time.Duration) {
	v := uint64(0)
	if d > 0 {
		v = uint64(d)
	}
	h.counts[histBucket(v)].Add(1)
	h.n.Add(1)
	h.sum.Add(v)
}

// Count is the number of observations.
func (h *Histogram) Count() uint64 { return h.n.Load() }

// Sum is the total of all observations.
func (h *Histogram) Sum() time.Duration { return time.Duration(h.sum.Load()) }

// Quantile returns the value at quantile q (0 < q <= 1): the midpoint of
// the bucket holding the ceil(q*n)-th smallest observation, or 0 when
// nothing was recorded.
func (h *Histogram) Quantile(q float64) time.Duration {
	n := h.n.Load()
	if n == 0 {
		return 0
	}
	target := uint64(q * float64(n))
	if float64(target) < q*float64(n) {
		target++
	}
	target = max(target, 1)
	var cum uint64
	for i := range histBuckets {
		cum += h.counts[i].Load()
		if cum >= target {
			lo := histLow(i)
			if i+1 >= histBuckets {
				return time.Duration(lo)
			}
			width := histLow(i+1) - lo
			return time.Duration(lo + (width-1)/2)
		}
	}
	return time.Duration(histLow(histBuckets - 1))
}

// Reset clears the histogram.
func (h *Histogram) Reset() {
	for i := range h.counts {
		h.counts[i].Store(0)
	}
	h.n.Store(0)
	h.sum.Store(0)
}

// cmdStats is one command's line in INFO commandstats.
type cmdStats struct {
	calls    atomic.Uint64
	rejected atomic.Uint64
	failed   atomic.Uint64
	hist     Histogram
}

// maxErrorCodes caps the distinct codes errorstats tracks, as Redis does.
const maxErrorCodes = 128

// opsSamples is the number of 100 ms samples averaged into
// instantaneous_ops_per_sec (Redis's STATS_METRIC_SAMPLES).
const opsSamples = 16

// stats holds the server-wide counters behind INFO.
type stats struct {
	cmds sync.Map // *command.Spec -> *cmdStats

	commands   atomic.Uint64 // total_commands_processed
	connsTotal atomic.Uint64 // total_connections_received
	errors     atomic.Uint64 // total_error_replies
	netIn      atomic.Uint64
	netOut     atomic.Uint64

	errMu    sync.Mutex
	errCodes map[string]uint64

	opsMu      sync.Mutex
	opsRing    [opsSamples]uint64
	opsIdx     int
	opsLast    uint64
	opsLastAt  time.Time
	opsSampled int
}

func newStats() *stats {
	return &stats{errCodes: make(map[string]uint64)}
}

// cmd returns the counters of spec, creating them on first use.
func (st *stats) cmd(spec *command.Spec) *cmdStats {
	if v, ok := st.cmds.Load(spec); ok {
		return v.(*cmdStats)
	}
	v, _ := st.cmds.LoadOrStore(spec, new(cmdStats))
	return v.(*cmdStats)
}

// record counts one executed command.
func (st *stats) record(spec *command.Spec, d time.Duration, reply resp.Reply) {
	cs := st.cmd(spec)
	cs.calls.Add(1)
	cs.hist.Record(d)
	if st.noteErr(reply) {
		cs.failed.Add(1)
	}
}

// reject counts a command refused before it ran (arity, NOAUTH, MULTI).
func (st *stats) reject(spec *command.Spec, reply resp.Reply) {
	if spec != nil {
		st.cmd(spec).rejected.Add(1)
	}
	st.noteErr(reply)
}

// noteErr counts reply in errorstats when it is an error and reports
// whether it was.
func (st *stats) noteErr(reply resp.Reply) bool {
	text, ok := resp.ErrorText(reply)
	if !ok {
		return false
	}
	st.errors.Add(1)
	code, _, _ := strings.Cut(text, " ")
	st.errMu.Lock()
	if _, seen := st.errCodes[code]; seen || len(st.errCodes) < maxErrorCodes {
		st.errCodes[code]++
	}
	st.errMu.Unlock()
	return true
}

// reset is CONFIG RESETSTAT.
func (st *stats) reset() {
	st.cmds.Range(func(_, v any) bool {
		cs := v.(*cmdStats)
		cs.calls.Store(0)
		cs.rejected.Store(0)
		cs.failed.Store(0)
		cs.hist.Reset()
		return true
	})
	st.commands.Store(0)
	st.connsTotal.Store(0)
	st.errors.Store(0)
	st.netIn.Store(0)
	st.netOut.Store(0)
	st.errMu.Lock()
	st.errCodes = make(map[string]uint64)
	st.errMu.Unlock()
}

// sampleOps records the command rate every 100 ms until stop closes.
func (st *stats) sampleOps(stop <-chan struct{}) {
	t := time.NewTicker(100 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case now := <-t.C:
			st.sample(now)
		}
	}
}

func (st *stats) sample(now time.Time) {
	total := st.commands.Load()
	st.opsMu.Lock()
	defer st.opsMu.Unlock()
	if !st.opsLastAt.IsZero() {
		ms := now.Sub(st.opsLastAt).Milliseconds()
		if ms > 0 && total >= st.opsLast {
			st.opsRing[st.opsIdx] = (total - st.opsLast) * 1000 / uint64(ms)
			st.opsIdx = (st.opsIdx + 1) % opsSamples
			st.opsSampled = min(st.opsSampled+1, opsSamples)
		}
	}
	st.opsLast, st.opsLastAt = total, now
}

// opsPerSec is instantaneous_ops_per_sec: the mean of the last samples.
func (st *stats) opsPerSec() uint64 {
	st.opsMu.Lock()
	defer st.opsMu.Unlock()
	if st.opsSampled == 0 {
		return 0
	}
	var sum uint64
	for _, v := range st.opsRing {
		sum += v
	}
	return sum / uint64(st.opsSampled)
}
