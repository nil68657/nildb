package layout

import (
	"sync/atomic"
	"time"
)

// VersionCounterBits is the width of the counter in the low bits of a
// version: version = unix_micros << 11 | counter.
const VersionCounterBits = 11

// maxVersionMicros keeps unix_micros << 11 inside a uint64.
const maxVersionMicros = 1<<(64-VersionCounterBits) - 1

// VersionMicros returns the Unix microsecond timestamp in a version's high
// bits. The sub-key compaction filter skips its existence lookup for
// versions younger than 60 s.
func VersionMicros(v uint64) int64 { return int64(v >> VersionCounterBits) }

// VersionGen issues collection versions that are unique across all
// databases and across restarts, and strictly increasing within a process.
// A version is unix_micros << 11 with the low 11 bits counting the versions
// issued in the same microsecond; when the clock stands still or steps back,
// the generator returns last+1 instead, so the counter can carry into the
// microsecond field and a version may run slightly ahead of the clock. It is
// safe for concurrent use.
type VersionGen struct {
	last atomic.Uint64
}

// NewVersionGen returns a generator whose every version is greater than
// seed. Pass the last version a previous run issued (catalog key
// SeqKey(SeqVersion)), or 0 for a new store.
func NewVersionGen(seed uint64) *VersionGen {
	g := &VersionGen{}
	g.last.Store(seed)
	return g
}

// Next returns max(now in micros << 11, Last()+1) and records it as Last.
func (g *VersionGen) Next(now time.Time) uint64 {
	micros := now.UnixMicro()
	if micros < 0 {
		micros = 0
	}
	if micros > maxVersionMicros {
		micros = maxVersionMicros
	}
	candidate := uint64(micros) << VersionCounterBits
	for {
		last := g.last.Load()
		next := candidate
		if next <= last {
			next = last + 1
		}
		if g.last.CompareAndSwap(last, next) {
			return next
		}
	}
}

// Last returns the most recent version issued, or the seed if none has been.
// Store it at shutdown so the next NewVersionGen starts above it.
func (g *VersionGen) Last() uint64 { return g.last.Load() }
