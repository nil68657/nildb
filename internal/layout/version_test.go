package layout

import (
	"math/rand/v2"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func micros(us int64) time.Time { return time.UnixMicro(us) }

func TestVersionGenClockStepsBack(t *testing.T) {
	g := NewVersionGen(0)
	steps := []struct {
		now  int64
		want uint64
	}{
		{1_000_000, 1_000_000 << 11},
		{1_000_000, 1_000_000<<11 + 1}, // same microsecond: counter
		{500_000, 1_000_000<<11 + 2},   // clock stepped back: last+1
		{999_999, 1_000_000<<11 + 3},
		{2_000_000, 2_000_000 << 11}, // clock caught up: counter resets
		{1_999_999, 2_000_000<<11 + 1},
	}
	for i, s := range steps {
		got := g.Next(micros(s.now))
		if got != s.want {
			t.Fatalf("step %d: Next(%d us) = %d, want %d", i, s.now, got, s.want)
		}
		if g.Last() != got {
			t.Fatalf("step %d: Last() = %d, want %d", i, g.Last(), got)
		}
	}
	if VersionMicros(g.Last()) != 2_000_000 {
		t.Errorf("VersionMicros = %d", VersionMicros(g.Last()))
	}
}

// TestVersionGenCounterCarries checks that more than 2048 versions in one
// microsecond stay unique by carrying into the microsecond field.
func TestVersionGenCounterCarries(t *testing.T) {
	g := NewVersionGen(0)
	now := micros(5_000_000)
	var prev uint64
	for i := 0; i < 3*(1<<VersionCounterBits); i++ {
		v := g.Next(now)
		if v <= prev {
			t.Fatalf("version %d not above %d", v, prev)
		}
		prev = v
	}
	if got := VersionMicros(prev); got != 5_000_002 {
		t.Errorf("after 3*2048 versions in one microsecond VersionMicros = %d, want 5000002", got)
	}
	// The next microsecond on the clock is now behind the generator.
	if v := g.Next(micros(5_000_001)); v != prev+1 {
		t.Errorf("Next after carry = %d, want %d", v, prev+1)
	}
}

func TestVersionGenSeed(t *testing.T) {
	seed := uint64(9_000_000)<<11 + 7 // a previous run ran ahead of today's clock
	g := NewVersionGen(seed)
	if g.Last() != seed {
		t.Fatalf("Last() before Next = %d, want the seed", g.Last())
	}
	if v := g.Next(micros(1_000_000)); v != seed+1 {
		t.Errorf("Next below seed = %d, want %d", v, seed+1)
	}
	g = NewVersionGen(42)
	if v := g.Next(micros(1_000_000)); v != 1_000_000<<11 {
		t.Errorf("Next above seed = %d", v)
	}
	g = NewVersionGen(0)
	if v := g.Next(time.Unix(-10, 0)); v != 1 {
		t.Errorf("Next before 1970 = %d, want 1", v)
	}
}

// TestVersionGenConcurrent runs 1,000 goroutines against one generator
// whose clock other goroutines keep stepping backwards and forwards. Run it
// with -race.
func TestVersionGenConcurrent(t *testing.T) {
	const goroutines, each = 1000, 200
	g := NewVersionGen(0)
	var clock atomic.Int64
	clock.Store(1_700_000_000_000_000)
	results := make([][]uint64, goroutines)
	var wg sync.WaitGroup
	for w := 0; w < goroutines; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			r := rand.New(rand.NewPCG(uint64(w), 1))
			out := make([]uint64, 0, each)
			for i := 0; i < each; i++ {
				if r.IntN(10) == 0 {
					clock.Add(r.Int64N(2001) - 1000) // step back or forward up to 1 ms
				}
				out = append(out, g.Next(micros(clock.Load())))
			}
			results[w] = out
		}(w)
	}
	wg.Wait()
	all := make([]uint64, 0, goroutines*each)
	for w, out := range results {
		for i := 1; i < len(out); i++ {
			if out[i] <= out[i-1] {
				t.Fatalf("goroutine %d: version %d after %d", w, out[i], out[i-1])
			}
		}
		all = append(all, out...)
	}
	slices.Sort(all)
	for i := 1; i < len(all); i++ {
		if all[i] == all[i-1] {
			t.Fatalf("version %d issued twice", all[i])
		}
	}
	if g.Last() != all[len(all)-1] {
		t.Errorf("Last() = %d, want the largest issued %d", g.Last(), all[len(all)-1])
	}
}
