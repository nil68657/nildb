package query

import (
	"sync"
	"sync/atomic"
	"time"
)

// Bucket is the token bucket that bounds how fast analytical runs read
// (nildb.analytics-read-bps): every iterator step and row costs its key
// plus value bytes, and a taker that overdraws the bucket sleeps until the
// debt is paid. All analytical commands share one Bucket, so the rate
// bounds their sum. A rate of 0 turns it off.
type Bucket struct {
	mu     sync.Mutex
	rate   int64 // bytes per second
	tokens float64
	last   time.Time

	slept atomic.Int64 // nanoseconds spent sleeping, for INFO

	now   func() time.Time
	sleep func(time.Duration)
}

// NewBucket returns a full bucket that refills at bytesPerSec.
func NewBucket(bytesPerSec int64) *Bucket {
	return &Bucket{rate: bytesPerSec, tokens: burstOf(bytesPerSec), now: time.Now, sleep: time.Sleep}
}

// burstOf is the bucket's capacity: 50 ms worth of reads, at least 16 KiB,
// so the achieved rate stays close to the configured one over a second.
func burstOf(rate int64) float64 {
	return max(float64(rate)/20, 16<<10)
}

// SetRate changes the rate; CONFIG SET nildb.analytics-read-bps reaches it
// through the analytics package before each command.
func (b *Bucket) SetRate(bytesPerSec int64) {
	b.mu.Lock()
	if bytesPerSec != b.rate {
		b.rate = bytesPerSec
		b.tokens = min(b.tokens, burstOf(bytesPerSec))
	}
	b.mu.Unlock()
}

// Rate returns the current rate in bytes per second.
func (b *Bucket) Rate() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.rate
}

// Slept returns the total time takers have slept.
func (b *Bucket) Slept() time.Duration { return time.Duration(b.slept.Load()) }

// Take charges n bytes and sleeps while the bucket is overdrawn. The sleep
// happens outside the lock, so concurrent takers each wait out their own
// share of the debt.
func (b *Bucket) Take(n int) {
	if b == nil || n <= 0 {
		return
	}
	b.mu.Lock()
	rate := b.rate
	if rate <= 0 {
		b.mu.Unlock()
		return
	}
	now := b.now()
	if !b.last.IsZero() {
		b.tokens = min(b.tokens+now.Sub(b.last).Seconds()*float64(rate), burstOf(rate))
	}
	b.last = now
	b.tokens -= float64(n)
	var wait time.Duration
	if b.tokens < 0 {
		wait = time.Duration(-b.tokens / float64(rate) * float64(time.Second))
	}
	b.mu.Unlock()
	if wait > 0 {
		b.slept.Add(int64(wait))
		b.sleep(wait)
	}
}
