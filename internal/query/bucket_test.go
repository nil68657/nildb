package query

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// TestBucketAccounting runs the bucket on a fake clock: after the initial
// burst every byte costs 1/rate seconds of sleep.
func TestBucketAccounting(t *testing.T) {
	const rate = 1 << 20
	start := time.Unix(1000, 0)
	now := start
	b := NewBucket(rate)
	b.now = func() time.Time { return now }
	b.sleep = func(d time.Duration) { now = now.Add(d) }
	const total = 2 << 20
	for n := 0; n < total; n += 32 << 10 {
		b.Take(32 << 10)
	}
	want := time.Duration((total - burstOf(rate)) / rate * float64(time.Second))
	if got := now.Sub(start); got < want-time.Millisecond || got > want+time.Millisecond {
		t.Fatalf("slept %v, want %v", got, want)
	}
	if b.Slept() != now.Sub(start) {
		t.Fatalf("Slept() = %v, clock moved %v", b.Slept(), now.Sub(start))
	}
	b.SetRate(0)
	before := now
	b.Take(10 << 20)
	if now != before {
		t.Fatalf("a bucket at rate 0 slept")
	}
}

func within20(t *testing.T, what string, got, want time.Duration) {
	t.Helper()
	if got < want*8/10 || got > want*12/10 {
		t.Fatalf("%s took %v, want %v within 20%%", what, got, want)
	}
}

func TestBucketRealTime(t *testing.T) {
	const rate = 4 << 20
	b := NewBucket(rate)
	const total = 2 << 20
	start := time.Now()
	for n := 0; n < total; n += 64 << 10 {
		b.Take(64 << 10)
	}
	want := time.Duration((total - burstOf(rate)) / rate * float64(time.Second))
	within20(t, "2 MiB at 4 MiB/s", time.Since(start), want)
}

// TestBucketThrottlesScan runs an analytical rowscan through a bucket and
// checks that its byte rate matches the bucket's.
func TestBucketThrottlesScan(t *testing.T) {
	e := newEnv(t)
	c := e.coll("test.bucket")
	pad := strings.Repeat("p", 1000)
	var docs []bson.Raw
	for i := range 1500 {
		docs = append(docs, mustMarshal(t, bson.D{{Key: "_id", Value: int32(i)}, {Key: "pad", Value: pad}, {Key: "tag", Value: fmt.Sprint(i)}}))
	}
	e.insert(c, docs...)
	const rate = 3 << 20
	b := NewBucket(rate)
	p, err := e.q.PlanFind(c, Find{}, Options{Mode: Analytic, Bucket: b})
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	run, err := p.Start()
	if err != nil {
		t.Fatal(err)
	}
	got, _, err := run.Next(-1, 0)
	run.Close()
	if err != nil || len(got) != 1500 {
		t.Fatalf("scan returned %d documents, err %v", len(got), err)
	}
	want := time.Duration((float64(run.Bytes()) - burstOf(rate)) / rate * float64(time.Second))
	within20(t, fmt.Sprintf("an analytical scan of %d bytes at %d B/s", run.Bytes(), rate), time.Since(start), want)
	if b.Slept() <= 0 {
		t.Fatalf("the bucket never made the scan sleep")
	}
}
