//go:build nildb_no_shim

package store

import "testing"

// TestShimRoundTrip documents the fallback: under nildb_no_shim,
// IterOpts.LowPriority does nothing and the analytics byte bucket and
// semaphore are the only throttle.
func TestShimRoundTrip(t *testing.T) {
	active, why := ShimActive()
	if active {
		t.Fatal("ShimActive under nildb_no_shim")
	}
	t.Skipf("built with nildb_no_shim: %v", why)
}
