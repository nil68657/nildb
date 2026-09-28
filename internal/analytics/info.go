package analytics

import (
	"fmt"
	"strings"
)

// info writes the "analytics" INFO section. Rows and bytes count what the
// NIL.* commands and their cursors read; throttle sleep is the time their
// byte buckets waited.
func (a *Analytics) info(b *strings.Builder) {
	k := a.cfg.Knobs()
	running, queued := a.sem.load()
	for _, f := range []struct {
		name string
		v    int64
	}{
		{"analytics_queries", a.queries.Load()},
		{"analytics_running", int64(running)},
		{"analytics_queued", int64(queued)},
		{"analytics_semaphore_waits", a.waits.Load()},
		{"analytics_rejections", a.rejections.Load()},
		{"analytics_rows_scanned", a.stats.Rows.Load()},
		{"analytics_bytes_scanned", a.stats.Bytes.Load()},
		{"analytics_throttle_sleep_ms", a.throttleNS.Load() / 1e6},
		{"analytics_open_leases", int64(len(a.st.Leases()))},
		{"analytics_open_cursors", int64(a.q.Cursors.Len())},
		{"analytics_max_concurrent", int64(k.AnalyticsMaxConcurrent)},
		{"analytics_read_bps", k.AnalyticsReadBPS},
	} {
		fmt.Fprintf(b, "%s:%d\r\n", f.name, f.v)
	}
}
