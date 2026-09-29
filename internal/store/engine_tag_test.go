//go:build !nilengine

package store

import (
	"os"
	"strings"
	"testing"
)

// TestRustEngineNeedsTag checks the error a RocksDB-only build gives for
// a Rust engine, and that it leaves the directory as it found it.
func TestRustEngineNeedsTag(t *testing.T) {
	for _, e := range []string{EngineLSM, EngineBTree, EnginePgHeap} {
		dir := t.TempDir()
		_, err := Open(Config{Dir: dir, Engine: e})
		if err == nil || !strings.Contains(err.Error(), "--engine "+e+" needs a nildb built with -tags nilengine") {
			t.Errorf("Open with engine %s: %v", e, err)
		}
		if entries, _ := os.ReadDir(dir); len(entries) != 0 {
			t.Errorf("engine %s: the refused open left %d entries in %s", e, len(entries), dir)
		}
	}
}
