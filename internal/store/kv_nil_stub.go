//go:build !nilengine

package store

import "fmt"

// engineAvailable reports whether this build has the named engine; one
// without the nilengine tag links RocksDB only.
func engineAvailable(name string) error {
	if name == EngineRocksDB {
		return nil
	}
	return fmt.Errorf("store: --engine %s needs a nildb built with -tags nilengine (make build-engines); this build has rocksdb only", name)
}

func openNil(cfg Config, _ bool) (kv, error) { return nil, engineAvailable(cfg.Engine) }
