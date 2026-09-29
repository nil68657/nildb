package store

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

// engineFile names the engine that owns a data directory. The LSM's
// CURRENT and MANIFEST-* files would collide with RocksDB's, so a
// directory opens only with the engine its marker names. A directory that
// holds files but no marker belongs to RocksDB: data directories and
// RocksDB checkpoints from before the Rust engines carry none.
const engineFile = "ENGINE"

// checkEngineMarker refuses a directory another engine owns. A missing or
// empty directory is a new database: it gets the marker, unless readOnly.
func checkEngineMarker(dir, engine string, readOnly bool) error {
	b, err := os.ReadFile(filepath.Join(dir, engineFile))
	switch {
	case err == nil:
		got := strings.TrimSpace(string(b))
		if got == engine {
			return nil
		}
		return fmt.Errorf("store: %s holds a %s database (its %s file says so); open it with --engine %s", dir, got, engineFile, got)
	case !errors.Is(err, fs.ErrNotExist):
		return fmt.Errorf("store: read %s marker: %w", engineFile, err)
	}
	empty, err := emptyDir(dir)
	if err != nil {
		return err
	}
	if !empty {
		if engine == EngineRocksDB {
			return nil
		}
		return fmt.Errorf("store: %s holds a rocksdb database (files and no %s file); open it with --engine rocksdb", dir, engineFile)
	}
	if readOnly {
		return nil
	}
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return fmt.Errorf("store: create %s: %w", dir, err)
	}
	return writeEngineMarker(dir, engine)
}

// emptyDir reports whether dir is missing or holds nothing but dot files
// and a marker left half-written.
func emptyDir(dir string) (bool, error) {
	entries, err := os.ReadDir(dir)
	if errors.Is(err, fs.ErrNotExist) {
		return true, nil
	}
	if err != nil {
		return false, fmt.Errorf("store: list %s: %w", dir, err)
	}
	for _, e := range entries {
		if n := e.Name(); !strings.HasPrefix(n, ".") && n != engineFile+".tmp" {
			return false, nil
		}
	}
	return true, nil
}

// writeEngineMarker writes the marker into dir through a synced temporary
// file and a rename, then syncs the directory.
func writeEngineMarker(dir, engine string) error {
	path := filepath.Join(dir, engineFile)
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o640)
	if err != nil {
		return fmt.Errorf("store: write %s marker: %w", engineFile, err)
	}
	_, err = f.WriteString(engine + "\n")
	if err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		err = os.Rename(tmp, path)
	}
	if err == nil {
		err = syncDir(dir)
	}
	if err != nil {
		return fmt.Errorf("store: write %s marker: %w", engineFile, err)
	}
	return nil
}

// syncDir syncs a directory so a rename in it survives a crash. Some file
// systems refuse to sync a directory descriptor; that is not an error.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	if err := d.Sync(); err != nil && !errors.Is(err, syscall.EINVAL) && !errors.Is(err, syscall.ENOTSUP) {
		return err
	}
	return nil
}
