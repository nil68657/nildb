//go:build nilengine

package main

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"testing"
)

// buildArgs are the go build flags that make the tested binary match this
// test binary: the nilengine tag, and a hash of libnilengine.a in the link
// flags, since go's build cache does not look inside a library named in
// cgo LDFLAGS (the Makefile's NILENGINE_LDFLAGS does the same).
func buildArgs(t testing.TB) []string {
	lib, err := os.ReadFile("../../rust/target/release/libnilengine.a")
	if err != nil {
		t.Fatalf("read the engine library (make rust-build): %v", err)
	}
	sum := sha256.Sum256(lib)
	return []string{"-tags", "nilengine",
		"-ldflags=-X=github.com/nil68657/nildb/internal/nilengine.libStamp=" + hex.EncodeToString(sum[:8])}
}
