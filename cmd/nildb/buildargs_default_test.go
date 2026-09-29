//go:build !nilengine

package main

import "testing"

// buildArgs are the go build flags that make the tested binary match this
// test binary: none in the default build.
func buildArgs(testing.TB) []string { return nil }
