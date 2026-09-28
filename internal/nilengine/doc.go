// Package nilengine binds NilDB's own storage engines, an LSM tree and a
// copy-on-write B+ tree written in Rust under rust/, through their C
// interface (rust/include/nilengine.h).
//
// The binding compiles only with the nilengine build tag, after
// `make rust-build` has produced rust/target/release/libnilengine.a:
//
//	make rust-build
//	go test -tags nilengine ./internal/nilengine
//
// Without the tag this file is the whole package, so `go test ./...` and
// `make test` never need a Rust toolchain. docs/design/rust-engines.md
// describes the engines and the plan for putting them behind
// internal/store.
package nilengine
