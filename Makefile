# NilDB build. RocksDB comes from Homebrew (brew install rocksdb); grocksdb links
# against it through cgo, so every go command needs these flags.
BREW_PREFIX ?= $(shell brew --prefix 2>/dev/null || echo /opt/homebrew)
export CGO_ENABLED = 1
export CGO_CFLAGS  = -I$(BREW_PREFIX)/include
export CGO_LDFLAGS = -L$(BREW_PREFIX)/lib
GO ?= go
PKG := ./...

.PHONY: all build test test-race vet fmt tidy rocksmoke run clean deps

all: build

deps:
	brew install go rocksdb

build:
	$(GO) build -o bin/nildb ./cmd/nildb

rocksmoke:
	$(GO) build -o bin/rocksmoke ./cmd/rocksmoke && ./bin/rocksmoke

test:
	$(GO) test $(PKG)

test-race:
	$(GO) test -race $(PKG)

vet:
	$(GO) vet $(PKG)

fmt:
	gofmt -l -w .

tidy:
	$(GO) mod tidy

run: build
	./bin/nildb --dir ./data --addr 127.0.0.1:6380

clean:
	rm -rf bin data

# Rust storage engines (docs/design/rust-engines.md). The toolchain comes from
# rustup (brew install rustup; rustup toolchain install stable). CARGO_TARGET_DIR
# is pinned so internal/nilengine finds rust/target/release/libnilengine.a.
CARGO ?= $(shell command -v cargo 2>/dev/null || echo $(BREW_PREFIX)/opt/rustup/bin/cargo)
RUST_DIR := rust
RUST_ENV := PATH="$(dir $(CARGO)):$$PATH" CARGO_TARGET_DIR=$(CURDIR)/$(RUST_DIR)/target
NILENGINE_LIB := $(RUST_DIR)/target/release/libnilengine.a
NILENGINE_PKG := ./internal/nilengine
# A hash of the library in the link flags makes go test relink after rust-build.
NILENGINE_LDFLAGS = -ldflags=-X=github.com/nil68657/nildb/internal/nilengine.libStamp=$$(shasum -a 256 $(NILENGINE_LIB) | cut -c1-16)

.PHONY: rust-build rust-test rust-soak rust-bench nilengine-test nilengine-bench

rust-build:
	cd $(RUST_DIR) && $(RUST_ENV) $(CARGO) build --release -p nilengine-capi

rust-test:
	cd $(RUST_DIR) && $(RUST_ENV) $(CARGO) test --workspace

rust-soak:
	cd $(RUST_DIR) && $(RUST_ENV) NILENGINE_SOAK=24 $(CARGO) test --workspace -- --ignored soak

rust-bench:
	cd $(RUST_DIR) && $(RUST_ENV) $(CARGO) run --release -p nilengine-bench -- \
		--num 2000000 --reads 1000000 --seeks 200000 --mixed-ops 1000000

nilengine-test: rust-build
	$(GO) test -tags nilengine -count=1 $(NILENGINE_LDFLAGS) $(NILENGINE_PKG)

nilengine-bench: rust-build
	$(GO) test -tags nilengine -count=1 -run '^$$' -bench . -benchtime 200000x $(NILENGINE_LDFLAGS) $(NILENGINE_PKG)
