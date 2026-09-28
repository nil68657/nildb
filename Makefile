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
