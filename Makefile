# Frostmap top-level Makefile
#
# Targets:
#   all               Build all binaries (Rust + Go)
#   check             Run fmt + lint (CI gate)
#   fmt               Check formatting (rustfmt + gofmt)
#   lint              Run linters (clippy + go vet)
#   test              Run all tests (unit + integration)
#   test-unit         Run unit tests only (no binary prereqs)
#   test-integration  Run integration tests (builds binaries first)
#   clean             Remove build artifacts

# Rust binaries (real file targets — Make skips rebuild if up-to-date)
RUST_RELEASE  := rust/target/release
FM            := $(RUST_RELEASE)/fm

# Go binaries
GO_BIN        := go/bin
FMTCTL        := $(GO_BIN)/fmtctl

.PHONY: all check fmt lint test test-unit test-integration clean

# --- Build ---

all: $(FM) $(FMTCTL)

$(FM): $(shell find rust/crates -name '*.rs' -o -name 'Cargo.toml') rust/Cargo.lock
	cd rust && cargo build --release

$(FMTCTL): $(shell find go -name '*.go') go/go.mod
	mkdir -p $(GO_BIN)
	cd go && go build -o ../$(FMTCTL) ./cmd/node-agent

# --- Check (lint + format) ---

check: fmt lint

fmt:
	cd rust && cargo fmt --all -- --check
	@test -z "$$(cd go && gofmt -l .)" || { echo "gofmt: the following files need formatting:"; cd go && gofmt -l .; exit 1; }

lint:
	cd rust && cargo clippy --all-targets -- -D warnings
	cd go && go vet ./...

# --- Test ---

test: test-unit test-integration

test-unit:
	cd rust && cargo test
	cd go && go test ./...

test-integration: $(FM)
	cd go && FM=$(abspath $(FM)) go test -tags integration ./...

# --- Clean ---

clean:
	cd rust && cargo clean
	rm -rf $(GO_BIN)
