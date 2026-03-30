# Frostmap top-level Makefile
#
# Targets:
#   all               Build all binaries (Rust + Go)
#   test              Run all tests (unit + integration)
#   test-unit         Run unit tests only (no binary prereqs)
#   test-integration  Run integration tests (builds binaries first)
#   clean             Remove build artifacts

# Rust binaries (real file targets — Make skips rebuild if up-to-date)
RUST_RELEASE  := rust/target/release
FM            := $(RUST_RELEASE)/fm
FM_SERVER     := $(RUST_RELEASE)/frostmap-server

# Go binaries
GO_BIN        := go/bin
FMTCTL        := $(GO_BIN)/fmtctl

.PHONY: all test test-unit test-integration clean

# --- Build ---

all: $(FM) $(FM_SERVER) $(FMTCTL)

$(FM) $(FM_SERVER): $(shell find rust/crates -name '*.rs' -o -name 'Cargo.toml') rust/Cargo.lock
	cd rust && cargo build --release

$(FMTCTL): $(shell find go -name '*.go') go/go.mod
	mkdir -p $(GO_BIN)
	cd go && go build -o ../$(FMTCTL) ./cmd/node-agent

# --- Test ---

test: test-unit test-integration

test-unit:
	cd rust && cargo test
	cd go && go test ./...

test-integration: $(FM) $(FM_SERVER)
	cd go && FM=$(abspath $(FM)) FM_SERVER=$(abspath $(FM_SERVER)) go test -tags integration ./...

# --- Clean ---

clean:
	cd rust && cargo clean
	rm -rf $(GO_BIN)
