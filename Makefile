CARGO_FLAGS ?=

.PHONY: build release format lint check-format check-lint check test test-unit test-integration clean

# --- Build ---

build:
	cd rust && cargo build $(CARGO_FLAGS)
	cd go && go build -o bin/fmtctl ./cmd/node-agent

release:
	cd rust && cargo build --release $(CARGO_FLAGS)
	cd go && go build -o bin/fmtctl ./cmd/node-agent

# --- Fix ---

format:
	cd rust && cargo fmt
	cd go && gofmt -w -l .

lint:
	cd rust && cargo clippy $(CARGO_FLAGS) --fix --allow-dirty -- -D warnings
	cd go && go vet ./...

# --- Check (read-only, CI gate) ---

check-format:
	cd rust && cargo fmt --check
	@test -z "$$(cd go && gofmt -l .)" || { echo "gofmt: the following files need formatting:"; cd go && gofmt -l .; exit 1; }

check-lint:
	cd rust && cargo clippy $(CARGO_FLAGS) -- -D warnings
	cd go && go vet ./...

check: check-format check-lint test

# --- Test ---

test: test-unit test-integration

test-unit:
	cd rust && cargo test $(CARGO_FLAGS)
	cd go && go test ./...

test-integration: build
	cd go && FM=$(abspath rust/target/debug/fm) go test -tags integration ./...

# --- Clean ---

clean:
	cd rust && cargo clean
	rm -rf go/bin
