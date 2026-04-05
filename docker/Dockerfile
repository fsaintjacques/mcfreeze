# --- Rust builder (fm binary) ---
FROM rust:1.94-bookworm AS rust-builder
WORKDIR /src

# Copy workspace manifests and create stub sources to cache dependencies.
COPY rust/Cargo.toml rust/Cargo.lock rust/
COPY rust/crates/frostmap-format/Cargo.toml rust/crates/frostmap-format/
COPY rust/crates/frostmap-loader/Cargo.toml rust/crates/frostmap-loader/
COPY rust/crates/frostmap-bq/Cargo.toml rust/crates/frostmap-bq/
COPY rust/crates/frostmap-encode/Cargo.toml rust/crates/frostmap-encode/
COPY rust/crates/frostmap-cli/Cargo.toml rust/crates/frostmap-cli/
COPY rust/crates/frostmap-server/Cargo.toml rust/crates/frostmap-server/

RUN for dir in rust/crates/*/; do \
      mkdir -p "$dir/src" && echo "" > "$dir/src/lib.rs"; \
    done && \
    echo "fn main() {}" > rust/crates/frostmap-cli/src/main.rs
# Only frostmap-cli has a binary; all others are library crates.
RUN cd rust && cargo build --release --bin fm || true

# Copy real source and build. Touch all source files to ensure cargo detects
# the change — COPY preserves layer timestamps that may match the stubs above.
COPY rust/ rust/
RUN find rust/crates -name '*.rs' -exec touch {} + && \
    cd rust && cargo build --release --bin fm && \
    strip target/release/fm

# --- Go builder (fmtctl binary) ---
FROM golang:1.26-bookworm AS go-builder
WORKDIR /src

COPY go/go.mod go/go.sum* go/
RUN cd go && go mod download || true

COPY go/ go/
RUN cd go && CGO_ENABLED=0 go build -ldflags="-s -w" -o /fmtctl ./cmd/fmtctl

# --- Runtime ---
FROM gcr.io/distroless/cc-debian12:nonroot
COPY --from=rust-builder /src/rust/target/release/fm /usr/local/bin/fm
COPY --from=go-builder /fmtctl /usr/local/bin/fmtctl
ENTRYPOINT ["fmtctl"]
