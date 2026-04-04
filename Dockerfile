# syntax=docker/dockerfile:1
#
# Multi-stage build for frostmap e2e testing.
# Produces a single image containing:
#   - /usr/local/bin/fm       (Rust: snapshot builder + KV server)
#   - /usr/local/bin/fmtctl   (Go: control-plane / node-agent)

# ---------------------------------------------------------------------------
# Stage 1: Build fm (Rust)
# ---------------------------------------------------------------------------
FROM rust:1-bookworm AS rust-builder

# Install protobuf compiler (needed by frostmap-encode / prost-build).
RUN apt-get update && apt-get install -y --no-install-recommends protobuf-compiler && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY rust/ rust/

WORKDIR /src/rust
RUN cargo build --release --bin fm \
    && strip target/release/fm

# ---------------------------------------------------------------------------
# Stage 2: Build fmtctl (Go)
# ---------------------------------------------------------------------------
FROM golang:1.23-bookworm AS go-builder

WORKDIR /src/go
COPY go/go.mod go/go.sum* ./
RUN go mod download 2>/dev/null || true

COPY go/ .
RUN CGO_ENABLED=0 go build -o /fmtctl ./cmd/node-agent

# ---------------------------------------------------------------------------
# Stage 3: Runtime image
# ---------------------------------------------------------------------------
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=rust-builder /src/rust/target/release/fm /usr/local/bin/fm
COPY --from=go-builder   /fmtctl                     /usr/local/bin/fmtctl

ENTRYPOINT ["/bin/sh"]
