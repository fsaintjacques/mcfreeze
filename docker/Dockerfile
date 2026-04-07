# syntax=docker/dockerfile:1.7

# --- Rust builder (fm binary) ---
FROM rust:1.94-bookworm AS rust-builder
WORKDIR /src

COPY rust/ rust/

# Cache mounts keep cargo's registry and target/ outside the image layer.
# The compiled binary is copied OUT of the cache mount before the RUN
# completes so it survives into the runtime stage.
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,target=/src/rust/target,sharing=locked \
    cd rust && \
    cargo build --release --bin fm && \
    strip target/release/fm && \
    cp target/release/fm /fm

# --- Go builder (fmtctl binary) ---
FROM golang:1.26-bookworm AS go-builder
WORKDIR /src

COPY go/go.mod go/go.sum* go/
RUN --mount=type=cache,target=/root/.cache/go-build,sharing=locked \
    --mount=type=cache,target=/go/pkg/mod,sharing=locked \
    cd go && go mod download

COPY go/ go/
RUN --mount=type=cache,target=/root/.cache/go-build,sharing=locked \
    --mount=type=cache,target=/go/pkg/mod,sharing=locked \
    cd go && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /fmtctl ./cmd/fmtctl

# --- Runtime ---
FROM gcr.io/distroless/cc-debian12:nonroot
COPY --from=rust-builder /fm /usr/local/bin/fm
COPY --from=go-builder /fmtctl /usr/local/bin/fmtctl
ENTRYPOINT ["fmtctl"]
