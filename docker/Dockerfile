# syntax=docker/dockerfile:1.7

# --- Rust builder (mcf binary) ---
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
    cargo build --release --bin mcf && \
    strip target/release/mcf && \
    cp target/release/mcf /mcf

# --- Go builder (mcfctl binary) ---
FROM golang:1.26-bookworm AS go-builder
WORKDIR /src

COPY go/go.mod go/go.sum* go/
RUN --mount=type=cache,target=/root/.cache/go-build,sharing=locked \
    --mount=type=cache,target=/go/pkg/mod,sharing=locked \
    cd go && go mod download

COPY go/ go/
RUN --mount=type=cache,target=/root/.cache/go-build,sharing=locked \
    --mount=type=cache,target=/go/pkg/mod,sharing=locked \
    cd go && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /mcfctl ./cmd/mcfctl

# --- Runtime ---
FROM gcr.io/distroless/cc-debian12:nonroot
COPY --from=rust-builder /mcf /usr/local/bin/mcf
COPY --from=go-builder /mcfctl /usr/local/bin/mcfctl
ENTRYPOINT ["mcfctl"]
