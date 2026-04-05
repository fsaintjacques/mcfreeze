CARGO_FLAGS ?=

.PHONY: build release format lint check-format check-lint check test test-unit test-integration test-kind clean \
       docker-build kind-up kind-down kind-load

# --- Build ---

build:
	cd rust && cargo build $(CARGO_FLAGS)
	cd go && go build -o bin/fmtctl ./cmd/fmtctl

release:
	cd rust && cargo build --release $(CARGO_FLAGS)
	cd go && go build -o bin/fmtctl ./cmd/fmtctl

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

test-kind:
	kind export kubeconfig --name $(KIND_CLUSTER_NAME)
	cd go && go test -tags kind -count=1 -v ./...

# --- Clean ---

clean:
	cd rust && cargo clean
	rm -rf go/bin

# --- KIND / Container ---

KIND_CLUSTER_NAME ?= frostmap
KIND_PROVIDER     ?= $(shell [ -n "$$GITHUB_ACTIONS" ] && echo docker || \
                       (command -v podman >/dev/null 2>&1 && echo podman || echo docker))
FM_IMAGE          ?= frostmap/fm:dev
FMTCTL_IMAGE      ?= frostmap/fmtctl:dev

export KIND_EXPERIMENTAL_PROVIDER = $(KIND_PROVIDER)

docker-build:
	$(KIND_PROVIDER) build -t $(FM_IMAGE) -f docker/Dockerfile.fm .
	$(KIND_PROVIDER) build -t $(FMTCTL_IMAGE) -f docker/Dockerfile.fmtctl .

kind-up:
	kind create cluster --name $(KIND_CLUSTER_NAME) --config kind/cluster.yaml
	bash kind/deploy-csi-hostpath.sh

kind-down:
	kind delete cluster --name $(KIND_CLUSTER_NAME)

# `kind load docker-image` shells out to `docker image inspect` to verify the
# image exists locally — even with the podman provider. When only podman is
# installed (no `docker` CLI), this always fails. Work around by saving to a
# tarball and loading via `kind load image-archive`.
kind-load: docker-build
ifeq ($(KIND_PROVIDER),podman)
	podman save $(FM_IMAGE) -o /tmp/fm-kind.tar
	kind load image-archive /tmp/fm-kind.tar --name $(KIND_CLUSTER_NAME)
	rm -f /tmp/fm-kind.tar
	podman save $(FMTCTL_IMAGE) -o /tmp/fmtctl-kind.tar
	kind load image-archive /tmp/fmtctl-kind.tar --name $(KIND_CLUSTER_NAME)
	rm -f /tmp/fmtctl-kind.tar
else
	kind load docker-image $(FM_IMAGE) $(FMTCTL_IMAGE) --name $(KIND_CLUSTER_NAME)
endif
