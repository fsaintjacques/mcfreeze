CARGO_FLAGS ?=

.PHONY: build release format lint check-format check-lint check test test-unit test-integration test-kind clean \
       docker-build kind-up kind-down kind-load generate

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

# --- Code generation ---

# Regenerates deepcopy methods and CRD manifests from kubebuilder markers in
# go/api/v1alpha1/. Requires controller-gen on PATH (install via:
#   go install sigs.k8s.io/controller-tools/cmd/controller-gen@latest)
generate:
	cd go && controller-gen object paths=./api/v1alpha1/...
	cd go && controller-gen crd paths=./api/v1alpha1/... output:crd:dir=../k8s/crds

# --- Clean ---

clean:
	cd rust && cargo clean
	rm -rf go/bin

# --- KIND / Container ---

KIND_CLUSTER_NAME ?= frostmap
KIND_PROVIDER     ?= $(shell [ -n "$$GITHUB_ACTIONS" ] && echo docker || \
                       (command -v podman >/dev/null 2>&1 && echo podman || echo docker))
FROSTMAP_IMAGE    ?= frostmap:dev

export KIND_EXPERIMENTAL_PROVIDER = $(KIND_PROVIDER)

docker-build:
	$(KIND_PROVIDER) build -t $(FROSTMAP_IMAGE) -f docker/Dockerfile .

kind-up:
	kind create cluster --name $(KIND_CLUSTER_NAME) --config k8s/kind/cluster.yaml
	bash k8s/kind/deploy-csi-hostpath.sh
	kubectl apply -f k8s/crds/

kind-down:
	kind delete cluster --name $(KIND_CLUSTER_NAME)

# `kind load docker-image` shells out to `docker image inspect` to verify the
# image exists locally — even with the podman provider. When only podman is
# installed (no `docker` CLI), this always fails. Work around by saving to a
# tarball and loading via `kind load image-archive`.
kind-load: docker-build
ifeq ($(KIND_PROVIDER),podman)
	podman save $(FROSTMAP_IMAGE) -o /tmp/frostmap-kind.tar
	kind load image-archive /tmp/frostmap-kind.tar --name $(KIND_CLUSTER_NAME)
	rm -f /tmp/frostmap-kind.tar
else
	kind load docker-image $(FROSTMAP_IMAGE) --name $(KIND_CLUSTER_NAME)
endif
