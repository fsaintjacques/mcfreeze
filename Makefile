# SPDX-License-Identifier: Apache-2.0
CARGO_FLAGS ?=

.PHONY: build release format lint check-format check-lint check test test-unit test-integration test-kind clean \
       docker-build kind-up kind-down kind-load helm-install-kind helm-uninstall-kind generate \
       gke-up gke-down gke-push gke-creds helm-install-gke helm-uninstall-gke test-gke

# --- Build ---

build:
	cd rust && cargo build $(CARGO_FLAGS)
	cd go && go build -o bin/mcfctl ./cmd/mcfctl

release:
	cd rust && cargo build --release $(CARGO_FLAGS)
	cd go && go build -o bin/mcfctl ./cmd/mcfctl

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

ENVTEST_K8S_VERSION ?= 1.31.0
SETUP_ENVTEST ?= $(shell go env GOPATH)/bin/setup-envtest

$(SETUP_ENVTEST):
	cd go && go install sigs.k8s.io/controller-runtime/tools/setup-envtest@latest

envtest-setup: $(SETUP_ENVTEST)
	$(SETUP_ENVTEST) use $(ENVTEST_K8S_VERSION) -p path

test-integration: build envtest-setup
	cd go && \
		MCF=$(abspath rust/target/debug/mcf) \
		KUBEBUILDER_ASSETS="$$($(SETUP_ENVTEST) use $(ENVTEST_K8S_VERSION) -p path)" \
		go test -tags integration ./...

test-kind: helm-install-kind
	kind export kubeconfig --name $(KIND_CLUSTER_NAME)
	cd go && MCFREEZE_IMAGE=$(MCFREEZE_IMAGE) go test -tags kind -count=1 -v ./...

# --- Code generation ---

# Regenerates deepcopy methods and CRD manifests from kubebuilder markers in
# go/api/v1alpha1/. Requires controller-gen on PATH (install via:
#   go install sigs.k8s.io/controller-tools/cmd/controller-gen@latest)
generate:
	cd go && controller-gen object paths=./api/v1alpha1/...
	cd go && controller-gen crd paths=./api/v1alpha1/... output:crd:dir=../k8s/charts/mcfreeze/crds

# --- Clean ---

clean:
	cd rust && cargo clean
	rm -rf go/bin

# --- KIND / Container ---

KIND_CLUSTER_NAME ?= mcfreeze
KIND_PROVIDER     ?= $(shell [ -n "$$GITHUB_ACTIONS" ] && echo docker || \
                       (command -v podman >/dev/null 2>&1 && echo podman || echo docker))
MCFREEZE_IMAGE    ?= $(shell [ "$(KIND_PROVIDER)" = "podman" ] && echo localhost/mcfreeze:dev || echo mcfreeze:dev)
MCFREEZE_IMAGE_REPO = $(firstword $(subst :, ,$(MCFREEZE_IMAGE)))
MCFREEZE_IMAGE_TAG  = $(lastword $(subst :, ,$(MCFREEZE_IMAGE)))

export KIND_EXPERIMENTAL_PROVIDER = $(KIND_PROVIDER)

GKE_PLATFORM ?= linux/arm64

docker-build:
	$(KIND_PROVIDER) build -t $(MCFREEZE_IMAGE) -f docker/Dockerfile .

kind-up:
	kind create cluster --name $(KIND_CLUSTER_NAME) --config k8s/kind/cluster.yaml
	bash k8s/kind/deploy-csi-hostpath.sh

kind-down:
	kind delete cluster --name $(KIND_CLUSTER_NAME)

# `kind load docker-image` shells out to `docker image inspect` to verify the
# image exists locally — even with the podman provider. When only podman is
# installed (no `docker` CLI), this always fails. Work around by saving to a
# tarball and loading via `kind load image-archive`.
kind-load: docker-build
ifeq ($(KIND_PROVIDER),podman)
	podman save $(MCFREEZE_IMAGE) -o /tmp/mcfreeze-kind.tar
	kind load image-archive /tmp/mcfreeze-kind.tar --name $(KIND_CLUSTER_NAME)
	rm -f /tmp/mcfreeze-kind.tar
else
	kind load docker-image $(MCFREEZE_IMAGE) --name $(KIND_CLUSTER_NAME)
endif

# Install (or upgrade) the mcfreeze chart into the running KIND cluster.
# Assumes `make kind-up kind-load` has already run.
HELM_RELEASE_NAME ?= mcfreeze
HELM_RELEASE_NS   ?= mcfreeze-system

helm-install-kind:
	helm upgrade --install $(HELM_RELEASE_NAME) ./k8s/charts/mcfreeze \
		-n $(HELM_RELEASE_NS) --create-namespace \
		-f ./k8s/charts/mcfreeze/values-kind.yaml \
		--set image.repository=$(MCFREEZE_IMAGE_REPO) \
		--set image.tag=$(MCFREEZE_IMAGE_TAG) \
		--wait

helm-uninstall-kind:
	helm uninstall $(HELM_RELEASE_NAME) -n $(HELM_RELEASE_NS) --ignore-not-found

# --- GKE / Terraform ---

TF_DIR     := tf/e2e
TF         := terraform -chdir=$(TF_DIR)
GIT_SHA    ?= $(shell git rev-parse --short HEAD)

# Resolve outputs once per recipe via $(shell …) so each target is self-contained.
GCP_PROJECT    = $(shell gcloud config get-value project 2>/dev/null)
GKE_IMAGE_REPO = $(shell $(TF) output -raw image_repo 2>/dev/null)
GKE_CLUSTER    = $(shell $(TF) output -raw cluster_name 2>/dev/null)
GKE_LOCATION   = $(shell $(TF) output -raw cluster_location 2>/dev/null)
GKE_BQ_TABLE   = $(shell $(TF) output -raw bq_table 2>/dev/null)
GKE_BUILDER_SA = $(shell $(TF) output -raw builder_sa 2>/dev/null)
GKE_NODE_SA    = $(shell $(TF) output -raw node_agent_sa 2>/dev/null)
GKE_DESC_URI   = $(shell $(TF) output -raw stackoverflow_descriptor_uri 2>/dev/null)

gke-up:
	$(TF) init
	$(TF) apply -auto-approve

gke-creds:
	gcloud container clusters get-credentials $(GKE_CLUSTER) --region $(GKE_LOCATION)

gke-push:
	podman build --platform $(GKE_PLATFORM) -t $(GKE_IMAGE_REPO):$(GIT_SHA) -f docker/Dockerfile .
	podman push $(GKE_IMAGE_REPO):$(GIT_SHA)

helm-install-gke: gke-creds
	helm upgrade --install $(HELM_RELEASE_NAME) ./k8s/charts/mcfreeze \
		-n $(HELM_RELEASE_NS) --create-namespace \
		-f $(TF_DIR)/values-gke.yaml \
		--set image.repository=$(GKE_IMAGE_REPO) \
		--set image.tag=$(GIT_SHA) \
		--set-string serviceAccount.builderAnnotations.iam\\.gke\\.io/gcp-service-account=$(GKE_BUILDER_SA) \
		--set-string serviceAccount.nodeAgentAnnotations.iam\\.gke\\.io/gcp-service-account=$(GKE_NODE_SA) \
		--wait --timeout 5m

helm-uninstall-gke: gke-creds
	helm uninstall $(HELM_RELEASE_NAME) -n $(HELM_RELEASE_NS) --ignore-not-found

test-gke: helm-install-gke
	cd go && \
		GCP_PROJECT=$(GCP_PROJECT) \
		MCFREEZE_IMAGE=$(GKE_IMAGE_REPO):$(GIT_SHA) \
		BQ_TABLE=$(GKE_BQ_TABLE) \
		MCFREEZE_E2E_DESCRIPTOR_URI=$(GKE_DESC_URI) \
		go test -tags gke -count=1 -v -timeout 20m ./...

gke-down:
	$(TF) destroy -auto-approve
