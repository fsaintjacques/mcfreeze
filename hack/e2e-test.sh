#!/usr/bin/env bash
#
# End-to-end test for frostmap snapshot mounting.
#
# Tests the full path: build Job writes snapshot -> PV created -> KV server
# mounts and serves data. Runs in a Kind cluster.
#
# Usage:
#   ./hack/e2e-test.sh              # create cluster, test, teardown
#   ./hack/e2e-test.sh --no-build   # skip image build (reuse existing)
#   ./hack/e2e-test.sh --keep       # keep cluster after test
#
# Environment:
#   KIND_EXPERIMENTAL_PROVIDER=podman   # use Podman instead of Docker
#   CONTAINER_RUNTIME=podman            # override container build tool
#   E2E_CLUSTER=frostmap-e2e           # Kind cluster name
#   E2E_IMAGE=frostmap:e2e             # container image tag

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

CLUSTER="${E2E_CLUSTER:-frostmap-e2e}"
IMAGE="${E2E_IMAGE:-frostmap:e2e}"
KEEP=false
BUILD=true

for arg in "$@"; do
  case "$arg" in
    --keep)    KEEP=true ;;
    --no-build) BUILD=false ;;
    --help|-h)
      echo "Usage: $0 [--no-build] [--keep]"
      exit 0
      ;;
  esac
done

# ---------------------------------------------------------------------------
# Detect container runtime
# ---------------------------------------------------------------------------
if [ -n "${CONTAINER_RUNTIME:-}" ]; then
  RUNTIME="$CONTAINER_RUNTIME"
elif command -v docker &>/dev/null; then
  RUNTIME=docker
elif command -v podman &>/dev/null; then
  RUNTIME=podman
else
  echo "ERROR: neither docker nor podman found" >&2
  exit 1
fi

# If using Podman, tell Kind.
if [ "$RUNTIME" = "podman" ]; then
  export KIND_EXPERIMENTAL_PROVIDER=podman
fi

echo "--- runtime: $RUNTIME, cluster: $CLUSTER, image: $IMAGE"

# ---------------------------------------------------------------------------
# Pre-flight checks
# ---------------------------------------------------------------------------
for cmd in kind kubectl "$RUNTIME"; do
  if ! command -v "$cmd" &>/dev/null; then
    echo "ERROR: $cmd is required but not found in PATH" >&2
    exit 1
  fi
done

# ---------------------------------------------------------------------------
# Cleanup on exit (unless --keep)
# ---------------------------------------------------------------------------
cleanup() {
  if [ "$KEEP" = false ]; then
    echo "--- deleting Kind cluster $CLUSTER"
    kind delete cluster --name "$CLUSTER" 2>/dev/null || true
  else
    echo "--- keeping cluster $CLUSTER (delete with: kind delete cluster --name $CLUSTER)"
  fi
}
trap cleanup EXIT

# ---------------------------------------------------------------------------
# Build container image
# ---------------------------------------------------------------------------
if [ "$BUILD" = true ]; then
  echo "--- building container image $IMAGE"
  "$RUNTIME" build -t "$IMAGE" -f "$ROOT_DIR/Dockerfile" "$ROOT_DIR"
fi

# ---------------------------------------------------------------------------
# Create Kind cluster
# ---------------------------------------------------------------------------
if kind get clusters 2>/dev/null | grep -q "^${CLUSTER}$"; then
  echo "--- cluster $CLUSTER already exists, reusing"
else
  echo "--- creating Kind cluster $CLUSTER"
  kind create cluster --name "$CLUSTER" --wait 60s
fi

# ---------------------------------------------------------------------------
# Load image into Kind
# ---------------------------------------------------------------------------
echo "--- loading image into Kind"
kind load docker-image "$IMAGE" --name "$CLUSTER"

# ---------------------------------------------------------------------------
# Deploy: build Job
# ---------------------------------------------------------------------------
echo "--- deploying build Job"
kubectl apply -f "$ROOT_DIR/hack/e2e/build-job.yaml"

echo "--- waiting for build Job to complete (timeout: 120s)"
kubectl wait --for=condition=complete --timeout=120s job/snapshot-build
echo "    build Job completed"

# Show build Job logs for debugging.
kubectl logs job/snapshot-build || true

# ---------------------------------------------------------------------------
# Deploy: PV + PVC
# ---------------------------------------------------------------------------
echo "--- creating PV and PVC"
kubectl apply -f "$ROOT_DIR/hack/e2e/snapshot-pv.yaml"

# Wait for PVC to bind.
for i in $(seq 1 30); do
  STATUS=$(kubectl get pvc pv-test-v1 -o jsonpath='{.status.phase}' 2>/dev/null || echo "")
  if [ "$STATUS" = "Bound" ]; then
    echo "    PVC bound"
    break
  fi
  if [ "$i" = "30" ]; then
    echo "FAIL: PVC did not bind within 30s (status: $STATUS)"
    kubectl describe pvc pv-test-v1
    exit 1
  fi
  sleep 1
done

# ---------------------------------------------------------------------------
# Deploy: KV server
# ---------------------------------------------------------------------------
echo "--- deploying KV server"
kubectl apply -f "$ROOT_DIR/hack/e2e/kv-server.yaml"

echo "--- waiting for KV server to be ready (timeout: 60s)"
kubectl wait --for=condition=ready --timeout=60s pod/kv-server
echo "    KV server ready"

# ---------------------------------------------------------------------------
# Deploy: verification Job
# ---------------------------------------------------------------------------
echo "--- deploying verification Job"
kubectl apply -f "$ROOT_DIR/hack/e2e/verify-job.yaml"

echo "--- waiting for verification Job to complete (timeout: 60s)"
if kubectl wait --for=condition=complete --timeout=60s job/verify-snapshot; then
  echo "    verification passed"
  kubectl logs job/verify-snapshot
  echo ""
  echo "=== E2E TEST PASSED ==="
else
  echo "FAIL: verification Job did not complete"
  kubectl logs job/verify-snapshot || true
  kubectl describe pod -l job-name=verify-snapshot || true
  exit 1
fi
