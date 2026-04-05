#!/usr/bin/env bash
set -euo pipefail

CSI_HOSTPATH_VERSION="v1.15.0"

WORKDIR=$(mktemp -d)
trap 'rm -rf "$WORKDIR"' EXIT

echo "Cloning csi-driver-host-path ${CSI_HOSTPATH_VERSION}..."
git clone --depth 1 --branch "$CSI_HOSTPATH_VERSION" \
    https://github.com/kubernetes-csi/csi-driver-host-path.git "$WORKDIR/csi"

# The upstream deploy.sh exits non-zero if VolumeSnapshotClass CRDs are
# missing, but the driver and storage class are created before that point.
echo "Deploying csi-driver-host-path..."
"$WORKDIR/csi/deploy/kubernetes-latest/deploy.sh" || true

# Verify the critical resource was created.
kubectl get csidriver hostpath.csi.k8s.io >/dev/null 2>&1 || {
    echo "ERROR: CSIDriver hostpath.csi.k8s.io not found after deploy"
    exit 1
}

echo "Waiting for CSI driver pod to be ready..."
kubectl rollout status statefulset/csi-hostpathplugin --timeout=180s

# Create the StorageClass if it wasn't created by the deploy script.
if ! kubectl get storageclass csi-hostpath-sc >/dev/null 2>&1; then
    echo "Creating csi-hostpath-sc StorageClass..."
    kubectl apply -f - <<'EOF'
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: csi-hostpath-sc
provisioner: hostpath.csi.k8s.io
reclaimPolicy: Delete
volumeBindingMode: Immediate
allowVolumeExpansion: true
EOF
fi

echo "csi-driver-host-path ${CSI_HOSTPATH_VERSION} deployed successfully"
