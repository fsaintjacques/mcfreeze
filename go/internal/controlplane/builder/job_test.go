package builder

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/fsaintjacques/mcfreeze/go/api"
	"github.com/fsaintjacques/mcfreeze/go/internal/controlplane/volume"
)

const testNamespace = "default"

var testSpec = api.DatasetSpec{
	Name:       "users",
	KeyPrefix:  "users",
	ShardCount: 4,
	Retention:  2,
	Source: api.SourceSpec{
		KeyColumn:   "id",
		ValueColumn: "payload",
	},
}

func newTestJob() (*Job, *fake.Clientset) {
	cs := fake.NewSimpleClientset()
	dm := &volume.LocalPathManager{Client: cs, Namespace: testNamespace}
	return &Job{
		Client:       cs,
		Volumes:      dm,
		Namespace:    testNamespace,
		Image:        "mcfreeze/mcf:dev",
		StorageClass: "local-path",
		DiskSizeGB:   10,
	}, cs
}

func TestJob_Start(t *testing.T) {
	jb, cs := newTestJob()
	ctx := context.Background()

	handle, err := jb.Start(ctx, testSpec, "v1")
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	wantHandle := Handle("mcf-build-users-v1")
	if handle != wantHandle {
		t.Errorf("handle = %q, want %q", handle, wantHandle)
	}

	// Verify PVC created.
	pvc, err := cs.CoreV1().PersistentVolumeClaims(testNamespace).Get(ctx, "mcf-pvc-users-v1", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get PVC: %v", err)
	}
	if pvc.Spec.AccessModes[0] != corev1.ReadWriteOnce {
		t.Errorf("PVC access mode = %v, want ReadWriteOnce", pvc.Spec.AccessModes[0])
	}
	wantSize := resource.MustParse("10Gi")
	gotSize := pvc.Spec.Resources.Requests[corev1.ResourceStorage]
	if gotSize.Cmp(wantSize) != 0 {
		t.Errorf("PVC size = %v, want %v", gotSize.String(), wantSize.String())
	}

	// Verify ConfigMap created with correct worker.json.
	cm, err := cs.CoreV1().ConfigMaps(testNamespace).Get(ctx, "mcf-config-users-v1", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get ConfigMap: %v", err)
	}
	var wc workerConfig
	if err := json.Unmarshal([]byte(cm.Data[workerConfigFile]), &wc); err != nil {
		t.Fatalf("unmarshal worker.json: %v", err)
	}
	if wc.Output != "/output" {
		t.Errorf("workerConfig.Output = %q, want %q", wc.Output, "/output")
	}
	if wc.Partitions != 4 {
		t.Errorf("workerConfig.Partitions = %d, want %d", wc.Partitions, 4)
	}
	if wc.Source.KeyColumn != "id" {
		t.Errorf("workerConfig.Source.KeyColumn = %q, want %q", wc.Source.KeyColumn, "id")
	}

	// Verify Job created with correct spec.
	job, err := cs.BatchV1().Jobs(testNamespace).Get(ctx, "mcf-build-users-v1", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get Job: %v", err)
	}
	if job.Labels["mcfreeze.io/dataset"] != "users" {
		t.Errorf("Job label dataset = %q, want %q", job.Labels["mcfreeze.io/dataset"], "users")
	}
	if job.Labels["mcfreeze.io/version"] != "v1" {
		t.Errorf("Job label version = %q, want %q", job.Labels["mcfreeze.io/version"], "v1")
	}
	if *job.Spec.BackoffLimit != 0 {
		t.Errorf("backoffLimit = %d, want 0", *job.Spec.BackoffLimit)
	}
	container := job.Spec.Template.Spec.Containers[0]
	if container.Image != "mcfreeze/mcf:dev" {
		t.Errorf("container image = %q, want %q", container.Image, "mcfreeze/mcf:dev")
	}
	if container.Command[0] != "mcfctl" || container.Command[1] != "job" {
		t.Errorf("container command = %v, want [mcfctl job ...]", container.Command)
	}

	// Verify volume mounts.
	if len(container.VolumeMounts) != 2 {
		t.Fatalf("volume mounts = %d, want 2", len(container.VolumeMounts))
	}
	var outputMount, configMount *corev1.VolumeMount
	for i := range container.VolumeMounts {
		switch container.VolumeMounts[i].Name {
		case "output":
			outputMount = &container.VolumeMounts[i]
		case "config":
			configMount = &container.VolumeMounts[i]
		}
	}
	if outputMount == nil || outputMount.MountPath != "/output" {
		t.Errorf("output mount = %+v, want /output", outputMount)
	}
	if configMount == nil || configMount.MountPath != "/config" || !configMount.ReadOnly {
		t.Errorf("config mount = %+v, want /config read-only", configMount)
	}
}

func TestJob_Start_Idempotent(t *testing.T) {
	jb, _ := newTestJob()
	ctx := context.Background()

	h1, err := jb.Start(ctx, testSpec, "v1")
	if err != nil {
		t.Fatalf("first Start: %v", err)
	}

	h2, err := jb.Start(ctx, testSpec, "v1")
	if err != nil {
		t.Fatalf("second Start: %v", err)
	}

	if h1 != h2 {
		t.Errorf("handles differ: %q vs %q", h1, h2)
	}
}

func TestJob_Poll_Running(t *testing.T) {
	jb, _ := newTestJob()
	ctx := context.Background()

	handle, err := jb.Start(ctx, testSpec, "v1")
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	status, err := jb.Poll(ctx, handle)
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if status.Phase != Running {
		t.Errorf("phase = %v, want %v", status.Phase, Running)
	}
}

func TestJob_Poll_Complete(t *testing.T) {
	jb, cs := newTestJob()
	ctx := context.Background()

	handle, err := jb.Start(ctx, testSpec, "v1")
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Simulate Job completion by setting the condition.
	job, _ := cs.BatchV1().Jobs(testNamespace).Get(ctx, string(handle), metav1.GetOptions{})
	job.Status.Conditions = []batchv1.JobCondition{
		{Type: batchv1.JobComplete, Status: corev1.ConditionTrue},
	}
	cs.BatchV1().Jobs(testNamespace).UpdateStatus(ctx, job, metav1.UpdateOptions{})

	// Simulate PVC bound (so FinalizeBuild works).
	pvc, _ := cs.CoreV1().PersistentVolumeClaims(testNamespace).Get(ctx, "mcf-pvc-users-v1", metav1.GetOptions{})
	pvName := "pv-users-v1"
	pvc.Spec.VolumeName = pvName
	pvc.Status.Phase = corev1.ClaimBound
	cs.CoreV1().PersistentVolumeClaims(testNamespace).Update(ctx, pvc, metav1.UpdateOptions{})
	cs.CoreV1().PersistentVolumeClaims(testNamespace).UpdateStatus(ctx, pvc, metav1.UpdateOptions{})

	// Create the backing PV.
	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: pvName},
		Spec: corev1.PersistentVolumeSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Capacity: corev1.ResourceList{
				corev1.ResourceStorage: resource.MustParse("10Gi"),
			},
			PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimDelete,
			ClaimRef: &corev1.ObjectReference{
				Name:      "mcf-pvc-users-v1",
				Namespace: testNamespace,
			},
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				HostPath: &corev1.HostPathVolumeSource{Path: "/tmp/test"},
			},
		},
	}
	cs.CoreV1().PersistentVolumes().Create(ctx, pv, metav1.CreateOptions{})

	status, err := jb.Poll(ctx, handle)
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if status.Phase != Complete {
		t.Errorf("phase = %v, want %v", status.Phase, Complete)
	}
	if status.Result.PVName != pvName {
		t.Errorf("PVName = %q, want %q", status.Result.PVName, pvName)
	}

	// Verify PV was finalized.
	finalPV, _ := cs.CoreV1().PersistentVolumes().Get(ctx, pvName, metav1.GetOptions{})
	if finalPV.Spec.ClaimRef != nil {
		t.Errorf("PV claimRef should be nil after finalize")
	}
	if finalPV.Spec.AccessModes[0] != corev1.ReadOnlyMany {
		t.Errorf("PV accessMode = %v, want ReadOnlyMany", finalPV.Spec.AccessModes[0])
	}
}

func TestJob_Poll_Failed(t *testing.T) {
	jb, cs := newTestJob()
	ctx := context.Background()

	handle, err := jb.Start(ctx, testSpec, "v1")
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Simulate Job failure.
	job, _ := cs.BatchV1().Jobs(testNamespace).Get(ctx, string(handle), metav1.GetOptions{})
	job.Status.Conditions = []batchv1.JobCondition{
		{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Message: "OOMKilled"},
	}
	cs.BatchV1().Jobs(testNamespace).UpdateStatus(ctx, job, metav1.UpdateOptions{})

	status, err := jb.Poll(ctx, handle)
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if status.Phase != Failed {
		t.Errorf("phase = %v, want %v", status.Phase, Failed)
	}
	if status.Error != "OOMKilled" {
		t.Errorf("error = %q, want %q", status.Error, "OOMKilled")
	}
}

func TestJob_Poll_NotFound(t *testing.T) {
	jb, _ := newTestJob()
	ctx := context.Background()

	status, err := jb.Poll(ctx, Handle("nonexistent-job"))
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if status.Phase != NotFound {
		t.Errorf("phase = %v, want %v", status.Phase, NotFound)
	}
}

func TestJob_Cancel(t *testing.T) {
	jb, cs := newTestJob()
	ctx := context.Background()

	handle, err := jb.Start(ctx, testSpec, "v1")
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	if err := jb.Cancel(ctx, handle); err != nil {
		t.Fatalf("Cancel: %v", err)
	}

	// Verify Job deleted.
	_, err = cs.BatchV1().Jobs(testNamespace).Get(ctx, string(handle), metav1.GetOptions{})
	if err == nil {
		t.Error("Job should be deleted after Cancel")
	}

	// Verify ConfigMap deleted.
	_, err = cs.CoreV1().ConfigMaps(testNamespace).Get(ctx, "mcf-config-users-v1", metav1.GetOptions{})
	if err == nil {
		t.Error("ConfigMap should be deleted after Cancel")
	}

	// Verify PVC deleted.
	_, err = cs.CoreV1().PersistentVolumeClaims(testNamespace).Get(ctx, "mcf-pvc-users-v1", metav1.GetOptions{})
	if err == nil {
		t.Error("PVC should be deleted after Cancel")
	}
}

func TestJob_Cancel_Idempotent(t *testing.T) {
	jb, _ := newTestJob()
	ctx := context.Background()

	// Cancel a non-existent build — should not error.
	if err := jb.Cancel(ctx, Handle("nonexistent-job")); err != nil {
		t.Fatalf("Cancel on nonexistent: %v", err)
	}
}

func TestJob_Poll_Complete_Idempotent(t *testing.T) {
	jb, cs := newTestJob()
	ctx := context.Background()

	handle, err := jb.Start(ctx, testSpec, "v1")
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Simulate Job completion.
	job, _ := cs.BatchV1().Jobs(testNamespace).Get(ctx, string(handle), metav1.GetOptions{})
	job.Status.Conditions = []batchv1.JobCondition{
		{Type: batchv1.JobComplete, Status: corev1.ConditionTrue},
	}
	cs.BatchV1().Jobs(testNamespace).UpdateStatus(ctx, job, metav1.UpdateOptions{})

	// Simulate PVC bound.
	pvc, _ := cs.CoreV1().PersistentVolumeClaims(testNamespace).Get(ctx, "mcf-pvc-users-v1", metav1.GetOptions{})
	pvName := "pv-users-v1"
	pvc.Spec.VolumeName = pvName
	pvc.Status.Phase = corev1.ClaimBound
	cs.CoreV1().PersistentVolumeClaims(testNamespace).Update(ctx, pvc, metav1.UpdateOptions{})
	cs.CoreV1().PersistentVolumeClaims(testNamespace).UpdateStatus(ctx, pvc, metav1.UpdateOptions{})

	// Create backing PV.
	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: pvName},
		Spec: corev1.PersistentVolumeSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Capacity: corev1.ResourceList{
				corev1.ResourceStorage: resource.MustParse("10Gi"),
			},
			PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimDelete,
			ClaimRef: &corev1.ObjectReference{
				Name:      "mcf-pvc-users-v1",
				Namespace: testNamespace,
			},
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				HostPath: &corev1.HostPathVolumeSource{Path: "/tmp/test"},
			},
		},
	}
	cs.CoreV1().PersistentVolumes().Create(ctx, pv, metav1.CreateOptions{})

	// First Poll — triggers FinalizeBuild.
	s1, err := jb.Poll(ctx, handle)
	if err != nil {
		t.Fatalf("first Poll: %v", err)
	}
	if s1.Phase != Complete {
		t.Fatalf("first Poll phase = %v, want %v", s1.Phase, Complete)
	}
	if s1.Result.PVName != pvName {
		t.Errorf("first Poll PVName = %q, want %q", s1.Result.PVName, pvName)
	}

	// Second Poll — PVC is gone, but annotation makes it idempotent.
	s2, err := jb.Poll(ctx, handle)
	if err != nil {
		t.Fatalf("second Poll: %v", err)
	}
	if s2.Phase != Complete {
		t.Errorf("second Poll phase = %v, want %v (should be idempotent)", s2.Phase, Complete)
	}
	if s2.Result.PVName != pvName {
		t.Errorf("second Poll PVName = %q, want %q", s2.Result.PVName, pvName)
	}
}

func TestResourceName_DNSSanitization(t *testing.T) {
	tests := []struct {
		prefix, dataset, version string
		want                     string
	}{
		{"mcf-build", "users", "v1", "mcf-build-users-v1"},
		{"mcf-build", "user_events", "v1", "mcf-build-user-events-v1"},
		{"mcf-pvc", "my.dataset", "v2", "mcf-pvc-my-dataset-v2"},
		{"mcf-config", "UPPER", "V1", "mcf-config-upper-v1"},
		{"mcf-build", "a__b", "v1", "mcf-build-a-b-v1"},
		{"mcf-build", "trailing-", "v1", "mcf-build-trailing-v1"},
	}
	for _, tt := range tests {
		got := resourceName(tt.prefix, tt.dataset, tt.version)
		if got != tt.want {
			t.Errorf("resourceName(%q, %q, %q) = %q, want %q", tt.prefix, tt.dataset, tt.version, got, tt.want)
		}
	}
}

func TestResourceName_TruncateNoTrailingHyphen(t *testing.T) {
	long := strings.Repeat("a", 250)
	name := resourceName("mcf", long, "v1")
	if len(name) > 253 {
		t.Errorf("name length = %d, want <= 253", len(name))
	}
	if strings.HasSuffix(name, "-") {
		t.Errorf("name %q ends with hyphen", name)
	}
}
