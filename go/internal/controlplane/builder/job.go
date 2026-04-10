package builder

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/fsaintjacques/frostmap/go/api"
	"github.com/fsaintjacques/frostmap/go/internal/controlplane/volume"
)

// BuilderPodTemplate contains scheduling and identity overrides applied to
// every builder Job's pod spec. Configured via Helm values and passed to the
// control-plane as a JSON flag.
type BuilderPodTemplate struct {
	ServiceAccountName string              `json:"serviceAccountName,omitempty"`
	Tolerations        []corev1.Toleration `json:"tolerations,omitempty"`
	NodeSelector       map[string]string   `json:"nodeSelector,omitempty"`
	Affinity           *corev1.Affinity    `json:"affinity,omitempty"`
}

// Job implements Async by creating Kubernetes Jobs. Each build
// writes to a PVC that is finalized into a read-only PV on completion.
//
// Concurrency: Job is safe for concurrent use across different
// (dataset, versionID) pairs. Callers must serialize Start calls for the
// same (dataset, versionID) — the orchestrator guarantees this.
type Job struct {
	Client          kubernetes.Interface
	Volumes         volume.Manager
	Namespace       string
	Image           string            // frostmap container image (must contain both fm and fmtctl)
	ImagePullPolicy corev1.PullPolicy // defaults to IfNotPresent
	StorageClass    string            // StorageClass for build PVCs
	DiskSizeGB      int64             // PVC size in GiB (defaults to 10)
	PodTemplate     BuilderPodTemplate
}

func (b *Job) imagePullPolicy() corev1.PullPolicy {
	if b.ImagePullPolicy != "" {
		return b.ImagePullPolicy
	}
	return corev1.PullIfNotPresent
}

func (b *Job) diskSizeGB() int64 {
	if b.DiskSizeGB > 0 {
		return b.DiskSizeGB
	}
	return 10
}

// annotationPVName is the Job annotation key that records the finalized PV
// name. Its presence means FinalizeBuild already ran successfully, making
// Poll idempotent across retries.
const annotationPVName = "frostmap.io/pv-name"

// resourceName returns a deterministic, DNS-safe name for a build resource.
// Non-alphanumeric characters (except hyphens) are replaced with hyphens,
// consecutive hyphens are collapsed, and trailing hyphens are trimmed.
func resourceName(prefix, dataset, versionID string) string {
	name := fmt.Sprintf("%s-%s-%s", prefix, dataset, versionID)
	name = strings.ToLower(name)

	// Replace non-DNS characters with hyphens.
	var b strings.Builder
	prevHyphen := false
	for _, r := range name {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
			prevHyphen = false
		} else {
			if !prevHyphen {
				b.WriteByte('-')
			}
			prevHyphen = true
		}
	}
	name = b.String()

	// Truncate and trim trailing hyphens.
	if len(name) > 253 {
		name = name[:253]
	}
	name = strings.TrimRight(name, "-")

	return name
}

// JobName returns the deterministic Kubernetes Job name used by the Job
// builder for a (dataset, versionID) pair. Exported so reconcilers can
// patch ownerReferences without coupling to a Job builder instance.
func JobName(dataset, versionID string) string {
	return resourceName("fm-build", dataset, versionID)
}

// ConfigMapName returns the deterministic ConfigMap name used by the Job
// builder for a (dataset, versionID) pair.
func ConfigMapName(dataset, versionID string) string {
	return resourceName("fm-config", dataset, versionID)
}

// PVCName returns the deterministic PersistentVolumeClaim name used by the
// Job builder for a (dataset, versionID) pair.
func PVCName(dataset, versionID string) string {
	return resourceName("fm-pvc", dataset, versionID)
}

func (b *Job) jobName(dataset, versionID string) string { return JobName(dataset, versionID) }
func (b *Job) configMapName(dataset, versionID string) string {
	return ConfigMapName(dataset, versionID)
}
func (b *Job) pvcName(dataset, versionID string) string { return PVCName(dataset, versionID) }

// Start creates a PVC, ConfigMap, and Job for the build. Idempotent: if the
// resources already exist, returns the existing handle.
func (b *Job) Start(ctx context.Context, spec api.DatasetSpec, versionID string) (Handle, error) {
	dataset := spec.Name
	jobName := b.jobName(dataset, versionID)
	cmName := b.configMapName(dataset, versionID)
	pvcName := b.pvcName(dataset, versionID)
	handle := Handle(jobName)

	// 1. Create PVC for build output.
	if err := b.Volumes.CreateBuildPVC(ctx, pvcName, b.StorageClass, b.diskSizeGB()); err != nil {
		return "", fmt.Errorf("job builder: create PVC %q: %w", pvcName, err)
	}

	// 2. Create ConfigMap with worker.json.
	wc := workerConfig{
		Source:     spec.Source,
		Output:     "/output",
		Partitions: spec.ShardCount,
	}
	configBytes, err := json.Marshal(wc)
	if err != nil {
		return "", fmt.Errorf("job builder: marshal config: %w", err)
	}

	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cmName,
			Namespace: b.Namespace,
		},
		Data: map[string]string{
			workerConfigFile: string(configBytes),
		},
	}
	if _, err := b.Client.CoreV1().ConfigMaps(b.Namespace).Create(ctx, cm, metav1.CreateOptions{}); err != nil && !errors.IsAlreadyExists(err) {
		return "", fmt.Errorf("job builder: create ConfigMap %q: %w", cmName, err)
	}

	// 3. Create Job.
	backoffLimit := int32(0)
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName,
			Namespace: b.Namespace,
			Labels: map[string]string{
				"app.kubernetes.io/managed-by": "frostmap",
				"frostmap.io/dataset":          dataset,
				"frostmap.io/version":          versionID,
			},
		},
		Spec: batchv1.JobSpec{
			BackoffLimit: &backoffLimit,
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					ServiceAccountName: b.PodTemplate.ServiceAccountName,
					Tolerations:        b.PodTemplate.Tolerations,
					NodeSelector:       b.PodTemplate.NodeSelector,
					Affinity:           b.PodTemplate.Affinity,
					RestartPolicy:      corev1.RestartPolicyNever,
					SecurityContext: &corev1.PodSecurityContext{
						// distroless runs as nonroot (65534); fsGroup grants
						// group-write on PVC mounts without running as root.
						FSGroup: ptrInt64(65534),
					},
					Containers: []corev1.Container{
						{
							Name:            "fm",
							Image:           b.Image,
							ImagePullPolicy: b.imagePullPolicy(),
							Command:         []string{"fmtctl", "job", "--config", "/config/" + workerConfigFile},
							Resources:       builderResources(spec.BuilderResources),
							VolumeMounts: []corev1.VolumeMount{
								{Name: "output", MountPath: "/output"},
								{Name: "config", MountPath: "/config", ReadOnly: true},
							},
						},
					},
					Volumes: []corev1.Volume{
						{
							Name: "output",
							VolumeSource: corev1.VolumeSource{
								PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
									ClaimName: pvcName,
								},
							},
						},
						{
							Name: "config",
							VolumeSource: corev1.VolumeSource{
								ConfigMap: &corev1.ConfigMapVolumeSource{
									LocalObjectReference: corev1.LocalObjectReference{Name: cmName},
								},
							},
						},
					},
				},
			},
		},
	}

	if _, err := b.Client.BatchV1().Jobs(b.Namespace).Create(ctx, job, metav1.CreateOptions{}); err != nil && !errors.IsAlreadyExists(err) {
		return "", fmt.Errorf("job builder: create Job %q: %w", jobName, err)
	}

	return handle, nil
}

// Poll checks the Job status. On first completion detection, it calls
// volume.Manager.FinalizeBuild and records the PV name as a Job annotation so
// that subsequent calls are idempotent.
func (b *Job) Poll(ctx context.Context, handle Handle) (Status, error) {
	jobName := string(handle)

	job, err := b.Client.BatchV1().Jobs(b.Namespace).Get(ctx, jobName, metav1.GetOptions{})
	if errors.IsNotFound(err) {
		return Status{Phase: NotFound}, nil
	}
	if err != nil {
		return Status{}, fmt.Errorf("job builder: get Job %q: %w", jobName, err)
	}

	for _, c := range job.Status.Conditions {
		if c.Type == batchv1.JobComplete && c.Status == corev1.ConditionTrue {
			// Check if finalization already ran (idempotency).
			if pvName, ok := job.Annotations[annotationPVName]; ok {
				return Status{
					Phase:  Complete,
					Result: Result{PVName: pvName},
				}, nil
			}

			// First completion: finalize the build.
			// Delete the Job's pods first — the pvc-protection
			// finalizer blocks PVC deletion while any pod (even
			// completed) references the claim.
			b.deleteJobPods(ctx, jobName)

			dataset := job.Labels["frostmap.io/dataset"]
			versionID := job.Labels["frostmap.io/version"]
			pvcName := b.pvcName(dataset, versionID)

			pvName, err := b.Volumes.FinalizeBuild(ctx, pvcName)
			if err != nil {
				return Status{
					Phase: Failed,
					Error: fmt.Sprintf("finalize build: %v", err),
				}, nil
			}

			// Record PV name on the Job so retries skip FinalizeBuild.
			if job.Annotations == nil {
				job.Annotations = make(map[string]string)
			}
			job.Annotations[annotationPVName] = pvName
			if _, err := b.Client.BatchV1().Jobs(b.Namespace).Update(ctx, job, metav1.UpdateOptions{}); err != nil {
				// Annotation failure is not fatal — the PV is finalized.
				// Worst case: next Poll re-runs FinalizeBuild which will
				// fail (PVC gone) and mark the build as failed. Log and
				// return success anyway.
				return Status{
					Phase:  Complete,
					Result: Result{PVName: pvName},
				}, nil
			}

			return Status{
				Phase:  Complete,
				Result: Result{PVName: pvName},
			}, nil
		}
		if c.Type == batchv1.JobFailed && c.Status == corev1.ConditionTrue {
			reason := c.Message
			if reason == "" {
				reason = c.Reason
			}
			if reason == "" {
				reason = "job failed"
			}
			return Status{Phase: Failed, Error: reason}, nil
		}
	}

	return Status{Phase: Running}, nil
}

// Cancel deletes the Job, ConfigMap, and PVC. All operations are best-effort
// and idempotent.
func (b *Job) Cancel(ctx context.Context, handle Handle) error {
	jobName := string(handle)

	// Extract dataset/versionID from the Job to derive resource names.
	// If the Job is already gone, parse from the handle name.
	var dataset, versionID string
	job, err := b.Client.BatchV1().Jobs(b.Namespace).Get(ctx, jobName, metav1.GetOptions{})
	if err == nil {
		dataset = job.Labels["frostmap.io/dataset"]
		versionID = job.Labels["frostmap.io/version"]
	}

	// Delete pods first so the pvc-protection finalizer releases the PVC.
	b.deleteJobPods(ctx, jobName)

	// Delete Job with background propagation.
	propagation := metav1.DeletePropagationBackground
	deleteOpts := metav1.DeleteOptions{PropagationPolicy: &propagation}
	if err := b.Client.BatchV1().Jobs(b.Namespace).Delete(ctx, jobName, deleteOpts); err != nil && !errors.IsNotFound(err) {
		return fmt.Errorf("job builder: delete Job %q: %w", jobName, err)
	}

	// Clean up ConfigMap and PVC if we know the names.
	if dataset != "" && versionID != "" {
		cmName := b.configMapName(dataset, versionID)
		if err := b.Client.CoreV1().ConfigMaps(b.Namespace).Delete(ctx, cmName, metav1.DeleteOptions{}); err != nil && !errors.IsNotFound(err) {
			return fmt.Errorf("job builder: delete ConfigMap %q: %w", cmName, err)
		}

		pvcName := b.pvcName(dataset, versionID)
		if err := b.Client.CoreV1().PersistentVolumeClaims(b.Namespace).Delete(ctx, pvcName, metav1.DeleteOptions{}); err != nil && !errors.IsNotFound(err) {
			return fmt.Errorf("job builder: delete PVC %q: %w", pvcName, err)
		}
	}

	return nil
}

// deleteJobPods deletes all pods owned by the given Job. Best-effort: errors
// are ignored because the only purpose is to unblock the pvc-protection
// finalizer before FinalizeBuild deletes the PVC.
func (b *Job) deleteJobPods(ctx context.Context, jobName string) {
	pods, err := b.Client.CoreV1().Pods(b.Namespace).List(ctx, metav1.ListOptions{
		LabelSelector: "job-name=" + jobName,
	})
	if err != nil {
		return
	}
	for _, p := range pods.Items {
		b.Client.CoreV1().Pods(b.Namespace).Delete(ctx, p.Name, metav1.DeleteOptions{})
	}
}

// builderResources converts per-dataset resource overrides to a K8s
// ResourceRequirements. Returns an empty struct (no constraints) when br is nil.
func builderResources(br *api.BuilderResources) corev1.ResourceRequirements {
	if br == nil {
		return corev1.ResourceRequirements{}
	}
	rr := corev1.ResourceRequirements{}
	if br.CPURequest != "" || br.MemoryRequest != "" {
		rr.Requests = corev1.ResourceList{}
		if br.CPURequest != "" {
			rr.Requests[corev1.ResourceCPU] = resource.MustParse(br.CPURequest)
		}
		if br.MemoryRequest != "" {
			rr.Requests[corev1.ResourceMemory] = resource.MustParse(br.MemoryRequest)
		}
	}
	if br.CPULimit != "" || br.MemoryLimit != "" {
		rr.Limits = corev1.ResourceList{}
		if br.CPULimit != "" {
			rr.Limits[corev1.ResourceCPU] = resource.MustParse(br.CPULimit)
		}
		if br.MemoryLimit != "" {
			rr.Limits[corev1.ResourceMemory] = resource.MustParse(br.MemoryLimit)
		}
	}
	return rr
}

func ptrInt64(v int64) *int64 { return &v }
