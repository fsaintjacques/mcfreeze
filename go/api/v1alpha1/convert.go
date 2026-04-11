package v1alpha1

import (
	"fmt"

	"github.com/fsaintjacques/frostmap/go/api"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

// VersionCRName returns the deterministic Kubernetes resource name for a
// DatasetVersion CR. It is unique per (dataset, versionID) pair and stable
// across reconciles.
func VersionCRName(dataset, versionID string) string {
	return fmt.Sprintf("%s-%s", dataset, versionID)
}

// DatasetLabel is the label key set on DatasetVersion CRs to identify
// the parent dataset for label-selector queries.
const DatasetLabel = "frostmap.dev/dataset"

// ToAPIDatasetSpec converts a Dataset CR to the canonical api.DatasetSpec.
func ToAPIDatasetSpec(cr *Dataset) api.DatasetSpec {
	spec := api.DatasetSpec{
		Name:       cr.Name,
		KeyPrefix:  cr.Spec.KeyPrefix,
		Source:     toAPISourceSpec(cr.Spec.Source),
		ShardCount: cr.Spec.ShardCount,
		Retention:  cr.Spec.Retention,
	}
	if s := cr.Spec.Storage; s != nil {
		spec.Storage = &api.StorageSpec{
			DiskSizeGB:            s.DiskSizeGB,
			ProvisionedThroughput: s.ProvisionedThroughput,
		}
	}
	if r := cr.Spec.BuilderResources; r != nil {
		spec.BuilderResources = toAPIBuilderResources(r)
	}
	return spec
}

// FromAPIDatasetSpec builds a Dataset CR from an api.DatasetSpec. The CR's
// metadata.name is set to spec.Name; namespace is the caller's responsibility.
func FromAPIDatasetSpec(spec api.DatasetSpec) *Dataset {
	ds := &Dataset{
		Spec: DatasetSpec{
			KeyPrefix:  spec.KeyPrefix,
			Source:     fromAPISourceSpec(spec.Source),
			ShardCount: spec.ShardCount,
			Retention:  spec.Retention,
		},
	}
	if s := spec.Storage; s != nil {
		ds.Spec.Storage = &StorageSpec{
			DiskSizeGB:            s.DiskSizeGB,
			ProvisionedThroughput: s.ProvisionedThroughput,
		}
	}
	if r := spec.BuilderResources; r != nil {
		ds.Spec.BuilderResources = fromAPIBuilderResources(r)
	}
	return ds
}

func toAPISourceSpec(s SourceSpec) api.SourceSpec {
	out := api.SourceSpec{
		KeyColumn:   s.KeyColumn,
		ValueColumn: s.ValueColumn,
	}
	if s.Encoding != nil && s.Encoding.Protobuf != nil {
		out.Encoding = &api.EncodingSpec{
			Protobuf: &api.ProtobufEncoding{
				Descriptor:    s.Encoding.Protobuf.Descriptor,
				DescriptorURI: s.Encoding.Protobuf.DescriptorURI,
				Package:       s.Encoding.Protobuf.Package,
				MessageName:   s.Encoding.Protobuf.MessageName,
			},
		}
	}
	if s.BigQuery != nil {
		out.BigQuery = &api.BigQuerySource{
			Project:        s.BigQuery.Project,
			Table:          s.BigQuery.Table,
			SelectedFields: append([]string(nil), s.BigQuery.SelectedFields...),
			RowRestriction: s.BigQuery.RowRestriction,
		}
	}
	if s.CSV != nil {
		out.CSV = &api.CsvSource{
			Data: s.CSV.Data,
			Path: s.CSV.Path,
		}
	}
	return out
}

func toAPIBuilderResources(r *corev1.ResourceRequirements) *api.BuilderResources {
	br := &api.BuilderResources{}
	if q, ok := r.Requests[corev1.ResourceCPU]; ok {
		br.CPURequest = q.String()
	}
	if q, ok := r.Requests[corev1.ResourceMemory]; ok {
		br.MemoryRequest = q.String()
	}
	if q, ok := r.Limits[corev1.ResourceCPU]; ok {
		br.CPULimit = q.String()
	}
	if q, ok := r.Limits[corev1.ResourceMemory]; ok {
		br.MemoryLimit = q.String()
	}
	return br
}

func fromAPIBuilderResources(r *api.BuilderResources) *corev1.ResourceRequirements {
	rr := &corev1.ResourceRequirements{}
	if r.CPURequest != "" || r.MemoryRequest != "" {
		rr.Requests = corev1.ResourceList{}
		if r.CPURequest != "" {
			rr.Requests[corev1.ResourceCPU] = resource.MustParse(r.CPURequest)
		}
		if r.MemoryRequest != "" {
			rr.Requests[corev1.ResourceMemory] = resource.MustParse(r.MemoryRequest)
		}
	}
	if r.CPULimit != "" || r.MemoryLimit != "" {
		rr.Limits = corev1.ResourceList{}
		if r.CPULimit != "" {
			rr.Limits[corev1.ResourceCPU] = resource.MustParse(r.CPULimit)
		}
		if r.MemoryLimit != "" {
			rr.Limits[corev1.ResourceMemory] = resource.MustParse(r.MemoryLimit)
		}
	}
	return rr
}

func fromAPISourceSpec(s api.SourceSpec) SourceSpec {
	out := SourceSpec{
		KeyColumn:   s.KeyColumn,
		ValueColumn: s.ValueColumn,
	}
	if s.Encoding != nil && s.Encoding.Protobuf != nil {
		out.Encoding = &EncodingSpec{
			Protobuf: &ProtobufEncoding{
				Descriptor:    s.Encoding.Protobuf.Descriptor,
				DescriptorURI: s.Encoding.Protobuf.DescriptorURI,
				Package:       s.Encoding.Protobuf.Package,
				MessageName:   s.Encoding.Protobuf.MessageName,
			},
		}
	}
	if s.BigQuery != nil {
		out.BigQuery = &BigQuerySource{
			Project:        s.BigQuery.Project,
			Table:          s.BigQuery.Table,
			SelectedFields: append([]string(nil), s.BigQuery.SelectedFields...),
			RowRestriction: s.BigQuery.RowRestriction,
		}
	}
	if s.CSV != nil {
		out.CSV = &CsvSource{
			Data: s.CSV.Data,
			Path: s.CSV.Path,
		}
	}
	return out
}
