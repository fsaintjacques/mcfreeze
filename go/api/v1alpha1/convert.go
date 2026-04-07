package v1alpha1

import (
	"fmt"

	"github.com/fsaintjacques/frostmap/go/api"
)

// VersionCRName returns the deterministic Kubernetes resource name for a
// DatasetVersion CR. It is unique per (dataset, versionID) pair and stable
// across reconciles.
func VersionCRName(dataset, versionID string) string {
	return fmt.Sprintf("%s-%s", dataset, versionID)
}

// DatasetSpecLabel is the label key set on DatasetVersion CRs to identify
// the parent dataset for label-selector queries.
const DatasetLabel = "frostmap.dev/dataset"

// ToAPIDatasetSpec converts a Dataset CR to the canonical api.DatasetSpec.
func ToAPIDatasetSpec(cr *Dataset) api.DatasetSpec {
	return api.DatasetSpec{
		Name:       cr.Name,
		KeyPrefix:  cr.Spec.KeyPrefix,
		Source:     toAPISourceSpec(cr.Spec.Source),
		ShardCount: cr.Spec.ShardCount,
		Retention:  cr.Spec.Retention,
	}
}

// FromAPIDatasetSpec builds a Dataset CR from an api.DatasetSpec. The CR's
// metadata.name is set to spec.Name; namespace is the caller's responsibility.
func FromAPIDatasetSpec(spec api.DatasetSpec) *Dataset {
	return &Dataset{
		Spec: DatasetSpec{
			KeyPrefix:  spec.KeyPrefix,
			Source:     fromAPISourceSpec(spec.Source),
			ShardCount: spec.ShardCount,
			Retention:  spec.Retention,
		},
	}
}

func toAPISourceSpec(s SourceSpec) api.SourceSpec {
	out := api.SourceSpec{
		KeyColumn:   s.KeyColumn,
		ValueColumn: s.ValueColumn,
	}
	if s.Encoding != nil {
		enc := api.EncodingSpec{}
		if s.Encoding.Protobuf != nil {
			enc.Protobuf = &api.ProtobufEncoding{
				Descriptor:    s.Encoding.Protobuf.Descriptor,
				DescriptorURI: s.Encoding.Protobuf.DescriptorURI,
				Package:       s.Encoding.Protobuf.Package,
				MessageName:   s.Encoding.Protobuf.MessageName,
			}
		}
		out.Encoding = &enc
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

func fromAPISourceSpec(s api.SourceSpec) SourceSpec {
	out := SourceSpec{
		KeyColumn:   s.KeyColumn,
		ValueColumn: s.ValueColumn,
	}
	if s.Encoding != nil {
		enc := EncodingSpec{}
		if s.Encoding.Protobuf != nil {
			enc.Protobuf = &ProtobufEncoding{
				Descriptor:    s.Encoding.Protobuf.Descriptor,
				DescriptorURI: s.Encoding.Protobuf.DescriptorURI,
				Package:       s.Encoding.Protobuf.Package,
				MessageName:   s.Encoding.Protobuf.MessageName,
			}
		}
		out.Encoding = &enc
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
