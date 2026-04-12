package ui

import (
	"encoding/base64"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
)

// buildTestDescriptor creates a minimal FileDescriptorSet with a single message
// containing the given fields.
func buildTestDescriptor(t *testing.T, pkg, msgName string, fields []*descriptorpb.FieldDescriptorProto) string {
	t.Helper()

	fqn := msgName
	if pkg != "" {
		fqn = pkg + "." + msgName
	}
	_ = fqn // used for the fully-qualified name in lookups

	fds := &descriptorpb.FileDescriptorSet{
		File: []*descriptorpb.FileDescriptorProto{
			{
				Name:    strPtr("test.proto"),
				Package: strPtr(pkg),
				Syntax:  strPtr("proto3"),
				MessageType: []*descriptorpb.DescriptorProto{
					{
						Name:  strPtr(msgName),
						Field: fields,
					},
				},
			},
		},
	}

	raw, err := proto.Marshal(fds)
	if err != nil {
		t.Fatalf("marshal FileDescriptorSet: %v", err)
	}
	return base64.StdEncoding.EncodeToString(raw)
}

func strPtr(s string) *string { return &s }
func int32Ptr(i int32) *int32 { return &i }

func TestDecodeProtobuf(t *testing.T) {
	// Build a descriptor for message "test.Person" with fields: name (string, tag 1), age (int32, tag 2).
	desc := buildTestDescriptor(t, "test", "Person", []*descriptorpb.FieldDescriptorProto{
		{
			Name:   strPtr("name"),
			Number: int32Ptr(1),
			Type:   descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
			Label:  descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
		},
		{
			Name:   strPtr("age"),
			Number: int32Ptr(2),
			Type:   descriptorpb.FieldDescriptorProto_TYPE_INT32.Enum(),
			Label:  descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
		},
	})

	// Manually encode a protobuf: field 1 (string) = "alice", field 2 (varint) = 30.
	// Tag 1, wire type 2 (length-delimited): 0x0a, length 5, "alice"
	// Tag 2, wire type 0 (varint): 0x10, value 30
	value := []byte{0x0a, 0x05, 'a', 'l', 'i', 'c', 'e', 0x10, 30}

	result, err := decodeProtobuf(value, desc, "test.Person")
	if err != nil {
		t.Fatalf("decodeProtobuf: %v", err)
	}

	if !strings.Contains(result, "alice") {
		t.Errorf("expected JSON to contain 'alice', got: %s", result)
	}
	if !strings.Contains(result, "30") {
		t.Errorf("expected JSON to contain '30', got: %s", result)
	}
}

func TestDecodeProtobuf_BadDescriptor(t *testing.T) {
	_, err := decodeProtobuf([]byte{}, "not-base64!", "test.Foo")
	if err == nil {
		t.Fatal("expected error for bad base64")
	}
}

func TestDecodeProtobuf_UnknownMessage(t *testing.T) {
	desc := buildTestDescriptor(t, "test", "Person", nil)
	_, err := decodeProtobuf([]byte{}, desc, "test.Unknown")
	if err == nil {
		t.Fatal("expected error for unknown message")
	}
}
