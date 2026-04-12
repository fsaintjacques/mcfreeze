package ui

import (
	"encoding/base64"
	"fmt"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

// decodeProtobuf decodes raw value bytes using the given base64-encoded
// FileDescriptorSet and fully-qualified message name. Returns pretty-printed
// JSON.
func decodeProtobuf(value []byte, descriptorB64, messageName string) (string, error) {
	md, err := resolveMessage(descriptorB64, messageName)
	if err != nil {
		return "", err
	}

	msg := dynamicpb.NewMessage(md)
	if err := proto.Unmarshal(value, msg); err != nil {
		return "", fmt.Errorf("unmarshal protobuf: %w", err)
	}

	opts := protojson.MarshalOptions{
		Multiline: true,
		Indent:    "  ",
	}
	jsonBytes, err := opts.Marshal(msg)
	if err != nil {
		return "", fmt.Errorf("marshal to JSON: %w", err)
	}
	return string(jsonBytes), nil
}

func resolveMessage(descriptorB64, messageName string) (protoreflect.MessageDescriptor, error) {
	raw, err := base64.StdEncoding.DecodeString(descriptorB64)
	if err != nil {
		return nil, fmt.Errorf("base64 decode descriptor: %w", err)
	}

	var fds descriptorpb.FileDescriptorSet
	if err := proto.Unmarshal(raw, &fds); err != nil {
		return nil, fmt.Errorf("unmarshal FileDescriptorSet: %w", err)
	}

	files, err := protodesc.NewFiles(&fds)
	if err != nil {
		return nil, fmt.Errorf("build file registry: %w", err)
	}

	md, err := files.FindDescriptorByName(protoreflect.FullName(messageName))
	if err != nil {
		return nil, fmt.Errorf("find message %q: %w", messageName, err)
	}

	msgDesc, ok := md.(protoreflect.MessageDescriptor)
	if !ok {
		return nil, fmt.Errorf("%q is not a message descriptor", messageName)
	}
	return msgDesc, nil
}
