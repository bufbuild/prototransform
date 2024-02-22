// Copyright 2023-2024 Buf Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package prototransform

import (
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

// Resolver is used to resolve symbol names and numbers into schema definitions.
type Resolver interface {
	protoregistry.ExtensionTypeResolver
	protoregistry.MessageTypeResolver

	// FindEnumByName looks up an enum by its full name.
	// E.g., "google.protobuf.Field.Kind".
	//
	// This returns (nil, NotFound) if not found.
	FindEnumByName(enum protoreflect.FullName) (protoreflect.EnumType, error)
}

type resolver struct {
	*protoregistry.Files
	*protoregistry.Types
}

// newResolver creates a new Resolver.
//
// If the input slice is empty, this returns nil
// The given FileDescriptors must be self-contained, that is they must contain all imports.
// This can NOT be guaranteed for FileDescriptorSets given over the wire, and can only be guaranteed from builds.
func newResolver(fileDescriptors *descriptorpb.FileDescriptorSet) (*resolver, error) {
	var result resolver
	// TODO(TCN-925): maybe should reparse unrecognized fields in fileDescriptors after creating resolver?
	if len(fileDescriptors.File) == 0 {
		return &result, nil
	}
	files, err := protodesc.FileOptions{AllowUnresolvable: true}.NewFiles(fileDescriptors)
	if err != nil {
		return nil, err
	}
	result.Files = files
	result.Types = &protoregistry.Types{}
	var rangeErr error
	files.RangeFiles(func(fileDescriptor protoreflect.FileDescriptor) bool {
		if err := registerTypes(result.Types, fileDescriptor); err != nil {
			rangeErr = err
			return false
		}
		return true
	})
	if rangeErr != nil {
		return nil, rangeErr
	}
	return &result, nil
}

type typeContainer interface {
	Enums() protoreflect.EnumDescriptors
	Messages() protoreflect.MessageDescriptors
	Extensions() protoreflect.ExtensionDescriptors
}

func registerTypes(types *protoregistry.Types, container typeContainer) error {
	for i := 0; i < container.Enums().Len(); i++ {
		if err := types.RegisterEnum(dynamicpb.NewEnumType(container.Enums().Get(i))); err != nil {
			return err
		}
	}
	for i := 0; i < container.Messages().Len(); i++ {
		msg := container.Messages().Get(i)
		if err := types.RegisterMessage(dynamicpb.NewMessageType(msg)); err != nil {
			return err
		}
		if err := registerTypes(types, msg); err != nil {
			return err
		}
	}
	for i := 0; i < container.Extensions().Len(); i++ {
		if err := types.RegisterExtension(dynamicpb.NewExtensionType(container.Extensions().Get(i))); err != nil {
			return err
		}
	}
	return nil
}
