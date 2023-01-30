// Copyright 2023 Buf Technologies, Inc.
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
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/dynamicpb"
)

func TestSchemaService_ConvertMessage(t *testing.T) {
	t.Parallel()
	// create schema for message to convert
	sourceFile := fakeFileDescriptorSet().File[0]

	// create test message
	fileDescriptor, err := protodesc.NewFile(sourceFile, nil)
	require.NoError(t, err)
	messageDescriptor := fileDescriptor.Messages().Get(0)
	message := dynamicpb.NewMessage(messageDescriptor)
	message.Set(messageDescriptor.Fields().ByNumber(1), protoreflect.ValueOfString("abcdef"))
	message.Set(messageDescriptor.Fields().ByNumber(2), protoreflect.ValueOfInt64(12345678))
	list := message.Mutable(messageDescriptor.Fields().ByNumber(3)).List()
	list.Append(protoreflect.ValueOfMessage(message.New()))
	list.Append(protoreflect.ValueOfMessage(message.New()))
	list.Append(protoreflect.ValueOfMessage(message.New()))
	message.Set(messageDescriptor.Fields().ByNumber(4), protoreflect.ValueOfEnum(3))

	formats := []struct {
		name         string
		outputFormat OutputFormat
		inputFormat  InputFormat
	}{
		{
			name:         "binary",
			outputFormat: BinaryOutputFormat(proto.MarshalOptions{}),
			inputFormat:  BinaryInputFormat(proto.UnmarshalOptions{}),
		},
		{
			name:         "json",
			outputFormat: JSONOutputFormat(protojson.MarshalOptions{}),
			inputFormat:  JSONInputFormat(protojson.UnmarshalOptions{}),
		},
		{
			name:         "text",
			outputFormat: TextOutputFormat(prototext.MarshalOptions{}),
			inputFormat:  TextInputFormat(prototext.UnmarshalOptions{}),
		},
		{
			name:         "TextWithoutResolver",
			outputFormat: OutputFormatWithoutResolver(prototext.MarshalOptions{}),
			inputFormat:  InputFormatWithoutResolver(prototext.UnmarshalOptions{}),
		},
		{
			name:         "custom",
			outputFormat: marshalProtoJSONWithResolver{},
			inputFormat:  unmarshalProtoJSONWithResolver{},
		},
	}

	resolver := &protoregistry.Types{}
	for i := 0; i < fileDescriptor.Messages().Len(); i++ {
		message := dynamicpb.NewMessageType(fileDescriptor.Messages().Get(i))
		require.NoError(t, resolver.RegisterMessage(message))
	}
	for i := 0; i < fileDescriptor.Enums().Len(); i++ {
		enum := dynamicpb.NewEnumType(fileDescriptor.Enums().Get(i))
		require.NoError(t, resolver.RegisterEnum(enum))
	}
	for i := 0; i < fileDescriptor.Extensions().Len(); i++ {
		extension := dynamicpb.NewExtensionType(fileDescriptor.Extensions().Get(i))
		require.NoError(t, resolver.RegisterExtension(extension))
	}

	for _, inFormat := range formats {
		inputFormat := inFormat
		for _, outFormat := range formats {
			outputFormat := outFormat
			t.Run(fmt.Sprintf("%v_to_%v", inputFormat.name, outputFormat.name), func(t *testing.T) {
				t.Parallel()
				data, err := inputFormat.outputFormat.WithResolver(nil).Marshal(message)
				require.NoError(t, err)

				converter := Converter{
					Resolver:     resolver,
					InputFormat:  inputFormat.inputFormat,
					OutputFormat: outputFormat.outputFormat,
				}
				require.NoError(t, err)
				resp, err := converter.ConvertMessage("foo.bar.Message", data)
				require.NoError(t, err)
				clone := message.New().Interface()
				err = outputFormat.inputFormat.WithResolver(nil).Unmarshal(resp, clone)
				require.NoError(t, err)
				diff := cmp.Diff(message, clone, protocmp.Transform())
				if diff != "" {
					t.Errorf("round-trip failure (-want +got):\n%s", diff)
				}
			})
		}
	}
}

type marshalProtoJSONWithResolver struct {
	protojson.MarshalOptions
}

func (p marshalProtoJSONWithResolver) WithResolver(r Resolver) Marshaler {
	return protojson.MarshalOptions{
		Resolver: r,
	}
}

type unmarshalProtoJSONWithResolver struct {
	protojson.UnmarshalOptions
}

func (p unmarshalProtoJSONWithResolver) WithResolver(r Resolver) Unmarshaler {
	return protojson.UnmarshalOptions{
		Resolver: r,
	}
}
