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
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoregistry"
)

// Resolver is a Resolver.
//
// This is only needed in cases where extensions may be present.
type Resolver interface {
	protoregistry.ExtensionTypeResolver
	protoregistry.MessageTypeResolver
}

// InputFormat provides the interface to supply the [Converter] with an input
// composition. The format provided must accept a [Resolver]. Includes
// WithResolver() method to return the [Unmarshaler] with [Resolver] Supplied.
type InputFormat interface {
	WithResolver(Resolver) Unmarshaler
}

// Unmarshaler is a [Unmarshaler].
type Unmarshaler interface {
	Unmarshal([]byte, proto.Message) error
}

// OutputFormat provides the interface to supply the [Converter] with an output
// composition. The format provided must accept a [Resolver]. Includes
// WithResolver() method to return the [Marshaler] with [Resolver] Supplied.
type OutputFormat interface {
	WithResolver(Resolver) Marshaler
}

// Marshaler is a [Marshaler].
type Marshaler interface {
	Marshal(proto.Message) ([]byte, error)
}

type binaryInputFormat struct {
	proto.UnmarshalOptions
}

// BinaryInputFormat convenience method for binary input format.
func BinaryInputFormat(in proto.UnmarshalOptions) InputFormat {
	return &binaryInputFormat{
		UnmarshalOptions: in,
	}
}

// WithResolver to supply binary input format with a prebuilt types [Resolver].
func (x binaryInputFormat) WithResolver(in Resolver) Unmarshaler {
	x.Resolver = in
	return x
}

// BinaryOutputFormat convenience method for binary output format.
func BinaryOutputFormat(in proto.MarshalOptions) OutputFormat {
	return outputFormatWithoutResolver{Marshaler: in}
}

type jSONInputFormat struct {
	protojson.UnmarshalOptions
}

// JSONInputFormat convenience method for JSON input format.
func JSONInputFormat(in protojson.UnmarshalOptions) InputFormat {
	return jSONInputFormat{
		UnmarshalOptions: in,
	}
}

// WithResolver to supply j s o n input format with a prebuilt types [Resolver].
func (x jSONInputFormat) WithResolver(in Resolver) Unmarshaler {
	x.Resolver = in
	return x
}

type jSONOutputFormat struct {
	protojson.MarshalOptions
}

// JSONOutputFormat convenience method for JSON output format.
func JSONOutputFormat(in protojson.MarshalOptions) OutputFormat {
	return jSONOutputFormat{
		MarshalOptions: in,
	}
}

// WithResolver to supply j s o n output format with a prebuilt types [Resolver].
func (x jSONOutputFormat) WithResolver(in Resolver) Marshaler {
	x.Resolver = in
	return x
}

type textInputFormat struct {
	prototext.UnmarshalOptions
}

// TextInputFormat convenience method for text input format.
func TextInputFormat(in prototext.UnmarshalOptions) InputFormat {
	return textInputFormat{
		UnmarshalOptions: in,
	}
}

// WithResolver to supply text input format with a prebuilt types [Resolver].
func (x textInputFormat) WithResolver(in Resolver) Unmarshaler {
	x.Resolver = in
	return x
}

type textOutputFormat struct {
	prototext.MarshalOptions
}

// TextOutputFormat convenience method for text output format.
func TextOutputFormat(in prototext.MarshalOptions) OutputFormat {
	return textOutputFormat{
		MarshalOptions: in,
	}
}

// WithResolver to supply text output format with a prebuilt types [Resolver].
func (x textOutputFormat) WithResolver(in Resolver) Marshaler {
	x.Resolver = in
	return x
}

type inputFormatWithoutResolver struct {
	Unmarshaler
}

// InputFormatWithoutResolver convenience method for input format without resolver.
func InputFormatWithoutResolver(in Unmarshaler) InputFormat {
	return inputFormatWithoutResolver{
		Unmarshaler: in,
	}
}

// WithResolver to supply input format without resolver.
func (x inputFormatWithoutResolver) WithResolver(_ Resolver) Unmarshaler {
	return x
}

type outputFormatWithoutResolver struct {
	Marshaler
}

// OutputFormatWithoutResolver convenience method for output format without resolver.
func OutputFormatWithoutResolver(in Marshaler) OutputFormat {
	return outputFormatWithoutResolver{
		Marshaler: in,
	}
}

// WithResolver to supply output format without resolver.
func (x outputFormatWithoutResolver) WithResolver(_ Resolver) Marshaler {
	return x
}
