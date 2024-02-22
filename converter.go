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
	"fmt"

	"github.com/pkg/errors"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

// Converter allows callers to convert byte payloads from one format to another.
type Converter struct {
	// A custom [Resolver] can be supplied with the InputFormat [Unmarshaler] and
	// OutputFormat [Marshaler] for looking up types when expanding
	// google.protobuf.Any messages. As such, this is likely only needed in cases
	// where extensions may be present. For [proto], [protojson], and [prototext]
	// marshalers and unmarshalers are already handled so there is no need to
	// provide a WithResolver method. If nil, this defaults to using
	// protoregistry.GlobalTypes.
	Resolver Resolver
	// InputFormat handles unmarshaling bytes from the expected input format.
	// You can use a [proto.Unmarshaler], [protojson.Unmarshaler], or
	// [prototext.Unmarshaler] as a value for this field. You can also supply
	// your own custom format that implements the [Unmarshaler] interface. If
	// your custom format needs a [Resolver] (e.g. to resolve types in a
	// google.protobuf.Any message or to resolve extensions), then your custom
	// type should provide a method with the following signature:
	//     WithResolver(Resolver) Unmarshaler
	// This method should return a new unmarshaler that will make use of the
	// given resolver.
	InputFormat InputFormat
	// OutputFormat handles marshaling to bytes in the desired output format.
	// You can use a [proto.Marshaler], [protojson.Marshaler], or
	// [prototext.Marshaler] as a value for this field. You can also supply
	// your own custom format that implements the [Marshaler] interface. If
	// your custom format needs a [Resolver] (e.g. to format types in a
	// google.protobuf.Any message), then your custom type should provide
	// a method with the following signature:
	//     WithResolver(Resolver) Marshaler
	// This method should return a new marshaler that will make use of the
	// given resolver.
	OutputFormat OutputFormat
	// Filters are a set of user-supplied actions which will be performed on a
	// [ConvertMessage] call before the conversion takes place, meaning the
	// output value can be modified according to some set of rules.
	Filters Filters
}

// ConvertMessage allows the caller to convert a given message data blob from
// one format to another by referring to a type schema for the blob.
func (c *Converter) ConvertMessage(messageName string, inputData []byte) ([]byte, error) {
	md, err := c.Resolver.FindMessageByName(protoreflect.FullName(messageName))
	if err != nil {
		return nil, errors.Wrapf(err, "message_name '%s' is not found in proto", messageName)
	}
	msg := dynamicpb.NewMessage(md.Descriptor())
	if err := c.InputFormat.WithResolver(c.Resolver).Unmarshal(inputData, msg); err != nil {
		return nil, fmt.Errorf("input_data cannot be unmarshaled to %s in %s: %w", messageName, c.InputFormat, err)
	}

	// apply filters
	c.Filters.do(msg)

	data, err := c.OutputFormat.WithResolver(c.Resolver).Marshal(msg)
	if err != nil {
		return nil, errors.Wrapf(err, "input message cannot be unmarshaled to %s", c.InputFormat)
	}
	return data, nil
}
