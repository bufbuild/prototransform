// Copyright 2023-2025 Buf Technologies, Inc.
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
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
)

// Filters is a slice of filters. When there is more than one element, they
// are applied in order. In other words, the first filter is evaluated first.
// The result of that is then provided as input to the second, and so on.
type Filters []Filter

func (f Filters) do(message protoreflect.Message) {
	for _, filter := range f {
		filter(message)
	}
}

// Filter provides a way for user-provided logic to alter the message being converted. It can
// return a derived message (which could even be a different type), or it can mutate the given
// message and return it.
type Filter func(protoreflect.Message) protoreflect.Message

// Redact returns a Filter that will remove information from a message. It invokes
// the given predicate for each field in the message (including in any nested
// messages) and _removes_ the field and corresponding value if the predicate
// returns true. This can be used to remove sensitive data from a message, for example.
func Redact(predicate func(protoreflect.FieldDescriptor) bool) Filter {
	return func(msg protoreflect.Message) protoreflect.Message {
		redactMessage(msg, predicate)
		return msg
	}
}

// HasDebugRedactOption returns a function that can be used as a predicate, with
// [Redact], to omit fields where the `debug_redact` field option is set to true.
//
//	message UserDetails {
//	  int64 user_id = 1;
//	  string name = 2;
//	  string email = 4;
//	  string ssn = 3 [debug_redact=true]; // social security number is sensitive
//	}
func HasDebugRedactOption(fd protoreflect.FieldDescriptor) bool {
	opts, ok := fd.Options().(*descriptorpb.FieldOptions)
	return ok && opts.GetDebugRedact()
}

func redactMessage(message protoreflect.Message, redaction func(protoreflect.FieldDescriptor) bool) {
	message.Range(
		func(descriptor protoreflect.FieldDescriptor, value protoreflect.Value) bool {
			if redaction(descriptor) {
				message.Clear(descriptor)
				return true
			}
			switch {
			case descriptor.IsMap() && isMessage(descriptor.MapValue()):
				redactMap(value, redaction)
			case descriptor.IsList() && isMessage(descriptor):
				redactList(value, redaction)
			case !descriptor.IsMap() && isMessage(descriptor):
				// isMessage by itself returns true for maps, since the type is
				// a synthetic map entry message, so we also need !IsMap in
				// this case's criteria.
				redactMessage(value.Message(), redaction)
			}
			return true
		},
	)
}

func redactList(value protoreflect.Value, redaction func(protoreflect.FieldDescriptor) bool) {
	for i := 0; i < value.List().Len(); i++ {
		redactMessage(value.List().Get(i).Message(), redaction)
	}
}

func redactMap(value protoreflect.Value, redaction func(protoreflect.FieldDescriptor) bool) {
	value.Map().Range(func(_ protoreflect.MapKey, mapValue protoreflect.Value) bool {
		redactMessage(mapValue.Message(), redaction)
		return true
	})
}

func isMessage(descriptor protoreflect.FieldDescriptor) bool {
	return descriptor.Kind() == protoreflect.MessageKind ||
		descriptor.Kind() == protoreflect.GroupKind
}
