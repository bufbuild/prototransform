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
	"testing"

	foov1 "github.com/bufbuild/prototransform/internal/testdata/gen/foo/v1"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

func TestRedact(t *testing.T) {
	t.Parallel()
	t.Run("RedactedMessage", func(t *testing.T) {
		t.Parallel()
		input := &foov1.RedactedMessage{
			Name: "sensitiveInformation",
		}
		got := Redact(removeSensitiveData())(input.ProtoReflect())
		want := &foov1.RedactedMessage{}
		assert.True(t, proto.Equal(want, got.Interface()))
	})
	t.Run("RedactedMessageField", func(t *testing.T) {
		t.Parallel()
		input := &foov1.RedactedMessageField{
			Name: &foov1.RedactedMessage{
				Name: "sensitiveInformation",
			},
		}
		got := Redact(removeSensitiveData())(input.ProtoReflect())
		want := &foov1.RedactedMessageField{
			Name: &foov1.RedactedMessage{},
		}
		assert.True(t, proto.Equal(want, got.Interface()))
	})
	t.Run("RedactedRepeatedField", func(t *testing.T) {
		t.Parallel()
		input := &foov1.RedactedRepeatedField{
			Name: []string{"sensitiveInformation"},
		}
		got := Redact(removeSensitiveData())(input.ProtoReflect())
		want := &foov1.RedactedRepeatedField{}
		assert.True(t, proto.Equal(want, got.Interface()))
	})
	t.Run("RedactedMap", func(t *testing.T) {
		t.Parallel()
		input := &foov1.RedactedMap{
			Name: map[string]*foov1.RedactedMessage{
				"foo": {
					Name: "sensitiveInformation",
				},
			},
		}
		got := Redact(removeSensitiveData())(input.ProtoReflect())
		want := &foov1.RedactedMap{
			Name: map[string]*foov1.RedactedMessage{
				"foo": {},
			},
		}
		assert.True(t, proto.Equal(want, got.Interface()))
	})
	t.Run("RedactedOneOf", func(t *testing.T) {
		t.Parallel()
		input := &foov1.RedactedOneOf{
			OneofField: &foov1.RedactedOneOf_Foo1{Foo1: 64},
		}
		got := Redact(removeSensitiveData())(input.ProtoReflect())
		want := &foov1.RedactedOneOf{}
		assert.True(t, proto.Equal(want, got.Interface()))
	})
	t.Run("RedactedEnum", func(t *testing.T) {
		t.Parallel()
		input := &foov1.RedactedEnum{
			Name: foov1.Enum_ENUM_FIRST,
		}
		got := Redact(removeSensitiveData())(input.ProtoReflect())
		want := &foov1.RedactedEnum{}
		assert.True(t, proto.Equal(want, got.Interface()))
	})
	t.Run("RedactedRepeatedEnum", func(t *testing.T) {
		t.Parallel()
		input := &foov1.RedactedRepeatedEnum{
			Name: []foov1.Enum{
				foov1.Enum_ENUM_FIRST,
				foov1.Enum_ENUM_SECOND,
				foov1.Enum_ENUM_THIRD,
			},
		}
		got := Redact(removeSensitiveData())(input.ProtoReflect())
		want := &foov1.RedactedRepeatedEnum{}
		assert.True(t, proto.Equal(want, got.Interface()))
	})
	t.Run("NotRedactedField", func(t *testing.T) {
		t.Parallel()
		input := &foov1.NotRedactedField{
			Name: "NotSensitiveData",
		}
		got := Redact(removeSensitiveData())(input.ProtoReflect())
		assert.True(t, proto.Equal(input, got.Interface()))
	})
}

func removeSensitiveData() func(in protoreflect.FieldDescriptor) bool {
	return func(in protoreflect.FieldDescriptor) bool {
		isSensitive, ok := proto.GetExtension(in.Options(), foov1.E_Sensitive).(bool)
		if !ok {
			return false
		}
		return isSensitive
	}
}
