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
	"testing"
	"time"

	prototransformv1alpha1 "github.com/bufbuild/prototransform/internal/proto/gen/buf/prototransform/v1alpha1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestCacheEntryRoundTrip(t *testing.T) {
	t.Parallel()
	descriptors := &descriptorpb.FileDescriptorSet{
		File: []*descriptorpb.FileDescriptorProto{
			{
				Name:    proto.String("test.proto"),
				Package: proto.String("test"),
			},
			{
				Name:       proto.String("foo.proto"),
				Package:    proto.String("foo"),
				Dependency: []string{"test.proto"},
				MessageType: []*descriptorpb.DescriptorProto{
					{
						Name: proto.String("Foo"),
					},
				},
			},
		},
	}
	version := "abcdefg"
	respTime := time.Date(2023, time.January, 1, 12, 0, 0, 0, time.UTC)
	data, err := encodeForCache("123", []string{"a1", "b2", "c3"}, descriptors, version, respTime)
	require.NoError(t, err)
	entry, err := decodeForCache(data)
	require.NoError(t, err)
	expectedEntry := &prototransformv1alpha1.CacheEntry{
		Schema: &prototransformv1alpha1.Schema{
			Descriptors: descriptors,
			Version:     version,
		},
		Id:              "123",
		IncludedSymbols: []string{"a1", "b2", "c3"},
		SchemaTimestamp: timestamppb.New(respTime),
	}
	assert.True(t, proto.Equal(expectedEntry, entry))
}

func TestIsSuperSet(t *testing.T) {
	t.Parallel()
	t.Run("both empty", func(t *testing.T) {
		t.Parallel()
		assert.True(t, isSuperSet(nil, nil))
		assert.True(t, isSuperSet([]string{}, nil))
		assert.True(t, isSuperSet(nil, make([]string, 0, 10)))
	})
	t.Run("superset is empty", func(t *testing.T) {
		t.Parallel()
		assert.False(t, isSuperSet(nil, []string{"abc"}))
	})
	t.Run("subset is empty", func(t *testing.T) {
		t.Parallel()
		assert.False(t, isSuperSet([]string{"abc"}, nil))
	})
	t.Run("same", func(t *testing.T) {
		t.Parallel()
		assert.True(t, isSuperSet([]string{"abc", "def", "ghi", "xyz"}, []string{"abc", "def", "ghi", "xyz"}))
	})
	t.Run("is superset (1)", func(t *testing.T) {
		t.Parallel()
		assert.True(t, isSuperSet([]string{"abc", "def", "ghi", "xyz"}, []string{"abc"}))
	})
	t.Run("is superset (2)", func(t *testing.T) {
		t.Parallel()
		assert.True(t, isSuperSet([]string{"abc", "def", "ghi", "xyz"}, []string{"abc", "ghi"}))
	})
	t.Run("is superset (3)", func(t *testing.T) {
		t.Parallel()
		assert.True(t, isSuperSet([]string{"abc", "def", "ghi", "xyz"}, []string{"abc", "xyz"}))
	})
	t.Run("is superset (4)", func(t *testing.T) {
		t.Parallel()
		assert.True(t, isSuperSet([]string{"abc", "def", "ghi", "xyz"}, []string{"xyz"}))
	})
	t.Run("is subset (1)", func(t *testing.T) {
		t.Parallel()
		assert.False(t, isSuperSet([]string{"abc"}, []string{"abc", "def", "ghi", "xyz"}))
	})
	t.Run("is subset (2)", func(t *testing.T) {
		t.Parallel()
		assert.False(t, isSuperSet([]string{"abc", "ghi"}, []string{"abc", "def", "ghi", "xyz"}))
	})
	t.Run("is subset (3)", func(t *testing.T) {
		t.Parallel()
		assert.False(t, isSuperSet([]string{"abc", "xyz"}, []string{"abc", "def", "ghi", "xyz"}))
	})
	t.Run("is subset (4)", func(t *testing.T) {
		t.Parallel()
		assert.False(t, isSuperSet([]string{"xyz"}, []string{"abc", "def", "ghi", "xyz"}))
	})
}
