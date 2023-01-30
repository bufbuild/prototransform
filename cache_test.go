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
	"time"

	reflectv1beta1 "buf.build/gen/go/bufbuild/reflect/protocolbuffers/go/buf/reflect/v1beta1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
)

func TestCacheEntryRoundTrip(t *testing.T) {
	resp := &reflectv1beta1.GetFileDescriptorSetResponse{
		FileDescriptorSet: &descriptorpb.FileDescriptorSet{
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
		},
		Version: "abcdefg",
	}
	respTime := time.Date(2023, time.January, 1, 12, 0, 0, 0, time.UTC)

	data, err := encodeForCache(resp, respTime)
	require.NoError(t, err)
	roundTripResp, roundTripRespTime, err := decodeForCache(data)
	require.NoError(t, err)
	assert.True(t, proto.Equal(roundTripResp, resp))
	assert.True(t, roundTripRespTime.Equal(respTime))
}
