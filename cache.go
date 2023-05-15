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
	"context"
	"time"

	reflectv1beta1 "buf.build/gen/go/bufbuild/reflect/protocolbuffers/go/buf/reflect/v1beta1"
	prototransformv1alpha1 "github.com/bufbuild/prototransform/internal/proto/gen/buf/prototransform/v1alpha1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Cache can be implemented and supplied to prototransform for added
// guarantees in environments where uptime is critical. If present and the API
// call to retrieve a schema fails, the schema will instead be loaded from this
// cache. Whenever a new schema is downloaded from the BSR, it will be saved to
// the cache. Cache can be used from multiple goroutines and thus must be
// thread-safe.
type Cache interface {
	Load(ctx context.Context, key string) ([]byte, error)
	Save(ctx context.Context, key string, data []byte) error
}

func encodeForCache(resp *reflectv1beta1.GetFileDescriptorSetResponse, respTime time.Time) ([]byte, error) {
	entry := &prototransformv1alpha1.CacheEntry{
		Response:     resp,
		ResponseTime: timestamppb.New(respTime),
	}
	return proto.Marshal(entry)
}

func decodeForCache(data []byte) (*reflectv1beta1.GetFileDescriptorSetResponse, time.Time, error) {
	var entry prototransformv1alpha1.CacheEntry
	if err := proto.Unmarshal(data, &entry); err != nil {
		return nil, time.Time{}, err
	}
	return entry.Response, entry.ResponseTime.AsTime(), nil
}
