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
	"context"
	"time"

	prototransformv1alpha1 "github.com/bufbuild/prototransform/internal/proto/gen/buf/prototransform/v1alpha1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
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

func encodeForCache(
	schemaID string,
	syms []string,
	descriptors *descriptorpb.FileDescriptorSet,
	version string,
	timestamp time.Time,
) ([]byte, error) {
	entry := &prototransformv1alpha1.CacheEntry{
		Schema: &prototransformv1alpha1.Schema{
			Descriptors: descriptors,
			Version:     version,
		},
		SchemaTimestamp: timestamppb.New(timestamp),
		Id:              schemaID,
		IncludedSymbols: syms,
	}
	return proto.Marshal(entry)
}

func decodeForCache(data []byte) (*prototransformv1alpha1.CacheEntry, error) {
	var entry prototransformv1alpha1.CacheEntry
	if err := proto.Unmarshal(data, &entry); err != nil {
		return nil, err
	}
	return &entry, nil
}

func isCorrectCacheEntry(entry *prototransformv1alpha1.CacheEntry, schemaID string, syms []string) bool {
	return entry.GetId() == schemaID && isSuperSet(entry.GetIncludedSymbols(), syms)
}

func isSuperSet(have, want []string) bool {
	if len(want) == 0 {
		return len(have) == 0
	}
	if len(have) < len(want) {
		// Technically, len(have) == 0 means it should be the full
		// schema and thus we should possibly return true. But there
		// are possible cases where "full schema, no filtering" could
		// actually return something other than a superset, such as
		// if the schema poller impl can't authoritatively enumerate
		// the entire schema (like for gRPC server reflection).
		return false
	}
	j := 0
	for i := range have {
		if have[i] == want[j] {
			j++
			if j == len(want) {
				// matched them all
				return true
			}
		}
	}
	return false
}
