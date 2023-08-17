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
	"errors"

	"buf.build/gen/go/bufbuild/reflect/connectrpc/go/buf/reflect/v1beta1/reflectv1beta1connect"
	reflectv1beta1 "buf.build/gen/go/bufbuild/reflect/protocolbuffers/go/buf/reflect/v1beta1"
	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/descriptorpb"
)

var (
	// ErrSchemaNotModified is an error that may be returned by a SchemaPoller to
	// indicate that the poller did not return any descriptors because the caller's
	// cached version is still sufficiently fresh.
	ErrSchemaNotModified = errors.New("no response because schema not modified")
)

// SchemaPoller polls for descriptors from a remote source.
// See [NewSchemaPoller].
type SchemaPoller interface {
	// GetSchema polls for a schema. The given symbols may be used to filter
	// the schema to return a smaller result. The given currentVersion, if not
	// empty, indicates the version that the caller already has fetched and
	// cached. So if that is still the current version of the schema (nothing
	// newer to download), the implementation may return an ErrSchemaNotModified
	// error.
	GetSchema(ctx context.Context, symbols []string, currentVersion string) (descriptors *descriptorpb.FileDescriptorSet, version string, err error)
	// GetSchemaID returns a string that identifies the schema that it fetches.
	// For a BSR module, for example, this might be "buf.build/owner/module:version".
	GetSchemaID() string
}

// NewSchemaPoller returns a SchemaPoller that uses the given Buf Reflection
// API client to download descriptors for the given module. If the given version is
// non-empty, the descriptors will be downloaded from that version of the module.
//
// The version should either be blank or indicate a tag that may change over time,
// such as a draft name. If a fixed tag or commit is provided, then the periodic
// polling is unnecessary since the schema for such a version is immutable.
//
// To create a client that can download descriptors from the buf.build public BSR,
// see [NewDefaultFileDescriptorSetServiceClient].
func NewSchemaPoller(
	client reflectv1beta1connect.FileDescriptorSetServiceClient,
	module string,
	version string,
) SchemaPoller {
	return &bufReflectPoller{
		client:  client,
		module:  module,
		version: version,
	}
}

type bufReflectPoller struct {
	client          reflectv1beta1connect.FileDescriptorSetServiceClient
	module, version string
}

func (b *bufReflectPoller) GetSchema(ctx context.Context, symbols []string, currentVersion string) (*descriptorpb.FileDescriptorSet, string, error) {
	req := connect.NewRequest(&reflectv1beta1.GetFileDescriptorSetRequest{
		Module:  b.module,
		Version: b.version,
		Symbols: symbols,
	})
	if currentVersion != "" {
		req.Header().Set("If-None-Match", currentVersion)
	}
	resp, err := b.client.GetFileDescriptorSet(ctx, req)
	if err != nil {
		if currentVersion != "" && connect.IsNotModifiedError(err) {
			return nil, "", ErrSchemaNotModified
		}
		return nil, "", err
	}
	return resp.Msg.FileDescriptorSet, resp.Msg.Version, err
}

func (b *bufReflectPoller) GetSchemaID() string {
	if b.version == "" {
		return b.module
	}
	return b.module + ":" + b.version
}
