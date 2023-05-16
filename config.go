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
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"buf.build/gen/go/bufbuild/reflect/bufbuild/connect-go/buf/reflect/v1beta1/reflectv1beta1connect"
	"buf.build/gen/go/bufbuild/reflect/protocolbuffers/go/buf/reflect/v1beta1"
	"github.com/bufbuild/connect-go"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
)

const defaultPollingPeriod = 5 * time.Minute

var (
	// ErrSchemaNotModified is an error that may be returned by a SchemaPoller to
	// indicate that the poller did not return any descriptors because the caller's
	// cached version is still sufficiently fresh.
	ErrSchemaNotModified = errors.New("no response because schema not modified")
)

// SchemaWatcherConfig contains the configurable attributes of the [SchemaWatcher].
type SchemaWatcherConfig struct {
	// The downloader of descriptors. See [NewSchemaPoller].
	SchemaPoller SchemaPoller
	// The symbols that should be included in the downloaded schema. These must be
	// the fully-qualified names of elements in the schema, which can include
	// packages, messages, enums, extensions, services, and methods. If specified,
	// the downloaded schema will only include descriptors to describe these symbols.
	// If left empty, the entire schema will be downloaded.
	IncludeSymbols []string
	// The period of the polling the BSR for new versions is specified by the
	// PollingPeriod argument. The PollingPeriod will adjust the time interval.
	// The duration must be greater than zero; if not, [NewSchemaWatcher] will
	// return an error. If unset and left zero, a default period of 5 minutes
	// is used.
	PollingPeriod time.Duration
	// A number between 0 and 1 that represents the amount of jitter to add to
	// the polling period. A value of zero means no jitter. A value of one means
	// up to 100% jitter, so the actual period would be between 0 and 2*PollingPeriod.
	// To prevent self-synchronization (and thus thundering herds) when there are
	// multiple pollers, a value of 0.1 to 0.3 is typical.
	Jitter float64
	// If Cache is non-nil, it is used for increased robustness, even in the
	// face of the remote schema registry being unavailable. If non-nil and the
	// API call to initially retrieve a schema fails, the schema will instead
	// be loaded from this cache. Whenever a new schema is downloaded from the
	// remote registry, it will be saved to the cache. So if the process is
	// restarted and the remote registry is unavailable, the latest cached schema
	// can still be used.
	Cache Cache
	// OnUpdate is an optional callback that will be invoked when a new schema
	// is fetched. This can be used by an application to take action when a new
	// schema becomes available.
	OnUpdate func()
	// OnError is an optional callback that will be invoked when a schema cannot
	// be fetched. This could be due to the SchemaPoller returning an error or
	// failure to convert the fetched descriptors into a resolver.
	OnError func(error)
}

func (c *SchemaWatcherConfig) validate() error {
	if c.SchemaPoller == nil {
		return fmt.Errorf("schema poller not provided")
	}
	if c.PollingPeriod < 0 {
		return fmt.Errorf("polling period duration cannot be negative")
	}
	for _, sym := range c.IncludeSymbols {
		if sym == "" {
			// Counter-intuitively, empty string is valid in this context as it
			// indicates the default/unnamed package. Requesting it will include
			// all files in the module that are defined without a package.
			continue
		}
		if !protoreflect.FullName(sym).IsValid() {
			return fmt.Errorf("%q is not a valid symbol name", sym)
		}
	}
	return nil
}

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

func (b *bufReflectPoller) GetSchema(ctx context.Context, symbols []string, _ string) (*descriptorpb.FileDescriptorSet, string, error) {
	req := connect.NewRequest(&reflectv1beta1.GetFileDescriptorSetRequest{
		Module:  b.module,
		Version: b.version,
		Symbols: symbols,
	})
	resp, err := b.client.GetFileDescriptorSet(ctx, req)
	if err != nil {
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

// NewDefaultFileDescriptorSetServiceClient will create an authenticated connection to the
// public Buf Schema Registry (BSR) at https://api.buf.build. If the given token is empty,
// the BUF_TOKEN environment variable will be consulted.
//
// If you require a connection to a different BSR instance, create your own
// [reflectv1beta1connect.FileDescriptorSetServiceClient]. You can use [NewAuthInterceptor]
// to configure authentication credentials.
//
// For help with authenticating with the Buf Schema Registry visit: https://docs.buf.build/bsr/authentication
func NewDefaultFileDescriptorSetServiceClient(token string) reflectv1beta1connect.FileDescriptorSetServiceClient {
	if token == "" {
		token, _ = BufTokenFromEnvironment("buf.build")
	}
	return reflectv1beta1connect.NewFileDescriptorSetServiceClient(
		http.DefaultClient, "https://api.buf.build",
		connect.WithInterceptors(NewAuthInterceptor(token)),
	)
}

// NewAuthInterceptor accepts a token for a Buf Schema Registry (BSR) and returns an
// interceptor which can be used when creating a Connect client so that every RPC
// to the BSR is correctly authenticated.
//
// To understand more about authenticating with the BSR visit: https://docs.buf.build/bsr/authentication
//
// To get a token from the environment (e.g. BUF_TOKEN env var), see BufTokenFromEnvironment.
func NewAuthInterceptor(token string) connect.Interceptor {
	bearerAuthValue := fmt.Sprintf("Bearer %s", token)
	return connect.UnaryInterceptorFunc(func(next connect.UnaryFunc) connect.UnaryFunc {
		return func(ctx context.Context, request connect.AnyRequest) (connect.AnyResponse, error) {
			request.Header().Set("Authorization", bearerAuthValue)
			return next(ctx, request)
		}
	})
}

// BufTokenFromEnvironment returns a token that can be used to download the given module from
// the BSR by inspecting the BUF_TOKEN environment variable. The given moduleRef can be a full
// full module reference, with or without a version, or it can just be the domain of the BSR.
func BufTokenFromEnvironment(moduleRef string) (string, error) {
	parts := strings.SplitN(moduleRef, "/", 2)
	envBufToken := os.Getenv("BUF_TOKEN")
	if envBufToken == "" {
		return "", fmt.Errorf("no BUF_TOKEN environment variable set")
	}
	tok := parseBufToken(envBufToken, parts[0])
	if tok == "" {
		return "", fmt.Errorf("BUF_TOKEN environment variable did not include a token for remote %q", parts[0])
	}
	return tok, nil
}

func parseBufToken(envVar, remote string) string {
	isMultiToken := strings.ContainsAny(envVar, "@,")
	if !isMultiToken {
		return envVar
	}
	tokenConfigs := strings.Split(envVar, ",")
	suffix := "@" + remote
	for _, tokenConfig := range tokenConfigs {
		token := strings.TrimSuffix(tokenConfig, suffix)
		if token == tokenConfig {
			// did not have the right suffix
			continue
		}
		return token
	}
	return ""
}
