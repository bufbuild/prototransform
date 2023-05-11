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
	// ErrSchemaNotModified is an error that may be returned by a DescriptorPoller to
	// indicate that the poller did not return any descriptors because the caller's
	// cached version is still sufficiently fresh.
	ErrSchemaNotModified = errors.New("no response because schema not modified")
)

// Config contains the configurable attributes of the [SchemaWatcher].
//
// Deprecated: Use SchemaWatcherConfig instead.
type Config struct {
	// Client is a client for the reflectv1beta1connect.FileDescriptorSetServiceClient service.
	Client reflectv1beta1connect.FileDescriptorSetServiceClient
	// Module which uniquely identifies and gives ownership to a collection of Protobuf
	// files found on the Buf Schema Registry. Owner is an entity that is
	// either a user or organization within the BSR ecosystem. Repository
	// stores all versions of a single module
	Module string
	// Version includes any number of changes, and each change is identifiable by a
	// unique commit, tag, or draft. (Optional, default: main)
	Version string
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
	// If Cache is non-nil, it is used for increased robustness, even in the
	// face of the remote schema registry being unavailable. If non-nil and the
	// API call to initially retrieve a schema fails, the schema will instead
	// be loaded from this cache. Whenever a new schema is downloaded from the
	// remote registry, it will be saved to the cache. So if the process is
	// restarted and the remote registry is unavailable, the latest cached schema
	// can still be used.
	Cache Cache
}

func (c *Config) validate() (*SchemaWatcherConfig, error) {
	if c.Client == nil {
		return nil, fmt.Errorf("schema service client not provided")
	}
	if c.Module == "" {
		return nil, fmt.Errorf("buf module not provided")
	}
	var cacheKeyPrefix string
	if c.Cache != nil {
		cacheKeyPrefix = c.Module
		if c.Version != "" {
			cacheKeyPrefix += "@" + c.Version
		}
	}
	return &SchemaWatcherConfig{
		DescriptorPoller: NewDescriptorPoller(c.Client, c.Module, c.Version),
		IncludeSymbols:   c.IncludeSymbols,
		PollingPeriod:    c.PollingPeriod,
		Cache:            c.Cache,
		CacheKeyPrefix:   cacheKeyPrefix,
	}, nil
}

// SchemaWatcherConfig contains the configurable attributes of the [SchemaWatcher].
type SchemaWatcherConfig struct {
	// The downloader of descriptors. See [NewDescriptorPoller].
	DescriptorPoller DescriptorPoller
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
	// If Cache is non-nil, it is used for increased robustness, even in the
	// face of the remote schema registry being unavailable. If non-nil and the
	// API call to initially retrieve a schema fails, the schema will instead
	// be loaded from this cache. Whenever a new schema is downloaded from the
	// remote registry, it will be saved to the cache. So if the process is
	// restarted and the remote registry is unavailable, the latest cached schema
	// can still be used.
	Cache Cache
	// CacheKeyPrefix is the prefix used for a cache key, when loading or storing
	// descriptors in Cache. Any symbols requested will be canonicalized and then
	// appended to this key. If Cache is non-nil, this must be non-empty. It is up
	// to the Cache to sanitize the key if necessary.
	CacheKeyPrefix string
}

func (c *SchemaWatcherConfig) validate() error {
	if c.DescriptorPoller == nil {
		return fmt.Errorf("descriptor poller not provided")
	}
	if c.PollingPeriod < 0 {
		return fmt.Errorf("polling period duration cannot be negative")
	}
	if c.Cache != nil && c.CacheKeyPrefix == "" {
		return fmt.Errorf("cache key prefix cannot be blank if cache is non-nil")
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

// DescriptorPoller polls for descriptors from a remote source.
// See [NewDescriptorPoller].
//
// Implementations should return ErrSchemaNotModified if there is no
// result to download because the given cachedVersion is still valid.
// The given cachedVersion will be blank when there is no version
// cached, in which case the poller should NOT return that error.
type DescriptorPoller interface {
	GetFileDescriptorSet(ctx context.Context, symbols []string, cachedVersion string) (descriptors *descriptorpb.FileDescriptorSet, version string, err error)
}

// NewDescriptorPoller returns a DescriptorPoller that uses the given Buf Reflection
// API client to download descriptors for the given module. If the given version is
// non-empty, the descriptors will be downloaded from that version of the module.
//
// The version should either be blank or indicate a tag that may change over time,
// such as a draft name. If a fixed tag or commit is provided, then the periodic
// polling is unnecessary since the schema for such a version is immutable.
//
// To create a client that can download descriptors from the buf.build public BSR,
// see [NewDefaultFileDescriptorSetServiceClient].
func NewDescriptorPoller(
	client reflectv1beta1connect.FileDescriptorSetServiceClient,
	module string,
	version string,
) DescriptorPoller {
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

func (b *bufReflectPoller) GetFileDescriptorSet(ctx context.Context, symbols []string, _ string) (*descriptorpb.FileDescriptorSet, string, error) {
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
