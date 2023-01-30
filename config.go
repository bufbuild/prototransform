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
	"fmt"
	"net/http"
	"time"

	"buf.build/gen/go/bufbuild/reflect/bufbuild/connect-go/buf/reflect/v1beta1/reflectv1beta1connect"
	"github.com/bufbuild/connect-go"
	"google.golang.org/protobuf/reflect/protoreflect"
)

const defaultPollingPeriod = 5 * time.Minute

// Config contains the configurable attributes of the [SchemaWatcher].
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

func (c *Config) validate() error {
	if c.Client == nil {
		return fmt.Errorf("schema service client not provided")
	}
	if c.Module == "" {
		return fmt.Errorf("buf module not provided")
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

// NewDefaultFileDescriptorSetServiceClient will create an authenticated connection to the
// public Buf Schema Registry (BSR) at https://api.buf.build.
//
// If you require a connection to a different BSR instance, create your own
// [reflectv1beta1connect.FileDescriptorSetServiceClient]. You can use [NewAuthInterceptor]
// to configure authentication credentials.
//
// For help with authenticating with the Buf Schema Registry visit: https://docs.buf.build/bsr/authentication
func NewDefaultFileDescriptorSetServiceClient(token string) reflectv1beta1connect.FileDescriptorSetServiceClient {
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
func NewAuthInterceptor(token string) connect.Interceptor {
	bearerAuthValue := fmt.Sprintf("Bearer %s", token)
	return connect.UnaryInterceptorFunc(func(next connect.UnaryFunc) connect.UnaryFunc {
		return func(ctx context.Context, request connect.AnyRequest) (connect.AnyResponse, error) {
			request.Header().Set("Authorization", bearerAuthValue)
			return next(ctx, request)
		}
	})
}
