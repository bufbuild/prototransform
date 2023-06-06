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
	"os"
	"strings"

	"buf.build/gen/go/bufbuild/reflect/bufbuild/connect-go/buf/reflect/v1beta1/reflectv1beta1connect"
	"github.com/bufbuild/connect-go"
)

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
// module reference, with or without a version, or it can just be the domain of the BSR.
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
