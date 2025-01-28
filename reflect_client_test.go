// Copyright 2023-2024 Buf Technologies, Inc.
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
	"testing"
	"time"

	"buf.build/gen/go/bufbuild/reflect/connectrpc/go/buf/reflect/v1beta1/reflectv1beta1connect"
	reflectv1beta1 "buf.build/gen/go/bufbuild/reflect/protocolbuffers/go/buf/reflect/v1beta1"
	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"
)

const (
	moduleName    = "buf.build/googleapis/googleapis"
	moduleVersion = "cc916c31859748a68fd229a3c8d7a2e8"
)

func TestReflectClientLargeRequest(t *testing.T) {
	t.Parallel()
	token, err := BufTokenFromEnvironment(moduleName)
	require.NoError(t, err)
	// Supply auth credentials to the BSR
	client := NewDefaultFileDescriptorSetServiceClient(token)

	// Max URL size is 8192. Subtract the preamble from that.
	maxRequestSz := 8192 - len("https://buf.build"+
		reflectv1beta1connect.FileDescriptorSetServiceGetFileDescriptorSetProcedure+
		"?encoding=proto&base64=1&connect=v1&message=")
	// The message will be base64-encoded, so we really only have 3/4 this length
	// due to expansion from the encoding.
	maxRequestSz = maxRequestSz * 3 / 4
	// Finally subtract the module and version request fields.
	maxRequestSz -= 2 + len(moduleName) + 2 + len(moduleVersion)
	// What remains is what we can use to enumerate types to fill up the message size.

	var types []string
	for maxRequestSz >= 0 {
		for _, name := range symbolNames {
			types = append(types, name)
			maxRequestSz -= 2 + len(name)
			if maxRequestSz < 0 {
				// that pushed us over the edge, so types slice has one extra item
				break
			}
		}
	}

	ctx := context.Background()
	req := connect.NewRequest(&reflectv1beta1.GetFileDescriptorSetRequest{
		Module:  moduleName,
		Version: moduleVersion,
		Symbols: types,
	})
	req.Header().Set("If-None-Match", moduleVersion)
	// The full types slice should be too large for a GET, which means we end up
	// downloading the entire response.
	resp, err := getFileDescriptorSet(ctx, client, req)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(resp.Msg.GetFileDescriptorSet().GetFile()), 42)

	// If we ask for one fewer type, we should be below the limit and get back
	// a "not modified" response code instead of a response body.
	req.Msg.Symbols = types[:len(types)-1]
	_, err = getFileDescriptorSet(ctx, client, req)
	require.True(t, connect.IsNotModifiedError(err))
}

func getFileDescriptorSet(
	ctx context.Context,
	client reflectv1beta1connect.FileDescriptorSetServiceClient,
	req *connect.Request[reflectv1beta1.GetFileDescriptorSetRequest],
) (*connect.Response[reflectv1beta1.GetFileDescriptorSetResponse], error) {
	// We are hitting the real buf.build endpoint. To work-around spurious errors
	// from a temporary network partition or encountering the endpoint rate limit,
	// we will retry for up to a minute. (The long retry window is to allow
	// exponential back-off delays between attempts and to be resilient to cases
	// in CI where multiple concurrent jobs are hitting the endpoint and exceeding
	// the rate limit.)
	var lastErr error
	delay := 250 * time.Millisecond
	start := time.Now()
	for {
		if lastErr != nil {
			// delay between attempts
			time.Sleep(delay)
			delay *= 2
		}
		resp, err := client.GetFileDescriptorSet(ctx, req)
		if err == nil {
			return resp, nil
		}
		code := connect.CodeOf(err)
		if code != connect.CodeUnavailable && code != connect.CodeResourceExhausted {
			return nil, err
		}
		if time.Since(start) > time.Minute {
			// Took too long. Fail.
			return nil, err
		}
		// On "unavailable" (could be transient network issue) or
		// "resource exhausted" (rate limited), we loop and try again.
		lastErr = err
	}
}

// Message types in buf.build/googleapis/googleapis. Generated via the following:
//
//	buf build -o -#format=json buf.build/googleapis/googleapis \
//		| jq  '.file[] | .package as $pkg  | .messageType[]? | ( $pkg + "." + .name )' \
//		| sort \
//		| jq -s .
//
//nolint:gochecknoglobals
var symbolNames = []string{
	"google.api.ClientLibrarySettings",
	"google.api.CommonLanguageSettings",
	"google.api.CppSettings",
	"google.api.CustomHttpPattern",
	"google.api.DotnetSettings",
	"google.api.GoSettings",
	"google.api.Http",
	"google.api.HttpBody",
	"google.api.HttpRule",
	"google.api.JavaSettings",
	"google.api.MethodSettings",
	"google.api.NodeSettings",
	"google.api.PhpSettings",
	"google.api.Publishing",
	"google.api.PythonSettings",
	"google.api.ResourceDescriptor",
	"google.api.ResourceReference",
	"google.api.RubySettings",
	"google.api.Visibility",
	"google.api.VisibilityRule",
	"google.api.expr.v1alpha1.CheckedExpr",
	"google.api.expr.v1alpha1.Constant",
	"google.api.expr.v1alpha1.Decl",
	"google.api.expr.v1alpha1.EnumValue",
	"google.api.expr.v1alpha1.ErrorSet",
	"google.api.expr.v1alpha1.EvalState",
	"google.api.expr.v1alpha1.Explain",
	"google.api.expr.v1alpha1.Expr",
	"google.api.expr.v1alpha1.ExprValue",
	"google.api.expr.v1alpha1.ListValue",
	"google.api.expr.v1alpha1.MapValue",
	"google.api.expr.v1alpha1.ParsedExpr",
	"google.api.expr.v1alpha1.Reference",
	"google.api.expr.v1alpha1.SourceInfo",
	"google.api.expr.v1alpha1.SourcePosition",
	"google.api.expr.v1alpha1.Type",
	"google.api.expr.v1alpha1.UnknownSet",
	"google.api.expr.v1alpha1.Value",
	"google.api.expr.v1beta1.Decl",
	"google.api.expr.v1beta1.DeclType",
	"google.api.expr.v1beta1.EnumValue",
	"google.api.expr.v1beta1.ErrorSet",
	"google.api.expr.v1beta1.EvalState",
	"google.api.expr.v1beta1.Expr",
	"google.api.expr.v1beta1.ExprValue",
	"google.api.expr.v1beta1.FunctionDecl",
	"google.api.expr.v1beta1.IdRef",
	"google.api.expr.v1beta1.IdentDecl",
	"google.api.expr.v1beta1.ListValue",
	"google.api.expr.v1beta1.Literal",
	"google.api.expr.v1beta1.MapValue",
	"google.api.expr.v1beta1.ParsedExpr",
	"google.api.expr.v1beta1.SourceInfo",
	"google.api.expr.v1beta1.SourcePosition",
	"google.api.expr.v1beta1.UnknownSet",
	"google.api.expr.v1beta1.Value",
	"google.bytestream.QueryWriteStatusRequest",
	"google.bytestream.QueryWriteStatusResponse",
	"google.bytestream.ReadRequest",
	"google.bytestream.ReadResponse",
	"google.bytestream.WriteRequest",
	"google.bytestream.WriteResponse",
	"google.geo.type.Viewport",
	"google.longrunning.CancelOperationRequest",
	"google.longrunning.DeleteOperationRequest",
	"google.longrunning.GetOperationRequest",
	"google.longrunning.ListOperationsRequest",
	"google.longrunning.ListOperationsResponse",
	"google.longrunning.Operation",
	"google.longrunning.OperationInfo",
	"google.longrunning.WaitOperationRequest",
	"google.protobuf.Any",
	"google.protobuf.BoolValue",
	"google.protobuf.BytesValue",
	"google.protobuf.DescriptorProto",
	"google.protobuf.DoubleValue",
	"google.protobuf.Duration",
	"google.protobuf.Empty",
	"google.protobuf.EnumDescriptorProto",
	"google.protobuf.EnumOptions",
	"google.protobuf.EnumValueDescriptorProto",
	"google.protobuf.EnumValueOptions",
	"google.protobuf.ExtensionRangeOptions",
	"google.protobuf.FieldDescriptorProto",
	"google.protobuf.FieldOptions",
	"google.protobuf.FileDescriptorProto",
	"google.protobuf.FileDescriptorSet",
	"google.protobuf.FileOptions",
	"google.protobuf.FloatValue",
	"google.protobuf.GeneratedCodeInfo",
	"google.protobuf.Int32Value",
	"google.protobuf.Int64Value",
	"google.protobuf.ListValue",
	"google.protobuf.MessageOptions",
	"google.protobuf.MethodDescriptorProto",
	"google.protobuf.MethodOptions",
	"google.protobuf.OneofDescriptorProto",
	"google.protobuf.OneofOptions",
	"google.protobuf.ServiceDescriptorProto",
	"google.protobuf.ServiceOptions",
	"google.protobuf.SourceCodeInfo",
	"google.protobuf.StringValue",
	"google.protobuf.Struct",
	"google.protobuf.Timestamp",
	"google.protobuf.UInt32Value",
	"google.protobuf.UInt64Value",
	"google.protobuf.UninterpretedOption",
	"google.protobuf.Value",
	"google.rpc.BadRequest",
	"google.rpc.DebugInfo",
	"google.rpc.ErrorInfo",
	"google.rpc.Help",
	"google.rpc.LocalizedMessage",
	"google.rpc.PreconditionFailure",
	"google.rpc.QuotaFailure",
	"google.rpc.RequestInfo",
	"google.rpc.ResourceInfo",
	"google.rpc.RetryInfo",
	"google.rpc.Status",
	"google.rpc.context.AttributeContext",
	"google.type.Color",
	"google.type.Date",
	"google.type.DateTime",
	"google.type.Decimal",
	"google.type.Expr",
	"google.type.Fraction",
	"google.type.Interval",
	"google.type.LatLng",
	"google.type.LocalizedText",
	"google.type.Money",
	"google.type.PhoneNumber",
	"google.type.PostalAddress",
	"google.type.Quaternion",
	"google.type.TimeOfDay",
	"google.type.TimeZone",
}
