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

package prototransform_test

import (
	"context"
	"encoding/hex"
	"fmt"
	"log"
	"time"

	"github.com/bufbuild/prototransform"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

var inputData = []byte(`{"sentence": "I feel happy."}`)

const (
	messageName = "buf.connect.demo.eliza.v1.SayRequest"
	moduleName  = "buf.build/bufbuild/eliza"
)

func Example() {
	token, err := prototransform.BufTokenFromEnvironment(moduleName)
	if err != nil {
		log.Fatalf("Failed to get token from environment: %v\n"+
			"For help with authenticating with the Buf Schema Registry visit: https://docs.buf.build/bsr/authentication",
			err)
	}
	// Supply auth credentials to the BSR
	client := prototransform.NewDefaultFileDescriptorSetServiceClient(token)
	// Configure the module for schema watcher
	cfg := &prototransform.SchemaWatcherConfig{
		SchemaPoller: prototransform.NewSchemaPoller(
			client,
			moduleName, // BSR module
			"main",     // tag or draft name or leave blank for "latest"
		),
	}
	ctx := context.Background()
	schemaWatcher, err := prototransform.NewSchemaWatcher(ctx, cfg)
	if err != nil {
		log.Fatalf("failed to create schema watcher: %v", err)
		return
	}
	// The BSR imposes a rate limit, so that multiple concurrent CI jobs can tickle it
	// and then cause this next call to fail because all calls get rejected with a
	// "resource exhausted" error. So that's why we have a large timeout of a whole
	// minute: eventually, it will succeed, even if we get rate-limited due to other
	// concurrent CI jobs hitting the same API with the same token.
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	if err := schemaWatcher.AwaitReady(ctx); err != nil {
		log.Fatalf("schema watcher never became ready: %v", err)
		return
	}
	converter := &prototransform.Converter{
		Resolver:     schemaWatcher,
		InputFormat:  prototransform.JSONInputFormat(protojson.UnmarshalOptions{}),
		OutputFormat: prototransform.BinaryOutputFormat(proto.MarshalOptions{}),
	}
	convertedMessage, err := converter.ConvertMessage(messageName, inputData)
	if err != nil {
		log.Fatalf("Converting message: %v\n", err)
		return
	}
	fmt.Printf("Converted message: 0x%s\n", hex.EncodeToString(convertedMessage))
	// Output: Converted message: 0x0a0d49206665656c2068617070792e
}
