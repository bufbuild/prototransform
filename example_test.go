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
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

var inputData = []byte(`{"sentence": "I feel happy."}`)

const (
	messageName = "buf.connect.demo.eliza.v1.SayRequest"
)

func Example() {
	token := os.Getenv("BUF_TOKEN")
	if token == "" {
		log.Fatalf("Token not supplied, for help with authenticating with the Buf Schema Registry visit: https://docs.buf.build/bsr/authentication")
	}
	// Supply auth credentials to the BSR
	client := NewDefaultFileDescriptorSetServiceClient(token)
	// Configure the module for schema watcher
	cfg := &SchemaWatcherConfig{
		SchemaPoller: NewSchemaPoller(
			client,
			"buf.build/bufbuild/eliza", // BSR module
			"main",                     // tag or draft name or leave blank for "latest"
		),
	}
	ctx := context.Background()
	schemaWatcher, err := NewSchemaWatcher(ctx, cfg)
	if err != nil {
		log.Fatalf("failed to create schema watcher: %v", err)
		return
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := schemaWatcher.AwaitReady(ctx); err != nil {
		log.Fatalf("schema watcher never became ready: %v", err)
		return
	}
	converter := &Converter{
		Resolver:     schemaWatcher,
		InputFormat:  JSONInputFormat(protojson.UnmarshalOptions{}),
		OutputFormat: BinaryOutputFormat(proto.MarshalOptions{}),
	}
	convertedMessage, err := converter.ConvertMessage(messageName, inputData)
	if err != nil {
		log.Fatalf("Converting message: %v\n", err)
		return
	}
	fmt.Printf("Converted message: 0x%s\n", hex.EncodeToString(convertedMessage))
	// Output: Converted message: 0x0a0d49206665656c2068617070792e
}
