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

// Package prototransform will fetch purpose-built descriptor sets on the run
// and easily converting protobuf messages into human-readable formats.
//
// Use the [prototransform] library to simplify your data transformation &
// collection. Our simple package allows the caller to convert a given message data
// blob from one format to another by referring to a type schema on the Buf Schema
// Registry.
//
// The package supports to and from Binary, JSON and Text formats out of the box,
// extensible for other/custom formats also.
//
// The Buf Schema Registry Schema API builds an integration that can easily make
// use of your protobuf messages in new ways. This package will reduce your
// serialization down to exactly what you need and forget about everything else
//
// Some advantages of using [prototransform] include: Automatic version handling,
// No baking proto files into containers, No flaky fetching logic, get only the
// descriptors you need.
package prototransform
