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

//go:build go1.20

package prototransform

import "fmt"

// cacheMultiErr wraps multiple errors with fmt.Errorf on Go 1.20 and later.
// When support for Go 1.19 and earlier is removed, this method can be removed and moved inline into schema_watcher.go.
func cacheMultiErr(msg string, err error, cacheErr error) error {
	return fmt.Errorf("%w (%s: %w)", err, msg, cacheErr)
}
