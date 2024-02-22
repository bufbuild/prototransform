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

//go:build !go1.20

package prototransform

import "fmt"

// cacheMultiErr is a temporary workaround for Go 1.19 and earlier versions that don't support wrapping multiple errors.
func cacheMultiErr(msg string, err error, cacheErr error) error {
	return fmt.Errorf("%w (%s: %v)", err, msg, cacheErr) //nolint:errorlint
}
