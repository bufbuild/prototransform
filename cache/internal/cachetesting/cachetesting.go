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

package cachetesting

import (
	"context"
	"crypto/rand"
	"testing"

	"github.com/bufbuild/prototransform"
	"github.com/stretchr/testify/require"
)

//nolint:revive // okay that ctx is second; prefer t to be first
func RunSimpleCacheTests(t *testing.T, ctx context.Context, cache prototransform.Cache) map[string][]byte {
	t.Helper()

	// In case tests are run concurrently, we want to make sure we aren't reading
	// values stored to a cache by another concurrent test. While the caller of this
	// function should arrange for that (using different server/directory/keyspace, etc)
	// we want to catch accidental misuse. So for that, we generate random data so that
	// the values we expect are different from one call to another.

	const (
		keyFoo   = "foo"
		keyBar   = "bar"
		keyEmpty = ""
	)

	entries := make(map[string][]byte, 3)
	for _, k := range []string{keyFoo, keyBar, keyEmpty} {
		val := make([]byte, 100)
		_, err := rand.Read(val)
		require.NoError(t, err)
		entries[k] = val
	}
	valFoo, valBar, valEmpty := entries[keyFoo], entries[keyBar], entries[keyEmpty]

	// load fails since nothing exists
	_, err := cache.Load(ctx, keyFoo)
	require.Error(t, err)
	err = cache.Save(ctx, keyFoo, valFoo)
	require.NoError(t, err)
	loaded, err := cache.Load(ctx, keyFoo)
	require.NoError(t, err)
	require.Equal(t, valFoo, loaded)

	// another key
	_, err = cache.Load(ctx, keyBar)
	require.Error(t, err)
	err = cache.Save(ctx, keyBar, valBar)
	require.NoError(t, err)
	loaded, err = cache.Load(ctx, keyBar)
	require.NoError(t, err)
	require.Equal(t, valBar, loaded)

	// original key unchanged
	loaded, err = cache.Load(ctx, keyFoo)
	require.NoError(t, err)
	require.Equal(t, valFoo, loaded)

	// empty key
	err = cache.Save(ctx, keyEmpty, valEmpty)
	require.NoError(t, err)
	loaded, err = cache.Load(ctx, keyEmpty)
	require.NoError(t, err)
	require.Equal(t, valEmpty, loaded)

	return entries
}
