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

// We use a build tag since memcached may not be running. If it is
// running, for use with the test, then pass flag "-tags with_servers"
// when running tests to enable these tests.
//go:build with_servers

package memcache

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"testing"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/bufbuild/prototransform/cache/internal/cachetesting"
	"github.com/stretchr/testify/require"
)

func TestMemcache(t *testing.T) {
	t.Parallel()

	client := memcache.New("localhost:11211")
	testCases := []struct {
		name          string
		expirySeconds int32
	}{
		{
			name: "without expiry",
		},
		{
			name: "with expiry",
			// this needs to be small, because we actually sleep this long to
			// verify that keys expire (since there's no way in memcache to
			// just query for the ttl)
			expirySeconds: 2,
		},
	}
	for _, testCase := range testCases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			prefix := make([]byte, 24)
			_, err := rand.Read(prefix)
			require.NoError(t, err)
			keyPrefix := base64.StdEncoding.EncodeToString(prefix) + ":"

			cache, err := New(Config{
				Client:            client,
				KeyPrefix:         keyPrefix,
				ExpirationSeconds: testCase.expirySeconds,
			})
			require.NoError(t, err)
			ctx := context.Background()

			entries := cachetesting.RunSimpleCacheTests(t, ctx, cache)

			// check the actual keys in memcache
			checkKeys(t, client, keyPrefix, true, entries)
			if testCase.expirySeconds != 0 {
				time.Sleep(time.Duration(testCase.expirySeconds) * time.Second) // let them all expire
				checkKeys(t, client, keyPrefix, false, entries)
			}
		})
	}
}

func checkKeys(t *testing.T, client *memcache.Client, keyPrefix string, present bool, entries map[string][]byte) {
	for k, v := range entries {
		item, err := client.Get(keyPrefix + k)
		if present {
			require.NoError(t, err)
			require.Equal(t, v, item.Value)
		} else {
			require.ErrorIs(t, err, memcache.ErrCacheMiss)
		}
	}
}
