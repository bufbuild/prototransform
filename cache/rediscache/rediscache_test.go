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

// We use a build tag since redis may not be running. If it is
// running, for use with the test, then pass flag "-tags with_servers"
// when running tests to enable these tests.
//go:build with_servers

package rediscache

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"testing"
	"time"

	"github.com/bufbuild/prototransform/cache/internal/cachetesting"
	"github.com/gomodule/redigo/redis"
	"github.com/stretchr/testify/require"
)

func TestRedisCache(t *testing.T) {
	t.Parallel()

	pool := &redis.Pool{
		DialContext: func(ctx context.Context) (redis.Conn, error) {
			return redis.Dial("tcp", "localhost:6379")
		},
	}
	t.Cleanup(func() {
		err := pool.Close()
		require.NoError(t, err)
	})

	testCases := []struct {
		name   string
		expiry time.Duration
	}{
		{
			name: "without expiry",
		},
		{
			name:   "with expiry",
			expiry: 60 * time.Second,
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
				Client:     pool,
				KeyPrefix:  keyPrefix,
				Expiration: testCase.expiry,
			})
			require.NoError(t, err)
			ctx := context.Background()

			entries := cachetesting.RunSimpleCacheTests(t, ctx, cache)

			// check the actual keys in memcache
			checkKeys(t, pool.Get(), keyPrefix, testCase.expiry, entries)
		})
	}
}

func checkKeys(t *testing.T, client redis.Conn, keyPrefix string, ttl time.Duration, entries map[string][]byte) {
	for k, v := range entries {
		data, err := redis.Bytes(client.Do("get", keyPrefix+k))
		require.NoError(t, err)
		require.Equal(t, v, data)
		if ttl != 0 {
			reply, err := client.Do("pttl", keyPrefix+k)
			require.NoError(t, err)
			ttlMillis, ok := reply.(int64)
			require.True(t, ok)
			actualTTL := time.Duration(ttlMillis) * time.Millisecond
			require.LessOrEqual(t, actualTTL, ttl)
			// 2 second window for TTL to have decreased: generous to accommodate slow/flaky CI machines
			require.Greater(t, actualTTL, ttl-2*time.Second)
		}
	}
}
