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

package redisleaser

import (
	"context"
	"testing"
	"time"

	"github.com/bufbuild/prototransform/leaser"
	"github.com/bufbuild/prototransform/leaser/internal/leasertesting"
	"github.com/gomodule/redigo/redis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRedisLeaser(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	pool := &redis.Pool{
		DialContext: func(ctx context.Context) (redis.Conn, error) {
			return redis.Dial("tcp", "localhost:6379")
		},
	}
	t.Cleanup(func() {
		err := pool.Close()
		require.NoError(t, err)
	})

	l, err := New(Config{
		Client: pool,
	})
	require.NoError(t, err)
	// check defaults
	assert.Equal(t, 10*time.Second, l.(*leaser.PollingLeaser).PollingPeriod)
	assert.Equal(t, 30*time.Second, l.(*leaser.PollingLeaser).LeaseTTL)

	keyPrefix := "abc:"
	l, err = New(Config{
		Client:        pool,
		LeaseTTL:      time.Second,
		PollingPeriod: 25 * time.Millisecond,
		KeyPrefix:     keyPrefix,
	})
	require.NoError(t, err)

	forceOwner := func(key string, owner []byte) error {
		conn, err := pool.GetContext(ctx)
		if err != nil {
			return err
		}
		defer func() {
			_ = conn.Close()
		}()
		_, err = redis.DoContext(conn, ctx, "set", keyPrefix+key, owner, "ex", 5)
		t.Cleanup(func() {
			// delete when test done
			conn, err := pool.GetContext(ctx)
			require.NoError(t, err)
			defer func() {
				_ = conn.Close()
			}()
			_, err = redis.DoContext(conn, ctx, "del", keyPrefix+key)
			require.NoError(t, err)
		})
		return err
	}

	leasertesting.RunSimpleLeaserTests(t, ctx, l, forceOwner)
}
