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

// We use a build tag since memcached may not be running. If it is
// running, for use with the test, then pass flag "-tags with_servers"
// when running tests to enable these tests.
//go:build with_servers

package memcacheleaser

import (
	"context"
	"testing"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/bufbuild/prototransform/leaser"
	"github.com/bufbuild/prototransform/leaser/internal/leasertesting"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMemcacheLeaser(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	client := memcache.New("localhost:11211")
	t.Cleanup(func() {
		err := client.Close()
		require.NoError(t, err)
	})

	l, err := New(Config{
		Client: client,
	})
	require.NoError(t, err)
	// check defaults
	assert.Equal(t, 10*time.Second, l.(*leaser.PollingLeaser).PollingPeriod)
	assert.Equal(t, 30*time.Second, l.(*leaser.PollingLeaser).LeaseTTL)

	keyPrefix := "abc:"
	l, err = New(Config{
		Client:          client,
		LeaseTTLSeconds: 1,
		PollingPeriod:   25 * time.Millisecond,
		KeyPrefix:       keyPrefix,
	})
	require.NoError(t, err)

	forceOwner := func(key string, owner []byte) error {
		item := &memcache.Item{
			Key:        keyPrefix + key,
			Value:      owner,
			Expiration: 5,
		}
		err = client.Set(item)
		t.Cleanup(func() {
			// delete when test done
			err = client.Delete(keyPrefix + key)
			require.NoError(t, err)
		})
		return err
	}

	leasertesting.RunSimpleLeaserTests(t, ctx, l, forceOwner)
}
