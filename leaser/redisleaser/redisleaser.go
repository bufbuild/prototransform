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

// Package redisleaser provides an implementation of prototransform.Leaser
// that is backed by a Redis instance: https://redis.io/.
package redisleaser

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/bufbuild/prototransform"
	"github.com/bufbuild/prototransform/leaser"
	"github.com/gomodule/redigo/redis"
)

const (
	// This script atomically tries to acquire a lease by creating a new key.
	// If creation fails, but the existing key has a value that indicates the
	// current process holds the lease, it bumps the lease's TTL.
	//
	// Input:
	//   KEYS[1]: the lease key
	//   ARGV[1]: the desired leaseholder value (current process)
	//   ARGV[2]: desired lease TTL in milliseconds
	//
	// Output:
	//   TABLE: {bool, bulk string}
	// bool is true if key was created; bulk string is value of key.
	tryAcquireLUAScript = `
		redis.setresp(3)
		local resp = redis.call('set', KEYS[1], ARGV[1], 'get', 'nx', 'px', ARGV[2])
		if resp == nil then
		  return {true, ARGV[1]}
		elseif resp == ARGV[1] then
		  redis.call('pexpire', KEYS[1], ARGV[2], 'lt')
		end
		return {false, resp}
		`

	// This script atomically releases a lease by deleting the key if
	// and only if the current process is the holder.
	//
	// Input:
	//   KEYS[1]: the lease key
	//   ARGV[1]: the leaseholder value of the current process
	//
	// Output:
	//   None
	releaseLUAScript = `
		if redis.call('get', KEYS[1]) == ARGV[1] then redis.call('del', KEYS[1]) end
		`
)

// Config represents the configuration parameters used to
// create a new Redis-backed leaser.
type Config struct {
	Client        *redis.Pool
	KeyPrefix     string
	LeaseTTL      time.Duration
	PollingPeriod time.Duration
}

// New creates a new Redis-backed leaser with the given configuration.
func New(config Config) (prototransform.Leaser, error) {
	// validate config
	if config.Client == nil {
		return nil, errors.New("client cannot be nil")
	}
	if config.LeaseTTL < 0 {
		return nil, fmt.Errorf("lease TTL (%v) cannot be negative", config.LeaseTTL)
	}
	if config.LeaseTTL == 0 {
		config.LeaseTTL = 30 * time.Second
	}
	if config.PollingPeriod < 0 {
		return nil, fmt.Errorf("polling period (%v) cannot be negative", config.PollingPeriod)
	}
	if config.PollingPeriod == 0 {
		config.PollingPeriod = 10 * time.Second
	}
	if config.PollingPeriod > config.LeaseTTL {
		return nil, fmt.Errorf("polling period (%v) should be <= lease TTL (%v)", config.PollingPeriod, config.LeaseTTL)
	}
	return &leaser.PollingLeaser{
		LeaseStore: &leaseStore{
			pool:      config.Client,
			keyPrefix: config.KeyPrefix,
		},
		LeaseTTL:      config.LeaseTTL,
		PollingPeriod: config.PollingPeriod,
	}, nil
}

type leaseStore struct {
	pool      *redis.Pool
	keyPrefix string

	scriptsMu     sync.RWMutex
	tryAcquireSHA string
	releaseSHA    string
}

func (l *leaseStore) TryAcquire(ctx context.Context, leaseName string, leaseHolder []byte, ttl time.Duration) (created bool, holder []byte, err error) {
	for {
		tryAcquire, _, err := l.getScriptSHAs(ctx)
		if tryAcquire == "" {
			return false, nil, err
		}
		conn, err := l.pool.GetContext(ctx)
		if err != nil {
			return false, nil, err
		}
		defer func() {
			_ = conn.Close()
		}()
		ttlMillis := ttl.Milliseconds()
		if ttlMillis == 0 {
			ttlMillis = 1 // must not be zero
		}
		resp, err := redis.Values(redis.DoContext(conn, ctx, "evalsha", tryAcquire, 1, l.keyPrefix+leaseName, leaseHolder, ttlMillis))
		if err != nil && strings.Contains(err.Error(), "NOSCRIPT") {
			// script cache was flushed; reload scripts and try again
			l.scriptsMu.Lock()
			l.tryAcquireSHA, l.releaseSHA = "", ""
			l.scriptsMu.Unlock()
			continue
		}
		if err != nil {
			return false, nil, err
		}
		if len(resp) != 2 {
			return false, nil, errors.New("tryacquire script returned wrong number of values")
		}
		boolVal, err := redis.Bool(resp[0], nil)
		if err != nil {
			return false, nil, fmt.Errorf("tryacquire script returned non-bool for first value: %w", err)
		}
		bytesVal, err := redis.Bytes(resp[1], nil)
		if err != nil {
			return false, nil, fmt.Errorf("tryacquire script returned non-bytes for second value: %w", err)
		}
		return boolVal, bytesVal, nil
	}
}

func (l *leaseStore) Release(ctx context.Context, leaseName string, leaseHolder []byte) error {
	for {
		_, release, err := l.getScriptSHAs(ctx)
		if release == "" {
			return err
		}
		conn, err := l.pool.GetContext(ctx)
		if err != nil {
			return err
		}
		defer func() {
			_ = conn.Close()
		}()
		_, err = redis.DoContext(conn, ctx, "evalsha", release, 1, l.keyPrefix+leaseName, leaseHolder)
		if err != nil && strings.Contains(err.Error(), "NOSCRIPT") {
			// script cache was flushed; reload scripts and try again
			l.scriptsMu.Lock()
			l.tryAcquireSHA, l.releaseSHA = "", ""
			l.scriptsMu.Unlock()
			continue
		}
		return err
	}
}

func (l *leaseStore) getScriptSHAs(ctx context.Context) (tryAcquire, release string, err error) {
	l.scriptsMu.RLock()
	tryAcquire, release = l.tryAcquireSHA, l.releaseSHA
	l.scriptsMu.RUnlock()
	if tryAcquire != "" && release != "" {
		return tryAcquire, release, nil
	}

	l.scriptsMu.Lock()
	defer l.scriptsMu.Unlock()
	// check again, in case they were concurrently added while upgrading lock
	tryAcquire, release = l.tryAcquireSHA, l.releaseSHA
	if tryAcquire != "" && release != "" {
		return tryAcquire, release, nil
	}
	conn, err := l.pool.GetContext(ctx)
	if err != nil {
		return tryAcquire, release, err
	}
	defer func() {
		_ = conn.Close()
	}()

	if tryAcquire == "" {
		tryAcquire, err = redis.String(redis.DoContext(conn, ctx, "script", "load", tryAcquireLUAScript))
	}
	if release == "" {
		var releaseErr error
		release, releaseErr = redis.String(redis.DoContext(conn, ctx, "script", "load", releaseLUAScript))
		if releaseErr != nil && err == nil {
			err = releaseErr
		}
	}
	l.tryAcquireSHA, l.releaseSHA = tryAcquire, release
	return tryAcquire, release, err
}
