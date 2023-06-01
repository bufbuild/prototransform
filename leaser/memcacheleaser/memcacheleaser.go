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

// Package memcacheleaser provides an implementation of prototransform.Leaser
// that is backed by a memcached instance: https://memcached.org/.
package memcacheleaser

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/bufbuild/prototransform"
	"github.com/bufbuild/prototransform/leaser"
)

// Config represents the configuration parameters used to
// create a new memcached-backed leaser.
type Config struct {
	Client          *memcache.Client
	KeyPrefix       string
	LeaseTTLSeconds int32
	PollingPeriod   time.Duration
}

// New creates a new memcached-backed leaser with the given configuration.
func New(config Config) (prototransform.Leaser, error) {
	// validate config
	if config.Client == nil {
		return nil, errors.New("client cannot be nil")
	}
	if config.LeaseTTLSeconds < 0 {
		return nil, fmt.Errorf("lease TTL seconds (%d) cannot be negative", config.LeaseTTLSeconds)
	}
	if config.LeaseTTLSeconds == 0 {
		config.LeaseTTLSeconds = 30
	}
	if config.PollingPeriod < 0 {
		return nil, fmt.Errorf("polling period (%v) cannot be negative", config.PollingPeriod)
	}
	if config.PollingPeriod == 0 {
		config.PollingPeriod = 10 * time.Second
	}
	leaseDuration := time.Duration(config.LeaseTTLSeconds) * time.Second
	if config.PollingPeriod > leaseDuration {
		return nil, fmt.Errorf("polling period (%v) should be <= lease TTL (%v)", config.PollingPeriod, leaseDuration)
	}
	return &leaser.PollingLeaser{
		LeaseStore: &leaseStore{
			client:    config.Client,
			keyPrefix: config.KeyPrefix,
		},
		LeaseTTL:      leaseDuration,
		PollingPeriod: config.PollingPeriod,
	}, nil
}

type leaseStore struct {
	client    *memcache.Client
	keyPrefix string
}

func (l *leaseStore) TryAcquire(_ context.Context, leaseName string, leaseHolder []byte, ttl time.Duration) (created bool, holder []byte, err error) {
	key := l.keyPrefix + leaseName
	expire := int32(ttl.Seconds())
	if expire == 0 {
		expire = 1 // minimum expiry is 1 second
	}
	for {
		// Optimize for the normal/expected case: lease exists.
		// So we first do a GET and then ADD if it doesn't exist.
		item, err := l.client.Get(key)
		if err != nil && !errors.Is(err, memcache.ErrCacheMiss) {
			return false, nil, err
		}
		if errors.Is(err, memcache.ErrCacheMiss) {
			// no such key; try to add it
			item = &memcache.Item{
				Key:        key,
				Value:      leaseHolder,
				Expiration: expire,
			}
			err := l.client.Add(item)
			if errors.Is(err, memcache.ErrNotStored) {
				// we lost race to create item, so re-query and try again
				continue
			}
			if err != nil {
				return false, nil, err
			}
			// success!
			return true, leaseHolder, nil
		}
		if len(item.Value) == 0 {
			// no current leaseholder; let's try to take it
			item.Value = leaseHolder
			item.Expiration = expire
			err := l.client.CompareAndSwap(item)
			if errors.Is(err, memcache.ErrCASConflict) || errors.Is(err, memcache.ErrNotStored) {
				// CAS failed, so re-query and try again
				continue
			}
			if err != nil {
				return false, nil, err
			}
			// success!
			return true, leaseHolder, nil
		}
		if bytes.Equal(item.Value, leaseHolder) {
			// we are the leaseholder; bump the expiry (best effort only)
			item.Expiration = expire
			_ = l.client.CompareAndSwap(item)
		}
		return false, item.Value, nil
	}
}

func (l *leaseStore) Release(_ context.Context, leaseName string, leaseHolder []byte) error {
	// memcached doesn't have a way to conditionally delete, so if we read
	// the lease value and then deleted if it has the right value, we could
	// end up racing with a concurrent writer. So instead of deleting, we
	// CAS the value to empty bytes and set the shortest possible TTL (which
	// is one second). So empty bytes value means "no leaseholder".
	key := l.keyPrefix + leaseName
	for {
		item, err := l.client.Get(key)
		if err != nil {
			if errors.Is(err, memcache.ErrCacheMiss) {
				// not there anymore so nothing to delete
				return nil
			}
			return err
		}
		if !bytes.Equal(item.Value, leaseHolder) {
			// lease is not ours; nothing to do
			return nil
		}
		item.Value = nil
		item.Expiration = 1
		err = l.client.CompareAndSwap(item)
		if errors.Is(err, memcache.ErrCASConflict) || errors.Is(err, memcache.ErrNotStored) {
			// CAS failed, so re-query and try again
			continue
		}
		return err
	}
}
