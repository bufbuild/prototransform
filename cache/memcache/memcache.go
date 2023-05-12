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

// Package memcache provides an implementation of prototransform.Cache
// that is backed by a memcached instance: https://memcached.org/.
package memcache

import (
	"context"
	"errors"
	"fmt"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/bufbuild/prototransform"
)

type Config struct {
	Client            *memcache.Client
	KeyPrefix         string
	ExpirationSeconds int32
}

func New(config Config) (prototransform.Cache, error) {
	// validate config
	if config.Client == nil {
		return nil, errors.New("client cannot be nil")
	}
	if config.ExpirationSeconds < 0 {
		return nil, fmt.Errorf("expiration seconds (%d) cannot be negative", config.ExpirationSeconds)
	}
	return (*cache)(&config), nil
}

type cache Config

func (c *cache) Load(_ context.Context, key string) ([]byte, error) {
	item, err := c.Client.Get(c.KeyPrefix + key)
	if err != nil {
		return nil, err
	}
	return item.Value, nil
}

func (c *cache) Save(_ context.Context, key string, data []byte) error {
	item := &memcache.Item{
		Key:        c.KeyPrefix + key,
		Value:      data,
		Expiration: c.ExpirationSeconds,
	}
	return c.Client.Set(item)
}
