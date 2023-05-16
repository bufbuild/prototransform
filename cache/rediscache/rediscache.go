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

// Package rediscache provides an implementation of prototransform.Cache
// that is backed by a Redis instance: https://redis.io/.
package rediscache

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/bufbuild/prototransform"
	"github.com/gomodule/redigo/redis"
)

type Config struct {
	Client     *redis.Pool
	KeyPrefix  string
	Expiration time.Duration
}

func New(config Config) (prototransform.Cache, error) {
	// validate config
	if config.Client == nil {
		return nil, errors.New("client cannot be nil")
	}
	if config.Expiration < 0 {
		return nil, fmt.Errorf("expiration (%v) cannot be negative", config.Expiration)
	}
	return (*cache)(&config), nil
}

type cache Config

func (c *cache) Load(ctx context.Context, key string) ([]byte, error) {
	conn, err := c.Client.GetContext(ctx)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = conn.Close()
	}()
	return redis.Bytes(redis.DoContext(conn, ctx, "get", c.KeyPrefix+key))
}

func (c *cache) Save(ctx context.Context, key string, data []byte) error {
	conn, err := c.Client.GetContext(ctx)
	if err != nil {
		return err
	}
	defer func() {
		_ = conn.Close()
	}()

	args := []any{c.KeyPrefix + key, data}
	if c.Expiration != 0 {
		millis := int(c.Expiration.Milliseconds())
		if millis > 0 {
			args = append(args, "px", millis)
		}
	}
	_, err = redis.DoContext(conn, ctx, "set", args...)
	return err
}
