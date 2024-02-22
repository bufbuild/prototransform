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

// Package filecache provides an implementation of prototransform.Cache
// that is based on the file system. Cached data is stored in and loaded
// from files, with cache keys being used to form the file names.
//
// This is the simplest form of caching when sharing cache results is
// not needed and the workload has a persistent volume (e.g. if the
// application restarts, it will still have access to the same cache
// files written before it restarted).
package filecache

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/bufbuild/prototransform"
)

// Config represents the configuration parameters used to
// create a new file-system-backed cache.
type Config struct {
	// Required: the folder in which cached files live.
	Path string
	// Defaults to "cache_" if left empty. This is added to the
	// cache key and the extension below to form a file name.
	// A trailing underscore is not necessary and will be added
	// if not present (to separate prefix from the rest of the
	// cache key).
	FilenamePrefix string
	// Defaults to ".bin" if left empty. This is added to the
	// cache key and prefix above to form a file name.
	FilenameExtension string
	// The mode to use when creating new files in the cache
	// directory. Defaults to 0600 if left zero. If not left
	// as default, the mode must have at least bits 0400 and
	// 0200 (read and write permissions for owner) set.
	FileMode fs.FileMode
}

// New creates a new file-system-backed cache with the given
// configuration.
func New(config Config) (prototransform.Cache, error) {
	// validate config
	if config.Path == "" {
		return nil, errors.New("path cannot be empty")
	}
	path, err := filepath.Abs(config.Path)
	if err != nil {
		return nil, err
	}
	config.Path = path
	if config.FilenamePrefix == "" {
		config.FilenamePrefix = "cache"
	} else {
		config.FilenamePrefix = strings.TrimSuffix(config.FilenamePrefix, "_")
	}
	if config.FilenameExtension == "" {
		config.FilenameExtension = ".bin"
	} else if !strings.HasPrefix(config.FilenameExtension, ".") {
		config.FilenameExtension = "." + config.FilenameExtension
	}
	if config.FileMode == 0 {
		config.FileMode = 0600
	} else if (config.FileMode & 0600) != 0600 {
		return nil, fmt.Errorf("mode %#o must include bits 0600", config.FileMode)
	}

	//  make sure we can write files to cache directory
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("%s is not a directory", path)
	}
	testFile := filepath.Join(path, ".test")
	file, err := os.OpenFile(testFile, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0600)
	if err != nil {
		if os.IsPermission(err) {
			return nil, fmt.Errorf("insufficient permission to create file in %s", path)
		}
		return nil, fmt.Errorf("failed to create file in %s: %w", path, err)
	}
	closeErr := file.Close()
	rmErr := os.Remove(testFile)
	if closeErr != nil {
		return nil, closeErr
	} else if rmErr != nil {
		return nil, rmErr
	}

	return (*cache)(&config), nil
}

type cache Config

func (c *cache) Load(_ context.Context, key string) ([]byte, error) {
	fileName := filepath.Join(c.Path, c.fileNameForKey(key))
	return os.ReadFile(fileName)
}

func (c *cache) Save(_ context.Context, key string, data []byte) error {
	fileName := filepath.Join(c.Path, c.fileNameForKey(key))
	return os.WriteFile(fileName, data, c.FileMode)
}

func (c *cache) fileNameForKey(key string) string {
	if key != "" {
		key = "_" + sanitize(key)
	}
	return c.FilenamePrefix + key + c.FilenameExtension
}

func sanitize(s string) string {
	var builder strings.Builder
	hexWriter := hex.NewEncoder(&builder)
	var buf [1]byte
	for i, length := 0, len(s); i < length; i++ {
		char := s[i]
		switch {
		case char >= 'a' && char <= 'z',
			char >= 'A' && char <= 'Z',
			char >= '0' && char <= '9',
			char == '.' || char == '-' || char == '_':
			builder.WriteByte(char)
		default:
			builder.WriteByte('%')
			buf[0] = char
			_, _ = hexWriter.Write(buf[:])
		}
	}
	return builder.String()
}
