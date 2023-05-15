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

package filecache

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"testing"

	"github.com/bufbuild/prototransform/cache/internal/cachetesting"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFileCache(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name                          string
		config                        Config
		expectPrefix, expectExtension string
		expectMode                    fs.FileMode
	}{
		{
			name:            "default config",
			expectPrefix:    "cache",
			expectExtension: "bin",
			expectMode:      0600,
		},
		{
			name:            "custom prefix with underscore",
			config:          Config{FilenamePrefix: "abc_"},
			expectPrefix:    "abc",
			expectExtension: "bin",
			expectMode:      0600,
		},
		{
			name:            "custom prefix without underscore",
			config:          Config{FilenamePrefix: "abc"},
			expectPrefix:    "abc",
			expectExtension: "bin",
			expectMode:      0600,
		},
		{
			name:            "custom extension",
			config:          Config{FilenameExtension: "cdb"},
			expectPrefix:    "cache",
			expectExtension: "cdb",
			expectMode:      0600,
		},
		{
			name:            "custom mode",
			config:          Config{FileMode: 0740},
			expectPrefix:    "cache",
			expectExtension: "bin",
			expectMode:      0740,
		},
	}
	for _, testCase := range testCases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			tmpDir, err := os.MkdirTemp("", "filecachetest")
			require.NoError(t, err)
			t.Cleanup(func() {
				err := os.RemoveAll(tmpDir)
				assert.NoError(t, err)
			})

			testCase.config.Path = tmpDir
			cache, err := New(testCase.config)
			require.NoError(t, err)
			ctx := context.Background()

			entries := cachetesting.RunSimpleCacheTests(t, ctx, cache)
			files := make(map[string]struct{}, len(entries))
			for k := range entries {
				if k == "" {
					files[fmt.Sprintf("%s.%s", testCase.expectPrefix, testCase.expectExtension)] = struct{}{}
				} else {
					files[fmt.Sprintf("%s_%s.%s", testCase.expectPrefix, k, testCase.expectExtension)] = struct{}{}
				}
			}

			// check the actual files
			checkFiles(t, tmpDir, testCase.expectMode, files)
		})
	}
}

func TestFileCache_ConfigValidation(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name      string
		config    Config
		expectErr string
	}{
		{
			name:      "no path",
			expectErr: "path cannot be empty",
		},
		{
			name:      "bad path",
			config:    Config{Path: "/some/path/that/certainly/does/not/exist/anywhere"},
			expectErr: "no such file or directory",
		},
		{
			name:      "bad mode",
			config:    Config{Path: "./", FileMode: 0111},
			expectErr: "mode 0111 must include bits 0600",
		},
	}
	for _, testCase := range testCases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			_, err := New(testCase.config)
			require.ErrorContains(t, err, testCase.expectErr)
		})
	}
}

func TestSanitize(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		key       string
		sanitized string
	}{
		{
			key:       "only-valid-chars",
			sanitized: "only-valid-chars",
		},
		{
			key:       "buf.build/foo/bar:draft-abc_23489abcf123400de",
			sanitized: "buf.build%2ffoo%2fbar%3adraft-abc_23489abcf123400de",
		},
		{
			key:       "has whitespace",
			sanitized: "has%20whitespace",
		},
		{
			key:       "other &!@#$ funny chars!",
			sanitized: "other%20%26%21%40%23%24%20funny%20chars%21",
		},
		{
			key:       "even unicode!! ↩ ↯ ψ 𝄞 😍 🌍",
			sanitized: "even%20unicode%21%21%20%e2%86%a9%20%e2%86%af%20%cf%88%20%f0%9d%84%9e%20%f0%9f%98%8d%20%f0%9f%8c%8d",
		},
	}
	for _, testCase := range testCases {
		testCase := testCase
		t.Run(testCase.key, func(t *testing.T) {
			t.Parallel()
			val := sanitize(testCase.key)
			require.Equal(t, testCase.sanitized, val)
		})
	}
}

func checkFiles(t *testing.T, dir string, mode fs.FileMode, names map[string]struct{}) {
	t.Helper()
	err := fs.WalkDir(os.DirFS(dir), ".", func(path string, d fs.DirEntry, err error) error {
		if !assert.NoError(t, err) {
			return nil
		}
		if d.IsDir() {
			if path == "." {
				return nil
			}
			t.Errorf("not expecting any sub-directories, found %s", path)
			return fs.SkipDir
		}
		_, ok := names[path]
		if !assert.Truef(t, ok, "not expecting file named %s", path) {
			return nil
		}
		delete(names, path)
		info, err := d.Info()
		if !assert.NoErrorf(t, err, "failed to get file info for %s", path) {
			return nil
		}
		assert.Equal(t, mode, info.Mode())
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, map[string]struct{}{}, names, "some files expected but not found")
}
