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

package prototransform

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"buf.build/gen/go/bufbuild/reflect/connectrpc/go/buf/reflect/v1beta1/reflectv1beta1connect"
	reflectv1beta1 "buf.build/gen/go/bufbuild/reflect/protocolbuffers/go/buf/reflect/v1beta1"
	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
)

func TestNewSchemaWatcher(t *testing.T) {
	t.Parallel()
	client := newFakeFileDescriptorSetService()
	poller := NewSchemaPoller(client, "buf.build/foo/bar", "abc")
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	t.Run("schema poller not provided", func(t *testing.T) {
		t.Parallel()
		config := &SchemaWatcherConfig{}
		got, err := NewSchemaWatcher(ctx, config)
		require.Error(t, err)
		assert.EqualError(t, err, "schema poller not provided")
		assert.Nil(t, got)
	})
	t.Run("polling period negative", func(t *testing.T) {
		t.Parallel()
		config := &SchemaWatcherConfig{
			SchemaPoller:  poller,
			PollingPeriod: -1,
		}
		got, err := NewSchemaWatcher(ctx, config)
		require.Error(t, err)
		assert.EqualError(t, err, "polling period duration cannot be negative")
		assert.Nil(t, got)
	})
	t.Run("jitter negative", func(t *testing.T) {
		t.Parallel()
		config := &SchemaWatcherConfig{
			SchemaPoller: poller,
			Jitter:       -1,
		}
		got, err := NewSchemaWatcher(ctx, config)
		require.Error(t, err)
		assert.EqualError(t, err, "jitter cannot be negative")
		assert.Nil(t, got)
	})
	t.Run("jitter > 1", func(t *testing.T) {
		t.Parallel()
		config := &SchemaWatcherConfig{
			SchemaPoller: poller,
			Jitter:       1.1,
		}
		got, err := NewSchemaWatcher(ctx, config)
		require.Error(t, err)
		assert.EqualError(t, err, "jitter cannot be greater than 1.0 (100%)")
		assert.Nil(t, got)
	})
	t.Run("invalid symbol name", func(t *testing.T) {
		t.Parallel()
		config := &SchemaWatcherConfig{
			SchemaPoller:   poller,
			IncludeSymbols: []string{"$poop"},
		}
		got, err := NewSchemaWatcher(ctx, config)
		require.Error(t, err)
		assert.EqualError(t, err, `"$poop" is not a valid symbol name`)
		assert.Nil(t, got)
	})
	t.Run("leaser without cache", func(t *testing.T) {
		t.Parallel()
		type leaser struct {
			Leaser
		}
		config := &SchemaWatcherConfig{
			SchemaPoller: poller,
			Leaser:       leaser{},
		}
		got, err := NewSchemaWatcher(ctx, config)
		require.Error(t, err)
		assert.EqualError(t, err, "leaser config should only be present when cache config also present")
		assert.Nil(t, got)
	})
	t.Run("successfully create schema watcher with default polling period", func(t *testing.T) {
		t.Parallel()
		config := &SchemaWatcherConfig{
			SchemaPoller: poller,
		}
		got, err := NewSchemaWatcher(ctx, config)
		require.NoError(t, err)
		assert.NotNil(t, got)
	})
}

func TestNewSchemaWatcher_cacheKey(t *testing.T) {
	t.Parallel()
	client := newFakeFileDescriptorSetService()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	testCases := []struct {
		name        string
		config      SchemaWatcherConfig
		expectedKey string
	}{
		{
			name: "no cache means no key",
			config: SchemaWatcherConfig{
				SchemaPoller: NewSchemaPoller(client, "buf.build/foo/bar", "blah"),
			},
			expectedKey: "",
		},
		{
			name: "module only",
			config: SchemaWatcherConfig{
				SchemaPoller: NewSchemaPoller(client, "buf.build/foo/bar", ""),
				Cache:        &fakeCache{},
			},
			expectedKey: "buf.build/foo/bar",
		},
		{
			name: "module and version",
			config: SchemaWatcherConfig{
				SchemaPoller: NewSchemaPoller(client, "buf.build/foo/bar", "blah"),
				Cache:        &fakeCache{},
			},
			expectedKey: "buf.build/foo/bar:blah",
		},
		{
			name: "module and symbol",
			config: SchemaWatcherConfig{
				SchemaPoller:   NewSchemaPoller(client, "buf.build/foo/bar", ""),
				IncludeSymbols: []string{"foo.Bar"},
				Cache:          &fakeCache{},
			},
			expectedKey: "buf.build/foo/bar_a410ef29f1e91dc3867dac52dc5006b0ffc8fca90bc3cf2a70b417a2cc46476f",
		},
		{
			name: "module and symbols",
			config: SchemaWatcherConfig{
				SchemaPoller:   NewSchemaPoller(client, "buf.build/foo/bar", ""),
				IncludeSymbols: []string{"frob.Nitz", "gyzmeaux.Thing", "" /* unnamed package */, "foo.Bar"},
				Cache:          &fakeCache{},
			},
			expectedKey: "buf.build/foo/bar_4dc0cce9f82a1cc92bd90cf759a5e07a2650f2ef2d6e1eedc8be478d93d5bf10",
		},
		{
			name: "module, version, and symbol",
			config: SchemaWatcherConfig{
				SchemaPoller:   NewSchemaPoller(client, "buf.build/foo/bar", "blah"),
				IncludeSymbols: []string{"foo.Bar"},
				Cache:          &fakeCache{},
			},
			expectedKey: "buf.build/foo/bar:blah_a410ef29f1e91dc3867dac52dc5006b0ffc8fca90bc3cf2a70b417a2cc46476f",
		},
	}
	for _, testCase := range testCases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			watcher, err := NewSchemaWatcher(ctx, &testCase.config)
			require.NoError(t, err)
			require.Equal(t, testCase.expectedKey, watcher.cacheKey)
		})
	}
}

func TestSchemaWatcher_getFileDescriptorSet(t *testing.T) {
	t.Parallel()
	s := &SchemaWatcher{
		poller: NewSchemaPoller(newFakeFileDescriptorSetService(), "", ""),
	}
	got, version, _, err := s.getFileDescriptorSet(context.Background())
	require.NoError(t, err)
	require.NotNil(t, got)
	require.NotEmpty(t, version)
}

func TestSchemaWatcher_FindExtensionByName(t *testing.T) {
	t.Parallel()
	resolver, err := newResolver(fakeFileDescriptorSet())
	require.NoError(t, err)
	schemaWatcher := &SchemaWatcher{
		resolver: resolver,
	}
	got, err := schemaWatcher.FindExtensionByName("foo.bar.xt")
	require.NoError(t, err)
	assert.Equal(t, "foo.bar.xt", string(got.TypeDescriptor().FullName()))
}

func TestSchemaWatcher_FindExtensionByNumber(t *testing.T) {
	t.Parallel()
	resolver, err := newResolver(fakeFileDescriptorSet())
	require.NoError(t, err)
	schemaWatcher := &SchemaWatcher{
		resolver: resolver,
	}
	got, err := schemaWatcher.FindExtensionByNumber("foo.bar.Message", protowire.Number(123))
	require.NoError(t, err)
	assert.Equal(t, "foo.bar.xt", string(got.TypeDescriptor().FullName()))
}

func TestSchemaWatcher_FindMessageByName(t *testing.T) {
	t.Parallel()
	resolver, err := newResolver(fakeFileDescriptorSet())
	require.NoError(t, err)
	schemaWatcher := &SchemaWatcher{
		resolver: resolver,
	}
	got, err := schemaWatcher.FindMessageByName("foo.bar.Message")
	require.NoError(t, err)
	assert.Equal(t, "foo.bar.Message", string(got.Descriptor().FullName()))
}

func TestSchemaWatcher_FindMessageByURL(t *testing.T) {
	t.Parallel()
	resolver, err := newResolver(fakeFileDescriptorSet())
	require.NoError(t, err)
	schemaWatcher := &SchemaWatcher{
		resolver: resolver,
	}
	got, err := schemaWatcher.FindMessageByURL("foo.bar.Message")
	require.NoError(t, err)
	assert.Equal(t, "foo.bar.Message", string(got.Descriptor().FullName()))
}

func TestSchemaWatcher_getResolver(t *testing.T) {
	t.Parallel()
	want := &resolver{}
	schemaWatcher := &SchemaWatcher{resolver: want}
	assert.True(t, schemaWatcher.resolverMu.TryRLock())
	assert.Equal(t, want, schemaWatcher.getResolver())
	assert.True(t, schemaWatcher.resolverMu.TryRLock())
}

func TestSchemaWatcher_updateResolver(t *testing.T) {
	t.Parallel()
	t.Run("updateResolver succeeds", func(t *testing.T) {
		t.Parallel()
		emptySchema := &descriptorpb.FileDescriptorSet{
			File: []*descriptorpb.FileDescriptorProto{
				{
					Name:    proto.String("test.proto"),
					Syntax:  proto.String("proto2"),
					Package: proto.String("foo.bar"),
				},
			},
		}
		resolver, err := newResolver(emptySchema)
		require.NoError(t, err)

		schemaWatcher := &SchemaWatcher{
			poller:   NewSchemaPoller(newFakeFileDescriptorSetService(), "", ""),
			resolver: resolver,
		}
		_, err = schemaWatcher.resolver.FindMessageByName("foo.bar.Message")
		require.Error(t, err, "not found")
		require.NoError(t, schemaWatcher.updateResolver(context.Background()))
		got, err := schemaWatcher.resolver.FindMessageByName("foo.bar.Message")
		require.NoError(t, err)
		assert.Equal(t, "foo.bar.Message", string(got.Descriptor().FullName()))
	})
	t.Run("updateResolver fails", func(t *testing.T) {
		t.Parallel()
		schemaWatcher := &SchemaWatcher{
			poller: NewSchemaPoller(&fakeFileDescriptorSetService{
				getFileDescriptorSetFunc: func(context.Context, *connect.Request[reflectv1beta1.GetFileDescriptorSetRequest]) (*connect.Response[reflectv1beta1.GetFileDescriptorSetResponse], error) {
					return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("foo"))
				},
			}, "", ""),
		}
		err := schemaWatcher.updateResolver(context.Background())
		require.Error(t, err)
		assert.EqualError(t, err, "failed to fetch schema: internal: foo")
	})
	t.Run("updateResolver fails", func(t *testing.T) {
		t.Parallel()
		emptySchema := &descriptorpb.FileDescriptorSet{
			File: []*descriptorpb.FileDescriptorProto{
				{},
			},
		}
		schemaWatcher := &SchemaWatcher{
			poller: NewSchemaPoller(&fakeFileDescriptorSetService{
				getFileDescriptorSetFunc: func(context.Context, *connect.Request[reflectv1beta1.GetFileDescriptorSetRequest]) (*connect.Response[reflectv1beta1.GetFileDescriptorSetResponse], error) {
					return connect.NewResponse(&reflectv1beta1.GetFileDescriptorSetResponse{
						FileDescriptorSet: emptySchema,
					}), nil
				},
			}, "", ""),
		}
		err := schemaWatcher.updateResolver(context.Background())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unable to create resolver from schema:")
	})
}

func TestSchemaWatcher_Stop(t *testing.T) {
	t.Parallel()
	t.Run("stops", func(t *testing.T) {
		t.Parallel()
		var stopped bool
		schemaWatcher := &SchemaWatcher{
			stop: func() { stopped = true },
		}
		assert.False(t, schemaWatcher.IsStopped())
		schemaWatcher.Stop()
		assert.True(t, stopped)
		assert.True(t, schemaWatcher.IsStopped())
	})
	t.Run("already stopped", func(t *testing.T) {
		t.Parallel()
		schemaWatcher := &SchemaWatcher{
			stop: nil,
		}
		assert.True(t, schemaWatcher.IsStopped())
		schemaWatcher.Stop()
		assert.True(t, schemaWatcher.IsStopped())
	})
}

func TestSchemaWatcher_AwaitReady(t *testing.T) {
	t.Parallel()
	t.Run("delays in becoming ready", func(t *testing.T) {
		t.Parallel()
		svc := newFakeFileDescriptorSetService()
		getFiles := svc.getFileDescriptorSetFunc
		latch := make(chan struct{})
		svc.getFileDescriptorSetFunc = func(ctx context.Context, req *connect.Request[reflectv1beta1.GetFileDescriptorSetRequest]) (*connect.Response[reflectv1beta1.GetFileDescriptorSetResponse], error) {
			<-latch
			return getFiles(ctx, req)
		}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		watcher, err := NewSchemaWatcher(ctx, &SchemaWatcherConfig{
			SchemaPoller: NewSchemaPoller(svc, "foo/bar", ""),
		})
		require.NoError(t, err)

		timeoutCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
		defer cancel()
		err = watcher.AwaitReady(timeoutCtx)
		require.Error(t, err)
		require.True(t, errors.Is(err, context.DeadlineExceeded))
		ok, _ := watcher.LastResolved()
		require.False(t, ok)
		require.Nil(t, watcher.ResolvedSchema())

		timestamp := time.Now()
		close(latch)
		timeoutCtx, cancel = context.WithTimeout(ctx, 500*time.Millisecond)
		defer cancel()
		err = watcher.AwaitReady(timeoutCtx)
		require.NoError(t, err)
		ok, resolvedTime := watcher.LastResolved()
		require.True(t, ok)
		require.False(t, resolvedTime.Before(timestamp))
		require.NotNil(t, watcher.ResolvedSchema())
	})
	t.Run("errors before becoming ready", func(t *testing.T) {
		t.Parallel()
		svc := newFakeFileDescriptorSetService()
		getFiles := svc.getFileDescriptorSetFunc
		var shouldSucceed atomic.Bool
		svc.getFileDescriptorSetFunc = func(ctx context.Context, req *connect.Request[reflectv1beta1.GetFileDescriptorSetRequest]) (*connect.Response[reflectv1beta1.GetFileDescriptorSetResponse], error) {
			if !shouldSucceed.Load() {
				return nil, connect.NewError(connect.CodeUnavailable, errors.New("unavailable"))
			}
			return getFiles(ctx, req)
		}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		watcher, err := NewSchemaWatcher(ctx, &SchemaWatcherConfig{
			SchemaPoller:  NewSchemaPoller(svc, "foo/bar", ""),
			PollingPeriod: 100 * time.Millisecond,
		})
		require.NoError(t, err)

		timeoutCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
		defer cancel()
		err = watcher.AwaitReady(timeoutCtx)
		// download should have failed at least once so this should be the RPC error
		require.Error(t, err)
		var connErr *connect.Error
		require.True(t, errors.As(err, &connErr))
		require.Equal(t, connect.CodeUnavailable, connErr.Code())
		ok, _ := watcher.LastResolved()
		require.False(t, ok)

		timestamp := time.Now()
		shouldSucceed.Store(true)
		timeoutCtx, cancel = context.WithTimeout(ctx, 500*time.Millisecond)
		defer cancel()
		err = watcher.AwaitReady(timeoutCtx)
		require.NoError(t, err)
		ok, resolvedTime := watcher.LastResolved()
		require.True(t, ok)
		require.False(t, resolvedTime.Before(timestamp))
	})
	t.Run("never becomes ready", func(t *testing.T) {
		t.Parallel()
		brokenService := &fakeFileDescriptorSetService{
			getFileDescriptorSetFunc: func(context.Context, *connect.Request[reflectv1beta1.GetFileDescriptorSetRequest]) (*connect.Response[reflectv1beta1.GetFileDescriptorSetResponse], error) {
				return nil, connect.NewError(connect.CodeUnavailable, errors.New("unavailable"))
			},
		}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		watcher, err := NewSchemaWatcher(ctx, &SchemaWatcherConfig{
			SchemaPoller: NewSchemaPoller(brokenService, "foo/bar", ""),
		})
		require.NoError(t, err)
		watcher.Stop()
		err = watcher.AwaitReady(ctx)
		require.Error(t, err)
		require.True(t, errors.Is(err, ErrSchemaWatcherStopped))
	})
}

func TestSchemaWatcher_UsingCache(t *testing.T) {
	t.Parallel()
	brokenService := &fakeFileDescriptorSetService{
		getFileDescriptorSetFunc: func(context.Context, *connect.Request[reflectv1beta1.GetFileDescriptorSetRequest]) (*connect.Response[reflectv1beta1.GetFileDescriptorSetResponse], error) {
			return nil, connect.NewError(connect.CodeUnavailable, errors.New("unavailable"))
		},
	}
	t.Run("loads from empty cache", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var loadHookCalled sync.Once
		loadHookChan := make(chan struct{})
		cache := &fakeCache{
			loadHook: func() {
				loadHookCalled.Do(func() {
					close(loadHookChan)
				})
			},
		}

		watcher, err := NewSchemaWatcher(ctx, &SchemaWatcherConfig{
			SchemaPoller: NewSchemaPoller(brokenService, "foo/bar", "main"),
			Cache:        cache,
		})
		require.NoError(t, err)

		select {
		case <-time.After(time.Second):
			t.Fatal("cache.Load never called")
		case <-loadHookChan:
		}
		loads := cache.getLoadCalls()
		require.GreaterOrEqual(t, len(loads), 1) // racing w/ retry so could be >1
		require.Equal(t, "foo/bar:main", loads[0].key)
		saves := cache.getSaveCalls()
		require.Equal(t, 0, len(saves))

		// schema watcher cannot become ready if service unavailable and cache has no entry
		_, err = watcher.FindMessageByName("foo.bar.Message")
		require.Error(t, err)
		require.True(t, errors.Is(err, ErrSchemaWatcherNotReady))
		ok, _ := watcher.LastResolved()
		require.False(t, ok)
	})
	t.Run("loads from populated cache", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		cache := &fakeCache{}
		// seed cache
		files := fakeFileDescriptorSet()
		timestamp := time.Now()
		data, err := encodeForCache("foo/bar:main", nil, files, "abcdefg", timestamp)
		require.NoError(t, err)
		err = cache.Save(ctx, "foo/bar:main", data)
		require.NoError(t, err)

		watcher, err := NewSchemaWatcher(ctx, &SchemaWatcherConfig{
			SchemaPoller: NewSchemaPoller(brokenService, "foo/bar", "main"),
			Cache:        cache,
		})
		require.NoError(t, err)

		readyCtx, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		err = watcher.AwaitReady(readyCtx)
		require.NoError(t, err)
		loads := cache.getLoadCalls()
		require.Equal(t, 1, len(loads))
		require.Equal(t, "foo/bar:main", loads[0].key)
		saves := cache.getSaveCalls()
		require.Equal(t, 1, len(saves)) // just one call to seed cache above

		_, err = watcher.FindMessageByName("foo.bar.Message")
		require.NoError(t, err)
		ok, resolveTime := watcher.LastResolved() // timestamp from cache
		require.True(t, ok)
		require.True(t, timestamp.Equal(resolveTime))
	})
	t.Run("saves to cache", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var saveHookCalled sync.Once
		saveHookChan := make(chan struct{})
		cache := &fakeCache{
			saveHook: func() {
				saveHookCalled.Do(func() {
					close(saveHookChan)
				})
			},
		}

		timestamp := time.Now()
		watcher, err := NewSchemaWatcher(ctx, &SchemaWatcherConfig{
			SchemaPoller: NewSchemaPoller(newFakeFileDescriptorSetService(), "foo/bar", "main"),
			Cache:        cache,
		})
		require.NoError(t, err)

		select {
		case <-time.After(time.Second):
			t.Fatal("cache.Save never called")
		case <-saveHookChan:
		}
		saves := cache.getSaveCalls()
		require.Equal(t, 1, len(saves))

		readyCtx, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		err = watcher.AwaitReady(readyCtx)
		require.NoError(t, err)
		_, err = watcher.FindMessageByName("foo.bar.Message")
		require.NoError(t, err)
		ok, resolveTime := watcher.LastResolved()
		require.True(t, ok)
		require.False(t, resolveTime.Before(timestamp))
	})
}

func TestSchemaWatcher_UsingLeaser(t *testing.T) {
	t.Parallel()
	svc := newFakeFileDescriptorSetService()
	var badServiceCalls atomic.Int32
	shouldNotUseService := &fakeFileDescriptorSetService{
		getFileDescriptorSetFunc: func(context.Context, *connect.Request[reflectv1beta1.GetFileDescriptorSetRequest]) (*connect.Response[reflectv1beta1.GetFileDescriptorSetResponse], error) {
			badServiceCalls.Add(1)
			return nil, connect.NewError(connect.CodeUnavailable, errors.New("unavailable"))
		},
	}
	cached := make(chan struct{}, 1)
	cache := &fakeCache{
		saveHook: func() {
			select {
			case cached <- struct{}{}:
			default:
			}
		},
	}
	leaser := &fakeLeaser{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	leader, err := NewSchemaWatcher(ctx, &SchemaWatcherConfig{
		SchemaPoller: NewSchemaPoller(svc, "foo/bar", "main"),
		Cache:        cache,
		Leaser:       leaser,
	})
	require.NoError(t, err)

	// Leader becomes ready without cache.
	awaitCtx, awaitCancel := context.WithTimeout(ctx, time.Second)
	defer awaitCancel()
	err = leader.AwaitReady(awaitCtx)
	require.NoError(t, err)

	// make sure leader's background save to cache is complete
	select {
	case <-time.After(time.Second):
		require.FailNow(t, "cache entry never saved")
	case <-cached:
	}

	// Followers all use the cache and will not poll because they don't have lease.
	for i := 0; i < 3; i++ {
		follower, err := NewSchemaWatcher(ctx, &SchemaWatcherConfig{
			SchemaPoller: NewSchemaPoller(shouldNotUseService, "foo/bar", "main"),
			Cache:        cache,
			Leaser:       leaser,
		})
		require.NoError(t, err)

		// Followers become ready from the cache.
		awaitCtx, awaitCancel := context.WithTimeout(ctx, time.Second)
		defer awaitCancel()
		err = follower.AwaitReady(awaitCtx)
		require.NoError(t, err)
	}

	// Followers shouldn't even try to poll since they don't have lease.
	require.Equal(t, int32(0), badServiceCalls.Load())

	// One cache load for each follower
	loads := cache.getLoadCalls()
	require.Equal(t, 3, len(loads))
}

func TestSchemaWatcher_callbacks(t *testing.T) {
	t.Parallel()

	var descriptors atomic.Pointer[descriptorpb.FileDescriptorSet]
	descriptors.Store(fakeFileDescriptorSet())
	var count atomic.Int32
	done := make(chan struct{})
	svc := &fakeFileDescriptorSetService{
		getFileDescriptorSetFunc: func(context.Context, *connect.Request[reflectv1beta1.GetFileDescriptorSetRequest]) (*connect.Response[reflectv1beta1.GetFileDescriptorSetResponse], error) {
			result := descriptors.Load()
			switch count.Add(1) {
			case 1:
				// remove so next call returns error
				descriptors.Store(nil)
			case 2:
				// back to previous (should not result in a callback since schema unchanged)
				descriptors.Store(fakeFileDescriptorSet())
			case 3:
				// now change descriptors for next call
				newDescriptors := fakeFileDescriptorSet()
				newDescriptors.File[0].MessageType[0].Name = proto.String("NewMessageName")
				descriptors.Store(newDescriptors)
			case 4:
				// do nothing, no change so should not result in a callback
			case 5:
				// let test know we've seen 'em all
				close(done)
			}
			if result == nil {
				return nil, connect.NewError(connect.CodeInternal, errors.New("no descriptors to return"))
			}
			return connect.NewResponse(&reflectv1beta1.GetFileDescriptorSetResponse{
				FileDescriptorSet: result,
				Version:           "main",
			}), nil
		},
	}

	notices := make(chan string, 4)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, err := NewSchemaWatcher(ctx, &SchemaWatcherConfig{
		SchemaPoller:  NewSchemaPoller(svc, "foo/bar", ""),
		PollingPeriod: 50 * time.Millisecond,
		OnUpdate: func(w *SchemaWatcher) {
			require.NotNil(t, w.ResolvedSchema())
			fd, err := w.FindFileByPath("test.proto")
			require.NoError(t, err)
			msg := fd.Messages().Get(0)
			select {
			case notices <- fmt.Sprintf("update: %s", msg.Name()):
			default:
			}
		},
		OnError: func(_ *SchemaWatcher, err error) {
			select {
			case notices <- fmt.Sprintf("error: %v", err):
			default:
			}
		},
	})
	require.NoError(t, err)
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatalf("schema poller not invoked expected number of times; want 5, got %d", count.Load())
	}
	var notice string
	getNextNotice := func() {
		select {
		case notice = <-notices:
		default:
			t.Fatal("callback not invoked")
		}
	}
	getNextNotice()
	require.Equal(t, "update: Message", notice) // initial resolution
	getNextNotice()
	require.Equal(t, "error: failed to fetch schema: internal: no descriptors to return", notice)
	getNextNotice()
	require.Equal(t, "update: NewMessageName", notice)
	// should be no more
	select {
	case notice = <-notices:
		t.Fatal("extra callback invocation")
	default:
	}
}

type fakeFileDescriptorSetService struct {
	reflectv1beta1connect.UnimplementedFileDescriptorSetServiceHandler
	getFileDescriptorSetFunc func(context.Context, *connect.Request[reflectv1beta1.GetFileDescriptorSetRequest]) (*connect.Response[reflectv1beta1.GetFileDescriptorSetResponse], error)
}

func (s *fakeFileDescriptorSetService) GetFileDescriptorSet(ctx context.Context, req *connect.Request[reflectv1beta1.GetFileDescriptorSetRequest]) (*connect.Response[reflectv1beta1.GetFileDescriptorSetResponse], error) {
	return s.getFileDescriptorSetFunc(ctx, req)
}

func newFakeFileDescriptorSetService() *fakeFileDescriptorSetService {
	return &fakeFileDescriptorSetService{
		getFileDescriptorSetFunc: func(context.Context, *connect.Request[reflectv1beta1.GetFileDescriptorSetRequest]) (*connect.Response[reflectv1beta1.GetFileDescriptorSetResponse], error) {
			return connect.NewResponse(&reflectv1beta1.GetFileDescriptorSetResponse{
				FileDescriptorSet: fakeFileDescriptorSet(),
				Version:           "main",
			}), nil
		},
	}
}

func fakeFileDescriptorSet() *descriptorpb.FileDescriptorSet {
	return &descriptorpb.FileDescriptorSet{
		File: []*descriptorpb.FileDescriptorProto{
			{
				Name:    proto.String("test.proto"),
				Syntax:  proto.String("proto2"),
				Package: proto.String("foo.bar"),
				MessageType: []*descriptorpb.DescriptorProto{
					{
						Name: proto.String("Message"),
						Field: []*descriptorpb.FieldDescriptorProto{
							{
								Name:     proto.String("name"),
								Number:   proto.Int32(1),
								Type:     descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
								Label:    descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
								JsonName: proto.String("name"),
							},
							{
								Name:     proto.String("id"),
								Number:   proto.Int32(2),
								Type:     descriptorpb.FieldDescriptorProto_TYPE_INT64.Enum(),
								Label:    descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
								JsonName: proto.String("id"),
							},
							{
								Name:     proto.String("child"),
								Number:   proto.Int32(3),
								Type:     descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum(),
								TypeName: proto.String(".foo.bar.Message"),
								Label:    descriptorpb.FieldDescriptorProto_LABEL_REPEATED.Enum(),
								JsonName: proto.String("children"),
							},
							{
								Name:     proto.String("kind"),
								Number:   proto.Int32(4),
								Type:     descriptorpb.FieldDescriptorProto_TYPE_ENUM.Enum(),
								TypeName: proto.String(".foo.bar.Kind"),
								Label:    descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
								JsonName: proto.String("kind"),
							},
						},
						ExtensionRange: []*descriptorpb.DescriptorProto_ExtensionRange{
							{
								Start: proto.Int32(100),
								End:   proto.Int32(10000),
							},
						},
					},
				},
				EnumType: []*descriptorpb.EnumDescriptorProto{
					{
						Name: proto.String("Kind"),
						Value: []*descriptorpb.EnumValueDescriptorProto{
							{
								Name:   proto.String("UNKNOWN"),
								Number: proto.Int32(0),
							},
							{
								Name:   proto.String("GOOD"),
								Number: proto.Int32(1),
							},
							{
								Name:   proto.String("BAD"),
								Number: proto.Int32(2),
							},
							{
								Name:   proto.String("UGLY"),
								Number: proto.Int32(3),
							},
						},
					},
				},
				Extension: []*descriptorpb.FieldDescriptorProto{
					{
						Name:     proto.String("xt"),
						Extendee: proto.String(".foo.bar.Message"),
						Number:   proto.Int32(123),
						Type:     descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
					},
				},
			},
		},
	}
}

type fakeCacheOp struct {
	key  string
	data []byte
	err  error
}

type fakeCache struct {
	mu       sync.Mutex
	cache    map[string][]byte
	loads    []fakeCacheOp
	saves    []fakeCacheOp
	loadHook func()
	saveHook func()
}

func (f *fakeCache) Load(_ context.Context, key string) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.loadHook != nil {
		defer f.loadHook()
	}

	// read cache
	var err error
	data, ok := f.cache[key]
	if !ok {
		err = fmt.Errorf("not found: %q", key)
	}

	// add to operation log
	f.loads = append(f.loads, fakeCacheOp{
		key: key, data: data, err: err,
	})

	// done
	if err != nil {
		return nil, err
	}
	return data, err
}

func (f *fakeCache) Save(_ context.Context, key string, data []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.saveHook != nil {
		defer f.saveHook()
	}

	// add to cache
	if f.cache == nil {
		f.cache = map[string][]byte{}
	}
	f.cache[key] = data

	// add to operation log
	f.saves = append(f.saves, fakeCacheOp{
		key: key, data: data,
	})

	// done
	return nil
}

func (f *fakeCache) getLoadCalls() []fakeCacheOp {
	f.mu.Lock()
	defer f.mu.Unlock()
	clone := make([]fakeCacheOp, len(f.loads))
	copy(clone, f.loads)
	return clone
}

func (f *fakeCache) getSaveCalls() []fakeCacheOp {
	f.mu.Lock()
	defer f.mu.Unlock()
	clone := make([]fakeCacheOp, len(f.saves))
	copy(clone, f.saves)
	return clone
}

type fakeLeaser struct {
	mu     sync.Mutex
	leases map[string][]*fakeLease
}

func (f *fakeLeaser) NewLease(ctx context.Context, leaseName string, leaseHolder []byte) Lease {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.leases == nil {
		f.leases = map[string][]*fakeLease{}
	}
	ctx, cancel := context.WithCancel(ctx)
	lease := &fakeLease{cancel: cancel, leaser: f, leaseName: leaseName, leaseHolder: leaseHolder}
	f.leases[leaseName] = append(f.leases[leaseName], lease)
	go func() {
		<-ctx.Done()
		f.removeLease(lease)
	}()
	return lease
}

func (f *fakeLeaser) getLeases(name string) []*fakeLease {
	f.mu.Lock()
	defer f.mu.Unlock()
	leases := f.leases[name]
	if len(leases) == 0 {
		return nil
	}
	clone := make([]*fakeLease, len(leases))
	copy(clone, leases)
	return clone
}

func (f *fakeLeaser) removeLease(leaseToRemove *fakeLease) {
	f.mu.Lock()
	defer f.mu.Unlock()
	leases := f.leases[leaseToRemove.leaseName]
	newLeases := make([]*fakeLease, 0, len(leases))
	for i := range leases {
		if leases[i] == leaseToRemove {
			continue
		}
		newLeases = append(newLeases, leases[i])
	}
	f.leases[leaseToRemove.leaseName] = newLeases
}

type fakeLease struct {
	cancel      context.CancelFunc
	leaser      *fakeLeaser
	leaseName   string
	leaseHolder []byte
}

func (f *fakeLease) IsHeld() (bool, error) {
	leases := f.leaser.getLeases(f.leaseName)
	return len(leases) > 0 && leases[0] == f, nil
}

func (f *fakeLease) SetCallbacks(_, _ func()) {
	// ignore
}

func (f *fakeLease) Cancel() {
	f.cancel()
}
