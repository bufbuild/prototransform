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

	"buf.build/gen/go/bufbuild/reflect/bufbuild/connect-go/buf/reflect/v1beta1/reflectv1beta1connect"
	"buf.build/gen/go/bufbuild/reflect/protocolbuffers/go/buf/reflect/v1beta1"
	"github.com/bufbuild/connect-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
)

func TestNewSchemaWatcher(t *testing.T) {
	t.Parallel()
	client := newFakeFileDescriptorSetService()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	t.Run("schema service client not provided", func(t *testing.T) {
		t.Parallel()
		config := &Config{}
		got, err := NewSchemaWatcher(ctx, config)
		require.Error(t, err)
		assert.EqualError(t, err, "schema service client not provided")
		assert.Nil(t, got)
	})
	t.Run("buf module not provided", func(t *testing.T) {
		t.Parallel()
		config := &Config{
			Client: client,
		}
		got, err := NewSchemaWatcher(ctx, config)
		require.Error(t, err)
		assert.EqualError(t, err, "buf module not provided")
		assert.Nil(t, got)
	})
	t.Run("invalid symbol name", func(t *testing.T) {
		t.Parallel()
		config := &Config{
			Client:         client,
			Module:         "buf.build/foo/bar",
			IncludeSymbols: []string{"$poop"},
		}
		got, err := NewSchemaWatcher(ctx, config)
		require.Error(t, err)
		assert.EqualError(t, err, `"$poop" is not a valid symbol name`)
		assert.Nil(t, got)
	})
	t.Run("invalid symbol name", func(t *testing.T) {
		t.Parallel()
		config := &Config{
			Client:         client,
			Module:         "buf.build/foo/bar",
			IncludeSymbols: []string{"$poop"},
		}
		got, err := NewSchemaWatcher(ctx, config)
		require.Error(t, err)
		assert.EqualError(t, err, `"$poop" is not a valid symbol name`)
		assert.Nil(t, got)
	})
	t.Run("successfully create schema watcher with default polling period", func(t *testing.T) {
		t.Parallel()
		config := &Config{
			Client: client,
			Module: "buf.build/foo/bar",
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
	defer cancel()
	testCases := []struct {
		name        string
		config      Config
		expectedKey string
	}{
		{
			name: "no cache means no key",
			config: Config{
				Client:  client,
				Module:  "buf.build/foo/bar",
				Version: "blah",
			},
			expectedKey: "",
		},
		{
			name: "module only",
			config: Config{
				Client: client,
				Module: "buf.build/foo/bar",
				Cache:  &fakeCache{},
			},
			expectedKey: "buf.build/foo/bar",
		},
		{
			name: "module and version",
			config: Config{
				Client:  client,
				Module:  "buf.build/foo/bar",
				Version: "blah",
				Cache:   &fakeCache{},
			},
			expectedKey: "buf.build/foo/bar@blah",
		},
		{
			name: "module and symbol",
			config: Config{
				Client:         client,
				Module:         "buf.build/foo/bar",
				IncludeSymbols: []string{"foo.Bar"},
				Cache:          &fakeCache{},
			},
			expectedKey: "buf.build/foo/bar;foo.Bar",
		},
		{
			name: "module and symbols",
			config: Config{
				Client:         client,
				Module:         "buf.build/foo/bar",
				IncludeSymbols: []string{"frob.Nitz", "gyzmeaux.Thing", "" /* unnamed package */, "foo.Bar"},
				Cache:          &fakeCache{},
			},
			expectedKey: "buf.build/foo/bar;,foo.Bar,frob.Nitz,gyzmeaux.Thing",
		},
		{
			name: "module, version, and symbol",
			config: Config{
				Client:         client,
				Module:         "buf.build/foo/bar",
				Version:        "blah",
				IncludeSymbols: []string{"foo.Bar"},
				Cache:          &fakeCache{},
			},
			expectedKey: "buf.build/foo/bar@blah;foo.Bar",
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			sw, err := NewSchemaWatcher(ctx, &testCase.config)
			require.NoError(t, err)
			require.Equal(t, testCase.expectedKey, sw.cacheKey)
		})
	}
}

func TestSchemaWatcher_getFileDescriptorSet(t *testing.T) {
	t.Parallel()
	s := &SchemaWatcher{
		poller: NewDescriptorPoller(newFakeFileDescriptorSetService(), "", ""),
	}
	got, _, err := s.getFileDescriptorSet(context.Background())
	require.NoError(t, err)
	require.NotNil(t, got)
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
	want := fakeResolver{}
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
			poller:   NewDescriptorPoller(newFakeFileDescriptorSetService(), "", ""),
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
			poller: NewDescriptorPoller(&fakeFileDescriptorSetService{
				getFileDescriptorSetFunc: func(context.Context, *connect.Request[reflectv1beta1.GetFileDescriptorSetRequest]) (*connect.Response[reflectv1beta1.GetFileDescriptorSetResponse], error) {
					return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("foo"))
				},
			}, "", ""),
		}
		err := schemaWatcher.updateResolver(context.Background())
		require.Error(t, err)
		assert.EqualError(t, err, "failed to get schema from remote: internal: foo")
	})
	t.Run("updateResolver fails", func(t *testing.T) {
		t.Parallel()
		emptySchema := &descriptorpb.FileDescriptorSet{
			File: []*descriptorpb.FileDescriptorProto{
				{},
			},
		}
		schemaWatcher := &SchemaWatcher{
			poller: NewDescriptorPoller(&fakeFileDescriptorSetService{
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
		sw, err := NewSchemaWatcher(ctx, &Config{Client: svc, Module: "foo/bar"})
		require.NoError(t, err)

		timeoutCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
		defer cancel()
		err = sw.AwaitReady(timeoutCtx)
		require.Error(t, err)
		require.True(t, errors.Is(err, context.DeadlineExceeded))
		ok, _ := sw.LastResolved()
		require.False(t, ok)

		ts := time.Now()
		close(latch)
		timeoutCtx, cancel = context.WithTimeout(ctx, 500*time.Millisecond)
		defer cancel()
		err = sw.AwaitReady(timeoutCtx)
		require.NoError(t, err)
		ok, resolvedTime := sw.LastResolved()
		require.True(t, ok)
		require.False(t, resolvedTime.Before(ts))
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
		sw, err := NewSchemaWatcher(ctx, &Config{Client: svc, Module: "foo/bar", PollingPeriod: 100 * time.Millisecond})
		require.NoError(t, err)

		timeoutCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
		defer cancel()
		err = sw.AwaitReady(timeoutCtx)
		// download should have failed at least once so this should be the RPC error
		require.Error(t, err)
		var connErr *connect.Error
		require.True(t, errors.As(err, &connErr))
		require.Equal(t, connect.CodeUnavailable, connErr.Code())
		ok, _ := sw.LastResolved()
		require.False(t, ok)

		ts := time.Now()
		shouldSucceed.Store(true)
		timeoutCtx, cancel = context.WithTimeout(ctx, 500*time.Millisecond)
		defer cancel()
		err = sw.AwaitReady(timeoutCtx)
		require.NoError(t, err)
		ok, resolvedTime := sw.LastResolved()
		require.True(t, ok)
		require.False(t, resolvedTime.Before(ts))
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
		sw, err := NewSchemaWatcher(ctx, &Config{Client: brokenService, Module: "foo/bar"})
		require.NoError(t, err)
		sw.Stop()
		err = sw.AwaitReady(ctx)
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
		ch := make(chan struct{})
		cache := &fakeCache{
			loadHook: func() {
				loadHookCalled.Do(func() {
					close(ch)
				})
			},
		}

		sw, err := NewSchemaWatcher(ctx, &Config{Client: brokenService, Module: "foo/bar", Version: "main", Cache: cache})
		require.NoError(t, err)

		select {
		case <-time.After(time.Second):
			t.Fatal("cache.Load never called")
		case <-ch:
		}
		loads := cache.getLoadCalls()
		require.GreaterOrEqual(t, len(loads), 1) // racing w/ retry so could be >1
		require.Equal(t, loads[0].key, "foo/bar@main")
		saves := cache.getSaveCalls()
		require.Equal(t, 0, len(saves))

		// schema watcher cannot become ready if service unavailable and cache has no entry
		_, err = sw.FindMessageByName("foo.bar.Message")
		require.Error(t, err)
		require.True(t, errors.Is(err, ErrSchemaWatcherNotReady))
		ok, _ := sw.LastResolved()
		require.False(t, ok)
	})
	t.Run("loads from populated cache", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		cache := &fakeCache{}
		// seed cache
		files := fakeFileDescriptorSet()
		ts := time.Now()
		data, err := encodeForCache(
			&reflectv1beta1.GetFileDescriptorSetResponse{
				FileDescriptorSet: files,
				Version:           "abcdefg",
			},
			ts)
		require.NoError(t, err)
		err = cache.Save("foo/bar@main", data)
		require.NoError(t, err)

		sw, err := NewSchemaWatcher(ctx, &Config{Client: brokenService, Module: "foo/bar", Version: "main", Cache: cache})
		require.NoError(t, err)

		readyCtx, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		err = sw.AwaitReady(readyCtx)
		require.NoError(t, err)
		loads := cache.getLoadCalls()
		require.Equal(t, 1, len(loads))
		require.Equal(t, loads[0].key, "foo/bar@main")
		saves := cache.getSaveCalls()
		require.Equal(t, 1, len(saves)) // just one call to seed cache above

		_, err = sw.FindMessageByName("foo.bar.Message")
		require.NoError(t, err)
		ok, resolveTime := sw.LastResolved() // timestamp from cache
		require.True(t, ok)
		require.True(t, ts.Equal(resolveTime))
	})
	t.Run("saves to cache", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var saveHookCalled sync.Once
		ch := make(chan struct{})
		cache := &fakeCache{
			saveHook: func() {
				saveHookCalled.Do(func() {
					close(ch)
				})
			},
		}

		ts := time.Now()
		sw, err := NewSchemaWatcher(ctx, &Config{Client: newFakeFileDescriptorSetService(), Module: "foo/bar", Version: "main", Cache: cache})
		require.NoError(t, err)

		select {
		case <-time.After(time.Second):
			t.Fatal("cache.Save never called")
		case <-ch:
		}
		saves := cache.getSaveCalls()
		require.Equal(t, 1, len(saves))

		readyCtx, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		err = sw.AwaitReady(readyCtx)
		require.NoError(t, err)
		_, err = sw.FindMessageByName("foo.bar.Message")
		require.NoError(t, err)
		ok, resolveTime := sw.LastResolved()
		require.True(t, ok)
		require.False(t, resolveTime.Before(ts))
	})
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

func newFakeDescriptorPoller() DescriptorPoller {
	return NewDescriptorPoller(newFakeFileDescriptorSetService(), "", "")
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

type fakeResolver struct {
	findExtensionByNameFunc   func(field protoreflect.FullName) (protoreflect.ExtensionType, error)
	findExtensionByNumberFunc func(message protoreflect.FullName, field protoreflect.FieldNumber) (protoreflect.ExtensionType, error)
	findMessageByNameFunc     func(message protoreflect.FullName) (protoreflect.MessageType, error)
	findMessageByURLFunc      func(url string) (protoreflect.MessageType, error)
}

func (f fakeResolver) FindExtensionByName(field protoreflect.FullName) (protoreflect.ExtensionType, error) {
	return f.findExtensionByNameFunc(field)
}

func (f fakeResolver) FindExtensionByNumber(message protoreflect.FullName, field protoreflect.FieldNumber) (protoreflect.ExtensionType, error) {
	return f.findExtensionByNumberFunc(message, field)
}

func (f fakeResolver) FindMessageByName(message protoreflect.FullName) (protoreflect.MessageType, error) {
	return f.findMessageByNameFunc(message)
}

func (f fakeResolver) FindMessageByURL(url string) (protoreflect.MessageType, error) {
	return f.findMessageByURLFunc(url)
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

func (f *fakeCache) Load(key string) ([]byte, error) {
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

func (f *fakeCache) Save(key string, data []byte) error {
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
