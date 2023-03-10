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
	"sort"
	"strings"
	"sync"
	"time"

	"buf.build/gen/go/bufbuild/reflect/bufbuild/connect-go/buf/reflect/v1beta1/reflectv1beta1connect"
	reflectv1beta1 "buf.build/gen/go/bufbuild/reflect/protocolbuffers/go/buf/reflect/v1beta1"
	"github.com/bufbuild/connect-go"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

var (
	// ErrSchemaWatcherStopped is an error returned from the AwaitReady method
	// that indicates the schema watcher was stopped before it ever became ready.
	ErrSchemaWatcherStopped = errors.New("SchemaWatcher was stopped")
	// ErrSchemaWatcherNotReady is an error returned from the various Find*
	// methods of SchemaWatcher an initial schema has not yet been downloaded (or
	// loaded from cache)
	ErrSchemaWatcherNotReady = errors.New("SchemaWatcher not ready")
)

// SchemaWatcher watches a schema in a remote registry by periodically polling.
// It implements the [Resolver] interface using the most recently downloaded
// schema. As schema changes are pushed to the remote registry, the watcher
// will incorporate the changes by downloading each change via regular polling.
type SchemaWatcher struct {
	client         reflectv1beta1connect.FileDescriptorSetServiceClient
	module         string
	version        string
	includeSymbols []string
	cacheKey       string

	// used to prevent concurrent calls to cache.Save, which could
	// otherwise potentially result in a known-stale value in the cache.
	cacheMu sync.Mutex
	cache   Cache

	resolverMu  sync.RWMutex
	resolver    Resolver
	resolveTime time.Time
	// if nil, watcher has been stopped; if not nil, will be called
	// when watcher is stopped
	stop context.CancelFunc
	// If nil, resolver is ready; if not nil, will be closed
	// once resolver is ready.
	resolverReady chan struct{}
	// set to most recent resolver error until resolver is ready
	resolverErr error
}

// NewSchemaWatcher creates a new [SchemaWatcher] for the given [Config].
//
// [Config] is first validated to ensure all required attributes are provided. A
// non-nil error is returned if the configuration is not valid.
//
// If the configuration is valid, a [SchemaWatcher] is returned, and the configured
// FileDescriptorSetService client is used to download a schema. The schema will
// then be periodically re-downloaded based on the configured polling period.
// Either the Stop() method of the [SchemaWatcher] must be called or the given ctx
// must be cancelled to release resources and stop the periodic re-download.
//
// This function returns immediately, even before a schema has been initially
// downloaded. If the Find* methods on the returned watcher are called before an
// initial schema has been downloaded, they will return ErrSchemaWatcherNotReady.
// Use the AwaitReady method to make sure the watcher is ready before use.
//
// If the Stop() method is called or the given ctx is cancelled, polling for an
// updated schema aborts. The SchemaWatcher may still be used after this, but it
// will be "frozen" using its last downloaded schema.
func NewSchemaWatcher(ctx context.Context, config *Config) (*SchemaWatcher, error) {
	if err := config.validate(); err != nil {
		return nil, err
	}
	pollingPeriod := config.PollingPeriod
	if pollingPeriod == 0 {
		pollingPeriod = defaultPollingPeriod
	}

	// canonicalize symbols: remove duplicates and sort
	symSet := map[string]struct{}{}
	for _, sym := range config.IncludeSymbols {
		symSet[sym] = struct{}{}
	}
	syms := make([]string, 0, len(symSet))
	for sym := range symSet {
		syms = append(syms, sym)
	}
	sort.Strings(syms)

	// compute cache key
	var cacheKey string
	if config.Cache != nil {
		cacheKey = config.Module
		if config.Version != "" {
			cacheKey += "@" + config.Version
		}
		if len(syms) > 0 {
			cacheKey += ";" + strings.Join(syms, ",")
		}
	}

	ctx, cancel := context.WithCancel(ctx)
	schemaWatcher := &SchemaWatcher{
		client:         config.Client,
		module:         config.Module,
		version:        config.Version,
		includeSymbols: syms,
		cacheKey:       cacheKey,
		cache:          config.Cache,
		stop:           cancel,
		resolverReady:  make(chan struct{}),
	}
	schemaWatcher.start(ctx, pollingPeriod)
	return schemaWatcher, nil
}

func (s *SchemaWatcher) getResolver() Resolver {
	s.resolverMu.RLock()
	defer s.resolverMu.RUnlock()
	return s.resolver
}

func (s *SchemaWatcher) updateResolver(ctx context.Context) error {
	schema, schemaTs, err := s.getFileDescriptorSet(ctx)
	if err != nil {
		return fmt.Errorf("failed to get schema from remote: %w", err)
	}
	resolver, err := newResolver(schema)
	if err != nil {
		return fmt.Errorf("unable to create resolver from schema: %w", err)
	}
	s.resolverMu.Lock()
	defer s.resolverMu.Unlock()
	if schemaTs.Before(s.resolveTime) {
		// Only possible if schemaTs is loaded from cache entry that is
		// older than last successful load. If that happens, just leave
		// the existing resolver in place.
		return nil
	}
	s.resolver = resolver
	s.resolveTime = schemaTs
	s.resolverErr = nil
	return nil
}

func (s *SchemaWatcher) initialUpdateResolver(ctx context.Context, pollingPeriod time.Duration) (success bool) {
	defer func() {
		s.resolverMu.Lock()
		defer s.resolverMu.Unlock()
		close(s.resolverReady)
		s.resolverReady = nil
		if !success {
			s.stop = nil
		}
	}()

	var delay time.Duration
	for {
		err := s.updateResolver(ctx)
		if err == nil {
			// success!
			return true
		}
		s.resolverMu.Lock()
		s.resolverErr = err
		s.resolverMu.Unlock()
		if delay == 0 {
			// immediately retry, but delay 1s if it fails again
			delay = time.Second
		} else {
			select {
			case <-ctx.Done():
				return false
			case <-time.After(delay):
			}
			delay = delay * 2 // exponential backoff
		}

		// we never wait longer than configured polling period, so we only apply
		// exponential backoff up to this point
		if delay > pollingPeriod {
			delay = pollingPeriod
		}
	}
}

// AwaitReady returns a non-nil error when s has downloaded a schema and is
// ready for use. If the given context is cancelled (or has a deadline that
// elapses) before s is ready, a non-nil error is returned. If an error
// occurred while trying to download a schema, that error will be returned
// at that time. If no error has yet occurred (e.g. the context was cancelled
// before a download attempt finished), this will return the context error.
//
// Even if an error is returned, the SchemaWatcher will still be trying to
// download the schema. It will keep trying/polling until s.Stop is called or
// until the context passed to [NewSchemaWatcher] is cancelled.
func (s *SchemaWatcher) AwaitReady(ctx context.Context) error {
	s.resolverMu.Lock()
	ready, stop := s.resolverReady, s.stop
	s.resolverMu.Unlock()
	if ready == nil {
		if stop == nil {
			return ErrSchemaWatcherStopped
		}
		return nil
	}
	select {
	case <-ready:
		s.resolverMu.Lock()
		stop = s.stop
		s.resolverMu.Unlock()
		if stop == nil {
			return ErrSchemaWatcherStopped
		}
		return nil
	case <-ctx.Done():
		s.resolverMu.Lock()
		err := s.resolverErr
		s.resolverMu.Unlock()
		if err != nil {
			return err
		}
		return ctx.Err()
	}
}

// LastResolved returns the time that a schema was last successfully downloaded.
// If the boolean value is false, the watcher is not yet ready and no schema has
// yet been successfully downloaded. Otherwise, the returned time indicates when
// the schema was downloaded. If the schema is loaded from a cache, the timestamp
// will indicate when that cached schema was originally downloaded.
//
// This can be used for staleness heuristics if a partition occurs that makes
// the remote registry unavailable. Under typical operations when no failures
// are occurring, the maximum age will up to the configured polling period plus
// the latency of the RPC to the remote registry.
func (s *SchemaWatcher) LastResolved() (bool, time.Time) {
	s.resolverMu.Lock()
	defer s.resolverMu.Unlock()
	if s.resolver == nil {
		return false, time.Time{}
	}
	return true, s.resolveTime
}

// FindExtensionByName looks up an extension field by the field's full name.
// Note that this is the full name of the field as determined by
// where the extension is declared and is unrelated to the full name of the
// message being extended.
//
// Implements [Resolver] using the most recently downloaded schema.
func (s *SchemaWatcher) FindExtensionByName(field protoreflect.FullName) (protoreflect.ExtensionType, error) {
	res := s.getResolver()
	if res == nil {
		return nil, ErrSchemaWatcherNotReady
	}
	return res.FindExtensionByName(field)
}

// FindExtensionByNumber looks up an extension field by the field number
// within some parent message, identified by full name.
//
// Implements [Resolver] using the most recently downloaded schema.
func (s *SchemaWatcher) FindExtensionByNumber(message protoreflect.FullName, field protoreflect.FieldNumber) (protoreflect.ExtensionType, error) {
	res := s.getResolver()
	if res == nil {
		return nil, ErrSchemaWatcherNotReady
	}
	return res.FindExtensionByNumber(message, field)
}

// FindMessageByName looks up a message by its full name.
// E.g., "google.protobuf.Any"
//
// Implements [Resolver] using the most recently downloaded schema.
func (s *SchemaWatcher) FindMessageByName(message protoreflect.FullName) (protoreflect.MessageType, error) {
	res := s.getResolver()
	if res == nil {
		return nil, ErrSchemaWatcherNotReady
	}
	return res.FindMessageByName(message)
}

// FindMessageByURL looks up a message by a URL identifier.
// See documentation on google.protobuf.Any.type_url for the URL format.
//
// Implements [Resolver] using the most recently downloaded schema.
func (s *SchemaWatcher) FindMessageByURL(url string) (protoreflect.MessageType, error) {
	res := s.getResolver()
	if res == nil {
		return nil, ErrSchemaWatcherNotReady
	}
	return res.FindMessageByURL(url)
}

func (s *SchemaWatcher) start(ctx context.Context, pollingPeriod time.Duration) {
	go func() {
		if !s.initialUpdateResolver(ctx, pollingPeriod) {
			return
		}
		defer s.Stop()
		ticker := time.NewTicker(pollingPeriod)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if ctx.Err() != nil {
					// don't bother fetching a schema if context is done
					return
				}
				_ = s.updateResolver(ctx)
			case <-ctx.Done():
				return
			}
		}
	}()
}

// Stop the [SchemaWatcher] from polling the BSR for new schemas. Can be called
// multiple times safely.
func (s *SchemaWatcher) Stop() {
	s.resolverMu.Lock()
	defer s.resolverMu.Unlock()
	if s.stop != nil {
		s.stop()
		s.stop = nil
	}
}

func (s *SchemaWatcher) IsStopped() bool {
	s.resolverMu.Lock()
	defer s.resolverMu.Unlock()
	return s.stop == nil
}

func (s *SchemaWatcher) getFileDescriptorSet(ctx context.Context) (*descriptorpb.FileDescriptorSet, time.Time, error) {
	req := connect.NewRequest(&reflectv1beta1.GetFileDescriptorSetRequest{
		Module:  s.module,
		Version: s.version,
		Symbols: s.includeSymbols,
	})
	resp, err := s.client.GetFileDescriptorSet(ctx, req)
	respTime := time.Now()
	if err != nil {
		if s.cache != nil {
			data, cacheErr := s.cache.Load(s.cacheKey)
			if cacheErr != nil {
				return nil, time.Time{}, fmt.Errorf("%w (failed to load from cache: %v)", err, cacheErr)
			}
			msg, ts, cacheErr := decodeForCache(data)
			if cacheErr != nil {
				return nil, time.Time{}, fmt.Errorf("%w (failed to decode cached value: %v)", err, cacheErr)
			}
			return msg.FileDescriptorSet, ts, nil
		}
		return nil, time.Time{}, err
	} else if s.cache != nil {
		go func() {
			data, err := encodeForCache(resp.Msg, respTime)
			if err != nil {
				// Since we got the data by unmarshalling it (either from RPC
				// response or cache), it must be marshallable. So this should
				// never actually happen.
				return
			}
			// though s.cache must be thread-safe, we use a mutex to
			// prevent racing, concurrent calls to Save, which could
			// potentially leave the cache in a bad/stale state if an
			// earlier call to Save actually succeeds last.
			s.cacheMu.Lock()
			defer s.cacheMu.Unlock()
			// We ignore the error since there's nothing we can do.
			// But keeping it in the interface signature means that
			// user code can wrap a cache implementation and observe
			// the error, in order to possibly take action (like write
			// a log message or update a counter metric, etc).
			_ = s.cache.Save(s.cacheKey, data)
		}()
	}
	return resp.Msg.FileDescriptorSet, respTime, nil
}

// newResolver creates a new Resolver.
//
// If the input slice is empty, this returns nil
// The given FileDescriptors must be self-contained, that is they must contain all imports.
// This can NOT be guaranteed for FileDescriptorSets given over the wire, and can only be guaranteed from builds.
func newResolver(fileDescriptors *descriptorpb.FileDescriptorSet) (Resolver, error) {
	// TODO(TCN-925): probably needs to reparse unrecognized fields after it creates a resolver.
	if len(fileDescriptors.File) == 0 {
		return (*protoregistry.Types)(nil), nil
	}
	files, err := protodesc.FileOptions{
		AllowUnresolvable: true,
	}.NewFiles(
		fileDescriptors,
	)
	if err != nil {
		return nil, err
	}
	types := &protoregistry.Types{}
	var rangeErr error
	files.RangeFiles(func(fileDescriptor protoreflect.FileDescriptor) bool {
		if err := registerTypes(types, fileDescriptor); err != nil {
			rangeErr = err
			return false
		}
		return true
	})
	if rangeErr != nil {
		return nil, rangeErr
	}
	return types, nil
}

type typeContainer interface {
	Enums() protoreflect.EnumDescriptors
	Messages() protoreflect.MessageDescriptors
	Extensions() protoreflect.ExtensionDescriptors
}

func registerTypes(types *protoregistry.Types, container typeContainer) error {
	for i := 0; i < container.Enums().Len(); i++ {
		if err := types.RegisterEnum(dynamicpb.NewEnumType(container.Enums().Get(i))); err != nil {
			return err
		}
	}
	for i := 0; i < container.Messages().Len(); i++ {
		msg := container.Messages().Get(i)
		if err := types.RegisterMessage(dynamicpb.NewMessageType(msg)); err != nil {
			return err
		}
		if err := registerTypes(types, msg); err != nil {
			return err
		}
	}
	for i := 0; i < container.Extensions().Len(); i++ {
		if err := types.RegisterExtension(dynamicpb.NewExtensionType(container.Extensions().Get(i))); err != nil {
			return err
		}
	}
	return nil
}
