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
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
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
	poller         SchemaPoller
	schemaID       string
	includeSymbols []string
	cacheKey       string
	resolveNow     chan struct{}

	// used to prevent concurrent calls to cache.Save, which could
	// otherwise potentially result in a known-stale value in the cache.
	cacheMu sync.Mutex
	cache   Cache

	callbackMu  sync.Mutex
	callback    func()
	errCallback func(error)

	resolverMu      sync.RWMutex
	resolver        resolver
	resolvedSchema  *descriptorpb.FileDescriptorSet
	resolveTime     time.Time
	resolvedVersion string
	// if nil, watcher has been stopped; if not nil, will be called
	// when watcher is stopped
	stop context.CancelFunc
	// If nil, resolver is ready; if not nil, will be closed
	// once resolver is ready.
	resolverReady chan struct{}
	// set to most recent resolver error until resolver is ready
	resolverErr error
}

// NewSchemaWatcher creates a new [SchemaWatcher] for the given
// [SchemaWatcherConfig].
//
// The config is first validated to ensure all required attributes are provided.
// A non-nil error is returned if the configuration is not valid.
//
// If the configuration is valid, a [SchemaWatcher] is returned, and the configured
// SchemaPoller is used to download a schema. The schema will then be periodically
// re-fetched based on the configured polling period. Either the Stop() method of the
// [SchemaWatcher] must be called or the given ctx must be cancelled to release
// resources and stop the periodic polling.
//
// This function returns immediately, even before a schema has been initially
// downloaded. If the Find* methods on the returned watcher are called before an
// initial schema has been downloaded, they will return ErrSchemaWatcherNotReady.
// Use the [SchemaWatcher.AwaitReady] method to make sure the watcher is ready
// before use.
//
// If the [SchemaWatcher.Stop]() method is called or the given ctx is cancelled,
// polling for an updated schema aborts. The SchemaWatcher may still be used after
// this, but it will be "frozen" using its most recently downloaded schema. If no
// schema was ever successfully downloaded, it will be frozen in a bad state and
// methods will return ErrSchemaWatcherNotReady.
func NewSchemaWatcher(ctx context.Context, config *SchemaWatcherConfig) (*SchemaWatcher, error) {
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
	schemaID := config.SchemaPoller.GetSchemaID()

	// compute cache key
	var cacheKey string
	if config.Cache != nil {
		cacheKey = schemaID
		if len(syms) > 0 {
			// Add a strong hash of symbols to the end.
			var sb strings.Builder
			sb.WriteString(cacheKey)
			sb.WriteByte('_')
			sha := sha256.New()
			for _, sym := range syms {
				sha.Write(([]byte)(sym))
			}
			hx := hex.NewEncoder(&sb)
			if _, err := hx.Write(sha.Sum(nil)); err != nil {
				// should never happen...
				return nil, fmt.Errorf("failed to generate hash of symbols for cache key: %w", err)
			}
			cacheKey = sb.String()
		}
	}

	ctx, cancel := context.WithCancel(ctx)
	schemaWatcher := &SchemaWatcher{
		poller:         config.SchemaPoller,
		schemaID:       schemaID,
		includeSymbols: syms,
		cacheKey:       cacheKey,
		callback:       config.OnUpdate,
		errCallback:    config.OnError,
		cache:          config.Cache,
		stop:           cancel,
		resolverReady:  make(chan struct{}),
		resolveNow:     make(chan struct{}, 1),
	}
	schemaWatcher.start(ctx, pollingPeriod, config.Jitter)
	return schemaWatcher, nil
}

func (s *SchemaWatcher) getResolver() resolver {
	s.resolverMu.RLock()
	defer s.resolverMu.RUnlock()
	return s.resolver
}

func (s *SchemaWatcher) updateResolver(ctx context.Context) (err error) {
	var changed bool
	if s.callback != nil || s.errCallback != nil {
		// make sure to invoke callback at the end to notify application
		defer func() {
			if changed && s.callback != nil {
				go func() {
					// Lock forces sequential calls to callback and also
					// means callback does not need to be thread-safe.
					s.callbackMu.Lock()
					defer s.callbackMu.Unlock()
					s.callback()
				}()
			} else if !errors.Is(err, ErrSchemaNotModified) && s.errCallback != nil {
				go func() {
					s.callbackMu.Lock()
					defer s.callbackMu.Unlock()
					s.errCallback(err)
				}()
			}
		}()
	}

	schema, schemaVersion, schemaTs, err := s.getFileDescriptorSet(ctx)
	if err != nil {
		return fmt.Errorf("failed to fetch schema: %w", err)
	}

	s.resolverMu.RLock()
	prevSchema, prevTimestamp := s.resolvedSchema, s.resolveTime
	s.resolverMu.RUnlock()

	if prevSchema != nil {
		if schemaTs.Before(prevTimestamp) {
			// Only possible if schemaTs is loaded from cache entry that is
			// older than last successful load. If that happens, just leave
			// the existing resolver in place.
			return nil
		}
		if proto.Equal(prevSchema, schema) {
			// nothing changed
			return nil
		}
	}

	resolver, err := newResolver(schema)
	if err != nil {
		return fmt.Errorf("unable to create resolver from schema: %w", err)
	}

	if len(s.includeSymbols) > 0 {
		var missingSymbols []string
		for _, sym := range s.includeSymbols {
			_, err := resolver.FindDescriptorByName(protoreflect.FullName(sym))
			if err != nil {
				missingSymbols = append(missingSymbols, sym)
			}
		}
		if len(missingSymbols) > 0 {
			sort.Strings(missingSymbols)
			for i, sym := range missingSymbols {
				missingSymbols[i] = strconv.Quote(sym)
			}
			return fmt.Errorf("schema poller returned incomplete schema: missing %v", strings.Join(missingSymbols, ", "))
		}
	}

	s.resolverMu.Lock()
	defer s.resolverMu.Unlock()
	s.resolver = resolver
	s.resolveTime = schemaTs
	s.resolvedSchema = schema
	s.resolvedVersion = schemaVersion
	s.resolverErr = nil
	changed = true
	return nil
}

func (s *SchemaWatcher) initialUpdateResolver(ctx context.Context, pollingPeriod time.Duration, jitter float64) (success bool) {
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
			timer := time.NewTimer(addJitter(delay, jitter))
			select {
			case <-ctx.Done():
				timer.Stop()
				return false
			case <-timer.C:
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
	s.resolverMu.RLock()
	ready, stop := s.resolverReady, s.stop
	s.resolverMu.RUnlock()
	if ready == nil {
		if stop == nil {
			return ErrSchemaWatcherStopped
		}
		return nil
	}
	select {
	case <-ready:
		s.resolverMu.RLock()
		stop = s.stop
		s.resolverMu.RUnlock()
		if stop == nil {
			return ErrSchemaWatcherStopped
		}
		return nil
	case <-ctx.Done():
		s.resolverMu.RLock()
		err := s.resolverErr
		s.resolverMu.RUnlock()
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
	s.resolverMu.RLock()
	defer s.resolverMu.RUnlock()
	if s.resolver == nil {
		return false, time.Time{}
	}
	return true, s.resolveTime
}

// ResolveNow tells the watcher to poll for a new schema immediately instead of
// waiting until the next scheduled time per the configured polling period.
func (s *SchemaWatcher) ResolveNow() {
	select {
	case s.resolveNow <- struct{}{}:
	default:
		// channel buffer is full, which means "resolve now" signal already pending
	}
}

// RangeFiles iterates over all registered files while f returns true. The
// iteration order is undefined.
//
// This uses a snapshot of the most recently downloaded schema. So if the
// schema is updated (via concurrent download) while iterating, f will only
// see the contents of the older schema.
//
// If the s is not yet ready, this will not call f at all and instead immediately
// return. This does not return an error so that the signature matches the method
// of the same name of *protoregistry.Files, allowing *SchemaWatcher to provide
// the same interface.
func (s *SchemaWatcher) RangeFiles(f func(protoreflect.FileDescriptor) bool) {
	res := s.getResolver()
	if res == nil {
		return
	}
	res.RangeFiles(f)
}

// RangeFilesByPackage iterates over all registered files in a given proto package
// while f returns true. The iteration order is undefined.
//
// This uses a snapshot of the most recently downloaded schema. So if the
// schema is updated (via concurrent download) while iterating, f will only
// see the contents of the older schema.
//
// If the s is not yet ready, this will not call f at all and instead immediately
// return. This does not return an error so that the signature matches the method
// of the same name of *protoregistry.Files, allowing *SchemaWatcher to provide
// the same interface.
func (s *SchemaWatcher) RangeFilesByPackage(name protoreflect.FullName, f func(protoreflect.FileDescriptor) bool) {
	res := s.getResolver()
	if res == nil {
		return
	}
	res.RangeFilesByPackage(name, f)
}

// FindFileByPath looks up a file by the path.
//
// This uses the most recently downloaded schema.
func (s *SchemaWatcher) FindFileByPath(path string) (protoreflect.FileDescriptor, error) {
	res := s.getResolver()
	if res == nil {
		return nil, ErrSchemaWatcherNotReady
	}
	return res.FindFileByPath(path)
}

// FindDescriptorByName looks up a descriptor by the full name.
//
// This uses the most recently downloaded schema.
func (s *SchemaWatcher) FindDescriptorByName(name protoreflect.FullName) (protoreflect.Descriptor, error) {
	res := s.getResolver()
	if res == nil {
		return nil, ErrSchemaWatcherNotReady
	}
	return res.FindDescriptorByName(name)
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

// FindEnumByName looks up an enum by its full name.
// E.g., "google.protobuf.Field.Kind".
//
// Implements [Resolver] using the most recently downloaded schema.
func (s *SchemaWatcher) FindEnumByName(enum protoreflect.FullName) (protoreflect.EnumType, error) {
	res := s.getResolver()
	if res == nil {
		return nil, ErrSchemaWatcherNotReady
	}
	return res.FindEnumByName(enum)
}

func (s *SchemaWatcher) start(ctx context.Context, pollingPeriod time.Duration, jitter float64) {
	go func() {
		if !s.initialUpdateResolver(ctx, pollingPeriod, jitter) {
			return
		}
		defer s.Stop()
		for {
			// consume any "resolve now" signal that arrived while we were concurrently resolving
			select {
			case <-s.resolveNow:
			default:
			}

			timer := time.NewTimer(addJitter(pollingPeriod, jitter))
			select {
			case <-timer.C:
				if ctx.Err() != nil {
					// don't bother fetching a schema if context is done
					return
				}
				_ = s.updateResolver(ctx)
			case <-s.resolveNow:
				timer.Stop()
				if ctx.Err() != nil {
					// don't bother fetching a schema if context is done
					return
				}
				_ = s.updateResolver(ctx)
			case <-ctx.Done():
				timer.Stop()
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
	s.resolverMu.RLock()
	defer s.resolverMu.RUnlock()
	return s.stop == nil
}

func (s *SchemaWatcher) getFileDescriptorSet(ctx context.Context) (*descriptorpb.FileDescriptorSet, string, time.Time, error) {
	s.resolverMu.RLock()
	currentVersion := s.resolvedVersion
	s.resolverMu.RUnlock()
	descriptors, version, err := s.poller.GetSchema(ctx, s.includeSymbols, currentVersion)
	respTime := time.Now()
	if err != nil {
		if errors.Is(err, ErrSchemaNotModified) || s.cache == nil {
			return nil, "", time.Time{}, err
		}
		// try to fallback to cache
		data, cacheErr := s.cache.Load(ctx, s.cacheKey)
		if cacheErr != nil {
			return nil, "", time.Time{}, fmt.Errorf("%w (failed to load from cache: %v)", err, cacheErr)
		}
		msg, cacheErr := decodeForCache(data)
		if cacheErr != nil {
			return nil, "", time.Time{}, fmt.Errorf("%w (failed to decode cached value: %v)", err, cacheErr)
		}
		if !isCorrectCacheEntry(msg, s.schemaID, s.includeSymbols) {
			// Cache key collision! Do not use this result!
			return nil, "", time.Time{}, err
		}
		return msg.GetSchema().GetDescriptors(), msg.GetSchema().GetVersion(), msg.GetSchemaTimestamp().AsTime(), nil
	}
	if s.cache != nil {
		go func() {
			data, err := encodeForCache(s.schemaID, s.includeSymbols, descriptors, version, respTime)
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
			_ = s.cache.Save(ctx, s.cacheKey, data)
		}()
	}
	return descriptors, version, respTime, nil
}
