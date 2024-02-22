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

package prototransform

import (
	"fmt"
	"time"

	"google.golang.org/protobuf/reflect/protoreflect"
)

const defaultPollingPeriod = 5 * time.Minute

// SchemaWatcherConfig contains the configurable attributes of the [SchemaWatcher].
type SchemaWatcherConfig struct {
	// The downloader of descriptors. See [NewSchemaPoller].
	SchemaPoller SchemaPoller
	// The symbols that should be included in the downloaded schema. These must be
	// the fully-qualified names of elements in the schema, which can include
	// packages, messages, enums, extensions, services, and methods. If specified,
	// the downloaded schema will only include descriptors to describe these symbols.
	// If left empty, the entire schema will be downloaded.
	IncludeSymbols []string
	// The period of the polling the BSR for new versions is specified by the
	// PollingPeriod argument. The PollingPeriod will adjust the time interval.
	// The duration must be greater than zero; if not, [NewSchemaWatcher] will
	// return an error. If unset and left zero, a default period of 5 minutes
	// is used.
	PollingPeriod time.Duration
	// A number between 0 and 1 that represents the amount of jitter to add to
	// the polling period. A value of zero means no jitter. A value of one means
	// up to 100% jitter, so the actual period would be between 0 and 2*PollingPeriod.
	// To prevent self-synchronization (and thus thundering herds) when there are
	// multiple pollers, a value of 0.1 to 0.3 is typical.
	Jitter float64
	// If Cache is non-nil, it is used for increased robustness, even in the
	// face of the remote schema registry being unavailable. If non-nil and the
	// API call to initially retrieve a schema fails, the schema will instead
	// be loaded from this cache. Whenever a new schema is downloaded from the
	// remote registry, it will be saved to the cache. So if the process is
	// restarted and the remote registry is unavailable, the latest cached schema
	// can still be used.
	Cache Cache
	// If Leaser is non-nil, it is used to decide whether the current process
	// can poll for the schema. Cache must be non-nil. This is useful when the
	// schema source is a remote process, and the current process is replicated
	// (e.g. many instances running the same workload, for redundancy and/or
	// capacity). This prevents all the replicas from polling. Instead, a single
	// replica will "own" the lease and poll for the schema. It will then store
	// the downloaded schema in the shared cache. A replica that does not have
	// the lease will look only in the cache instead of polling the remote
	// source.
	Leaser Leaser
	// CurrentProcess is an optional identifier for the current process. This
	// is only used if Leaser is non-nil. If present, this value is used to
	// identify the current process as the leaseholder. If not present, a
	// default value will be computed using the current process's PID and the
	// host name and network addresses of the current host. If present, this
	// value must be unique for all other processes that might try to acquire
	// the same lease.
	CurrentProcess []byte
	// OnUpdate is an optional callback that will be invoked when a new schema
	// is fetched. This can be used by an application to take action when a new
	// schema becomes available.
	OnUpdate func(*SchemaWatcher)
	// OnError is an optional callback that will be invoked when a schema cannot
	// be fetched. This could be due to the SchemaPoller returning an error or
	// failure to convert the fetched descriptors into a resolver.
	OnError func(*SchemaWatcher, error)
}

func (c *SchemaWatcherConfig) validate() error {
	if c.SchemaPoller == nil {
		return fmt.Errorf("schema poller not provided")
	}
	if c.PollingPeriod < 0 {
		return fmt.Errorf("polling period duration cannot be negative")
	}
	if c.Jitter < 0 {
		return fmt.Errorf("jitter cannot be negative")
	}
	if c.Jitter > 1.0 {
		return fmt.Errorf("jitter cannot be greater than 1.0 (100%%)")
	}
	if c.Leaser != nil && c.Cache == nil {
		return fmt.Errorf("leaser config should only be present when cache config also present")
	}
	if c.Leaser != nil && c.CurrentProcess != nil && len(c.CurrentProcess) == 0 {
		return fmt.Errorf("current process is empty but not nil; leave nil or set to non-empty value")
	}
	for _, sym := range c.IncludeSymbols {
		if sym == "" {
			// Counter-intuitively, empty string is valid in this context as it
			// indicates the default/unnamed package. Requesting it will include
			// all files in the module that are defined without a package.
			continue
		}
		if !protoreflect.FullName(sym).IsValid() {
			return fmt.Errorf("%q is not a valid symbol name", sym)
		}
	}
	return nil
}
