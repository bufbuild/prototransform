// Copyright 2023-2025 Buf Technologies, Inc.
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

package leaser

import (
	"bytes"
	"context"
	"sync"
	"time"

	"github.com/bufbuild/prototransform"
)

// PollingLeaser implements prototransform.Leaser by polling a lease
// store, periodically trying to create a lease or query for the
// current owner. Leaser implementations need only provide a lease
// store implementation. This is suitable for many types of stores,
// including Redis, memcached, or an RDBMS. It is not necessarily
// suitable for stores that have better primitives for distributed
// locking or leader election, such as ZooKeeper.
type PollingLeaser struct {
	LeaseStore    LeaseStore
	LeaseTTL      time.Duration
	PollingPeriod time.Duration
}

// LeaseStore is the interface used to try to acquire and release leases.
type LeaseStore interface {
	// TryAcquire tries to create a lease with the given leaseName and leaseHolder. If it is
	// successful (implying the given leaseHolder is the active holder), then it should return
	// true for the first value. If creation fails, the store should query for the leaseholder
	// value of the existing lease. The second returned value is the actual current leaseholder,
	// which can only differ from the given value if creation failed. If creation failed, but
	// the current process is the holder (i.e. the given leaseHolder matches the value in the
	// store), the store should bump the lease's TTL, so that the current process will continue
	// to hold it.
	//
	// This method is invoked regardless of whether the leaser believes that the current process
	// is the leaseholder. This is used to both try to acquire a lease, bump the TTL on a lease
	// if already held, and check the owner of the lease if not held. The store must be able to
	// do all of this atomically so that it is safe and correct in the face of concurrent
	// processes all trying to manage the same leaseName.
	TryAcquire(ctx context.Context, leaseName string, leaseHolder []byte, ttl time.Duration) (created bool, holder []byte, err error)
	// Release tries to delete a lease with the given leaseName and leaseHolder. If the lease
	// exists but is held by a different owner, it should not be deleted.
	//
	// This method will only be called when the leaser believes that the current process holds
	// the lease, for cleanup.
	Release(ctx context.Context, leaseName string, leaseHolder []byte) error
}

// NewLease implements the prototransform.Leaser interface.
func (l *PollingLeaser) NewLease(ctx context.Context, leaseName string, leaseHolder []byte) prototransform.Lease {
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	newLease := &lease{
		cancel: cancel,
		done:   done,
		err:    prototransform.ErrLeaseStateNotYetKnown,
	}
	go newLease.run(ctx, l, leaseName, leaseHolder, done)
	return newLease
}

type lease struct {
	cancel context.CancelFunc
	done   <-chan struct{}

	mu     sync.Mutex
	isHeld bool
	err    error

	notifyMu             sync.Mutex
	onAcquire, onRelease func()
}

func (l *lease) IsHeld() (bool, error) {
	l.mu.Lock()
	isHeld, err := l.isHeld, l.err
	l.mu.Unlock()
	return isHeld, err
}

func (l *lease) SetCallbacks(onAcquire, onRelease func()) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.onAcquire, l.onRelease = onAcquire, onRelease
	if l.isHeld && l.onAcquire != nil {
		go func() {
			l.notifyMu.Lock()
			defer l.notifyMu.Unlock()
			l.onAcquire()
		}()
	}
}

func (l *lease) Cancel() {
	l.cancel()
	<-l.done
}

func (l *lease) run(ctx context.Context, leaser *PollingLeaser, key string, value []byte, done chan<- struct{}) {
	defer close(done)
	ticker := time.NewTicker(leaser.PollingPeriod)
	defer ticker.Stop()
	l.poll(ctx, leaser, key, value)
	for {
		select {
		case <-ctx.Done():
			l.releaseNow(ctx, leaser, key, value)
			return
		case <-ticker.C:
			if ctx.Err() != nil {
				// skip polling if context is done
				l.releaseNow(ctx, leaser, key, value)
				return
			}
			l.poll(ctx, leaser, key, value)
		}
	}
}

func (l *lease) poll(ctx context.Context, leaser *PollingLeaser, key string, value []byte) {
	created, holder, err := leaser.LeaseStore.TryAcquire(ctx, key, value, leaser.LeaseTTL)
	if err != nil {
		l.released(err)
		return
	}
	if created {
		l.acquired()
		return
	}
	if bytes.Equal(holder, value) {
		// The existing lease is ours
		l.acquired()
		return
	}
	// The existing lease is not ours
	l.released(nil)
}

func (l *lease) releaseNow(ctx context.Context, leaser *PollingLeaser, key string, value []byte) {
	l.mu.Lock()
	isHeld := l.isHeld
	l.mu.Unlock()
	if isHeld {
		// best effort: immediately release if we hold it
		_ = leaser.LeaseStore.Release(ctx, key, value)
	}
	l.released(nil)
}

func (l *lease) acquired() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.isHeld && l.onAcquire != nil {
		go func() {
			l.notifyMu.Lock()
			defer l.notifyMu.Unlock()
			l.onAcquire()
		}()
	}
	l.isHeld = true
	l.err = nil
}

func (l *lease) released(err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.isHeld && l.onRelease != nil {
		go func() {
			l.notifyMu.Lock()
			defer l.notifyMu.Unlock()
			l.onRelease()
		}()
	}
	l.isHeld = false
	l.err = err
}
