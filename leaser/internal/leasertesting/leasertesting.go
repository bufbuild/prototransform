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

package leasertesting

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bufbuild/prototransform"
	"github.com/stretchr/testify/require"
)

//nolint:revive // okay that ctx is second; prefer t to be first
func RunSimpleLeaserTests(t *testing.T, ctx context.Context, leaser prototransform.Leaser, forceOwner func(key string, val []byte) error) map[string][]byte {
	t.Helper()

	var mutex sync.Mutex
	acquired, released := map[string]int{}, map[string]int{}
	track := func(key string, channel chan struct{}, acq bool) func() {
		return func() {
			mutex.Lock()
			defer mutex.Unlock()
			var trackingMap map[string]int
			msg := key + " "
			if acq {
				msg += "acquired"
				trackingMap = acquired
			} else {
				msg += "released"
				trackingMap = released
			}
			trackingMap[key]++
			select {
			case channel <- struct{}{}:
			default:
				msg += "(could not notify; channel full)"
			}
			t.Log(msg)
		}
	}

	aAcquiredByX := make(chan struct{}, 1)
	aReleasedByX := make(chan struct{}, 1)
	aAcquiredByY := make(chan struct{}, 1)
	aReleasedByY := make(chan struct{}, 1)
	bAcquiredByX := make(chan struct{}, 1)
	bReleasedByX := make(chan struct{}, 1)
	cAcquiredByX := make(chan struct{}, 1)
	cReleasedByX := make(chan struct{}, 1)
	dAcquiredByZ := make(chan struct{}, 1)
	dReleasedByZ := make(chan struct{}, 1)

	checkLeaseInitial := func(lease prototransform.Lease, success bool) {
		// initial check should complete in 200 millis
		for i := range 4 {
			held, err := lease.IsHeld()
			if err == nil {
				require.Equal(t, held, success)
				return
			}
			require.ErrorIs(t, err, prototransform.ErrLeaseStateNotYetKnown)
			if i < 3 {
				time.Sleep(50 * time.Millisecond)
			}
		}
		require.Fail(t, "lease never checked successfully")
	}
	awaitSignal := func(key string, channel chan struct{}, expectAcquired, expectReleased int) {
		select {
		case <-time.After(500 * time.Millisecond):
			require.Fail(t, "callback never invoked")
		case <-channel:
		}
		mutex.Lock()
		defer mutex.Unlock()
		require.Equal(t, expectAcquired, acquired[key])
		require.Equal(t, expectReleased, released[key])
	}
	noSignal := func(key string, channel chan struct{}, expectAcquired, expectReleased int) {
		select {
		case <-time.After(500 * time.Millisecond):
		case <-channel:
			require.Fail(t, "callback invoked but should not have been")
		}
		mutex.Lock()
		defer mutex.Unlock()
		require.Equal(t, expectAcquired, acquired[key])
		require.Equal(t, expectReleased, released[key])
	}
	noSignalsPending := func(key string, acqCh, relCh chan struct{}, expectAcquired, expectReleased int) {
		select {
		case <-acqCh:
			require.Fail(t, "acquired callback invoked but should not have been")
		case <-relCh:
			require.Fail(t, "released callback invoked but should not have been")
		default:
		}
		mutex.Lock()
		defer mutex.Unlock()
		require.Equal(t, expectAcquired, acquired[key])
		require.Equal(t, expectReleased, released[key])
	}
	aLeaseForX := leaser.NewLease(ctx, "a", []byte{'x'})
	checkLeaseInitial(aLeaseForX, true)
	aLeaseForX.SetCallbacks(track("a:x", aAcquiredByX, true), track("a:x", aReleasedByX, false))
	awaitSignal("a:x", aAcquiredByX, 1, 0)

	aLeaseForY := leaser.NewLease(ctx, "a", []byte{'y'})
	checkLeaseInitial(aLeaseForY, false)
	aLeaseForY.SetCallbacks(track("a:y", aAcquiredByY, true), track("a:y", aReleasedByY, false))
	noSignal("a:y", aReleasedByY, 0, 0)

	bLeaseForX := leaser.NewLease(ctx, "b", []byte{'x'})
	checkLeaseInitial(bLeaseForX, true)
	bLeaseForX.SetCallbacks(track("b", bAcquiredByX, true), track("b", bReleasedByX, false))
	awaitSignal("b", bAcquiredByX, 1, 0)

	cLeaseForX := leaser.NewLease(ctx, "c", []byte{'x'})
	checkLeaseInitial(cLeaseForX, true)
	cLeaseForX.SetCallbacks(track("c", cAcquiredByX, true), track("c", cReleasedByX, false))
	awaitSignal("c", cAcquiredByX, 1, 0)

	dCtx, dCancel := context.WithCancel(ctx)
	dLeaseForZ := leaser.NewLease(dCtx, "d", []byte{'z'})
	checkLeaseInitial(dLeaseForZ, true)
	dLeaseForZ.SetCallbacks(track("d", dAcquiredByZ, true), track("d", dReleasedByZ, false))
	awaitSignal("d", dAcquiredByZ, 1, 0)

	aLeaseForX.Cancel()
	awaitSignal("a:x", aReleasedByX, 1, 1)
	awaitSignal("a:y", aAcquiredByY, 1, 0)

	dCancel()
	awaitSignal("d", dReleasedByZ, 1, 1)

	finalOwners := map[string][]byte{
		"a": {'y'},
		"b": {'x'},
		"c": {'x'},
	}

	cExpectReleased := 0
	if forceOwner != nil {
		err := forceOwner("c", []byte{'x', 'y', 'z'})
		require.NoError(t, err)
		awaitSignal("c", cReleasedByX, 1, 1)

		finalOwners["c"] = []byte{'x', 'y', 'z'}
		cExpectReleased = 1
	}

	// final check of stats and signals
	time.Sleep(200 * time.Millisecond)
	noSignalsPending("a:x", aAcquiredByX, aReleasedByX, 1, 1)
	noSignalsPending("a:y", aAcquiredByY, aReleasedByY, 1, 0)
	noSignalsPending("b", bAcquiredByX, bReleasedByX, 1, 0)
	noSignalsPending("c", cAcquiredByX, cReleasedByX, 1, cExpectReleased)
	noSignalsPending("d", dAcquiredByZ, dReleasedByZ, 1, 1)

	return finalOwners
}
