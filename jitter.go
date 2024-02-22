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
	"hash/maphash"
	"math/rand"
	"sync"
	"time"
)

// We create our own locked RNG so we won't have lock contention with the global
// RNG that package "math/rand" creates. Also, that way we are not at the mercy
// of other packages that might be seeding "math/rand"'s global RNG in a bad way.
var rnd = newLockedRand() //nolint:gochecknoglobals

func addJitter(period time.Duration, jitter float64) time.Duration {
	factor := (rnd.Float64()*2 - 1) * jitter // produces a number between -jitter and jitter
	period = time.Duration(float64(period) * (factor + 1))
	if period == 0 {
		period = 1 // ticker.Reset panics if duration is zero
	}
	return period
}

type lockedSource struct {
	mu  sync.Mutex
	src rand.Source
}

func (s *lockedSource) Int63() int64 {
	s.mu.Lock()
	i := s.src.Int63()
	s.mu.Unlock()
	return i
}

func (s *lockedSource) Seed(seed int64) {
	s.mu.Lock()
	s.src.Seed(seed)
	s.mu.Unlock()
}

func newLockedRand() *rand.Rand {
	//nolint:gosec // don't need secure RNG for this and prefer something that can't exhaust entropy
	return rand.New(&lockedSource{src: rand.NewSource(int64(seed()))})
}

func seed() uint64 {
	// lock-free and fast; under-the-hood calls runtime.fastrand
	// https://www.reddit.com/r/golang/comments/ntyi7i/comment/h0w0tu7/
	return new(maphash.Hash).Sum64()
}
