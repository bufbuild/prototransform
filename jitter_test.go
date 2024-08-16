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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestAddJitter(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name          string
		pollingPeriod time.Duration
		jitter        float64
		min, max      time.Duration
	}{
		{
			name:          "no jitter",
			pollingPeriod: time.Minute,
			jitter:        0,
			min:           time.Minute,
			max:           time.Minute,
		},
		{
			name:          "100% jitter",
			pollingPeriod: 3 * time.Minute,
			jitter:        1,
			min:           1,
			max:           6 * time.Minute,
		},
		{
			name:          "25% jitter",
			pollingPeriod: 4 * time.Second,
			jitter:        0.25,
			min:           3 * time.Second,
			max:           5 * time.Second,
		},
	}
	for _, testCase := range testCases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			var minPeriod, maxPeriod time.Duration
			for i := 0; i < 10_000; i++ {
				period := addJitter(testCase.pollingPeriod, testCase.jitter)
				require.GreaterOrEqual(t, period, testCase.min)
				require.LessOrEqual(t, period, testCase.max)
				if i == 0 || period < minPeriod {
					minPeriod = period
				}
				if i == 0 || period > maxPeriod {
					maxPeriod = period
				}
			}
			// After 10k iterations, with uniform distribution RNG, we could
			// probably put much tighter bound on this; but we're a bit lenient
			// to make sure we don't see flaky failures in CI. We want to observe
			// at least 90% of the effective jitter range.
			minVariation := time.Duration(0.9 * float64(testCase.max-testCase.min))
			require.GreaterOrEqual(t, maxPeriod-minPeriod, minVariation)
		})
	}
}
