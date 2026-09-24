package ingester

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/sorotrail/sorotrail/internal/rpc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRun_MaxRetries(t *testing.T) {
	origSleep := sleepCtx
	t.Cleanup(func() { sleepCtx = origSleep })

	tests := []struct {
		name       string
		maxRetries int
		failCount  int
		wantSleeps []time.Duration // upper bound check for backoff duration
	}{
		{
			name:       "disabled, backoff increases infinitely",
			maxRetries: 0,
			failCount:  4,
			wantSleeps: []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second},
		},
		{
			name:       "enabled, backoff resets after max",
			maxRetries: 2,
			failCount:  4,
			// err 1: retries=0->1, backoff=1s, sleeps <= 1s, next backoff=2s
			// err 2: retries=1->2, backoff=2s, sleeps <= 2s, next backoff=4s
			// err 3: retries=2 (>= max) -> resets to 0. retries=0->1, backoff=1s, sleeps <= 1s, next backoff=2s
			// err 4: retries=1->2, backoff=2s, sleeps <= 2s, next backoff=4s
			wantSleeps: []time.Duration{time.Second, 2 * time.Second, time.Second, 2 * time.Second},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &mockRPC{}
			for i := 0; i < tt.failCount; i++ {
				client.eventsErrs = append(client.eventsErrs, fmt.Errorf("boom"))
			}
			// One success at the end to break the retry loop and allow clean exit.
			client.eventsResps = []rpc.GetEventsResponse{{LatestLedger: 100}}

			opts := Options{
				StartLedger: 100,
				MaxRetries:  tt.maxRetries,
				MaxBackoff:  time.Hour, PollInterval: 10 * time.Millisecond,
			}
			ing := newTestIngester(client, newMockStore(), opts)

			var sleeps []time.Duration
			sleepCtx = func(ctx context.Context, d time.Duration) bool {
				// Record the maximum possible sleep (d is the jittered value <= backoff)
				// wait, backoff/2 + rand.N(backoff/2). The max is backoff.
				// Since we want to assert the backoff scale, we can just record the pre-jitter backoff if we knew it,
				// or we can just assert that the actual sleep duration falls in the expected range.
				sleeps = append(sleeps, d)
				return true // simulate instant sleep
			}

			// We need a context that will eventually cancel or we just let it run out of mockRPC responses.
			// runOnce will eventually succeed and wait in PollInterval sleep.
			// Let's cancel context inside sleepCtx when it hits PollInterval sleep to exit.
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			sleepCtx = func(ctx context.Context, d time.Duration) bool {
				if d == 10 * time.Millisecond {
					cancel()
					return false
				}
				sleeps = append(sleeps, d)
				return true
			}

			err := ing.Run(ctx)
			require.ErrorIs(t, err, context.Canceled)

			require.Len(t, sleeps, tt.failCount)
			for i, sleep := range sleeps {
				// sleep is jittered: backoff/2 <= sleep < backoff
				minSleep := tt.wantSleeps[i] / 2
				maxSleep := tt.wantSleeps[i]
				assert.GreaterOrEqual(t, sleep.Nanoseconds(), minSleep.Nanoseconds(), "sleep %d too short", i)
				assert.LessOrEqual(t, sleep.Nanoseconds(), maxSleep.Nanoseconds(), "sleep %d too long", i)
			}
		})
	}
}
