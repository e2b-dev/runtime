package permissions_test

import (
	"testing"
	"testing/synctest"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/envd/internal/permissions"
)

func TestGetKeepAliveTicker(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		header   string
		interval time.Duration
	}{
		{name: "missing", interval: 90 * time.Second},
		{name: "non-numeric", header: "invalid", interval: 90 * time.Second},
		{name: "zero", header: "0", interval: 90 * time.Second},
		{name: "negative", header: "-1", interval: 90 * time.Second},
		{name: "duration overflow", header: "9223372037", interval: 90 * time.Second},
		{name: "positive duration overflow", header: "18446744074", interval: 90 * time.Second},
		{name: "integer overflow", header: "9223372036854775808", interval: 90 * time.Second},
		{name: "one second", header: "1", interval: time.Second},
		{name: "SDK interval", header: "50", interval: 50 * time.Second},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				req := connect.NewRequest(&struct{}{})
				if tt.header != "" {
					req.Header().Set("Keepalive-Ping-Interval", tt.header)
				}

				var ticker *time.Ticker
				var reset func()
				require.NotPanics(t, func() {
					ticker, reset = permissions.GetKeepAliveTicker(req)
				})
				defer ticker.Stop()

				started := time.Now()
				time.Sleep(tt.interval)
				select {
				case tick := <-ticker.C:
					require.Equal(t, tt.interval, tick.Sub(started))
				default:
					t.Fatal("keepalive did not tick at the expected interval")
				}

				time.Sleep(tt.interval / 2)
				reset()
				resetAt := time.Now()
				time.Sleep(tt.interval)
				select {
				case tick := <-ticker.C:
					require.Equal(t, tt.interval, tick.Sub(resetAt), "reset should restart the same interval")
				default:
					t.Fatal("keepalive did not tick after reset")
				}
			})
		})
	}
}

func TestGetKeepAliveTicker_MaximumInterval(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		req := connect.NewRequest(&struct{}{})
		req.Header().Set("Keepalive-Ping-Interval", "9223372036")
		ticker, reset := permissions.GetKeepAliveTicker(req)
		defer ticker.Stop()

		// The largest representable whole-second interval must not fall back to 90s.
		// Observe a short window to avoid overflowing the virtual monotonic clock.
		for range 2 {
			time.Sleep(90 * time.Second)
			select {
			case <-ticker.C:
				t.Fatal("maximum valid interval fell back to the default")
			default:
			}
			reset()
		}
	})
}
