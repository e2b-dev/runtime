package pool

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestDestination_LimiterKey verifies the limiter key is decoupled from the
// pool key: LimitKey when set, else ConnectionKey. This is what lets the
// client-proxy share one ConnectionKey (pool reuse) while limiting per sandbox.
func TestDestination_LimiterKey(t *testing.T) {
	t.Parallel()

	// No LimitKey: falls back to ConnectionKey (orchestrator behavior — limiter
	// scoped per lifecycle, identical to before this field existed).
	lifecycle := Destination{ConnectionKey: "lifecycle-abc"}
	assert.Equal(t, "lifecycle-abc", lifecycle.LimiterKey())

	// LimitKey set: limiter is keyed per sandbox while the pool key stays shared
	// (client-proxy behavior — one pool, per-sandbox limit).
	shared := Destination{ConnectionKey: "client-proxy", LimitKey: "sbx-123"}
	assert.Equal(t, "sbx-123", shared.LimiterKey())
	assert.Equal(t, "client-proxy", shared.ConnectionKey, "pool key must stay shared, not fragmented per sandbox")

	// Empty both: empty (admission with an empty key is the caller's concern).
	assert.Equal(t, "", Destination{}.LimiterKey())
}
