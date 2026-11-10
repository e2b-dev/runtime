//go:build linux

package server

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestWaitForAcquire_ResourceExhaustedMessageFormat verifies the resuming-gate
// ResourceExhausted error carries the differentiating waited= and the gate's
// max= cap. The single slot is pre-taken and the context is already cancelled,
// so Acquire returns immediately with the saturated-gate error.
func TestWaitForAcquire_ResourceExhaustedMessageFormat(t *testing.T) {
	t.Parallel()

	s, _ := startGateTestServer(t) // startingSandboxes limit = 1
	require.True(t, s.startingSandboxes.TryAcquire(1), "pre-take the only slot")

	ctx, cancel := context.WithCancel(t.Context())
	cancel() // make Acquire give up at once rather than wait acquireTimeout

	err := s.waitForAcquire(ctx, startGateResume)
	require.Error(t, err)

	st, ok := status.FromError(err)
	require.True(t, ok, "must be a gRPC status error")
	assert.Equal(t, codes.ResourceExhausted, st.Code())

	msg := st.Message()
	assert.Contains(t, msg, "too many sandboxes resuming on this node")
	assert.Contains(t, msg, "waited=", "message must carry the differentiating wait duration")
	assert.Contains(t, msg, "max=1", "message must carry the gate's cap via Limit()")
	assert.Contains(t, msg, "please retry")
}
