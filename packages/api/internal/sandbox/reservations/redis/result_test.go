package redis

import (
	"errors"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/sandbox/sandboxtypes"
)

func TestEncodeDecodeResult_APIErrorRoundTripsAllFields(t *testing.T) {
	t.Parallel()

	in := &api.APIError{
		Code:      http.StatusServiceUnavailable,
		ClientMsg: "Failed to place sandbox: not enough capacity for the requested resources right now, please retry shortly",
		ErrorCode: "sandbox_capacity_unavailable",
		Err:       errors.New("no nodes available"),
	}

	data, err := encodeResult(sandboxtypes.Sandbox{}, in)
	require.NoError(t, err)

	_, decodedErr := decodeResult(data)
	require.Error(t, decodedErr)

	var out *api.APIError
	require.ErrorAs(t, decodedErr, &out)
	assert.Equal(t, in.Code, out.Code)
	assert.Equal(t, in.ClientMsg, out.ClientMsg)
	assert.Equal(t, in.ErrorCode, out.ErrorCode)
	assert.Equal(t, in.Err.Error(), out.Err.Error())
}

// A failure the leader published only because its own context was cancelled is
// carried across instances as ErrReservationLeaderLeft, so a joined waiter can
// detect it (via errors.Is) and re-contend. See issue #3686.
func TestEncodeDecodeResult_LeaderLeftIsCarriedAsSentinel(t *testing.T) {
	t.Parallel()

	leaderErr := fmt.Errorf("%w: %w", sandboxtypes.ErrReservationLeaderLeft, &api.APIError{
		Code:      http.StatusGatewayTimeout,
		ClientMsg: "Failed to place sandbox: placement timed out, please retry",
		ErrorCode: "sandbox_placement_timeout",
		Err:       errors.New("context canceled"),
	})

	data, err := encodeResult(sandboxtypes.Sandbox{}, leaderErr)
	require.NoError(t, err)

	_, decodedErr := decodeResult(data)
	require.Error(t, decodedErr)
	assert.ErrorIs(t, decodedErr, sandboxtypes.ErrReservationLeaderLeft,
		"a leader-left result must decode to the re-contend sentinel")
}

// A genuine start failure (the leader's context was NOT cut) must not be
// mistaken for a leader-left result: the waiter adopts it as before.
func TestEncodeDecodeResult_GenuineFailureIsNotLeaderLeft(t *testing.T) {
	t.Parallel()

	data, err := encodeResult(sandboxtypes.Sandbox{}, &api.APIError{
		Code:      http.StatusServiceUnavailable,
		ClientMsg: "Failed to place sandbox: no compatible node for this template's requirements",
		ErrorCode: "sandbox_no_compatible_node",
		Err:       errors.New("no eligible nodes"),
	})
	require.NoError(t, err)

	_, decodedErr := decodeResult(data)
	require.Error(t, decodedErr)
	require.NotErrorIs(t, decodedErr, sandboxtypes.ErrReservationLeaderLeft)

	var out *api.APIError
	require.ErrorAs(t, decodedErr, &out)
	assert.Equal(t, http.StatusServiceUnavailable, out.Code)
}
