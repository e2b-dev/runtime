package server

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
)

// checkpointInProgressDetail returns the CHECKPOINT_IN_PROGRESS detail an
// error carries, or nil.
func checkpointInProgressDetail(t *testing.T, err error) *orchestrator.UserError {
	t.Helper()

	st, ok := status.FromError(err)
	require.True(t, ok)
	for _, detail := range st.Details() {
		if userErr, ok := detail.(*orchestrator.UserError); ok && userErr.GetCode() == orchestrator.UserErrorCode_CHECKPOINT_IN_PROGRESS {
			return userErr
		}
	}

	return nil
}

// The refusal keeps the code each RPC answered before and adds the typed
// detail, so the API keys on the detail and never on the message.
func TestCheckpointInProgressStatus_CarriesDetailOnEitherCode(t *testing.T) {
	t.Parallel()

	for name, code := range map[string]codes.Code{"pause": codes.ResourceExhausted, "checkpoint": codes.FailedPrecondition} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			err := checkpointInProgressStatus(code, "sandbox '%s' is being checkpointed, please retry", "sbx-1").Err()
			require.Equal(t, code, status.Code(err))
			require.ErrorContains(t, err, "sandbox 'sbx-1' is being checkpointed")

			detail := checkpointInProgressDetail(t, err)
			require.NotNil(t, detail, "the refusal must carry its typed detail")
			assert.Equal(t, uint32(http.StatusServiceUnavailable), detail.GetHttpStatus())
			assert.Equal(t, "sandbox 'sbx-1' is being checkpointed, please retry", detail.GetMessage())
		})
	}
}
