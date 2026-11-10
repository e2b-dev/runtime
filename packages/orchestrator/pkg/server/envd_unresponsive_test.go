//go:build linux

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

// envdUnresponsiveDetail returns the ENVD_UNRESPONSIVE detail an error
// carries, or nil.
func envdUnresponsiveDetail(t *testing.T, err error) *orchestrator.UserError {
	t.Helper()

	st, ok := status.FromError(err)
	require.True(t, ok)
	for _, detail := range st.Details() {
		if userErr, ok := detail.(*orchestrator.UserError); ok && userErr.GetCode() == orchestrator.UserErrorCode_ENVD_UNRESPONSIVE {
			return userErr
		}
	}

	return nil
}

// The refusal keeps the retryable code the API restores on and adds the typed
// detail, so the API keys on the detail and never on the message.
func TestEnvdUnresponsiveStatus_CarriesDetail(t *testing.T) {
	t.Parallel()

	err := envdUnresponsiveStatus("sbx-1").Err()
	require.Equal(t, codes.ResourceExhausted, status.Code(err))
	require.ErrorContains(t, err, "sandbox 'sbx-1' guest agent is not responding, please retry")

	detail := envdUnresponsiveDetail(t, err)
	require.NotNil(t, detail, "the refusal must carry its typed detail")
	assert.Equal(t, uint32(http.StatusServiceUnavailable), detail.GetHttpStatus())
	assert.Equal(t, "sandbox 'sbx-1' guest agent is not responding, please retry", detail.GetMessage())
	assert.Nil(t, checkpointInProgressDetail(t, err), "one detail, one refusal")
}
