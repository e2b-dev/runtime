package server

import (
	"errors"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox"
)

// A volume envd could not mount is refused for good, so placement asks no other
// node and the API answers with the path; any other error is left to the caller.
func TestVolumeRefusal(t *testing.T) {
	t.Parallel()

	err := volumeRefusal(t.Context(), fmt.Errorf("failed to wait for sandbox start: %w", sandbox.VolumeMountError{Path: "/data"}))
	require.Error(t, err)

	st, ok := status.FromError(err)
	require.True(t, ok)
	assert.Equal(t, codes.FailedPrecondition, st.Code())
	assert.Equal(t, `failed to mount volume at "/data"`, st.Message())

	refusal := createRefusalDetail(t, st)
	assert.Equal(t, st.Message(), refusal.GetMessage())
	assert.EqualValues(t, http.StatusInternalServerError, refusal.GetHttpStatus())

	assert.NoError(t, volumeRefusal(t.Context(), errors.New("envd did not start")))
}
