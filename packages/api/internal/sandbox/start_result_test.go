package sandbox

import (
	"errors"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxstore/sandboxtypes"
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
