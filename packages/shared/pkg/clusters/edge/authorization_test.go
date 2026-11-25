package edge

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/shared/pkg/consts"
)

func TestAuthorization_CarriesTokenAndServiceInstanceID(t *testing.T) {
	t.Parallel()

	auth := Authorization{Token: "shh", ServiceInstanceID: "svc-a", TLS: true}

	md, err := auth.GetRequestMetadata(t.Context())
	require.NoError(t, err)
	assert.Equal(t, map[string]string{consts.EdgeRpcAuthHeader: "shh", consts.EdgeRpcServiceInstanceIDHeader: "svc-a"}, md)
	assert.True(t, auth.RequireTransportSecurity())
	assert.False(t, Authorization{}.RequireTransportSecurity())
}
