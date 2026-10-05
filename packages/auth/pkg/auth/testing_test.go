package auth_test

import (
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/auth/pkg/auth"
)

// The helpers stand in for an authenticator, so what they set has to be what
// the accessors read: a context key that drifted would leave a test asserting
// against a value its handler never sees.
func TestTestHelpersSetWhatTheAccessorsRead(t *testing.T) {
	t.Parallel()

	c, _ := gin.CreateTestContext(httptest.NewRecorder())

	issuer, ok := auth.GetServiceIssuer(c)
	require.False(t, ok, "a bare context names an issuer")
	require.Empty(t, issuer)

	auth.SetServiceIssuerForTest(t, c, "https://issuer.example.test")
	issuer, ok = auth.GetServiceIssuer(c)
	require.True(t, ok)
	require.Equal(t, "https://issuer.example.test", issuer)

	userID := uuid.New()
	auth.SetUserIDForTest(t, c, userID)
	require.Equal(t, userID, auth.MustGetUserID(c))

	issuer, ok = auth.GetServiceIssuer(c)
	require.True(t, ok, "setting a user displaced the issuer")
	require.Equal(t, "https://issuer.example.test", issuer)
}
