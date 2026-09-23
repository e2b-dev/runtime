package auth

import (
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/e2b-dev/infra/packages/auth/pkg/auth/internal/authcontext"
	"github.com/e2b-dev/infra/packages/auth/pkg/types"
)

// SetUserIDForTest sets the user ID on the gin context for use in tests.
func SetUserIDForTest(t *testing.T, c *gin.Context, userID uuid.UUID) {
	t.Helper()

	authcontext.SetUserID(c, userID)
}

// SetServiceIssuerForTest sets the verified service issuer on the gin context
// for use in tests, standing in for what the admin JWT authenticator records.
func SetServiceIssuerForTest(t *testing.T, c *gin.Context, issuer string) {
	t.Helper()

	authcontext.SetServiceIssuer(c, issuer)
}

// SetTeamInfoForTest sets the team info on the gin context for use in tests.
func SetTeamInfoForTest(t *testing.T, c *gin.Context, team *types.Team) {
	t.Helper()

	authcontext.SetTeamInfo(c, team)
}
