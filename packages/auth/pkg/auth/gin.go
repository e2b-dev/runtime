package auth

import (
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/e2b-dev/infra/packages/auth/pkg/auth/internal/authcontext"
	"github.com/e2b-dev/infra/packages/auth/pkg/types"
)

func GetUserID(c *gin.Context) (uuid.UUID, bool) {
	return authcontext.GetUserID(c)
}

func MustGetUserID(c *gin.Context) uuid.UUID {
	return authcontext.MustGetUserID(c)
}

func MustGetTeamInfo(c *gin.Context) *types.Team {
	return authcontext.MustGetTeamInfo(c)
}

func MustGetTeamID(c *gin.Context) uuid.UUID {
	return authcontext.MustGetTeamID(c)
}

func GetTeamInfo(c *gin.Context) (*types.Team, bool) {
	return authcontext.GetTeamInfo(c)
}

func GetServiceIssuer(c *gin.Context) (string, bool) {
	return authcontext.GetServiceIssuer(c)
}
