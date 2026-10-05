package authcontext

import (
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/e2b-dev/infra/packages/auth/pkg/types"
)

const (
	teamContextKey          = "team"
	userIDContextKey        = "user_id"
	serviceIssuerContextKey = "service_issuer"
)

func SetUserID(c *gin.Context, userID uuid.UUID) {
	setInGinContext(c, userIDContextKey, userID)
}

func GetUserID(c *gin.Context) (uuid.UUID, bool) {
	return getFromGinContextSafely[uuid.UUID](c, userIDContextKey)
}

func MustGetUserID(c *gin.Context) uuid.UUID {
	userID, ok := GetUserID(c)
	if !ok {
		panic("user id not found in context")
	}

	return userID
}

func SetTeamInfo(c *gin.Context, t *types.Team) {
	setInGinContext(c, teamContextKey, t)
}

func MustGetTeamInfo(c *gin.Context) *types.Team {
	team, ok := GetTeamInfo(c)
	if !ok {
		panic("team not found in context")
	}

	return team
}

func MustGetTeamID(c *gin.Context) uuid.UUID {
	return MustGetTeamInfo(c).Team.ID
}

func GetTeamInfo(c *gin.Context) (*types.Team, bool) {
	return getFromGinContextSafely[*types.Team](c, teamContextKey)
}

func setInGinContext(c *gin.Context, key string, value any) {
	c.Set(key, value)
}

func getFromGinContextSafely[T any](c *gin.Context, contextKey string) (T, bool) {
	var t T

	val, ok := c.Get(contextKey)
	if !ok {
		return t, false
	}

	t, ok = val.(T)

	return t, ok
}

func SetServiceIssuer(c *gin.Context, issuer string) {
	setInGinContext(c, serviceIssuerContextKey, issuer)
}

func GetServiceIssuer(c *gin.Context) (string, bool) {
	return getFromGinContextSafely[string](c, serviceIssuerContextKey)
}
