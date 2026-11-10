package middleware

import (
	"github.com/gin-gonic/gin"
	"github.com/launchdarkly/go-sdk-common/v3/ldcontext"

	"github.com/e2b-dev/infra/packages/auth/pkg/auth"
	"github.com/e2b-dev/infra/packages/shared/pkg/clusters"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
)

func InitLaunchDarklyContext(c *gin.Context) {
	// generate contexts where appropriate
	var contexts []ldcontext.Context
	if context, ok := createLaunchDarklyTeamContext(c); ok {
		contexts = append(contexts, context)
	}
	if context, ok := createLaunchDarklyUserContext(c); ok {
		contexts = append(contexts, context)
	}

	// store the context in ctx
	ctx := c.Request.Context()
	ctx = featureflags.AddToContext(ctx, contexts...)
	c.Request = c.Request.WithContext(ctx)

	// we're done, move on
	c.Next()
}

func createLaunchDarklyUserContext(c *gin.Context) (ldcontext.Context, bool) {
	userID, ok := auth.GetUserID(c)
	if !ok {
		return ldcontext.Context{}, false
	}

	return featureflags.UserContext(userID.String()), true
}

func createLaunchDarklyTeamContext(c *gin.Context) (ldcontext.Context, bool) {
	team, ok := auth.GetTeamInfo(c)
	if !ok {
		return ldcontext.Context{}, false
	}

	var contexts []ldcontext.Context
	if team != nil {
		contexts = append(contexts, featureflags.TeamContextWithName(team.ID.String(), team.Name))
		realClusterID := clusters.WithClusterFallback(team.ClusterID)
		contexts = append(contexts, featureflags.ClusterContext(realClusterID))
	}

	if len(contexts) == 0 {
		return ldcontext.Context{}, false
	}

	return ldcontext.NewMulti(contexts...), true
}
