package handlers

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

// retiredBootstrapMessage tells the remaining caller why the route no longer
// works. The route stays registered so the contract is still documented while
// the Stripe projects coordinator migrates.
const retiredBootstrapMessage = "Team bootstrap is retired; create teams through the workspace API"

func (s *APIStore) PostAdminTeamsBootstrap(c *gin.Context) {
	ctx := c.Request.Context()

	logger.L().Error(ctx, "rejected retired admin team bootstrap request")
	s.sendAPIStoreError(c, http.StatusInternalServerError, retiredBootstrapMessage)
}
