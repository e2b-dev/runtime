package handlers

import (
	"bytes"
	"net/http"
	"slices"

	"github.com/gin-gonic/gin"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/team"
	"github.com/e2b-dev/infra/packages/auth/pkg/auth"
	authqueries "github.com/e2b-dev/infra/packages/db/pkg/auth/queries"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

func (a *APIStore) GetTeams(c *gin.Context) {
	ctx := c.Request.Context()

	userID := auth.MustGetUserID(c)

	results, err := a.authDB.GetTeamsWithUsersTeams(ctx, userID)
	if err != nil {
		telemetry.ReportCriticalError(ctx, "error when getting teams", err)
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Error when starting transaction")

		return
	}

	slices.SortFunc(results, func(a, b authqueries.GetTeamsWithUsersTeamsRow) int {
		if order := a.Team.CreatedAt.Compare(b.Team.CreatedAt); order != 0 {
			return order
		}

		return bytes.Compare(a.Team.ID[:], b.Team.ID[:])
	})

	teams := make([]api.Team, len(results))
	hasDefault := false
	for i, row := range results {
		// We create a new API key for the CLI and backwards compatibility with API Keys hashing
		apiKey, err := team.CreateAPIKey(ctx, a.authDB, row.Team.ID, &userID, "CLI login/configure")
		if err != nil {
			telemetry.ReportCriticalError(ctx, "error when creating team API key", err)
			a.sendAPIStoreError(c, http.StatusInternalServerError, "Error when creating team API key")

			return
		}

		teams[i] = api.Team{
			TeamID:    row.Team.ID.String(),
			Name:      row.Team.Name,
			ApiKey:    apiKey.RawAPIKey,
			IsDefault: row.IsDefault,
		}
		hasDefault = hasDefault || row.IsDefault
	}

	if !hasDefault && len(teams) > 0 {
		teams[0].IsDefault = true
	}

	c.JSON(http.StatusOK, teams)
}
