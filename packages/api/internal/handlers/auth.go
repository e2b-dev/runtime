package handlers

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	templatecache "github.com/e2b-dev/infra/packages/api/internal/cache/templates"
	dbapi "github.com/e2b-dev/infra/packages/api/internal/db"
	"github.com/e2b-dev/infra/packages/api/internal/middleware"
	"github.com/e2b-dev/infra/packages/auth/pkg/auth"
	"github.com/e2b-dev/infra/packages/auth/pkg/types"
	"github.com/e2b-dev/infra/packages/shared/pkg/id"
)

// applyTeamAccessCheck maps a team-access denial to a 403 *api.APIError.
func applyTeamAccessCheck(c *gin.Context, team *types.Team) *api.APIError {
	if err := middleware.CheckTeamAccessForRoute(c, team); err != nil {
		return &api.APIError{
			Code:      http.StatusForbidden,
			ClientMsg: err.Error(),
			Err:       err,
		}
	}

	return nil
}

func (a *APIStore) GetTeam(
	ctx context.Context,
	c *gin.Context,
	// Deprecated: use API Token authentication instead.
	teamID *string,
) (*types.Team, *api.APIError) {
	ctx, span := tracer.Start(ctx, "get team-and-tier")
	defer span.End()

	if team, ok := auth.GetTeamInfo(c); ok {
		return team, nil
	}

	if userID, ok := auth.GetUserID(c); ok {
		teams, apiErr := a.getUserTeams(ctx, userID)
		if apiErr != nil {
			return nil, apiErr
		}

		team, err := findTeam(teams, teamID)
		if err != nil {
			if teamID == nil {
				return nil, &api.APIError{
					Code:      http.StatusInternalServerError,
					ClientMsg: "Default team not found",
					Err:       err,
				}
			}

			return nil, &api.APIError{
				Code:      http.StatusForbidden,
				ClientMsg: "You are not allowed to access this team",
				Err:       err,
			}
		}

		// More restrictive than before, but OK on this deprecated path.
		if apiErr := applyTeamAccessCheck(c, team); apiErr != nil {
			return nil, apiErr
		}

		return team, nil
	}

	return nil, &api.APIError{
		Code:      http.StatusUnauthorized,
		ClientMsg: "You are not authenticated",
		Err:       errors.New("invalid authentication context"),
	}
}

func findTeam(teams []*types.TeamWithDefault, teamID *string) (*types.Team, error) {
	if teamID != nil {
		teamUUID, err := id.ParseTeamID(*teamID)
		if err != nil {
			return nil, fmt.Errorf("invalid team ID: %s", *teamID)
		}

		for _, t := range teams {
			if t.Team.ID == teamUUID {
				return t.Team, nil
			}
		}

		return nil, fmt.Errorf("team '%s' not found", *teamID)
	}

	for _, t := range teams {
		if t.IsDefault {
			return t.Team, nil
		}
	}

	return nil, errors.New("default team not found")
}

// teamIDMatches reports whether candidate names teamID, as a UUID or as its
// public project ID.
func teamIDMatches(teamID uuid.UUID, candidate string) bool {
	parsed, err := id.ParseTeamID(candidate)

	return err == nil && parsed == teamID
}

func (a *APIStore) getUserTeams(ctx context.Context, userID uuid.UUID) ([]*types.TeamWithDefault, *api.APIError) {
	teams, err := dbapi.GetTeamsByUser(ctx, a.authDB, userID)
	if err != nil {
		return nil, &api.APIError{
			Code:      http.StatusInternalServerError,
			ClientMsg: "Error getting user teams",
			Err:       err,
		}
	}

	if len(teams) == 0 {
		return nil, &api.APIError{
			Code:      http.StatusForbidden,
			ClientMsg: "You don't have access to any teams",
			Err:       errors.New("user has no teams"),
		}
	}

	return teams, nil
}

// resolveTemplateAndTeam resolves a template identifier and returns both the alias info and the owning team.
// For API key auth: supports both template ID and alias (team context is unambiguous).
// For user bearer auth: only template ID lookup (aliases are ambiguous across multiple teams).
// Returns 403 if the template is found but user doesn't have ownership.
func (a *APIStore) resolveTemplateAndTeam(
	ctx context.Context,
	c *gin.Context,
	identifier string,
) (*types.Team, *templatecache.AliasInfo, *api.APIError) {
	if team, ok := auth.GetTeamInfo(c); ok {
		aliasInfo, err := a.templateCache.ResolveAlias(ctx, identifier, team.Slug)
		if err != nil {
			return nil, nil, templatecache.ErrorToAPIError(err, identifier)
		}

		if aliasInfo.TeamID != team.ID {
			return nil, nil, &api.APIError{
				Code:      http.StatusForbidden,
				ClientMsg: fmt.Sprintf("You don't have access to template '%s'", identifier),
				Err:       fmt.Errorf("team '%s' does not own template", team.ID),
			}
		}

		return team, aliasInfo, nil
	}

	if userID, ok := auth.GetUserID(c); ok {
		aliasInfo, err := a.templateCache.GetByID(ctx, identifier)
		if err != nil {
			return nil, nil, templatecache.ErrorToAPIError(err, identifier)
		}

		userTeams, apiErr := a.getUserTeams(ctx, userID)
		if apiErr != nil {
			return nil, nil, apiErr
		}

		for _, t := range userTeams {
			if t.Team.ID == aliasInfo.TeamID {
				// More restrictive than before, but OK on this deprecated path.
				if apiErr := applyTeamAccessCheck(c, t.Team); apiErr != nil {
					return nil, nil, apiErr
				}

				return t.Team, aliasInfo, nil
			}
		}

		return nil, nil, &api.APIError{
			Code:      http.StatusForbidden,
			ClientMsg: fmt.Sprintf("You don't have access to template '%s'", identifier),
			Err:       errors.New("user does not have access to template's team"),
		}
	}

	return nil, nil, &api.APIError{
		Code:      http.StatusUnauthorized,
		ClientMsg: "You are not authenticated",
		Err:       errors.New("invalid authentication context"),
	}
}
