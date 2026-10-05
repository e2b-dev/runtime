package handlers

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/dashboard-api/internal/api"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

func (s *APIStore) GetTeamsTeamIDMembers(c *gin.Context, teamID api.TeamID) {
	ctx := c.Request.Context()
	telemetry.ReportEvent(ctx, "list team members")

	authTeamID, ok := s.requireAuthedTeamMatchesPath(c, teamID)
	if !ok {
		return
	}

	telemetry.SetAttributes(ctx, telemetry.WithTeamID(authTeamID.String()))

	rows, err := s.db.GetTeamMembers(ctx, authTeamID)
	if err != nil {
		logger.L().Error(ctx, "failed to get team members", zap.Error(err), logger.WithTeamID(authTeamID.String()))
		s.sendAPIStoreError(c, http.StatusInternalServerError, "Failed to get team members")

		return
	}

	userIDs := make([]uuid.UUID, 0, len(rows))
	for _, row := range rows {
		userIDs = append(userIDs, row.UserID)
	}

	profiles, err := s.identityService.ProfilesByUserID(ctx, userIDs)
	if err != nil {
		if s.abortIfNoIdentityProvider(c, err) {
			return
		}

		logger.L().Error(ctx, "failed to get member profiles", zap.Error(err), logger.WithTeamID(authTeamID.String()))
		s.sendAPIStoreError(c, http.StatusInternalServerError, "Failed to get team member profiles")

		return
	}

	members := make([]api.TeamMember, 0, len(rows))
	for _, row := range rows {
		profile, ok := profiles[row.UserID]
		if !ok || profile.Email == "" {
			logger.L().Warn(ctx, "team member has missing profile", logger.WithTeamID(authTeamID.String()), logger.WithUserID(row.UserID.String()))

			continue
		}

		member := api.TeamMember{
			Id:        row.UserID,
			Email:     profile.Email,
			IsDefault: row.IsDefault,
			AddedBy:   row.AddedBy,
			Providers: profile.Providers,
		}
		if member.Providers == nil {
			member.Providers = []string{}
		}
		if profile.Name != "" {
			member.Name = new(profile.Name)
		}
		if profile.ProfilePictureURL != "" {
			member.ProfilePictureUrl = new(profile.ProfilePictureURL)
		}

		if row.CreatedAt.Valid {
			t := row.CreatedAt.Time.UTC()
			member.CreatedAt = &t
		}

		members = append(members, member)
	}

	c.JSON(http.StatusOK, api.TeamMembersResponse{
		Members: members,
	})
}
