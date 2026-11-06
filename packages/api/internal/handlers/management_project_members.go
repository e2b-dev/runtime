package handlers

import (
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/management"
	"github.com/e2b-dev/infra/packages/shared/pkg/ginutils"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

func (a *APIStore) ManagementApplyProjectMember(c *gin.Context, projectID uuid.UUID, userID uuid.UUID) {
	ctx := c.Request.Context()
	attrs := []attribute.KeyValue{telemetry.WithTeamID(projectID.String())}

	body, err := ginutils.ParseBody[api.ManagementProjectMemberApplyRequest](ctx, c)
	if err != nil {
		telemetry.ReportErrorByCode(ctx, http.StatusBadRequest, "apply project member failed", fmt.Errorf("parse project member request: %w", err), attrs...)
		a.sendAPIStoreError(c, http.StatusBadRequest, "Invalid request body")

		return
	}

	var identities []management.ProjectMemberIdentity
	if body.Identities != nil {
		identities = make([]management.ProjectMemberIdentity, 0, len(*body.Identities))
		for _, identity := range *body.Identities {
			identities = append(identities, management.ProjectMemberIdentity{
				Issuer:  identity.Issuer,
				Subject: identity.Subject,
			})
		}
	}
	isDefault := false
	if body.IsDefault != nil {
		isDefault = *body.IsDefault
	}

	if err := a.managementService.ApplyProjectMember(ctx, management.ProjectMemberProjection{
		ProjectID:  projectID,
		UserID:     userID,
		Revision:   body.Revision,
		Present:    body.Present,
		IsDefault:  isDefault,
		Identities: identities,
	}); err != nil {
		a.sendProjectMemberError(c, err, attrs...)

		return
	}

	c.Status(http.StatusNoContent)
}
