package handlers

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/dashboard-api/internal/identity"
)

func TestGetAdminUserProfilesUserIdWithoutIdentityProvider(t *testing.T) {
	t.Parallel()

	userID := uuid.New()
	recorder := httptest.NewRecorder()
	ginCtx, _ := gin.CreateTestContext(recorder)
	ginCtx.Request = httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/admin/user-profiles/"+userID.String(), nil)

	store := &APIStore{identityService: identity.NewUnavailableService()}
	store.GetAdminUserProfilesUserId(ginCtx, userID)

	require.Equal(t, http.StatusServiceUnavailable, recorder.Code)
	require.JSONEq(t, `{"code":503,"message":"No identity provider is configured; this endpoint is unavailable"}`, recorder.Body.String())
}
