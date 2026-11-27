package authservice

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	authqueries "github.com/e2b-dev/infra/packages/api/internal/db/pkg/auth/queries"
	db "github.com/e2b-dev/infra/packages/api/internal/db/readmodel"
	"github.com/e2b-dev/infra/packages/auth/pkg/auth"
	"github.com/e2b-dev/infra/packages/auth/pkg/types"
	"github.com/e2b-dev/infra/packages/shared/pkg/id"
	redis_utils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
)

type recordingMemberStore struct {
	staticAuthStore

	mu      sync.Mutex
	teamIDs []string
}

func (s *recordingMemberStore) GetTeamByIDAndUserID(_ context.Context, _ uuid.UUID, teamID string) (*types.Team, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.teamIDs = append(s.teamIDs, teamID)

	parsed, err := uuid.Parse(teamID)
	if err != nil {
		return nil, err
	}

	return db.NewTeam(authqueries.GetTeamByTeamIDRow{ID: parsed}), nil
}

func (s *recordingMemberStore) lookups() []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	return append([]string(nil), s.teamIDs...)
}

// The team header may carry the public project ID. Both spellings must share
// one cache entry, or evicting a removed member by UUID would leave the
// project-ID entry authorizing them until it expires.
func TestValidateAuthProviderTeamKeysBothSpellingsByUUID(t *testing.T) {
	t.Parallel()

	userID := uuid.New()
	teamID := uuid.MustParse("f47ac10b-58cc-4372-a567-0e02b2c3d479")
	projectID := id.ProjectID(teamID).String()

	store := &recordingMemberStore{}
	service := &AuthService{store: store, teamCache: newAuthCache(redis_utils.SetupInstance(t))}

	validate := func(header string) (*types.Team, *APIError) {
		c, _ := gin.CreateTestContext(httptest.NewRecorder())
		c.Request = httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", nil)
		auth.SetUserIDForTest(t, c, userID)

		return service.ValidateAuthProviderTeam(t.Context(), c, header)
	}

	team, apiErr := validate(projectID)
	require.Nil(t, apiErr)
	require.Equal(t, teamID, team.ID)
	require.Equal(t, []string{teamID.String()}, store.lookups(), "the store is queried by UUID")

	team, apiErr = validate(teamID.String())
	require.Nil(t, apiErr)
	require.Equal(t, teamID, team.ID)
	require.Len(t, store.lookups(), 1, "the UUID spelling hits the project-ID entry")

	service.InvalidateTeamMemberCache(t.Context(), userID, teamID.String())

	_, apiErr = validate(projectID)
	require.Nil(t, apiErr)
	require.Len(t, store.lookups(), 2, "evicting by UUID evicts the project-ID spelling")

	_, apiErr = validate(id.WorkspaceID(teamID).String())
	require.NotNil(t, apiErr)
	require.Equal(t, http.StatusUnauthorized, apiErr.Code)
	require.Len(t, store.lookups(), 2, "a malformed team ID never reaches the store")
}
