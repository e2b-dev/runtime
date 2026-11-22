package handlers

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/db/pkg/testutils"
	dbtypes "github.com/e2b-dev/infra/packages/api/internal/db/pkg/types"
	"github.com/e2b-dev/infra/packages/api/internal/team"
	sharedauth "github.com/e2b-dev/infra/packages/auth/pkg/auth"
	authtypes "github.com/e2b-dev/infra/packages/auth/pkg/types"
	sharedkeys "github.com/e2b-dev/infra/packages/shared/pkg/keys"
)

func TestPostAdminTeamsTeamIDApiKeysCreatesTeamKey(t *testing.T) {
	t.Parallel()

	testDB := testutils.SetupDatabase(t)
	teamID := testutils.CreateTestTeam(t, testDB)

	recorder := httptest.NewRecorder()
	ginCtx, _ := gin.CreateTestContext(recorder)
	ginCtx.Request = httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/admin/teams/"+teamID.String()+"/api-keys", strings.NewReader(`{
		"name": "Admin integration"
	}`))
	ginCtx.Request.Header.Set("Content-Type", "application/json")

	store := &APIStore{
		authDB:      testDB.AuthDB,
		authService: fakeAPIKeyAuthService{team: &authtypes.Team{Team: &dbtypes.Team{ID: teamID}}},
	}
	store.PostAdminTeamsTeamIDApiKeys(ginCtx, teamID)

	require.Equal(t, http.StatusCreated, recorder.Code, recorder.Body.String())

	var body api.CreatedTeamAPIKey
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &body))
	require.NotEqual(t, uuid.Nil, body.Id)
	require.Equal(t, "Admin integration", body.Name)
	require.NotEmpty(t, body.Key)
	require.Nil(t, body.CreatedBy)

	keys, err := testDB.AuthDB.GetTeamAPIKeysWithCreator(t.Context(), teamID)
	require.NoError(t, err)
	require.Len(t, keys, 1)
	require.Equal(t, "Admin integration", keys[0].Name)
	require.Nil(t, keys[0].CreatedByID)
}

func TestPostAdminTeamsTeamIDApiKeysRejectsBlockedTeam(t *testing.T) {
	t.Parallel()

	testDB := testutils.SetupDatabase(t)
	teamID := testutils.CreateTestTeam(t, testDB)
	blockedReason := "payment required"

	recorder := httptest.NewRecorder()
	ginCtx, _ := gin.CreateTestContext(recorder)
	ginCtx.Request = httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/admin/teams/"+teamID.String()+"/api-keys", strings.NewReader(`{
		"name": "Admin integration"
	}`))
	ginCtx.Request.Header.Set("Content-Type", "application/json")

	store := &APIStore{
		authDB: testDB.AuthDB,
		authService: fakeAPIKeyAuthService{team: &authtypes.Team{Team: &dbtypes.Team{
			ID:            teamID,
			IsBlocked:     true,
			BlockedReason: &blockedReason,
		}}},
	}
	store.PostAdminTeamsTeamIDApiKeys(ginCtx, teamID)

	require.Equal(t, http.StatusForbidden, recorder.Code, recorder.Body.String())

	keys, err := testDB.AuthDB.GetTeamAPIKeysWithCreator(t.Context(), teamID)
	require.NoError(t, err)
	require.Empty(t, keys)
}

func TestPostAdminTeamsTeamIDApiKeysRejectsBannedTeam(t *testing.T) {
	t.Parallel()

	testDB := testutils.SetupDatabase(t)
	teamID := testutils.CreateTestTeam(t, testDB)

	recorder := httptest.NewRecorder()
	ginCtx, _ := gin.CreateTestContext(recorder)
	ginCtx.Request = httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/admin/teams/"+teamID.String()+"/api-keys", strings.NewReader(`{
		"name": "Admin integration"
	}`))
	ginCtx.Request.Header.Set("Content-Type", "application/json")

	store := &APIStore{
		authDB:      testDB.AuthDB,
		authService: fakeAPIKeyAuthService{err: &sharedauth.TeamForbiddenError{Message: "team is banned"}},
	}
	store.PostAdminTeamsTeamIDApiKeys(ginCtx, teamID)

	require.Equal(t, http.StatusForbidden, recorder.Code, recorder.Body.String())
}

func TestPostAdminTeamsTeamIDApiKeysRejectsMissingTeam(t *testing.T) {
	t.Parallel()

	testDB := testutils.SetupDatabase(t)
	teamID := uuid.New()

	recorder := httptest.NewRecorder()
	ginCtx, _ := gin.CreateTestContext(recorder)
	ginCtx.Request = httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/admin/teams/"+teamID.String()+"/api-keys", strings.NewReader(`{
		"name": "Admin integration"
	}`))
	ginCtx.Request.Header.Set("Content-Type", "application/json")

	store := &APIStore{
		authDB:      testDB.AuthDB,
		authService: fakeAPIKeyAuthService{err: sql.ErrNoRows},
	}
	store.PostAdminTeamsTeamIDApiKeys(ginCtx, teamID)

	require.Equal(t, http.StatusNotFound, recorder.Code, recorder.Body.String())
}

func TestPostAdminTeamsTeamIDApiKeysRejectsNilTeam(t *testing.T) {
	t.Parallel()

	testDB := testutils.SetupDatabase(t)
	teamID := uuid.New()

	recorder := httptest.NewRecorder()
	ginCtx, _ := gin.CreateTestContext(recorder)
	ginCtx.Request = httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/admin/teams/"+teamID.String()+"/api-keys", strings.NewReader(`{
		"name": "Admin integration"
	}`))
	ginCtx.Request.Header.Set("Content-Type", "application/json")

	store := &APIStore{
		authDB:      testDB.AuthDB,
		authService: fakeAPIKeyAuthService{},
	}
	store.PostAdminTeamsTeamIDApiKeys(ginCtx, teamID)

	require.Equal(t, http.StatusNotFound, recorder.Code, recorder.Body.String())
}

func TestDeleteAdminTeamsTeamIDApiKeysDeletesTeamKey(t *testing.T) {
	t.Parallel()

	testDB := testutils.SetupDatabase(t)
	teamID := testutils.CreateTestTeam(t, testDB)

	createRecorder := httptest.NewRecorder()
	createCtx, _ := gin.CreateTestContext(createRecorder)
	createCtx.Request = httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/admin/teams/"+teamID.String()+"/api-keys", strings.NewReader(`{"name":"Admin integration"}`))
	createCtx.Request.Header.Set("Content-Type", "application/json")

	var invalidatedHashes []string
	store := &APIStore{
		authDB: testDB.AuthDB,
		authService: fakeAPIKeyAuthService{
			team:                    &authtypes.Team{Team: &dbtypes.Team{ID: teamID}},
			invalidatedAPIKeyHashes: &invalidatedHashes,
		},
	}
	store.PostAdminTeamsTeamIDApiKeys(createCtx, teamID)
	require.Equal(t, http.StatusCreated, createRecorder.Code, createRecorder.Body.String())

	var created api.CreatedTeamAPIKey
	require.NoError(t, json.Unmarshal(createRecorder.Body.Bytes(), &created))

	deleteRecorder := httptest.NewRecorder()
	deleteCtx, _ := gin.CreateTestContext(deleteRecorder)
	deleteCtx.Request = httptest.NewRequestWithContext(t.Context(), http.MethodDelete, "/admin/teams/"+teamID.String()+"/api-keys/"+created.Id.String(), nil)

	store.DeleteAdminTeamsTeamIDApiKeysApiKeyID(deleteCtx, teamID, created.Id.String())
	require.Equal(t, http.StatusNoContent, deleteCtx.Writer.Status(), deleteRecorder.Body.String())

	keys, err := testDB.AuthDB.GetTeamAPIKeysWithCreator(t.Context(), teamID)
	require.NoError(t, err)
	require.Empty(t, keys)

	deletedKeyHash, err := sharedkeys.VerifyKey(sharedkeys.ApiKeyPrefix, created.Key)
	require.NoError(t, err)
	require.Equal(t, []string{deletedKeyHash}, invalidatedHashes, "expected the deleted key's auth cache entry to be invalidated")
}

func TestDeleteAdminTeamsTeamIDApiKeysRejectsMissingKey(t *testing.T) {
	t.Parallel()

	testDB := testutils.SetupDatabase(t)
	teamID := testutils.CreateTestTeam(t, testDB)

	store := &APIStore{authDB: testDB.AuthDB, authService: fakeAPIKeyAuthService{}}
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	missingKeyID := uuid.New()
	ctx.Request = httptest.NewRequestWithContext(t.Context(), http.MethodDelete, "/admin/teams/"+teamID.String()+"/api-keys/"+missingKeyID.String(), nil)

	store.DeleteAdminTeamsTeamIDApiKeysApiKeyID(ctx, teamID, missingKeyID.String())

	require.Equal(t, http.StatusNotFound, recorder.Code)
	require.Contains(t, recorder.Header().Get("Content-Type"), "application/json")
	require.JSONEq(t, `{"code":404,"message":"API key not found"}`, recorder.Body.String())
}

func TestGetAdminTeamsTeamIDApiKeysListsKeysOfBannedTeam(t *testing.T) {
	t.Parallel()

	testDB := testutils.SetupDatabase(t)
	teamID := testutils.CreateTestTeam(t, testDB)
	otherTeamID := testutils.CreateTestTeam(t, testDB)

	created, err := team.CreateAPIKey(t.Context(), testDB.AuthDB, teamID, nil, "Admin integration")
	require.NoError(t, err)
	_, err = team.CreateAPIKey(t.Context(), testDB.AuthDB, otherTeamID, nil, "Other team")
	require.NoError(t, err)

	recorder := httptest.NewRecorder()
	ginCtx, _ := gin.CreateTestContext(recorder)
	ginCtx.Request = httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/admin/teams/"+teamID.String()+"/api-keys", nil)

	store := &APIStore{
		authDB:      testDB.AuthDB,
		authService: fakeAPIKeyAuthService{err: &sharedauth.TeamForbiddenError{Message: "team is banned"}},
	}
	store.GetAdminTeamsTeamIDApiKeys(ginCtx, teamID)

	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())

	var body []api.TeamAPIKey
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &body))
	require.Len(t, body, 1)
	require.Equal(t, created.ID, body[0].Id)
	require.Equal(t, "Admin integration", body[0].Name)
	require.Equal(t, api.IdentifierMaskingDetails{
		Prefix:            created.ApiKeyPrefix,
		ValueLength:       int(created.ApiKeyLength),
		MaskedValuePrefix: created.ApiKeyMaskPrefix,
		MaskedValueSuffix: created.ApiKeyMaskSuffix,
	}, body[0].Mask)
	require.Nil(t, body[0].CreatedBy)
}

type fakeAPIKeyAuthService struct {
	team *authtypes.Team
	err  error

	invalidatedAPIKeyHashes *[]string
	invalidatedTeamIDs      *[]uuid.UUID
	invalidateTeamErr       error
}

func (f fakeAPIKeyAuthService) ValidateAPIKey(context.Context, *gin.Context, string) (*authtypes.Team, *sharedauth.APIError) {
	return nil, nil
}

func (f fakeAPIKeyAuthService) ValidateAuthProviderToken(context.Context, *gin.Context, string) (uuid.UUID, *sharedauth.APIError) {
	return uuid.Nil, nil
}

func (f fakeAPIKeyAuthService) ValidateAuthProviderTeam(context.Context, *gin.Context, string) (*authtypes.Team, *sharedauth.APIError) {
	return nil, nil
}

func (f fakeAPIKeyAuthService) GetTeamByID(context.Context, uuid.UUID) (*authtypes.Team, error) {
	if f.err != nil {
		return nil, f.err
	}

	return f.team, nil
}

func (f fakeAPIKeyAuthService) InvalidateTeamMemberCache(context.Context, uuid.UUID, string) {}

func (f fakeAPIKeyAuthService) InvalidateAPIKeyCache(_ context.Context, hashedKey string) {
	if f.invalidatedAPIKeyHashes != nil {
		*f.invalidatedAPIKeyHashes = append(*f.invalidatedAPIKeyHashes, hashedKey)
	}
}

func (f fakeAPIKeyAuthService) InvalidateTeamCache(_ context.Context, teamID uuid.UUID) error {
	if f.invalidatedTeamIDs != nil {
		*f.invalidatedTeamIDs = append(*f.invalidatedTeamIDs, teamID)
	}

	return f.invalidateTeamErr
}

func (f fakeAPIKeyAuthService) Close(context.Context) error {
	return nil
}
