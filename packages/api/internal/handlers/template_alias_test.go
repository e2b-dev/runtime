package handlers

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	apispec "github.com/e2b-dev/infra/packages/api/internal/api"
	templatecache "github.com/e2b-dev/infra/packages/api/internal/cache/templates"
	"github.com/e2b-dev/infra/packages/auth/pkg/auth"
	"github.com/e2b-dev/infra/packages/auth/pkg/types"
	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
	dbtypes "github.com/e2b-dev/infra/packages/db/pkg/types"
	"github.com/e2b-dev/infra/packages/shared/pkg/id"
	redis_utils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
)

func queryTemplateAliasForTeam(
	t *testing.T,
	store *APIStore,
	teamID uuid.UUID,
	teamSlug string,
	alias string,
) *apispec.GetTemplatesAliasesAliasResponse {
	t.Helper()

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequestWithContext(t.Context(), http.MethodGet, fmt.Sprintf("/templates/aliases/%s", alias), nil)
	auth.SetTeamInfoForTest(t, c, &types.Team{
		Team: &dbtypes.Team{
			ID:   teamID,
			Slug: teamSlug,
		},
	})

	store.GetTemplatesAliasesAlias(c, alias)

	res, err := apispec.ParseGetTemplatesAliasesAliasResponse(w.Result())
	require.NoError(t, err)

	return res
}

func TestQueryNotExistingTemplateAlias(t *testing.T) {
	t.Parallel()

	testDB := testutils.SetupDatabase(t)
	redis := redis_utils.SetupInstance(t)
	ctx := t.Context()

	store := &APIStore{
		sqlcDB:        testDB.SqlcClient,
		authDB:        testDB.AuthDb,
		templateCache: templatecache.NewTemplateCache(testDB.SqlcClient, redis),
	}

	alias := "non-existing-template-alias"
	teamID := testutils.CreateTestTeam(t, testDB)
	teamSlug := testutils.GetTeamSlug(t, ctx, testDB, teamID)

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequestWithContext(t.Context(), http.MethodGet, fmt.Sprintf("/templates/aliases/%s", alias), nil)
	auth.SetTeamInfoForTest(t, c, &types.Team{
		Team: &dbtypes.Team{
			ID:   teamID,
			Slug: teamSlug,
		},
	})

	store.GetTemplatesAliasesAlias(c, alias)

	res, err := apispec.ParseGetTemplatesAliasesAliasResponse(w.Result())
	require.NoError(t, err)
	require.Equal(t, http.StatusNotFound, res.StatusCode())
}

func TestQueryExistingTemplateAlias(t *testing.T) {
	t.Parallel()

	testDB := testutils.SetupDatabase(t)
	redis := redis_utils.SetupInstance(t)
	ctx := t.Context()

	teamID := testutils.CreateTestTeam(t, testDB)
	teamSlug := testutils.GetTeamSlug(t, ctx, testDB, teamID)
	templateID := testutils.CreateTestTemplate(t, testDB, teamID)

	// Create alias with team's namespace
	alias := testutils.CreateTestTemplateAliasWithNamespace(t, testDB, templateID, &teamSlug)

	store := &APIStore{
		sqlcDB:        testDB.SqlcClient,
		authDB:        testDB.AuthDb,
		templateCache: templatecache.NewTemplateCache(testDB.SqlcClient, redis),
	}

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequestWithContext(t.Context(), http.MethodGet, fmt.Sprintf("/templates/aliases/%s", alias), nil)
	auth.SetTeamInfoForTest(t, c, &types.Team{
		Team: &dbtypes.Team{
			ID:   teamID,
			Slug: teamSlug,
		},
	})

	store.GetTemplatesAliasesAlias(c, alias)

	res, err := apispec.ParseGetTemplatesAliasesAliasResponse(w.Result())
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, res.StatusCode())

	require.NotNil(t, res.JSON200)

	resBody := *res.JSON200
	assert.Equal(t, templateID, resBody.TemplateID)
	assert.True(t, resBody.Public)
}

func TestQueryExistingTemplateAliasAsNotOwnerTeam(t *testing.T) {
	t.Parallel()

	testDB := testutils.SetupDatabase(t)
	redis := redis_utils.SetupInstance(t)
	ctx := t.Context()

	ownerTeamID := testutils.CreateTestTeam(t, testDB)
	ownerTeamSlug := testutils.GetTeamSlug(t, ctx, testDB, ownerTeamID)
	foreignTeamID := testutils.CreateTestTeam(t, testDB)
	foreignTeamSlug := testutils.GetTeamSlug(t, ctx, testDB, foreignTeamID)

	templateID := testutils.CreateTestTemplate(t, testDB, ownerTeamID)
	// Create alias with owner's namespace
	alias := testutils.CreateTestTemplateAliasWithNamespace(t, testDB, templateID, &ownerTeamSlug)

	store := &APIStore{
		sqlcDB:        testDB.SqlcClient,
		authDB:        testDB.AuthDb,
		templateCache: templatecache.NewTemplateCache(testDB.SqlcClient, redis),
	}

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequestWithContext(t.Context(), http.MethodGet, fmt.Sprintf("/templates/aliases/%s", alias), nil)
	auth.SetTeamInfoForTest(t, c,
		&types.Team{
			Team: &dbtypes.Team{
				ID:   foreignTeamID,
				Slug: foreignTeamSlug,
			},
		},
	)

	store.GetTemplatesAliasesAlias(c, alias)

	res, err := apispec.ParseGetTemplatesAliasesAliasResponse(w.Result())
	require.NoError(t, err)
	// Foreign team uses their own namespace for lookup, which won't match the owner's namespace
	// This results in 404 (not found) instead of 403 (forbidden) with the new exact match behavior
	require.Equal(t, http.StatusNotFound, res.StatusCode())
}

// TestTaggedTemplateAliasTransferredTemplate verifies that when an alias still
// resolves to a template whose ownership has moved to a different team, the
// ownership check returns 403 *before* the tag-existence probe runs. Otherwise
// a non-owner could distinguish existing tags from missing ones on a template
// they no longer have access to via the 404 vs 403 response.
func TestTaggedTemplateAliasTransferredTemplate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		setTags func(t *testing.T, ctx context.Context, db *testutils.Database, templateID string)
		tag     string
	}{
		{
			name: "tag exists on transferred template",
			setTags: func(t *testing.T, ctx context.Context, db *testutils.Database, templateID string) {
				t.Helper()
				buildID := testutils.CreateTestBuild(t, ctx, db, templateID, dbtypes.BuildStatusGroupReady)
				testutils.CreateTestBuildAssignment(t, ctx, db, templateID, buildID, "dev")
			},
			tag: "dev",
		},
		{
			name:    "tag missing on transferred template",
			setTags: func(_ *testing.T, _ context.Context, _ *testutils.Database, _ string) {},
			tag:     "dev",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			testDB := testutils.SetupDatabase(t)
			redis := redis_utils.SetupInstance(t)
			ctx := t.Context()

			requesterTeamID := testutils.CreateTestTeam(t, testDB)
			requesterTeamSlug := testutils.GetTeamSlug(t, ctx, testDB, requesterTeamID)
			newOwnerTeamID := testutils.CreateTestTeam(t, testDB)

			// Template owned by the new team but alias still lives in the requester's
			// namespace, mirroring the post-transfer state the in-code comment guards.
			templateID := testutils.CreateTestTemplate(t, testDB, newOwnerTeamID)
			alias := testutils.CreateTestTemplateAliasWithNamespace(t, testDB, templateID, &requesterTeamSlug)

			tt.setTags(t, ctx, testDB, templateID)

			store := &APIStore{
				sqlcDB:        testDB.SqlcClient,
				authDB:        testDB.AuthDb,
				templateCache: templatecache.NewTemplateCache(testDB.SqlcClient, redis),
			}

			res := queryTemplateAliasForTeam(t, store, requesterTeamID, requesterTeamSlug, id.WithTag(alias, tt.tag))

			require.Equal(t, http.StatusForbidden, res.StatusCode(),
				"requester must get 403 regardless of tag presence on a foreign-owned template")
		})
	}
}

func TestQueryTaggedTemplateAlias(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setupAlias func(t *testing.T, ctx context.Context, db *testutils.Database, templateID string, alias string) string
		wantStatus int
	}{
		{
			name: "existing alias with ready tag returns ok",
			setupAlias: func(t *testing.T, ctx context.Context, db *testutils.Database, templateID string, alias string) string {
				t.Helper()
				buildID := testutils.CreateTestBuild(t, ctx, db, templateID, dbtypes.BuildStatusGroupReady)
				testutils.CreateTestBuildAssignment(t, ctx, db, templateID, buildID, "dev")

				return id.WithTag(alias, "dev")
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "existing alias with missing tag returns not found",
			setupAlias: func(_ *testing.T, _ context.Context, _ *testutils.Database, _ string, alias string) string {
				return id.WithTag(alias, "missing")
			},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "existing alias with explicit default tag returns ok",
			setupAlias: func(t *testing.T, ctx context.Context, db *testutils.Database, templateID string, alias string) string {
				t.Helper()
				buildID := testutils.CreateTestBuild(t, ctx, db, templateID, dbtypes.BuildStatusGroupReady)
				testutils.CreateTestBuildAssignment(t, ctx, db, templateID, buildID, id.DefaultTag)

				return id.WithTag(alias, id.DefaultTag)
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "existing alias with missing explicit default tag returns not found",
			setupAlias: func(_ *testing.T, _ context.Context, _ *testutils.Database, _ string, alias string) string {
				return id.WithTag(alias, id.DefaultTag)
			},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "existing alias with non-ready tag returns not found",
			setupAlias: func(t *testing.T, ctx context.Context, db *testutils.Database, templateID string, alias string) string {
				t.Helper()
				buildID := testutils.CreateTestBuild(t, ctx, db, templateID, dbtypes.BuildStatusGroupPending)
				testutils.CreateTestBuildAssignment(t, ctx, db, templateID, buildID, "dev")

				return id.WithTag(alias, "dev")
			},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "existing alias with ready build id suffix returns ok",
			setupAlias: func(t *testing.T, ctx context.Context, db *testutils.Database, templateID string, alias string) string {
				t.Helper()
				buildID := testutils.CreateTestBuild(t, ctx, db, templateID, dbtypes.BuildStatusGroupReady)
				testutils.CreateTestBuildAssignment(t, ctx, db, templateID, buildID, id.DefaultTag)

				return id.WithTag(alias, buildID.String())
			},
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			testDB := testutils.SetupDatabase(t)
			redis := redis_utils.SetupInstance(t)
			ctx := t.Context()

			teamID := testutils.CreateTestTeam(t, testDB)
			teamSlug := testutils.GetTeamSlug(t, ctx, testDB, teamID)
			templateID := testutils.CreateTestTemplate(t, testDB, teamID)
			alias := testutils.CreateTestTemplateAliasWithNamespace(t, testDB, templateID, &teamSlug)

			store := &APIStore{
				sqlcDB:        testDB.SqlcClient,
				authDB:        testDB.AuthDb,
				templateCache: templatecache.NewTemplateCache(testDB.SqlcClient, redis),
			}

			taggedAlias := tt.setupAlias(t, ctx, testDB, templateID, alias)
			res := queryTemplateAliasForTeam(t, store, teamID, teamSlug, taggedAlias)

			require.Equal(t, tt.wantStatus, res.StatusCode())
			if tt.wantStatus != http.StatusOK {
				return
			}

			require.NotNil(t, res.JSON200)
			assert.Equal(t, templateID, res.JSON200.TemplateID)
		})
	}
}
