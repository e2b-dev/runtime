package handlers

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/launchdarkly/go-sdk-common/v3/ldlog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	templatecache "github.com/e2b-dev/infra/packages/api/internal/cache/templates"
	"github.com/e2b-dev/infra/packages/auth/pkg/auth"
	authtypes "github.com/e2b-dev/infra/packages/auth/pkg/types"
	authqueries "github.com/e2b-dev/infra/packages/db/pkg/auth/queries"
	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/id"
	redis_utils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
)

// TestPostSandboxes_FsOnlyAutoPauseVersionGate pins the create-time 400: a
// sandbox must not be born with autoPauseMemory=false on a template whose
// Firecracker version can never honor it — the alternative is a policy the
// timeout eviction silently degrades later. The test template's build carries
// the testutils default version ("1.4.0", a bare upstream version), which
// fails the fs-only floor both directly and through the offline flag
// resolution fcgate falls back to.
func TestPostSandboxes_FsOnlyAutoPauseVersionGate(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	redis := redis_utils.SetupInstance(t)
	ctx := t.Context()

	teamID := testutils.CreateTestTeam(t, db)
	teamSlug := testutils.GetTeamSlug(t, ctx, db, teamID)

	flags, err := featureflags.NewClientWithLogLevel(ldlog.Error)
	require.NoError(t, err)

	store := &APIStore{
		templateCache: templatecache.NewTemplateCache(db.SqlcClient, redis),
		featureFlags:  flags,
	}
	defer func() {
		require.NoError(t, store.templateCache.Close(ctx))
	}()

	alias := "fs-only-gate-template"
	templateID := createTestTemplate(ctx, t, db, teamID)
	createTestTemplateAliasWithName(ctx, t, db, templateID, alias, &teamSlug)
	buildID := testutils.CreateTestBuild(t, ctx, db, templateID, "ready")
	testutils.CreateTestBuildAssignment(t, ctx, db, templateID, buildID, id.DefaultTag)

	recorder := httptest.NewRecorder()
	ginCtx, _ := gin.CreateTestContext(recorder)

	autoPause := true
	autoPauseMemory := false
	body, err := json.Marshal(api.PostSandboxesJSONRequestBody{
		TemplateID:      id.WithNamespace(teamSlug, alias),
		AutoPause:       &autoPause,
		AutoPauseMemory: &autoPauseMemory,
	})
	require.NoError(t, err)

	ginCtx.Request = httptest.NewRequestWithContext(ctx, http.MethodPost, "/sandboxes", bytes.NewReader(body))
	ginCtx.Request.Header.Set("Content-Type", "application/json")
	auth.SetTeamInfoForTest(t, ginCtx, &authtypes.Team{
		Team: &authqueries.Team{
			ID:   teamID,
			Slug: teamSlug,
		},
		Limits: &authtypes.TeamLimits{MaxLengthHours: 24},
	})

	store.PostSandboxes(ginCtx, api.PostSandboxesParams{})

	require.Equal(t, http.StatusBadRequest, recorder.Code)

	var apiErr api.Error
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &apiErr))
	assert.Contains(t, apiErr.Message, "autoPauseMemory=false requires a Firecracker release with filesystem-only snapshot support")
}
