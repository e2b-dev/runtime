package handlers

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/clusters"
	"github.com/e2b-dev/infra/packages/auth/pkg/auth"
	dbtypes "github.com/e2b-dev/infra/packages/db/pkg/types"
	"github.com/e2b-dev/infra/packages/db/queries"
	"github.com/e2b-dev/infra/packages/shared/pkg/consts"
)

// Builds predating the version column carry no version at all, which used to
// select the string log format the retired V1 CLI read.
func TestTemplateBuildStatusLogsStayEmptyWithoutABuildVersion(t *testing.T) {
	t.Parallel()
	store, db := templateClusterStore(t)
	team := templateClusterTeam(t, db, nil)
	created := requestClusterTemplate(t, store, team)
	require.Equal(t, http.StatusAccepted, created.StatusCode(), string(created.Body))
	require.NotNil(t, created.JSON202)
	buildID := uuid.MustParse(created.JSON202.BuildID)
	require.NoError(t, db.SqlcClient.TestsRawSQL(t.Context(), `UPDATE public.env_builds SET version = NULL WHERE id = $1`, buildID))
	require.NoError(t, db.SqlcClient.FinishTemplateBuild(t.Context(), queries.FinishTemplateBuildParams{
		BuildID: buildID, Status: dbtypes.BuildStatusUploaded,
	}))
	store.clusters = clusters.NewTestPool(clusters.NewCluster(consts.LocalClusterID, nil, "", nil, nil, &originalClusterLogs{}))

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/templates/builds", nil)
	auth.SetTeamInfoForTest(t, c, team)
	store.GetTemplatesTemplateIDBuildsBuildIDStatus(c, created.JSON202.TemplateID, created.JSON202.BuildID, api.GetTemplatesTemplateIDBuildsBuildIDStatusParams{})

	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	status, err := api.ParseGetTemplatesTemplateIDBuildsBuildIDStatusResponse(w.Result())
	require.NoError(t, err)
	require.NotNil(t, status.JSON200)
	require.Empty(t, status.JSON200.Logs)
	require.NotEmpty(t, status.JSON200.LogEntries)
}
