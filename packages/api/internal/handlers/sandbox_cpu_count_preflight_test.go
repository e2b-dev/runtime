package handlers

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/launchdarkly/go-sdk-common/v3/ldvalue"
	"github.com/launchdarkly/go-server-sdk/v7/testhelpers/ldtestdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	snapshotcache "github.com/e2b-dev/infra/packages/api/internal/cache/snapshots"
	"github.com/e2b-dev/infra/packages/auth/pkg/auth"
	authtypes "github.com/e2b-dev/infra/packages/auth/pkg/types"
	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
	dbtypes "github.com/e2b-dev/infra/packages/db/pkg/types"
	"github.com/e2b-dev/infra/packages/db/queries"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/id"
	redis_utils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
)

// The store below carries no orchestrator: a request that reaches the start
// path dereferences nil, so the test proves its 400 is sent before a start
// is reserved.

func resizableTemplatesFlags(t *testing.T, enabled bool) *featureflags.Client {
	t.Helper()

	source := ldtestdata.DataSource()
	source.Update(source.Flag(featureflags.ResizableTemplatesFlag.Key()).ValueForAll(ldvalue.Bool(enabled)))
	flags, err := featureflags.NewClientWithDatasource(source)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, flags.Close(t.Context())) })

	return flags
}

func requireCPUCountRefused(t *testing.T, recorder *httptest.ResponseRecorder, wantCode string) {
	t.Helper()

	require.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
	var apiErr api.Error
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &apiErr))
	require.NotNil(t, apiErr.ErrorCode)
	assert.Equal(t, wantCode, *apiErr.ErrorCode)
}

func cpuCountRequest(t *testing.T, teamID uuid.UUID, path string, body any) (*httptest.ResponseRecorder, *gin.Context) {
	t.Helper()

	raw, err := json.Marshal(body)
	require.NoError(t, err)

	recorder := httptest.NewRecorder()
	ginCtx, _ := gin.CreateTestContext(recorder)
	ginCtx.Request = httptest.NewRequestWithContext(t.Context(), http.MethodPost, path, bytes.NewReader(raw))
	ginCtx.Request.Header.Set("Content-Type", "application/json")
	auth.SetTeamInfoForTest(t, ginCtx, &authtypes.Team{
		Team:   &dbtypes.Team{ID: teamID, Slug: "test-team"},
		Limits: &authtypes.TeamLimits{MaxLengthHours: 24, MaxVcpu: 8},
	})

	return recorder, ginCtx
}

// A resume naming a cpuCount the snapshot cannot run is refused before the
// start is reserved, so a connect or auto-resume arriving meanwhile never
// inherits the refusal.
func TestResume_CPUCountRefusedBeforeTheStartIsReserved(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name     string
		flag     bool
		wantCode string
	}{
		{name: "fixed-size snapshot", flag: true, wantCode: "sandbox_cpu_count_not_resizable"},
		{name: "feature off", flag: false, wantCode: "sandbox_cpu_count_disabled"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db := testutils.SetupDatabase(t)
			teamID := testutils.CreateTestTeam(t, db)
			sandboxID := "i" + id.Generate()
			totalDiskSize := int64(1024)
			envdVersion := "v1.0.0"
			_, err := db.SqlcClient.UpsertSnapshot(t.Context(), queries.UpsertSnapshotParams{
				TemplateID:         id.Generate(),
				TeamID:             teamID,
				SandboxID:          sandboxID,
				BaseTemplateID:     testutils.CreateTestTemplate(t, db, teamID),
				StartedAt:          pgtype.Timestamptz{Time: time.Now(), Valid: true},
				Vcpu:               2,
				RamMb:              2048,
				TotalDiskSizeMb:    &totalDiskSize,
				Metadata:           dbtypes.JSONBStringMap{},
				KernelVersion:      "6.1.0",
				FirecrackerVersion: "1.4.0",
				EnvdVersion:        &envdVersion,
				OriginNodeID:       "test-node",
				StatusGroup:        dbtypes.BuildStatusGroupReady,
				Config:             &dbtypes.PausedSandboxConfig{},
			})
			require.NoError(t, err)

			store := &APIStore{
				snapshotCache:         snapshotcache.NewSnapshotCache(db.SqlcClient, redis_utils.SetupInstance(t)),
				featureFlags:          resizableTemplatesFlags(t, tt.flag),
				resumeBackendOverride: notFoundResumeBackend{},
			}
			recorder, ginCtx := cpuCountRequest(t, teamID, "/sandboxes/"+sandboxID+"/resume",
				api.PostSandboxesSandboxIDResumeJSONRequestBody{CpuCount: new(api.CPUCount(4))})

			store.PostSandboxesSandboxIDResume(ginCtx, sandboxID)

			requireCPUCountRefused(t, recorder, tt.wantCode)
		})
	}
}
