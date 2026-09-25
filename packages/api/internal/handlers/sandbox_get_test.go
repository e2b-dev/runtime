package handlers

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	apispec "github.com/e2b-dev/infra/packages/api/internal/api"
	snapshotcache "github.com/e2b-dev/infra/packages/api/internal/cache/snapshots"
	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
	"github.com/e2b-dev/infra/packages/auth/pkg/auth"
	"github.com/e2b-dev/infra/packages/auth/pkg/types"
	authqueries "github.com/e2b-dev/infra/packages/db/pkg/auth/queries"
	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
	dbtypes "github.com/e2b-dev/infra/packages/db/pkg/types"
	"github.com/e2b-dev/infra/packages/db/queries"
	"github.com/e2b-dev/infra/packages/shared/pkg/id"
	redis_utils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
)

func TestGetSandboxesSandboxID_PausedWithVolumeMountsAndNilAlias(t *testing.T) {
	t.Parallel()

	testDB := testutils.SetupDatabase(t)
	redis := redis_utils.SetupInstance(t)
	ctx := t.Context()

	teamID := testutils.CreateTestTeam(t, testDB)
	teamSlug := testutils.GetTeamSlug(t, ctx, testDB, teamID)
	baseTemplateID := testutils.CreateTestTemplate(t, testDB, teamID)
	snapshotTemplateID := id.Generate()
	sandboxID := id.Generate()

	volID := uuid.New()
	totalDiskSize := int64(1024)
	envdVersion := "v1.0.0"
	allowInternet := true

	config := &dbtypes.PausedSandboxConfig{
		VolumeMounts: []*dbtypes.SandboxVolumeMountConfig{
			{
				ID:   volID.String(),
				Name: "my-volume",
				Path: "/mnt/data",
				Type: "nfs",
			},
		},
	}

	_, err := testDB.SqlcClient.UpsertSnapshot(ctx, queries.UpsertSnapshotParams{
		TemplateID:          snapshotTemplateID,
		TeamID:              teamID,
		SandboxID:           sandboxID,
		BaseTemplateID:      baseTemplateID,
		StartedAt:           pgtype.Timestamptz{Time: time.Now(), Valid: true},
		Vcpu:                2,
		RamMb:               2048,
		TotalDiskSizeMb:     &totalDiskSize,
		Metadata:            dbtypes.JSONBStringMap{},
		KernelVersion:       "6.1.0",
		FirecrackerVersion:  "1.4.0",
		EnvdVersion:         &envdVersion,
		Secure:              true,
		AllowInternetAccess: &allowInternet,
		AutoPause:           true,
		OriginNodeID:        "test-node",
		Status:              dbtypes.BuildStatusSuccess,
		Config:              config,
	})
	require.NoError(t, err)

	tokenGen, err := sandbox.NewAccessTokenGenerator("test-secret-at-least-32-chars-long!")
	require.NoError(t, err)

	store := &APIStore{
		sqlcDB:               testDB.SqlcClient,
		authDB:               testDB.AuthDb,
		snapshotCache:        snapshotcache.NewSnapshotCache(testDB.SqlcClient, redis),
		accessTokenGenerator: tokenGen,
	}

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequestWithContext(ctx, http.MethodGet, "/sandboxes/"+sandboxID, nil)
	auth.SetTeamInfoForTest(t, c, &types.Team{
		Team: &authqueries.Team{
			ID:   teamID,
			Slug: teamSlug,
		},
	})

	store.GetSandboxesSandboxID(c, sandboxID)

	require.Equal(t, http.StatusOK, w.Code)

	var res apispec.SandboxDetail
	err = json.Unmarshal(w.Body.Bytes(), &res)
	require.NoError(t, err)

	assert.Equal(t, apispec.Paused, res.State)
	assert.Nil(t, res.Alias, "Alias should be nil when there are no aliases")
	require.NotNil(t, res.VolumeMounts, "VolumeMounts should be populated for paused sandbox")
	require.Len(t, *res.VolumeMounts, 1)
	assert.Equal(t, "my-volume", (*res.VolumeMounts)[0].Name)
	assert.Equal(t, "/mnt/data", (*res.VolumeMounts)[0].Path)
}
