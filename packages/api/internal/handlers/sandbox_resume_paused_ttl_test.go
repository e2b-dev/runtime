package handlers

import (
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	snapshotcache "github.com/e2b-dev/infra/packages/api/internal/cache/snapshots"
	"github.com/e2b-dev/infra/packages/api/internal/db/pkg/testutils"
	dbtypes "github.com/e2b-dev/infra/packages/api/internal/db/pkg/types"
	"github.com/e2b-dev/infra/packages/api/internal/db/queries"
	"github.com/e2b-dev/infra/packages/shared/pkg/id"
	redis_utils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
)

func TestResumeCarriesThePausedTTL(t *testing.T) {
	t.Parallel()

	hour := int64(3600)
	for name, tc := range map[string]struct {
		stored     *int64
		wantTTL    *time.Duration
		wantStored bool
	}{
		"with a TTL":    {stored: &hour, wantTTL: new(time.Hour), wantStored: true},
		"without a TTL": {},
	} {
		t.Run(name, func(t *testing.T) {
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
				Config:             &dbtypes.PausedSandboxConfig{PausedTTL: tc.stored},
			})
			require.NoError(t, err)

			store := &APIStore{snapshotCache: snapshotcache.NewSnapshotCache(db.SqlcClient, redis_utils.SetupInstance(t))}

			data, apiErr := store.buildResumeSandboxData(sandboxID, nil, nil)(t.Context())
			require.Nil(t, apiErr)

			assert.Equal(t, tc.wantTTL, data.PausedTTL)
			assert.Equal(t, tc.wantStored, data.PausedTTLStored)
		})
	}
}
