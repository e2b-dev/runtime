package volumes

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/db/pkg/testutils"
	"github.com/e2b-dev/infra/packages/api/internal/db/queries"
	"github.com/e2b-dev/infra/packages/shared/pkg/postgres/dberrors"
)

func TestQueries_Volumes(t *testing.T) {
	t.Parallel()

	t.Run("happy path", func(t *testing.T) {
		t.Parallel()

		ctx := t.Context()

		// init database
		result := testutils.SetupDatabase(t)
		db := result.SqlcClient

		teamID := testutils.CreateTestTeam(t, result)

		// create volume = success
		volume, err := db.CreateVolume(ctx, queries.CreateVolumeParams{
			ID:         uuid.New(),
			TeamID:     teamID,
			VolumeType: "volume-type",
			Name:       "volume-name",
		})
		require.NoError(t, err)
		assert.Equal(t, "volume-name", volume.Name)
		assert.NotEmpty(t, volume.ID)
		assert.Equal(t, teamID, volume.TeamID)
		assert.Equal(t, "volume-type", volume.VolumeType)

		// create dupe volume = error
		_, err = db.CreateVolume(ctx, queries.CreateVolumeParams{
			TeamID:     teamID,
			VolumeType: "volume-type",
			Name:       "volume-name",
		})
		require.Error(t, err)
		assert.True(t, dberrors.IsUniqueConstraintViolation(err))

		// get volume = success
		gotVolume, err := db.GetVolume(ctx, queries.GetVolumeParams{
			VolumeID: volume.ID,
			TeamID:   teamID,
		})
		require.NoError(t, err)
		assert.Equal(t, volume, gotVolume)

		// list volume = success
		volumes, err := db.FindVolumesByTeamID(ctx, teamID)
		require.NoError(t, err)
		assert.Len(t, volumes, 1)
		assert.Contains(t, volumes, volume)

		// list volumes by name
		volumesByName, err := db.GetVolumesByName(ctx, queries.GetVolumesByNameParams{
			TeamID:      teamID,
			VolumeNames: []string{"volume-name"},
		})
		require.NoError(t, err)
		assert.Len(t, volumesByName, 1)
		assert.Contains(t, volumesByName, volume)
	})
}
