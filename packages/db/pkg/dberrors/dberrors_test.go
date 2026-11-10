package dberrors

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
	"github.com/e2b-dev/infra/packages/db/queries"
)

func TestIsUniqueConstraintViolation(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)

	teamID := testutils.CreateTestTeam(t, db)

	_, err := db.SqlcClient.CreateVolume(t.Context(), queries.CreateVolumeParams{
		TeamID:     teamID,
		VolumeType: "testing",
		Name:       "test",
	})
	require.NoError(t, err)

	_, err = db.SqlcClient.CreateVolume(t.Context(), queries.CreateVolumeParams{
		TeamID:     teamID,
		VolumeType: "different-volume",
		Name:       "test",
	})
	require.True(t, IsUniqueConstraintViolation(err))
}

func TestIsUndefinedTableRecognisesAQueryAgainstAMissingTable(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)

	err := db.SqlcClient.TestsRawSQL(t.Context(), `SELECT 1 FROM public.no_such_table`)
	require.True(t, IsUndefinedTable(err))
}
