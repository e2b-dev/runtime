package outbox_test

import (
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/stretchr/testify/require"

	dbmodule "github.com/e2b-dev/infra/packages/db"
	"github.com/e2b-dev/infra/packages/db/pkg/outbox"
	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
)

func newInsertOnlyClient(t *testing.T) (*river.Client[pgx.Tx], *testutils.Database) {
	t.Helper()

	db := testutils.SetupDatabase(t)
	client, err := river.NewClient(riverpgxv5.New(db.SqlcClient.Pool()), &river.Config{Schema: dbmodule.RiverSchema})
	require.NoError(t, err)

	return client, db
}

func TestTeardownTeamResourcesIsEnqueuedOncePerTeamWhileOpen(t *testing.T) {
	t.Parallel()

	client, _ := newInsertOnlyClient(t)
	teamID := uuid.New()

	first, err := client.Insert(t.Context(), outbox.TeardownTeamResources{TeamID: teamID}, nil)
	require.NoError(t, err)
	require.False(t, first.UniqueSkippedAsDuplicate)

	repeat, err := client.Insert(t.Context(), outbox.TeardownTeamResources{TeamID: teamID}, nil)
	require.NoError(t, err)
	require.True(t, repeat.UniqueSkippedAsDuplicate)
	require.Equal(t, first.Job.ID, repeat.Job.ID)

	otherTeam, err := client.Insert(t.Context(), outbox.TeardownTeamResources{TeamID: uuid.New()}, nil)
	require.NoError(t, err)
	require.False(t, otherTeam.UniqueSkippedAsDuplicate)
}

func TestTeardownTeamResourcesCanBeEnqueuedAgainOnceTheLastOneCompleted(t *testing.T) {
	t.Parallel()

	client, db := newInsertOnlyClient(t)
	teamID := uuid.New()

	first, err := client.Insert(t.Context(), outbox.TeardownTeamResources{TeamID: teamID}, nil)
	require.NoError(t, err)
	_, err = db.SqlcClient.Pool().Exec(t.Context(),
		`UPDATE river.river_job SET state = 'completed', finalized_at = now() WHERE id = $1`, first.Job.ID)
	require.NoError(t, err)

	again, err := client.Insert(t.Context(), outbox.TeardownTeamResources{TeamID: teamID}, nil)
	require.NoError(t, err)
	require.False(t, again.UniqueSkippedAsDuplicate)
	require.NotEqual(t, first.Job.ID, again.Job.ID)
}
