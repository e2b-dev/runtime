package orchestrator

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
	"github.com/e2b-dev/infra/packages/db/pkg/types"
)

// newSnapshotBuildFixture returns an orchestrator on a test database and a build
// in the non-terminal status a snapshot starts out in.
func newSnapshotBuildFixture(t *testing.T) (*Orchestrator, *testutils.Database, uuid.UUID) {
	t.Helper()

	db := testutils.SetupDatabase(t)
	teamID := testutils.CreateTestTeam(t, db)
	templateID := testutils.CreateTestTemplate(t, db, teamID)
	buildID := testutils.CreateTestBuild(t, t.Context(), db, templateID, string(types.BuildStatusSnapshotting))

	return &Orchestrator{sqlcDB: db.SqlcClient}, db, buildID
}

// cancelledContext stands in for a request whose client disconnected.
func cancelledContext(t *testing.T) context.Context {
	t.Helper()

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	return ctx
}

func buildFinishedAt(t *testing.T, db *testutils.Database, buildID uuid.UUID) *time.Time {
	t.Helper()

	var finishedAt *time.Time

	err := db.SqlcClient.TestsRawSQLQuery(t.Context(),
		"SELECT finished_at FROM public.env_builds WHERE id = $1",
		func(rows pgx.Rows) error {
			if !rows.Next() {
				return errors.New("build not found")
			}

			return rows.Scan(&finishedAt)
		},
		buildID,
	)
	require.NoError(t, err)

	return finishedAt
}

// A build left at 'snapshotting' hides its template from
// ListTeamSnapshotTemplates for good, so a cancelled request must not abandon
// the terminal write.
func TestFinishSnapshotBuild_OutlivesCancelledRequest(t *testing.T) {
	t.Parallel()

	o, db, buildID := newSnapshotBuildFixture(t)

	require.NoError(t, o.finishSnapshotBuild(cancelledContext(t), buildID))

	assert.Equal(t, types.BuildStatusGroupReady, testutils.GetBuildStatusGroup(t, t.Context(), db, buildID))
	assert.NotNil(t, buildFinishedAt(t, db, buildID), "a terminal build must record finished_at")
}

// Pause and checkpoint record 'success' where snapshot templates record
// 'uploaded'.
func TestFinishSnapshotBuild_WritesSuccessStatus(t *testing.T) {
	t.Parallel()

	o, db, buildID := newSnapshotBuildFixture(t)

	require.NoError(t, o.finishSnapshotBuild(cancelledContext(t), buildID))

	assert.Equal(t, types.BuildStatusGroupReady, testutils.GetBuildStatusGroup(t, t.Context(), db, buildID))
	assert.NotNil(t, buildFinishedAt(t, db, buildID), "a terminal build must record finished_at")
}

// Detaching drops the request deadline, and the pool sets no statement timeout,
// so the write needs a bound of its own.
func TestFinishSnapshotBuild_GivesUpWhenBlocked(t *testing.T) {
	t.Parallel()

	o, db, buildID := newSnapshotBuildFixture(t)

	// Hold a row lock so the status write cannot proceed.
	_, tx, err := db.SqlcClient.WithTx(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = tx.Rollback(context.WithoutCancel(t.Context()))
	})

	_, err = tx.Exec(t.Context(), "SELECT id FROM public.env_builds WHERE id = $1 FOR UPDATE", buildID)
	require.NoError(t, err)

	start := time.Now()
	err = o.finishSnapshotBuild(t.Context(), buildID)
	elapsed := time.Since(start)

	require.ErrorIs(t, err, context.DeadlineExceeded, "a blocked status write must give up on its own budget")
	assert.Less(t, elapsed, 2*buildStatusWriteTimeout, "the write must be bounded by its own budget")
}

// Failing a build is subject to the same rule.
func TestFailSnapshotBuild_OutlivesCancelledRequest(t *testing.T) {
	t.Parallel()

	o, db, buildID := newSnapshotBuildFixture(t)

	o.failSnapshotBuild(cancelledContext(t), buildID, errors.New("checkpoint exploded"))

	assert.Equal(t, types.BuildStatusGroupFailed, testutils.GetBuildStatusGroup(t, t.Context(), db, buildID))
	assert.NotNil(t, buildFinishedAt(t, db, buildID), "a terminal build must record finished_at")
}
