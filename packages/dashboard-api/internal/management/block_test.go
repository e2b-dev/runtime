package management

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
)

func TestApplyProjectBlockRetriesFailedCacheInvalidation(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	cache := &failingBlockCache{err: errors.New("cache unavailable")}
	service := NewService(db.AuthDB, db.SqlcClient, cache)
	teamID := testutils.CreateTestTeam(t, db)
	projection := ProjectBlockProjection{ProjectID: teamID, Revision: 1, Blocked: true, Reason: "credit_exhausted"}

	require.ErrorIs(t, service.ApplyProjectBlock(t.Context(), projection), cache.err)
	blocked, _ := blockState(t, db, teamID)
	require.True(t, blocked)
	require.EqualValues(t, 1, *blockLedgerRevision(t, db, teamID))

	cache.err = nil
	require.NoError(t, service.ApplyProjectBlock(t.Context(), projection))
	require.Equal(t, 2, cache.calls)
	require.EqualValues(t, 1, *blockLedgerRevision(t, db, teamID))
}

type failingBlockCache struct {
	noopAuthService

	err   error
	calls int
}

func (c *failingBlockCache) InvalidateTeamCache(context.Context, uuid.UUID) error {
	c.calls++

	return c.err
}

func blockState(t *testing.T, db *testutils.Database, teamID uuid.UUID) (bool, *string) {
	t.Helper()

	var blocked bool
	var reason *string
	require.NoError(t, db.SqlcClient.TestsRawSQLQuery(t.Context(),
		"SELECT is_blocked, blocked_reason FROM public.teams WHERE id = $1",
		func(rows pgx.Rows) error {
			rows.Next()

			return rows.Scan(&blocked, &reason)
		}, teamID))

	return blocked, reason
}

func blockLedgerRevision(t *testing.T, db *testutils.Database, teamID uuid.UUID) *int64 {
	t.Helper()

	var revision *int64
	require.NoError(t, db.SqlcClient.TestsRawSQLQuery(t.Context(),
		"SELECT revision FROM projection.project_blocks WHERE project_id = $1",
		func(rows pgx.Rows) error {
			if !rows.Next() {
				return nil
			}

			return rows.Scan(&revision)
		}, teamID))

	return revision
}

func TestApplyProjectBlockWritesTheStateTheAuthPathReads(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	service, cache := newService(db)
	teamID := testutils.CreateTestTeam(t, db)

	require.NoError(t, service.ApplyProjectBlock(t.Context(), ProjectBlockProjection{
		ProjectID: teamID, Revision: 1, Blocked: true, Reason: "credit_exhausted",
	}))

	blocked, reason := blockState(t, db, teamID)
	require.True(t, blocked)
	require.Equal(t, "credit_exhausted", *reason)
	require.Equal(t, []uuid.UUID{teamID}, cache.teams)
}

func TestApplyProjectBlockClearsTheReasonOnUnblock(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	service, _ := newService(db)
	teamID := testutils.CreateTestTeam(t, db)

	require.NoError(t, service.ApplyProjectBlock(t.Context(), ProjectBlockProjection{
		ProjectID: teamID, Revision: 1, Blocked: true, Reason: "credit_exhausted",
	}))
	require.NoError(t, service.ApplyProjectBlock(t.Context(), ProjectBlockProjection{
		ProjectID: teamID, Revision: 2, Blocked: false, Reason: "credit_exhausted",
	}))

	blocked, reason := blockState(t, db, teamID)
	require.False(t, blocked)
	require.Nil(t, reason)
}

func TestApplyProjectBlockHonorsTheNewestRevision(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	service, cache := newService(db)
	teamID := testutils.CreateTestTeam(t, db)

	require.NoError(t, service.ApplyProjectBlock(t.Context(), ProjectBlockProjection{
		ProjectID: teamID, Revision: 1, Blocked: true, Reason: "credit_exhausted",
	}))
	require.NoError(t, service.ApplyProjectBlock(t.Context(), ProjectBlockProjection{
		ProjectID: teamID, Revision: 2, Blocked: false,
	}))

	cache.reset()

	require.NoError(t, service.ApplyProjectBlock(t.Context(), ProjectBlockProjection{
		ProjectID: teamID, Revision: 1, Blocked: true, Reason: "credit_exhausted",
	}), "a superseded delivery is satisfied, not an error the caller should retry")

	blocked, _ := blockState(t, db, teamID)
	require.False(t, blocked)
	require.EqualValues(t, 2, *blockLedgerRevision(t, db, teamID))

	require.Equal(t, []uuid.UUID{teamID}, cache.teams)
}

func TestApplyProjectBlockReportsAnUnknownProject(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	service, cache := newService(db)

	err := service.ApplyProjectBlock(t.Context(), ProjectBlockProjection{
		ProjectID: uuid.New(), Revision: 1, Blocked: true,
	})

	require.ErrorIs(t, err, ErrProjectNotFound)
	require.Empty(t, cache.teams)
}

func TestApplyProjectBlockRefusesAnUnfenceableDelivery(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	service, cache := newService(db)
	teamID := testutils.CreateTestTeam(t, db)

	for _, projection := range []ProjectBlockProjection{
		{ProjectID: uuid.Nil, Revision: 1, Blocked: true},
		{ProjectID: teamID, Revision: 0, Blocked: true},
		{ProjectID: teamID, Revision: -1, Blocked: true},
	} {
		require.ErrorIs(t, service.ApplyProjectBlock(t.Context(), projection), ErrInvalidProjectBlock)
	}

	require.Nil(t, blockLedgerRevision(t, db, teamID))
	require.Empty(t, cache.teams)
}
