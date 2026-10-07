package management

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
)

func TestApplyBillingProfileWritesAndAdvances(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	service := NewService(db.AuthDB, db.SqlcClient, &countingProfileCache{}, noop.NewMeterProvider())
	teamID := testutils.CreateTestTeam(t, db)

	require.NoError(t, service.ApplyBillingProfile(t.Context(), BillingProfileProjection{
		ProjectID: teamID, Revision: 1, HasPaymentMethod: false, Enterprise: false,
	}))
	hasPM, enterprise, plan := billingProfileState(t, db, teamID)
	require.False(t, hasPM)
	require.False(t, enterprise)
	require.Nil(t, plan)

	require.NoError(t, service.ApplyBillingProfile(t.Context(), BillingProfileProjection{
		ProjectID: teamID, Revision: 2, HasPaymentMethod: true, Enterprise: false, Plan: "pro_v1",
	}))
	hasPM, _, plan = billingProfileState(t, db, teamID)
	require.True(t, hasPM)
	require.NotNil(t, plan)
	require.Equal(t, "pro_v1", *plan)
}

func TestApplyBillingProfileDropsStaleRevision(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	service := NewService(db.AuthDB, db.SqlcClient, &countingProfileCache{}, noop.NewMeterProvider())
	teamID := testutils.CreateTestTeam(t, db)

	require.NoError(t, service.ApplyBillingProfile(t.Context(), BillingProfileProjection{
		ProjectID: teamID, Revision: 5, HasPaymentMethod: true, Enterprise: true,
	}))
	// A delayed duplicate and an older delivery both succeed without writing.
	for _, revision := range []int64{5, 4} {
		require.NoError(t, service.ApplyBillingProfile(t.Context(), BillingProfileProjection{
			ProjectID: teamID, Revision: revision, HasPaymentMethod: false, Enterprise: false,
		}))
	}
	hasPM, enterprise, _ := billingProfileState(t, db, teamID)
	require.True(t, hasPM)
	require.True(t, enterprise)
}

func TestApplyBillingProfileRejectsInvalidAndMissing(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	service := NewService(db.AuthDB, db.SqlcClient, &countingProfileCache{}, noop.NewMeterProvider())

	require.ErrorIs(t, service.ApplyBillingProfile(t.Context(),
		BillingProfileProjection{ProjectID: uuid.Nil, Revision: 1}), ErrInvalidBillingProfile)
	require.ErrorIs(t, service.ApplyBillingProfile(t.Context(),
		BillingProfileProjection{ProjectID: uuid.New(), Revision: 0}), ErrInvalidBillingProfile)
	require.ErrorIs(t, service.ApplyBillingProfile(t.Context(),
		BillingProfileProjection{ProjectID: uuid.New(), Revision: 1}), ErrProjectNotFound)
}

func TestApplyBillingProfileSurvivesFailedCacheInvalidation(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	cache := &countingProfileCache{}
	service := NewService(db.AuthDB, db.SqlcClient, cache, noop.NewMeterProvider())
	teamID := testutils.CreateTestTeam(t, db)

	// Unlike blocks, a failed eviction is logged, not returned: the row is
	// committed and the caller must not re-send a revision this side has.
	cache.err = errors.New("cache unavailable")
	require.NoError(t, service.ApplyBillingProfile(t.Context(), BillingProfileProjection{
		ProjectID: teamID, Revision: 1, HasPaymentMethod: true, Enterprise: false,
	}))
	hasPM, _, _ := billingProfileState(t, db, teamID)
	require.True(t, hasPM)
	require.Equal(t, 1, cache.calls)
}

type countingProfileCache struct {
	noopAuthService

	err   error
	calls int
}

func (c *countingProfileCache) InvalidateTeamCache(context.Context, uuid.UUID) error {
	c.calls++

	return c.err
}

func billingProfileState(t *testing.T, db *testutils.Database, teamID uuid.UUID) (bool, bool, *string) {
	t.Helper()

	var hasPM, enterprise bool
	var plan *string
	require.NoError(t, db.SqlcClient.TestsRawSQLQuery(t.Context(),
		"SELECT has_payment_method, enterprise, plan FROM public.team_billing_profiles WHERE team_id = $1",
		func(rows pgx.Rows) error {
			rows.Next()

			return rows.Scan(&hasPM, &enterprise, &plan)
		}, teamID))

	return hasPM, enterprise, plan
}
