package management

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
)

func TestApplyProjectLimitsMeasuresHowLongTheDecisionTookToApply(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	service, reader := newMeasuredService(db)
	teamID := testutils.CreateTestTeam(t, db)
	decidedAt := time.Now().Add(-90 * time.Second).UTC().Truncate(time.Microsecond)

	projection := projectLimits(teamID, 1)
	projection.DecidedAt = decidedAt
	require.NoError(t, service.ApplyProjectLimits(t.Context(), projection))

	seen := collectApplyLag(t, reader)
	require.EqualValues(t, 1, seen.lag[projectionProjectLimits].Count)
	require.InDelta(t, 105, seen.lag[projectionProjectLimits].Sum, 15, "one point, 90s old when sent")
	require.Empty(t, seen.unknown)
	require.Equal(t, decidedAt, *ledgerDecidedAt(t, db, teamID))

	duplicate := projectLimits(teamID, 1)
	duplicate.DecidedAt = time.Now()
	require.NoError(t, service.ApplyProjectLimits(t.Context(), duplicate))

	seen = collectApplyLag(t, reader)
	require.EqualValues(t, 1, seen.lag[projectionProjectLimits].Count, "a dropped duplicate applied nothing")
	require.Equal(t, decidedAt, *ledgerDecidedAt(t, db, teamID))
}

func TestAStoredRevisionWithoutDecidedAtIsCountedAsOriginUnknown(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	service, reader := newMeasuredService(db)
	teamID := testutils.CreateTestTeam(t, db)

	require.NoError(t, service.ApplyProjectLimits(t.Context(), projectLimits(teamID, 1)))

	seen := collectApplyLag(t, reader)
	require.Empty(t, seen.lag, "a missing decision time is not zero lag")
	require.Equal(t, map[projectionKind]int64{projectionProjectLimits: 1}, seen.unknown)
	require.Nil(t, ledgerDecidedAt(t, db, teamID))
}

func TestADeliveryThatFailsRecordsNoApplyLag(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	service, reader := newMeasuredService(db)
	teamID := testutils.CreateTestTeam(t, db)
	decidedAt := time.Now().Add(-time.Minute)

	unknownProject := projectLimits(uuid.New(), 1)
	unknownProject.DecidedAt = decidedAt
	require.ErrorIs(t, service.ApplyProjectLimits(t.Context(), unknownProject), ErrProjectNotFound)

	rejected := projectLimits(teamID, 1)
	rejected.DecidedAt = decidedAt
	rejected.DefaultFreeDiskSizeMB = rejected.MaxFreeDiskSizeMB + 1
	require.ErrorIs(t, service.ApplyProjectLimits(t.Context(), rejected), ErrInvalidProjectLimits)

	seen := collectApplyLag(t, reader)
	require.Empty(t, seen.lag)
	require.Empty(t, seen.unknown)
}

func TestAWriteThatRollsBackAfterTheFenceRecordsNoApplyLag(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	service, reader := newMeasuredService(db)
	teamID := testutils.CreateTestTeam(t, db)
	require.NoError(t, db.SqlcClient.TestsRawSQL(t.Context(), `
		CREATE FUNCTION public.refuse_write() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN RAISE EXCEPTION 'refused'; END $$;
		CREATE TRIGGER refuse_limits BEFORE INSERT ON public.project_limits
			FOR EACH ROW EXECUTE FUNCTION public.refuse_write();
	`))
	decidedAt := time.Now().Add(-time.Minute)

	limits := projectLimits(teamID, 1)
	limits.DecidedAt = decidedAt
	require.Error(t, service.ApplyProjectLimits(t.Context(), limits))

	seen := collectApplyLag(t, reader)
	require.Empty(t, seen.lag, "the revision was accepted, but the write did not commit")
	require.Empty(t, seen.unknown)
	require.Nil(t, ledgerRevision(t, db, teamID))
}

func TestApplyLagFloorsADecisionAheadOfThisClockAtZero(t *testing.T) {
	t.Parallel()

	reader := sdkmetric.NewManualReader()
	lag := newApplyLag(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))

	lag.stored(t.Context(), projectionProjectLimits, time.Now().Add(time.Hour))

	seen := collectApplyLag(t, reader)
	require.EqualValues(t, 1, seen.lag[projectionProjectLimits].Count)
	require.Zero(t, seen.lag[projectionProjectLimits].Sum)
}

func newMeasuredService(db *testutils.Database) (*Service, *sdkmetric.ManualReader) {
	reader := sdkmetric.NewManualReader()

	return NewService(db.AuthDB, db.SqlcClient, &recordingCache{}, sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))), reader
}

type applyLagPoints struct {
	lag     map[projectionKind]metricdata.HistogramDataPoint[float64]
	unknown map[projectionKind]int64
}

func collectApplyLag(t *testing.T, reader *sdkmetric.ManualReader) applyLagPoints {
	t.Helper()

	var collected metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &collected))
	seen := applyLagPoints{
		lag:     map[projectionKind]metricdata.HistogramDataPoint[float64]{},
		unknown: map[projectionKind]int64{},
	}
	for _, scope := range collected.ScopeMetrics {
		for _, recorded := range scope.Metrics {
			switch recorded.Name {
			case applyLagMetric:
				require.Equal(t, "s", recorded.Unit)
				histogram, ok := recorded.Data.(metricdata.Histogram[float64])
				require.True(t, ok, "%s is not a float histogram", recorded.Name)
				for _, point := range histogram.DataPoints {
					seen.lag[projectionOf(point.Attributes)] = point
				}
			case originUnknownMetric:
				sum, ok := recorded.Data.(metricdata.Sum[int64])
				require.True(t, ok, "%s is not an integer sum", recorded.Name)
				for _, point := range sum.DataPoints {
					seen.unknown[projectionOf(point.Attributes)] = point.Value
				}
			}
		}
	}

	return seen
}

func projectionOf(attributes attribute.Set) projectionKind {
	value, _ := attributes.Value(attribute.Key(projectionAttr))

	return projectionKind(value.AsString())
}

func ledgerDecidedAt(t *testing.T, db *testutils.Database, teamID uuid.UUID) *time.Time {
	t.Helper()

	var decidedAt *time.Time
	require.NoError(t, db.SqlcClient.TestsRawSQLQuery(t.Context(),
		"SELECT decided_at FROM projection.project_limits WHERE project_id = $1",
		func(rows pgx.Rows) error {
			require.True(t, rows.Next(), "no project_limits ledger row")

			return rows.Scan(&decidedAt)
		}, teamID))
	if decidedAt != nil {
		utc := decidedAt.UTC()
		decidedAt = &utc
	}

	return decidedAt
}
