package main

import (
	"context"
	"net/url"
	"regexp"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/e2b-dev/infra/packages/scheduling-api/internal/db/dbtest"
	"github.com/e2b-dev/infra/packages/scheduling-api/internal/store"
)

// metricReader reads the process-wide meter provider, which every test of the
// package shares. Tests filter by the pool names that only they use.
var metricReader = sdkmetric.NewManualReader()

func TestMain(m *testing.M) {
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(metricReader)))
	m.Run()
}

func TestOpenDatabaseConnections(t *testing.T) {
	t.Parallel()
	databaseURL, err := url.Parse(dbtest.ConnString(t, dbtest.EmptyPool(t)))
	require.NoError(t, err)
	query := databaseURL.Query()
	query.Set("pool_max_conns", "30")
	query.Set("synchronous_commit", "off")
	databaseURL.RawQuery = query.Encode()
	pool, err := openDatabase(t.Context(), "write", databaseURL.String(), 2)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	require.Equal(t, int32(2), pool.Config().MaxConns)

	for range 2 {
		first, err := pool.Acquire(t.Context())
		require.NoError(t, err)
		t.Cleanup(first.Release)
		second, err := pool.Acquire(t.Context())
		require.NoError(t, err)
		t.Cleanup(second.Release)
		var synchronousCommit string
		require.NoError(t, first.QueryRow(t.Context(), "SHOW synchronous_commit").Scan(&synchronousCommit))
		require.Equal(t, "on", synchronousCommit)
		require.NoError(t, second.QueryRow(t.Context(), "SHOW synchronous_commit").Scan(&synchronousCommit))
		require.Equal(t, "on", synchronousCommit)
		first.Release()
		second.Release()
		pool.Reset()
	}
}

func TestOpenDatabaseRejectsReadOnly(t *testing.T) {
	t.Parallel()
	databaseURL, err := url.Parse(dbtest.ConnString(t, dbtest.EmptyPool(t)))
	require.NoError(t, err)
	query := databaseURL.Query()
	query.Set("default_transaction_read_only", "on")
	databaseURL.RawQuery = query.Encode()
	pool, err := openDatabase(t.Context(), "write", databaseURL.String(), 1)
	if pool != nil {
		t.Cleanup(pool.Close)
	}
	require.ErrorContains(t, err, "must be a writable primary")
	require.Nil(t, pool)
}

func TestOpenDatabasePoolLimits(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct {
		query   string
		maximum int32
	}{
		"zero":         {maximum: 0},
		"negative":     {maximum: -1},
		"minimum":      {query: "pool_min_conns=3", maximum: 2},
		"minimum idle": {query: "pool_min_idle_conns=3", maximum: 2},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			pool, err := openDatabase(t.Context(), "write", "postgres://localhost/scheduling?"+tc.query, tc.maximum)
			if pool != nil {
				t.Cleanup(pool.Close)
			}
			require.Error(t, err)
			require.Nil(t, pool)
		})
	}
}

// A write pool whose only connection is held makes callers wait or give up.
// Only the write pool reports it; the read pool of the same database does not.
func TestOpenDatabasePoolMetrics(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	databaseURL := dbtest.ConnString(t, dbtest.EmptyPool(t))
	writePool, err := openDatabase(ctx, "pool-metrics-write", databaseURL, 1)
	require.NoError(t, err)
	t.Cleanup(writePool.Close)
	readPool, err := openDatabase(ctx, "pool-metrics-read", databaseURL, 1)
	require.NoError(t, err)
	t.Cleanup(readPool.Close)

	held, err := writePool.Acquire(ctx)
	require.NoError(t, err)
	timeoutCtx, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	_, err = writePool.Acquire(timeoutCtx)
	cancel()
	require.ErrorIs(t, err, context.DeadlineExceeded)

	const hold = 200 * time.Millisecond
	waited := make(chan error, 1)
	go func() {
		conn, err := writePool.Acquire(ctx)
		if err == nil {
			conn.Release()
		}
		waited <- err
	}()
	time.Sleep(hold)
	held.Release()
	require.NoError(t, <-waited)
	var one int
	require.NoError(t, readPool.QueryRow(ctx, "SELECT 1").Scan(&one))

	// otelpgx keeps a pool's statistics for a second, and a collection by
	// another test can have read them before the acquires above.
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		var collected metricdata.ResourceMetrics
		require.NoError(c, metricReader.Collect(ctx, &collected))
		write, read := poolPoints(collected, "pool-metrics-write"), poolPoints(collected, "pool-metrics-read")

		assert.Equal(c, read["pgxpool.empty_acquire"]+1, write["pgxpool.empty_acquire"],
			"only the write pool has an acquire that waited for a busy connection")
		assert.Positive(c, write["pgxpool.empty_acquire_wait_time"])
		assert.Equal(c, int64(1), write["pgxpool.canceled_acquires"])
		assert.Zero(c, read["pgxpool.canceled_acquires"])
		assert.Positive(c, read["pgxpool.acquires"])
		for name, points := range map[string]map[string]int64{"write": write, "read": read} {
			assert.Equal(c, int64(1), points["pgxpool.max_connections"], name)
			assert.Equal(c, int64(1), points["pgxpool.total_connections"], name)
			assert.Equal(c, int64(1), points["pgxpool.idle_connections"], name)
			assert.Zero(c, points["pgxpool.acquired_connections"], name)
		}
	}, 5*time.Second, 100*time.Millisecond)
}

func poolPoints(collected metricdata.ResourceMetrics, pool string) map[string]int64 {
	points := map[string]int64{}
	matches := func(attributes attribute.Set) bool {
		value, ok := attributes.Value("pool.name")

		return ok && value.AsString() == pool
	}
	for _, scope := range collected.ScopeMetrics {
		for _, m := range scope.Metrics {
			switch data := m.Data.(type) {
			case metricdata.Sum[int64]:
				for _, point := range data.DataPoints {
					if matches(point.Attributes) {
						points[m.Name] += point.Value
					}
				}
			case metricdata.Gauge[int64]:
				for _, point := range data.DataPoints {
					if matches(point.Attributes) {
						points[m.Name] += point.Value
					}
				}
			}
		}
	}

	return points
}

// Statements are timed by pool, by a fixed name and by failure, never by
// their SQL text.
func TestOpenDatabaseStatementDuration(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	const write, read = "statement-write", "statement-read"
	databaseURL := dbtest.ConnString(t, dbtest.MigratedPool(t))
	writePool, err := openDatabase(ctx, write, databaseURL, 1)
	require.NoError(t, err)
	t.Cleanup(writePool.Close)
	readPool, err := openDatabase(ctx, read, databaseURL, 1)
	require.NoError(t, err)
	t.Cleanup(readPool.Close)
	st := store.New(writePool, readPool)
	project, actor := uuid.New(), uuid.New()

	claimed, err := st.ClaimSnapshotDeletion(ctx, project, actor, uuid.New())
	require.NoError(t, err)
	require.True(t, claimed)
	_, err = st.UpdateDeadline(ctx, project, actor, "execution", time.Now(), time.Now())
	require.ErrorIs(t, err, store.ErrConflict)
	_, err = st.Get(ctx, project, actor)
	require.NoError(t, err)

	tx, err := writePool.Begin(ctx)
	require.NoError(t, err)
	_, err = tx.Exec(ctx, "SELECT 1/0")
	require.Error(t, err)
	require.ErrorIs(t, tx.Commit(ctx), pgx.ErrTxCommitRollback)
	timeoutCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	_, err = readPool.Exec(timeoutCtx, "SELECT pg_sleep(5)")
	cancel()
	require.ErrorIs(t, err, context.DeadlineExceeded)

	var collected metricdata.ResourceMetrics
	require.NoError(t, metricReader.Collect(ctx, &collected))
	seen := map[series]bool{}
	for _, scope := range collected.ScopeMetrics {
		for _, m := range scope.Metrics {
			histogram, ok := m.Data.(metricdata.Histogram[float64])
			if !ok {
				continue
			}
			for _, point := range histogram.DataPoints {
				pool, hasPool := point.Attributes.Value("pool.name")
				statement, hasStatement := point.Attributes.Value("statement")
				if !hasPool || !hasStatement {
					continue
				}
				failed, hasFailed := point.Attributes.Value("error")
				require.True(t, hasFailed)
				require.Equal(t, 3, point.Attributes.Len())
				require.NotContains(t, statement.AsString(), " ")
				require.Positive(t, point.Count)
				if pool.AsString() != write && pool.AsString() != read {
					continue
				}
				seen[series{pool.AsString(), statement.AsString(), failed.AsBool()}] = true
			}
		}
	}

	for _, control := range []string{"begin", "commit", "rollback"} {
		require.True(t, seen[series{write, control, false}], control)
		require.False(t, seen[series{read, control, false}], control)
	}
	require.True(t, seen[series{write, "commit", true}], "a commit that PostgreSQL rolled back")
	require.True(t, seen[series{write, "other", false}], "the writable-primary check")
	require.True(t, seen[series{write, "other", true}], "a failed statement")
	require.True(t, seen[series{read, "other", true}], "a statement cancelled by its context")
	sqlcName := regexp.MustCompile(`^[A-Z][A-Za-z0-9]*$`)
	named := func(pool string) int {
		n := 0
		for s := range seen {
			if s.pool == pool && !s.failed && !slices.Contains([]string{"begin", "commit", "rollback", "other"}, s.statement) {
				require.Regexp(t, sqlcName, s.statement)
				n++
			}
		}

		return n
	}
	require.GreaterOrEqual(t, named(write), 3, "the admission lock, the reservation read, the claim and the deadline update")
	require.Positive(t, named(read), "the actor read")
}

type series struct {
	pool, statement string
	failed          bool
}

// statementSeries returns every statement series of the shared meter
// provider.
func statementSeries(t *testing.T) map[series]bool {
	t.Helper()

	var collected metricdata.ResourceMetrics
	require.NoError(t, metricReader.Collect(t.Context(), &collected))
	seen := map[series]bool{}
	for _, scope := range collected.ScopeMetrics {
		for _, m := range scope.Metrics {
			histogram, ok := m.Data.(metricdata.Histogram[float64])
			if !ok {
				continue
			}
			for _, point := range histogram.DataPoints {
				pool, hasPool := point.Attributes.Value("pool.name")
				statement, hasStatement := point.Attributes.Value("statement")
				failed, _ := point.Attributes.Value("error")
				if hasPool && hasStatement {
					seen[series{pool.AsString(), statement.AsString(), failed.AsBool()}] = true
				}
			}
		}
	}

	return seen
}
