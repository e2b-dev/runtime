package clickhouse_test

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/launchdarkly/go-sdk-common/v3/ldvalue"
	"github.com/launchdarkly/go-server-sdk/v7/testhelpers/ldtestdata"
	"github.com/stretchr/testify/require"

	chconfig "github.com/e2b-dev/infra/packages/clickhouse/pkg"
	chevents "github.com/e2b-dev/infra/packages/clickhouse/pkg/events"
	"github.com/e2b-dev/infra/packages/clickhouse/pkg/hoststats"
	sharedevents "github.com/e2b-dev/infra/packages/shared/pkg/events"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
)

func TestInsertSettingsTargeting(t *testing.T) {
	t.Parallel()
	ff, source := insertTestFlags(t)
	source.Update(source.Flag(featureflags.ClickhouseAsyncInsertFlag.Key()).IfMatchContext(featureflags.BatcherKind, "key", ldvalue.String("sandbox-events")).ThenReturn(true).FallthroughVariation(false))
	source.Update(source.Flag(featureflags.ClickhouseWaitForAsyncInsertFlag.Key()).IfMatchContext(featureflags.BatcherKind, "key", ldvalue.String("sandbox-host-stats")).ThenReturn(false).FallthroughVariation(true))
	require.Equal(t, clickhouse.Settings{"async_insert": 1, "wait_for_async_insert": 1}, chconfig.InsertSettings(t.Context(), ff, "sandbox-events", nil))
	fallback := clickhouse.Settings{"async_insert": 1}
	require.Equal(t, clickhouse.Settings{"async_insert": 0, "wait_for_async_insert": 0}, chconfig.InsertSettings(t.Context(), ff, "sandbox-host-stats", fallback))
	require.Equal(t, clickhouse.Settings{"async_insert": 1}, fallback)
	source.Update(source.Flag(featureflags.ClickhouseAsyncInsertFlag.Key()).VariationForAll(false))
	source.Update(source.Flag(featureflags.ClickhouseWaitForAsyncInsertFlag.Key()).VariationForAll(false))
	require.Equal(t, clickhouse.Settings{"async_insert": 0, "wait_for_async_insert": 0}, chconfig.InsertSettings(t.Context(), ff, "sandbox-events", nil))
}

func TestInsertSettingsFallbacks(t *testing.T) {
	t.Parallel()
	ff, source := insertTestFlags(t)
	for _, invalid := range []bool{false, true} {
		if invalid {
			source.Update(source.Flag(featureflags.ClickhouseAsyncInsertFlag.Key()).ValueForAll(ldvalue.String("invalid boolean")))
		}
		require.Equal(t, clickhouse.Settings{"wait_for_async_insert": 1}, chconfig.InsertSettings(t.Context(), ff, "sandbox-events", nil))
		for _, writer := range []string{"sandbox-host-stats", "webhook-deliveries"} {
			require.Equal(t, clickhouse.Settings{"async_insert": 1, "wait_for_async_insert": 1}, chconfig.InsertSettings(t.Context(), ff, writer, clickhouse.Settings{"async_insert": 1}))
		}
	}
	source.Update(source.Flag(featureflags.ClickhouseWaitForAsyncInsertFlag.Key()).VariationForAll(false))
	require.Equal(t, clickhouse.Settings{"wait_for_async_insert": 0}, chconfig.InsertSettings(t.Context(), ff, "sandbox-events", nil))
	require.Nil(t, chconfig.InsertSettings(t.Context(), nil, "sandbox-events", nil))
}

func insertTestFlags(t *testing.T) (*featureflags.Client, *ldtestdata.TestDataSource) {
	t.Helper()
	source := ldtestdata.DataSource()
	ff, err := featureflags.NewClientWithDatasource(source)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, ff.Close(context.WithoutCancel(t.Context()))) })

	return ff, source
}

type insertCaptureConn struct {
	driver.Conn

	contexts chan context.Context
}

func (c *insertCaptureConn) PrepareBatch(ctx context.Context, _ string, _ ...driver.PrepareBatchOption) (driver.Batch, error) {
	c.contexts <- ctx

	return nil, errors.New("intentional prepare failure")
}

// Run against an isolated ClickHouse server. Each subtest creates its own table.
func TestInsertWritersClickHouse(t *testing.T) {
	t.Parallel()
	dsn := os.Getenv("CLICKHOUSE_INSERT_TEST_DSN")
	if dsn == "" {
		t.Skip("set CLICKHOUSE_INSERT_TEST_DSN to an isolated ClickHouse server")
	}
	for _, writer := range []string{"sandbox-events", "sandbox-host-stats"} {
		t.Run(writer, func(t *testing.T) {
			t.Parallel()
			ff, source := insertTestFlags(t)
			source.Update(source.Flag(featureflags.ClickhouseBatcherMaxBatchSize.Key()).ValueForAll(ldvalue.Int(1)))
			captured := &insertCaptureConn{contexts: make(chan context.Context, 2)}
			var push func() error
			var closeWriter func() error
			if writer == "sandbox-events" {
				d, err := chevents.NewDefaultClickhouseSandboxEventsDelivery(t.Context(), captured, ff, writer)
				require.NoError(t, err)
				push = func() error { return d.Publish(t.Context(), "", sharedevents.SandboxEvent{}) }
				closeWriter = func() error { return d.Close(t.Context()) }
			} else {
				d, err := hoststats.NewDefaultClickhouseHostStatsDelivery(t.Context(), captured, ff, writer)
				require.NoError(t, err)
				push = func() error { return d.Push(hoststats.SandboxHostStat{}) }
				closeWriter = func() error { return d.Close(t.Context()) }
			}
			t.Cleanup(func() { require.NoError(t, closeWriter()) })
			opts, err := clickhouse.ParseDSN(dsn)
			require.NoError(t, err)
			// Contradictory DSN defaults ensure the writer's per-query values win.
			opts.Settings = clickhouse.Settings{"async_insert": 0, "wait_for_async_insert": 0}
			conn, err := clickhouse.Open(opts)
			require.NoError(t, err)
			defer conn.Close()
			table := "insert_probe_" + writer[len("sandbox-"):]
			if writer == "sandbox-host-stats" {
				table = "insert_probe_host_stats"
			}
			require.NoError(t, conn.Exec(t.Context(), "CREATE TABLE "+table+" (v UInt64) ENGINE=MergeTree ORDER BY tuple()"))
			defer conn.Exec(context.WithoutCancel(t.Context()), "DROP TABLE "+table)
			for _, enabled := range []bool{true, false} {
				source.Update(source.Flag(featureflags.ClickhouseAsyncInsertFlag.Key()).VariationForAll(enabled))
				require.NoError(t, push())
				var ctx context.Context
				select {
				case ctx = <-captured.contexts:
				case <-time.After(5 * time.Second):
					t.Fatal("writer did not flush")
				}
				var async, wait bool
				require.NoError(t, conn.QueryRow(ctx, "SELECT getSetting('async_insert'), getSetting('wait_for_async_insert')").Scan(&async, &wait))
				require.Equal(t, enabled, async)
				require.True(t, wait)
				batch, err := conn.PrepareBatch(ctx, "INSERT INTO "+table, driver.WithReleaseConnection())
				require.NoError(t, err)
				defer batch.Close()
				require.NoError(t, batch.Append(uint64(7)))
				require.NoError(t, batch.Send())
			}
			var rows uint64
			require.NoError(t, conn.QueryRow(t.Context(), "SELECT count() FROM "+table).Scan(&rows))
			require.Equal(t, uint64(2), rows, "acknowledged inserts must be immediately visible")
		})
	}
}
