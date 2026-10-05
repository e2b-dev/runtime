package clickhouse

import (
	"context"
	"maps"

	"github.com/ClickHouse/clickhouse-go/v2"

	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
)

// InsertSettings evaluates typed flags for each batch. If the shared async flag
// cannot be evaluated, retain this writer's legacy async behavior. Server busy
// timeouts are not overridden. Async inserts wait for persistence by default.
func InsertSettings(ctx context.Context, ff *featureflags.Client, writer string, fallback clickhouse.Settings) clickhouse.Settings {
	settings := maps.Clone(fallback)
	if ff == nil {
		return settings
	}
	if settings == nil {
		settings = clickhouse.Settings{}
	}
	target := featureflags.BatcherContext(writer)
	if async, ok := ff.BoolFlagOverride(ctx, featureflags.ClickhouseAsyncInsertFlag, target); ok {
		settings["async_insert"] = boolSetting(async)
	}
	settings["wait_for_async_insert"] = boolSetting(ff.BoolFlag(ctx, featureflags.ClickhouseWaitForAsyncInsertFlag, target))

	return settings
}

func boolSetting(value bool) int {
	if value {
		return 1
	}

	return 0
}
