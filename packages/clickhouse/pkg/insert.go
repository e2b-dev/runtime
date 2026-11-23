package clickhouse

import (
	"context"
	"errors"
	"maps"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"

	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
)

// InsertTimeout bounds one insert attempt, so a stalled server cannot hold a
// batcher's only flush goroutine.
const InsertTimeout = 15 * time.Second

const (
	codeTooManySimultaneousQueries = 202
	codeTooManyParts               = 252
)

// prepareError marks a PrepareBatch failure: no row of the batch was sent.
type prepareError struct {
	err error
}

func (e *prepareError) Error() string { return e.err.Error() }

func (e *prepareError) Unwrap() error { return e.err }

// PrepareBatch starts an insert and keeps its connection until Send, so the
// server admits the statement here, before any row is sent. Releasing the
// connection would make Send start a second statement.
func PrepareBatch(ctx context.Context, conn driver.Conn, query string) (driver.Batch, error) {
	batch, err := conn.PrepareBatch(ctx, query)
	if err != nil {
		return nil, &prepareError{err: err}
	}

	return batch, nil
}

// IsRetryableInsertError reports whether a batch can be inserted again without
// duplicating rows: the server refused the statement in PrepareBatch. A Send
// failure never qualifies, because an insert into a Distributed table can
// commit on one shard and fail on another.
func IsRetryableInsertError(err error) bool {
	prepare, ok := errors.AsType[*prepareError](err)
	if !ok {
		return false
	}
	exception, ok := errors.AsType[*proto.Exception](prepare.err)
	if !ok {
		return false
	}

	switch exception.Code {
	case codeTooManySimultaneousQueries, codeTooManyParts:
		return true
	default:
		return false
	}
}

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
