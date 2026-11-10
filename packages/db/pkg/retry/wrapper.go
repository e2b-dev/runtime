package retry

import (
	"context"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/db/pkg/types"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

type operation string

const (
	operationExec  operation = "Exec"
	operationQuery operation = "Query"
	operationScan  operation = "Scan"

	jitterMultiplier = 0.25
)

// RetryableDBTX wraps a DBTX with retry logic.
type RetryableDBTX struct {
	db     types.DBTX
	config Config
}

// Wrap wraps a DBTX with retry logic using the provided options.
func Wrap(db types.DBTX, config Config) types.DBTX {
	// Skip wrapping if max attempts is 0
	if config.MaxAttempts <= 0 {
		return db
	}

	return &RetryableDBTX{
		db:     db,
		config: config,
	}
}

// RetryOnRecoveryConflict replays complete autocommit reads. fn must consume and
// close its rows before returning; recovery conflicts can surface from Rows.Err().
func RetryOnRecoveryConflict[T any](ctx context.Context, fn func(context.Context) (T, error)) (T, error) {
	const maxRetries = 3
	var zero T

	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return zero, err
		}

		result, err := fn(ctx)
		if err == nil {
			return result, nil
		}
		if ctx.Err() != nil {
			return zero, ctx.Err()
		}
		if !isRecoveryConflict(err) {
			return zero, err
		}
		if attempt >= maxRetries {
			return zero, fmt.Errorf("recovery conflict persisted after %d attempts: %w", attempt+1, err)
		}

		logger.L().Warn(ctx, "retrying query after recovery conflict",
			zap.Int("attempt", attempt+1), zap.Error(err))
	}
}

// Exec executes a query with retry logic.
func (r *RetryableDBTX) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	var result pgconn.CommandTag
	var lastErr error

	for attempt := 1; attempt <= r.config.MaxAttempts; attempt++ {
		result, lastErr = r.db.Exec(ctx, sql, args...)
		if lastErr == nil {
			return result, nil
		}

		if err := handleRetry(ctx, operationExec, attempt, r.config, lastErr); err != nil {
			return result, err
		}
	}

	return result, lastErr
}

// Query executes a query that returns rows with retry logic.
// The caller is responsible for closing the returned rows.
func (r *RetryableDBTX) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	var rows pgx.Rows
	var lastErr error

	for attempt := 1; attempt <= r.config.MaxAttempts; attempt++ {
		rows, lastErr = r.db.Query(ctx, sql, args...)
		if lastErr == nil {
			return rows, nil
		}

		if err := handleRetry(ctx, operationQuery, attempt, r.config, lastErr); err != nil {
			return nil, err
		}
	}

	return nil, lastErr
}

// QueryRow executes a query that returns a single row with retry logic.
// Since pgx.Row doesn't expose errors until Scan(), we wrap it in a retryableRow.
func (r *RetryableDBTX) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return &retryableRow{
		ctx:    ctx,
		sql:    sql,
		args:   args,
		db:     r.db,
		config: r.config,
	}
}

// retryableRow wraps a pgx.Row to provide retry logic.
// Since pgx.Row.Scan() is where errors are surfaced, we need to retry the entire query.
type retryableRow struct {
	ctx    context.Context //nolint:containedctx // Context must be stored for deferred Scan() retry
	sql    string
	args   []any
	db     types.DBTX
	config Config
}

// Scan implements pgx.Row.Scan with retry logic.
func (r *retryableRow) Scan(dest ...any) error {
	var lastErr error

	for attempt := 1; attempt <= r.config.MaxAttempts; attempt++ {
		row := r.db.QueryRow(r.ctx, r.sql, r.args...)
		lastErr = row.Scan(dest...)
		if lastErr == nil {
			return nil
		}

		if err := handleRetry(r.ctx, operationScan, attempt, r.config, lastErr); err != nil {
			return fmt.Errorf("failed to scan row after %d attempts: %w, original error %w", attempt, err, lastErr)
		}
	}

	return lastErr
}

func handleRetry(ctx context.Context, operation operation, attempt int, config Config, dbErr error) error {
	if !shouldRetry(ctx, attempt, config.MaxAttempts, dbErr) {
		return dbErr
	}

	logRetry(ctx, operation, attempt, config.MaxAttempts, dbErr)
	recordRetrySpan(ctx, attempt, dbErr)

	if err := backoffFunc(ctx, attempt, float64(config.InitialBackoff), config.BackoffMultiplier, float64(config.MaxBackoff)); err != nil {
		return fmt.Errorf("retry backoff interrupted: %w, original error: %w", err, dbErr)
	}

	return nil
}

// shouldRetry determines if we should retry based on error type and attempt count.
func shouldRetry(ctx context.Context, attempt, maxAttempts int, err error) bool {
	if ctx.Err() != nil {
		return false
	}
	if attempt >= maxAttempts {
		return false
	}

	return IsRetriable(err)
}

// backoffFunc waits before the next retry attempt with exponential backoff and jitter.
func backoffFunc(ctx context.Context, attempt int, initialBackoff, backoffMultiplier, maxBackoff float64) error {
	duration := calculateBackoff(initialBackoff, backoffMultiplier, maxBackoff, attempt)

	timer := time.NewTimer(duration)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// calculateBackoff computes the backoff duration for a given attempt.
func calculateBackoff(initialBackoff, backoffMultiplier, maxBackoff float64, attempt int) time.Duration {
	backoff := initialBackoff
	for i := 1; i < attempt; i++ {
		backoff *= backoffMultiplier
	}

	if backoff > maxBackoff {
		backoff = maxBackoff
	}

	// Add jitter: +/- 25%
	jitter := backoff * jitterMultiplier * (rand.Float64()*2 - 1)
	backoff += jitter

	return time.Duration(backoff)
}

// logRetry logs a retry attempt.
func logRetry(ctx context.Context, operation operation, attempt, maxAttempts int, err error) {
	logger.L().Warn(ctx, "retrying database operation",
		zap.String("operation", string(operation)),
		zap.Int("attempt", attempt),
		zap.Int("max_attempts", maxAttempts),
		zap.Error(err),
	)
}

// recordRetrySpan records retry information in the current span.
func recordRetrySpan(ctx context.Context, attempt int, err error) {
	span := trace.SpanFromContext(ctx)
	if !span.IsRecording() {
		return
	}

	span.AddEvent("db.retry", trace.WithAttributes(
		attribute.Int("attempt", attempt),
		attribute.String("error", err.Error()),
	))
}
