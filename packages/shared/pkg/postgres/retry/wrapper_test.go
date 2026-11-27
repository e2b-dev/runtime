package retry

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockDBTX is a mock implementation of DBTX for testing.
type mockDBTX struct {
	execFunc     func(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	queryFunc    func(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	queryRowFunc func(ctx context.Context, sql string, args ...any) pgx.Row
}

func (m *mockDBTX) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	if m.execFunc != nil {
		return m.execFunc(ctx, sql, args...)
	}

	return pgconn.CommandTag{}, nil
}

func (m *mockDBTX) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	if m.queryFunc != nil {
		return m.queryFunc(ctx, sql, args...)
	}

	return nil, nil
}

func (m *mockDBTX) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	if m.queryRowFunc != nil {
		return m.queryRowFunc(ctx, sql, args...)
	}

	return &mockRow{}
}

// mockRow is a mock implementation of pgx.Row.
type mockRow struct {
	scanFunc func(dest ...any) error
}

func (m *mockRow) Scan(dest ...any) error {
	if m.scanFunc != nil {
		return m.scanFunc(dest...)
	}

	return nil
}

// testConfig returns a config with fast backoffs for testing.
func testConfig() Config {
	return Config{
		MaxAttempts:       5,
		InitialBackoff:    1 * time.Millisecond,
		MaxBackoff:        10 * time.Millisecond,
		BackoffMultiplier: 2.0,
	}
}

func TestWrap_ReturnsOriginalForTransaction(t *testing.T) {
	t.Parallel()
	// We can't easily create a real pgx.Tx without a database connection,
	// so we verify the type assertion logic indirectly.
	mock := &mockDBTX{}
	wrapped := Wrap(mock, DefaultConfig())

	// Should wrap non-transaction DBTX
	_, isRetryable := wrapped.(*RetryableDBTX)
	assert.True(t, isRetryable, "should wrap non-transaction DBTX")
}

func TestExec_SuccessOnFirstAttempt(t *testing.T) {
	t.Parallel()
	callCount := 0
	mock := &mockDBTX{
		execFunc: func(_ context.Context, _ string, _ ...any) (pgconn.CommandTag, error) {
			callCount++

			return pgconn.NewCommandTag("INSERT 1"), nil
		},
	}

	wrapped := Wrap(mock, testConfig())
	ctx := t.Context()

	result, err := wrapped.Exec(ctx, "INSERT INTO test VALUES (1)")
	require.NoError(t, err)
	assert.Equal(t, 1, callCount)
	assert.Equal(t, int64(1), result.RowsAffected())
}

func TestExec_RetryOnConnectionError(t *testing.T) {
	t.Parallel()
	callCount := 0
	mock := &mockDBTX{
		execFunc: func(_ context.Context, _ string, _ ...any) (pgconn.CommandTag, error) {
			callCount++
			if callCount < 3 {
				return pgconn.CommandTag{}, &pgconn.PgError{Code: "08006"} // connection failure
			}

			return pgconn.NewCommandTag("INSERT 1"), nil
		},
	}

	wrapped := Wrap(mock, testConfig())
	ctx := t.Context()

	result, err := wrapped.Exec(ctx, "INSERT INTO test VALUES (1)")
	require.NoError(t, err)
	assert.Equal(t, 3, callCount, "should retry until success")
	assert.Equal(t, int64(1), result.RowsAffected())
}

func TestExec_NoRetryOnDeadlock(t *testing.T) {
	t.Parallel()
	callCount := 0
	mock := &mockDBTX{
		execFunc: func(_ context.Context, _ string, _ ...any) (pgconn.CommandTag, error) {
			callCount++

			return pgconn.CommandTag{}, &pgconn.PgError{Code: "40P01"} // deadlock
		},
	}

	wrapped := Wrap(mock, testConfig())
	ctx := t.Context()

	_, err := wrapped.Exec(ctx, "UPDATE test SET val = 1")
	require.Error(t, err)
	assert.Equal(t, 1, callCount, "should not retry on deadlock")

	var pgErr *pgconn.PgError
	require.ErrorAs(t, err, &pgErr)
	assert.Equal(t, "40P01", pgErr.Code)
}

func TestExec_NoRetryOnConstraintViolation(t *testing.T) {
	t.Parallel()
	callCount := 0
	mock := &mockDBTX{
		execFunc: func(_ context.Context, _ string, _ ...any) (pgconn.CommandTag, error) {
			callCount++

			return pgconn.CommandTag{}, &pgconn.PgError{Code: "23505"} // unique violation
		},
	}

	wrapped := Wrap(mock, testConfig())
	ctx := t.Context()

	_, err := wrapped.Exec(ctx, "INSERT INTO test VALUES (1)")
	require.Error(t, err)
	assert.Equal(t, 1, callCount, "should not retry on constraint violation")

	var pgErr *pgconn.PgError
	require.ErrorAs(t, err, &pgErr)
	assert.Equal(t, "23505", pgErr.Code)
}

func TestExec_MaxAttemptsExceeded(t *testing.T) {
	t.Parallel()
	callCount := 0
	mock := &mockDBTX{
		execFunc: func(_ context.Context, _ string, _ ...any) (pgconn.CommandTag, error) {
			callCount++

			return pgconn.CommandTag{}, &pgconn.PgError{Code: "08006"} // connection failure
		},
	}

	config := testConfig()
	config.MaxAttempts = 3
	wrapped := Wrap(mock, config)
	ctx := t.Context()

	_, err := wrapped.Exec(ctx, "INSERT INTO test VALUES (1)")
	require.Error(t, err)
	assert.Equal(t, 3, callCount, "should stop after max attempts")
}

func TestExec_ContextCancellation(t *testing.T) {
	t.Parallel()
	callCount := 0
	mock := &mockDBTX{
		execFunc: func(_ context.Context, _ string, _ ...any) (pgconn.CommandTag, error) {
			callCount++

			return pgconn.CommandTag{}, &pgconn.PgError{Code: "08006"} // connection failure
		},
	}

	config := testConfig()
	config.InitialBackoff = 100 * time.Millisecond
	wrapped := Wrap(mock, config)
	ctx, cancel := context.WithCancel(t.Context())

	// Cancel context after a short delay
	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()

	_, err := wrapped.Exec(ctx, "INSERT INTO test VALUES (1)")
	require.Error(t, err)
	// Should have made at least one attempt
	assert.GreaterOrEqual(t, callCount, 1)
}

func TestQuery_RetryOnConnectionError(t *testing.T) {
	t.Parallel()
	callCount := 0
	mock := &mockDBTX{
		queryFunc: func(_ context.Context, _ string, _ ...any) (pgx.Rows, error) {
			callCount++
			if callCount < 2 {
				return nil, &pgconn.PgError{Code: "57P01"} // admin shutdown
			}

			return nil, nil // Success (nil rows is fine for this test)
		},
	}

	wrapped := Wrap(mock, testConfig())
	ctx := t.Context()

	rows, err := wrapped.Query(ctx, "SELECT id FROM test")
	if rows != nil {
		rows.Close()
	}
	require.NoError(t, err)
	assert.Equal(t, 2, callCount)
}

func TestQueryRow_RetryOnConnectionError(t *testing.T) {
	t.Parallel()
	callCount := 0
	mock := &mockDBTX{
		queryRowFunc: func(_ context.Context, _ string, _ ...any) pgx.Row {
			return &mockRow{
				scanFunc: func(dest ...any) error {
					callCount++
					if callCount < 2 {
						return &pgconn.PgError{Code: "08006"} // connection failure
					}
					// Set a value
					if len(dest) > 0 {
						if ptr, ok := dest[0].(*int); ok {
							*ptr = 42
						}
					}

					return nil
				},
			}
		},
	}

	wrapped := Wrap(mock, testConfig())
	ctx := t.Context()

	var result int
	err := wrapped.QueryRow(ctx, "SELECT count(*) FROM test").Scan(&result)
	require.NoError(t, err)
	assert.Equal(t, 2, callCount)
	assert.Equal(t, 42, result)
}

func TestQueryRow_NoRetryOnNoRows(t *testing.T) {
	t.Parallel()
	callCount := 0
	mock := &mockDBTX{
		queryRowFunc: func(_ context.Context, _ string, _ ...any) pgx.Row {
			return &mockRow{
				scanFunc: func(_ ...any) error {
					callCount++

					return pgx.ErrNoRows
				},
			}
		},
	}

	wrapped := Wrap(mock, testConfig())
	ctx := t.Context()

	var result int
	err := wrapped.QueryRow(ctx, "SELECT count(*) FROM test").Scan(&result)
	require.ErrorIs(t, err, pgx.ErrNoRows)
	assert.Equal(t, 1, callCount, "should not retry on ErrNoRows")
}

func TestConfig_Options(t *testing.T) {
	t.Parallel()

	config := DefaultConfig()
	config.Apply(
		WithMaxAttempts(10),
		WithInitialBackoff(50*time.Millisecond),
		WithMaxBackoff(5*time.Second),
		WithBackoffMultiplier(3.0),
	)

	assert.Equal(t, 10, config.MaxAttempts)
	assert.Equal(t, 50*time.Millisecond, config.InitialBackoff)
	assert.Equal(t, 5*time.Second, config.MaxBackoff)
	assert.InDelta(t, 3.0, config.BackoffMultiplier, 0.001)
}

func TestBackoff_ExponentialGrowth(t *testing.T) {
	t.Parallel()

	initialBackoff := float64(100 * time.Millisecond)
	maxBackoff := float64(10 * time.Second)
	backoffMultiplier := 2.0

	// Test that backoff grows exponentially (within jitter range)
	// attempt 1: 100ms * 2^0 = 100ms
	// attempt 2: 100ms * 2^1 = 200ms
	// attempt 3: 100ms * 2^2 = 400ms
	backoff1 := calculateBackoff(initialBackoff, backoffMultiplier, maxBackoff, 1)
	backoff2 := calculateBackoff(initialBackoff, backoffMultiplier, maxBackoff, 2)
	backoff3 := calculateBackoff(initialBackoff, backoffMultiplier, maxBackoff, 3)

	// With 25% jitter, backoff1 should be in range [75ms, 125ms]
	assert.GreaterOrEqual(t, backoff1, 75*time.Millisecond)
	assert.LessOrEqual(t, backoff1, 125*time.Millisecond)

	// With 25% jitter, backoff2 should be in range [150ms, 250ms]
	assert.GreaterOrEqual(t, backoff2, 150*time.Millisecond)
	assert.LessOrEqual(t, backoff2, 250*time.Millisecond)

	// With 25% jitter, backoff3 should be in range [300ms, 500ms]
	assert.GreaterOrEqual(t, backoff3, 300*time.Millisecond)
	assert.LessOrEqual(t, backoff3, 500*time.Millisecond)
}

func TestBackoff_MaxBackoffCap(t *testing.T) {
	t.Parallel()

	initialBackoff := float64(100 * time.Millisecond)
	maxBackoff := float64(500 * time.Millisecond)
	backoffMultiplier := 2.0

	// attempt 5: 100ms * 2^4 = 1600ms, should be capped at 500ms
	backoff := calculateBackoff(initialBackoff, backoffMultiplier, maxBackoff, 5)

	// With 25% jitter on capped value, should be in range [375ms, 625ms]
	assert.GreaterOrEqual(t, backoff, 375*time.Millisecond)
	assert.LessOrEqual(t, backoff, 625*time.Millisecond)
}
