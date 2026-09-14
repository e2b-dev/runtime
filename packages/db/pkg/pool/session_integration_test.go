package pool

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

const testPostgresImage = "postgres:18-alpine"

func TestSerializableRetryDelay(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		attempt int
		minimum time.Duration
		maximum time.Duration
	}{
		{1, 10 * time.Millisecond, 20 * time.Millisecond},
		{2, 20 * time.Millisecond, 40 * time.Millisecond},
		{3, 40 * time.Millisecond, 80 * time.Millisecond},
		{5, 160 * time.Millisecond, 320 * time.Millisecond},
		{6, 250 * time.Millisecond, 500 * time.Millisecond},
		{9, 250 * time.Millisecond, 500 * time.Millisecond},
	} {
		t.Run(fmt.Sprintf("attempt-%d", tc.attempt), func(t *testing.T) {
			t.Parallel()

			delays := make(map[time.Duration]struct{})
			for range 32 {
				delay := serializableRetryDelay(tc.attempt)
				require.GreaterOrEqual(t, delay, tc.minimum)
				require.Less(t, delay, tc.maximum)
				delays[delay] = struct{}{}
			}
			assert.Greater(t, len(delays), 1, "retries must not use a deterministic delay")
		})
	}
}

func TestConnectChecksAndConfiguresPool(t *testing.T) {
	t.Parallel()

	databaseURL := testDatabaseURL(t)
	client, err := Connect(t.Context(), databaseURL, "session-test",
		WithMaxConnections(2),
		WithRuntimeParam("search_path", "public"),
	)
	require.NoError(t, err)
	t.Cleanup(client.Close)

	assert.EqualValues(t, 2, client.Pool().Config().MaxConns)
	assert.Equal(t, pgx.QueryExecModeExec, client.Pool().Config().ConnConfig.DefaultQueryExecMode)

	var searchPath string
	require.NoError(t, client.Pool().QueryRow(t.Context(),
		"SELECT current_setting('search_path')",
	).Scan(&searchPath))
	assert.Equal(t, "public", searchPath)

	missing, err := url.Parse(databaseURL)
	require.NoError(t, err)
	missing.Path = "/missing"
	_, err = Connect(t.Context(), missing.String(), "missing")
	require.Error(t, err, "Connect accepted a pool that could not reach its database")
}

func TestAdvisoryLockSerializesOneKeyAndReleases(t *testing.T) {
	t.Parallel()

	client := testClient(t)
	held, err := client.AcquireAdvisoryLock(t.Context(), "held")
	require.NoError(t, err)
	// A failed assertion below must not leave the session lock's connection
	// checked out: client.Close would then wait for it forever.
	t.Cleanup(func() { _ = held.Release(context.WithoutCancel(t.Context())) })

	// Try is non-blocking, so only a busy result is acceptable; a deadline
	// here would turn a slow round-trip on a loaded runner into a failure.
	_, err = client.TryAcquireAdvisoryLock(t.Context(), "held")
	require.ErrorIs(t, err, ErrAdvisoryLockBusy)
	assert.EqualValues(t, 1, client.Pool().Stat().AcquiredConns())

	// The blocking form would wait for the lock; the deadline is what ends it.
	contended, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	_, err = client.AcquireAdvisoryLock(contended, "held")
	require.Error(t, err, "the same key acquired twice")

	other, err := client.TryAcquireAdvisoryLock(t.Context(), "other")
	require.NoError(t, err, "an unrelated key was blocked")
	require.NoError(t, other.Release(t.Context()))

	require.NoError(t, held.Release(t.Context()))
	require.NoError(t, held.Release(t.Context()))
	require.ErrorIs(t, held.InSerializableTx(t.Context(),
		func(context.Context, pgx.Tx) error { return nil }), errLockReleased)

	regained, err := client.AcquireAdvisoryLock(t.Context(), "held")
	require.NoError(t, err)
	require.NoError(t, regained.Release(t.Context()))
}

func TestAdvisoryLockRetriesSerializableTransactionOnItsSession(t *testing.T) {
	t.Parallel()

	client := testClient(t)
	_, err := client.Pool().Exec(t.Context(), `
		CREATE TABLE counters (id integer PRIMARY KEY, value integer NOT NULL);
		INSERT INTO counters (id, value) VALUES (1, 0);
	`)
	require.NoError(t, err)

	lock, err := client.AcquireAdvisoryLock(t.Context(), "counter")
	require.NoError(t, err)
	t.Cleanup(func() { _ = lock.Release(context.WithoutCancel(t.Context())) })

	type transactionResult struct {
		attempt int
		backend int
	}
	attempts := 0
	result, err := InSerializableTxReturn1(t.Context(), lock, func(ctx context.Context, tx pgx.Tx) (transactionResult, error) {
		attempts++
		deadline, ok := ctx.Deadline()
		require.True(t, ok, "transaction attempts must have a deadline")
		assert.LessOrEqual(t, time.Until(deadline), 5*time.Second)

		var value int
		var backend int
		if err := tx.QueryRow(ctx,
			"SELECT value, pg_backend_pid() FROM counters WHERE id = 1",
		).Scan(&value, &backend); err != nil {
			return transactionResult{}, err
		}
		if attempts == 1 {
			if _, err := client.Pool().Exec(ctx,
				"UPDATE counters SET value = 1 WHERE id = 1",
			); err != nil {
				return transactionResult{}, err
			}
		}

		if _, err := tx.Exec(ctx, "UPDATE counters SET value = 2 WHERE id = 1"); err != nil {
			return transactionResult{attempt: attempts, backend: backend}, err
		}

		return transactionResult{attempt: attempts, backend: backend}, nil
	})
	require.NoError(t, err)
	assert.Equal(t, 2, attempts)
	assert.Equal(t, 2, result.attempt, "returned a value from an attempt that did not commit")

	var value int
	var lockBackend int
	require.NoError(t, client.Pool().QueryRow(t.Context(),
		"SELECT value FROM counters WHERE id = 1",
	).Scan(&value))
	require.NoError(t, client.Pool().QueryRow(t.Context(), `SELECT pid FROM pg_locks
		WHERE locktype = 'advisory'
		  AND database = (SELECT oid FROM pg_database WHERE datname = current_database())`,
	).Scan(&lockBackend))
	assert.Equal(t, 2, value)
	assert.Equal(t, lockBackend, result.backend)
}

func TestSerializableTransactionCleansUpAndBoundsReplays(t *testing.T) {
	t.Parallel()

	client := testClient(t)
	_, err := client.Pool().Exec(t.Context(),
		"CREATE TABLE counters (id integer PRIMARY KEY, value integer NOT NULL); INSERT INTO counters VALUES (1, 0)",
	)
	require.NoError(t, err)

	lock, err := client.AcquireAdvisoryLock(t.Context(), "counter")
	require.NoError(t, err)
	t.Cleanup(func() { _ = lock.Release(context.WithoutCancel(t.Context())) })

	refused := errors.New("refused")
	returned, err := InSerializableTxReturn1(t.Context(), lock, func(ctx context.Context, tx pgx.Tx) (int, error) {
		_, err := tx.Exec(ctx, "UPDATE counters SET value = 1 WHERE id = 1")
		if err != nil {
			return 0, err
		}

		return 42, refused
	})
	require.ErrorIs(t, err, refused)
	assert.Zero(t, returned, "returned a value from a refused transaction")
	var value int
	require.NoError(t, client.Pool().QueryRow(t.Context(),
		"SELECT value FROM counters WHERE id = 1",
	).Scan(&value))
	require.Zero(t, value, "a refused write was committed")

	const panicValue = "panic"
	recovered := func() (recovered any) {
		defer func() { recovered = recover() }()

		_ = lock.InSerializableTx(t.Context(), func(ctx context.Context, tx pgx.Tx) error {
			if _, err := tx.Exec(ctx, "UPDATE counters SET value = 2 WHERE id = 1"); err != nil {
				return err
			}
			panic(panicValue)
		})

		return nil
	}()
	require.Equal(t, panicValue, recovered)
	require.NoError(t, client.Pool().QueryRow(t.Context(),
		"SELECT value FROM counters WHERE id = 1",
	).Scan(&value))
	require.Zero(t, value, "a panicking write was committed")

	require.NoError(t, lock.InSerializableTx(t.Context(), func(ctx context.Context, tx pgx.Tx) error {
		_, err := tx.Exec(ctx, "UPDATE counters SET value = 3 WHERE id = 1")

		return err
	}))
	require.NoError(t, client.Pool().QueryRow(t.Context(),
		"SELECT value FROM counters WHERE id = 1",
	).Scan(&value))
	assert.Equal(t, 3, value)

	deadline := time.Now().Add(time.Second)
	shorter, cancelShorter := context.WithDeadline(t.Context(), deadline)
	defer cancelShorter()
	require.NoError(t, lock.InSerializableTx(shorter, func(ctx context.Context, _ pgx.Tx) error {
		actual, ok := ctx.Deadline()
		require.True(t, ok)
		assert.Equal(t, deadline, actual, "transaction budget must not extend the caller's deadline")

		return nil
	}))

	attempts := 0
	err = lock.InSerializableTx(t.Context(), func(context.Context, pgx.Tx) error {
		attempts++

		return &pgconn.PgError{Code: pgerrcode.SerializationFailure}
	})
	require.Error(t, err)
	assert.Equal(t, serializableAttempts, attempts)

	cancelled, cancel := context.WithCancel(t.Context())
	attempts = 0
	err = lock.InSerializableTx(cancelled, func(context.Context, pgx.Tx) error {
		attempts++
		cancel()

		return &pgconn.PgError{Code: pgerrcode.SerializationFailure}
	})
	require.ErrorIs(t, err, context.Canceled)
	var conflict *pgconn.PgError
	require.ErrorAs(t, err, &conflict)
	assert.Equal(t, 1, attempts)
}

func TestAdvisoryLockDiscardsSessionWithUnknownState(t *testing.T) {
	t.Parallel()

	client := testClient(t)
	lock, err := client.AcquireAdvisoryLock(t.Context(), "discarded")
	require.NoError(t, err)

	require.NoError(t, lock.InSerializableTx(t.Context(), func(ctx context.Context, tx pgx.Tx) error {
		_, err := tx.Exec(ctx, "SELECT pg_advisory_unlock_all()")

		return err
	}))
	require.ErrorIs(t, lock.Release(t.Context()), errLockNotHeld)
	assert.Zero(t, client.Pool().Stat().TotalConns())

	regained, err := client.AcquireAdvisoryLock(t.Context(), "discarded")
	require.NoError(t, err)
	require.NoError(t, regained.Release(t.Context()))
}

func testClient(t *testing.T) *Client {
	t.Helper()

	client, err := Connect(t.Context(), testDatabaseURL(t), "session-test",
		WithMaxConnections(4),
	)
	require.NoError(t, err)
	t.Cleanup(client.Close)

	return client
}

func testDatabaseURL(t *testing.T) string {
	t.Helper()

	container, err := postgres.Run(
		t.Context(),
		testPostgresImage,
		postgres.WithDatabase("postgres"),
		postgres.WithUsername("postgres"),
		postgres.WithPassword("postgres"),
		postgres.BasicWaitStrategies(),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, container.Terminate(context.WithoutCancel(t.Context()))) })

	endpoint, err := container.Endpoint(t.Context(), "")
	require.NoError(t, err)

	return fmt.Sprintf("postgres://postgres:postgres@%s/postgres?sslmode=disable", endpoint)
}
