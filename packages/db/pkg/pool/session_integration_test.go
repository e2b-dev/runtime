package pool

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/e2b-dev/infra/packages/db/pkg/retry"
)

const testPostgresImage = "postgres:18-alpine"

func TestTransactionRetryDelay(t *testing.T) {
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
				delay := transactionRetryDelay(tc.attempt)
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
	t.Cleanup(func() { closeBounded(t, client) })

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
	// An unexpected success must release before the assertion fails the
	// test: the leaked lock connection would otherwise keep pgxpool's Close
	// waiting for the rest of the go test timeout.
	stolen, err := client.TryAcquireAdvisoryLock(t.Context(), "held")
	if stolen != nil {
		_ = stolen.Release(context.WithoutCancel(t.Context()))
	}
	require.ErrorIs(t, err, ErrAdvisoryLockBusy)
	assert.EqualValues(t, 1, client.Pool().Stat().AcquiredConns())

	// The blocking form would wait for the lock; the deadline is what ends it.
	contended, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	reacquired, err := client.AcquireAdvisoryLock(contended, "held")
	if reacquired != nil {
		_ = reacquired.Release(context.WithoutCancel(t.Context()))
	}
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

func TestPoolTransactionRetriesOnlyAbortedTransactions(t *testing.T) {
	t.Parallel()
	client := testClient(t)
	_, err := client.Pool().Exec(t.Context(), "CREATE TABLE counters (code text PRIMARY KEY, value integer NOT NULL)")
	require.NoError(t, err)

	for _, tc := range []struct {
		code     string
		attempts int
	}{
		{pgerrcode.SerializationFailure, 2},
		{pgerrcode.DeadlockDetected, 2},
		{pgerrcode.TransactionResolutionUnknown, 1},
		{pgerrcode.CheckViolation, 1},
	} {
		t.Run(tc.code, func(t *testing.T) {
			t.Parallel()
			_, err := client.Pool().Exec(t.Context(), "INSERT INTO counters VALUES ($1, 0)", tc.code)
			require.NoError(t, err)
			attempts := 0
			result, err := InTxReturn1(t.Context(), client.Pool(), func(ctx context.Context, tx pgx.Tx) (int, error) {
				attempts++
				var isolation string
				if err := tx.QueryRow(ctx, "SHOW transaction_isolation").Scan(&isolation); err != nil {
					return 0, err
				}
				require.Equal(t, "read committed", isolation)
				if _, err := tx.Exec(ctx, "UPDATE counters SET value=value+1 WHERE code=$1", tc.code); err != nil {
					return 0, err
				}
				if attempts == 1 {
					_, err := tx.Exec(ctx, fmt.Sprintf("DO $$ BEGIN RAISE EXCEPTION USING ERRCODE = '%s'; END $$", tc.code))

					return attempts, err
				}

				return attempts, nil
			})
			require.Equal(t, tc.attempts, attempts)
			var stored int
			require.NoError(t, client.Pool().QueryRow(t.Context(), "SELECT value FROM counters WHERE code=$1", tc.code).Scan(&stored))
			if tc.attempts == 2 {
				require.NoError(t, err)
				require.Equal(t, 2, result)
				require.Equal(t, 1, stored, "the failed attempt must roll back")
			} else {
				var pgErr *pgconn.PgError
				require.ErrorAs(t, err, &pgErr)
				require.Equal(t, tc.code, pgErr.Code)
				require.Zero(t, result, "a failed transaction must not return its value")
				require.Zero(t, stored)
			}
		})
	}
}

// A COMMIT that was never sent, or that PostgreSQL rejected with an ordinary
// ERROR, is a known abort. Any other failure after PostgreSQL received the
// COMMIT reports an unknown outcome with the original error, and is never
// replayed, even when that error carries a serialization SQLSTATE.
func TestPoolTransactionMarksOnlyUnknownCommitOutcomes(t *testing.T) {
	t.Parallel()
	dsn := testDatabaseURL(t)
	client, err := Connect(t.Context(), dsn, "session-test", WithMaxConnections(4))
	require.NoError(t, err)
	t.Cleanup(func() { closeBounded(t, client) })
	_, err = client.Pool().Exec(t.Context(), `
		CREATE TABLE parents (id integer PRIMARY KEY);
		CREATE TABLE children (id integer PRIMARY KEY, parent integer REFERENCES parents DEFERRABLE INITIALLY DEFERRED)`)
	require.NoError(t, err)
	committed := func(id int) bool {
		var found bool
		require.NoError(t, client.Pool().QueryRow(t.Context(), "SELECT EXISTS (SELECT 1 FROM parents WHERE id = $1)", id).Scan(&found))

		return found
	}

	attempts := 0
	_, err = InTxReturn1(t.Context(), client.Pool(), func(ctx context.Context, tx pgx.Tx) (int, error) {
		attempts++
		_, err := tx.Exec(ctx, "INSERT INTO children VALUES (1, 1)")

		return 0, err
	})
	var pgErr *pgconn.PgError
	require.ErrorAs(t, err, &pgErr, "the deferred check fails at commit")
	require.Equal(t, pgerrcode.ForeignKeyViolation, pgErr.Code)
	require.NotErrorIs(t, err, ErrCommitOutcomeUnknown)
	require.Equal(t, 1, attempts)

	// The caller is cancelled after a successful write, so pgx never sends
	// the COMMIT.
	ctx, cancel := context.WithCancel(t.Context())
	attempts = 0
	_, err = InTxReturn1(ctx, client.Pool(), func(ctx context.Context, tx pgx.Tx) (int, error) {
		attempts++
		_, err := tx.Exec(ctx, "INSERT INTO parents VALUES (1)")
		cancel()

		return 0, err
	})
	require.ErrorIs(t, err, context.Canceled)
	require.True(t, pgconn.SafeToRetry(err))
	require.NotErrorIs(t, err, ErrCommitOutcomeUnknown)
	require.Equal(t, 1, attempts)
	require.False(t, committed(1))

	// PostgreSQL commits each of these, then the client loses the reply or
	// receives a FATAL or connection-exception error instead.
	for i, reply := range []*pgproto3.ErrorResponse{
		nil,
		{Severity: "ERROR", SeverityUnlocalized: "ERROR", Code: pgerrcode.ConnectionFailure, Message: "server conn crashed?"},
		{Severity: "FATAL", Code: pgerrcode.SerializationFailure, Message: "server conn crashed?"},
	} {
		var encoded []byte
		if reply != nil {
			encoded, err = reply.Encode(nil)
			require.NoError(t, err)
		}
		config, err := pgxpool.ParseConfig(dsn)
		require.NoError(t, err)
		config.ConnConfig.DialFunc = func(ctx context.Context, network, address string) (net.Conn, error) {
			conn, err := (&net.Dialer{}).DialContext(ctx, network, address)
			if err != nil {
				return nil, err
			}

			return &commitReply{Conn: conn, reply: encoded}, nil
		}
		lossy, err := pgxpool.NewWithConfig(t.Context(), config)
		require.NoError(t, err)
		t.Cleanup(lossy.Close)
		id := 10 + i
		attempts = 0
		_, err = InTxReturn1(t.Context(), lossy, func(ctx context.Context, tx pgx.Tx) (int, error) {
			attempts++
			_, err := tx.Exec(ctx, "INSERT INTO parents VALUES ($1)", id)

			return 0, err
		})
		require.ErrorIs(t, err, ErrCommitOutcomeUnknown, reply)
		if reply == nil {
			// pgx reports the dead connection as safe to retry after sending.
			require.ErrorIs(t, err, pgconn.ErrConnClosed)
			require.True(t, pgconn.SafeToRetry(err))
		} else {
			require.ErrorAs(t, err, &pgErr)
			require.Equal(t, reply.Code, pgErr.Code)
		}
		require.Equal(t, 1, attempts, "an unknown outcome is never replayed")
		require.True(t, committed(id), "PostgreSQL applied the transaction")
	}
}

// commitReply receives PostgreSQL's successful reply to a COMMIT, then
// discards it and closes the connection or, given an encoded ErrorResponse,
// delivers that in place of the reply's CommandComplete.
type commitReply struct {
	net.Conn

	reply      []byte
	committing atomic.Bool
}

func (c *commitReply) Write(p []byte) (int, error) {
	if bytes.Contains(p, []byte("commit\x00")) {
		c.committing.Store(true)
	}

	return c.Conn.Write(p)
}

func (c *commitReply) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	tag := bytes.Index(p[:n], []byte("COMMIT\x00"))
	if !c.committing.Load() || tag < 0 {
		return n, err
	}
	c.committing.Store(false)
	if c.reply == nil {
		_ = c.Conn.Close()

		return 0, io.EOF
	}
	// CommandComplete is a type byte and a four-byte length before its tag.
	rest := slices.Clone(p[tag+len("COMMIT\x00") : n])
	n = tag - 5 + copy(p[tag-5:], c.reply)

	return n + copy(p[n:], rest), err
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
	assert.Equal(t, transactionAttempts, attempts)

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

func TestAdvisoryLockHoldsASecondKeyAroundEachAttempt(t *testing.T) {
	t.Parallel()

	client := testClient(t)
	// busy may run inside Eventually or fn, so it reports rather than stops.
	busy := func(key string) bool {
		probe, err := client.TryAcquireAdvisoryLock(t.Context(), key)
		if err == nil {
			assert.NoError(t, probe.Release(t.Context()))

			return false
		}
		assert.ErrorIs(t, err, ErrAdvisoryLockBusy)

		return true
	}
	backend := func(ctx context.Context, tx pgx.Tx) int {
		var pid int
		require.NoError(t, tx.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&pid))

		return pid
	}
	conflict := &pgconn.PgError{Code: pgerrcode.SerializationFailure}
	nothing := func(context.Context, pgx.Tx) error { return nil }

	outer, err := client.AcquireAdvisoryLock(t.Context(), "outer")
	require.NoError(t, err)
	t.Cleanup(func() { _ = outer.Release(context.WithoutCancel(t.Context())) })
	session, err := InSerializableTxReturn1(t.Context(), outer, func(ctx context.Context, tx pgx.Tx) (int, error) {
		return backend(ctx, tx), nil
	})
	require.NoError(t, err)

	contender, err := client.Pool().Acquire(t.Context())
	require.NoError(t, err)
	defer contender.Release()

	// A queued competitor must acquire between attempts, even when the
	// final attempt refuses. Keeping the key across retries would block it.
	refused := errors.New("refused")
	for _, returned := range []error{nil, refused} {
		attempts := 0
		competed := make(chan error, 1)
		value, err := InSerializableTxUnderAdvisoryLockReturn1(t.Context(), outer, "inner", func(ctx context.Context, tx pgx.Tx) (int, error) {
			attempts++
			assert.True(t, busy("inner"))
			assert.Equal(t, session, backend(ctx, tx))
			if attempts == 1 {
				go func() {
					_, err := contender.Exec(ctx, `SELECT pg_advisory_lock(hashtextextended('inner', 0));
						SELECT pg_advisory_unlock(hashtextextended('inner', 0))`)
					competed <- err
				}()
				require.Eventually(t, func() bool {
					var waiting bool
					err := client.Pool().QueryRow(ctx,
						"SELECT wait_event = 'advisory' FROM pg_stat_activity WHERE pid = $1",
						contender.Conn().PgConn().PID()).Scan(&waiting)

					return err == nil && waiting
				}, time.Second, time.Millisecond)

				return 0, conflict
			}
			select {
			case err := <-competed:
				require.NoError(t, err)
			case <-ctx.Done():
				t.Error("the competitor could not acquire between attempts")
			}

			return 7, returned
		})
		require.ErrorIs(t, err, returned)
		assert.Equal(t, 2, attempts)
		if returned == nil {
			assert.Equal(t, 7, value)
		}
		assert.False(t, busy("inner"), "the second key outlived the attempts")
		assert.True(t, busy("outer"), "releasing the second key released the first")
		assert.EqualValues(t, 2, client.Pool().Stat().AcquiredConns())
	}
	contender.Release()

	require.NoError(t, outer.Release(t.Context()))

	// An unlock that finds the key already gone destroys the session, and a
	// conflict of that attempt is not replayed on it.
	outer, err = client.AcquireAdvisoryLock(t.Context(), "outer")
	require.NoError(t, err)
	attempts := 0
	_, err = InSerializableTxUnderAdvisoryLockReturn1(t.Context(), outer, "inner", func(ctx context.Context, tx pgx.Tx) (int, error) {
		attempts++
		if _, err := tx.Exec(ctx, "SELECT pg_advisory_unlock_all()"); err != nil {
			return 0, err
		}

		return 0, conflict
	})
	require.ErrorIs(t, err, errLockNotHeld)
	assert.Equal(t, 1, attempts)
	require.ErrorIs(t, outer.InSerializableTx(t.Context(), nothing), errLockReleased)

	// So does a panic, which leaves the second key's state unknown.
	outer, err = client.AcquireAdvisoryLock(t.Context(), "outer")
	require.NoError(t, err)
	require.PanicsWithValue(t, "panic", func() {
		_, _ = InSerializableTxUnderAdvisoryLockReturn1(t.Context(), outer, "inner", func(context.Context, pgx.Tx) (int, error) {
			panic("panic")
		})
	})
	require.ErrorIs(t, outer.InSerializableTx(t.Context(), nothing), errLockReleased)
	require.Eventually(t, func() bool { return !busy("outer") && !busy("inner") }, 10*time.Second, 5*time.Millisecond)
	assert.Zero(t, client.Pool().Stat().AcquiredConns())
}

// Keep the holder locked through cleanup. A failed CancelRequest must not
// leave the backend queued, even after it disappears from local pool accounting.
func TestAdvisoryLockWaitHasServerTimeout(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name        string
		timeout     string
		cancelEarly bool
	}{
		{name: "confirmed timeout reuses session", timeout: "25ms"},
		{name: "lost cancellation still ends wait", timeout: "1s", cancelEarly: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			url := testDatabaseURL(t)
			holder, err := pgxpool.New(t.Context(), url)
			require.NoError(t, err)
			t.Cleanup(holder.Close)
			_, err = holder.Exec(t.Context(), "SELECT pg_advisory_lock(hashtextextended('inner', 0))")
			require.NoError(t, err)

			var blockDial atomic.Bool
			var blocked atomic.Int32
			client, err := Connect(t.Context(), url, "lock-timeout-test", WithMaxConnections(1),
				WithRuntimeParam("lock_timeout", tc.timeout),
				func(config *pgxpool.Config, _ *retry.Config) {
					dial := config.ConnConfig.DialFunc
					config.ConnConfig.DialFunc = func(ctx context.Context, network, address string) (net.Conn, error) {
						if blockDial.Load() {
							blocked.Add(1)

							return nil, errors.New("cancel connection unavailable")
						}

						return dial(ctx, network, address)
					}
				})
			require.NoError(t, err)
			t.Cleanup(func() { closeBounded(t, client) })
			outer, err := client.AcquireAdvisoryLock(t.Context(), "outer")
			require.NoError(t, err)
			t.Cleanup(func() { _ = outer.Release(context.WithoutCancel(t.Context())) })
			pid := outer.conn.Conn().PgConn().PID()
			blockDial.Store(true)

			ctx, cancel := context.WithCancel(t.Context())
			result := make(chan error, 1)
			done := make(chan struct{})
			t.Cleanup(func() {
				cancel()
				<-done
			})
			go func() {
				defer close(done)
				_, err := InSerializableTxUnderAdvisoryLockReturn1(ctx, outer, "inner", func(context.Context, pgx.Tx) (int, error) {
					t.Error("transaction ran without the second lock")

					return 0, nil
				})
				result <- err
			}()
			if tc.cancelEarly {
				require.Eventually(t, func() bool {
					var waiting bool
					err := holder.QueryRow(t.Context(), "SELECT wait_event = 'advisory' FROM pg_stat_activity WHERE pid = $1", pid).Scan(&waiting)

					return err == nil && waiting
				}, 5*time.Second, time.Millisecond)
				cancel()
				require.ErrorIs(t, <-result, context.Canceled)
				require.Eventually(t, func() bool {
					var alive bool
					err := holder.QueryRow(t.Context(), "SELECT EXISTS (SELECT FROM pg_stat_activity WHERE pid = $1)", pid).Scan(&alive)

					return err == nil && !alive && blocked.Load() > 0
				}, 10*time.Second, 5*time.Millisecond)

				return
			}

			var pgErr *pgconn.PgError
			require.ErrorAs(t, <-result, &pgErr)
			require.Equal(t, pgerrcode.LockNotAvailable, pgErr.Code)
			require.NoError(t, outer.Release(t.Context()))
			// The initial name-lock path must also return a healthy session
			// after a confirmed timeout, since both paths use the same pool.
			_, err = client.AcquireAdvisoryLock(t.Context(), "inner")
			require.ErrorAs(t, err, &pgErr)
			require.Equal(t, pgerrcode.LockNotAvailable, pgErr.Code)
			var reused uint32
			var timeout string
			require.NoError(t, client.Pool().QueryRow(t.Context(),
				"SELECT pg_backend_pid(), current_setting('lock_timeout')").Scan(&reused, &timeout))
			assert.Equal(t, pid, reused)
			assert.Equal(t, tc.timeout, timeout)
			assert.Zero(t, blocked.Load())
			var locks int
			require.NoError(t, holder.QueryRow(t.Context(),
				"SELECT count(*) FROM pg_locks WHERE pid = $1 AND locktype = 'advisory'", pid).Scan(&locks))
			assert.Zero(t, locks, "the returned session still holds a lock")
		})
	}
}

func testClient(t *testing.T) *Client {
	t.Helper()

	client, err := Connect(t.Context(), testDatabaseURL(t), "session-test",
		WithMaxConnections(4),
	)
	require.NoError(t, err)
	t.Cleanup(func() { closeBounded(t, client) })

	return client
}

// closeBounded fails instead of hanging: pgxpool's Close blocks until every
// checked-out connection returns, so a test that leaks a session-lock
// connection would otherwise sit in cleanup until the go test timeout.
func closeBounded(t *testing.T, client *Client) {
	t.Helper()

	done := make(chan struct{})
	go func() {
		client.Close()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Error("client.Close did not return within 30s; a lock connection leaked")
	}
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
