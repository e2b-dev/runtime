package pool

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	sessionReleaseTimeout   = 5 * time.Second
	transactionAttempts     = 10
	transactionRetryBase    = 10 * time.Millisecond
	transactionRetryMax     = 500 * time.Millisecond
	transactionRetryTimeout = 5 * time.Second
)

var (
	errLockReleased = errors.New("advisory lock has been released")
	errLockNotHeld  = errors.New("database advisory lock was not held")
)

// ErrAdvisoryLockBusy reports that a non-blocking lock attempt found a holder.
var ErrAdvisoryLockBusy = errors.New("database advisory lock is held")

// ErrCommitOutcomeUnknown marks a COMMIT that was sent but not answered with a
// rollback or an ordinary ERROR outside SQLSTATE class 08. A lost reply, a
// closed connection, a FATAL error and a connection exception all carry it,
// because the server may have applied the transaction first. It wraps the
// original error and is never replayed, whatever SQLSTATE that error carries.
// A COMMIT pgx reports as never sent does not carry it, unless pgx cannot
// tell, as with "conn closed".
var ErrCommitOutcomeUnknown = errors.New("transaction commit outcome is unknown")

// AdvisoryLock is a PostgreSQL session lock held on one pool connection. One
// lock belongs to one goroutine and must be released when its work finishes.
type AdvisoryLock struct {
	key  string
	conn *pgxpool.Conn
}

// AcquireAdvisoryLock waits for a session lock derived from key. Hash
// collisions may serialize unrelated callers, but cannot weaken exclusion.
func (c *Client) AcquireAdvisoryLock(ctx context.Context, key string) (*AdvisoryLock, error) {
	conn, err := c.pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquire an advisory lock connection: %w", err)
	}

	if _, err := conn.Exec(ctx,
		"SELECT pg_advisory_lock(hashtextextended($1, 0))", key,
	); err != nil {
		if isLockTimeout(err) {
			conn.Release()
		} else {
			// The server may have granted the lock before cancellation reached
			// the client, so an ambiguous session must not return to the pool.
			discard(ctx, conn)
		}

		return nil, fmt.Errorf("acquire an advisory lock: %w", err)
	}

	return &AdvisoryLock{key: key, conn: conn}, nil
}

// TryAcquireAdvisoryLock takes the same session lock without waiting.
func (c *Client) TryAcquireAdvisoryLock(ctx context.Context, key string) (*AdvisoryLock, error) {
	conn, err := c.pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquire an advisory lock connection: %w", err)
	}

	var locked bool
	if err := conn.QueryRow(ctx,
		"SELECT pg_try_advisory_lock(hashtextextended($1, 0))", key,
	).Scan(&locked); err != nil {
		discard(ctx, conn)

		return nil, fmt.Errorf("try an advisory lock: %w", err)
	}
	if !locked {
		conn.Release()

		return nil, ErrAdvisoryLockBusy
	}

	return &AdvisoryLock{key: key, conn: conn}, nil
}

// InTxReturn1 runs fn in a READ COMMITTED transaction without advisory locks.
// Serialization failures and deadlocks replay the whole callback, so fn must
// only perform work inside the transaction. Only a committed value is returned.
// Connection errors and uncertain commit outcomes are not retried. An uncertain
// commit outcome wraps ErrCommitOutcomeUnknown.
// Attempts and backoff share a five-second budget, bounded by the caller's deadline.
func InTxReturn1[T any](ctx context.Context, pool *pgxpool.Pool, fn func(context.Context, pgx.Tx) (T, error)) (T, error) {
	return replayTransaction(ctx, func(ctx context.Context) (T, error) {
		conn, err := pool.Acquire(ctx)
		if err != nil {
			var zero T

			return zero, err
		}
		defer conn.Release()

		return runInTxReturn1(ctx, conn, pgx.ReadCommitted, fn)
	}, isSerializationConflict)
}

// InSerializableTx runs fn in a SERIALIZABLE transaction on the session that
// holds the lock. Serialization failures and deadlocks replay the whole
// callback, so fn must not perform work outside PostgreSQL.
func (l *AdvisoryLock) InSerializableTx(ctx context.Context, fn func(context.Context, pgx.Tx) error) error {
	_, err := InSerializableTxReturn1(ctx, l, func(ctx context.Context, tx pgx.Tx) (struct{}, error) {
		return struct{}{}, fn(ctx, tx)
	})

	return err
}

// InSerializableTxReturn1 runs fn in a SERIALIZABLE transaction on the session
// that holds the lock. Serialization failures and deadlocks replay the whole
// callback; only the value produced by the attempt that commits is returned.
// Attempts and backoff share a five-second budget, bounded by the caller's deadline.
func InSerializableTxReturn1[T any](
	ctx context.Context,
	lock *AdvisoryLock,
	fn func(context.Context, pgx.Tx) (T, error),
) (T, error) {
	return replaySerializable(ctx, lock, func(ctx context.Context) (T, error) {
		return runInTxReturn1(ctx, lock.conn, pgx.Serializable, fn)
	})
}

// InSerializableTxUnderAdvisoryLockReturn1 runs fn like InSerializableTxReturn1
// and holds a second session lock, derived from key, around each attempt. The
// session that holds lock takes key before the attempt's transaction begins and
// releases it after its commit or rollback, so no backoff between attempts
// holds it. Waiting for key counts against the budget the attempts and backoff
// share. Configure lock_timeout on the client to bound server waits even when
// cancellation cannot be delivered. A confirmed lock timeout preserves the
// session and outer lock for the caller to release. If acquiring or releasing
// key has an unknown result, or fn panics, the session is destroyed.
//
// Waiting here while holding lock can deadlock with a session that holds key
// and waits for lock, so callers must take their locks in one fixed order.
func InSerializableTxUnderAdvisoryLockReturn1[T any](
	ctx context.Context,
	lock *AdvisoryLock,
	key string,
	fn func(context.Context, pgx.Tx) (T, error),
) (T, error) {
	return replaySerializable(ctx, lock, func(ctx context.Context) (T, error) {
		var zero T
		if _, err := lock.conn.Exec(ctx,
			"SELECT pg_advisory_lock(hashtextextended($1, 0))", key,
		); err != nil {
			if !isLockTimeout(err) {
				lock.destroy(ctx)
			}

			return zero, fmt.Errorf("acquire an advisory lock on a held session: %w", err)
		}

		finished := false
		defer func() {
			if !finished {
				lock.destroy(ctx)
			}
		}()
		value, err := runInTxReturn1(ctx, lock.conn, pgx.Serializable, fn)
		finished = true

		if unlockErr := lock.unlock(ctx, key); unlockErr != nil {
			return zero, errors.Join(err, unlockErr)
		}

		return value, err
	})
}

// replaySerializable runs attempt until it commits, replaying serialization
// failures and deadlocks with backoff under one budget.
func replaySerializable[T any](
	ctx context.Context,
	lock *AdvisoryLock,
	run func(context.Context) (T, error),
) (T, error) {
	var zero T
	if lock.conn == nil {
		return zero, errLockReleased
	}

	return replayTransaction(ctx, run, func(err error) bool {
		// A destroyed session cannot replay anything.
		return lock.conn != nil && isSerializationConflict(err)
	})
}

func replayTransaction[T any](ctx context.Context, run func(context.Context) (T, error), retryable func(error) bool) (T, error) {
	var zero T
	ctx, cancel := context.WithTimeout(ctx, transactionRetryTimeout)
	defer cancel()

	var conflict error
	for attempt := 1; attempt <= transactionAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return zero, errors.Join(err, conflict)
		}

		value, err := run(ctx)
		switch {
		case err == nil:
			return value, nil
		case errors.Is(err, ErrCommitOutcomeUnknown):
			return zero, err
		case retryable(err):
			conflict = err
		default:
			return zero, err
		}

		if attempt == transactionAttempts {
			break
		}
		if err := waitToReplay(ctx, attempt); err != nil {
			return zero, errors.Join(err, conflict)
		}
	}

	return zero, fmt.Errorf("transaction did not commit in %d attempts: %w",
		transactionAttempts, conflict)
}

func runInTxReturn1[T any](
	ctx context.Context,
	conn *pgxpool.Conn,
	isolation pgx.TxIsoLevel,
	fn func(context.Context, pgx.Tx) (T, error),
) (T, error) {
	var zero T

	tx, err := conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: isolation})
	if err != nil {
		return zero, fmt.Errorf("begin a transaction: %w", err)
	}
	defer func() {
		releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), sessionReleaseTimeout)
		defer cancel()

		_ = tx.Rollback(releaseCtx)
	}()

	value, err := fn(ctx, tx)
	if err != nil {
		return zero, err
	}
	if err := tx.Commit(ctx); err != nil {
		if isKnownCommitAbort(err) {
			return zero, fmt.Errorf("commit a transaction: %w", err)
		}

		return zero, fmt.Errorf("commit a transaction: %w: %w", ErrCommitOutcomeUnknown, err)
	}

	return value, nil
}

// isKnownCommitAbort reports whether a failed COMMIT certainly did not apply:
// it was never sent, the server rolled it back, or the server answered with an
// ordinary ERROR. pgx also marks "conn closed" safe to retry when the
// connection died while it read the reply, so that error counts as unknown. A
// FATAL error or a connection exception, which a pooler can report after its
// server connection died during the COMMIT, counts as unknown too, as does a
// localized severity sent without the unlocalized field.
func isKnownCommitAbort(err error) bool {
	if (pgconn.SafeToRetry(err) && !errors.Is(err, pgconn.ErrConnClosed)) || errors.Is(err, pgx.ErrTxCommitRollback) {
		return true
	}
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return false
	}
	severity := pgErr.SeverityUnlocalized
	if severity == "" {
		severity = pgErr.Severity
	}

	return severity == "ERROR" && !pgerrcode.IsConnectionException(pgErr.Code)
}

func isSerializationConflict(err error) bool {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return false
	}

	return pgErr.Code == pgerrcode.SerializationFailure || pgErr.Code == pgerrcode.DeadlockDetected
}

func isLockTimeout(err error) bool {
	var pgErr *pgconn.PgError

	return errors.As(err, &pgErr) && pgErr.Code == pgerrcode.LockNotAvailable
}

func waitToReplay(ctx context.Context, attempt int) error {
	timer := time.NewTimer(transactionRetryDelay(attempt))
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func transactionRetryDelay(attempt int) time.Duration {
	// Equal jitter keeps retries apart without allowing an immediate replay.
	// attempt is bounded by transactionAttempts, so the shift cannot overflow.
	window := min(2*transactionRetryBase<<(attempt-1), transactionRetryMax)

	return window/2 + rand.N(window/2)
}

// Release unlocks the session and returns its connection to the pool. It is
// idempotent. If the unlock result is unknown, Release destroys the session so
// a connection carrying an unknown lock cannot re-enter the pool.
func (l *AdvisoryLock) Release(ctx context.Context) error {
	conn := l.conn
	if conn == nil {
		return nil
	}
	if err := l.unlock(ctx, l.key); err != nil {
		return err
	}
	l.conn = nil
	conn.Release()

	return nil
}

// unlock releases one key held by the session. If the result is unknown, it
// destroys the session and leaves the lock without a connection.
func (l *AdvisoryLock) unlock(ctx context.Context, key string) error {
	releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), sessionReleaseTimeout)
	defer cancel()

	var unlocked bool
	err := l.conn.QueryRow(releaseCtx,
		"SELECT pg_advisory_unlock(hashtextextended($1, 0))", key,
	).Scan(&unlocked)

	switch {
	case err != nil:
		l.destroy(ctx)

		return fmt.Errorf("release an advisory lock: %w", err)
	case !unlocked:
		l.destroy(ctx)

		return errLockNotHeld
	}

	return nil
}

// destroy closes the session, which releases every lock it holds.
func (l *AdvisoryLock) destroy(ctx context.Context) {
	conn := l.conn
	l.conn = nil
	discard(ctx, conn)
}

func discard(ctx context.Context, conn *pgxpool.Conn) {
	releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), sessionReleaseTimeout)
	defer cancel()

	_ = conn.Hijack().Close(releaseCtx)
}
