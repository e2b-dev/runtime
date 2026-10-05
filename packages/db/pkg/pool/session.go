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
	sessionReleaseTimeout    = 5 * time.Second
	serializableAttempts     = 10
	serializableRetryBase    = 10 * time.Millisecond
	serializableRetryMax     = 500 * time.Millisecond
	serializableRetryTimeout = 5 * time.Second
)

var (
	errLockReleased = errors.New("advisory lock has been released")
	errLockNotHeld  = errors.New("database advisory lock was not held")
)

// ErrAdvisoryLockBusy reports that a non-blocking lock attempt found a holder.
var ErrAdvisoryLockBusy = errors.New("database advisory lock is held")

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
		return runInTxReturn1(ctx, lock.conn, fn)
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
		value, err := runInTxReturn1(ctx, lock.conn, fn)
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

	ctx, cancel := context.WithTimeout(ctx, serializableRetryTimeout)
	defer cancel()

	var conflict error
	for attempt := 1; attempt <= serializableAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return zero, errors.Join(err, conflict)
		}

		value, err := run(ctx)
		switch {
		case err == nil:
			return value, nil
		// A destroyed session cannot replay anything.
		case isSerializationConflict(err) && lock.conn != nil:
			conflict = err
		default:
			return zero, err
		}

		if attempt == serializableAttempts {
			break
		}
		if err := waitToReplay(ctx, attempt); err != nil {
			return zero, errors.Join(err, conflict)
		}
	}

	return zero, fmt.Errorf("serializable transaction did not commit in %d attempts: %w",
		serializableAttempts, conflict)
}

func runInTxReturn1[T any](
	ctx context.Context,
	conn *pgxpool.Conn,
	fn func(context.Context, pgx.Tx) (T, error),
) (T, error) {
	var zero T

	tx, err := conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return zero, fmt.Errorf("begin a serializable transaction: %w", err)
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
		return zero, fmt.Errorf("commit a serializable transaction: %w", err)
	}

	return value, nil
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
	timer := time.NewTimer(serializableRetryDelay(attempt))
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func serializableRetryDelay(attempt int) time.Duration {
	// Equal jitter keeps retries apart without allowing an immediate replay.
	// attempt is bounded by serializableAttempts, so the shift cannot overflow.
	window := min(2*serializableRetryBase<<(attempt-1), serializableRetryMax)

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
