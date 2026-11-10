// Package dbmigrate provides the PostgreSQL migration mechanics shared by
// services while leaving each service in control of its migration streams,
// ledger names, queue schema, and surrounding bootstrap policy.
package dbmigrate

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"time"

	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/database"
	"github.com/pressly/goose/v3/lock"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivermigrate"
)

const unlockTimeout = 5 * time.Second

// ErrLockNotHeld reports that PostgreSQL did not release the requested lock
// from the dedicated session that acquired it.
var ErrLockNotHeld = errors.New("database advisory lock was not held")

// NewGooseStore creates a PostgreSQL migration store using the caller's
// tracking table without changing Goose's package globals.
func NewGooseStore(trackingTable string) (database.Store, error) {
	store, err := database.NewStore(goose.DialectPostgres, trackingTable)
	if err != nil {
		return nil, fmt.Errorf("create migration store: %w", err)
	}

	return store, nil
}

// NewGooseProvider creates a PostgreSQL Goose provider without changing
// Goose's package globals. The caller owns the migration stream and tracking
// table name.
func NewGooseProvider(databaseConnection *sql.DB, migrations fs.FS, trackingTable string) (*goose.Provider, error) {
	store, err := NewGooseStore(trackingTable)
	if err != nil {
		return nil, err
	}
	locker, err := lock.NewPostgresSessionLocker()
	if err != nil {
		return nil, fmt.Errorf("create migration session locker: %w", err)
	}
	provider, err := goose.NewProvider(
		"",
		databaseConnection,
		migrations,
		goose.WithStore(store),
		goose.WithSessionLocker(locker),
	)
	if err != nil {
		return nil, fmt.Errorf("create migration provider: %w", err)
	}

	return provider, nil
}

// NewRiverMigrator creates a River migrator targeting an explicit schema.
func NewRiverMigrator[TTx any](driver riverdriver.Driver[TTx], schema string) (*rivermigrate.Migrator[TTx], error) {
	migrator, err := rivermigrate.New(driver, &rivermigrate.Config{Schema: schema})
	if err != nil {
		return nil, fmt.Errorf("create river migrator: %w", err)
	}

	return migrator, nil
}

// WithPostgresSessionLock runs operation while holding a named PostgreSQL
// advisory lock. The lock is acquired and released on the same dedicated
// connection because advisory locks are scoped to a backend session.
func WithPostgresSessionLock(
	ctx context.Context,
	databaseConnection *sql.DB,
	name string,
	operation func(context.Context) error,
) (err error) {
	release, err := acquirePostgresSessionLock(ctx, databaseConnection, name)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, release()) }()

	return operation(ctx)
}

func acquirePostgresSessionLock(ctx context.Context, databaseConnection *sql.DB, name string) (func() error, error) {
	connection, err := databaseConnection.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("open the %s lock connection: %w", name, err)
	}

	var ignored any
	if err := connection.QueryRowContext(ctx,
		"SELECT pg_advisory_lock(hashtextextended($1, 0))", name,
	).Scan(&ignored); err != nil {
		return nil, errors.Join(fmt.Errorf("acquire the %s lock: %w", name, err), connection.Close())
	}

	return func() error {
		releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), unlockTimeout)
		defer cancel()

		var unlocked bool
		unlockErr := connection.QueryRowContext(releaseCtx,
			"SELECT pg_advisory_unlock(hashtextextended($1, 0))", name,
		).Scan(&unlocked)
		closeErr := connection.Close()

		switch {
		case unlockErr != nil:
			return fmt.Errorf("release the %s lock: %w", name, unlockErr)
		case !unlocked:
			return fmt.Errorf("%w: %s", ErrLockNotHeld, name)
		case closeErr != nil:
			return fmt.Errorf("close the %s lock connection: %w", name, closeErr)
		default:
			return nil
		}
	}, nil
}
