package client

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"time"

	"github.com/lib/pq"
	"github.com/lib/pq/pqerror"
	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/database"
	"github.com/riverqueue/river/riverdriver/riverdatabasesql"
	"github.com/riverqueue/river/rivermigrate"
	"go.uber.org/zap"

	dbmodule "github.com/e2b-dev/infra/packages/db"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/retry"
)

// startupReadPolicy outlasts a Postgres failover; callers exit when this check fails.
var startupReadPolicy = retry.Policy{
	TotalBudget:    5 * time.Minute,
	AttemptTimeout: 10 * time.Second,
	InitialBackoff: 250 * time.Millisecond,
	MaxBackoff:     5 * time.Second,
	Multiplier:     2,
}

// CheckMigrationVersion refuses a database older than the caller requires, or
// one whose River migrations are not current.
//
// It only reads. The version comes from an explicit store rather than goose's
// package-level version API, for two reasons. That API's read path creates the
// tracking table when it is missing — a write, on exactly the unmigrated
// database this check exists to turn away, and one a service's own database role
// may not be granted. And it selects the table through a global that any
// goroutine may rewrite, which the race detector reports as soon as two callers
// overlap.
func CheckMigrationVersion(ctx context.Context, connectionString string, expectedMigration int64) error {
	cfg, err := pq.NewConfig(connectionString)
	if err != nil {
		return fmt.Errorf("failed to parse connection string: %w", err)
	}
	// lib/pq bounds the startup handshake only by connect_timeout, not by the context.
	if cfg.ConnectTimeout == 0 || cfg.ConnectTimeout > startupReadPolicy.AttemptTimeout {
		cfg.ConnectTimeout = startupReadPolicy.AttemptTimeout
	}
	connector, err := pq.NewConnectorConfig(cfg)
	if err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}
	db := sql.OpenDB(connector)
	db.SetMaxOpenConns(1)
	defer func() {
		dbErr := db.Close()
		if dbErr != nil {
			logger.L().Error(ctx, "Failed to close database connection", zap.Error(dbErr))
		}
	}()

	store, err := database.NewStore(goose.DialectPostgres, dbmodule.TrackingTable)
	if err != nil {
		return fmt.Errorf("failed to create migration store: %w", err)
	}

	var version int64
	err = retry.Do(ctx, startupReadPolicy, isTransientReadError, func(ctx context.Context) error {
		var readErr error
		version, readErr = store.GetLatestVersion(ctx, db)

		return readErr
	}, nil)
	if err != nil {
		// A tracking table with no rows is version 0; an absent one means the
		// migrator has never run here, which is worth saying plainly.
		if !errors.Is(err, database.ErrVersionNotFound) {
			return fmt.Errorf("failed to get database version, has the migrator run: %w", err)
		}
		version = 0
	}

	// Check if the database version is less than the expected migration
	// We allow higher versions to account for future migrations and rollbacks
	if version < expectedMigration {
		return fmt.Errorf("database version %d is less than expected %d", version, expectedMigration)
	}

	riverMigrator, err := dbmodule.RiverMigrator(riverdatabasesql.New(db))
	if err != nil {
		return err
	}
	var validation *rivermigrate.ValidateResult
	err = retry.Do(ctx, startupReadPolicy, isTransientReadError, func(ctx context.Context) error {
		var validateErr error
		validation, validateErr = riverMigrator.Validate(ctx, nil)

		return validateErr
	}, nil)
	if err != nil {
		return fmt.Errorf("failed to validate river migrations: %w", err)
	}
	if !validation.OK {
		return fmt.Errorf("river migrations are not current: %s", strings.Join(validation.Messages, "; "))
	}

	logger.L().Info(ctx, "Database version", zap.Int64("version", version), zap.Int64("expected_migration", expectedMigration))

	return nil
}

// isTransientReadError reports whether a failed read may succeed on retry: the
// connection failed or dropped, or the server answered that it cannot serve right now.
func isTransientReadError(err error) bool {
	if pqErr, ok := errors.AsType[*pq.Error](err); ok {
		switch pqErr.Code.Class() {
		case pqerror.ClassConnectionException, pqerror.ClassInsufficientResources, pqerror.ClassOperatorIntervention:
			return true
		default:
			return false
		}
	}
	if _, ok := errors.AsType[*net.OpError](err); ok {
		return true
	}

	return errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, driver.ErrBadConn) || errors.Is(err, context.DeadlineExceeded)
}
