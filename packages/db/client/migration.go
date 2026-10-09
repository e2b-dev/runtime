package client

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	_ "github.com/lib/pq" //nolint:blank-imports
	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/database"
	"github.com/riverqueue/river/riverdriver/riverdatabasesql"
	"go.uber.org/zap"

	dbmodule "github.com/e2b-dev/infra/packages/db"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

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
	db, err := sql.Open("postgres", connectionString)
	if err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}
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

	version, err := store.GetLatestVersion(ctx, db)
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
	validation, err := riverMigrator.Validate(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to validate river migrations: %w", err)
	}
	if !validation.OK {
		return fmt.Errorf("river migrations are not current: %s", strings.Join(validation.Messages, "; "))
	}

	logger.L().Info(ctx, "Database version", zap.Int64("version", version), zap.Int64("expected_migration", expectedMigration))

	return nil
}
