package db

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/riverdriver/riverdatabasesql"
	"github.com/riverqueue/river/rivermigrate"

	"github.com/e2b-dev/infra/packages/shared/pkg/dbmigrate"
)

// TrackingTable is goose's bookkeeping table for Migrations.
const TrackingTable = "_migrations"

// RiverSchema keeps River's job tables out of public. A goose migration
// creates it; River's migrator fills it.
const RiverSchema = "river"

const migrationLockName = "db-migrations"

// RiverMigrator targets RiverSchema; the default resolves against search_path
// and silently lands in public. Use it for every migrate and validate call.
func RiverMigrator[TTx any](driver riverdriver.Driver[TTx]) (*rivermigrate.Migrator[TTx], error) {
	return dbmigrate.NewRiverMigrator(driver, RiverSchema)
}

// Migrate applies the goose and River migration streams under one lock:
// River does not share goose's.
func Migrate(ctx context.Context, database *sql.DB) error {
	return dbmigrate.WithPostgresSessionLock(ctx, database, migrationLockName, func(ctx context.Context) error {
		provider, err := dbmigrate.NewGooseProvider(database, Migrations(), TrackingTable)
		if err != nil {
			return err
		}
		if _, err := provider.Up(ctx); err != nil {
			return fmt.Errorf("apply migrations: %w", err)
		}

		riverMigrator, err := RiverMigrator(riverdatabasesql.New(database))
		if err != nil {
			return err
		}
		if _, err := riverMigrator.Migrate(ctx, rivermigrate.DirectionUp, nil); err != nil {
			return fmt.Errorf("apply river migrations: %w", err)
		}

		return nil
	})
}
