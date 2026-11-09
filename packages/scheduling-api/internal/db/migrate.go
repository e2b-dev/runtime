// Package db holds the versioned schema of the scheduling database.
package db

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"io/fs"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"

	"github.com/e2b-dev/infra/packages/shared/pkg/dbmigrate"
)

// TrackingTable is the goose migration ledger.
const TrackingTable = "public.scheduling_migrations"

//go:embed migrations/*.sql
var embeddedMigrations embed.FS

// Migrations returns the goose migration files.
func Migrations() fs.FS {
	sub, err := fs.Sub(embeddedMigrations, "migrations")
	if err != nil {
		panic(fmt.Errorf("embedded migrations missing: %w", err))
	}

	return sub
}

// Open returns a handle for Migrate.
func Open(databaseURL string) (*sql.DB, error) {
	config, err := pgx.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse the database URL: %w", err)
	}

	return stdlib.OpenDB(*config), nil
}

// Migrate applies every pending migration. Concurrent runs are safe.
func Migrate(ctx context.Context, database *sql.DB) error {
	provider, err := dbmigrate.NewGooseProvider(database, Migrations(), TrackingTable)
	if err != nil {
		return err
	}
	if _, err := provider.Up(ctx); err != nil {
		return fmt.Errorf("apply migrations: %w", err)
	}

	return nil
}
