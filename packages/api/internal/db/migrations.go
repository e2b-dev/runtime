// Package db carries the schema migrations of the shared PostgreSQL database
// in the binary, so tests and tools apply the same files the migrator applies
// without reading them from a checkout.
package db

import (
	"embed"
	"fmt"
	"io/fs"
)

//go:embed migrations/*.sql
var embeddedMigrations embed.FS

// Migrations returns the goose migration files rooted like the directory the
// migrator reads, so a provider over either sees the same set.
func Migrations() fs.FS {
	sub, err := fs.Sub(embeddedMigrations, "migrations")
	if err != nil {
		// Unreachable: the directory is embedded at compile time.
		panic(fmt.Errorf("embedded migrations missing: %w", err))
	}

	return sub
}
