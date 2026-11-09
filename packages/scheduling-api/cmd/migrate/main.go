// Command migrate applies the scheduling database migrations and exits.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/e2b-dev/infra/packages/scheduling-api/internal/config"
	"github.com/e2b-dev/infra/packages/scheduling-api/internal/db"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "scheduling-api migrate: %v\n", err)
		os.Exit(1)
	}
}

func run() (err error) {
	cfg, err := config.ParseMigrator()
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	database, err := db.Open(cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, database.Close()) }()

	return db.Migrate(ctx, database)
}
