package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	_ "github.com/lib/pq"

	"github.com/e2b-dev/infra/packages/api/internal/db"
	"github.com/e2b-dev/infra/packages/shared/pkg/dbmigrate"
)

const statementTimeout = 3 * time.Hour

func main() {
	fmt.Printf("Starting migrations...\n")
	ctx := context.Background()

	dbString := os.Getenv("POSTGRES_CONNECTION_STRING")
	if dbString == "" {
		log.Fatal("Database connection string is required. Set POSTGRES_CONNECTION_STRING env var.")
	}

	poolConfig, err := pgxpool.ParseConfig(dbString)
	if err != nil {
		log.Fatalf("failed to parse connection string: %v", err)
	}

	poolConfig.MaxConns = 4
	poolConfig.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		_, err := conn.Exec(ctx, fmt.Sprintf("SET statement_timeout = %d", statementTimeout.Milliseconds()))

		return err
	}
	poolConfig.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeExec

	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		log.Fatalf("failed to create connection pool: %v", err)
	}
	defer pool.Close()

	// Convert pgxpool to *sql.DB for goose compatibility
	sqlDB := stdlib.OpenDBFromPool(pool)

	provider, err := dbmigrate.NewGooseProvider(sqlDB, db.Migrations(), db.TrackingTable)
	if err != nil {
		log.Fatalf("failed to create goose provider: %v", err) //nolint:gocritic // process exits, db cleanup not critical
	}

	version, err := provider.GetDBVersion(ctx)
	if err != nil {
		log.Fatalf("failed to get current DB version: %v", err)
	}

	fmt.Printf("Current DB version: %d\n", version)

	if err := db.Migrate(ctx, sqlDB); err != nil {
		log.Fatalf("failed to apply migrations: %v", err)
	}

	fmt.Println("Migrations applied successfully.")
}
