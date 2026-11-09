package dbtest

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/require"
	"go.segfaultmedaddy.com/pgxephemeraltest/v2"
	"go.segfaultmedaddy.com/pgxephemeraltest/v2/pkg/migrator"

	"github.com/e2b-dev/infra/packages/scheduling-api/internal/db"
)

const EnvDatabaseURL = "PG_TEST_DATABASE_URL"

var (
	emptyPoolFactory    = newPoolFactory(emptyTemplate{})
	migratedPoolFactory = newPoolFactory(newMigratedTemplate())
)

func EmptyPool(t *testing.T) *pgxpool.Pool {
	t.Helper()

	return testPool(t, emptyPoolFactory)
}

func MigratedPool(t *testing.T) *pgxpool.Pool {
	t.Helper()

	return testPool(t, migratedPoolFactory)
}

func ConnString(t *testing.T, pool *pgxpool.Pool) string {
	t.Helper()

	config := pool.Config()
	u, err := url.Parse(config.ConnString())
	require.NoError(t, err)
	u.Path = "/" + config.ConnConfig.Database

	return u.String()
}

func testPool(t *testing.T, factory *pgxephemeraltest.PoolFactory) *pgxpool.Pool {
	t.Helper()

	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}
	require.NotEmpty(t, os.Getenv(EnvDatabaseURL), "%s must name a PostgreSQL server for the tests", EnvDatabaseURL)

	return factory.Pool(t)
}

func newPoolFactory(template pgxephemeraltest.Migrator) *pgxephemeraltest.PoolFactory {
	config, err := pgxpool.ParseConfig(os.Getenv(EnvDatabaseURL))
	if err != nil {
		panic(fmt.Errorf("parse %s: %w", EnvDatabaseURL, err))
	}
	config.MaxConns = 8

	factory, err := pgxephemeraltest.NewPoolFactory(context.Background(), config, template,
		pgxephemeraltest.WithLazyInitialization(true))
	if err != nil {
		panic(fmt.Errorf("create the pool factory: %w", err))
	}

	return factory
}

type emptyTemplate struct{}

func (emptyTemplate) Hash() string { return "scheduling_empty" }

func (emptyTemplate) Migrate(context.Context, *pgx.Conn) error { return nil }

type migratedTemplate struct {
	hash string
}

func newMigratedTemplate() migratedTemplate {
	files, err := migrator.FromFS(db.Migrations())
	if err != nil {
		panic(fmt.Errorf("hash the migrations: %w", err))
	}

	return migratedTemplate{hash: "scheduling_" + files.Hash()}
}

func (m migratedTemplate) Hash() string { return m.hash }

// Migrate opens its own connections and closes them before returning:
// PostgreSQL refuses to copy a template with open sessions.
func (migratedTemplate) Migrate(ctx context.Context, conn *pgx.Conn) error {
	database := stdlib.OpenDB(*conn.Config())

	return errors.Join(db.Migrate(ctx, database), database.Close())
}
