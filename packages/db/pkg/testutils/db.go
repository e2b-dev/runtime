package testutils

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib" // also registers the "pgx" driver callers open with database/sql
	"github.com/riverqueue/river/riverdriver/riverdatabasesql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.segfaultmedaddy.com/pgxephemeraltest/v2"
	"go.segfaultmedaddy.com/pgxephemeraltest/v2/pkg/migrator"

	dbmodule "github.com/e2b-dev/infra/packages/db"
	db "github.com/e2b-dev/infra/packages/db/client"
	authdb "github.com/e2b-dev/infra/packages/db/pkg/auth"
	"github.com/e2b-dev/infra/packages/db/pkg/pool"
	"github.com/e2b-dev/infra/packages/db/pkg/testutils/queries"
)

const (
	// TrackingTable is goose's bookkeeping table for this database. Exported
	// because migration tests step the schema through their own provider and
	// must target the same table the harness migrated.
	TrackingTable = dbmodule.TrackingTable

	// EnvDatabaseURL names the environment variable carrying the URL of the
	// PostgreSQL server the tests share. Each test gets its own database on
	// it, copied from a migrated template.
	EnvDatabaseURL = "PG_TEST_DATABASE_URL"
)

// migratedPoolFactory is created lazily: the template is migrated when the first test
// asks for a database, so a test binary that never does needs no server.
var migratedPoolFactory = newPoolFactory()

// Database encapsulates the test database and clients
type Database struct {
	SqlcClient  *db.Client
	AuthDb      *authdb.Client
	AuthDB      *authdb.Client
	TestQueries *queries.Queries
	connStr     string
}

// SetupDatabase creates a fresh database with migrations applied on the
// server EnvDatabaseURL names
func SetupDatabase(t *testing.T) *Database {
	t.Helper()

	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	require.NotEmpty(t, os.Getenv(EnvDatabaseURL), "%s must name a PostgreSQL server for the tests", EnvDatabaseURL)

	// The database is dropped when the test ends, after the clients below
	// are closed: cleanups run in reverse order.
	connStr := connString(t, migratedPoolFactory.Pool(t))

	// create test queries client
	dbClient, connPool, err := pool.New(t.Context(), connStr, "tests")
	require.NoError(t, err)
	t.Cleanup(func() {
		connPool.Close()
	})
	testQueries := queries.New(dbClient)

	// Create app db client
	sqlcClient, err := db.NewClient(t.Context(), connStr)
	require.NoError(t, err, "Failed to create sqlc client")
	t.Cleanup(func() {
		err := sqlcClient.Close()
		assert.NoError(t, err)
	})

	// Create the auth db client
	authDB, err := authdb.NewClient(t.Context(), connStr)
	require.NoError(t, err, "Failed to create auth db client")
	t.Cleanup(func() {
		err := authDB.Close()
		assert.NoError(t, err)
	})

	return &Database{
		SqlcClient:  sqlcClient,
		AuthDb:      authDB,
		AuthDB:      authDB,
		TestQueries: testQueries,
		connStr:     connStr,
	}
}

func (db *Database) ConnStr() string {
	return db.connStr
}

func newPoolFactory() *pgxephemeraltest.PoolFactory {
	config, err := pgxpool.ParseConfig(os.Getenv(EnvDatabaseURL))
	if err != nil {
		panic(fmt.Errorf("parse %s: %w", EnvDatabaseURL, err))
	}
	// The pool only hands out the database's address; the clients open their own.
	config.MaxConns = 1

	factory, err := pgxephemeraltest.NewPoolFactory(context.Background(), config, newMigrator(),
		pgxephemeraltest.WithLazyInitialization(true))
	if err != nil {
		panic(fmt.Errorf("create the pool factory: %w", err))
	}

	return factory
}

// connString returns a URL for the database p is connected to.
func connString(t *testing.T, p *pgxpool.Pool) string {
	t.Helper()

	config := p.Config()
	u, err := url.Parse(config.ConnString())
	require.NoError(t, err, "parse the pool's connection string")
	u.Path = "/" + config.ConnConfig.Database

	return u.String()
}

// templateMigrator applies the schema the way the migrator does, from the
// copy embedded in the binary rather than a directory on disk: go test caches
// a result by the inputs the binary observed, and a file outside the test's
// own module is not one of them, so a schema edit read from a checkout would
// leave dependent modules' cached passes standing.
//
// The template is named by a hash of the migration files and by River's newest
// migration, which is compiled in rather than read from a file, so an edited
// or added migration, or a River upgrade, builds a new template rather than
// reusing a stale one.
type templateMigrator struct {
	hash string
}

var _ pgxephemeraltest.Migrator = (*templateMigrator)(nil)

func newMigrator() *templateMigrator {
	files, err := migrator.FromFS(dbmodule.Migrations())
	if err != nil {
		// Unreachable: the migrations are embedded at compile time.
		panic(fmt.Errorf("hash the migrations: %w", err))
	}

	return &templateMigrator{hash: fmt.Sprintf("db_%s_river_%d", files.Hash(), riverVersion())}
}

func riverVersion() int {
	river, err := dbmodule.RiverMigrator(riverdatabasesql.New(nil))
	if err != nil {
		panic(fmt.Errorf("read River's migrations: %w", err))
	}
	versions := river.AllVersions()

	return versions[len(versions)-1].Version
}

func (m *templateMigrator) Hash() string { return m.hash }

// Migrate opens its own connections to the template: goose needs a *sql.DB.
// Two, because the migration lock holds one session while goose runs on the
// other. They are closed before returning, since PostgreSQL refuses to copy a
// template with open sessions.
func (m *templateMigrator) Migrate(ctx context.Context, conn *pgx.Conn) error {
	sqlDB := stdlib.OpenDB(*conn.Config())
	sqlDB.SetMaxOpenConns(2)

	err := dbmodule.Migrate(ctx, sqlDB)

	return errors.Join(err, sqlDB.Close())
}
