package testutils

import (
	"context"
	"database/sql"
	"io/fs"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib" // this allows goose to function
	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/database"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

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
	TrackingTable = "_migrations"

	testPostgresImage = "postgres:18-alpine"
	testDatabaseName  = "test_db"
	testUsername      = "postgres"
	testPassword      = "test_password"
)

// Database encapsulates the test database container and clients
type Database struct {
	SqlcClient  *db.Client
	AuthDb      *authdb.Client
	AuthDB      *authdb.Client
	TestQueries *queries.Queries
	connStr     string
}

// SetupDatabase creates a fresh PostgreSQL container with migrations applied
func SetupDatabase(t *testing.T) *Database {
	t.Helper()

	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Start PostgreSQL container
	container, err := postgres.Run(
		t.Context(),
		testPostgresImage,
		postgres.WithDatabase(testDatabaseName),
		postgres.WithUsername(testUsername),
		postgres.WithPassword(testPassword),
		testcontainers.WithCmdArgs("-c", "fsync=off", "-c", "synchronous_commit=off", "-c", "full_page_writes=off"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(60*time.Second),
		),
	)
	require.NoError(t, err, "Failed to start postgres container")
	t.Cleanup(func() {
		ctx := t.Context()
		ctx = context.WithoutCancel(ctx)
		err := container.Terminate(ctx)
		assert.NoError(t, err)
	})

	connStr, err := container.ConnectionString(t.Context(), "sslmode=disable")
	require.NoError(t, err, "Failed to get connection string")

	// Setup environment and run migrations
	runDatabaseMigrations(t, connStr)

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

// ApplyMigrations applies each goose migration stream to the database, in
// order. Streams come from the binary (dbmodule.Migrations) rather than from
// a directory on disk: go test caches a result by the inputs the binary
// observed, and a file outside the test's own module is not one of them, so
// a schema edit read from a checkout would leave dependent modules' cached
// passes standing.
func (db *Database) ApplyMigrations(t *testing.T, migrations ...fs.FS) {
	t.Helper()

	db.applyGooseMigrations(t, migrations...)
}

func (db *Database) ConnStr() string {
	return db.connStr
}

func (db *Database) applyGooseMigrations(t *testing.T, migrations ...fs.FS) {
	t.Helper()

	sqlDB, err := sql.Open("pgx", db.connStr)
	require.NoError(t, err)
	t.Cleanup(func() {
		err := sqlDB.Close()
		assert.NoError(t, err)
	})

	// A provider per directory, each carrying its own store, so nothing here
	// depends on goose's package-level dialect and tracking-table globals. That
	// is what the mutex this replaced was guarding: parallel tests raced on
	// those globals, and the race detector caught it on ARM64.
	store, err := database.NewStore(goose.DialectPostgres, TrackingTable)
	require.NoError(t, err)

	for _, stream := range migrations {
		provider, err := goose.NewProvider(
			"", // Has to be empty when using a custom store
			sqlDB,
			stream,
			goose.WithStore(store),
		)
		require.NoError(t, err)

		_, err = provider.Up(t.Context())
		require.NoError(t, err)
	}
}

// runDatabaseMigrations executes all required database migrations
func runDatabaseMigrations(t *testing.T, connStr string) {
	t.Helper()

	db := &Database{connStr: connStr}
	db.ApplyMigrations(t, dbmodule.Migrations())
}
