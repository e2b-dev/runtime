package client_test

import (
	"database/sql"
	"testing"

	"github.com/riverqueue/river/riverdriver/riverdatabasesql"
	"github.com/riverqueue/river/rivermigrate"
	"github.com/stretchr/testify/require"

	dbmodule "github.com/e2b-dev/infra/packages/db"
	"github.com/e2b-dev/infra/packages/db/client"
	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
)

func TestCheckMigrationVersionAcceptsAMigratedDatabase(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)

	require.NoError(t, client.CheckMigrationVersion(t.Context(), db.ConnStr(), 1))
}

func TestCheckMigrationVersionRefusesADatabaseWithoutRiverMigrations(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	sqlDB, err := sql.Open("pgx", db.ConnStr())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sqlDB.Close()) })
	riverMigrator, err := dbmodule.RiverMigrator(riverdatabasesql.New(sqlDB))
	require.NoError(t, err)
	_, err = riverMigrator.Migrate(t.Context(), rivermigrate.DirectionDown, &rivermigrate.MigrateOpts{TargetVersion: -1})
	require.NoError(t, err)

	err = client.CheckMigrationVersion(t.Context(), db.ConnStr(), 1)

	require.ErrorContains(t, err, "river")
}
