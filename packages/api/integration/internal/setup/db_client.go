package setup

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/db/client"
	authdb "github.com/e2b-dev/infra/packages/api/internal/db/pkg/auth"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

type Database struct {
	Db     *client.Client
	AuthDb *authdb.Client
}

func GetTestDBClient(tb testing.TB) *Database {
	tb.Helper()

	databaseURL := utils.RequiredEnv("POSTGRES_CONNECTION_STRING", "Postgres connection string")

	db, err := client.NewClient(tb.Context(), databaseURL)
	require.NoError(tb, err)

	authDb, err := authdb.NewClient(tb.Context(), databaseURL)
	require.NoError(tb, err)

	tb.Cleanup(func() {
		db.Close()
		authDb.Close()
	})

	return &Database{
		Db:     db,
		AuthDb: authDb,
	}
}
