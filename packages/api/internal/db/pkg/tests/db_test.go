package tests

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/db/pkg/testutils"
)

func TestNoRowLevelSecurity(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)

	results, err := db.TestQueries.GetRowLevelSecurity(t.Context())
	require.NoError(t, err)
	assert.NotEmpty(t, results)

	var checked int
	for _, item := range results {
		if item.NamespaceName != "public" {
			continue
		}
		if item.Kind == "v" {
			continue
		}

		checked++

		assert.Falsef(t, item.RowLevelSecurity, "database object %s.%s still has row level security enabled [%s]", item.NamespaceName, item.TableName, item.Kind)
	}
	assert.NotEmpty(t, checked)

	sqlDB, err := sql.Open("pgx", db.ConnStr())
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, sqlDB.Close())
	})

	rows, err := sqlDB.QueryContext(t.Context(), `
		SELECT policyname
		FROM pg_policies
		WHERE schemaname = 'public'
	`)
	require.NoError(t, err)
	defer rows.Close()

	var policies []string
	for rows.Next() {
		var policy string
		require.NoError(t, rows.Scan(&policy))
		policies = append(policies, policy)
	}
	require.NoError(t, rows.Err())

	assert.Empty(t, policies, "public RLS policies should not be managed by default")
}
