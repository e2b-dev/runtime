package tests

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/database"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
)

const dashboardSchemaVersion int64 = 20260915204740

func TestDashboardSchemaMigration(t *testing.T) {
	t.Parallel()

	for _, state := range []string{"fresh", "upgrade_without_dashboard", "upgrade_with_dashboard"} {
		t.Run(state, func(t *testing.T) {
			t.Parallel()

			db := testutils.SetupDatabase(t)
			ctx := t.Context()
			sqlDB, err := sql.Open("pgx", db.ConnStr())
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, sqlDB.Close()) })

			newProvider := func(dir, table string) *goose.Provider {
				t.Helper()
				store, err := database.NewStore(goose.DialectPostgres, table)
				require.NoError(t, err)
				provider, err := goose.NewProvider("", sqlDB, os.DirFS(dir), goose.WithStore(store))
				require.NoError(t, err)

				return provider
			}
			migrations := newProvider(filepath.Join("..", "..", "migrations"), testutils.TrackingTable)
			withDashboard := state == "upgrade_with_dashboard"

			if state != "fresh" {
				_, err = migrations.DownTo(ctx, dashboardSchemaVersion-1)
				require.NoError(t, err)

				// Down preserves the table; drop it to reconstruct the pre-upgrade schema.
				_, err = sqlDB.ExecContext(ctx, `DROP TABLE public.env_defaults`)
				require.NoError(t, err)

				if withDashboard {
					legacy := newProvider(filepath.Join("testdata", "legacy_dashboard"), "_dashboard_migrations")
					_, err = legacy.Up(ctx)
					require.NoError(t, err)
				}
			}

			teamID := testutils.CreateTestTeam(t, db)
			templateID := testutils.CreateTestTemplate(t, db, teamID)
			const description = "Default template"

			seedDashboardData := func() {
				t.Helper()
				_, err := sqlDB.ExecContext(ctx,
					`INSERT INTO public.env_defaults (env_id, description) VALUES ($1, $2)`,
					templateID, description)
				require.NoError(t, err)
			}
			if withDashboard {
				seedDashboardData()
			}

			if state != "fresh" {
				results, err := migrations.UpTo(ctx, dashboardSchemaVersion)
				require.NoError(t, err)
				require.Len(t, results, 1)
				require.Equal(t, dashboardSchemaVersion, results[0].Source.Version)
			}
			if !withDashboard {
				seedDashboardData()
			}

			assertDashboardData := func() {
				t.Helper()
				var gotDescription string
				err := sqlDB.QueryRowContext(ctx, `
					SELECT d.description
					FROM public.env_defaults d
					JOIN public.envs e ON e.id = d.env_id
					WHERE d.env_id = $1
				`, templateID).Scan(&gotDescription)
				require.NoError(t, err)
				require.Equal(t, description, gotDescription)
			}
			assertDashboardData()

			var applied bool
			err = sqlDB.QueryRowContext(ctx,
				`SELECT is_applied FROM public._migrations WHERE version_id = $1`, dashboardSchemaVersion).Scan(&applied)
			require.NoError(t, err)
			require.True(t, applied)

			_, err = migrations.DownTo(ctx, dashboardSchemaVersion-1)
			require.NoError(t, err)
			assertDashboardData()
			_, err = migrations.UpTo(ctx, dashboardSchemaVersion)
			require.NoError(t, err)
			assertDashboardData()
			results, err := migrations.UpTo(ctx, dashboardSchemaVersion)
			require.NoError(t, err)
			require.Empty(t, results)

			var legacyTable sql.NullString
			err = sqlDB.QueryRowContext(ctx, `SELECT to_regclass('public._dashboard_migrations')::text`).Scan(&legacyTable)
			require.NoError(t, err)
			require.Equal(t, withDashboard, legacyTable.Valid)
			if withDashboard {
				var legacyVersion int64
				err = sqlDB.QueryRowContext(ctx,
					`SELECT MAX(version_id) FROM public._dashboard_migrations WHERE is_applied`).Scan(&legacyVersion)
				require.NoError(t, err)
				require.Equal(t, int64(20260316130000), legacyVersion)
			}
		})
	}
}
