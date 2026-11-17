package tests

import (
	"database/sql"
	"reflect"
	"testing"

	"github.com/google/uuid"
	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/database"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	dbmodule "github.com/e2b-dev/infra/packages/db"
	authqueries "github.com/e2b-dev/infra/packages/db/pkg/auth/queries"
	"github.com/e2b-dev/infra/packages/db/pkg/dberrors"
	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
	testqueries "github.com/e2b-dev/infra/packages/db/pkg/testutils/queries"
)

const baseDefaultLimitsMigration = 20261111132251

// The lookups repeat their limit columns instead of reading a view, so a limit
// added to default_limits but forgotten in one of them must fail here.
func TestTeamLookupsReturnEveryDefaultLimit(t *testing.T) {
	t.Parallel()

	defaults := reflect.TypeFor[testqueries.DefaultLimit]()
	want := map[string]reflect.Type{}
	for field := range defaults.Fields() {
		if field.Name != "ID" {
			want[field.Name] = field.Type
		}
	}
	// The ceiling is served under its old and new name.
	want["MaxFreeDiskSizeMb"] = want["MaxDiskSizeMb"]

	for _, row := range []reflect.Type{
		reflect.TypeFor[authqueries.GetTeamByAPIKeyRow](),
		reflect.TypeFor[authqueries.GetTeamByTeamAndUserRow](),
		reflect.TypeFor[authqueries.GetTeamByTeamIDRow](),
		reflect.TypeFor[authqueries.GetUserTeamsWithLimitsRow](),
	} {
		for name, typ := range want {
			field, ok := row.FieldByName(name)
			if assert.True(t, ok, "%s has no %s", row.Name(), name) {
				assert.Equal(t, typ, field.Type, "%s.%s", row.Name(), name)
			}
		}
	}
}

// Every lookup resolves limits the same way: the pushed row, else the
// deployment's defaults. The team's tier may not reach them.
func TestTeamLookupsResolveProjectLimitsElseDefaults(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	ctx := t.Context()
	sqlDB, err := sql.Open("pgx", db.ConnStr())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sqlDB.Close()) })

	_, err = sqlDB.ExecContext(ctx, `
		UPDATE public.default_limits SET
			max_length_hours = 101, concurrent_sandboxes = 102, concurrent_template_builds = 103,
			max_vcpu = 104, max_ram_mb = 105, disk_mb = 106, events_ttl_days = 107,
			default_free_disk_size_mb = 108, max_disk_size_mb = 109,
			api_team_rps_list = 110, api_team_rps_delete = 111, max_secrets = 112
	`)
	require.NoError(t, err)
	defaults := map[string]int64{
		"MaxLengthHours": 101, "ConcurrentSandboxes": 102, "ConcurrentTemplateBuilds": 103,
		"MaxVcpu": 104, "MaxRamMb": 105, "DiskMb": 106, "EventsTtlDays": 107,
		"DefaultFreeDiskSizeMb": 108, "MaxDiskSizeMb": 109, "MaxFreeDiskSizeMb": 109,
		"ApiTeamRpsList": 110, "ApiTeamRpsDelete": 111, "MaxSecrets": 112,
	}

	unpushed := seedTeam(t, sqlDB, "lookup-unpushed")
	_, err = sqlDB.ExecContext(ctx, `UPDATE public.teams SET tier = 'lookup_other' WHERE id = $1`, unpushed)
	require.NoError(t, err)
	pushed := seedTeam(t, sqlDB, "lookup-pushed")
	_, err = sqlDB.ExecContext(ctx, `
		INSERT INTO public.project_limits (
			team_id, max_length_hours, concurrent_sandboxes, concurrent_template_builds,
			max_vcpu, max_ram_mb, disk_mb, events_ttl_days,
			default_free_disk_size_mb, max_disk_size_mb, max_free_disk_size_mb,
			api_team_rps_list, api_team_rps_delete, max_secrets
		) VALUES ($1, 1, 2, 3, 4, 5, 6, 7, 8, 9, 1, 0, 11, 12)
	`, pushed)
	require.NoError(t, err)
	pushedLimits := map[string]int64{
		"MaxLengthHours": 1, "ConcurrentSandboxes": 2, "ConcurrentTemplateBuilds": 3,
		"MaxVcpu": 4, "MaxRamMb": 5, "DiskMb": 6, "EventsTtlDays": 7,
		"DefaultFreeDiskSizeMb": 8, "MaxDiskSizeMb": 9, "MaxFreeDiskSizeMb": 9,
		"ApiTeamRpsList": 0, "ApiTeamRpsDelete": 11, "MaxSecrets": 12,
	}

	userID := uuid.New()
	require.NoError(t, db.AuthDB.UpsertPublicUser(ctx, userID))
	for _, teamID := range []uuid.UUID{unpushed, pushed} {
		require.NoError(t, db.AuthDB.CreateTeamMembership(ctx, authqueries.CreateTeamMembershipParams{
			UserID: userID, TeamID: teamID,
		}))
		_, err = db.AuthDB.CreateTeamAPIKey(ctx, authqueries.CreateTeamAPIKeyParams{
			TeamID: teamID, ApiKeyHash: "hash-" + teamID.String(), ApiKeyPrefix: "e2b_", ApiKeyLength: 40,
			ApiKeyMaskPrefix: "ab", ApiKeyMaskSuffix: "yz", Name: "lookup test",
		})
		require.NoError(t, err)
	}

	type lookup func(teamID uuid.UUID) (found bool, row any, err error)
	single := func(row any, err error) (bool, any, error) {
		if dberrors.IsNotFoundError(err) {
			return false, nil, nil
		}

		return err == nil, row, err
	}
	lookups := map[string]lookup{
		"GetTeamByAPIKey": func(teamID uuid.UUID) (bool, any, error) {
			return single(db.AuthDB.GetTeamByAPIKey(ctx, "hash-"+teamID.String()))
		},
		"GetTeamByTeamAndUser": func(teamID uuid.UUID) (bool, any, error) {
			return single(db.AuthDB.GetTeamByTeamAndUser(ctx, authqueries.GetTeamByTeamAndUserParams{UserID: userID, ID: teamID}))
		},
		"GetTeamByTeamID": func(teamID uuid.UUID) (bool, any, error) {
			return single(db.AuthDB.GetTeamByTeamID(ctx, teamID))
		},
		"GetUserTeamsWithLimits": func(teamID uuid.UUID) (bool, any, error) {
			rows, err := db.AuthDB.GetUserTeamsWithLimits(ctx, userID)
			for _, row := range rows {
				if row.ID == teamID {
					return true, row, err
				}
			}

			return false, nil, err
		},
	}

	resolve := func(name string, teamID uuid.UUID) map[string]int64 {
		t.Helper()
		found, row, err := lookups[name](teamID)
		require.NoError(t, err, name)
		require.True(t, found, "%s finds team %s", name, teamID)
		assert.Equal(t, teamID, reflect.ValueOf(row).FieldByName("ID").Interface(), name)

		return limitColumns(row)
	}
	for name := range lookups {
		assert.Equal(t, pushedLimits, resolve(name, pushed), "%s: a pushed row wins, zeros included", name)
		assert.Equal(t, defaults, resolve(name, unpushed), "%s: the defaults, not the tier or add-on", name)
		found, _, err := lookups[name](uuid.New())
		require.NoError(t, err, name)
		assert.False(t, found, "%s: a missing team resolves nothing", name)
	}

	_, err = sqlDB.ExecContext(ctx, `UPDATE public.default_limits SET concurrent_sandboxes = 77`)
	require.NoError(t, err)
	for name := range lookups {
		assert.EqualValues(t, 77, resolve(name, unpushed)["ConcurrentSandboxes"], "%s follows the defaults row", name)
	}
}

// The team lookups cross-join the table: a second row would duplicate every
// team, and no row would leave every team without project_limits unable to
// authenticate.
func TestDefaultLimitsKeepsExactlyOneRow(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	ctx := t.Context()
	sqlDB, err := sql.Open("pgx", db.ConnStr())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sqlDB.Close()) })

	rows := func() int {
		t.Helper()
		var count int
		require.NoError(t, sqlDB.QueryRowContext(ctx, `SELECT count(*) FROM public.default_limits`).Scan(&count))

		return count
	}
	require.Equal(t, 1, rows())

	for _, statement := range []string{
		`INSERT INTO public.default_limits SELECT * FROM public.default_limits`,
		`INSERT INTO public.default_limits (id, max_length_hours, concurrent_sandboxes, concurrent_template_builds,
			max_vcpu, max_ram_mb, disk_mb, events_ttl_days, default_free_disk_size_mb, max_disk_size_mb,
			api_team_rps_list, api_team_rps_delete, max_secrets)
			VALUES (false, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1)`,
		`UPDATE public.default_limits SET id = false`,
		`DELETE FROM public.default_limits`,
		`TRUNCATE public.default_limits`,
		`UPDATE public.default_limits SET concurrent_sandboxes = 0`,
		`UPDATE public.default_limits SET max_secrets = 0`,
		`UPDATE public.default_limits SET api_team_rps_delete = -1`,
		`UPDATE public.default_limits SET default_free_disk_size_mb = max_disk_size_mb + 1`,
	} {
		_, err = sqlDB.ExecContext(ctx, statement)
		require.Error(t, err, statement)
	}

	_, err = sqlDB.ExecContext(ctx, `UPDATE public.default_limits SET concurrent_sandboxes = 64`)
	require.NoError(t, err)
	require.Equal(t, 1, rows())
}

// The seed's API rates of 0 would leave every team without a project_limits
// row unthrottled.
func TestDefaultLimitsStartFromTheBaseLimits(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	sqlDB, err := sql.Open("pgx", db.ConnStr())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sqlDB.Close()) })

	assert.JSONEq(t, `{
		"max_length_hours": 1, "concurrent_sandboxes": 10, "concurrent_template_builds": 20,
		"max_vcpu": 8, "max_ram_mb": 8192, "disk_mb": 10240, "events_ttl_days": 7,
		"default_free_disk_size_mb": 10240, "max_disk_size_mb": 25600,
		"api_team_rps_list": 10, "api_team_rps_delete": 20, "max_secrets": 10
	}`, defaultLimitsRow(t, sqlDB))
}

// Only a column still at its seed value moves, so a deployment keeps the
// defaults it configured.
func TestBaseDefaultLimitsMigrationKeepsConfiguredValues(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	ctx := t.Context()
	sqlDB, err := sql.Open("pgx", db.ConnStr())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sqlDB.Close()) })
	provider := migrationProvider(t, sqlDB)

	_, err = provider.DownTo(ctx, baseDefaultLimitsMigration-1)
	require.NoError(t, err)
	_, err = sqlDB.ExecContext(ctx, `
		UPDATE public.default_limits SET
			concurrent_sandboxes = 64, disk_mb = 512, default_free_disk_size_mb = 512,
			max_disk_size_mb = 4096, api_team_rps_list = 0, api_team_rps_delete = 35
	`)
	require.NoError(t, err)

	_, err = provider.UpTo(ctx, baseDefaultLimitsMigration)
	require.NoError(t, err)
	assert.JSONEq(t, `{
		"max_length_hours": 1, "concurrent_sandboxes": 64, "concurrent_template_builds": 20,
		"max_vcpu": 8, "max_ram_mb": 8192, "disk_mb": 10240, "events_ttl_days": 7,
		"default_free_disk_size_mb": 512, "max_disk_size_mb": 4096,
		"api_team_rps_list": 10, "api_team_rps_delete": 35, "max_secrets": 10
	}`, defaultLimitsRow(t, sqlDB), "free disk stays under the configured ceiling")
}

func defaultLimitsRow(t *testing.T, sqlDB *sql.DB) string {
	t.Helper()

	var row string
	require.NoError(t, sqlDB.QueryRowContext(t.Context(),
		`SELECT (to_jsonb(d) - 'id')::text FROM public.default_limits d`).Scan(&row))

	return row
}

// limitColumns reads a lookup row's limit columns, its int64 fields, by name.
func limitColumns(row any) map[string]int64 {
	value := reflect.ValueOf(row)
	limits := map[string]int64{}
	for i := range value.NumField() {
		if field := value.Field(i); field.Kind() == reflect.Int64 {
			limits[value.Type().Field(i).Name] = field.Int()
		}
	}

	return limits
}

func migrationProvider(t *testing.T, sqlDB *sql.DB) *goose.Provider {
	t.Helper()

	store, err := database.NewStore(goose.DialectPostgres, testutils.TrackingTable)
	require.NoError(t, err)
	provider, err := goose.NewProvider("", sqlDB, dbmodule.Migrations(), goose.WithStore(store))
	require.NoError(t, err)

	return provider
}
