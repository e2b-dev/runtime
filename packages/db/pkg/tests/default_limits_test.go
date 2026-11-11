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

const (
	defaultLimitsMigration     = 20261019080937
	baseDefaultLimitsMigration = 20261111132251
)

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
// deployment's defaults. Neither the team's tier nor an active add-on may
// reach them.
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
	seedAddon(t, sqlDB, unpushed)
	_, err = sqlDB.ExecContext(ctx, `
		INSERT INTO public.tiers (id, name, disk_mb, concurrent_instances, max_length_hours,
			default_free_disk_size_mb, max_disk_size_mb, api_team_rps_list, api_team_rps_delete, max_secrets)
		VALUES ('lookup_other', 'Other', 2048, 500, 24, 1024, 51200, 50, 60, 70)
	`)
	require.NoError(t, err)
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

// The team lookups stop reading tiers and add-ons, so the migration pins every
// team without a row to what team_limits resolved for it, and starts the
// defaults from the deployment's own base tier.
func TestDefaultLimitsMigrationKeepsWhatEveryTeamResolvedTo(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	ctx := t.Context()
	sqlDB, err := sql.Open("pgx", db.ConnStr())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sqlDB.Close()) })
	provider := migrationProvider(t, sqlDB)

	_, err = provider.DownTo(ctx, defaultLimitsMigration-1)
	require.NoError(t, err)

	_, err = sqlDB.ExecContext(ctx, `
		UPDATE public.tiers
		SET concurrent_instances = 33, max_vcpu = 3, disk_mb = 2048, default_free_disk_size_mb = 1024,
			api_team_rps_list = 7, api_team_rps_delete = 9, max_secrets = 4
		WHERE id = 'base_v1'
	`)
	require.NoError(t, err)

	onBase := seedTeam(t, sqlDB, "pinned-base")
	onOtherTierWithAddons := seedTeam(t, sqlDB, "pinned-other-tier-addons")
	_, err = sqlDB.ExecContext(ctx, `
		INSERT INTO public.tiers (id, name, disk_mb, concurrent_instances, max_length_hours, max_vcpu, max_ram_mb,
			concurrent_template_builds, events_ttl_days, default_free_disk_size_mb, max_disk_size_mb,
			api_team_rps_list, api_team_rps_delete, max_secrets)
		VALUES ('pinned_other', 'Other', 20480, 100, 24, 16, 16384, 21, 30, 20480, 51200, 20, 100, 100)
	`)
	require.NoError(t, err)
	_, err = sqlDB.ExecContext(ctx, `UPDATE public.teams SET tier = 'pinned_other' WHERE id = $1`, onOtherTierWithAddons)
	require.NoError(t, err)
	_, err = sqlDB.ExecContext(ctx, `
		INSERT INTO public.addons (
			team_id, name, extra_concurrent_sandboxes, extra_concurrent_template_builds,
			extra_max_vcpu, extra_max_ram_mb, extra_disk_mb, extra_events_ttl_days,
			extra_max_disk_size_mb, extra_api_team_rps_list, extra_api_team_rps_delete, extra_max_secrets,
			valid_from, valid_to, added_by
		) VALUES
		($1, 'active', 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, now() - interval '1 hour', NULL, $2),
		($1, 'expired', 100, 100, 100, 100, 100, 100, 100, 100, 100, 100,
		 now() - interval '2 hours', now() - interval '1 hour', $2)
	`, onOtherTierWithAddons, seedUser(t, sqlDB))
	require.NoError(t, err)
	pushed := seedTeam(t, sqlDB, "pinned-pushed")
	_, err = sqlDB.ExecContext(ctx, `
		INSERT INTO public.project_limits (
			team_id, max_length_hours, concurrent_sandboxes, concurrent_template_builds,
			max_vcpu, max_ram_mb, disk_mb, events_ttl_days, default_free_disk_size_mb, max_disk_size_mb,
			api_team_rps_list, api_team_rps_delete, max_secrets, updated_at
		) VALUES ($1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, '2026-01-01T00:00:00Z')
	`, pushed)
	require.NoError(t, err)
	teams := []uuid.UUID{onBase, onOtherTierWithAddons, pushed}

	count := func(query string, args ...any) int {
		t.Helper()
		var n int
		require.NoError(t, sqlDB.QueryRowContext(ctx, query, args...).Scan(&n))

		return n
	}
	pushedRow := func() string {
		t.Helper()
		var row string
		require.NoError(t, sqlDB.QueryRowContext(ctx,
			`SELECT to_jsonb(pl)::text FROM public.project_limits pl WHERE team_id = $1`, pushed).Scan(&row))

		return row
	}
	const unpinned = `SELECT count(*) FROM public.teams t
		WHERE NOT EXISTS (SELECT 1 FROM public.project_limits pl WHERE pl.team_id = t.id)`
	const stored = `SELECT count(*) FROM public.project_limits`

	before := viewLimits(t, sqlDB, teams)
	require.Len(t, before, len(teams))
	assert.EqualValues(t, 100+5, before[onOtherTierWithAddons]["ConcurrentSandboxes"], "the view sums the tier and active add-ons")
	pushedBefore := pushedRow()
	missing := count(unpinned)
	storedBefore := count(stored)
	assert.Equal(t, 2, missing, "the two teams seeded without a row")

	// Apply the latest schema before using generated queries.
	_, err = provider.Up(ctx)
	require.NoError(t, err)

	assert.Equal(t, before, queryLimits(t, db, teams), "no team's limits move")
	assert.Zero(t, count(unpinned))
	assert.Equal(t, storedBefore+missing, count(stored))
	assert.JSONEq(t, pushedBefore, pushedRow(), "a pushed row is left alone")
	assert.Zero(t, count(`SELECT count(*) FROM projection.project_limits`), "the ledger is not advanced")
	assert.Zero(t, count(`SELECT count(*) FROM public.project_limits
		WHERE max_free_disk_size_mb IS DISTINCT FROM max_disk_size_mb AND team_id <> $1`, pushed))

	var defaults string
	require.NoError(t, sqlDB.QueryRowContext(ctx,
		`SELECT (to_jsonb(d) - 'id')::text FROM public.default_limits d`).Scan(&defaults))
	assert.JSONEq(t, `{
		"max_length_hours": 1, "concurrent_sandboxes": 33, "concurrent_template_builds": 20,
		"max_vcpu": 3, "max_ram_mb": 8192, "disk_mb": 2048, "events_ttl_days": 7,
		"default_free_disk_size_mb": 1024, "max_disk_size_mb": 25600,
		"api_team_rps_list": 7, "api_team_rps_delete": 9, "max_secrets": 4
	}`, defaults, "seeded from the tuned base tier")

	_, err = sqlDB.ExecContext(ctx, `UPDATE public.default_limits SET concurrent_sandboxes = 77`)
	require.NoError(t, err)
	assert.Equal(t, before, queryLimits(t, db, teams), "pinned teams do not follow the defaults")
	newTeam := seedTeam(t, sqlDB, "after-defaults")
	row, err := db.AuthDB.GetTeamByTeamID(ctx, newTeam)
	require.NoError(t, err)
	assert.EqualValues(t, 77, row.ConcurrentSandboxes, "a new team follows them")

	_, err = provider.DownTo(ctx, defaultLimitsMigration-1)
	require.NoError(t, err)
	var table *string
	require.NoError(t, sqlDB.QueryRowContext(ctx, `SELECT to_regclass('public.default_limits')::text`).Scan(&table))
	assert.Nil(t, table)
	assert.Equal(t, 1, count(unpinned), "down keeps the rows it pinned")
	_, err = provider.Up(ctx)
	require.NoError(t, err)
	assert.Zero(t, count(unpinned))
}

// Clamping would change the team's limits silently; failing leaves the
// deployment on the previous release until someone looks.
func TestDefaultLimitsMigrationFailsOnATeamItCannotPin(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	ctx := t.Context()
	sqlDB, err := sql.Open("pgx", db.ConnStr())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sqlDB.Close()) })
	provider := migrationProvider(t, sqlDB)

	_, err = provider.DownTo(ctx, defaultLimitsMigration-1)
	require.NoError(t, err)
	teamID := seedTeam(t, sqlDB, "unpinnable")
	_, err = sqlDB.ExecContext(ctx, `
		INSERT INTO public.addons (team_id, name, extra_disk_mb, extra_max_disk_size_mb, added_by)
		VALUES ($1, 'free disk above the ceiling', 30000, 0, $2)
	`, teamID, seedUser(t, sqlDB))
	require.NoError(t, err)

	_, err = provider.UpTo(ctx, defaultLimitsMigration)
	require.Error(t, err)
	assertNoDefaultLimitsTable(t, sqlDB)
}

func TestDefaultLimitsMigrationFailsWithoutTheBaseTier(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	ctx := t.Context()
	sqlDB, err := sql.Open("pgx", db.ConnStr())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sqlDB.Close()) })
	provider := migrationProvider(t, sqlDB)

	_, err = provider.DownTo(ctx, defaultLimitsMigration-1)
	require.NoError(t, err)
	// Every database has a system team, on base_v1.
	_, err = sqlDB.ExecContext(ctx, `
		INSERT INTO public.tiers (id, name, disk_mb, concurrent_instances, max_length_hours,
			default_free_disk_size_mb, max_disk_size_mb)
		VALUES ('not_base', 'Not base', 512, 20, 1, 512, 25600)
	`)
	require.NoError(t, err)
	_, err = sqlDB.ExecContext(ctx, `UPDATE public.teams SET tier = 'not_base'`)
	require.NoError(t, err)
	_, err = sqlDB.ExecContext(ctx, `DELETE FROM public.tiers WHERE id = 'base_v1'`)
	require.NoError(t, err)

	_, err = provider.UpTo(ctx, defaultLimitsMigration)
	require.ErrorContains(t, err, "tier base_v1 is missing")
	assertNoDefaultLimitsTable(t, sqlDB)
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

func assertNoDefaultLimitsTable(t *testing.T, sqlDB *sql.DB) {
	t.Helper()

	var table *string
	require.NoError(t, sqlDB.QueryRowContext(t.Context(), `SELECT to_regclass('public.default_limits')::text`).Scan(&table))
	assert.Nil(t, table, "the migration rolls back whole")
}

func viewLimits(t *testing.T, sqlDB *sql.DB, teamIDs []uuid.UUID) map[uuid.UUID]map[string]int64 {
	t.Helper()

	ids := make([]string, 0, len(teamIDs))
	for _, id := range teamIDs {
		ids = append(ids, id.String())
	}
	rows, err := sqlDB.QueryContext(t.Context(), `
		SELECT id, max_length_hours, concurrent_sandboxes, concurrent_template_builds,
		       max_vcpu, max_ram_mb, disk_mb, events_ttl_days,
		       default_free_disk_size_mb, max_disk_size_mb, max_free_disk_size_mb,
		       api_team_rps_list, api_team_rps_delete, max_secrets
		FROM public.team_limits
		WHERE id::text = ANY($1)`, ids)
	require.NoError(t, err)
	defer rows.Close()

	limits := map[uuid.UUID]map[string]int64{}
	for rows.Next() {
		var row authqueries.GetTeamByTeamIDRow
		require.NoError(t, rows.Scan(&row.ID, &row.MaxLengthHours, &row.ConcurrentSandboxes,
			&row.ConcurrentTemplateBuilds, &row.MaxVcpu, &row.MaxRamMb, &row.DiskMb, &row.EventsTtlDays,
			&row.DefaultFreeDiskSizeMb, &row.MaxDiskSizeMb, &row.MaxFreeDiskSizeMb,
			&row.ApiTeamRpsList, &row.ApiTeamRpsDelete, &row.MaxSecrets))
		limits[row.ID] = limitColumns(row)
	}
	require.NoError(t, rows.Err())

	return limits
}

func queryLimits(t *testing.T, db *testutils.Database, teamIDs []uuid.UUID) map[uuid.UUID]map[string]int64 {
	t.Helper()

	limits := make(map[uuid.UUID]map[string]int64, len(teamIDs))
	for _, teamID := range teamIDs {
		row, err := db.AuthDB.GetTeamByTeamID(t.Context(), teamID)
		require.NoError(t, err)
		limits[teamID] = limitColumns(row)
	}

	return limits
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
