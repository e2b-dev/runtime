package management

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
)

func projectLimits(projectID uuid.UUID, revision int64) ProjectLimitsProjection {
	return ProjectLimitsProjection{
		ProjectID:                projectID,
		Revision:                 revision,
		MaxLengthHours:           12,
		ConcurrentSandboxes:      40,
		ConcurrentTemplateBuilds: 30,
		MaxVCPU:                  16,
		MaxRAMMB:                 32768,
		DiskMB:                   20480,
		EventsTTLDays:            14,
		DefaultFreeDiskSizeMB:    10240,
		MaxFreeDiskSizeMB:        51200,
		APITeamRPSList:           60,
	}
}

// Read through the view, not the table: team_limits is what gates sandbox
// creation, and the push only matters where it is read.
func concurrentSandboxes(t *testing.T, db *testutils.Database, teamID uuid.UUID) int64 {
	t.Helper()

	var sandboxes int64
	require.NoError(t, db.SqlcClient.TestsRawSQLQuery(t.Context(),
		"SELECT concurrent_sandboxes FROM public.team_limits WHERE id = $1",
		func(rows pgx.Rows) error {
			rows.Next()

			return rows.Scan(&sandboxes)
		}, teamID))

	return sandboxes
}

func ledgerRevision(t *testing.T, db *testutils.Database, teamID uuid.UUID) *int64 {
	t.Helper()

	var revision *int64
	require.NoError(t, db.SqlcClient.TestsRawSQLQuery(t.Context(),
		"SELECT revision FROM projection.project_limits WHERE project_id = $1",
		func(rows pgx.Rows) error {
			if !rows.Next() {
				return nil
			}

			return rows.Scan(&revision)
		}, teamID))

	return revision
}

func limitsUpdatedAt(t *testing.T, db *testutils.Database, teamID uuid.UUID) time.Time {
	t.Helper()

	var updatedAt time.Time
	require.NoError(t, db.SqlcClient.TestsRawSQLQuery(t.Context(),
		"SELECT updated_at FROM public.project_limits WHERE team_id = $1",
		func(rows pgx.Rows) error {
			rows.Next()

			return rows.Scan(&updatedAt)
		}, teamID))

	return updatedAt
}

func TestValidateProjectLimitsProjection(t *testing.T) {
	t.Parallel()

	require.NoError(t, validateProjectLimitsProjection(projectLimits(uuid.New(), 1)))

	accepted := map[string]func(*ProjectLimitsProjection){
		"no default free disk":   func(p *ProjectLimitsProjection) { p.DefaultFreeDiskSizeMB = 0 },
		"disabled list rate":     func(p *ProjectLimitsProjection) { p.APITeamRPSList = 0 },
		"free disk at ceiling":   func(p *ProjectLimitsProjection) { p.DefaultFreeDiskSizeMB = p.MaxFreeDiskSizeMB },
		"every limit at a floor": func(p *ProjectLimitsProjection) { *p = floors(p.ProjectID) },
	}
	for name, change := range accepted {
		projection := projectLimits(uuid.New(), 1)
		change(&projection)
		require.NoError(t, validateProjectLimitsProjection(projection), name)
	}

	refused := map[string]func(*ProjectLimitsProjection){
		"no project":                  func(p *ProjectLimitsProjection) { p.ProjectID = uuid.Nil },
		"zero revision":               func(p *ProjectLimitsProjection) { p.Revision = 0 },
		"negative revision":           func(p *ProjectLimitsProjection) { p.Revision = -1 },
		"zero sandbox length":         func(p *ProjectLimitsProjection) { p.MaxLengthHours = 0 },
		"zero sandboxes":              func(p *ProjectLimitsProjection) { p.ConcurrentSandboxes = 0 },
		"zero template builds":        func(p *ProjectLimitsProjection) { p.ConcurrentTemplateBuilds = 0 },
		"zero vcpu":                   func(p *ProjectLimitsProjection) { p.MaxVCPU = 0 },
		"zero ram":                    func(p *ProjectLimitsProjection) { p.MaxRAMMB = 0 },
		"zero disk":                   func(p *ProjectLimitsProjection) { p.DiskMB = 0 },
		"zero events ttl":             func(p *ProjectLimitsProjection) { p.EventsTTLDays = 0 },
		"negative default free disk":  func(p *ProjectLimitsProjection) { p.DefaultFreeDiskSizeMB = -1 },
		"zero free disk ceiling":      func(p *ProjectLimitsProjection) { p.MaxFreeDiskSizeMB, p.DefaultFreeDiskSizeMB = 0, 0 },
		"negative list rate":          func(p *ProjectLimitsProjection) { p.APITeamRPSList = -1 },
		"free disk above its ceiling": func(p *ProjectLimitsProjection) { p.DefaultFreeDiskSizeMB = p.MaxFreeDiskSizeMB + 1 },
	}
	for name, change := range refused {
		projection := projectLimits(uuid.New(), 1)
		change(&projection)
		require.ErrorIs(t, validateProjectLimitsProjection(projection), ErrInvalidProjectLimits, name)
	}
}

func floors(projectID uuid.UUID) ProjectLimitsProjection {
	return ProjectLimitsProjection{
		ProjectID: projectID, Revision: 1, MaxLengthHours: 1, ConcurrentSandboxes: 1,
		ConcurrentTemplateBuilds: 1, MaxVCPU: 1, MaxRAMMB: 1, DiskMB: 1, EventsTTLDays: 1,
		MaxFreeDiskSizeMB: 1,
	}
}

// The delivery that arrives late must not put the project back on limits it has
// already left. The caller fences what it sends, but two pushes in flight arrive
// in whichever order the network gives them, so only this side can refuse it.
func TestApplyProjectLimitsHonorsTheNewestRevision(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	service, cache := newService(db)
	teamID := testutils.CreateTestTeam(t, db)

	first := projectLimits(teamID, 1)
	require.NoError(t, service.ApplyProjectLimits(t.Context(), first))
	require.EqualValues(t, 40, concurrentSandboxes(t, db, teamID))

	raised := projectLimits(teamID, 2)
	raised.ConcurrentSandboxes = 90
	require.NoError(t, service.ApplyProjectLimits(t.Context(), raised))
	require.EqualValues(t, 90, concurrentSandboxes(t, db, teamID))

	cache.reset()

	overtaken := projectLimits(teamID, 1)
	overtaken.ConcurrentSandboxes = 1
	require.NoError(t, service.ApplyProjectLimits(t.Context(), overtaken),
		"a superseded delivery is satisfied, not an error the caller should retry")
	require.EqualValues(t, 90, concurrentSandboxes(t, db, teamID))
	require.EqualValues(t, 2, *ledgerRevision(t, db, teamID))

	// Evicted on the dropped delivery too: the retry that carries a revision
	// this side already has is how an earlier eviction that never finished gets
	// repaired.
	require.Equal(t, []uuid.UUID{teamID}, cache.teams)
}

// A repeat of the revision already recorded is the same answer, so it changes
// nothing — including updated_at, which is what says when the answer last moved.
func TestApplyProjectLimitsTreatsTheSameRevisionAsAlreadyStored(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	service, _ := newService(db)
	teamID := testutils.CreateTestTeam(t, db)

	require.NoError(t, service.ApplyProjectLimits(t.Context(), projectLimits(teamID, 3)))
	stored := limitsUpdatedAt(t, db, teamID)

	require.NoError(t, service.ApplyProjectLimits(t.Context(), projectLimits(teamID, 3)))
	require.Equal(t, stored, limitsUpdatedAt(t, db, teamID))
}

func TestApplyProjectLimitsReportsAnUnknownProject(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	service, cache := newService(db)

	err := service.ApplyProjectLimits(t.Context(), projectLimits(uuid.New(), 1))

	require.ErrorIs(t, err, ErrProjectNotFound)
	require.Empty(t, cache.teams)
}

// The ledger and the values commit together. A rejected pair must leave the
// ledger where it was, or the delivery that carries the corrected values under
// the same revision would be dropped as a duplicate and the project would keep
// the old limits for good.
func TestApplyProjectLimitsLeavesTheLedgerBehindARejectedWrite(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	service, cache := newService(db)
	teamID := testutils.CreateTestTeam(t, db)

	incoherent := projectLimits(teamID, 4)
	incoherent.DefaultFreeDiskSizeMB = incoherent.MaxFreeDiskSizeMB + 1

	require.ErrorIs(t, service.ApplyProjectLimits(t.Context(), incoherent), ErrInvalidProjectLimits)
	require.Nil(t, ledgerRevision(t, db, teamID))
	require.Empty(t, cache.teams)

	require.NoError(t, service.ApplyProjectLimits(t.Context(), projectLimits(teamID, 4)))
	require.EqualValues(t, 4, *ledgerRevision(t, db, teamID))
	require.EqualValues(t, 40, concurrentSandboxes(t, db, teamID))
}

// Until a project is pushed to, team_limits answers from the tier as it always
// has. The ledger row alone must not change that.
func TestApplyProjectLimitsLeavesAnUnpushedProjectOnItsTier(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	_, _ = newService(db)
	teamID := testutils.CreateTestTeam(t, db)

	require.Nil(t, ledgerRevision(t, db, teamID))

	var overrides int
	require.NoError(t, db.SqlcClient.TestsRawSQLQuery(t.Context(),
		"SELECT count(*) FROM public.project_limits WHERE team_id = $1",
		func(rows pgx.Rows) error {
			rows.Next()

			return rows.Scan(&overrides)
		}, teamID))
	require.Zero(t, overrides)
}

func listRate(t *testing.T, db *testutils.Database, teamID uuid.UUID) int64 {
	t.Helper()

	var rate int64
	require.NoError(t, db.SqlcClient.TestsRawSQLQuery(t.Context(),
		"SELECT api_team_rps_list FROM public.team_limits WHERE id = $1",
		func(rows pgx.Rows) error {
			rows.Next()

			return rows.Scan(&rate)
		}, teamID))

	return rate
}

func setTierListRate(t *testing.T, db *testutils.Database, teamID uuid.UUID, rate int64) {
	t.Helper()

	require.NoError(t, db.SqlcClient.TestsRawSQLQuery(t.Context(),
		"UPDATE public.tiers SET api_team_rps_list = $2 WHERE id = (SELECT tier FROM public.teams WHERE id = $1)",
		func(pgx.Rows) error { return nil }, teamID, rate))
}

func TestApplyProjectLimitsPushesTheListRate(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	service, _ := newService(db)
	teamID := testutils.CreateTestTeam(t, db)
	setTierListRate(t, db, teamID, 25)

	pushed := projectLimits(teamID, 1)
	pushed.APITeamRPSList = 70
	require.NoError(t, service.ApplyProjectLimits(t.Context(), pushed))
	require.EqualValues(t, 70, listRate(t, db, teamID))

	disabled := projectLimits(teamID, 2)
	disabled.APITeamRPSList = 0
	require.NoError(t, service.ApplyProjectLimits(t.Context(), disabled))
	require.EqualValues(t, 0, listRate(t, db, teamID), "a pushed zero disables the limit rather than falling back")
}

func TestAnUnpushedProjectKeepsItsTierListRate(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	_, _ = newService(db)
	teamID := testutils.CreateTestTeam(t, db)
	setTierListRate(t, db, teamID, 25)

	require.EqualValues(t, 25, listRate(t, db, teamID))
}
