package handlers

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
	"github.com/e2b-dev/infra/packages/shared/pkg/id"
)

type adminTestTeam struct {
	name      string
	banned    bool
	blocked   bool
	createdAt time.Time
}

func insertAdminTestTeam(t *testing.T, db *testutils.Database, team adminTestTeam) uuid.UUID {
	t.Helper()

	teamID := uuid.New()

	require.NoError(t, db.SqlcClient.TestsRawSQL(t.Context(),
		`INSERT INTO public.teams (id, name, email, slug, is_banned, is_blocked, created_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		teamID, team.name, strings.ToLower(strings.ReplaceAll(team.name, " ", "."))+"@example.com",
		"slug-"+teamID.String()[:8], team.banned, team.blocked, team.createdAt,
	))

	return teamID
}

func postAdminTeamsQuery(t *testing.T, store *APIStore, body string) (int, api.AdminTeamList, string) {
	t.Helper()

	recorder := httptest.NewRecorder()
	ginCtx, _ := gin.CreateTestContext(recorder)
	ginCtx.Request = httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/unstable/admin/teams/query", strings.NewReader(body))
	ginCtx.Request.Header.Set("Content-Type", "application/json")

	store.PostUnstableAdminTeamsQuery(ginCtx)

	var list api.AdminTeamList
	if recorder.Code == http.StatusOK {
		require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &list))
	}

	return recorder.Code, list, recorder.Body.String()
}

func adminTeamIDs(list api.AdminTeamList) []uuid.UUID {
	ids := make([]uuid.UUID, 0, len(list.Items))
	for _, item := range list.Items {
		ids = append(ids, item.Id)
	}

	return ids
}

// Every list below is bounded by creation date, which leaves out the system
// team the migrations create.
func TestPostUnstableAdminTeamsQueryAppliesFilters(t *testing.T) {
	t.Parallel()

	testDB := testutils.SetupDatabase(t)
	alpha := insertAdminTestTeam(t, testDB, adminTestTeam{name: "Alpha Labs", createdAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)})
	banned := insertAdminTestTeam(t, testDB, adminTestTeam{name: "Banned Co", banned: true, createdAt: time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)})
	blocked := insertAdminTestTeam(t, testDB, adminTestTeam{name: "Blocked Co", blocked: true, createdAt: time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)})
	plain := insertAdminTestTeam(t, testDB, adminTestTeam{name: "Plain Co", createdAt: time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)})

	store := &APIStore{sqlcReadDB: testDB.SqlcClient, teamSandboxCounter: fakeTeamRunningSandboxCounter{}}

	tests := map[string]struct {
		body string
		want []uuid.UUID
	}{
		"teams are listed newest first": {
			body: `{"createdBefore":"2026-06-01T00:00:00Z"}`,
			want: []uuid.UUID{plain, blocked, banned, alpha},
		},
		"absent project IDs restrict nothing": {
			body: `{"sortBy":"createdAt","sortOrder":"asc","createdBefore":"2026-06-01T00:00:00Z"}`,
			want: []uuid.UUID{alpha, banned, blocked, plain},
		},
		"empty project IDs match no team": {
			body: `{"projectIds":[]}`,
			want: []uuid.UUID{},
		},
		"project IDs restrict to those teams": {
			body: `{"projectIds":["` + alpha.String() + `","` + plain.String() + `"]}`,
			want: []uuid.UUID{plain, alpha},
		},
		"hiding banned and blocked teams leaves the rest": {
			body: `{"hideBanned":true,"hideBlocked":true,"createdBefore":"2026-06-01T00:00:00Z"}`,
			want: []uuid.UUID{plain, alpha},
		},
		"creation bounds are inclusive": {
			body: `{"createdAfter":"2026-02-01T00:00:00Z","createdBefore":"2026-03-01T00:00:00Z"}`,
			want: []uuid.UUID{blocked, banned},
		},
		"a team UUID matches that team only": {
			body: `{"search":"` + banned.String() + `"}`,
			want: []uuid.UUID{banned},
		},
		"a public project ID matches that team only": {
			body: `{"search":"` + id.ConvertTeamIDToProjectID(blocked).String() + `"}`,
			want: []uuid.UUID{blocked},
		},
		"text matches a name substring ignoring case": {
			body: `{"search":"  alpha "}`,
			want: []uuid.UUID{alpha},
		},
		"text matches an email substring": {
			body: `{"search":"plain.co@"}`,
			want: []uuid.UUID{plain},
		},
		"wildcards in text match literally": {
			body: `{"search":"_co@"}`,
			want: []uuid.UUID{},
		},
		"text matches a slug substring": {
			body: `{"search":"slug-` + banned.String()[:8] + `"}`,
			want: []uuid.UUID{banned},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			code, list, body := postAdminTeamsQuery(t, store, tc.body)

			require.Equal(t, http.StatusOK, code, body)
			require.Equal(t, tc.want, adminTeamIDs(list))
			require.Equal(t, int64(len(tc.want)), list.Total)
		})
	}
}

func TestPostUnstableAdminTeamsQueryCountsEveryMatchingTeamAcrossPages(t *testing.T) {
	t.Parallel()

	testDB := testutils.SetupDatabase(t)
	var newestFirst []uuid.UUID
	for day := 5; day >= 1; day-- {
		newestFirst = append(newestFirst, insertAdminTestTeam(t, testDB, adminTestTeam{
			name:      "Team",
			createdAt: time.Date(2026, 1, day, 0, 0, 0, 0, time.UTC),
		}))
	}

	store := &APIStore{sqlcReadDB: testDB.SqlcClient, teamSandboxCounter: fakeTeamRunningSandboxCounter{}}

	code, list, body := postAdminTeamsQuery(t, store, `{"limit":2,"offset":1,"createdBefore":"2026-06-01T00:00:00Z"}`)
	require.Equal(t, http.StatusOK, code, body)
	require.Equal(t, newestFirst[1:3], adminTeamIDs(list))
	require.Equal(t, int64(5), list.Total)

	code, list, body = postAdminTeamsQuery(t, store, `{"limit":2,"offset":10,"createdBefore":"2026-06-01T00:00:00Z"}`)
	require.Equal(t, http.StatusOK, code, body)
	require.Empty(t, list.Items)
	require.Equal(t, int64(5), list.Total)
}

func TestPostUnstableAdminTeamsQueryOrdersByRunningSandboxesWithCounts(t *testing.T) {
	t.Parallel()

	testDB := testutils.SetupDatabase(t)
	idleOld := insertAdminTestTeam(t, testDB, adminTestTeam{name: "Idle Old", createdAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)})
	idleNew := insertAdminTestTeam(t, testDB, adminTestTeam{name: "Idle New", createdAt: time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)})
	busy := insertAdminTestTeam(t, testDB, adminTestTeam{name: "Busy", createdAt: time.Date(2026, 1, 3, 0, 0, 0, 0, time.UTC)})
	quiet := insertAdminTestTeam(t, testDB, adminTestTeam{name: "Quiet", createdAt: time.Date(2026, 1, 4, 0, 0, 0, 0, time.UTC)})

	userID := uuid.New()
	require.NoError(t, testDB.AuthDB.UpsertPublicUser(t.Context(), userID))
	require.NoError(t, testDB.AuthDB.TestsRawSQL(t.Context(),
		`INSERT INTO public.users_teams (user_id, team_id, is_default) VALUES ($1, $2, true)`, userID, busy))

	store := &APIStore{
		sqlcReadDB: testDB.SqlcClient,
		teamSandboxCounter: fakeTeamRunningSandboxCounter{counts: map[uuid.UUID]int64{
			busy:       7,
			quiet:      2,
			uuid.New(): 3,
		}},
	}

	code, list, body := postAdminTeamsQuery(t, store, `{"sortBy":"runningSandboxes","createdBefore":"2026-06-01T00:00:00Z"}`)
	require.Equal(t, http.StatusOK, code, body)
	require.Equal(t, []uuid.UUID{busy, quiet, idleNew, idleOld}, adminTeamIDs(list))
	require.Equal(t, new(int64(7)), list.Items[0].RunningSandboxCount)
	require.Equal(t, int64(1), list.Items[0].MemberCount)
	require.Equal(t, new(int64(2)), list.Items[1].RunningSandboxCount)
	require.Equal(t, new(int64(0)), list.Items[2].RunningSandboxCount)
	require.Equal(t, int64(0), list.Items[2].MemberCount)

	code, list, body = postAdminTeamsQuery(t, store, `{"sortBy":"runningSandboxes","sortOrder":"asc","createdBefore":"2026-06-01T00:00:00Z"}`)
	require.Equal(t, http.StatusOK, code, body)
	require.Equal(t, []uuid.UUID{idleNew, idleOld, quiet, busy}, adminTeamIDs(list))
}

func TestPostUnstableAdminTeamsQueryFailsWhenRunningCountsAreUnavailable(t *testing.T) {
	t.Parallel()

	store := &APIStore{teamSandboxCounter: fakeTeamRunningSandboxCounter{err: errors.New("redis unavailable")}}

	code, _, body := postAdminTeamsQuery(t, store, `{"sortBy":"runningSandboxes"}`)

	require.Equal(t, http.StatusInternalServerError, code)
	require.JSONEq(t, `{"code":500,"message":"Failed to count running sandboxes"}`, body)
}

func TestPostUnstableAdminTeamsQueryListsWithoutRunningCountsUnlessSortedByThem(t *testing.T) {
	t.Parallel()

	testDB := testutils.SetupDatabase(t)
	teamID := insertAdminTestTeam(t, testDB, adminTestTeam{name: "Named", createdAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)})
	store := &APIStore{
		sqlcReadDB:         testDB.SqlcClient,
		teamSandboxCounter: fakeTeamRunningSandboxCounter{err: errors.New("redis unavailable")},
	}

	code, list, body := postAdminTeamsQuery(t, store, `{"sortBy":"name","createdBefore":"2026-06-01T00:00:00Z"}`)

	require.Equal(t, http.StatusOK, code, body)
	require.Equal(t, []uuid.UUID{teamID}, adminTeamIDs(list))
	require.Nil(t, list.Items[0].RunningSandboxCount)
	require.NotContains(t, body, "runningSandboxCount")
}

func getAdminTeam(t *testing.T, store *APIStore, teamID uuid.UUID) *httptest.ResponseRecorder {
	t.Helper()

	recorder := httptest.NewRecorder()
	ginCtx, _ := gin.CreateTestContext(recorder)
	ginCtx.Request = httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/unstable/admin/teams/"+teamID.String(), nil)

	store.GetUnstableAdminTeamsTeamID(ginCtx, teamID)

	return recorder
}

func TestGetUnstableAdminTeamsTeamIDReturnsBannedTeamWithCluster(t *testing.T) {
	t.Parallel()

	testDB := testutils.SetupDatabase(t)
	teamID := testutils.CreateTestTeam(t, testDB)
	clusterID := uuid.New()
	require.NoError(t, testDB.SqlcClient.TestsRawSQL(t.Context(),
		`INSERT INTO public.clusters (id, name, endpoint, endpoint_tls, token) VALUES ($1, 'eu-west', 'api.example.test:5008', true, 'token')`, clusterID))
	require.NoError(t, testDB.SqlcClient.TestsRawSQL(t.Context(),
		`UPDATE public.teams SET cluster_id = $2, is_banned = true, is_blocked = true, blocked_reason = 'abuse', sandbox_scheduling_labels = '{gpu}' WHERE id = $1`, teamID, clusterID))

	recorder := getAdminTeam(t, &APIStore{sqlcDB: testDB.SqlcClient}, teamID)

	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
	var body api.AdminTeam
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &body))
	require.Equal(t, teamID, body.Id)
	require.Equal(t, "Test Team "+teamID.String(), body.Name)
	require.Equal(t, "test-"+teamID.String()+"@example.com", body.Email)
	require.Equal(t, "test-team-"+teamID.String()[:8], body.Slug)
	require.True(t, body.IsBanned)
	require.True(t, body.IsBlocked)
	require.Equal(t, new("abuse"), body.BlockedReason)
	require.Equal(t, []string{"gpu"}, body.SandboxSchedulingLabels)
	require.Equal(t, &clusterID, body.ClusterId)
	require.Equal(t, new("eu-west"), body.ClusterName)
}

func TestGetUnstableAdminTeamsTeamIDReturnsEmptyLabelsAsAList(t *testing.T) {
	t.Parallel()

	testDB := testutils.SetupDatabase(t)
	teamID := testutils.CreateTestTeam(t, testDB)

	recorder := getAdminTeam(t, &APIStore{sqlcDB: testDB.SqlcClient}, teamID)

	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
	var body map[string]any
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &body))
	require.Equal(t, []any{}, body["sandboxSchedulingLabels"])
	require.Nil(t, body["clusterId"])
	require.Nil(t, body["clusterName"])
}

func TestGetUnstableAdminTeamsTeamIDRejectsUnknownTeam(t *testing.T) {
	t.Parallel()

	testDB := testutils.SetupDatabase(t)

	recorder := getAdminTeam(t, &APIStore{sqlcDB: testDB.SqlcClient}, uuid.New())

	require.Equal(t, http.StatusNotFound, recorder.Code)
	require.JSONEq(t, `{"code":404,"message":"Team not found"}`, recorder.Body.String())
}

func putAdminTeamSchedulingLabels(t *testing.T, store *APIStore, teamID uuid.UUID, body string) *httptest.ResponseRecorder {
	t.Helper()

	recorder := httptest.NewRecorder()
	ginCtx, _ := gin.CreateTestContext(recorder)
	ginCtx.Request = httptest.NewRequestWithContext(t.Context(), http.MethodPut, "/unstable/admin/teams/"+teamID.String()+"/scheduling-labels", strings.NewReader(body))
	ginCtx.Request.Header.Set("Content-Type", "application/json")

	store.PutUnstableAdminTeamsTeamIDSchedulingLabels(ginCtx, teamID)

	return recorder
}

func teamSchedulingLabels(t *testing.T, db *testutils.Database, teamID uuid.UUID) []string {
	t.Helper()

	var labels []string
	require.NoError(t, db.SqlcClient.TestsRawSQLQuery(t.Context(),
		`SELECT sandbox_scheduling_labels FROM public.teams WHERE id = $1`,
		func(rows pgx.Rows) error {
			for rows.Next() {
				if err := rows.Scan(&labels); err != nil {
					return err
				}
			}

			return rows.Err()
		},
		teamID,
	))

	return labels
}

func TestPutUnstableAdminTeamsTeamIDSchedulingLabelsWritesDedupedLabelsAndEvictsTeam(t *testing.T) {
	t.Parallel()

	testDB := testutils.SetupDatabase(t)
	teamID := testutils.CreateTestTeam(t, testDB)

	var invalidated []uuid.UUID
	store := &APIStore{sqlcDB: testDB.SqlcClient, authService: fakeAPIKeyAuthService{invalidatedTeamIDs: &invalidated}}

	recorder := putAdminTeamSchedulingLabels(t, store, teamID, `{"labels":["high-memory","gpu","high-memory"]}`)

	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
	require.JSONEq(t, `{"labels":["high-memory","gpu"]}`, recorder.Body.String())
	require.Equal(t, []string{"high-memory", "gpu"}, teamSchedulingLabels(t, testDB, teamID))
	require.Equal(t, []uuid.UUID{teamID}, invalidated)
}

func TestPutUnstableAdminTeamsTeamIDSchedulingLabelsClearsLabelsWithAnEmptyList(t *testing.T) {
	t.Parallel()

	testDB := testutils.SetupDatabase(t)
	teamID := testutils.CreateTestTeam(t, testDB)
	require.NoError(t, testDB.SqlcClient.TestsRawSQL(t.Context(),
		`UPDATE public.teams SET sandbox_scheduling_labels = '{gpu}' WHERE id = $1`, teamID))

	store := &APIStore{sqlcDB: testDB.SqlcClient, authService: fakeAPIKeyAuthService{}}

	recorder := putAdminTeamSchedulingLabels(t, store, teamID, `{"labels":[]}`)

	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
	require.JSONEq(t, `{"labels":[]}`, recorder.Body.String())
	require.Empty(t, teamSchedulingLabels(t, testDB, teamID))
}

func TestPutUnstableAdminTeamsTeamIDSchedulingLabelsRejectsUnknownTeam(t *testing.T) {
	t.Parallel()

	testDB := testutils.SetupDatabase(t)
	var invalidated []uuid.UUID
	store := &APIStore{sqlcDB: testDB.SqlcClient, authService: fakeAPIKeyAuthService{invalidatedTeamIDs: &invalidated}}

	recorder := putAdminTeamSchedulingLabels(t, store, uuid.New(), `{"labels":["gpu"]}`)

	require.Equal(t, http.StatusNotFound, recorder.Code)
	require.JSONEq(t, `{"code":404,"message":"Team not found"}`, recorder.Body.String())
	require.Empty(t, invalidated)
}

func TestPutUnstableAdminTeamsTeamIDSchedulingLabelsSucceedsWhenEvictionFailsAfterWriting(t *testing.T) {
	t.Parallel()

	testDB := testutils.SetupDatabase(t)
	teamID := testutils.CreateTestTeam(t, testDB)
	store := &APIStore{sqlcDB: testDB.SqlcClient, authService: fakeAPIKeyAuthService{invalidateTeamErr: errors.New("redis unavailable")}}

	recorder := putAdminTeamSchedulingLabels(t, store, teamID, `{"labels":["gpu"]}`)

	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
	require.JSONEq(t, `{"labels":["gpu"]}`, recorder.Body.String())
	require.Equal(t, []string{"gpu"}, teamSchedulingLabels(t, testDB, teamID))
}
