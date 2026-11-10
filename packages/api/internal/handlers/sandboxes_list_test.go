package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/launchdarkly/go-sdk-common/v3/ldvalue"
	"github.com/launchdarkly/go-server-sdk/v7/testhelpers/ldtestdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	analyticscollector "github.com/e2b-dev/infra/packages/api/internal/analytics_collector"
	"github.com/e2b-dev/infra/packages/api/internal/api"
	templatecache "github.com/e2b-dev/infra/packages/api/internal/cache/templates"
	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
	"github.com/e2b-dev/infra/packages/api/internal/utils"
	"github.com/e2b-dev/infra/packages/auth/pkg/auth"
	authtypes "github.com/e2b-dev/infra/packages/auth/pkg/types"
	sqlcdb "github.com/e2b-dev/infra/packages/db/client"
	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
	dbtypes "github.com/e2b-dev/infra/packages/db/pkg/types"
	"github.com/e2b-dev/infra/packages/db/queries"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	redis_utils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
	sharedutils "github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

func TestParseSandboxListTemplateFilter_AllowsExplicitNamespace(t *testing.T) {
	t.Parallel()

	identifier, err := parseSandboxListTemplateFilter("public-owner/template")
	require.NoError(t, err)
	assert.Equal(t, "public-owner/template", identifier)
}

func TestParseSandboxListOrder(t *testing.T) {
	t.Parallel()

	orderPtr := func(v api.OrderDirection) *api.OrderDirection { return &v }

	t.Run("omitted defaults to newest first", func(t *testing.T) {
		t.Parallel()

		order, err := parseSandboxListOrder(nil)
		require.NoError(t, err)
		assert.Equal(t, utils.SortDesc, order)
	})

	t.Run("known values map through", func(t *testing.T) {
		t.Parallel()

		order, err := parseSandboxListOrder(orderPtr(api.Asc))
		require.NoError(t, err)
		assert.Equal(t, utils.SortAsc, order)

		order, err = parseSandboxListOrder(orderPtr(api.Desc))
		require.NoError(t, err)
		assert.Equal(t, utils.SortDesc, order)
	})

	t.Run("unknown values are rejected, not defaulted", func(t *testing.T) {
		t.Parallel()

		// A typo must not quietly page in the opposite direction.
		for _, bogus := range []api.OrderDirection{"", "dsc", "ASC", "ascending"} {
			_, err := parseSandboxListOrder(orderPtr(bogus))
			require.Error(t, err, "order %q should be rejected", string(bogus))
		}
	})
}

// newSandboxListTestStore builds the minimum APIStore that GetV2Sandboxes needs to
// reach its parameter-handling branches. The orchestrator is deliberately absent: every
// case below returns before any sandbox is fetched, so a nil orchestrator also proves
// the short circuit really did short-circuit.
func newSandboxListTestStore(t *testing.T) (*APIStore, uuid.UUID, string) {
	t.Helper()

	db := testutils.SetupDatabase(t)
	redis := redis_utils.SetupInstance(t)
	ctx := t.Context()

	teamID := testutils.CreateTestTeam(t, db)
	teamSlug := testutils.GetTeamSlug(t, ctx, db, teamID)

	// An empty API key silences the client, and its batch interval is far longer than
	// a test's lifetime, so nothing is sent anywhere.
	posthogClient, err := analyticscollector.NewPosthogClient(ctx, "")
	require.NoError(t, err)

	store := &APIStore{
		posthog:       posthogClient,
		templateCache: templatecache.NewTemplateCache(db.SqlcClient, redis),
	}
	t.Cleanup(func() {
		require.NoError(t, store.templateCache.Close(context.WithoutCancel(ctx)))
	})

	return store, teamID, teamSlug
}

func newSandboxListRequest(t *testing.T, teamID uuid.UUID, teamSlug string) (*httptest.ResponseRecorder, *gin.Context) {
	t.Helper()

	recorder := httptest.NewRecorder()
	ginCtx, _ := gin.CreateTestContext(recorder)
	ginCtx.Request = httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/v2/sandboxes", nil)
	auth.SetTeamInfoForTest(t, ginCtx, &authtypes.Team{
		Team: &dbtypes.Team{ID: teamID, Slug: teamSlug},
	})

	return recorder, ginCtx
}

// TestGetV2Sandboxes_RejectsUnknownOrder pins the 400 at the handler, not only in the
// helper, so the validation cannot be dropped from the wiring without a failure.
func TestGetV2Sandboxes_RejectsUnknownOrder(t *testing.T) {
	t.Parallel()

	store, teamID, teamSlug := newSandboxListTestStore(t)
	recorder, ginCtx := newSandboxListRequest(t, teamID, teamSlug)

	bogus := api.OrderDirection("dsc")

	store.GetV2Sandboxes(ginCtx, api.GetV2SandboxesParams{Order: &bogus})

	require.Equal(t, http.StatusBadRequest, recorder.Code)
	assert.Empty(t, recorder.Header().Get(headerTotalRunning))
}

// TestGetV2Sandboxes_UnknownTemplate covers the short circuit for a template filter
// that resolves to nothing: an empty list rather than a 404, and the running total
// header only when running sandboxes were actually requested.
func TestGetV2Sandboxes_UnknownTemplate(t *testing.T) {
	t.Parallel()

	assertEmptyList := func(t *testing.T, recorder *httptest.ResponseRecorder) {
		t.Helper()

		require.Equal(t, http.StatusOK, recorder.Code)

		var sandboxes []api.ListedSandbox
		require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &sandboxes))
		assert.Empty(t, sandboxes)
		assert.Empty(t, recorder.Header().Get("X-Next-Token"))
	}

	t.Run("running requested reports a zero total", func(t *testing.T) {
		t.Parallel()

		store, teamID, teamSlug := newSandboxListTestStore(t)
		recorder, ginCtx := newSandboxListRequest(t, teamID, teamSlug)

		template := "no-such-template"
		states := []api.SandboxState{api.Running}

		store.GetV2Sandboxes(ginCtx, api.GetV2SandboxesParams{Template: &template, State: &states})

		assertEmptyList(t, recorder)
		assert.Equal(t, "0", recorder.Header().Get(headerTotalRunning))
	})

	t.Run("paused only omits the running total", func(t *testing.T) {
		t.Parallel()

		store, teamID, teamSlug := newSandboxListTestStore(t)
		recorder, ginCtx := newSandboxListRequest(t, teamID, teamSlug)

		template := "no-such-template"
		states := []api.SandboxState{api.Paused}

		store.GetV2Sandboxes(ginCtx, api.GetV2SandboxesParams{Template: &template, State: &states})

		assertEmptyList(t, recorder)
		assert.Empty(t, recorder.Header().Get(headerTotalRunning),
			"the header is documented as present only when running sandboxes were requested")
	})

	t.Run("tagged template is rejected before resolution", func(t *testing.T) {
		t.Parallel()

		store, teamID, teamSlug := newSandboxListTestStore(t)
		recorder, ginCtx := newSandboxListRequest(t, teamID, teamSlug)

		template := "no-such-template:v2"

		store.GetV2Sandboxes(ginCtx, api.GetV2SandboxesParams{Template: &template})

		require.Equal(t, http.StatusBadRequest, recorder.Code)
	})
}

// TestParseSandboxListTemplateFilter_RejectsTags pins the 400: sandboxes record only
// their base template, which carries no tag, so a tagged filter cannot be honored.
// Accepting one would silently return sandboxes from every tag of the template.
func TestParseSandboxListTemplateFilter_RejectsTags(t *testing.T) {
	t.Parallel()

	for _, template := range []string{"template:v2", "public-owner/template:v2", "template:default"} {
		_, err := parseSandboxListTemplateFilter(template)
		require.Error(t, err, "template %q should be rejected", template)
		assert.Contains(t, err.Error(), "tags are not supported")
	}
}

// TestParseSandboxListStartedAfter pins the bound onto the microsecond grid. Without
// the truncation the bound is the only value in the request carrying sub-microsecond
// bits, which makes it exclusive for running sandboxes (compared against the truncated
// PaginationTimestamp) while Postgres, receiving the same bound floored by pgx, treats
// it as inclusive for paused ones.
func TestParseSandboxListStartedAfter(t *testing.T) {
	t.Parallel()

	t.Run("omitted is the zero time", func(t *testing.T) {
		t.Parallel()
		assert.True(t, parseSandboxListStartedAfter(nil).IsZero())
	})

	t.Run("sub-microsecond bits are floored", func(t *testing.T) {
		t.Parallel()

		aligned := time.Date(2026, 1, 2, 3, 4, 5, 123456000, time.UTC)
		bound := aligned.Add(789 * time.Nanosecond)

		assert.Equal(t, aligned, parseSandboxListStartedAfter(&bound))
		assert.Zero(t, parseSandboxListStartedAfter(&bound).Nanosecond()%1000)
	})

	t.Run("an already-aligned bound is unchanged", func(t *testing.T) {
		t.Parallel()

		aligned := time.Date(2026, 1, 2, 3, 4, 5, 123456000, time.UTC)
		assert.Equal(t, aligned, parseSandboxListStartedAfter(&aligned))
	})

	// The scenario the truncation exists for: a client reads a running sandbox's
	// startedAt at full precision and hands it straight back as the bound.
	t.Run("a sandbox own startedAt matches its keyset value", func(t *testing.T) {
		t.Parallel()

		start := time.Date(2026, 1, 2, 3, 4, 5, 123456789, time.UTC)

		listed := instanceInfoToPaginatedSandboxes([]sandbox.Sandbox{
			{SandboxID: "sbx", StartTime: start, State: sandbox.StateRunning},
		})
		require.Len(t, listed, 1)

		bound := parseSandboxListStartedAfter(&listed[0].StartedAt)

		// The in-memory filter keeps it...
		kept := utils.FilterSandboxesOnStartedAtAndTemplate(listed, bound, nil)
		require.Len(t, kept, 1)

		// ...and the paused page still excludes it, so the snapshot query returning
		// the same row cannot list it a second time.
		assert.True(t, sandboxCanAppearInPausedPage(
			sandbox.Sandbox{SandboxID: "sbx", StartTime: start},
			bound,
			nil,
		))
	})
}

func TestSandboxCanAppearInPausedPage(t *testing.T) {
	t.Parallel()

	startedAfter := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	templateID := "template-a"

	sbx := func(baseTemplateID string, startTime time.Time) sandbox.Sandbox {
		return sandbox.Sandbox{SandboxID: "sbx", BaseTemplateID: baseTemplateID, StartTime: startTime}
	}

	t.Run("no filters keeps every live sandbox excluded", func(t *testing.T) {
		t.Parallel()
		assert.True(t, sandboxCanAppearInPausedPage(sbx("any", startedAfter), time.Time{}, nil))
	})

	t.Run("other template cannot come back", func(t *testing.T) {
		t.Parallel()
		assert.False(t, sandboxCanAppearInPausedPage(sbx("template-b", startedAfter), time.Time{}, &templateID))
		assert.True(t, sandboxCanAppearInPausedPage(sbx(templateID, startedAfter), time.Time{}, &templateID))
	})

	t.Run("start time before the bound cannot come back", func(t *testing.T) {
		t.Parallel()
		assert.False(t, sandboxCanAppearInPausedPage(sbx(templateID, startedAfter.Add(-time.Second)), startedAfter, nil))
		assert.True(t, sandboxCanAppearInPausedPage(sbx(templateID, startedAfter), startedAfter, nil))
	})

	t.Run("sub-microsecond start time uses the stored precision", func(t *testing.T) {
		t.Parallel()
		// The snapshot row would hold the truncated value, which is before the bound,
		// so the query cannot return this sandbox.
		assert.False(t, sandboxCanAppearInPausedPage(
			sbx(templateID, startedAfter.Add(-500*time.Nanosecond)),
			startedAfter,
			nil,
		))
	})
}

// TestInstanceInfoToPaginatedSandboxes_PaginationTimestampPrecision guards the keyset
// pagination boundary: running sandboxes carry nanosecond StartTime while paused
// snapshots are microsecond-precision in Postgres. Only the PaginationTimestamp keyset
// value is truncated to microseconds (so the in-memory sort/cursor and the SQL predicate
// agree); the public StartedAt must keep its full precision so list responses match the
// sandbox detail endpoint.
func TestInstanceInfoToPaginatedSandboxes_PaginationTimestampPrecision(t *testing.T) {
	t.Parallel()

	// Sub-microsecond bits set (…789 ns) so truncation is observable.
	start := time.Date(2026, 1, 2, 3, 4, 5, 123456789, time.UTC)

	sandboxes := instanceInfoToPaginatedSandboxes([]sandbox.Sandbox{
		{SandboxID: "sbx", StartTime: start, State: sandbox.StateRunning},
	})

	require.Len(t, sandboxes, 1)

	assert.Equal(t, start, sandboxes[0].StartedAt, "public StartedAt must keep full precision")
	assert.Equal(t, start.Truncate(time.Microsecond), sandboxes[0].PaginationTimestamp,
		"pagination key must be microsecond-aligned")
	assert.Zero(t, sandboxes[0].PaginationTimestamp.Nanosecond()%1000,
		"pagination key should have no sub-microsecond bits")
}

// GET /sandboxes refuses a list above the cap, counting the sandboxes the
// metadata filter leaves, since those are what the response would carry.
func TestGetRunningSandboxes_Cap(t *testing.T) {
	t.Parallel()

	runningSandboxes := func(n int) []sandbox.Sandbox {
		sandboxes := make([]sandbox.Sandbox, n)
		for i := range sandboxes {
			sandboxes[i] = sandbox.Sandbox{SandboxID: fmt.Sprintf("sbx-%d", i), State: sandbox.StateRunning}
		}
		sandboxes[0].Metadata = map[string]string{"app": "one"}

		return sandboxes
	}

	for _, tc := range []struct {
		name      string
		capped    bool
		sandboxes int
		metadata  *map[string]string
		wantCount int
		wantErr   bool
	}{
		{name: "at the cap", capped: true, sandboxes: sandboxesUnboundedMaxCount, wantCount: sandboxesUnboundedMaxCount},
		{name: "above the cap", capped: true, sandboxes: sandboxesUnboundedMaxCount + 1, wantErr: true},
		{name: "above the cap before the metadata filter", capped: true, sandboxes: sandboxesUnboundedMaxCount + 1, metadata: &map[string]string{"app": "one"}, wantCount: 1},
		{name: "cap off", sandboxes: sandboxesUnboundedMaxCount + 1, wantCount: sandboxesUnboundedMaxCount + 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			listed, err := getRunningSandboxes(runningSandboxes(tc.sandboxes), tc.metadata, tc.capped)
			if tc.wantErr {
				require.ErrorIs(t, err, errTooManyRunningSandboxes)

				return
			}

			require.NoError(t, err)
			assert.Len(t, listed, tc.wantCount)
		})
	}
}

// TestGetPausedSandboxes_ReadsFromTheReadClient checks that the paused page is read
// through sqlcReadDB. The snapshot exists only in the database behind that client.
func TestGetPausedSandboxes_ReadsFromTheReadClient(t *testing.T) {
	t.Parallel()

	primary, replica := testutils.SetupDatabase(t), testutils.SetupDatabase(t)
	ctx := t.Context()
	teamID := testutils.CreateTestTeam(t, replica)
	templateID := testutils.CreateTestTemplate(t, replica, teamID)
	testutils.UpsertTestSnapshot(t, ctx, replica, "snap-replica", "sbx-replica", teamID, templateID)
	sem, err := sharedutils.NewAdjustableSemaphore(1)
	require.NoError(t, err)
	store := &APIStore{sqlcDB: primary.SqlcClient, sqlcReadDB: replica.SqlcClient, sandboxListSem: sem}

	page, _, err := store.getPausedSandboxes(ctx, teamID, nil, nil, 10,
		time.Now().Add(time.Hour), utils.MaxSandboxID, utils.SortDesc, time.Time{}, nil)
	require.NoError(t, err)
	require.Len(t, page, 1)
	require.Equal(t, "sbx-replica", page[0].SandboxID)
}

func TestThrottledGetSnapshots_Retries(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		order     utils.SortDirection
		filtered  bool
		code      string
		message   string
		failures  int
		attempts  int
		wantError bool
	}{
		{"desc", utils.SortDesc, false, "40P01", "canceling statement due to conflict with recovery", 2, 3, false},
		{"asc", utils.SortAsc, false, "40P01", "canceling statement due to conflict with recovery", 2, 3, false},
		{"desc by template", utils.SortDesc, true, "40P01", "canceling statement due to conflict with recovery", 2, 3, false},
		{"asc by template", utils.SortAsc, true, "40P01", "canceling statement due to conflict with recovery", 2, 3, false},
		{"success", utils.SortDesc, false, "40P01", "conflict with recovery", 0, 1, false},
		{"serialization recovery", utils.SortDesc, false, "40001", "conflict with recovery", 1, 2, false},
		{"connection failure during row consumption", utils.SortDesc, false, "08006", "connection failure", 1, 1, true},
		{"success on final attempt", utils.SortDesc, false, "40P01", "conflict with recovery", 3, 4, false},
		{"persistent recovery conflict", utils.SortDesc, false, "40P01", "conflict with recovery", 10, 4, true},
		{"ordinary deadlock", utils.SortDesc, false, "40P01", "deadlock detected", 1, 1, true},
		{"ordinary serialization", utils.SortDesc, false, "40001", "could not serialize access", 1, 1, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			store, teamID, templateID := snapshotRetryStore(t, tt.failures, tt.code, tt.message)

			var templateFilter *string
			if tt.filtered {
				templateFilter = &templateID
			}
			cursorTime := time.Now().Add(time.Hour)
			cursorID := utils.MaxSandboxID
			if tt.order == utils.SortAsc {
				cursorTime = time.Time{}
				cursorID = ""
			}
			page, _, err := store.getPausedSandboxes(ctx, teamID, nil, nil, 10,
				cursorTime, cursorID, tt.order, time.Time{}, templateFilter)
			if tt.wantError {
				var pgErr *pgconn.PgError
				require.ErrorAs(t, err, &pgErr)
				assert.Equal(t, tt.code, pgErr.Code)
				assert.Empty(t, page)
			} else {
				require.NoError(t, err)
				require.Len(t, page, 1)
				assert.Equal(t, "sbx-retry", page[0].SandboxID)
			}
			var attempts int
			require.NoError(t, store.sqlcReadDB.Pool().QueryRow(ctx, "SELECT last_value FROM snapshot_read_attempt").Scan(&attempts))
			assert.Equal(t, tt.attempts, attempts)
			assert.True(t, store.sandboxListSem.TryAcquire(1), "the semaphore slot is released after retries")
		})
	}
}

func TestThrottledGetSnapshots_Cancellation(t *testing.T) {
	t.Parallel()
	store, teamID, _ := snapshotRetryStore(t, 10, "40P01", "conflict with recovery")
	_, err := store.sqlcReadDB.Pool().Exec(t.Context(), `
CREATE OR REPLACE FUNCTION fail_snapshot_read() RETURNS boolean LANGUAGE plpgsql AS $$
BEGIN
    PERFORM nextval('snapshot_read_attempt');
    PERFORM pg_sleep(30);
    RAISE EXCEPTION USING ERRCODE = '40P01', MESSAGE = 'conflict with recovery';
END;
$$`)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, _, err := store.getPausedSandboxes(ctx, teamID, nil, nil, 10,
			time.Now().Add(time.Hour), utils.MaxSandboxID, utils.SortDesc, time.Time{}, nil)
		result <- err
	}()
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		var started bool
		if assert.NoError(c, store.sqlcReadDB.Pool().QueryRow(t.Context(), "SELECT is_called FROM snapshot_read_attempt").Scan(&started)) {
			assert.True(c, started)
		}
	}, 5*time.Second, 10*time.Millisecond)
	assert.False(t, store.sandboxListSem.TryAcquire(1), "the read holds the semaphore slot")
	cancel()
	select {
	case err := <-result:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("sandbox list did not stop after cancellation")
	}
	var attempts int
	require.NoError(t, store.sqlcReadDB.Pool().QueryRow(t.Context(), "SELECT last_value FROM snapshot_read_attempt").Scan(&attempts))
	assert.Equal(t, 1, attempts)
	assert.True(t, store.sandboxListSem.TryAcquire(1), "cancellation releases the semaphore slot")
}

func snapshotRetryStore(t *testing.T, failures int, code, message string) (*APIStore, uuid.UUID, string) {
	t.Helper()
	ctx := t.Context()
	db := testutils.SetupDatabase(t)
	teamID := testutils.CreateTestTeam(t, db)
	templateID := testutils.CreateTestTemplate(t, db, teamID)
	testutils.UpsertTestSnapshot(t, ctx, db, "snap-retry", "sbx-retry", teamID, templateID)
	_, err := db.SqlcClient.Pool().Exec(ctx, `
ALTER TABLE public.snapshots RENAME TO snapshots_for_retry;
CREATE SEQUENCE snapshot_read_attempt;
CREATE TABLE snapshot_read_failure (failures integer, code text, message text);
CREATE FUNCTION fail_snapshot_read() RETURNS boolean LANGUAGE plpgsql AS $$
DECLARE failure snapshot_read_failure;
BEGIN
    SELECT failures, code, message INTO failure FROM snapshot_read_failure;
    IF nextval('snapshot_read_attempt') <= failure.failures THEN
        RAISE EXCEPTION USING ERRCODE = failure.code, MESSAGE = failure.message;
    END IF;
    RETURN true;
END;
$$;
CREATE VIEW public.snapshots AS
SELECT created_at, env_id, sandbox_id, id, metadata, base_env_id, sandbox_started_at,
       env_secure, origin_node_id, allow_internet_access, auto_pause, team_id, config, env_deleted_at
FROM public.snapshots_for_retry WHERE fail_snapshot_read();`)
	require.NoError(t, err)
	_, err = db.SqlcClient.Pool().Exec(ctx, "INSERT INTO snapshot_read_failure (failures, code, message) VALUES ($1, $2, $3)", failures, code, message)
	require.NoError(t, err)
	sem, err := sharedutils.NewAdjustableSemaphore(1)
	require.NoError(t, err)
	store := &APIStore{sqlcReadDB: db.SqlcClient, sandboxListSem: sem}

	return store, teamID, templateID
}

type limitRecorderDBTX struct {
	queries.DBTX

	limits []int32
	// onQuery runs before each snapshot query, with the limits of every query so far
	// including this one.
	onQuery func(limits []int32)
}

func (r *limitRecorderDBTX) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	if len(args) > 0 {
		if limit, ok := args[0].(int32); ok {
			r.limits = append(r.limits, limit)
		}
	}
	if r.onQuery != nil {
		r.onQuery(r.limits)
	}

	return r.DBTX.Query(ctx, sql, args...)
}

func pausedBatchLimitFlags(ctx context.Context, t *testing.T, limit int) *featureflags.Client {
	t.Helper()

	td := ldtestdata.DataSource()
	td.Update(td.Flag(featureflags.PausedSandboxListBatchLimit.Key()).ValueForAll(ldvalue.Int(limit)))
	ff, err := featureflags.NewClientWithDatasource(td)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ff.Close(context.WithoutCancel(ctx)) })

	return ff
}

//nolint:tparallel // subtests share a teamID fixture in public.snapshots and ascending order mutates a row
func TestGetPausedSandboxes_LargeRunningExcludeList(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	db := testutils.SetupDatabase(t)
	teamID := testutils.CreateTestTeam(t, db)
	templateID := testutils.CreateTestTemplate(t, db, teamID)

	baseTime := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

	// Create 260 snapshot rows in DB, spaced 1 minute apart (sbx-000 .. sbx-259).
	for i := range 260 {
		startedAt := baseTime.Add(time.Duration(i) * time.Minute)
		totalDiskSize := int64(1024)
		envdVersion := "v1.0.0"
		allowInternet := true
		snapTemplateID := fmt.Sprintf("snap-template-%03d", i)
		_, err := db.SqlcClient.UpsertSnapshot(ctx, queries.UpsertSnapshotParams{
			TemplateID:          snapTemplateID,
			TeamID:              teamID,
			SandboxID:           fmt.Sprintf("sbx-%03d", i),
			BaseTemplateID:      templateID,
			StartedAt:           pgtype.Timestamptz{Time: startedAt, Valid: true},
			Vcpu:                2,
			RamMb:               2048,
			TotalDiskSizeMb:     &totalDiskSize,
			Metadata:            dbtypes.JSONBStringMap{},
			KernelVersion:       "6.1.0",
			FirecrackerVersion:  "1.4.0",
			EnvdVersion:         &envdVersion,
			Secure:              true,
			AllowInternetAccess: &allowInternet,
			AutoPause:           true,
			OriginNodeID:        "test-node",
			Status:              dbtypes.BuildStatusSuccess,
		})
		require.NoError(t, err)
	}

	sem, err := sharedutils.NewAdjustableSemaphore(1)
	require.NoError(t, err)

	// 800 fake running IDs
	fakeIDs := make([]string, 0, 800)
	for i := range 800 {
		fakeIDs = append(fakeIDs, fmt.Sprintf("fake-running-%d", i))
	}

	// In SortDesc, newest first: sbx-259 down to sbx-000. Excluding the first 250 in
	// sort order, sbx-259 down to sbx-010, a queryLimit of 3 returns sbx-009, sbx-008
	// and sbx-007.
	descRunning := append([]string{}, fakeIDs...)
	for i := 259; i >= 10; i-- {
		descRunning = append(descRunning, fmt.Sprintf("sbx-%03d", i))
	}

	for _, maxRequest := range []int32{250, 150} { //nolint:paralleltest // subtests share a teamID fixture
		t.Run(fmt.Sprintf("flag caps every request at %d", maxRequest), func(t *testing.T) {
			ctx := t.Context()
			recorder := &limitRecorderDBTX{DBTX: db.SqlcClient.Pool()}
			subStore := &APIStore{
				sqlcReadDB:     &sqlcdb.Client{Queries: queries.New(recorder)},
				sandboxListSem: sem,
				featureFlags:   pausedBatchLimitFlags(ctx, t, int(maxRequest)),
			}

			cursorTime := baseTime.Add(24 * time.Hour)
			page, walkedTo, err := subStore.getPausedSandboxes(ctx, teamID, descRunning, nil, 3, cursorTime, utils.MaxSandboxID, utils.SortDesc, time.Time{}, nil)
			require.NoError(t, err)
			assert.Nil(t, walkedTo)
			require.Len(t, page, 3)
			assert.Equal(t, "sbx-009", page[0].SandboxID)
			assert.Equal(t, "sbx-008", page[1].SandboxID)
			assert.Equal(t, "sbx-007", page[2].SandboxID)

			for _, limit := range recorder.limits {
				assert.LessOrEqual(t, limit, maxRequest)
			}
			assert.Equal(t, maxRequest, recorder.limits[len(recorder.limits)-1], "the request finishing the page is the one the cap shrinks")
		})
	}

	t.Run("ascending order keeps a re-paused excluded row excluded", func(t *testing.T) { //nolint:paralleltest // subtests share a teamID fixture
		ctx := t.Context()

		// sbx-100 is in ASC's exclude list ([0..249]) and in the DESC subtests' exclude
		// list ([10..259]). Register cleanup immediately to restore its original timestamp
		// so a failure cannot leave the shared fixture modified.
		t.Cleanup(func() {
			_, restoreErr := db.SqlcClient.Pool().Exec(context.WithoutCancel(ctx),
				"UPDATE public.snapshots SET sandbox_started_at = $1 WHERE team_id = $2 AND sandbox_id = 'sbx-100'",
				baseTime.Add(100*time.Minute),
				teamID,
			)
			require.NoError(t, restoreErr)
		})

		// Once a query has read past sbx-100, move its started_at between sbx-250 and
		// sbx-251, as a pause between two statements does, so a later query meets it
		// again. It was excluded when first met and must stay excluded. Every query before
		// the last comes back full, so the rows read are the sum of their limits.
		moved := false
		recorder := &limitRecorderDBTX{DBTX: db.SqlcClient.Pool()}
		recorder.onQuery = func(limits []int32) {
			var read int32
			for _, limit := range limits[:len(limits)-1] {
				read += limit
			}
			if moved || read <= 100 {
				return
			}

			_, updateErr := db.SqlcClient.Pool().Exec(ctx,
				"UPDATE public.snapshots SET sandbox_started_at = $1 WHERE team_id = $2 AND sandbox_id = 'sbx-100'",
				baseTime.Add(250*time.Minute+30*time.Second),
				teamID,
			)
			require.NoError(t, updateErr)
			moved = true
		}

		subStore := &APIStore{
			sqlcReadDB:     &sqlcdb.Client{Queries: queries.New(recorder)},
			sandboxListSem: sem,
			featureFlags:   pausedBatchLimitFlags(ctx, t, 250),
		}

		// In SortAsc, oldest first: sbx-000 up to sbx-259. Excluding the first 250 in
		// sort order, sbx-000 up to sbx-249, a queryLimit of 3 returns sbx-250, sbx-251
		// and sbx-252.
		ascRunning := append([]string{}, fakeIDs...)
		for i := range 250 {
			ascRunning = append(ascRunning, fmt.Sprintf("sbx-%03d", i))
		}

		page, walkedTo, err := subStore.getPausedSandboxes(ctx, teamID, ascRunning, nil, 3, time.Time{}, "", utils.SortAsc, time.Time{}, nil)
		require.NoError(t, err)
		require.True(t, moved, "a query ran after the walk passed sbx-100")
		assert.Nil(t, walkedTo)
		require.Len(t, page, 3)
		assert.Equal(t, "sbx-250", page[0].SandboxID)
		assert.Equal(t, "sbx-251", page[1].SandboxID)
		assert.Equal(t, "sbx-252", page[2].SandboxID)
	})

	t.Run("flag unset asks for no more rows than one over-fetched request", func(t *testing.T) { //nolint:paralleltest // subtests share a teamID fixture
		recorder := &limitRecorderDBTX{DBTX: db.SqlcClient.Pool()}
		subStore := &APIStore{
			sqlcReadDB:     &sqlcdb.Client{Queries: queries.New(recorder)},
			sandboxListSem: sem,
		}

		cursorTime := baseTime.Add(24 * time.Hour)
		page, walkedTo, err := subStore.getPausedSandboxes(ctx, teamID, descRunning, nil, 3, cursorTime, utils.MaxSandboxID, utils.SortDesc, time.Time{}, nil)
		require.NoError(t, err)
		assert.Nil(t, walkedTo)
		require.Len(t, page, 3)
		assert.Equal(t, "sbx-009", page[0].SandboxID)
		assert.Equal(t, "sbx-008", page[1].SandboxID)
		assert.Equal(t, "sbx-007", page[2].SandboxID)

		var requested int32
		for _, limit := range recorder.limits {
			requested += limit
		}
		assert.Equal(t, int32(3), recorder.limits[0], "the first request asks for the page alone")
		assert.LessOrEqual(t, requested, int32(3+len(descRunning)))
	})
}
