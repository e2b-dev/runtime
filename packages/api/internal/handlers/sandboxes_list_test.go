package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	analyticscollector "github.com/e2b-dev/infra/packages/api/internal/analytics_collector"
	"github.com/e2b-dev/infra/packages/api/internal/api"
	templatecache "github.com/e2b-dev/infra/packages/api/internal/cache/templates"
	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
	"github.com/e2b-dev/infra/packages/api/internal/utils"
	"github.com/e2b-dev/infra/packages/auth/pkg/auth"
	authtypes "github.com/e2b-dev/infra/packages/auth/pkg/types"
	authqueries "github.com/e2b-dev/infra/packages/db/pkg/auth/queries"
	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
	redis_utils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
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
		Team: &authqueries.Team{ID: teamID, Slug: teamSlug},
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
