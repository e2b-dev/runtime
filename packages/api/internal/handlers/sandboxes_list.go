package handlers

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
	"github.com/e2b-dev/infra/packages/api/internal/utils"
	"github.com/e2b-dev/infra/packages/auth/pkg/auth"
	"github.com/e2b-dev/infra/packages/db/pkg/retry"
	dbtypes "github.com/e2b-dev/infra/packages/db/pkg/types"
	"github.com/e2b-dev/infra/packages/db/queries"
	"github.com/e2b-dev/infra/packages/shared/pkg/consts"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/id"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
	sharedUtils "github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

const (
	sandboxesDefaultLimit = int32(100)
	sandboxesMaxLimit     = int32(100)

	// sandboxesUnboundedMaxCount caps GET /sandboxes, which returns every running
	// sandbox in one response; larger teams must page through GET /v2/sandboxes.
	sandboxesUnboundedMaxCount = 5000

	// headerTotalRunning is documented as present only when running sandboxes were
	// requested, so every write of it stays behind that check.
	headerTotalRunning = "X-Total-Running"
)

// parseSandboxListOrder maps the request's order parameter onto the keyset sort
// direction, defaulting to descending (newest first) when it is omitted. An
// unrecognized value is an error rather than a fallback to the default, so a typo
// cannot silently return the opposite page from the one that was asked for.
func parseSandboxListOrder(order *api.OrderDirection) (utils.SortDirection, error) {
	if order == nil {
		return utils.SortDesc, nil
	}

	if !order.Valid() {
		return utils.SortDesc, fmt.Errorf("unknown order %q", string(*order))
	}

	if *order == api.Asc {
		return utils.SortAsc, nil
	}

	return utils.SortDesc, nil
}

// parseSandboxListTemplateFilter validates a template filter value and returns the
// identifier ("namespace/alias" or a bare alias) to resolve. An explicit namespace
// is allowed so the filter can name a public template owned by another team; the
// listing itself stays scoped to the caller's team.
//
// A sandbox records only its base template, which carries no tag, so this filter
// cannot narrow to one tag. Reject a tagged value instead of accepting it and
// quietly returning sandboxes from every tag of the template.
func parseSandboxListTemplateFilter(template string) (string, error) {
	if _, _, hasTag := strings.Cut(template, id.TagSeparator); hasTag {
		return "", errors.New("tags are not supported here because sandboxes are not tag-scoped")
	}

	identifier, _, err := id.ParseName(template)
	if err != nil {
		return "", err
	}

	return identifier, nil
}

// parseSandboxListStartedAfter normalizes the startedAfter lower bound onto the
// microsecond grid every value it is compared against already sits on, and returns the
// zero time when no bound was given.
//
// The client's value keeps nanoseconds, but all three comparisons in a request are
// microsecond-aligned: FilterSandboxesOnStartedAtAndTemplate compares
// PaginationTimestamp, sandboxCanAppearInPausedPage truncates the live StartTime, and
// pgx floors a timestamptz on its way to Postgres (Unix()*1e6 + Nanosecond()/1000 on
// the binary path, an explicit truncation on the text path) before
// `sandbox_started_at >= @started_after` runs. An untruncated bound is therefore
// exclusive for running sandboxes and inclusive for paused ones: a client feeding a
// sandbox's own startedAt back -- the documented use of an at-or-after bound -- would
// drop that sandbox from the running half of the same response that returns it from
// the paused half, and undercount X-Total-Running by one.
func parseSandboxListStartedAfter(startedAfter *time.Time) time.Time {
	if startedAfter == nil {
		return time.Time{}
	}

	return startedAfter.Truncate(time.Microsecond)
}

// sandboxCanAppearInPausedPage reports whether a live sandbox could still be returned
// by the paused snapshot query under the request's filters, and therefore has to be
// excluded from that page to avoid listing the sandbox twice.
//
// Only a predicate that a live sandbox failing it proves its snapshot row fails too may
// be applied here, and for different reasons per filter:
//
//   - The base template matches exactly. base_env_id is write-once — UpsertSnapshot
//     leaves it out of its ON CONFLICT DO UPDATE list, and resume reads it back into
//     BaseTemplateID — so the row's template is the live sandbox's template.
//
//   - The start time only ever lags. sandbox_started_at is rewritten on every pause with
//     that run's start time, so after a resume the row still holds the previous run's
//     value while the live sandbox carries a fresh time.Now(). It is never ahead, so a
//     live sandbox below an at-or-after bound puts its row below the bound as well. The
//     bound and both sides of the comparison sit on the microsecond grid, which is what
//     keeps the boundary itself consistent. Note this direction is what makes the
//     narrowing correct: a startedBefore filter would invert it and could not be applied
//     here.
//
// Metadata deliberately is not applied — a live sandbox's metadata can differ from what
// its snapshot recorded, so a sandbox the metadata filter drops can still come back from
// the query and must stay excluded.
func sandboxCanAppearInPausedPage(sbx sandbox.Sandbox, startedAfter time.Time, templateID *string) bool {
	if templateID != nil && sbx.BaseTemplateID != *templateID {
		return false
	}

	if !startedAfter.IsZero() && sbx.StartTime.Truncate(time.Microsecond).Before(startedAfter) {
		return false
	}

	return true
}

const (
	// pausedPageEstimatedFetches is how many requests fetchPausedPage sizes from the
	// share of rows kept so far before it asks for enough rows to finish the page.
	pausedPageEstimatedFetches = 4

	// pausedPageMaxGrowth caps an estimated request at this multiple of the rows it
	// aims to keep. It is what a request asks for when almost nothing read was kept.
	pausedPageMaxGrowth = 8

	// pausedPageMaxFetches bounds the requests of one page. Only rows moved by writes
	// between requests take a page past pausedPageEstimatedFetches+1 of them.
	pausedPageMaxFetches = 2 * pausedPageEstimatedFetches
)

// snapshotFetch runs the snapshot cursor query once with the given limit and cursor.
type snapshotFetch func(ctx context.Context, params queries.GetSnapshotsWithCursorParams) ([]queries.GetSnapshotsWithCursorRow, error)

// pausedPageFetchesInChunks reports whether the paused page may be fetched in chunks.
// Without a metadata filter the query walks the keyset index and stops at its LIMIT, so
// a chunk costs about the rows it asks for. With one, the planner can read, join and sort
// every matching row before the LIMIT applies, so each extra chunk would repeat that
// whole cost and one over-fetched request is cheaper.
// A windowed path for metadata-filtered lists would extend this condition.
func pausedPageFetchesInChunks(metadataFilter *map[string]string) bool {
	return metadataFilter == nil || len(*metadataFilter) == 0
}

// fetchPausedPage returns up to params.Limit snapshot rows after the cursor, leaving out
// the sandboxes in exclude because they are listed from the live set instead. The
// exclusion happens here rather than in SQL, where a NOT (sandbox_id = ANY(array))
// predicate costs rows times array size.
//
// The pagination layer decides whether another page exists from the merged page's
// length, so this must return the full limit whenever that many rows remain: a short
// page would end pagination early and silently.
//
// Every request continues the keyset walk from the last row the database returned, kept
// or not, and asks for at most the rows still missing plus the excluded IDs not met yet.
// sandbox_id is unique, so if no row moves between requests, at most that many of the
// rows it reads can be excluded and a request of that size completes the page. An
// unchunked fetch makes only that request: the page plus one row per excluded ID.
//
// A chunked fetch first asks for the page alone, so live sandboxes outside the walked
// range cost nothing. Each later request is estimated from the share of rows kept so
// far, and after pausedPageEstimatedFetches it asks for the full bound above. Unless
// rows move between requests, it never reads more rows in total than the unchunked
// request would.
//
// Each request is its own statement, and a pause rewrites its snapshot's start time, so
// a row met once can be met again further along the walk. A row already kept is skipped
// when it is met again. A full request that still leaves the page short makes the walk
// continue rather than return a short page, up to pausedPageMaxFetches requests. A page
// still short after the last of them comes back with walkedTo, the last row read: every
// row up to it was kept or excluded, so the list continues after it on the next page
// instead of failing a request clients make in a loop. Only a request that comes back
// short ends the walk with a short page and no walkedTo, because the rows have run out.
//
// A positive maxRequest caps every request at that many rows, or at the missing rows
// when more are missing, chunked or not. A capped walk can use up its requests before
// the page fills even when no row moves, and then also returns walkedTo.
func fetchPausedPage(
	ctx context.Context,
	fetch snapshotFetch,
	params queries.GetSnapshotsWithCursorParams,
	exclude map[string]struct{},
	chunked bool,
	maxRequest int,
) (rows []queries.GetSnapshotsWithCursorRow, walkedTo *utils.Cursor, err error) {
	need := int(params.Limit)
	kept := make([]queries.GetSnapshotsWithCursorRow, 0, need)
	keptIDs := make(map[string]struct{}, need)
	met := make(map[string]struct{})
	read := 0

	for fetches := 1; fetches <= pausedPageMaxFetches; fetches++ {
		missing := need - len(kept)
		limit := missing + len(exclude) - len(met)
		if chunked && fetches <= pausedPageEstimatedFetches {
			limit = min(limit, estimatePausedFetch(missing, len(kept), read))
		}
		if maxRequest > 0 {
			limit = min(limit, max(missing, maxRequest))
		}
		params.Limit = int32(limit)

		batch, err := fetch(ctx, params)
		if err != nil {
			return nil, nil, err
		}
		read += len(batch)

		for _, row := range batch {
			id := row.Snapshot.SandboxID
			if _, excluded := exclude[id]; excluded {
				met[id] = struct{}{}

				continue
			}

			if _, listed := keptIDs[id]; listed {
				continue
			}

			keptIDs[id] = struct{}{}
			kept = append(kept, row)
		}

		if len(kept) >= need || len(batch) < limit {
			return kept[:min(len(kept), need)], nil, nil
		}

		last := batch[len(batch)-1].Snapshot
		params.CursorTime = last.SandboxStartedAt
		params.CursorID = last.SandboxID
	}

	return kept, &utils.Cursor{Time: params.CursorTime.Time, ID: params.CursorID}, nil
}

// estimatePausedFetch sizes an estimated request. The first one asks for the missing
// rows alone. Later ones aim to keep the missing rows plus a margin of about one
// standard deviation of the kept count, so the usual shortfall of an estimate does not
// cost another statement, and scale that by the share of rows read so far that were kept.
func estimatePausedFetch(missing, kept, read int) int {
	if read == 0 {
		return missing
	}

	target := missing + int(math.Sqrt(float64(missing))) + 1
	if kept*pausedPageMaxGrowth <= read {
		return target * pausedPageMaxGrowth
	}

	return (target*read + kept - 1) / kept
}

func (a *APIStore) getPausedSandboxes(
	ctx context.Context,
	teamID uuid.UUID,
	runningSandboxesIDs []string,
	metadataFilter *map[string]string,
	queryLimit int32,
	cursorTime time.Time,
	cursorID string,
	order utils.SortDirection,
	startedAfter time.Time,
	templateID *string,
) (sandboxes []utils.PaginatedSandbox, walkedTo *utils.Cursor, err error) {
	queryMetadata := dbtypes.JSONBStringMap{}
	if metadataFilter != nil {
		queryMetadata = *metadataFilter
	}

	excludeSet := make(map[string]struct{}, len(runningSandboxesIDs))
	for _, id := range runningSandboxesIDs {
		excludeSet[id] = struct{}{}
	}

	var maxRequest int
	if a.featureFlags != nil {
		maxRequest = a.featureFlags.IntFlag(ctx, featureflags.PausedSandboxListBatchLimit, featureflags.TeamContext(teamID.String()))
	}

	snapshots, walkedTo, err := a.throttledFetchPausedPage(ctx, order, templateID, queries.GetSnapshotsWithCursorParams{
		Limit:        queryLimit,
		TeamID:       teamID,
		Metadata:     queryMetadata,
		CursorTime:   pgtype.Timestamptz{Time: cursorTime, Valid: true},
		CursorID:     cursorID,
		StartedAfter: startedAfter,
	}, excludeSet, pausedPageFetchesInChunks(metadataFilter), maxRequest)
	if err != nil {
		return nil, nil, fmt.Errorf("error getting team snapshots: %w", err)
	}

	return snapshotsToPaginatedSandboxes(ctx, snapshots), walkedTo, nil
}

// throttledFetchPausedPage runs fetchPausedPage holding one slot of the sandbox list
// semaphore for the whole page. The page's statements run one after another, so the slot
// still bounds concurrent queries, and a page partway through does not wait for the
// semaphore again behind new requests. Each statement retries on recovery conflicts.
func (a *APIStore) throttledFetchPausedPage(
	ctx context.Context,
	order utils.SortDirection,
	templateID *string,
	params queries.GetSnapshotsWithCursorParams,
	exclude map[string]struct{},
	chunked bool,
	maxRequest int,
) ([]queries.GetSnapshotsWithCursorRow, *utils.Cursor, error) {
	if err := a.sandboxListSem.Acquire(ctx, 1); err != nil {
		return nil, nil, err
	}
	defer a.sandboxListSem.Release(1)

	fetch := func(ctx context.Context, params queries.GetSnapshotsWithCursorParams) ([]queries.GetSnapshotsWithCursorRow, error) {
		return retry.RetryOnRecoveryConflict(ctx, func(ctx context.Context) ([]queries.GetSnapshotsWithCursorRow, error) {
			return a.getSnapshots(ctx, order, templateID, params)
		})
	}

	return fetchPausedPage(ctx, fetch, params, exclude, chunked, maxRequest)
}

var errTooManyRunningSandboxes = fmt.Errorf("more than %d running sandboxes match", sandboxesUnboundedMaxCount)

func getRunningSandboxes(runningSandboxes []sandbox.Sandbox, metadataFilter *map[string]string, capped bool) ([]utils.PaginatedSandbox, error) {
	// Running Sandbox IDs
	runningSandboxList := instanceInfoToPaginatedSandboxes(runningSandboxes)

	// Filter sandboxes based on metadata
	runningSandboxList = utils.FilterSandboxesOnMetadata(runningSandboxList, metadataFilter)

	if capped && len(runningSandboxList) > sandboxesUnboundedMaxCount {
		return nil, errTooManyRunningSandboxes
	}

	return runningSandboxList, nil
}

func (a *APIStore) GetSandboxes(c *gin.Context, params api.GetSandboxesParams) {
	ctx := c.Request.Context()
	telemetry.ReportEvent(ctx, "list sandboxes")

	teamInfo := auth.MustGetTeamInfo(c)
	team := teamInfo.Team

	a.posthog.IdentifyAnalyticsTeam(ctx, team.ID.String(), team.Name)
	properties := a.posthog.GetPackageToPosthogProperties(&c.Request.Header)
	a.posthog.CreateAnalyticsTeamEvent(ctx, team.ID.String(), "listed sandboxes", properties)

	metadataFilter, err := utils.ParseMetadata(ctx, params.Metadata)
	if err != nil {
		logger.L().Error(ctx, "Error parsing metadata", zap.Error(err))
		a.sendAPIStoreError(c, http.StatusBadRequest, fmt.Sprintf("Error parsing metadata: %s", err))

		return
	}

	sandboxes, err := a.orchestrator.ListSandboxes(ctx, team.ID, []sandbox.State{sandbox.StateRunning})
	if err != nil {
		logger.L().Error(ctx, "Error getting sandboxes", zap.Error(err))
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Error when getting sandboxes")

		return
	}

	capped := a.featureFlags.BoolFlag(ctx, featureflags.UnpaginatedSandboxListCapFlag, featureflags.TeamContext(team.ID.String()))
	runningSandboxes, err := getRunningSandboxes(sandboxes, metadataFilter, capped)
	if err != nil {
		a.sendAPIStoreError(c, http.StatusBadRequest, fmt.Sprintf("Team has more than %d running sandboxes; update to the latest E2B SDK or list them with GET /v2/sandboxes", sandboxesUnboundedMaxCount))
		telemetry.ReportEvent(ctx, "sandbox list exceeds the unbounded limit")

		return
	}

	// Sort sandboxes by start time descending
	utils.SortPaginatedSandboxesDesc(runningSandboxes)

	c.JSON(http.StatusOK, runningSandboxes)
}

func (a *APIStore) GetV2Sandboxes(c *gin.Context, params api.GetV2SandboxesParams) {
	ctx := c.Request.Context()
	telemetry.ReportEvent(ctx, "list sandboxes")

	teamInfo := auth.MustGetTeamInfo(c)
	team := teamInfo.Team

	a.posthog.IdentifyAnalyticsTeam(ctx, team.ID.String(), team.Name)
	properties := a.posthog.GetPackageToPosthogProperties(&c.Request.Header)
	a.posthog.CreateAnalyticsTeamEvent(ctx, team.ID.String(), "listed sandboxes", properties)

	// If no state is provided we want to return both running and paused sandboxes
	states := make([]api.SandboxState, 0)
	if params.State == nil {
		states = append(states, api.Running, api.Paused)
	} else {
		states = append(states, *params.State...)
	}

	order, err := parseSandboxListOrder(params.Order)
	if err != nil {
		a.sendAPIStoreError(c, http.StatusBadRequest, fmt.Sprintf("Invalid order parameter: %s", err))

		return
	}

	// Initialize pagination
	pagination, err := utils.NewPagination[utils.PaginatedSandbox](
		utils.PaginationParams{
			Limit:     params.Limit,
			NextToken: params.NextToken,
		},
		utils.PaginationConfig{
			DefaultLimit: sandboxesDefaultLimit,
			MaxLimit:     sandboxesMaxLimit,
			DefaultID:    utils.MaxSandboxID,
			Order:        order,
		},
	)
	if err != nil {
		telemetry.ReportError(ctx, "error parsing pagination cursor", err)
		a.sendAPIStoreError(c, http.StatusBadRequest, "Invalid next token")

		return
	}

	metadataFilter, err := utils.ParseMetadata(ctx, params.Metadata)
	if err != nil {
		logger.L().Error(ctx, "Error parsing metadata", zap.Error(err))
		a.sendAPIStoreError(c, http.StatusBadRequest, "Error parsing metadata")

		return
	}

	startedAfter := parseSandboxListStartedAfter(params.StartedAfter)

	var templateIDFilter *string
	if params.Template != nil {
		identifier, err := parseSandboxListTemplateFilter(*params.Template)
		if err != nil {
			a.sendAPIStoreError(c, http.StatusBadRequest, fmt.Sprintf("Invalid template: %s", err))

			return
		}

		templateID, outcome := a.resolveTemplateFilter(c, identifier, team.Slug, "error resolving sandbox list template")
		switch outcome {
		case templateFilterResolved:
			templateIDFilter = &templateID
		case templateFilterNoMatch:
			// The header is documented as present only when running sandboxes were
			// requested, so a paused-only request must not see a running total here
			// either -- a client that keys off its presence would read the 0 as a
			// real count.
			if slices.Contains(states, api.Running) {
				c.Header(headerTotalRunning, "0")
			}

			c.JSON(http.StatusOK, []api.ListedSandbox{})

			return
		case templateFilterFailed:
			return
		}
	}

	// Get sandboxes with pagination
	sandboxes := make([]utils.PaginatedSandbox, 0)
	var pausedWalkedTo *utils.Cursor

	allSandboxes, err := a.orchestrator.ListSandboxes(ctx, team.ID, []sandbox.State{sandbox.StateRunning, sandbox.StatePausing})
	if err != nil {
		logger.L().Error(ctx, "Error getting sandboxes", zap.Error(err))
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Error when getting sandboxes")

		return
	}

	runningSandboxes := sharedUtils.Filter(allSandboxes, func(sbx sandbox.Sandbox) bool {
		return sbx.State == sandbox.StateRunning
	})
	pausingSandboxes := sharedUtils.Filter(allSandboxes, func(sbx sandbox.Sandbox) bool {
		return sbx.State == sandbox.StatePausing
	})

	if slices.Contains(states, api.Running) {
		runningSandboxList := instanceInfoToPaginatedSandboxes(runningSandboxes)

		// Filter based on metadata
		runningSandboxList = utils.FilterSandboxesOnMetadata(runningSandboxList, metadataFilter)
		runningSandboxList = utils.FilterSandboxesOnStartedAtAndTemplate(runningSandboxList, startedAfter, templateIDFilter)

		// Set the total (before we apply the limit, but already with all filters)
		c.Header(headerTotalRunning, strconv.Itoa(len(runningSandboxList)))

		// Filter based on cursor
		runningSandboxList = utils.FilterBasedOnCursor(runningSandboxList, pagination.CursorTime(), pagination.CursorID(), order)

		sandboxes = append(sandboxes, runningSandboxList...)
	}

	if slices.Contains(states, api.Paused) {
		// Live sandboxes are excluded from the paused page so none is listed twice.
		// Each excluded ID can cost getPausedSandboxes an extra row, and with a metadata
		// filter it is asked for up front, so a sandbox the paused query cannot return
		// anyway is worth leaving out.
		runningSandboxesIDs := make([]string, 0, len(runningSandboxes)+len(pausingSandboxes))
		for _, info := range runningSandboxes {
			if sandboxCanAppearInPausedPage(info, startedAfter, templateIDFilter) {
				runningSandboxesIDs = append(runningSandboxesIDs, info.SandboxID)
			}
		}
		for _, info := range pausingSandboxes {
			if sandboxCanAppearInPausedPage(info, startedAfter, templateIDFilter) {
				runningSandboxesIDs = append(runningSandboxesIDs, info.SandboxID)
			}
		}

		pausedSandboxList, walkedTo, err := a.getPausedSandboxes(ctx, team.ID, runningSandboxesIDs, metadataFilter, pagination.QueryLimit(), pagination.CursorTime(), pagination.CursorID(), order, startedAfter, templateIDFilter)
		pausedWalkedTo = walkedTo
		if err != nil {
			logger.L().Error(ctx, "Error getting paused sandboxes", zap.Error(err))
			a.sendAPIStoreError(c, http.StatusInternalServerError, "Error getting paused sandboxes")

			return
		}

		pausingSandboxList := instanceInfoToPaginatedSandboxes(pausingSandboxes)
		pausingSandboxList = utils.FilterSandboxesOnMetadata(pausingSandboxList, metadataFilter)
		pausingSandboxList = utils.FilterSandboxesOnStartedAtAndTemplate(pausingSandboxList, startedAfter, templateIDFilter)
		pausingSandboxList = utils.FilterBasedOnCursor(pausingSandboxList, pagination.CursorTime(), pagination.CursorID(), order)

		sandboxes = append(sandboxes, pausedSandboxList...)
		sandboxes = append(sandboxes, pausingSandboxList...)
	}

	if pausedWalkedTo != nil {
		logger.L().Warn(ctx, "Paused page stopped early, continuing on the next page", logger.WithTeamID(team.ID.String()))
		telemetry.ReportEvent(ctx, "paused page stopped early")
	}

	c.JSON(http.StatusOK, pageSandboxes(c, pagination, sandboxes, order, pausedWalkedTo))
}

func snapshotsToPaginatedSandboxes(ctx context.Context, snapshots []queries.GetSnapshotsWithCursorRow) []utils.PaginatedSandbox {
	sandboxes := make([]utils.PaginatedSandbox, 0)

	// Add snapshots to results
	for _, record := range snapshots {
		snapshot := record.Snapshot

		var alias *string
		if len(record.Aliases) > 0 {
			alias = &record.Aliases[0]
		}

		diskSize := int32(0)
		if record.BuildTotalDiskSizeMb != nil {
			diskSize = int32(*record.BuildTotalDiskSizeMb)
		} else {
			logger.L().Error(ctx, "disk size is not set for the sandbox", logger.WithSandboxID(snapshot.SandboxID))
		}

		envdVersion := ""
		if record.BuildEnvdVersion != nil {
			envdVersion = *record.BuildEnvdVersion
		} else {
			logger.L().Error(ctx, "envd version is not set for the sandbox", logger.WithSandboxID(snapshot.SandboxID))
		}

		sandbox := utils.PaginatedSandbox{
			ListedSandbox: api.ListedSandbox{
				ClientID:                consts.ClientID, // for backwards compatibility we need to return a client id
				Alias:                   alias,
				TemplateID:              snapshot.BaseEnvID,
				SandboxID:               snapshot.SandboxID,
				StartedAt:               snapshot.SandboxStartedAt.Time,
				CpuCount:                int32(record.BuildVcpu),
				MemoryMB:                int32(record.BuildRamMb),
				DiskSizeMB:              diskSize,
				EndAt:                   record.BuildCreatedAt,
				State:                   api.Paused,
				EnvdVersion:             envdVersion,
				EnvdUnresponsiveAtPause: envdUnresponsiveAtPause(record.BuildEnvdUnresponsive),
			},
			PaginationTimestamp: snapshot.SandboxStartedAt.Time,
		}

		if snapshot.Config != nil {
			sandbox.ListedSandbox.VolumeMounts = convertFromDBMountsToAPIMounts(snapshot.Config.VolumeMounts)
		}

		if snapshot.Metadata != nil {
			meta := api.SandboxMetadata(snapshot.Metadata)
			sandbox.Metadata = &meta
		}

		sandboxes = append(sandboxes, sandbox)
	}

	return sandboxes
}

// pageSandboxes sorts the merged running and paused sandboxes and cuts the page. When
// the paused walk stopped early at walkedTo, nothing past it is listed, since paused rows
// past it were not read, and the next page continues after it.
func pageSandboxes(
	c *gin.Context,
	pagination *utils.Pagination[utils.PaginatedSandbox],
	sandboxes []utils.PaginatedSandbox,
	order utils.SortDirection,
	walkedTo *utils.Cursor,
) []utils.PaginatedSandbox {
	utils.SortPaginatedSandboxes(sandboxes, order)
	if walkedTo != nil {
		sandboxes = utils.FilterUpToCursor(sandboxes, walkedTo.Time, walkedTo.ID, order)
	}

	return pagination.ProcessResultsWithHeaderContinuing(c, sandboxes, func(s utils.PaginatedSandbox) (time.Time, string) {
		return s.PaginationTimestamp, s.SandboxID
	}, walkedTo)
}

func instanceInfoToPaginatedSandboxes(runningSandboxes []sandbox.Sandbox) []utils.PaginatedSandbox {
	sandboxes := make([]utils.PaginatedSandbox, 0)

	// Add running sandboxes to results
	for _, info := range runningSandboxes {
		state := api.Running
		// If the sandbox is pausing, for the user it behaves like a paused sandbox - it can be resumed, etc.
		if info.State == sandbox.StatePausing {
			state = api.Paused
		}

		sandbox := utils.PaginatedSandbox{
			ListedSandbox: api.ListedSandbox{
				ClientID:     info.ClientID,
				TemplateID:   info.BaseTemplateID,
				Alias:        info.Alias,
				SandboxID:    info.SandboxID,
				StartedAt:    info.StartTime,
				CpuCount:     api.CPUCount(info.VCpu),
				MemoryMB:     api.MemoryMB(info.RamMB),
				DiskSizeMB:   api.DiskSizeMB(info.TotalDiskSizeMB),
				EndAt:        info.EndTime,
				State:        state,
				EnvdVersion:  info.EnvdVersion,
				VolumeMounts: convertFromDBMountsToAPIMounts(info.VolumeMounts),
			},
			// Paused snapshots come from Postgres at microsecond precision, but running
			// sandboxes carry nanosecond StartTime from time.Now(). Truncate only the
			// pagination key (not the public StartedAt) so the in-memory sort/cursor and
			// the SQL predicate agree at the running/paused boundary; otherwise asc
			// pagination can re-emit rows that share a truncated microsecond with the cursor.
			PaginationTimestamp: info.StartTime.Truncate(time.Microsecond),
		}

		if info.Metadata != nil {
			meta := api.SandboxMetadata(info.Metadata)
			sandbox.Metadata = &meta
		}

		sandboxes = append(sandboxes, sandbox)
	}

	return sandboxes
}

func convertFromDBMountsToAPIMounts(mounts []*dbtypes.SandboxVolumeMountConfig) *[]api.SandboxVolumeMount {
	results := make([]api.SandboxVolumeMount, 0, len(mounts))

	for _, item := range mounts {
		if item == nil {
			continue
		}

		results = append(results, api.SandboxVolumeMount{
			Name: item.Name,
			Path: item.Path,
		})
	}

	// this intentionally returns a pointer to the slice.
	// generated code adds `omitempty` for backwards compatibility reasons; we should always return a slice here.
	return &results
}

// The snapshot cursor query exists in four variants because sqlc emits one function
// per query and the template predicate has to be a literal `base_env_id = $n` for the
// planner to key the composite index on it. Each variant takes its own generated params
// type with its own field order, so the translations below cannot be plain struct
// conversions; keeping each field list in one named function stops the call site from
// copying them twice over.

func snapshotsAscParams(p queries.GetSnapshotsWithCursorParams) queries.GetSnapshotsWithCursorAscParams {
	return queries.GetSnapshotsWithCursorAscParams{
		Limit:        p.Limit,
		TeamID:       p.TeamID,
		Metadata:     p.Metadata,
		CursorTime:   p.CursorTime,
		CursorID:     p.CursorID,
		StartedAfter: p.StartedAfter,
	}
}

func snapshotsByTemplateParams(p queries.GetSnapshotsWithCursorParams, templateID string) queries.GetSnapshotsByTemplateWithCursorParams {
	return queries.GetSnapshotsByTemplateWithCursorParams{
		Limit:        p.Limit,
		TeamID:       p.TeamID,
		TemplateID:   templateID,
		Metadata:     p.Metadata,
		CursorTime:   p.CursorTime,
		CursorID:     p.CursorID,
		StartedAfter: p.StartedAfter,
	}
}

func snapshotsByTemplateAscParams(p queries.GetSnapshotsWithCursorParams, templateID string) queries.GetSnapshotsByTemplateWithCursorAscParams {
	return queries.GetSnapshotsByTemplateWithCursorAscParams{
		Limit:        p.Limit,
		TeamID:       p.TeamID,
		TemplateID:   templateID,
		Metadata:     p.Metadata,
		CursorTime:   p.CursorTime,
		CursorID:     p.CursorID,
		StartedAfter: p.StartedAfter,
	}
}

// getSnapshots runs the cursor snapshot query, picking the variant that matches the
// requested order and whether a template filter is set. The variants return
// identically-shaped rows, converted back to the descending row type so callers share a
// single conversion path; the compiler enforces that shape, and
// TestSnapshotCursorQueriesShareOneProjection enforces that the four queries still
// select it the same way. It reads from the read replica when one is configured: a
// snapshot paused or deleted a moment ago can lag there briefly.
func (a *APIStore) getSnapshots(ctx context.Context, order utils.SortDirection, templateID *string, params queries.GetSnapshotsWithCursorParams) ([]queries.GetSnapshotsWithCursorRow, error) {
	switch {
	case templateID == nil && order != utils.SortAsc:
		return a.sqlcReadDB.GetSnapshotsWithCursor(ctx, params)

	case templateID == nil:
		rows, err := a.sqlcReadDB.GetSnapshotsWithCursorAsc(ctx, snapshotsAscParams(params))
		if err != nil {
			return nil, err
		}

		return sharedUtils.Map(rows, func(row queries.GetSnapshotsWithCursorAscRow) queries.GetSnapshotsWithCursorRow {
			return queries.GetSnapshotsWithCursorRow(row)
		}), nil

	case order != utils.SortAsc:
		rows, err := a.sqlcReadDB.GetSnapshotsByTemplateWithCursor(ctx, snapshotsByTemplateParams(params, *templateID))
		if err != nil {
			return nil, err
		}

		return sharedUtils.Map(rows, func(row queries.GetSnapshotsByTemplateWithCursorRow) queries.GetSnapshotsWithCursorRow {
			return queries.GetSnapshotsWithCursorRow(row)
		}), nil

	default:
		rows, err := a.sqlcReadDB.GetSnapshotsByTemplateWithCursorAsc(ctx, snapshotsByTemplateAscParams(params, *templateID))
		if err != nil {
			return nil, err
		}

		return sharedUtils.Map(rows, func(row queries.GetSnapshotsByTemplateWithCursorAscRow) queries.GetSnapshotsWithCursorRow {
			return queries.GetSnapshotsWithCursorRow(row)
		}), nil
	}
}
