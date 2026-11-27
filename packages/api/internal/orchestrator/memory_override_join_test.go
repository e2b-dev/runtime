package orchestrator

import (
	"context"
	"errors"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/launchdarkly/go-server-sdk/v7/testhelpers/ldtestdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/clusters"
	"github.com/e2b-dev/infra/packages/api/internal/db/queries"
	"github.com/e2b-dev/infra/packages/api/internal/orchestrator/nodemanager"
	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
	teamtypes "github.com/e2b-dev/infra/packages/auth/pkg/types"
	"github.com/e2b-dev/infra/packages/shared/pkg/apierrors"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/middleware/otel/joined"
	redis_utils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
)

// joinSignal reports each request that starts waiting on a start another
// request holds, and runs afterWait (when set) once each wait ends, before the
// joiner acts on the start's result.
type joinSignal struct {
	sandbox.ReservationStorage

	waiting   chan string
	waits     atomic.Int32
	afterWait func(wait int32)
}

func (j *joinSignal) WaitForStart(ctx context.Context, teamID uuid.UUID, sandboxID string) ([]byte, error) {
	select {
	case j.waiting <- sandboxID:
	default:
	}

	result, err := j.ReservationStorage.WaitForStart(ctx, teamID, sandboxID)
	wait := j.waits.Add(1)
	if j.afterWait != nil {
		j.afterWait(wait)
	}

	return result, err
}

// newJoinTestOrchestrator is an orchestrator that can place growable starts,
// counts its memory decisions into the returned reader, and reports requests
// that join a start.
func newJoinTestOrchestrator(t *testing.T, resizable bool) (*Orchestrator, *joinSignal, *sdkmetric.ManualReader) {
	t.Helper()

	source := ldtestdata.DataSource()
	source.Update(source.Flag(featureflags.ResizableTemplatesFlag.Key()).VariationForAll(resizable))
	signal := &joinSignal{waiting: make(chan string, 16)}
	o := newCreateSandboxTestOrchestratorWrapping(t, redis_utils.SetupInstance(t), source,
		func(r sandbox.ReservationStorage) sandbox.ReservationStorage {
			signal.ReservationStorage = r

			return signal
		},
		nodemanager.WithOrchestratorVersion(clusters.SandboxMemoryResizeMinVersion.String()))

	reader := sdkmetric.NewManualReader()
	meter := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("github.com/e2b-dev/infra/packages/api/internal/orchestrator")
	counter, err := meter.Int64Counter("api.sandbox.memory_override")
	require.NoError(t, err)
	o.memoryOverrideCounter = counter
	o.joinExtensionFailedCounter, err = meter.Int64Counter("api.sandbox.connect_join_extension_failed")
	require.NoError(t, err)
	// On-demand node discovery finds nothing, so a sandbox on a node the test
	// deregisters cannot have its timeout extended.
	o.clusters = clusters.NewTestPool()
	o.nodeDiscovery = &countingDiscovery{}

	return o, signal, reader
}

type createResult struct {
	sbx    sandbox.Sandbox
	apiErr *api.APIError
}

// heldStart is a resume that holds the sandbox's start until released, then
// fetches meta (or fails with metaErr).
type heldStart struct {
	entered chan struct{}
	release chan struct{}
	done    chan createResult
}

func holdStart(ctx context.Context, o *Orchestrator, team *teamtypes.Team, sandboxID string, meta SandboxMetadata, metaErr *api.APIError, timeout time.Duration, options ...CreateOption) *heldStart {
	h := &heldStart{entered: make(chan struct{}), release: make(chan struct{}), done: make(chan createResult, 1)}
	go func() {
		now := time.Now()
		sbx, apiErr := o.CreateSandbox(ctx, sandboxID, uuid.NewString(), team,
			func(context.Context) (SandboxMetadata, *api.APIError) {
				close(h.entered)
				<-h.release

				return meta, metaErr
			}, now, now.Add(timeout), timeout, true, false, sandbox.CreationMetadata{IsResume: true}, options...)
		h.done <- createResult{sbx: sbx, apiErr: apiErr}
	}()

	return h
}

// resume runs a resume of sandboxID that fetches meta when it holds the start.
func resume(ctx context.Context, o *Orchestrator, team *teamtypes.Team, sandboxID string, meta SandboxMetadata, timeout time.Duration, coldBoot bool, options ...CreateOption) createResult {
	now := time.Now()
	sbx, apiErr := o.CreateSandbox(ctx, sandboxID, uuid.NewString(), team,
		func(context.Context) (SandboxMetadata, *api.APIError) { return meta, nil },
		now, now.Add(timeout), timeout, true, coldBoot, sandbox.CreationMetadata{IsResume: true}, options...)

	return createResult{sbx: sbx, apiErr: apiErr}
}

// joinHeld sends a request while h holds the start, ends the start once the
// request waits on it or has already answered, and returns both results.
func joinHeld(t *testing.T, signal *joinSignal, h *heldStart, request func() createResult) (joiner, starter createResult) {
	t.Helper()

	<-h.entered
	answered := make(chan createResult, 1)
	go func() { answered <- request() }()

	select {
	case <-signal.waiting:
		close(h.release)
		joiner = <-answered
	case joiner = <-answered:
		close(h.release)
	case <-time.After(20 * time.Second):
		close(h.release)
		require.Fail(t, "the request neither joined the start nor answered")
	}

	select {
	case starter = <-h.done:
	case <-time.After(20 * time.Second):
		require.Fail(t, "the held start did not end")
	}

	return joiner, starter
}

func meta(build queries.EnvBuild) SandboxMetadata {
	return SandboxMetadata{TemplateID: "tpl", BaseTemplateID: "base-tpl", Build: build}
}

func newSandboxID() string {
	return "sbx-memjoin-" + uuid.NewString()[:8]
}

// A resume naming memoryMB that meets a start already in flight is refused, and
// counted rejected with the in-flight code; so is a connect demanding a cold
// boot, which must not ride the memory restore in flight. A create inheriting a
// size above the team's limit counts as an inherited refusal.
func TestCreateSandbox_MemoryJoinRefusals(t *testing.T) {
	t.Parallel()

	o, signal, reader := newJoinTestOrchestrator(t, true)
	team := memoryTeam()
	resumeCtx := WithMemoryRoute(t.Context(), MemoryRouteResume)
	connectCtx := WithMemoryRoute(t.Context(), MemoryRouteConnect)

	sandboxID := newSandboxID()
	h := holdStart(resumeCtx, o, team, sandboxID, meta(growableBuild(4096, 1024)), nil, time.Hour)
	joiner, starter := joinHeld(t, signal, h, func() createResult {
		return resume(resumeCtx, o, team, sandboxID, meta(growableBuild(4096, 1024)), time.Hour, false, WithMemoryMB(new(int32(2048))))
	})
	require.Nil(t, starter.apiErr)
	require.NotNil(t, joiner.apiErr)
	assert.Equal(t, http.StatusConflict, joiner.apiErr.Code)
	assert.Equal(t, ErrCodeStartInFlight, joiner.apiErr.ErrorCode)

	sandboxID = newSandboxID()
	h = holdStart(resumeCtx, o, team, sandboxID, meta(growableBuild(4096, 1024)), nil, time.Hour)
	joiner, starter = joinHeld(t, signal, h, func() createResult {
		return resume(connectCtx, o, team, sandboxID, meta(growableBuild(4096, 1024)), time.Hour, true, WithMemoryMB(new(int32(3072))), Connect())
	})
	require.Nil(t, starter.apiErr)
	require.NotNil(t, joiner.apiErr)
	assert.Equal(t, ErrCodeStartInFlight, joiner.apiErr.ErrorCode)

	assert.Equal(t, int64(1), memoryOverrideCount(t, reader, map[string]string{"route": MemoryRouteResume, "outcome": memoryOverrideRejected, "error_code": ErrCodeStartInFlight}))
	assert.Equal(t, int64(1), memoryOverrideCount(t, reader, map[string]string{"route": MemoryRouteConnect, "outcome": memoryOverrideRejected, "error_code": ErrCodeStartInFlight}))

	limited := memoryTeam()
	limited.Limits.MaxRamMb = 2048
	now := time.Now()
	_, inheritedErr := o.CreateSandbox(t.Context(), newSandboxID(), uuid.NewString(), limited,
		func(context.Context) (SandboxMetadata, *api.APIError) { return meta(growableBuild(4096, 3072)), nil },
		now, now.Add(time.Hour), time.Hour, false, false, sandbox.CreationMetadata{})
	require.NotNil(t, inheritedErr)
	assert.Equal(t, errCodeSandboxMemoryTeamLimit, inheritedErr.ErrorCode)
	assert.Equal(t, int64(1), memoryOverrideCount(t, reader, map[string]string{"outcome": memoryOverrideRejected, "error_code": errCodeSandboxMemoryTeamLimit, "source": memorySourceInherited}))
}

// A connect's size is checked only when the connect holds the start. Holding
// it, a size the snapshot or the team cannot take is refused with the check's
// code, before any node is asked.
func TestCreateSandbox_ConnectMemoryCheckedWhenItStarts(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name      string
		resizable bool
		build     queries.EnvBuild
		requested int32
		wantCode  string
	}{
		{name: "resizable templates off", build: growableBuild(4096, 1024), requested: 2048, wantCode: errCodeSandboxMemoryDisabled},
		{name: "above the maximum", resizable: true, build: growableBuild(4096, 1024), requested: 8192, wantCode: errCodeSandboxMemoryOutOfRange},
		{name: "shrink below the paused size", resizable: true, build: growableBuild(4096, 3072), requested: 2048, wantCode: errCodeSandboxMemoryShrinkUnsupported},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			o, _, reader := newJoinTestOrchestrator(t, tt.resizable)
			ctx := WithMemoryRoute(t.Context(), MemoryRouteConnect)

			r := resume(ctx, o, memoryTeam(), newSandboxID(), meta(tt.build), time.Hour, false, WithMemoryMB(&tt.requested), Connect())

			require.NotNil(t, r.apiErr)
			assert.Equal(t, http.StatusBadRequest, r.apiErr.Code)
			assert.Equal(t, tt.wantCode, r.apiErr.ErrorCode)
			assert.Equal(t, int64(1), memoryOverrideCount(t, reader, map[string]string{"route": MemoryRouteConnect, "outcome": memoryOverrideRejected, "error_code": tt.wantCode}))
		})
	}
}

// A connect that joins another request's start ignores its size, whatever the
// size and whether the team may choose one: never a 400 for a size that does
// not apply. (The connect handler refuses a size the snapshot or the team
// cannot take before it gets here; this pins the start's own rule.) It gets the sandbox that start produced, at that start's size,
// for its own timeout, and counts as ignored once the start succeeded.
func TestCreateSandbox_ConnectJoinIgnoresItsSize(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name      string
		resizable bool
		requested *int32
	}{
		{name: "no size", resizable: true},
		{name: "a size", resizable: true, requested: new(int32(3072))},
		{name: "above the maximum", resizable: true, requested: new(int32(8192))},
		{name: "below the size started with", resizable: true, requested: new(int32(1024))},
		{name: "resizable templates off", requested: new(int32(3072))},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			o, signal, reader := newJoinTestOrchestrator(t, tt.resizable)
			team := memoryTeam()
			sandboxID := newSandboxID()
			build := testBuild()
			if tt.resizable {
				build = growableBuild(4096, 2048)
			}
			h := holdStart(t.Context(), o, team, sandboxID, meta(build), nil, time.Minute)
			joiner, starter := joinHeld(t, signal, h, func() createResult {
				return resume(WithMemoryRoute(t.Context(), MemoryRouteConnect), o, team, sandboxID, meta(build), 2*time.Hour, false, WithMemoryMB(tt.requested), Connect())
			})

			require.Nil(t, starter.apiErr)
			require.Nil(t, joiner.apiErr)
			assert.Equal(t, starter.sbx.TotalMemoryMB(), joiner.sbx.TotalMemoryMB(), "the joined start's size, not the requested one")
			assert.WithinDuration(t, time.Now().Add(2*time.Hour), joiner.sbx.EndTime, time.Minute, "the connect's own timeout")
			stored, err := o.GetSandbox(t.Context(), team.ID, sandboxID)
			require.NoError(t, err)
			assert.WithinDuration(t, time.Now().Add(2*time.Hour), stored.EndTime, time.Minute)

			wantIgnored := int64(0)
			if tt.requested != nil {
				wantIgnored = 1
			}
			assert.Equal(t, wantIgnored, memoryOverrideCount(t, reader, map[string]string{"route": MemoryRouteConnect, "outcome": memoryOverrideIgnored}))
			assert.Zero(t, memoryOverrideCount(t, reader, map[string]string{"route": MemoryRouteConnect, "outcome": memoryOverrideRejected}))
		})
	}
}

// A connect naming a size that joins a start which then fails gets that
// failure and counts as failed, not ignored.
func TestCreateSandbox_ConnectJoinOfAFailedStartCountsAsFailed(t *testing.T) {
	t.Parallel()

	o, signal, reader := newJoinTestOrchestrator(t, true)
	team := memoryTeam()
	sandboxID := newSandboxID()
	h := holdStart(t.Context(), o, team, sandboxID, SandboxMetadata{}, &api.APIError{Code: http.StatusInternalServerError, ErrorCode: "test_start_failed", ClientMsg: "start failed", Err: errors.New("start failed")}, time.Hour)
	joiner, starter := joinHeld(t, signal, h, func() createResult {
		return resume(WithMemoryRoute(t.Context(), MemoryRouteConnect), o, team, sandboxID, meta(growableBuild(4096, 1024)), time.Hour, false, WithMemoryMB(new(int32(2048))), Connect())
	})

	require.NotNil(t, starter.apiErr)
	require.NotNil(t, joiner.apiErr)
	assert.Equal(t, "test_start_failed", joiner.apiErr.ErrorCode)
	assert.Equal(t, int64(1), memoryOverrideCount(t, reader, map[string]string{"route": MemoryRouteConnect, "outcome": memoryOverrideFailed, "error_code": "test_start_failed"}))
	assert.Zero(t, memoryOverrideCount(t, reader, map[string]string{"route": MemoryRouteConnect, "outcome": memoryOverrideIgnored}))
}

// When a start that named a size fails, a request that joined it does not take
// a failure that may be the size's: it starts the sandbox again on its own
// terms, here at the build's size.
func TestCreateSandbox_SizedStartFailureRestartsItsJoiners(t *testing.T) {
	t.Parallel()

	o, signal, _ := newJoinTestOrchestrator(t, true)
	team := memoryTeam()
	sandboxID := newSandboxID()
	build := growableBuild(4096, 1024)
	h := holdStart(WithMemoryRoute(t.Context(), MemoryRouteConnect), o, team, sandboxID, meta(build), nil, time.Hour,
		WithMemoryMB(new(int32(8192))), Connect())
	joiner, starter := joinHeld(t, signal, h, func() createResult {
		return resume(t.Context(), o, team, sandboxID, meta(build), time.Hour, false)
	})

	require.NotNil(t, starter.apiErr, "the sized start fails its own size check")
	assert.Equal(t, errCodeSandboxMemoryOutOfRange, starter.apiErr.ErrorCode)
	require.Nil(t, joiner.apiErr, "the joiner starts the sandbox itself")
	assert.Equal(t, sandboxID, joiner.sbx.SandboxID)
	assert.Equal(t, int64(1024), joiner.sbx.TotalMemoryMB())
}

// The connect to a running sandbox the handler reports counts as ignored.
func TestRecordConnectMemoryIgnored(t *testing.T) {
	t.Parallel()

	reader := sdkmetric.NewManualReader()
	counter, err := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("github.com/e2b-dev/infra/packages/api/internal/orchestrator").Int64Counter("api.sandbox.memory_override")
	require.NoError(t, err)
	o := &Orchestrator{memoryOverrideCounter: counter}

	o.RecordConnectMemoryIgnored(WithMemoryRoute(t.Context(), MemoryRouteConnect))

	assert.Equal(t, int64(1), memoryOverrideCount(t, reader, map[string]string{"route": MemoryRouteConnect, "outcome": memoryOverrideIgnored, "source": memorySourceNamed}))
}

// memoryOverrideCount sums api.sandbox.memory_override over the points whose
// attributes match every key in want (an empty value matches an absent one).
func memoryOverrideCount(t *testing.T, reader *sdkmetric.ManualReader, want map[string]string) int64 {
	t.Helper()

	return metricCount(t, reader, "api.sandbox.memory_override", want)
}

// metricCount sums the named counter's datapoints whose attributes include want.
func metricCount(t *testing.T, reader *sdkmetric.ManualReader, name string, want map[string]string) int64 {
	t.Helper()

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))
	var total int64
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			require.True(t, ok)
		points:
			for _, dp := range sum.DataPoints {
				for key, value := range want {
					got, _ := dp.Attributes.Value(attribute.Key(key))
					if got.AsString() != value {
						continue points
					}
				}
				total += dp.Value
			}
		}
	}

	return total
}

// A start that named a memory size and failed for any reason but the size's
// refusal hands its joiners its own error, and they do not start the sandbox
// again: the failure may have come after the sandbox was created, while it is
// still being removed.
func TestCreateSandbox_SizedStartOtherFailureReachesItsJoiners(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name    string
		options []CreateOption
	}{
		{name: "resume naming memoryMB", options: []CreateOption{WithMemoryMB(new(int32(2048)))}},
		{name: "connect naming memoryMB", options: []CreateOption{WithMemoryMB(new(int32(2048))), Connect()}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			o, signal, _ := newJoinTestOrchestrator(t, true)
			team := memoryTeam()
			sandboxID := newSandboxID()
			startErr := &api.APIError{Code: http.StatusInternalServerError, ErrorCode: "test_start_failed", ClientMsg: "start failed", Err: errors.New("start failed"), Retryable: apierrors.NoRetry}
			h := holdStart(WithMemoryRoute(t.Context(), MemoryRouteConnect), o, team, sandboxID, SandboxMetadata{}, startErr, time.Hour, tt.options...)
			joiner, starter := joinHeld(t, signal, h, func() createResult {
				return resume(t.Context(), o, team, sandboxID, meta(growableBuild(4096, 1024)), time.Hour, false, Connect())
			})

			require.NotNil(t, starter.apiErr)
			require.NotNil(t, joiner.apiErr, "the joiner does not start the sandbox again")
			assert.Equal(t, "test_start_failed", joiner.apiErr.ErrorCode)
			assert.Equal(t, int32(1), signal.waits.Load(), "the joiner waited once and did not start the sandbox itself")
			_, err := o.GetSandbox(t.Context(), team.ID, sandboxID)
			require.Error(t, err, "no sandbox was started")
		})
	}
}

// deregisterSandboxNode removes the node the sandbox runs on from the pool, so
// its timeout can no longer be extended.
func deregisterSandboxNode(t *testing.T, o *Orchestrator, team *teamtypes.Team, sandboxID string) {
	t.Helper()

	rec, err := o.GetSandbox(context.WithoutCancel(t.Context()), team.ID, sandboxID)
	require.NoError(t, err)
	node := o.GetNode(rec.ClusterID, rec.NodeID)
	require.NotNil(t, node)
	o.deregisterNode(node)
}

// A connect that joined a start whose sandbox is running answers with that
// sandbox even when its own timeout extension fails, and counts the failed
// extension; a sandbox that stopped running before the extension is returned
// without counting one.
func TestCreateSandbox_JoinedConnectKeepAlive(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name          string
		requested     *int32
		afterWait     func(t *testing.T, o *Orchestrator, team *teamtypes.Team, sandboxID string)
		wantExtended  bool
		wantExtFailed int64
	}{
		{name: "extension succeeds", requested: new(int32(3072)), wantExtended: true},
		{name: "extension fails, sized connect", requested: new(int32(3072)), afterWait: deregisterSandboxNode, wantExtFailed: 1},
		{name: "extension fails, plain connect", afterWait: deregisterSandboxNode, wantExtFailed: 1},
		{name: "sandbox stopped running", requested: new(int32(3072)), afterWait: func(t *testing.T, o *Orchestrator, team *teamtypes.Team, sandboxID string) {
			t.Helper()
			_, err := o.sandboxStore.Update(context.WithoutCancel(t.Context()), team.ID, sandboxID, func(s sandbox.Sandbox) (sandbox.Sandbox, error) {
				s.State = sandbox.StatePausing

				return s, nil
			})
			require.NoError(t, err)
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			o, signal, reader := newJoinTestOrchestrator(t, true)
			team := memoryTeam()
			sandboxID := newSandboxID()
			build := growableBuild(4096, 2048)
			if tt.afterWait != nil {
				signal.afterWait = func(int32) { tt.afterWait(t, o, team, sandboxID) }
			}
			h := holdStart(t.Context(), o, team, sandboxID, meta(build), nil, time.Minute)
			joiner, starter := joinHeld(t, signal, h, func() createResult {
				return resume(WithMemoryRoute(t.Context(), MemoryRouteConnect), o, team, sandboxID, meta(build), 2*time.Hour, false, WithMemoryMB(tt.requested), Connect())
			})

			require.Nil(t, starter.apiErr)
			require.Nil(t, joiner.apiErr, "the connect answers with the sandbox it joined")
			assert.Equal(t, sandboxID, joiner.sbx.SandboxID)
			if tt.wantExtended {
				assert.WithinDuration(t, time.Now().Add(2*time.Hour), joiner.sbx.EndTime, time.Minute, "the connect's own timeout")
			} else {
				assert.WithinDuration(t, time.Now().Add(time.Minute), joiner.sbx.EndTime, time.Minute, "the joined start's timeout")
			}
			assert.Equal(t, tt.wantExtFailed, metricCount(t, reader, "api.sandbox.connect_join_extension_failed", nil))
			wantIgnored := int64(0)
			if tt.requested != nil {
				wantIgnored = 1
			}
			assert.Equal(t, wantIgnored, memoryOverrideCount(t, reader, map[string]string{"route": MemoryRouteConnect, "outcome": memoryOverrideIgnored}))
			assert.Zero(t, memoryOverrideCount(t, reader, map[string]string{"outcome": memoryOverrideFailed}))
		})
	}
}

// A request restarts a sandbox after a sized start it joined had its size
// refused at most once: when the start it then joins is refused its size too,
// it answers 503 sandbox_sized_start_failed, retryable.
func TestCreateSandbox_SecondSizedRefusalAnswersRetryable(t *testing.T) {
	t.Parallel()

	o, signal, _ := newJoinTestOrchestrator(t, true)
	restarts := countJoinerRestarts(t, o)
	team := memoryTeam()
	sandboxID := newSandboxID()
	build := growableBuild(4096, 1024)
	sized := []CreateOption{WithMemoryMB(new(int32(8192))), Connect()}
	sizedCtx := WithMemoryRoute(t.Context(), MemoryRouteConnect)

	// Once the joiner's first wait ends, a second sized start takes the
	// sandbox before the joiner restarts it.
	second := make(chan *heldStart, 1)
	signal.afterWait = func(wait int32) {
		if wait != 1 {
			return
		}
		h2 := holdStart(sizedCtx, o, team, sandboxID, meta(build), nil, time.Hour, sized...)
		<-h2.entered
		second <- h2
	}
	h1 := holdStart(sizedCtx, o, team, sandboxID, meta(build), nil, time.Hour, sized...)
	<-h1.entered

	answered := make(chan createResult, 1)
	go func() { answered <- resume(t.Context(), o, team, sandboxID, meta(build), time.Hour, false) }()

	<-signal.waiting
	close(h1.release)
	h2 := <-second
	select {
	case <-signal.waiting:
	case <-time.After(20 * time.Second):
		require.Fail(t, "the joiner did not join the second start")
	}
	close(h2.release)

	var joiner createResult
	select {
	case joiner = <-answered:
	case <-time.After(20 * time.Second):
		require.Fail(t, "the joiner did not answer")
	}
	require.NotNil(t, joiner.apiErr)
	assert.Equal(t, http.StatusServiceUnavailable, joiner.apiErr.Code)
	assert.Equal(t, errCodeSizedStartFailed, joiner.apiErr.ErrorCode)
	assert.Equal(t, apierrors.CanRetry, joiner.apiErr.Retryable)
	assert.Equal(t, int64(1), metricCount(t, restarts, "api.sandbox.start_joiner_restart", map[string]string{"outcome": joinerRestarted}))
	assert.Equal(t, int64(1), metricCount(t, restarts, "api.sandbox.start_joiner_restart", map[string]string{"outcome": joinerGaveUp}))
	for _, h := range []*heldStart{h1, h2} {
		select {
		case r := <-h.done:
			require.NotNil(t, r.apiErr)
			assert.Equal(t, errCodeSandboxMemoryOutOfRange, r.apiErr.ErrorCode)
		case <-time.After(20 * time.Second):
			require.Fail(t, "a held start did not end")
		}
	}
}

// A request is labelled as joined only when it ends up riding another
// request's start; one that restarts the sandbox after a refused sized start
// is not.
func TestCreateSandbox_JoinedLabel(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name       string
		starter    []CreateOption
		wantJoined bool
	}{
		{name: "rides the start", wantJoined: true},
		{name: "restarts after a refused size", starter: []CreateOption{WithMemoryMB(new(int32(8192))), Connect()}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			o, signal, _ := newJoinTestOrchestrator(t, true)
			team := memoryTeam()
			sandboxID := newSandboxID()
			build := growableBuild(4096, 1024)
			h := holdStart(WithMemoryRoute(t.Context(), MemoryRouteConnect), o, team, sandboxID, meta(build), nil, time.Hour, tt.starter...)
			ctx := joined.WithHolder(t.Context())
			joiner, _ := joinHeld(t, signal, h, func() createResult {
				return resume(ctx, o, team, sandboxID, meta(build), time.Hour, false)
			})

			require.Nil(t, joiner.apiErr)
			assert.Equal(t, tt.wantJoined, joined.Attribute(ctx).Value.AsBool())
		})
	}
}
