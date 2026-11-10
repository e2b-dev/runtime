package evictor

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/launchdarkly/go-sdk-common/v3/ldvalue"
	"github.com/launchdarkly/go-server-sdk/v7/testhelpers/ldtestdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	"github.com/e2b-dev/infra/packages/api/internal/pause"
	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

// unquiescedNode answers the evictor as the API does for a sandbox whose guest
// offers no filesystem quiesce: a
// filesystem-only pause is refused as unsupported (and the record restored),
// a memory pause gets memoryErr.
type unquiescedNode struct {
	memoryErr error
	calls     []sandbox.RemoveOpts
}

func (n *unquiescedNode) remove(_ context.Context, _ uuid.UUID, _ string, opts sandbox.RemoveOpts) error {
	n.calls = append(n.calls, opts)
	if opts.FilesystemOnly {
		return sandbox.PauseUnsupportedError{}
	}

	return n.memoryErr
}

func (n *unquiescedNode) kinds() []bool {
	kinds := make([]bool, 0, len(n.calls))
	for _, c := range n.calls {
		kinds = append(kinds, c.FilesystemOnly)
	}

	return kinds
}

// refusingEvictor degrades at the first refusal (a zero overstay budget), the
// configuration in which a refused memory pause is retried filesystem-only.
func refusingEvictor(t *testing.T, node *unquiescedNode) *Evictor {
	t.Helper()

	td := ldtestdata.DataSource()
	td.Update(td.Flag(featureflags.AutoPauseOverstayBudgetMs.Key()).ValueForAll(ldvalue.Int(0)))
	flags, err := featureflags.NewClientWithDatasource(td)
	require.NoError(t, err)
	t.Cleanup(func() { _ = flags.Close(context.WithoutCancel(t.Context())) })
	counter, err := telemetry.GetCounter(noop.NewMeterProvider().Meter("github.com/e2b-dev/infra/packages/api/internal/orchestrator/evictor"), telemetry.ApiEvictorFsOnlyAutoPause)
	require.NoError(t, err)

	return &Evictor{featureFlags: flags, fsOnlyAutoPauseCounter: counter, degradedCounter: counter, removeSandbox: node.remove}
}

func expiredUnquiescedSandbox(fsOnly bool) sandbox.Sandbox {
	return sandbox.Sandbox{
		SandboxID:               "sbx-unquiesced",
		TeamID:                  uuid.New(),
		ExecutionID:             "exec-1",
		State:                   sandbox.StateRunning,
		AutoPause:               true,
		AutoPauseFilesystemOnly: fsOnly,
		FirecrackerVersion:      "v1.18.0-dev_4bb5b4de0",
		EndTime:                 time.Now().Add(-time.Minute),
	}
}

// An expired sandbox of such a guest with a filesystem-only auto-pause policy
// is never paused with memory: the node refuses the capture at create, so a
// refusal here is a skew between the two, and the sweep asks for nothing
// else. Whether the sweep knows of the refusal or not, the policy's capture
// is asked for or the sandbox is killed, never paused with memory.
func TestEvictSandbox_UnsupportedFilesystemOnlyIsNeverPausedWithMemory(t *testing.T) {
	t.Parallel()

	node := &unquiescedNode{}
	e := refusingEvictor(t, node)
	sbx := expiredUnquiescedSandbox(true)

	e.evictSandbox(t.Context(), sbx, causeTimeout)
	assert.Equal(t, []bool{true}, node.kinds(), "sweep 1: the refused filesystem-only pause, and no memory pause in its place")
	assert.True(t, node.calls[0].RequireEnvdHealthy, "the sweep's pause is strict, and the unsupported answer never reaches the retry that is not")

	node.calls = nil
	e.unsupportedFilesystemOnly.Delete(sbx.SandboxID)
	e.evictSandbox(t.Context(), sbx, causeTimeout)
	assert.Equal(t, []bool{true}, node.kinds(), "a sweep that does not know of the refusal asks for the policy's capture, never a memory pause")

	node.calls = nil
	e.evictSandbox(t.Context(), sbx, causeTimeout)
	require.Len(t, node.calls, 1)
	assert.Equal(t, sandbox.StateActionKill, node.calls[0].Action, "a sweep that knows of the refusal kills, never pauses with memory")
}

// A filesystem-only capture the node refuses as unsupported is asked for
// once: the refusal is counted and the record restored for its retry window,
// and the sweep after the window (the restored record is still expired)
// kills the sandbox with its timeout reason, pinned to the execution it
// read, as a sandbox whose pause cannot be taken is. No sweep asks the node
// again, so the refusal costs one snapshot build, not one per sweep; the
// kill that lands clears the entry.
func TestEvictSandbox_RefusedFilesystemOnlyCaptureEndsInTheKill(t *testing.T) {
	t.Parallel()

	node := &unquiescedNode{}
	e := refusingEvictor(t, node)
	sbx := expiredUnquiescedSandbox(true)

	e.evictSandbox(t.Context(), sbx, causeTimeout)
	require.Len(t, node.calls, 1, "sweep 1: one pause, so one snapshot build")
	assert.Equal(t, sandbox.StateActionPause, node.calls[0].Action)
	assert.True(t, node.calls[0].FilesystemOnly)
	assert.True(t, e.refusedFilesystemOnly(sbx), "the refusal is remembered for the sweep after the retry window")

	node.calls = nil
	e.evictSandbox(t.Context(), sbx, causeTimeout)
	require.Len(t, node.calls, 1, "sweep 2: one call")
	got := node.calls[0]
	assert.Equal(t, sandbox.StateActionKill, got.Action, "sweep 2: the kill, not another pause")
	assert.Equal(t, sandbox.KillReasonTimeout, got.Reason)
	assert.Equal(t, sbx.ExecutionID, got.ExpectExecutionID)
	assert.True(t, got.Eviction)
	assert.False(t, got.FilesystemOnly, "a kill takes no snapshot")
	assert.False(t, e.refusedFilesystemOnly(sbx), "the kill that landed clears the entry")

	// The kill is decided for the execution the node refused: a later
	// execution under the same ID is asked for its capture afresh.
	e.rememberUnsupported(sbx, time.Now())
	resumed := sbx
	resumed.ExecutionID = "exec-2"
	node.calls = nil
	e.evictSandbox(t.Context(), resumed, causeTimeout)
	require.Len(t, node.calls, 1)
	assert.Equal(t, sandbox.StateActionPause, node.calls[0].Action, "another execution is not killed on the old refusal")
	assert.True(t, node.calls[0].FilesystemOnly)
}

// A memory auto-pause the node refuses retryably is degraded to filesystem-only
// once; when that is refused as unsupported the sweep is recorded as the
// admission refusal, and the next sweep does not degrade again.
func TestEvictSandbox_DegradeStopsAfterAnUnsupportedRefusal(t *testing.T) {
	t.Parallel()

	node := &unquiescedNode{memoryErr: sandbox.PauseQueueExhaustedError{}}
	e := refusingEvictor(t, node)
	sbx := expiredUnquiescedSandbox(false)

	e.evictSandbox(t.Context(), sbx, causeTimeout)
	assert.Equal(t, []bool{false, true}, node.kinds(), "sweep 1: the refused memory pause, then the degrade that is refused as unsupported")
	assert.True(t, e.refusedFilesystemOnly(sbx))

	node.calls = nil
	e.evictSandbox(t.Context(), sbx, causeTimeout)
	assert.Equal(t, []bool{false}, node.kinds(), "sweep 2: the memory pause alone, no degrade")

	skip, ok := pauseSkipReason(sandbox.PauseUnsupportedError{}, sandbox.StateRunning)
	assert.True(t, ok)
	assert.Equal(t, pause.SkipReasonUnsupported, skip)
}

// A Linux memory auto-pause refused retryably still degrades once per sweep,
// as before, when the node takes the filesystem-only pause.
func TestEvictSandbox_LinuxDegradeUnchanged(t *testing.T) {
	t.Parallel()

	var calls []sandbox.RemoveOpts
	td := ldtestdata.DataSource()
	td.Update(td.Flag(featureflags.AutoPauseOverstayBudgetMs.Key()).ValueForAll(ldvalue.Int(0)))
	flags, err := featureflags.NewClientWithDatasource(td)
	require.NoError(t, err)
	t.Cleanup(func() { _ = flags.Close(context.WithoutCancel(t.Context())) })
	counter, err := telemetry.GetCounter(noop.NewMeterProvider().Meter("github.com/e2b-dev/infra/packages/api/internal/orchestrator/evictor"), telemetry.ApiEvictorFsOnlyAutoPause)
	require.NoError(t, err)
	e := &Evictor{featureFlags: flags, fsOnlyAutoPauseCounter: counter, degradedCounter: counter, removeSandbox: func(_ context.Context, _ uuid.UUID, _ string, opts sandbox.RemoveOpts) error {
		calls = append(calls, opts)
		if opts.FilesystemOnly {
			return nil
		}

		return sandbox.PauseQueueExhaustedError{}
	}}

	sbx := expiredUnquiescedSandbox(false)
	sbx.FirecrackerVersion = "v1.14-0.2.0"
	e.evictSandbox(t.Context(), sbx, causeTimeout)
	require.Len(t, calls, 2)
	assert.False(t, calls[0].FilesystemOnly)
	assert.True(t, calls[1].FilesystemOnly)
	assert.False(t, e.refusedFilesystemOnly(sbx))
}

// An entry outlives a sandbox removed by a user or another replica, and a
// refused sandbox leaves the expired set for its retry window, so entries are
// bounded by age: a stale one is dropped, a recent one stays.
func TestForgetStaleUnsupported_DropsOnlyOldEntries(t *testing.T) {
	t.Parallel()

	now := time.Now()
	e := &Evictor{}
	e.unsupportedFilesystemOnly.Store("stale", unsupportedRefusal{at: now.Add(-unsupportedEntryTTL - time.Second)})
	e.unsupportedFilesystemOnly.Store("recent", unsupportedRefusal{at: now.Add(-time.Minute)})

	e.forgetStaleUnsupported(now)
	_, stale := e.unsupportedFilesystemOnly.Load("stale")
	assert.False(t, stale)
	_, recent := e.unsupportedFilesystemOnly.Load("recent")
	assert.True(t, recent)
}
