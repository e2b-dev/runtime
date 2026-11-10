//go:build linux

package sandbox

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/uffd/userfaultfd"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxtypes"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
)

// fired is a timer channel that has already fired.
func fired() <-chan time.Time {
	c := make(chan time.Time, 1)
	c <- time.Time{}

	return c
}

type emitted struct {
	window string
	pages  int64
}

func windows(ds ...time.Duration) []postStartWindow {
	ws := make([]postStartWindow, len(ds))
	for i, d := range ds {
		ws[i] = postStartWindow{after: d, label: d.String()}
	}

	return ws
}

// Each window waits from the start, not from the previous window, and hands
// emit the snapshot taken when it came due.
func TestRunPostStartWindows_WaitsFromStartAndEmitsInOrder(t *testing.T) {
	t.Parallel()

	start := time.Unix(1000, 0)
	clock := start
	var waits []time.Duration
	after := func(d time.Duration) <-chan time.Time {
		waits = append(waits, d)
		clock = clock.Add(d)

		return fired()
	}
	pages := int64(0)
	snapshot := func() userfaultfd.ServeSnapshot {
		pages += 10

		return userfaultfd.ServeSnapshot{Pages: pages}
	}
	var got []emitted

	runPostStartWindows(t.Context(), start, windows(10*time.Second, time.Minute),
		func() time.Time { return clock }, after, snapshot,
		func(w postStartWindow, s userfaultfd.ServeSnapshot) { got = append(got, emitted{w.label, s.Pages}) })

	assert.Equal(t, []time.Duration{10 * time.Second, 50 * time.Second}, waits)
	assert.Equal(t, []emitted{{"10s", 10}, {"1m0s", 20}}, got)
}

// A sandbox that stops (checks cancelled) records only the windows it reached.
func TestRunPostStartWindows_StopsWithTheSandbox(t *testing.T) {
	t.Parallel()

	start := time.Unix(1000, 0)
	ctx, cancel := context.WithCancel(t.Context())
	calls := 0
	after := func(time.Duration) <-chan time.Time {
		calls++
		if calls == 1 {
			return fired()
		}
		cancel()

		return make(chan time.Time) // never fires
	}
	var got []time.Duration

	runPostStartWindows(ctx, start, windows(10*time.Second, time.Minute, 5*time.Minute),
		func() time.Time { return start }, after,
		func() userfaultfd.ServeSnapshot { return userfaultfd.ServeSnapshot{} },
		func(w postStartWindow, _ userfaultfd.ServeSnapshot) { got = append(got, w.after) })

	assert.Equal(t, []time.Duration{10 * time.Second}, got)
}

// Windows already behind the clock (checks started late) are recorded at once
// rather than waited for.
func TestRunPostStartWindows_PastWindowsDoNotWait(t *testing.T) {
	t.Parallel()

	start := time.Unix(1000, 0)
	after := func(time.Duration) <-chan time.Time {
		t.Fatal("no window should wait")

		return nil
	}
	var got []time.Duration

	runPostStartWindows(t.Context(), start, windows(10*time.Second, time.Minute),
		func() time.Time { return start.Add(2 * time.Minute) }, after,
		func() userfaultfd.ServeSnapshot { return userfaultfd.ServeSnapshot{} },
		func(w postStartWindow, _ userfaultfd.ServeSnapshot) { got = append(got, w.after) })

	assert.Equal(t, []time.Duration{10 * time.Second, time.Minute}, got)
}

// A cancelled sandbox records nothing, even for a window that is already due.
func TestRunPostStartWindows_CancelledRecordsNothing(t *testing.T) {
	t.Parallel()

	start := time.Unix(1000, 0)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	var got []time.Duration

	runPostStartWindows(ctx, start, windows(10*time.Second),
		func() time.Time { return start.Add(time.Minute) }, time.After,
		func() userfaultfd.ServeSnapshot { return userfaultfd.ServeSnapshot{} },
		func(w postStartWindow, _ userfaultfd.ServeSnapshot) { got = append(got, w.after) })

	assert.Empty(t, got)
}

// The window labels are what queries select on: fixed, without "0s" tails.
func TestPostStartWindowLabels(t *testing.T) {
	t.Parallel()

	var labels []string
	for _, w := range postStartWindows {
		labels = append(labels, w.label)
	}
	assert.Equal(t, []string{"10s", "1m", "5m"}, labels)
}

func TestPostStartWindowAppearsOnResumeTrace(t *testing.T) {
	t.Parallel()

	_, parent := tracer.Start(t.Context(), "resume-test")
	wantTraceID := parent.SpanContext().TraceID()
	wantParentID := parent.SpanContext().SpanID()
	ctx := trace.ContextWithSpanContext(t.Context(), parent.SpanContext())
	parent.End()

	var bySource [storage.NumSources]int64
	bySource[storage.SourceGCS] = 2
	s := &Sandbox{Metadata: &Metadata{}}
	s.emitPostStartWindow(ctx, &postStartBase{startType: StartTypeResume}, postStartWindow{label: "10s"}, userfaultfd.ServeSnapshot{
		Pages: 3, SourcePages: 2, ServeTime: 12 * time.Millisecond, SourceServeTime: 10 * time.Millisecond, BySource: bySource,
	})

	for _, sp := range testSpanRecorder.Ended() {
		if sp.Name() != "post-start-fault-window" || sp.SpanContext().TraceID() != wantTraceID {
			continue
		}
		assert.Equal(t, wantParentID, sp.Parent().SpanID())
		attrs := attrMap(sp.Attributes())
		assert.Equal(t, "10s", attrs["window"])
		assert.Equal(t, int64(3), attrs["post_start.pages"])
		assert.Equal(t, int64(2), attrs["post_start.source_pages.gcs"])

		return
	}
	t.Fatal("post-start fault window missing from the resume trace")
}

// Only a customer memory resume records windows: a create or reboot cold boots
// on no-op memory, and a build's resumes are not the team's workload.
func TestRecordsPostStartFaultIn(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		startType StartType
		sbxType   sandboxtypes.SandboxType
		want      bool
	}{
		{StartTypeResume, sandboxtypes.SandboxTypeSandbox, true},
		{StartTypeCreate, sandboxtypes.SandboxTypeSandbox, false},
		{StartTypeReboot, sandboxtypes.SandboxTypeSandbox, false},
		{StartTypeResume, sandboxtypes.SandboxTypeBuild, false},
		{StartTypeCreate, sandboxtypes.SandboxTypeBuild, false},
	} {
		sbx := &Sandbox{Metadata: &Metadata{Runtime: sandboxtypes.RuntimeMetadata{SandboxType: tc.sbxType}}}
		assert.Equal(t, tc.want, sbx.recordsPostStartFaultIn(tc.startType), "%s/%s", tc.startType, tc.sbxType)
	}
}

// The sampling context ends when the process exits, and not before.
func TestContextUntil(t *testing.T) {
	t.Parallel()

	exit := make(chan struct{})
	ctx, cancel := contextUntil(t.Context(), exit)
	defer cancel()

	require.NoError(t, ctx.Err(), "still running before the process exits")
	close(exit)
	select {
	case <-ctx.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("context not cancelled after the process exited")
	}
}
