//go:build linux

package server

import (
	"context"
	"errors"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/launchdarkly/go-sdk-common/v3/ldvalue"
	"github.com/launchdarkly/go-server-sdk/v7/testhelpers/ldtestdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/fc"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/network"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/template"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/service"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/metadata"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxtypes"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

// admissionRODevice is a minimal block.ReadonlyDevice whose durable header is
// a real SetOnce future, like the real template.Storage over build.File.
type admissionRODevice struct {
	durable *utils.SetOnce[*header.Header]
	// Closed when the pre-flight enters its grace wait, so a test can resolve
	// or cancel strictly after the wait began instead of after a sleep.
	waiting chan struct{}
	once    sync.Once
}

func (d *admissionRODevice) ReadAt(context.Context, []byte, int64) (int, error) { return 0, nil }
func (d *admissionRODevice) Slice(context.Context, int64, int64) ([]byte, error) {
	return nil, nil
}
func (d *admissionRODevice) Size(context.Context) (int64, error) { return 0, nil }
func (d *admissionRODevice) Close() error                        { return nil }
func (d *admissionRODevice) BlockSize() int64                    { return int64(header.PageSize) }
func (d *admissionRODevice) Header() *header.Header              { return nil }
func (d *admissionRODevice) SwapHeader(*header.Header)           {}

func (d *admissionRODevice) DurableHeaderNow() (*header.Header, bool) {
	select {
	case <-d.durable.Done:
		h, err := d.durable.Wait()

		return h, err == nil
	default:
		return nil, false
	}
}

func (d *admissionRODevice) DurableHeader(ctx context.Context) (*header.Header, error) {
	d.once.Do(func() { close(d.waiting) })

	return d.durable.WaitWithContext(ctx)
}

func admissionWaitBegun(sbx *sandbox.Sandbox) <-chan struct{} {
	return sbx.Template.(admissionTestTemplate).memfile.(*admissionRODevice).waiting
}

// admissionTestTemplate serves Memfile and parks Metadata forever. Parking is
// what lets the flag-off regression test observe that Pause crossed
// MarkStopping without running the teardown machinery a unit sandbox cannot
// survive; the parked goroutine dies with the test process.
type admissionTestTemplate struct {
	template.Template

	memfile block.ReadonlyDevice
}

func (t admissionTestTemplate) Memfile(context.Context) (block.ReadonlyDevice, error) {
	return t.memfile, nil
}

func (t admissionTestTemplate) Metadata() (metadata.Template, error) {
	select {}
}

func admissionFlagClient(t *testing.T, graceMs *int) *featureflags.Client {
	t.Helper()

	td := ldtestdata.DataSource()
	if graceMs != nil {
		td.Update(td.Flag(featureflags.PauseAdmissionGraceMs.Key()).ValueForAll(ldvalue.Int(*graceMs)))
	}

	ff, err := featureflags.NewClientWithDatasource(td)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ff.Close(context.WithoutCancel(t.Context())) })

	return ff
}

func admissionTestServer(t *testing.T, graceMs *int) *Server {
	t.Helper()

	meter := noop.NewMeterProvider().Meter("github.com/e2b-dev/infra/packages/orchestrator/pkg/server")

	return &Server{
		info:                       &service.ServiceInfo{},
		sandboxFactory:             &sandbox.Factory{Sandboxes: sandbox.NewSandboxesMap()},
		startingSandboxes:          utils.Must(utils.NewAdjustableSemaphore(1)),
		sandboxPauseDuration:       utils.Must(telemetry.GetHistogram(meter, telemetry.PauseDurationHistogramName)),
		sandboxCheckpointCounter:   utils.Must(telemetry.GetCounter(meter, telemetry.OrchestratorSandboxCheckpointCounterName)),
		pauseAdmissionCounter:      utils.Must(telemetry.GetCounter(meter, telemetry.OrchestratorSandboxPauseAdmissionCounterName)),
		pauseAdmissionWaitDuration: utils.Must(telemetry.GetHistogram(meter, telemetry.OrchestratorSandboxPauseAdmissionWaitDurationName)),
		featureFlags:               admissionFlagClient(t, graceMs),
	}
}

func admissionTestSandbox(t *testing.T, sandboxID string, slotIdx int, durable *utils.SetOnce[*header.Header]) *sandbox.Sandbox {
	t.Helper()

	slot, err := network.NewSlot("test", slotIdx, network.Config{}, network.NoopEgressProxy{})
	require.NoError(t, err)

	return &sandbox.Sandbox{
		LifecycleID: "lifecycle-1",
		Metadata: &sandbox.Metadata{
			Config: sandbox.NewConfig(sandbox.Config{
				Envd:              sandbox.EnvdMetadata{Version: "9.9.9"},
				FirecrackerConfig: fc.Config{FirecrackerVersion: "v1.14.1", KernelVersion: "vmlinux-6.1"},
			}),
			Runtime: sandboxtypes.RuntimeMetadata{SandboxID: sandboxID, ExecutionID: sandboxID},
		},
		Resources: &sandbox.Resources{Slot: slot},
		Template:  admissionTestTemplate{memfile: &admissionRODevice{durable: durable, waiting: make(chan struct{})}},
	}
}

// With the flag on and the parent header still deduplicating past the
// grace, Pause refuses with ResourceExhausted BEFORE MarkStopping — the
// sandbox is still fully live, unmarked, with no stop reason set.
func TestPause_AdmissionRefusesBeforeMarkStopping(t *testing.T) {
	t.Parallel()

	s := admissionTestServer(t, new(40))
	sbx := admissionTestSandbox(t, "sbx-admission-refuse", 11, utils.NewSetOnce[*header.Header]())
	s.sandboxFactory.Sandboxes.MarkRunning(t.Context(), sbx)

	start := time.Now()
	_, pauseErr := s.Pause(t.Context(), &orchestrator.SandboxPauseRequest{SandboxId: "sbx-admission-refuse", ExecutionId: "sbx-admission-refuse"})
	elapsed := time.Since(start)

	require.Error(t, pauseErr)
	st, ok := status.FromError(pauseErr)
	require.True(t, ok)
	assert.Equal(t, codes.ResourceExhausted, st.Code())
	assert.GreaterOrEqual(t, elapsed, 40*time.Millisecond, "the refusal must come after the grace wait")

	// Pre-destructive: still in the live registry, no stop reason recorded,
	// and a later legitimate MarkStopping must still succeed.
	_, live := s.sandboxFactory.Sandboxes.Get("sbx-admission-refuse")
	assert.True(t, live, "a refused pause must leave the sandbox live")
	assert.Equal(t, sandbox.StopReasonCrashed, sbx.GetStopReason(), "no stop reason may be set by a refusal")
	assert.True(t, s.sandboxFactory.Sandboxes.MarkStopping(t.Context(), "sbx-admission-refuse", "lifecycle-1"),
		"a refused pause must leave the sandbox unmarked")
}

func TestPause_StaleExecutionCannotPauseReplacement(t *testing.T) {
	t.Parallel()

	s := admissionTestServer(t, new(0))
	sbx := admissionTestSandbox(t, "sbx-stale-pause", 31, utils.NewSetOnce[*header.Header]())
	require.NoError(t, s.sandboxFactory.Sandboxes.MarkRunning(t.Context(), sbx))

	_, err := s.Pause(t.Context(), &orchestrator.SandboxPauseRequest{
		SandboxId: sbx.Runtime.SandboxID, ExecutionId: "stale-execution",
	})
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	got, live := s.sandboxFactory.Sandboxes.Get(sbx.Runtime.SandboxID)
	require.True(t, live)
	require.Same(t, sbx, got)
}

// The swap resolving mid-grace admits the pause, which
// then proceeds into the destructive path (MarkStopping crossed).
func TestPause_AdmissionAdmitsWhenSwapResolvesMidGrace(t *testing.T) {
	t.Parallel()

	durable := utils.NewSetOnce[*header.Header]()
	s := admissionTestServer(t, new(5000))
	sbx := admissionTestSandbox(t, "sbx-admission-midgrace", 12, durable)
	s.sandboxFactory.Sandboxes.MarkRunning(t.Context(), sbx)

	go func() {
		<-admissionWaitBegun(sbx)
		hdr, err := header.NewHeader(&header.Metadata{Version: 3, BlockSize: 4096, Size: 4096}, nil)
		if err == nil {
			_ = durable.SetValue(hdr)
		}
	}()

	// Pause parks forever in the fake template's Metadata once admitted.
	go func() {
		_, _ = s.Pause(context.WithoutCancel(t.Context()), &orchestrator.SandboxPauseRequest{SandboxId: "sbx-admission-midgrace", ExecutionId: "sbx-admission-midgrace"})
	}()

	require.Eventually(t, func() bool {
		_, live := s.sandboxFactory.Sandboxes.Get("sbx-admission-midgrace")

		return !live
	}, 5*time.Second, 5*time.Millisecond, "an admitted pause must proceed to MarkStopping")
	require.Eventually(t, func() bool {
		return sbx.GetStopReason() == sandbox.StopReasonPaused
	}, 2*time.Second, time.Millisecond)
}

// Flag absent (and explicitly negative) means no pre-flight — with the
// parent header still deduplicating, Pause does NOT refuse and does NOT wait;
// it proceeds straight into today's destructive order (MarkStopping, then the
// unchanged pause path).
func TestPause_FlagOffRunsTodaysOrder(t *testing.T) {
	t.Parallel()

	for name, grace := range map[string]*int{"absent": nil, "negative": new(-1)} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			slotIdx := 13
			if name == "negative" {
				slotIdx = 14
			}

			s := admissionTestServer(t, grace)
			sandboxID := "sbx-admission-off-" + name
			sbx := admissionTestSandbox(t, sandboxID, slotIdx, utils.NewSetOnce[*header.Header]())
			s.sandboxFactory.Sandboxes.MarkRunning(t.Context(), sbx)

			start := time.Now()
			done := make(chan struct{})
			go func() {
				defer close(done)
				_, _ = s.Pause(context.WithoutCancel(t.Context()), &orchestrator.SandboxPauseRequest{SandboxId: sandboxID, ExecutionId: sandboxID})
			}()

			// Today's order: MarkStopping happens promptly — no admission
			// refusal, no grace wait — then EnsurePausable passes and the
			// pause-stop reason is set, exactly the pre-flag sequence.
			require.Eventually(t, func() bool {
				_, live := s.sandboxFactory.Sandboxes.Get(sandboxID)

				return !live
			}, 2*time.Second, time.Millisecond, "flag off must reach MarkStopping without refusing")
			assert.Less(t, time.Since(start), 2*time.Second, "flag off must add no admission wait")
			require.Eventually(t, func() bool {
				return sbx.GetStopReason() == sandbox.StopReasonPaused
			}, 2*time.Second, time.Millisecond)

			select {
			case <-done:
				t.Fatal("pause must still be inside the unchanged snapshot path (parked in the test template), not refused")
			default:
			}
		})
	}
}

// The instant probe (grace 0) refuses through the Pause handler too — pins
// the handler gate at graceMs >= 0, not > 0.
func TestPause_AdmissionInstantProbeRefuses(t *testing.T) {
	t.Parallel()

	s := admissionTestServer(t, new(0))
	sbx := admissionTestSandbox(t, "sbx-admission-instant", 17, utils.NewSetOnce[*header.Header]())
	s.sandboxFactory.Sandboxes.MarkRunning(t.Context(), sbx)

	_, pauseErr := s.Pause(t.Context(), &orchestrator.SandboxPauseRequest{SandboxId: "sbx-admission-instant", ExecutionId: "sbx-admission-instant"})

	require.Error(t, pauseErr)
	st, ok := status.FromError(pauseErr)
	require.True(t, ok)
	assert.Equal(t, codes.ResourceExhausted, st.Code())
	_, live := s.sandboxFactory.Sandboxes.Get("sbx-admission-instant")
	assert.True(t, live)
}

// The Checkpoint handler runs the same pre-flight — a pending parent
// header refuses with ResourceExhausted, pre-destructively, on both the
// in-place and resume-fresh flows (the pre-flight sits before the split).
func TestCheckpoint_AdmissionRefusesBeforeAnyDestructiveStep(t *testing.T) {
	t.Parallel()

	s := admissionTestServer(t, new(40))
	sbx := admissionTestSandbox(t, "sbx-admission-ckpt", 15, utils.NewSetOnce[*header.Header]())
	s.sandboxFactory.Sandboxes.MarkRunning(t.Context(), sbx)

	_, ckptErr := s.Checkpoint(t.Context(), &orchestrator.SandboxCheckpointRequest{SandboxId: "sbx-admission-ckpt"})

	require.Error(t, ckptErr)
	st, ok := status.FromError(ckptErr)
	require.True(t, ok)
	assert.Equal(t, codes.ResourceExhausted, st.Code())

	_, live := s.sandboxFactory.Sandboxes.Get("sbx-admission-ckpt")
	assert.True(t, live, "a refused checkpoint must leave the sandbox live")
	assert.Equal(t, sandbox.StopReasonCrashed, sbx.GetStopReason())
	assert.True(t, s.sandboxFactory.Sandboxes.MarkStopping(t.Context(), "sbx-admission-ckpt", "lifecycle-1"),
		"a refused checkpoint must leave the sandbox unmarked")
}

// The retryable sentinel is scoped to admission: a canceled caller context
// mid-wait surfaces as a context status, never ResourceExhausted, and stays
// pre-destructive.
func TestPause_AdmissionCallerCancelIsNotARefusal(t *testing.T) {
	t.Parallel()

	s := admissionTestServer(t, new(5000))
	sbx := admissionTestSandbox(t, "sbx-admission-cancel", 16, utils.NewSetOnce[*header.Header]())
	s.sandboxFactory.Sandboxes.MarkRunning(t.Context(), sbx)

	ctx, cancel := context.WithCancel(t.Context())
	go func() {
		<-admissionWaitBegun(sbx)
		assert.Equal(t, int64(1), s.info.OutstandingWork())
		cancel()
	}()

	_, pauseErr := s.Pause(ctx, &orchestrator.SandboxPauseRequest{SandboxId: "sbx-admission-cancel", ExecutionId: "sbx-admission-cancel"})
	assert.Zero(t, s.info.OutstandingWork())

	require.Error(t, pauseErr)
	st, ok := status.FromError(pauseErr)
	require.True(t, ok)
	assert.Equal(t, codes.Canceled, st.Code())

	_, live := s.sandboxFactory.Sandboxes.Get("sbx-admission-cancel")
	assert.True(t, live)
	assert.ErrorIs(t, ctx.Err(), context.Canceled)
}

// Checkpoint carries its own copy of the gate: flag absent (and explicitly
// negative) means no pre-flight there either — with the parent header still
// deduplicating it neither refuses nor waits, and proceeds into today's
// resume-fresh order (MarkStopping, the checkpointing stop reason, then the
// unchanged snapshot path).
func TestCheckpoint_FlagOffRunsTodaysOrder(t *testing.T) {
	t.Parallel()

	for name, grace := range map[string]*int{"absent": nil, "negative": new(-1)} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			slotIdx := 25
			if name == "negative" {
				slotIdx = 26
			}

			s := admissionTestServer(t, grace)
			sandboxID := "sbx-admission-ckpt-off-" + name
			sbx := admissionTestSandbox(t, sandboxID, slotIdx, utils.NewSetOnce[*header.Header]())
			s.sandboxFactory.Sandboxes.MarkRunning(t.Context(), sbx)

			start := time.Now()
			done := make(chan struct{})
			go func() {
				defer close(done)
				_, _ = s.Checkpoint(context.WithoutCancel(t.Context()), &orchestrator.SandboxCheckpointRequest{SandboxId: sandboxID})
			}()

			require.Eventually(t, func() bool {
				_, live := s.sandboxFactory.Sandboxes.Get(sandboxID)

				return !live
			}, 2*time.Second, time.Millisecond, "flag off must reach MarkStopping without refusing")
			assert.Less(t, time.Since(start), 2*time.Second, "flag off must add no admission wait")
			require.Eventually(t, func() bool {
				return sbx.GetStopReason() == sandbox.StopReasonCheckpointing
			}, 2*time.Second, time.Millisecond)

			select {
			case <-done:
				t.Fatal("checkpoint must still be inside the unchanged snapshot path (parked in the test template), not refused")
			default:
			}
		})
	}
}

// Checkpoint's copy of the caller-cancel branch: a canceled context mid-wait
// surfaces as a context status, never ResourceExhausted, pre-destructively.
func TestCheckpoint_AdmissionCallerCancelIsNotARefusal(t *testing.T) {
	t.Parallel()

	s := admissionTestServer(t, new(5000))
	sbx := admissionTestSandbox(t, "sbx-admission-ckpt-cancel", 27, utils.NewSetOnce[*header.Header]())
	s.sandboxFactory.Sandboxes.MarkRunning(t.Context(), sbx)

	ctx, cancel := context.WithCancel(t.Context())
	go func() {
		<-admissionWaitBegun(sbx)
		assert.Equal(t, int64(1), s.info.OutstandingWork())
		cancel()
	}()

	_, ckptErr := s.Checkpoint(ctx, &orchestrator.SandboxCheckpointRequest{SandboxId: "sbx-admission-ckpt-cancel"})
	assert.Zero(t, s.info.OutstandingWork())

	require.Error(t, ckptErr)
	st, ok := status.FromError(ckptErr)
	require.True(t, ok)
	assert.Equal(t, codes.Canceled, st.Code())

	_, live := s.sandboxFactory.Sandboxes.Get("sbx-admission-ckpt-cancel")
	assert.True(t, live)
	assert.True(t, s.sandboxFactory.Sandboxes.MarkStopping(t.Context(), "sbx-admission-ckpt-cancel", "lifecycle-1"),
		"a canceled checkpoint must leave the sandbox unmarked")
}

// A durable future resolved with an error is permanent. Checkpoint has no
// downstream seal check to fall through to, so it refuses with
// FailedPrecondition — the code the API restores the sandbox to Running on —
// and stays pre-destructive.
func TestCheckpoint_LatchedErrorRefusesFailedPrecondition(t *testing.T) {
	t.Parallel()

	durable := utils.NewSetOnce[*header.Header]()
	require.NoError(t, durable.SetError(errors.New("dedup failed")))

	s := admissionTestServer(t, new(40))
	sbx := admissionTestSandbox(t, "sbx-admission-ckpt-latched", 28, durable)
	s.sandboxFactory.Sandboxes.MarkRunning(t.Context(), sbx)

	// Run it detached: a checkpoint that wrongly proceeds parks in the test
	// template forever, and that must read as a failure, not a hang.
	errCh := make(chan error, 1)
	go func() {
		_, err := s.Checkpoint(context.WithoutCancel(t.Context()), &orchestrator.SandboxCheckpointRequest{SandboxId: "sbx-admission-ckpt-latched"})
		errCh <- err
	}()

	var ckptErr error
	select {
	case ckptErr = <-errCh:
	case <-time.After(2 * time.Second):
		t.Fatal("a latched refusal must return before any destructive step, not enter the snapshot path")
	}

	require.Error(t, ckptErr)
	st, ok := status.FromError(ckptErr)
	require.True(t, ok)
	assert.Equal(t, codes.FailedPrecondition, st.Code())
	assert.Contains(t, st.Message(), "dedup failed")

	_, live := s.sandboxFactory.Sandboxes.Get("sbx-admission-ckpt-latched")
	assert.True(t, live, "a latched refusal must leave the sandbox live")
	assert.Equal(t, sandbox.StopReasonCrashed, sbx.GetStopReason())
	assert.True(t, s.sandboxFactory.Sandboxes.MarkStopping(t.Context(), "sbx-admission-ckpt-latched", "lifecycle-1"),
		"a latched refusal must leave the sandbox unmarked")
}

func admissionMetricsServer(t *testing.T, graceMs int) (*Server, *sdkmetric.ManualReader) {
	t.Helper()

	reader := sdkmetric.NewManualReader()
	meter := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("github.com/e2b-dev/infra/packages/orchestrator/pkg/server")

	return &Server{
		info:                       &service.ServiceInfo{},
		sandboxFactory:             &sandbox.Factory{Sandboxes: sandbox.NewSandboxesMap()},
		sandboxPauseDuration:       utils.Must(telemetry.GetHistogram(meter, telemetry.PauseDurationHistogramName)),
		pauseAdmissionCounter:      utils.Must(telemetry.GetCounter(meter, telemetry.OrchestratorSandboxPauseAdmissionCounterName)),
		pauseAdmissionWaitDuration: utils.Must(telemetry.GetHistogram(meter, telemetry.OrchestratorSandboxPauseAdmissionWaitDurationName)),
		featureFlags:               admissionFlagClient(t, &graceMs),
	}, reader
}

func attrsAsMap(t *testing.T, set attribute.Set) map[string]string {
	t.Helper()

	out := map[string]string{}
	for _, kv := range set.ToSlice() {
		out[string(kv.Key)] = kv.Value.Emit()
	}

	return out
}

func admissionCounterPoints(t *testing.T, reader *sdkmetric.ManualReader) []metricdata.DataPoint[int64] {
	t.Helper()

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))

	var out []metricdata.DataPoint[int64]
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != string(telemetry.OrchestratorSandboxPauseAdmissionCounterName) {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			require.True(t, ok)
			out = append(out, sum.DataPoints...)
		}
	}

	return out
}

func admissionWaitPoints(t *testing.T, reader *sdkmetric.ManualReader) []metricdata.HistogramDataPoint[int64] {
	t.Helper()

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))

	var out []metricdata.HistogramDataPoint[int64]
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != string(telemetry.OrchestratorSandboxPauseAdmissionWaitDurationName) {
				continue
			}
			hist, ok := m.Data.(metricdata.Histogram[int64])
			require.True(t, ok)
			out = append(out, hist.DataPoints...)
		}
	}

	return out
}

// The admission counter carries exactly {outcome, rpc}; the wait
// histogram carries exactly {outcome} and samples only the waiting outcomes.
// Flat cardinality: no per-sandbox or per-instance labels anywhere.
func TestPauseAdmissionMetrics_RefusedPause(t *testing.T) {
	t.Parallel()

	s, reader := admissionMetricsServer(t, 30)
	synctest.Test(t, func(t *testing.T) {
		sbx := admissionTestSandbox(t, "sbx-metrics-refused", 21, utils.NewSetOnce[*header.Header]())
		s.sandboxFactory.Sandboxes.MarkRunning(t.Context(), sbx)

		_, pauseErr := s.Pause(t.Context(), &orchestrator.SandboxPauseRequest{SandboxId: "sbx-metrics-refused", ExecutionId: "sbx-metrics-refused"})
		require.Error(t, pauseErr)

		points := admissionCounterPoints(t, reader)
		require.Len(t, points, 1)
		assert.EqualValues(t, 1, points[0].Value)
		assert.Equal(t, map[string]string{"outcome": "refused", "rpc": "pause"}, attrsAsMap(t, points[0].Attributes),
			"counter attributes must be exactly outcome+rpc")

		waits := admissionWaitPoints(t, reader)
		require.Len(t, waits, 1)
		require.EqualValues(t, 1, waits[0].Count)
		assert.Equal(t, map[string]string{"outcome": "refused"}, attrsAsMap(t, waits[0].Attributes),
			"wait histogram attribute must be exactly outcome")
		assert.Equal(t, int64(30), waits[0].Sum)
	})
}

func TestPauseAdmissionMetrics_RefusedCheckpoint(t *testing.T) {
	t.Parallel()

	s, reader := admissionMetricsServer(t, 0)
	sbx := admissionTestSandbox(t, "sbx-metrics-ckpt", 22, utils.NewSetOnce[*header.Header]())
	s.sandboxFactory.Sandboxes.MarkRunning(t.Context(), sbx)

	_, ckptErr := s.Checkpoint(t.Context(), &orchestrator.SandboxCheckpointRequest{SandboxId: "sbx-metrics-ckpt"})
	require.Error(t, ckptErr)

	points := admissionCounterPoints(t, reader)
	require.Len(t, points, 1)
	assert.Equal(t, map[string]string{"outcome": "refused", "rpc": "checkpoint"}, attrsAsMap(t, points[0].Attributes))

	// An instant probe (grace 0) still entered the waiting outcome: recorded.
	waits := admissionWaitPoints(t, reader)
	require.Len(t, waits, 1)
	assert.Equal(t, map[string]string{"outcome": "refused"}, attrsAsMap(t, waits[0].Attributes))
}

// A latched checkpoint refusal records latched_error under the checkpoint
// rpc: the decision took effect, so the counter tells the truth.
func TestPauseAdmissionMetrics_LatchedCheckpoint(t *testing.T) {
	t.Parallel()

	durable := utils.NewSetOnce[*header.Header]()
	require.NoError(t, durable.SetError(errors.New("dedup failed")))

	s, reader := admissionMetricsServer(t, 0)
	sbx := admissionTestSandbox(t, "sbx-metrics-ckpt-latched", 29, durable)
	s.sandboxFactory.Sandboxes.MarkRunning(t.Context(), sbx)

	_, ckptErr := s.Checkpoint(t.Context(), &orchestrator.SandboxCheckpointRequest{SandboxId: "sbx-metrics-ckpt-latched"})
	require.Error(t, ckptErr)

	points := admissionCounterPoints(t, reader)
	require.Len(t, points, 1)
	assert.Equal(t, map[string]string{"outcome": "latched_error", "rpc": "checkpoint"}, attrsAsMap(t, points[0].Attributes))
}

// A swap resolving mid-grace records ready_after_wait on
// both instruments; a ready-now admission records the counter only.
func TestPauseAdmissionMetrics_ReadyOutcomes(t *testing.T) {
	t.Parallel()

	t.Run("ready_after_wait", func(t *testing.T) {
		t.Parallel()

		durable := utils.NewSetOnce[*header.Header]()
		s, reader := admissionMetricsServer(t, 5000)
		sbx := admissionTestSandbox(t, "sbx-metrics-raw", 23, durable)
		s.sandboxFactory.Sandboxes.MarkRunning(t.Context(), sbx)

		go func() {
			<-admissionWaitBegun(sbx)
			// Ordering is guaranteed by the channel; the sleep only makes the
			// recorded wait non-zero at millisecond resolution.
			time.Sleep(10 * time.Millisecond)
			hdr, err := header.NewHeader(&header.Metadata{Version: 3, BlockSize: 4096, Size: 4096}, nil)
			if err == nil {
				_ = durable.SetValue(hdr)
			}
		}()
		go func() {
			_, _ = s.Pause(context.WithoutCancel(t.Context()), &orchestrator.SandboxPauseRequest{SandboxId: "sbx-metrics-raw", ExecutionId: "sbx-metrics-raw"})
		}()

		// The paused state follows completion of admission metric recording.
		require.Eventually(t, func() bool {
			return sbx.GetStopReason() == sandbox.StopReasonPaused
		}, 5*time.Second, 5*time.Millisecond)

		points := admissionCounterPoints(t, reader)
		require.Len(t, points, 1)
		assert.Equal(t, map[string]string{"outcome": "ready_after_wait", "rpc": "pause"}, attrsAsMap(t, points[0].Attributes))

		waits := admissionWaitPoints(t, reader)
		require.Len(t, waits, 1)
		assert.Equal(t, map[string]string{"outcome": "ready_after_wait"}, attrsAsMap(t, waits[0].Attributes))
		assert.Positive(t, waits[0].Sum)
	})

	t.Run("ready", func(t *testing.T) {
		t.Parallel()

		durable := utils.NewSetOnce[*header.Header]()
		hdr, err := header.NewHeader(&header.Metadata{Version: 3, BlockSize: 4096, Size: 4096}, nil)
		require.NoError(t, err)
		require.NoError(t, durable.SetValue(hdr))

		s, reader := admissionMetricsServer(t, 5000)
		sbx := admissionTestSandbox(t, "sbx-metrics-ready", 24, durable)
		s.sandboxFactory.Sandboxes.MarkRunning(t.Context(), sbx)

		go func() {
			_, _ = s.Pause(context.WithoutCancel(t.Context()), &orchestrator.SandboxPauseRequest{SandboxId: "sbx-metrics-ready", ExecutionId: "sbx-metrics-ready"})
		}()

		require.Eventually(t, func() bool {
			return sbx.GetStopReason() == sandbox.StopReasonPaused
		}, 5*time.Second, 5*time.Millisecond)

		points := admissionCounterPoints(t, reader)
		require.Len(t, points, 1)
		assert.Equal(t, map[string]string{"outcome": "ready", "rpc": "pause"}, attrsAsMap(t, points[0].Attributes))
		assert.Empty(t, admissionWaitPoints(t, reader), "a no-wait admission must not sample the wait histogram")
	})
}
