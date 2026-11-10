//go:build linux

package server

import (
	"context"
	"errors"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/launchdarkly/go-sdk-common/v3/ldvalue"
	"github.com/launchdarkly/go-server-sdk/v7/testhelpers/ldtestdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	internalevents "github.com/e2b-dev/infra/packages/orchestrator/pkg/events"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

// envdProbeMs is the probe timeout the tests enable. Each test serves envd for
// its sandbox from its own test server (registerEnvd), so nothing is shared.
const envdProbeMs = 100

// envdAnswer is how a test's envd answers /health: a status, a status after a
// delay, or no listener (the connection is dropped).
type envdAnswer struct {
	status      int
	delay       time.Duration
	unreachable bool
	// begun, when set, is closed once the first probe reaches the server.
	begun chan struct{}
	once  sync.Once
}

func (answer *envdAnswer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if answer.begun != nil {
		answer.once.Do(func() { close(answer.begun) })
	}
	if answer.unreachable {
		panic(http.ErrAbortHandler)
	}
	if answer.delay > 0 {
		select {
		case <-time.After(answer.delay):
		case <-r.Context().Done():
			return
		}
	}
	w.WriteHeader(answer.status)
}

// registerEnvd serves envd for sbx from a test server that answers as given,
// for the test's lifetime.
func registerEnvd(t *testing.T, sbx *sandbox.Sandbox, answer *envdAnswer) {
	t.Helper()

	srv := httptest.NewServer(answer)
	srv.Config.ErrorLog = log.New(io.Discard, "", 0)
	t.Cleanup(srv.Close)
	sbx.SetEnvdServerURLForTest(srv.URL)
}

// envdProbeFlagClient enables the envd probe with the given timeout, or
// leaves it at its default when envdMs is nil; the dedup pre-flight stays off.
func envdProbeFlagClient(t *testing.T, envdMs *int) *featureflags.Client {
	t.Helper()

	return envdProbeFlagClientWith(t, envdMs, false)
}

// envdProbeFlagClientWith is envdProbeFlagClient with filesystem-only
// checkpoints enabled or not.
func envdProbeFlagClientWith(t *testing.T, envdMs *int, fsOnlyCheckpoint bool) *featureflags.Client {
	t.Helper()

	td := ldtestdata.DataSource()
	if envdMs != nil {
		td.Update(td.Flag(featureflags.PauseEnvdHealthTimeoutMs.Key()).ValueForAll(ldvalue.Int(*envdMs)))
	}
	if fsOnlyCheckpoint {
		td.Update(td.Flag(featureflags.FilesystemOnlyCheckpointFlag.Key()).ValueForAll(ldvalue.Bool(true)))
	}

	ff, err := featureflags.NewClientWithDatasource(td)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ff.Close(context.WithoutCancel(t.Context())) })

	return ff
}

// envdProbeSandbox is an admission test sandbox with a live health checker,
// as every published sandbox has; the probe reads it under the sandbox lock.
func envdProbeSandbox(t *testing.T, sandboxID string, slotIdx int) *sandbox.Sandbox {
	t.Helper()

	sbx := admissionTestSandbox(t, sandboxID, slotIdx, utils.NewSetOnce[*header.Header]())
	sbx.SetChecks(sandbox.NewChecks(sbx))

	return sbx
}

func requireEnvdUnresponsive(t *testing.T, err error) {
	t.Helper()

	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok)
	assert.Equal(t, codes.ResourceExhausted, st.Code(), "the refusal keeps the retryable code the API restores on")
	assert.Contains(t, st.Message(), "guest agent is not responding, please retry")
	assert.NotNil(t, envdUnresponsiveDetail(t, err), "the refusal must carry the ENVD_UNRESPONSIVE detail")
}

// awaitPauseCrossedMarkStopping waits for a pause that was admitted: it has
// left the live map and set the pause stop reason, then parks forever in the
// fake template's Metadata.
func awaitPauseCrossedMarkStopping(t *testing.T, s *Server, sbx *sandbox.Sandbox, msg string) {
	t.Helper()

	require.Eventually(t, func() bool {
		_, live := s.sandboxFactory.Sandboxes.Get(sbx.Runtime.SandboxID)

		return !live
	}, 2*time.Second, time.Millisecond, msg)
	require.Eventually(t, func() bool {
		return sbx.GetStopReason() == sandbox.StopReasonPaused
	}, 2*time.Second, time.Millisecond)
}

// fakePauseSteps stand in for the VM steps of a pause: the snapshot records
// what it was asked and answers a minimal result, the upload and the stop
// are counted.
type fakePauseSteps struct {
	snapshots        atomic.Int32
	uploads          atomic.Int32
	stops            atomic.Int32
	filesystemOnly   atomic.Bool
	envdUnresponsive atomic.Bool
}

func (f *fakePauseSteps) snapshot(_ context.Context, _ *sandbox.Sandbox, in *orchestrator.SandboxPauseRequest, filesystemOnly, _, envdUnresponsive bool) (*snapshotResult, error) {
	f.snapshots.Add(1)
	f.filesystemOnly.Store(filesystemOnly)
	f.envdUnresponsive.Store(envdUnresponsive)

	return &snapshotResult{
		schedulingMetadata: &orchestrator.SchedulingMetadata{BuildId: in.GetBuildId()},
		completeUpload:     func(context.Context, error) {},
		abandonUpload:      func(context.Context, error) {},
		filesystemOnly:     filesystemOnly,
	}, nil
}

func (f *fakePauseSteps) upload(context.Context, *sandbox.Sandbox, *snapshotResult) { f.uploads.Add(1) }

func (f *fakePauseSteps) stopAsync(context.Context, *sandbox.Sandbox) { f.stops.Add(1) }

// envdProbePauseServer is the admission metrics server with the event
// service a pause that runs to its response publishes through.
func envdProbePauseServer(t *testing.T, envdMs *int) (*Server, *sdkmetric.ManualReader) {
	t.Helper()

	s, reader := admissionMetricsServer(t, -1)
	s.featureFlags = envdProbeFlagClient(t, envdMs)
	s.sbxEventsService = internalevents.NewEventsService(nil)

	return s, reader
}

// A memory pause whose guest does not answer the probe proceeds when the
// caller allows it: the probe is asked, the pause runs to its response with
// the snapshot stamped and the response reporting the miss, and the decision
// is counted as envd_unhealthy_proceeded with its duration in the wait
// histogram under that outcome, apart from the refusals.
func TestPause_EnvdUnresponsiveProceedsWhenAllowed(t *testing.T) {
	t.Parallel()

	s, reader := envdProbePauseServer(t, new(envdProbeMs))
	sbx := envdProbeSandbox(t, "sbx-envd-pause-proceed", 30)
	answer := &envdAnswer{unreachable: true, begun: make(chan struct{})}
	registerEnvd(t, sbx, answer)
	s.sandboxFactory.Sandboxes.MarkRunning(t.Context(), sbx)

	steps := &fakePauseSteps{}
	resp, pauseErr := s.pauseWith(t.Context(), &orchestrator.SandboxPauseRequest{SandboxId: "sbx-envd-pause-proceed", BuildId: "build-1", AllowEnvdUnresponsive: true}, steps)
	require.NoError(t, pauseErr)
	select {
	case <-answer.begun:
	default:
		t.Fatal("the guest must still be probed")
	}
	assert.True(t, resp.GetEnvdUnresponsive(), "the response must report the miss the API persists")
	assert.Equal(t, "build-1", resp.GetSchedulingMetadata().GetBuildId())
	assert.True(t, steps.envdUnresponsive.Load(), "the snapshot must be stamped with the miss")
	assert.EqualValues(t, 1, steps.snapshots.Load())
	assert.EqualValues(t, 1, steps.uploads.Load())
	assert.EqualValues(t, 1, steps.stops.Load())
	_, live := s.sandboxFactory.Sandboxes.Get("sbx-envd-pause-proceed")
	assert.False(t, live, "the pause crossed MarkStopping")
	assert.Equal(t, sandbox.StopReasonPaused, sbx.GetStopReason())
	points := admissionCounterPoints(t, reader)
	require.Len(t, points, 1)
	attrs := attrsAsMap(t, points[0].Attributes)
	assert.Equal(t, string(sandbox.SnapshotAdmissionEnvdUnhealthyProceeded), attrs["outcome"])
	assert.Equal(t, "pause", attrs["rpc"])
	waits := admissionWaitPoints(t, reader)
	require.Len(t, waits, 1)
	assert.Equal(t, string(sandbox.SnapshotAdmissionEnvdUnhealthyProceeded), attrsAsMap(t, waits[0].Attributes)["outcome"])
}

// A pause that does not say it may proceed, as every caller from before the
// field, is refused retryably before MarkStopping on an unanswered probe,
// with the detail that lets the API name the cause; the decision is counted
// as envd_unhealthy and its duration lands in the admission wait histogram
// under that outcome.
func TestPause_EnvdUnresponsiveRefusesBeforeMarkStoppingByDefault(t *testing.T) {
	t.Parallel()

	s, reader := admissionMetricsServer(t, -1)
	s.featureFlags = envdProbeFlagClient(t, new(envdProbeMs))
	sbx := envdProbeSandbox(t, "sbx-envd-pause", 31)
	registerEnvd(t, sbx, &envdAnswer{unreachable: true})
	s.sandboxFactory.Sandboxes.MarkRunning(t.Context(), sbx)

	_, pauseErr := s.Pause(t.Context(), &orchestrator.SandboxPauseRequest{SandboxId: "sbx-envd-pause"})
	requireEnvdUnresponsive(t, pauseErr)

	_, live := s.sandboxFactory.Sandboxes.Get("sbx-envd-pause")
	assert.True(t, live, "a refused pause must leave the sandbox live")
	assert.Equal(t, sandbox.StopReasonCrashed, sbx.GetStopReason(), "no stop reason may be set by a refusal")
	assert.True(t, s.sandboxFactory.Sandboxes.MarkStopping(t.Context(), "sbx-envd-pause", "lifecycle-1"),
		"a refused pause must leave the sandbox unmarked")

	points := admissionCounterPoints(t, reader)
	require.Len(t, points, 1)
	attrs := attrsAsMap(t, points[0].Attributes)
	assert.Equal(t, string(sandbox.SnapshotAdmissionEnvdUnhealthy), attrs["outcome"])
	assert.Equal(t, "pause", attrs["rpc"])
	waits := admissionWaitPoints(t, reader)
	require.Len(t, waits, 1)
	assert.Equal(t, string(sandbox.SnapshotAdmissionEnvdUnhealthy), attrsAsMap(t, waits[0].Attributes)["outcome"])
}

// An envd that answers admits the pause: it runs to its response, which
// reports no miss and whose snapshot is not stamped, and the decision is
// counted as envd_healthy.
func TestPause_EnvdHealthyAdmits(t *testing.T) {
	t.Parallel()

	s, reader := envdProbePauseServer(t, new(envdProbeMs))
	sbx := envdProbeSandbox(t, "sbx-envd-pause-healthy", 32)
	registerEnvd(t, sbx, &envdAnswer{status: http.StatusNoContent})
	s.sandboxFactory.Sandboxes.MarkRunning(t.Context(), sbx)

	steps := &fakePauseSteps{}
	resp, pauseErr := s.pauseWith(t.Context(), &orchestrator.SandboxPauseRequest{SandboxId: "sbx-envd-pause-healthy", AllowEnvdUnresponsive: true}, steps)
	require.NoError(t, pauseErr)
	assert.False(t, resp.GetEnvdUnresponsive(), "a guest that answered is not reported as a miss")
	assert.False(t, steps.envdUnresponsive.Load(), "the snapshot is not stamped")
	assert.EqualValues(t, 1, steps.snapshots.Load())
	_, live := s.sandboxFactory.Sandboxes.Get("sbx-envd-pause-healthy")
	assert.False(t, live, "the pause crossed MarkStopping")
	points := admissionCounterPoints(t, reader)
	require.Len(t, points, 1)
	assert.Equal(t, string(sandbox.SnapshotAdmissionEnvdHealthy), attrsAsMap(t, points[0].Attributes)["outcome"])
}

// A filesystem-only pause is always strict, whatever the request says: its
// capture asks the guest to freeze the rootfs, which a hung agent never does,
// and that failure comes after MarkStopping and loses the sandbox. With envd
// unreachable the pause is refused before anything destructive and the
// sandbox stays live.
func TestPause_FilesystemOnlyIsRefusedOnAMissRegardless(t *testing.T) {
	t.Parallel()

	for name, c := range map[string]struct {
		allow bool
		slot  int
	}{
		"default": {allow: false, slot: 33},
		"allowed": {allow: true, slot: 40},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			s, reader := admissionMetricsServer(t, -1)
			s.featureFlags = envdProbeFlagClient(t, new(envdProbeMs))
			sandboxID := "sbx-envd-pause-fs-only-" + name
			sbx := envdProbeSandbox(t, sandboxID, c.slot)
			registerEnvd(t, sbx, &envdAnswer{unreachable: true})
			s.sandboxFactory.Sandboxes.MarkRunning(t.Context(), sbx)

			_, pauseErr := s.Pause(t.Context(), &orchestrator.SandboxPauseRequest{SandboxId: sandboxID, FilesystemOnly: true, AllowEnvdUnresponsive: c.allow})
			requireEnvdUnresponsive(t, pauseErr)

			_, live := s.sandboxFactory.Sandboxes.Get(sandboxID)
			assert.True(t, live, "a refused filesystem-only pause must leave the sandbox live")
			assert.True(t, s.sandboxFactory.Sandboxes.MarkStopping(t.Context(), sandboxID, "lifecycle-1"),
				"a refused pause must leave the sandbox unmarked")
			points := admissionCounterPoints(t, reader)
			require.Len(t, points, 1)
			attrs := attrsAsMap(t, points[0].Attributes)
			assert.Equal(t, string(sandbox.SnapshotAdmissionEnvdUnhealthy), attrs["outcome"])
			assert.Equal(t, "pause", attrs["rpc"])
		})
	}
}

// A filesystem-only pause of a guest that answers proceeds past the probe
// into today's order, counted as envd_healthy.
func TestPause_FilesystemOnlyEnvdHealthyAdmits(t *testing.T) {
	t.Parallel()

	s, reader := admissionMetricsServer(t, -1)
	s.featureFlags = envdProbeFlagClient(t, new(envdProbeMs))
	sbx := envdProbeSandbox(t, "sbx-envd-pause-fs-only-ok", 34)
	registerEnvd(t, sbx, &envdAnswer{status: http.StatusNoContent})
	s.sandboxFactory.Sandboxes.MarkRunning(t.Context(), sbx)

	go func() {
		_, _ = s.Pause(context.WithoutCancel(t.Context()), &orchestrator.SandboxPauseRequest{SandboxId: "sbx-envd-pause-fs-only-ok", FilesystemOnly: true})
	}()

	awaitPauseCrossedMarkStopping(t, s, sbx, "a healthy guest's filesystem-only pause must proceed")
	points := admissionCounterPoints(t, reader)
	require.Len(t, points, 1)
	assert.Equal(t, string(sandbox.SnapshotAdmissionEnvdHealthy), attrsAsMap(t, points[0].Attributes)["outcome"])
}

// Flag absent, negative or zero means no probe: with envd unreachable the
// memory pause proceeds straight into today's order and records no decision,
// whatever the request asks for.
func TestPause_EnvdProbeFlagOffRunsTodaysOrder(t *testing.T) {
	t.Parallel()

	for name, c := range map[string]struct {
		flag *int
		slot int
	}{
		"absent":   {flag: nil, slot: 34},
		"negative": {flag: new(-1), slot: 35},
		"zero":     {flag: new(0), slot: 36},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			s, reader := admissionMetricsServer(t, -1)
			s.featureFlags = envdProbeFlagClient(t, c.flag)
			sandboxID := "sbx-envd-pause-off-" + name
			sbx := envdProbeSandbox(t, sandboxID, c.slot)
			registerEnvd(t, sbx, &envdAnswer{unreachable: true})
			s.sandboxFactory.Sandboxes.MarkRunning(t.Context(), sbx)

			go func() {
				_, _ = s.Pause(context.WithoutCancel(t.Context()), &orchestrator.SandboxPauseRequest{SandboxId: sandboxID})
			}()

			awaitPauseCrossedMarkStopping(t, s, sbx, "flag off must reach MarkStopping without probing")
			assert.Empty(t, admissionCounterPoints(t, reader), "no probe, no decision")
		})
	}
}

// The caller's context ending mid-probe is not evidence about envd: the pause
// answers the context status, never a refusal, records no decision, and the
// sandbox is untouched.
func TestPause_EnvdProbeCallerCancelIsNotARefusal(t *testing.T) {
	t.Parallel()

	s, reader := admissionMetricsServer(t, -1)
	s.featureFlags = envdProbeFlagClient(t, new(5000))
	sbx := envdProbeSandbox(t, "sbx-envd-pause-cancel", 37)
	answer := &envdAnswer{status: http.StatusNoContent, delay: time.Hour, begun: make(chan struct{})}
	registerEnvd(t, sbx, answer)
	s.sandboxFactory.Sandboxes.MarkRunning(t.Context(), sbx)

	ctx, cancel := context.WithCancel(t.Context())
	go func() {
		<-answer.begun
		cancel()
	}()

	_, pauseErr := s.Pause(ctx, &orchestrator.SandboxPauseRequest{SandboxId: "sbx-envd-pause-cancel", AllowEnvdUnresponsive: true})
	require.Error(t, pauseErr)
	assert.Equal(t, codes.Canceled, status.Code(pauseErr))
	assert.Nil(t, envdUnresponsiveDetail(t, pauseErr), "a cancelled probe is not a refusal")

	_, live := s.sandboxFactory.Sandboxes.Get("sbx-envd-pause-cancel")
	assert.True(t, live)
	assert.Equal(t, sandbox.StopReasonCrashed, sbx.GetStopReason())
	assert.True(t, s.sandboxFactory.Sandboxes.MarkStopping(t.Context(), "sbx-envd-pause-cancel", "lifecycle-1"),
		"a cancelled pause must leave the sandbox unmarked")
	assert.Empty(t, admissionCounterPoints(t, reader), "nothing was decided")
}

// A checkpoint can claim the sandbox while the probe waits on its guest, and
// the claim freezes the guest, so the probe fails for that reason. The entry
// is gone from the live map by then, but the answer is the checkpoint hold's
// retryable refusal, which the API restores on, never NotFound, which would
// drop the record and kill the resumed sandbox as an orphan.
func TestPause_EnvdProbeMissWhileACheckpointClaimsTheSandboxIsRetryable(t *testing.T) {
	t.Parallel()

	s, sbx, steps, _ := slowResumeServer(t)
	defer steps.releaseAll()
	s.featureFlags = envdProbeFlagClient(t, new(300))
	meter := sdkmetric.NewMeterProvider(sdkmetric.WithReader(sdkmetric.NewManualReader())).Meter("github.com/e2b-dev/infra/packages/orchestrator/pkg/server")
	s.pauseAdmissionCounter = utils.Must(telemetry.GetCounter(meter, telemetry.OrchestratorSandboxPauseAdmissionCounterName))
	s.pauseAdmissionWaitDuration = utils.Must(telemetry.GetHistogram(meter, telemetry.OrchestratorSandboxPauseAdmissionWaitDurationName))
	sbx.SetChecks(sandbox.NewChecks(sbx))
	answer := &envdAnswer{status: http.StatusNoContent, delay: time.Hour, begun: make(chan struct{})}
	registerEnvd(t, sbx, answer)

	pauseErr := make(chan error, 1)
	go func() {
		_, err := s.Pause(t.Context(), &orchestrator.SandboxPauseRequest{SandboxId: sbx.Runtime.SandboxID, AllowEnvdUnresponsive: true})
		pauseErr <- err
	}()
	select {
	case <-answer.begun:
	case <-time.After(5 * time.Second):
		t.Fatal("the probe never reached the guest")
	}

	done := startSlowCheckpoint(t.Context(), s, steps, sbx)
	awaitStep(t, steps.resumeStarted, "resume")

	var err error
	select {
	case err = <-pauseErr:
	case <-time.After(5 * time.Second):
		t.Fatal("the pause never answered")
	}
	assert.Equal(t, codes.ResourceExhausted, status.Code(err))
	assert.NotNil(t, checkpointInProgressDetail(t, err), "the hold, not the guest, is the reason the API acts on")
	assert.Nil(t, envdUnresponsiveDetail(t, err))

	steps.releaseResume()
	awaitCheckpoint(t, done)
}

// A probe miss on an execution that already exited is not a hung guest: the
// live-map entry is kept until cleanup ends, so the exit itself is read, and
// the answer is NotFound, never the retryable refusal that would restore a
// record for a VM that is gone, and never a pause proceeded past the miss
// even when the caller allows one.
func TestPause_EnvdProbeMissAfterTheVMExitedIsNotFound(t *testing.T) {
	t.Parallel()

	s, reader := admissionMetricsServer(t, -1)
	s.featureFlags = envdProbeFlagClient(t, new(envdProbeMs))
	sbx := envdProbeSandbox(t, "sbx-envd-exited", 78)
	registerEnvd(t, sbx, &envdAnswer{unreachable: true})
	s.sandboxFactory.Sandboxes.MarkRunning(t.Context(), sbx)
	sbx.MarkExitedForTest(errors.New("firecracker died during the probe"))

	_, pauseErr := s.Pause(t.Context(), &orchestrator.SandboxPauseRequest{SandboxId: "sbx-envd-exited", AllowEnvdUnresponsive: true})
	require.Error(t, pauseErr)
	st, ok := status.FromError(pauseErr)
	require.True(t, ok)
	assert.Equal(t, codes.NotFound, st.Code(), "a VM that exited is answered as gone: %s", pauseErr)
	assert.Nil(t, envdUnresponsiveDetail(t, pauseErr), "no retryable refusal for a dead VM")

	points := admissionCounterPoints(t, reader)
	require.Len(t, points, 1, "the probe decision is still counted")
	assert.Equal(t, string(sandbox.SnapshotAdmissionEnvdUnhealthy), attrsAsMap(t, points[0].Attributes)["outcome"])
}

// Only the probed execution's exit counts. When a checkpoint resumes the
// sandbox fresh while the probe waits, the probed sandbox has exited but a
// successor with the same ID is live: the answer is the retryable refusal,
// never NotFound, since NotFound would make the API drop the record of a
// sandbox that is running, and never a pause proceeded past the miss, since
// the probed execution is gone.
func TestPause_EnvdProbeMissAfterASuccessorTookTheIDIsRetryable(t *testing.T) {
	t.Parallel()

	s, _ := admissionMetricsServer(t, -1)
	s.featureFlags = envdProbeFlagClient(t, new(2000))
	sbx := envdProbeSandbox(t, "sbx-envd-successor", 79)
	answer := &envdAnswer{status: http.StatusNoContent, delay: time.Hour, begun: make(chan struct{})}
	registerEnvd(t, sbx, answer)
	require.NoError(t, s.sandboxFactory.Sandboxes.MarkRunning(t.Context(), sbx))

	pauseErr := make(chan error, 1)
	go func() {
		_, err := s.Pause(t.Context(), &orchestrator.SandboxPauseRequest{SandboxId: "sbx-envd-successor", AllowEnvdUnresponsive: true})
		pauseErr <- err
	}()
	select {
	case <-answer.begun:
	case <-time.After(5 * time.Second):
		t.Fatal("the probe never reached the guest")
	}

	// The probed execution ends and a fresh one takes its ID.
	require.True(t, s.sandboxFactory.Sandboxes.MarkStopping(t.Context(), "sbx-envd-successor", sbx.LifecycleID))
	sbx.MarkExitedForTest(errors.New("stopped by the checkpoint"))
	successor := envdProbeSandbox(t, "sbx-envd-successor", 80)
	successor.LifecycleID = "lifecycle-2"
	require.NoError(t, s.sandboxFactory.Sandboxes.MarkRunning(t.Context(), successor))

	var err error
	select {
	case err = <-pauseErr:
	case <-time.After(5 * time.Second):
		t.Fatal("the pause never answered")
	}
	requireEnvdUnresponsive(t, err)
	cur, live := s.sandboxFactory.Sandboxes.Get("sbx-envd-successor")
	assert.True(t, live, "the successor stays live")
	assert.Same(t, successor, cur)
}

// A probe miss while an in-place checkpoint has the guest frozen is the
// freeze, not a hung agent: the sandbox stays listed through an in-place
// checkpoint, so its in-flight marker is what tells the two apart, and the
// answer is the checkpoint hold the API retries on, never the envd refusal
// that would open a guest-agent episode and never a pause proceeded past the
// miss.
func TestPause_EnvdProbeMissDuringAnInPlaceCheckpointAnswersTheHold(t *testing.T) {
	t.Parallel()

	s, reader := admissionMetricsServer(t, -1)
	s.featureFlags = envdProbeFlagClient(t, new(envdProbeMs))
	sbx := envdProbeSandbox(t, "sbx-envd-inplace", 82)
	registerEnvd(t, sbx, &envdAnswer{unreachable: true})
	s.sandboxFactory.Sandboxes.MarkRunning(t.Context(), sbx)
	require.True(t, sbx.BeginCheckpoint(), "the test's in-place checkpoint holds the marker")
	defer sbx.EndCheckpoint()

	_, pauseErr := s.Pause(t.Context(), &orchestrator.SandboxPauseRequest{SandboxId: "sbx-envd-inplace", AllowEnvdUnresponsive: true})
	require.Error(t, pauseErr)
	assert.Equal(t, codes.ResourceExhausted, status.Code(pauseErr))
	assert.NotNil(t, checkpointInProgressDetail(t, pauseErr), "the hold, not the guest, is the reason the API acts on")
	assert.Nil(t, envdUnresponsiveDetail(t, pauseErr))

	_, live := s.sandboxFactory.Sandboxes.Get("sbx-envd-inplace")
	assert.True(t, live, "the sandbox is untouched")
	points := admissionCounterPoints(t, reader)
	require.Len(t, points, 1, "the probe decision is still counted")
	assert.Equal(t, string(sandbox.SnapshotAdmissionEnvdUnhealthy), attrsAsMap(t, points[0].Attributes)["outcome"])
}
