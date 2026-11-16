//go:build linux

package sandbox

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/launchdarkly/go-sdk-common/v3/ldvalue"
	"github.com/launchdarkly/go-server-sdk/v7/testhelpers/ldtestdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/fc"
	"github.com/e2b-dev/infra/packages/shared/pkg/fc/client/operations"
	"github.com/e2b-dev/infra/packages/shared/pkg/fc/models"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxtypes"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

// fakeHotplug is a Firecracker hotplug region: requests are refused refuse times, then taken; the
// guest plugs a taken request after plugAfter status reads. With statusHangs a status read answers
// only when its context ends.
type fakeHotplug struct {
	mu          sync.Mutex
	refuse      int
	plugAfter   int
	statusHangs bool
	// plugAt, when set, is the moment the guest plugs a taken request, in place of plugAfter.
	plugAt     time.Time
	requests   []int64
	requested  int64
	plugged    int64
	statusErr  error
	requestErr error
}

func fcFault(code int, message string) error {
	e := operations.NewPatchMemoryHotplugDefault(code)
	e.Payload = &models.Error{FaultMessage: message}

	return fmt.Errorf("error requesting memory hotplug: %w", e)
}

func (f *fakeHotplug) RequestMemoryHotplug(_ context.Context, mb int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.requests = append(f.requests, mb)
	if f.requestErr != nil {
		return f.requestErr
	}
	if f.refuse > 0 {
		f.refuse--

		return fcFault(http.StatusBadRequest, "Virtio memory error: Device is not active")
	}
	f.requested = mb

	return nil
}

func (f *fakeHotplug) MemoryHotplugStatus(ctx context.Context) (fc.MemoryHotplugStatus, error) {
	if f.statusHangs {
		<-ctx.Done()

		return fc.MemoryHotplugStatus{}, ctx.Err()
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.statusErr != nil {
		return fc.MemoryHotplugStatus{}, f.statusErr
	}
	switch {
	case !f.plugAt.IsZero():
		if !time.Now().Before(f.plugAt) {
			f.plugged = f.requested
		}
	case f.plugAfter > 0:
		f.plugAfter--
	default:
		f.plugged = f.requested
	}

	return fc.MemoryHotplugStatus{PluggedMB: f.plugged, RequestedMB: f.requested}, nil
}

func (f *fakeHotplug) sent() []int64 {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]int64(nil), f.requests...)
}

const mib = uint64(1) << 20

// resizeSandbox is a 1 GiB sandbox whose envd /init answers with header, or with none for "".
func resizeSandbox(hotplug *fakeHotplug, r *memoryResize, header string) (*Sandbox, *int) {
	return resizeSandboxID("resize-test", hotplug, r, header)
}

// resizeSandboxID is resizeSandbox with its own sandbox ID, so its logs can be told apart.
func resizeSandboxID(id string, hotplug *fakeHotplug, r *memoryResize, header string) (*Sandbox, *int) {
	s := &Sandbox{
		Metadata:        &Metadata{Runtime: sandboxtypes.RuntimeMetadata{SandboxID: id}, Config: NewConfig(Config{RamMB: 1024})},
		memoryHotplugMB: 3072,
		hotplugOps:      hotplug,
	}
	s.memoryResize.Store(r)
	inits := new(int)
	s.memoryReinit = func(context.Context) *envdReinit {
		*inits++
		reinit := newEnvdReinit(func(error) {})
		s.recordEnvdMemoryReport(header)
		reinit.publishReturned()

		return reinit
	}

	return s, inits
}

func memoryHeader(memTotal, applied, movable uint64, pending bool) string {
	return fmt.Sprintf(`{"request":0,"low":0,"floor":0,"partial":false,"limit":{"mem_total":%d,"applied":%d,"pending":%t},"layout":{"policy":"auto-movable","auto_online":"online","movable":%d,"kernel":%d}}`,
		memTotal, applied, pending, movable, 1023741952)
}

func TestRebootMemoryMB(t *testing.T) {
	t.Parallel()

	assert.Equal(t, int64(3072), rebootMemoryMB(0, 1024, 2048), "omission plugs back what the lineage had asked for")
	assert.Equal(t, int64(2048), rebootMemoryMB(2048, 1024, 2048), "an explicit size wins")
	assert.Zero(t, rebootMemoryMB(0, 1024, 0), "a lineage that plugged nothing boots with boot RAM")
}

// A sandbox runs with its boot RAM plus the hotplug memory it asked to plug, a size kept from its
// snapshot included; that is what the node counts it at.
func TestSandboxMemoryMB(t *testing.T) {
	t.Parallel()

	s := &Sandbox{Metadata: &Metadata{Config: NewConfig(Config{RamMB: 1024})}}
	assert.Equal(t, int64(1024), s.MemoryMB())
	s.memoryHotplugRequestedMB = 2048
	assert.Equal(t, int64(3072), s.MemoryMB())
}

// A resume that grew to 3 GiB: Firecracker reports 2 GiB plugged and envd's limit follows 3 GiB with
// 2 GiB movable, the dev guest's numbers.
func TestCompleteMemoryResizeConfirmsAGrownResume(t *testing.T) {
	t.Parallel()

	hotplug := &fakeHotplug{requested: 2048, plugAfter: 2}
	s, inits := resizeSandbox(hotplug, &memoryResize{requestedMB: 2048}, memoryHeader(3171225600, 3037007872, 2048*mib, false))
	outcome, settled := s.driveMemoryResize(t.Context(), s.memoryResize.Load(), StartTypeResume, time.Now().Add(time.Second))
	assert.Equal(t, memoryResizeConfirmed, outcome)
	assert.True(t, settled)
	assert.Equal(t, 1, *inits)
	assert.Empty(t, hotplug.sent(), "a resume sent its target before the vCPUs ran")
	assert.Equal(t, int64(2048), s.lastPluggedMB.Load())
}

// A cold boot sends its request once Firecracker takes it, then confirms the same way.
func TestCompleteMemoryResizeSendsAndConfirmsAColdBoot(t *testing.T) {
	t.Parallel()

	hotplug := &fakeHotplug{refuse: 2}
	s, _ := resizeSandbox(hotplug, &memoryResize{requestedMB: 2048, coldBoot: true}, memoryHeader(3171225600, 3037007872, 2048*mib, false))
	outcome, settled := s.driveMemoryResize(t.Context(), s.memoryResize.Load(), StartTypeResume, time.Now().Add(time.Second))
	assert.Equal(t, memoryResizeConfirmed, outcome)
	assert.True(t, settled)
	assert.Equal(t, []int64{2048, 2048, 2048}, hotplug.sent())
}

// Every shortfall leaves the confirmation unsettled with its outcome, within the wait.
func TestDriveMemoryResizeShortfalls(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		hotplug *fakeHotplug
		r       *memoryResize
		header  string
		want    string
		settled bool
	}{
		"the guest never plugs":                {hotplug: &fakeHotplug{requested: 2048, plugAfter: 1 << 30}, r: &memoryResize{requestedMB: 2048}, want: memoryResizePlugTimeout},
		"firecracker cannot be reached":        {hotplug: &fakeHotplug{statusErr: errors.New("dial unix: connection refused")}, r: &memoryResize{requestedMB: 2048}, want: memoryResizeFirecrackerUnavailable},
		"the device stays inactive":            {hotplug: &fakeHotplug{refuse: 1 << 30}, r: &memoryResize{requestedMB: 2048, coldBoot: true}, want: memoryResizeDeviceInactive},
		"firecracker refuses for good":         {hotplug: &fakeHotplug{requestErr: fcFault(http.StatusBadRequest, "Virtio memory error: Size 3 is invalid")}, r: &memoryResize{requestedMB: 2048, coldBoot: true}, want: memoryResizeFirecrackerError, settled: true},
		"envd's limit is pending":              {hotplug: &fakeHotplug{requested: 2048}, r: &memoryResize{requestedMB: 2048}, header: memoryHeader(3171225600, 895774208, 2048*mib, true), want: memoryResizeEnvdMismatch},
		"envd still sees boot RAM":             {hotplug: &fakeHotplug{requested: 2048}, r: &memoryResize{requestedMB: 2048}, header: memoryHeader(1023741952, 895774208, 0, false), want: memoryResizeEnvdMismatch},
		"the plug did not come online movable": {hotplug: &fakeHotplug{requested: 2048}, r: &memoryResize{requestedMB: 2048}, header: memoryHeader(3171225600, 3037007872, 0, false), want: memoryResizeEnvdMismatch},
		"envd predates the report":             {hotplug: &fakeHotplug{requested: 2048}, r: &memoryResize{requestedMB: 2048}, header: `{"request":0,"low":0,"floor":0,"partial":false}`, want: memoryResizeEnvdUnsupported, settled: true},
		"envd without the header":              {hotplug: &fakeHotplug{requested: 2048}, r: &memoryResize{requestedMB: 2048}, header: "", want: memoryResizeEnvdUnsupported, settled: true},
		"envd with a limit but no layout":      {hotplug: &fakeHotplug{requested: 2048}, r: &memoryResize{requestedMB: 2048}, header: `{"limit":{"mem_total":3171225600,"applied":3037007872}}`, want: memoryResizeEnvdUnsupported, settled: true},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			s, _ := resizeSandbox(tc.hotplug, tc.r, tc.header)
			outcome, settled := s.driveMemoryResize(t.Context(), tc.r, StartTypeResume, time.Now().Add(150*time.Millisecond))
			assert.Equal(t, tc.want, outcome)
			assert.Equal(t, tc.settled, settled)
		})
	}
}

// A confirmation the start's wait could not settle goes on afterwards from where it stopped.
func TestDriveMemoryResizeResumesWhereItStopped(t *testing.T) {
	t.Parallel()

	hotplug := &fakeHotplug{requested: 2048, plugAfter: 1 << 30}
	s, inits := resizeSandbox(hotplug, &memoryResize{requestedMB: 2048}, memoryHeader(3171225600, 3037007872, 2048*mib, false))
	outcome, settled := s.driveMemoryResize(t.Context(), s.memoryResize.Load(), StartTypeResume, time.Now().Add(50*time.Millisecond))
	require.False(t, settled)
	require.Equal(t, memoryResizePlugTimeout, outcome)

	hotplug.mu.Lock()
	hotplug.plugAfter = 0
	hotplug.mu.Unlock()
	outcome, settled = s.driveMemoryResize(t.Context(), s.memoryResize.Load(), StartTypeResume, time.Now().Add(time.Second))
	assert.Equal(t, memoryResizeConfirmed, outcome)
	assert.True(t, settled)
	assert.Equal(t, 1, *inits)
}

// A start that kept its size waits for nothing: no time passes, and nothing is sent to Firecracker
// or envd.
func TestCompleteMemoryResizeWithoutAResize(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		hotplug := &fakeHotplug{}
		s, inits := resizeSandbox(hotplug, nil, "")
		began := time.Now()
		s.completeMemoryResize(t.Context(), StartTypeResume)
		assert.Zero(t, time.Since(began))
		assert.Empty(t, hotplug.sent())
		assert.Zero(t, *inits)
	})
}

// The start's wait is bounded: a guest that never plugs returns the start when the wait ends, and
// the confirmation goes on in the background.
func TestCompleteMemoryResizeIsBounded(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		hotplug := &fakeHotplug{requested: 2048, plugAfter: 1 << 30}
		s, _ := resizeSandbox(hotplug, &memoryResize{requestedMB: 2048}, "")
		began := time.Now()
		s.completeMemoryResize(t.Context(), StartTypeResume)
		assert.Equal(t, time.Duration(featureflags.MemoryResizeWaitMilliseconds.Fallback())*time.Millisecond, time.Since(began))
		// Let the background confirmation run out its budget before the bubble ends.
		time.Sleep(memoryResizeBackgroundBudget + time.Second)
	})
}

// A Firecracker status read that hangs is cut at the stage's deadline, so it cannot hold the start
// past its wait.
func TestMemoryResizeBoundsAHangingStatusRead(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		hotplug := &fakeHotplug{requested: 2048, statusHangs: true}
		s, _ := resizeSandbox(hotplug, &memoryResize{requestedMB: 2048}, "")
		began := time.Now()
		outcome, settled := s.driveMemoryResize(t.Context(), s.memoryResize.Load(), StartTypeResume, began.Add(1500*time.Millisecond))
		assert.Equal(t, 1500*time.Millisecond, time.Since(began))
		assert.Equal(t, memoryResizeFirecrackerUnavailable, outcome)
		assert.False(t, settled)
	})
}

// A start's resize runs once per lifecycle: a ResumeVM of a sandbox already running, as a retained
// capture's recovery runs it, finds it claimed while the first confirmation still runs in the
// background, and returns at once without sending anything.
func TestCompleteMemoryResizeRunsOncePerLifecycle(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		hotplug := &fakeHotplug{plugAfter: 1 << 30}
		s, inits := resizeSandbox(hotplug, &memoryResize{requestedMB: 2048, coldBoot: true}, "")
		s.completeMemoryResize(t.Context(), StartTypeReboot)
		require.Equal(t, []int64{2048}, hotplug.sent())
		require.Nil(t, s.memoryResize.Load())

		began := time.Now()
		s.completeMemoryResize(t.Context(), StartTypeResume)
		assert.Zero(t, time.Since(began))
		assert.Equal(t, []int64{2048}, hotplug.sent())
		assert.Zero(t, *inits)
		time.Sleep(memoryResizeBackgroundBudget + time.Second)
	})
}

// When the start's wait ends with its confirmation /init unanswered, the /init is cut, so none can
// reach envd across the resume's envd live upgrade; the background stage sends none until the
// sandbox went live.
func TestStartWaitCutsAnUnansweredConfirmInit(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		s, _ := resizeSandbox(&fakeHotplug{requested: 2048}, &memoryResize{requestedMB: 2048}, "")
		var sent []*envdReinit
		s.memoryReinit = func(context.Context) *envdReinit {
			h := newEnvdReinit(func(error) {})
			sent = append(sent, h)

			return h
		}
		r := s.memoryResize.Load()
		outcome, settled := s.driveMemoryResize(t.Context(), r, StartTypeResume, time.Now().Add(time.Second))
		assert.Equal(t, memoryResizeEnvdUnconfirmed, outcome)
		assert.False(t, settled)
		require.Len(t, sent, 1)
		assert.Nil(t, r.reinit)
		sent[0].mu.Lock()
		assert.True(t, sent[0].replaced, "the unanswered /init was cut")
		sent[0].mu.Unlock()

		r.background = true
		outcome, _ = s.driveMemoryResize(t.Context(), r, StartTypeResume, time.Now().Add(time.Second))
		assert.Equal(t, memoryResizeEnvdUnconfirmed, outcome)
		assert.Len(t, sent, 1, "no /init before the sandbox went live")
	})
}

func TestMemoryReportShowsPlug(t *testing.T) {
	t.Parallel()

	report := func(memTotal, movable uint64, pending bool) *EnvdMemoryReport {
		return &EnvdMemoryReport{Limit: &EnvdMemoryLimit{MemTotal: memTotal, Applied: memTotal - 128*mib, Pending: pending}, Layout: &EnvdMemoryLayout{Movable: movable}}
	}
	assert.True(t, memoryReportShowsPlug(report(3171225600, 2048*mib, false), 1024, 2048), "the dev guest after a 2 GiB plug")
	assert.True(t, memoryReportShowsPlug(report(1023741952, 0, false), 1024, 0), "the dev guest at boot RAM")
	assert.False(t, memoryReportShowsPlug(report(1023741952, 0, false), 1024, 2048), "still at boot RAM")
	assert.False(t, memoryReportShowsPlug(report(3171225600, 2048*mib, true), 1024, 2048), "pending")
	assert.True(t, memoryReportShowsPlug(report(8704*mib, 6528*mib, false), 512, 8192), "512 MiB + 8 GiB under auto-movable on n2-dev")
}

// The wait is measured and the outcome counted, by start_type; a test start type keeps this test's
// points apart from the others'.
func TestCompleteMemoryResizeRecordsTheWait(t *testing.T) {
	t.Parallel()

	startType := StartType("resize-metrics-test")
	hotplug := &fakeHotplug{requested: 2048}
	s, _ := resizeSandbox(hotplug, &memoryResize{requestedMB: 2048}, memoryHeader(3171225600, 3037007872, 2048*mib, false))
	s.completeMemoryResize(t.Context(), startType)

	var rm metricdata.ResourceMetrics
	require.NoError(t, testMetricReader.Collect(t.Context(), &rm))
	var counted, waits int64
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			switch m.Name {
			case string(telemetry.SandboxMemoryResize):
				for _, dp := range m.Data.(metricdata.Sum[int64]).DataPoints {
					st, _ := dp.Attributes.Value("start_type")
					outcome, _ := dp.Attributes.Value("outcome")
					stage, _ := dp.Attributes.Value("stage")
					if st.AsString() == string(startType) && outcome.AsString() == memoryResizeConfirmed && stage.AsString() == "start" {
						counted += dp.Value
					}
				}
			case string(telemetry.SandboxMemoryResizeWaitHistogramName):
				for _, dp := range m.Data.(metricdata.Histogram[int64]).DataPoints {
					st, _ := dp.Attributes.Value("start_type")
					if st.AsString() == string(startType) {
						waits += int64(dp.Count)
					}
				}
			}
		}
	}
	assert.Equal(t, int64(1), counted)
	assert.Equal(t, int64(1), waits)
}

// Every served /init is a reading, numbered; one without the limit object reads as none.
func TestRecordEnvdMemoryReport(t *testing.T) {
	t.Parallel()

	s := &Sandbox{}
	assert.Nil(t, s.latestEnvdMemoryReading())

	s.recordEnvdMemoryReport(memoryHeader(3171225600, 3037007872, 2048*mib, false))
	first := s.latestEnvdMemoryReading()
	require.NotNil(t, first.report)
	assert.Equal(t, uint64(3171225600), first.report.Limit.MemTotal)
	assert.Equal(t, "auto-movable", first.report.Layout.Policy)
	assert.Equal(t, 2048*mib, first.report.Layout.Movable)

	s.recordEnvdMemoryReport(`{"request":1,"low":2,"floor":3,"partial":false}`)
	second := s.latestEnvdMemoryReading()
	assert.Nil(t, second.report)
	assert.Greater(t, second.seq, first.seq)

	s.recordEnvdMemoryReport("not json")
	assert.Nil(t, s.latestEnvdMemoryReading().report)

	oversized := `{"limit":{"mem_total":3171225600,"applied":3037007872,"error":"` + strings.Repeat("x", int(envdMemoryHeaderMaxBytes)) + `"}}`
	s.recordEnvdMemoryReport(oversized)
	assert.Nil(t, s.latestEnvdMemoryReading().report, "a header over the cap reads as no report")

	longError := `{"limit":{"mem_total":3171225600,"applied":3037007872,"pending":true,"error":"` + strings.Repeat("e", 600) + `"}}`
	s.recordEnvdMemoryReport(longError)
	limit := s.latestEnvdMemoryReading().report.Limit
	assert.LessOrEqual(t, len([]rune(limit.Error)), envdMemoryLimitErrorMaxLen, "the guest-written error is truncated")
}

// The start's own /init already showing the plug confirms it with no further /init, and one from an
// envd without the report settles it as unsupported; a reading without the layout yet settles
// nothing until the confirmation's own /init.
func TestConfirmEnvdMemoryReadsTheStartsReadingFirst(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		startReading string
		want         string
		wantInits    int
	}{
		"already shows the plug": {startReading: memoryHeader(3171225600, 3037007872, 2048*mib, false), want: memoryResizeConfirmed, wantInits: 0},
		"an envd without it":     {startReading: `{"request":0,"low":0,"floor":0,"partial":false}`, want: memoryResizeEnvdUnsupported, wantInits: 0},
		"no layout read yet":     {startReading: `{"limit":{"mem_total":1023741952,"applied":895774208}}`, want: memoryResizeConfirmed, wantInits: 1},
		"still boot RAM":         {startReading: memoryHeader(1023741952, 895774208, 0, false), want: memoryResizeConfirmed, wantInits: 1},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			s, inits := resizeSandbox(&fakeHotplug{requested: 2048}, &memoryResize{requestedMB: 2048}, memoryHeader(3171225600, 3037007872, 2048*mib, false))
			s.recordEnvdMemoryReport(tc.startReading)
			outcome, settled := s.driveMemoryResize(t.Context(), s.memoryResize.Load(), StartTypeResume, time.Now().Add(time.Second))
			assert.Equal(t, tc.want, outcome)
			assert.True(t, settled)
			assert.Equal(t, tc.wantInits, *inits)
		})
	}
}

// In the background no /init is sent before the sandbox went live: a resume's envd live upgrade runs
// between the start's return and that, and an /init across it would reach the envd being replaced.
func TestBackgroundConfirmationWaitsForTheSandboxToGoLive(t *testing.T) {
	t.Parallel()

	s, inits := resizeSandbox(&fakeHotplug{requested: 2048}, &memoryResize{requestedMB: 2048, background: true}, memoryHeader(3171225600, 3037007872, 2048*mib, false))
	outcome, settled := s.driveMemoryResize(t.Context(), s.memoryResize.Load(), StartTypeResume, time.Now().Add(150*time.Millisecond))
	assert.Equal(t, memoryResizeEnvdUnconfirmed, outcome)
	assert.False(t, settled)
	assert.Zero(t, *inits)

	s.wentLive.Store(true)
	outcome, settled = s.driveMemoryResize(t.Context(), s.memoryResize.Load(), StartTypeResume, time.Now().Add(time.Second))
	assert.Equal(t, memoryResizeConfirmed, outcome)
	assert.True(t, settled)
	assert.Equal(t, 1, *inits)
}

// A start that ended, or a sandbox that stopped, aborts the confirmation: not a failure.
func TestDriveMemoryResizeAborts(t *testing.T) {
	t.Parallel()

	s, _ := resizeSandbox(&fakeHotplug{requested: 2048, plugAfter: 1 << 30}, &memoryResize{requestedMB: 2048}, "")
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	outcome, settled := s.driveMemoryResize(ctx, s.memoryResize.Load(), StartTypeResume, time.Now().Add(time.Second))
	assert.Equal(t, memoryResizeAborted, outcome)
	assert.True(t, settled)
}

// The start's wait polls Firecracker at a fixed interval and re-asks envd after 2 ms, doubling to a
// 20 ms cap; the background stage backs both off exponentially to longer caps.
func TestMemoryResizeBackoff(t *testing.T) {
	t.Parallel()

	start := &memoryResize{}
	assert.Equal(t, 5*time.Millisecond, start.pollInterval(), "the flag's default")
	assert.Equal(t, 5*time.Millisecond, start.pollInterval())
	assert.Equal(t, 20*time.Millisecond, (&memoryResize{statusPoll: 20 * time.Millisecond}).pollInterval())
	var startEnvd []time.Duration
	for range 6 {
		startEnvd = append(startEnvd, start.envdInterval())
	}
	assert.Equal(t, []time.Duration{2 * time.Millisecond, 4 * time.Millisecond, 8 * time.Millisecond, 16 * time.Millisecond, 20 * time.Millisecond, 20 * time.Millisecond}, startEnvd)

	background := &memoryResize{background: true}
	var polls, envd []time.Duration
	for range 10 {
		polls = append(polls, background.pollInterval())
		envd = append(envd, background.envdInterval())
	}
	assert.Equal(t, memoryResizeBackgroundPoll, polls[0])
	assert.Equal(t, 2*memoryResizeBackgroundPoll, polls[1])
	assert.Equal(t, memoryResizeBackgroundPollCap, polls[9])
	assert.Equal(t, memoryResizeBackgroundEnvd, envd[0])
	assert.Equal(t, memoryResizeBackgroundEnvdCap, envd[9])
}

// The confirmation's /init is labelled as such, with the start's type, and logs its own failure.
func TestMemoryConfirmReinitPurpose(t *testing.T) {
	t.Parallel()

	purpose := memoryConfirmReinit(StartTypeReboot)
	assert.Equal(t, StartTypeReboot, purpose.startType)
	assert.Equal(t, envdInitMemoryConfirm, purpose.reason)
	assert.NotContains(t, purpose.failure, "clock")
	assert.Equal(t, envdInitReinit, newEnvdReinit(func(error) {}).purpose.reason, "a re-init is a post-checkpoint one unless told otherwise")
}

// sandboxLog is the one entry message logged for sandbox id.
func sandboxLog(t *testing.T, message, id string) map[string]any {
	t.Helper()

	var found []map[string]any
	for _, e := range testLogObserver.FilterMessage(message).All() {
		for _, f := range e.Context {
			if f.String == id {
				found = append(found, e.ContextMap())
			}
		}
	}
	require.Len(t, found, 1, "expected one %q for %s", message, id)

	return found[0]
}

// A permanent refusal keeps the reason: the error and Firecracker's fault message.
func TestMemoryResizeKeepsTheFirecrackerFault(t *testing.T) {
	t.Parallel()

	const id = "resize-fault-test"
	hotplug := &fakeHotplug{requestErr: fcFault(http.StatusBadRequest, "Virtio memory error: Size 3 is invalid")}
	s, _ := resizeSandboxID(id, hotplug, &memoryResize{requestedMB: 2048, coldBoot: true}, "")
	s.completeMemoryResize(t.Context(), StartTypeReboot)

	fields := sandboxLog(t, "Firecracker refused the start's memory resize for good", id)
	assert.Equal(t, memoryResizeFirecrackerError, fields["outcome"])
	assert.Contains(t, fields["error"], "Size 3 is invalid")
	assert.Equal(t, "Virtio memory error: Size 3 is invalid", fields["firecracker_fault"])
	assert.Equal(t, int64(2048), fields["requested_mb"])
}

// A start that returns unconfirmed logs what envd reported against what was expected, and what
// Firecracker reported.
func TestMemoryResizeMismatchLogCarriesTheNumbers(t *testing.T) {
	t.Parallel()

	const id = "resize-mismatch-test"
	synctest.Test(t, func(t *testing.T) {
		hotplug := &fakeHotplug{requested: 2048}
		s, _ := resizeSandboxID(id, hotplug, &memoryResize{requestedMB: 2048}, memoryHeader(1023741952, 895774208, 0, false))
		s.completeMemoryResize(t.Context(), StartTypeResume)
		time.Sleep(memoryResizeBackgroundBudget + time.Second)
	})

	fields := sandboxLog(t, "start returned before its plugged memory was confirmed, confirming in the background", id)
	assert.Equal(t, memoryResizeEnvdMismatch, fields["outcome"])
	assert.Equal(t, uint64(1023741952), fields["envd_mem_total"])
	assert.Equal(t, uint64(895774208), fields["envd_applied"])
	assert.Equal(t, false, fields["envd_pending"])
	assert.Equal(t, uint64(0), fields["envd_movable"])
	assert.Equal(t, uint64(1023741952), fields["envd_kernel"])
	expect := expectPlug(1024, 2048)
	assert.Equal(t, expect.memTotalMin, fields["expected_mem_total_min"])
	assert.Equal(t, expect.allowance, fields["mem_total_tolerance"])
	assert.Equal(t, expect.movableMin, fields["expected_movable_min"])
	assert.Equal(t, int64(2048), fields["firecracker_plugged_mb"])
	assert.Equal(t, int64(2048), fields["firecracker_requested_mb"])
	assert.Equal(t, int64(3072), fields["region_mb"])
}

// A confirmed resize records its wait split at the plug, a span with the plug and envd's answer,
// and one Info line.
func TestMemoryResizeRecordsTheSplitAndTheSpan(t *testing.T) {
	t.Parallel()

	const id = "resize-split-test"
	startType := StartType("resize-split-start")
	hotplug := &fakeHotplug{requested: 2048, plugAfter: 2}
	s, _ := resizeSandboxID(id, hotplug, &memoryResize{requestedMB: 2048}, memoryHeader(3171225600, 3037007872, 2048*mib, false))
	s.completeMemoryResize(t.Context(), startType)

	var rm metricdata.ResourceMetrics
	require.NoError(t, testMetricReader.Collect(t.Context(), &rm))
	counts := map[string]uint64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != string(telemetry.SandboxMemoryResizePlugHistogramName) && m.Name != string(telemetry.SandboxMemoryResizeConfirmHistogramName) {
				continue
			}
			for _, dp := range m.Data.(metricdata.Histogram[int64]).DataPoints {
				st, _ := dp.Attributes.Value("start_type")
				outcome, _ := dp.Attributes.Value("outcome")
				if st.AsString() == string(startType) && outcome.AsString() == memoryResizeConfirmed {
					counts[m.Name] += dp.Count
				}
			}
		}
	}
	assert.Equal(t, uint64(1), counts[string(telemetry.SandboxMemoryResizePlugHistogramName)])
	assert.Equal(t, uint64(1), counts[string(telemetry.SandboxMemoryResizeConfirmHistogramName)])

	var events []string
	for _, sp := range testSpanRecorder.Ended() {
		if sp.Name() != "memory_resize" {
			continue
		}
		matches := false
		for _, a := range sp.Attributes() {
			if string(a.Key) == "start_type" && a.Value.AsString() == string(startType) {
				matches = true
			}
		}
		if !matches {
			continue
		}
		for _, e := range sp.Events() {
			events = append(events, e.Name)
		}
	}
	assert.Contains(t, events, "plugged")
	assert.Contains(t, events, "envd_confirmed")
	assert.Contains(t, events, "retry", "the polls before the plug are recorded once")

	fields := sandboxLog(t, "sandbox memory resize confirmed", id)
	assert.Equal(t, int64(2048), fields["plugged_mb"])
	assert.Equal(t, false, fields["init_skipped"])
	assert.Contains(t, fields, "plug_ms")
	assert.Contains(t, fields, "confirm_ms")
	assert.Contains(t, fields, "total_ms")
}

// A start with little time left keeps memoryResizeDeadlineMargin of it for what follows envd: the
// wait ends that much before the deadline, or is skipped when less is left, and the confirmation
// goes on in the background, where it settles.
func TestCompleteMemoryResizeLeavesTheStartItsDeadline(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		left time.Duration
		want time.Duration
	}{
		"the wait ends before the deadline": {left: 1200 * time.Millisecond, want: 200 * time.Millisecond},
		"too little left to wait at all":    {left: 500 * time.Millisecond, want: 0},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			id := "resize-deadline-" + strings.ReplaceAll(name, " ", "-")
			synctest.Test(t, func(t *testing.T) {
				hotplug := &fakeHotplug{requested: 2048, plugAfter: 1 << 30}
				s, _ := resizeSandboxID(id, hotplug, &memoryResize{requestedMB: 2048}, memoryHeader(3171225600, 3037007872, 2048*mib, false))
				ctx, cancel := context.WithTimeout(t.Context(), tc.left)
				defer cancel()
				began := time.Now()
				s.completeMemoryResize(ctx, StartTypeResume)
				assert.Equal(t, tc.want, time.Since(began))
				require.NoError(t, ctx.Err(), "the start keeps time to go live")

				hotplug.mu.Lock()
				hotplug.plugAfter = 0
				hotplug.mu.Unlock()
				s.wentLive.Store(true)
				time.Sleep(memoryResizeBackgroundBudget + time.Second)
			})
			fields := sandboxLog(t, "sandbox memory resize confirmed", id)
			assert.Equal(t, memoryResizeConfirmed, fields["outcome"])
		})
	}
}

// A start that ends before its wait does hands the confirmation to the background; it is not
// aborted, which only the sandbox stopping does.
func TestCompleteMemoryResizeHandsOffWhenTheStartEnds(t *testing.T) {
	t.Parallel()

	const id = "resize-handoff-test"
	synctest.Test(t, func(t *testing.T) {
		hotplug := &fakeHotplug{requested: 2048, plugAfter: 1 << 30}
		s, _ := resizeSandboxID(id, hotplug, &memoryResize{requestedMB: 2048}, "")
		ctx, cancel := context.WithCancel(t.Context())
		time.AfterFunc(300*time.Millisecond, cancel)
		began := time.Now()
		s.completeMemoryResize(ctx, StartTypeResume)
		assert.Equal(t, 300*time.Millisecond, time.Since(began))
		time.Sleep(memoryResizeBackgroundBudget + time.Second)
	})
	fields := sandboxLog(t, "start returned before its plugged memory was confirmed, confirming in the background", id)
	assert.Equal(t, memoryResizePlugTimeout, fields["outcome"])
}

// A start that skipped envd (a bare boot, or one held by a debugger) confirms in the background: a
// cold boot's request is retried while the device is not active, and Firecracker's report of the
// plug settles it, with no /init.
func TestCompleteMemoryResizeWithoutEnvd(t *testing.T) {
	t.Parallel()

	const id = "resize-without-envd-test"
	synctest.Test(t, func(t *testing.T) {
		hotplug := &fakeHotplug{refuse: 5}
		s, inits := resizeSandboxID(id, hotplug, &memoryResize{requestedMB: 2048, coldBoot: true}, "")
		began := time.Now()
		s.completeMemoryResizeWithoutEnvd(t.Context(), StartTypeReboot)
		assert.Zero(t, time.Since(began), "the start does not wait")
		assert.Nil(t, s.memoryResize.Load(), "the resize is claimed")

		time.Sleep(memoryResizeBackgroundBudget + time.Second)
		assert.Equal(t, []int64{2048, 2048, 2048, 2048, 2048, 2048}, hotplug.sent())
		assert.Zero(t, *inits)
	})
	fields := sandboxLog(t, "sandbox memory resize confirmed", id)
	assert.Equal(t, memoryResizeEnvdUnsupported, fields["outcome"])
	assert.Equal(t, int64(2048), fields["plugged_mb"])
}

// A memory confirmation's /init and a post-checkpoint re-init each have their own slot, so neither
// replaces the other; a Pause's cut stops both.
func TestMemoryConfirmReinitKeepsItsOwnSlot(t *testing.T) {
	t.Parallel()

	s := &Sandbox{}
	checkpoint := newEnvdReinit(func(error) {})
	require.True(t, s.publishEnvdReinit(&s.envdReinit, checkpoint))
	confirm := newEnvdReinit(func(error) {})
	require.True(t, s.publishEnvdReinit(&s.memoryConfirmReinit, confirm))
	assert.Same(t, checkpoint, s.envdReinit.Load())
	assert.False(t, checkpoint.replaced, "a confirmation does not replace the checkpoint's re-init")

	next := newEnvdReinit(func(error) {})
	require.True(t, s.publishEnvdReinit(&s.envdReinit, next))
	assert.True(t, checkpoint.replaced)
	assert.False(t, confirm.replaced, "a checkpoint's re-init does not replace the confirmation's")

	_, _, err := s.cutEnvdReinit(t.Context())
	require.NoError(t, err)
	assert.True(t, next.stopped)
	assert.True(t, confirm.stopped, "a Pause stops the confirmation's /init too")
	assert.False(t, s.publishEnvdReinit(&s.memoryConfirmReinit, newEnvdReinit(func(error) {})), "nothing publishes under the Pause's hold")
	s.releaseEnvdReinit()
}

// A resume whose first /init after the plug still shows the limit pending confirms on the next one,
// 2 ms later: once the request is plugged the start waits for envd's retry, not the status poll.
func TestMemoryResizeReasksEnvdQuickly(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		s, _ := resizeSandbox(&fakeHotplug{requested: 2048}, &memoryResize{requestedMB: 2048}, "")
		headers := []string{
			memoryHeader(3171225600, 895774208, 2048*mib, true),
			memoryHeader(3171225600, 3037007872, 2048*mib, false),
		}
		inits := 0
		s.memoryReinit = func(context.Context) *envdReinit {
			reinit := newEnvdReinit(func(error) {})
			s.recordEnvdMemoryReport(headers[min(inits, len(headers)-1)])
			inits++
			reinit.publishReturned()

			return reinit
		}
		began := time.Now()
		outcome, settled := s.driveMemoryResize(t.Context(), s.memoryResize.Load(), StartTypeResume, began.Add(1500*time.Millisecond))
		assert.Equal(t, memoryResizeConfirmed, outcome)
		assert.True(t, settled)
		assert.Equal(t, 2, inits)
		assert.Equal(t, memoryResizeEnvdRetry, time.Since(began))
	})
}

// The start's status poll is the memory-resize-poll-milliseconds flag, kept within 1-100 ms; 0 or
// less is the default, so a bad value can neither busy-loop nor stall the wait.
func TestMemoryResizePollFlag(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		ms   int
		want time.Duration
	}{
		"the default":         {ms: 5, want: 5 * time.Millisecond},
		"a value in range":    {ms: 40, want: 40 * time.Millisecond},
		"the lower bound":     {ms: 1, want: time.Millisecond},
		"above the bound":     {ms: 1000, want: 100 * time.Millisecond},
		"zero is default":     {ms: 0, want: 5 * time.Millisecond},
		"negative is default": {ms: -7, want: 5 * time.Millisecond},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			td := ldtestdata.DataSource()
			td.Update(td.Flag(featureflags.MemoryResizePollMilliseconds.Key()).ValueForAll(ldvalue.Int(tc.ms)))
			ff, err := featureflags.NewClientWithDatasource(td)
			require.NoError(t, err)
			t.Cleanup(func() { _ = ff.Close(context.WithoutCancel(t.Context())) })

			s, _ := resizeSandbox(&fakeHotplug{}, nil, "")
			s.featureFlags = ff
			assert.Equal(t, tc.want, s.memoryResizePollInterval(t.Context()))
		})
	}
}

// A plug that lands between two status reads is noticed within one poll interval.
func TestMemoryResizeNoticesAPlugWithinOnePoll(t *testing.T) {
	t.Parallel()

	for _, poll := range []time.Duration{5 * time.Millisecond, 20 * time.Millisecond} {
		t.Run(poll.String(), func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				began := time.Now()
				plugAt := began.Add(41 * time.Millisecond)
				hotplug := &fakeHotplug{requested: 2048, plugAt: plugAt}
				r := &memoryResize{requestedMB: 2048, statusPoll: poll, began: began}
				s, _ := resizeSandbox(hotplug, r, memoryHeader(3171225600, 3037007872, 2048*mib, false))
				outcome, settled := s.driveMemoryResize(t.Context(), r, StartTypeResume, began.Add(1500*time.Millisecond))
				require.True(t, settled)
				assert.Equal(t, memoryResizeConfirmed, outcome)
				assert.False(t, r.pluggedAt.Before(plugAt))
				assert.Less(t, r.pluggedAt.Sub(plugAt), poll)
			})
		})
	}
}
