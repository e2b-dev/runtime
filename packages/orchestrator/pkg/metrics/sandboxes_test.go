//go:build linux

package metrics

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/envd"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/network"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxtypes"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

// A sandbox that goes live while a collection is under way must not wedge it.
// A wedged callback holds the SDK pipeline lock for good: the node stops
// reporting sandbox metrics and Close blocks the orchestrator's shutdown.
func TestSandboxObserverCollectsWhileSandboxesStartAndStop(t *testing.T) {
	t.Parallel()

	sandboxes := sandbox.NewSandboxesMap()
	reader := sdkmetric.NewManualReader()
	so := newTestSandboxObserver(t, sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("github.com/e2b-dev/infra/packages/orchestrator/pkg/metrics"), sandboxes)
	registration, err := so.startObserving()
	require.NoError(t, err)
	so.registration = registration

	sbx := observableSandbox(t)

	ctx, stopChurn := context.WithCancel(t.Context())
	churned := make(chan struct{})
	t.Cleanup(func() {
		stopChurn()
		<-churned
	})
	go func() {
		defer close(churned)
		for ctx.Err() == nil {
			assert.NoError(t, sandboxes.MarkRunning(ctx, sbx))
			assert.True(t, sandboxes.MarkStopping(ctx, sbx.Runtime.SandboxID, sbx.LifecycleID))
		}
	}()

	const collections = 2000
	collected := make(chan struct{})
	go func() {
		defer close(collected)
		var rm metricdata.ResourceMetrics
		for range collections {
			assert.NoError(t, reader.Collect(t.Context(), &rm))
		}
	}()

	select {
	case <-collected:
	case <-time.After(10 * time.Second):
		t.Fatal("a collection never returned while sandboxes started and stopped")
	}
}

// observableSandbox is new enough for metrics, and its envd address refuses
// connections at once, so each metrics request fails fast.
func observableSandbox(t *testing.T) *sandbox.Sandbox {
	t.Helper()

	slot, err := network.NewSlot("test", 1, network.Config{}, network.NoopEgressProxy{})
	require.NoError(t, err)
	slot.HostIP = net.IPv4(127, 0, 0, 1)

	sbx := &sandbox.Sandbox{
		LifecycleID: "lifecycle-1",
		Metadata: &sandbox.Metadata{
			Config:  sandbox.NewConfig(sandbox.Config{Envd: sandbox.EnvdMetadata{Version: "0.5.0"}}),
			Runtime: sandboxtypes.RuntimeMetadata{SandboxID: "sandbox-1"},
		},
		Resources: &sandbox.Resources{Slot: slot},
	}
	sbx.SetChecks(sandbox.NewChecks(sbx))

	return sbx
}

func newTestSandboxObserver(t *testing.T, meter metric.Meter, sandboxes *sandbox.Map) *SandboxObserver {
	t.Helper()

	so := &SandboxObserver{meter: meter, sandboxes: sandboxes}
	for _, g := range []struct {
		dst  *metric.Int64ObservableGauge
		name telemetry.GaugeIntType
	}{
		{&so.cpuTotal, telemetry.SandboxCpuTotalGaugeName},
		{&so.cpuPossible, telemetry.SandboxCpuPossibleGaugeName},
		{&so.cpuTarget, telemetry.SandboxCpuTargetGaugeName},
		{&so.cpuAttempts, telemetry.SandboxCpuTargetAttemptsGaugeName},
		{&so.cpuPending, telemetry.SandboxCpuWritePendingGaugeName},
		{&so.memoryTotal, telemetry.SandboxRamTotalGaugeName},
		{&so.memoryUsed, telemetry.SandboxRamUsedGaugeName},
		{&so.memoryCache, telemetry.SandboxRamCacheGaugeName},
		{&so.diskTotal, telemetry.SandboxDiskTotalGaugeName},
		{&so.diskUsed, telemetry.SandboxDiskUsedGaugeName},
		{&so.memWorkingSet, telemetry.SandboxMemWorkingSetGaugeName},
		{&so.memReclaimable, telemetry.SandboxMemReclaimableGaugeName},
		{&so.memShareableBase, telemetry.SandboxMemShareableBaseGaugeName},
	} {
		gauge, err := telemetry.GetGaugeInt(meter, g.name)
		require.NoError(t, err)
		*g.dst = gauge
	}
	cpuUsed, err := telemetry.GetGaugeFloat(meter, telemetry.SandboxCpuUsedGaugeName)
	require.NoError(t, err)
	so.cpuUsed = cpuUsed

	return so
}

func TestUnseenOOMKills(t *testing.T) {
	t.Parallel()

	poll := func(sbx *sandbox.Sandbox, body string) []envd.OOMKill {
		var m sandbox.Metrics
		require.NoError(t, json.Unmarshal([]byte(body), &m))

		kills, more := unseenOOMKills(sbx, &m)
		assert.Zero(t, more)

		return kills
	}

	sbx := &sandbox.Sandbox{Metadata: &sandbox.Metadata{}}
	assert.Empty(t, poll(sbx, `{"oom_kills":[{"seq":4,"process":"template"},{"seq":6,"process":"early"}]}`),
		"a poll before the seed logs nothing")
	sbx.OOMKills.Seed([]envd.OOMKill{{Seq: 4, Process: "template"}})
	assert.Equal(t,
		[]envd.OOMKill{{Seq: 6, Process: "early"}, {Seq: 9, Process: "python3"}},
		poll(sbx, `{"oom_kills":[{"seq":4,"process":"template"},{"seq":6,"process":"early"},{"seq":9,"process":"python3"}]}`),
		"only the seed sets the watermark, so a kill the early poll saw is still logged",
	)
	assert.Empty(t, poll(sbx, `{"oom_kills":[{"seq":6,"process":"early"},{"seq":9,"process":"python3"}]}`), "each kill is logged once")
	assert.Empty(t, poll(sbx, `{}`), "an envd that doesn't report kills")
	sbx.OOMKills.Seed(nil)
	assert.Equal(t, []envd.OOMKill{{Seq: 12, Process: "node"}}, poll(sbx, `{"oom_kills":[{"seq":12,"process":"node"}]}`),
		"a second seed, after a checkpoint, keeps the watermark")

	unseeded := &sandbox.Sandbox{Metadata: &sandbox.Metadata{}}
	unseeded.OOMKills.SeedFailed()
	poll(unseeded, `{}`)
	assert.Empty(t, poll(unseeded, `{"oom_kills":[{"seq":4,"process":"old"}]}`), "without a seed, the first list sets the watermark")
	assert.Equal(t, []envd.OOMKill{{Seq: 7, Process: "new"}},
		poll(unseeded, `{"oom_kills":[{"seq":4,"process":"old"},{"seq":7,"process":"new"}]}`),
		"an omitted list doesn't set the watermark, so the log's older kills aren't taken for new ones")

	long := poll(sbx, `{"oom_kills":[{"seq":13,"process":"`+strings.Repeat("x", 1000)+`"}]}`)
	require.Len(t, long, 1)
	assert.Len(t, long[0].Process, maxOomProcessLen, "the guest can't make the line arbitrarily long")

	var flood strings.Builder
	for i := range maxOomKillsLogged + 5 {
		if i > 0 {
			flood.WriteString(",")
		}
		fmt.Fprintf(&flood, `{"seq":%d,"process":"p"}`, 100+i)
	}
	var m sandbox.Metrics
	require.NoError(t, json.Unmarshal([]byte(`{"oom_kills":[`+flood.String()+`]}`), &m))
	logged, more := unseenOOMKills(sbx, &m)
	assert.Len(t, logged, maxOomKillsLogged, "the guest can't make one poll write any number of lines")
	assert.Equal(t, int64(100), logged[0].Seq, "the oldest kills are logged")
	assert.Equal(t, 5, more)
	assert.Empty(t, poll(sbx, `{"oom_kills":[{"seq":114,"process":"p"}]}`), "the kills counted but not logged still count as seen")

	build := &sandbox.Sandbox{Metadata: &sandbox.Metadata{}}
	build.Runtime.SandboxType = sandboxtypes.SandboxTypeBuild
	build.OOMKills.Seed(nil)
	assert.Empty(t, poll(build, `{"oom_kills":[{"seq":3,"process":"apt"}]}`), "a build's kills stay out of the sandbox's logs")
}

func TestSandboxMemory(t *testing.T) {
	t.Parallel()

	m := &sandbox.Metrics{MemTotal: 4 << 30, MemUsed: 1 << 30}

	tests := []struct {
		name         string
		envdVersion  string
		wantTotal    int64
		wantUsed     int64
		wantReported bool
		wantErr      bool
	}{
		{name: "first envd reporting bytes", envdVersion: "0.2.4", wantTotal: 4 << 30, wantUsed: 1 << 30, wantReported: true},
		{name: "current envd", envdVersion: "0.9.0", wantTotal: 4 << 30, wantUsed: 1 << 30, wantReported: true},
		{name: "envd reporting only MiB", envdVersion: "0.2.3"},
		{name: "unparsable version", envdVersion: "not-a-version", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			total, used, reported, err := sandboxMemory(tt.envdVersion, m)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			assert.Equal(t, tt.wantReported, reported)
			assert.Equal(t, tt.wantTotal, total)
			assert.Equal(t, tt.wantUsed, used)
		})
	}
}
