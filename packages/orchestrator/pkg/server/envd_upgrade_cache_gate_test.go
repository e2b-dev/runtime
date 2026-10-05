//go:build linux

package server

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/launchdarkly/go-sdk-common/v3/ldvalue"
	"github.com/launchdarkly/go-server-sdk/v7/testhelpers/ldtestdata"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/cfg"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/envdbin"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxtypes"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

// The gates of maybeUpgradeEnvd that the binary cache participates in.
//
// These tests drive maybeUpgradeEnvd itself rather than the helpers under it.
// resolvePhaseResult, deliveryResult and envdbin.GatedReason are each pinned as
// pure functions elsewhere; what none of them can show is that this function
// wires them in the right ORDER and counts each outcome once. Every gate here
// returns the same (false, nil) to the resume, so the only observable that tells
// them apart is which reason was counted -- and an operator sizing a cohort reads
// exactly that label.
//
// The upgrade's delivery half needs a running guest and stops at the boundary of
// what a unit test can reach; every gate BEFORE it does not, and that is where
// the flag decisions live.

// upgradeGateServer builds a Server whose upgrade path is driveable from a unit
// test, and the manual reader its counters land on.
func upgradeGateServer(t *testing.T, hostEnvdPath, baseDir string, cacheOn bool) (*Server, *sdkmetric.ManualReader) {
	t.Helper()

	td := ldtestdata.DataSource()
	td.Update(td.Flag(featureflags.EnvdBinaryCacheFlag.Key()).VariationForAll(cacheOn))
	// Every test here drives the promoted target; the version-pinned form is
	// resolved by featureflags and pinned by its own tests.
	td.Update(td.Flag(featureflags.EnvdUpgradeTargetFlag.Key()).ValueForAll(ldvalue.String("promoted")))
	ff, err := featureflags.NewClientWithDatasource(td)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ff.Close(context.WithoutCancel(t.Context())) })

	reader := sdkmetric.NewManualReader()
	meter := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).
		Meter("github.com/e2b-dev/infra/packages/orchestrator/pkg/server")

	builder := cfg.BuilderConfig{HostEnvdPath: hostEnvdPath, OrchestratorBaseDir: baseDir}
	factory := sandbox.NewFactory(
		t.Context(), builder,
		nil, nil, ff, nil, nil, nil, nil, sandbox.NewSandboxesMap(),
	)

	return &Server{
		config:                   cfg.Config{BuilderConfig: builder},
		sandboxFactory:           factory,
		featureFlags:             ff,
		envdUpgradeAttempts:      utils.Must(telemetry.GetCounter(meter, telemetry.OrchestratorEnvdUpgradeAttempts)),
		envdUpgradeGated:         utils.Must(telemetry.GetCounter(meter, telemetry.OrchestratorEnvdUpgradeGated)),
		envdUpgradeHandover:      utils.Must(telemetry.GetCounter(meter, telemetry.OrchestratorEnvdUpgradeHandover)),
		envdUpgradeDuration:      utils.Must(telemetry.GetHistogram(meter, telemetry.OrchestratorEnvdUpgradeDurationName)),
		envdUpgradePhaseDuration: utils.Must(telemetry.GetHistogram(meter, telemetry.OrchestratorEnvdUpgradePhaseDurationName)),
	}, reader
}

// upgradeSandbox is the minimum a resume-time gate reads: the version the guest's
// envd reports. A struct literal is deliberate -- everything these tests assert
// happens before the first call into the guest.
func upgradeSandbox(version string) *sandbox.Sandbox {
	return &sandbox.Sandbox{
		Metadata: &sandbox.Metadata{
			Config:  &sandbox.Config{Envd: sandbox.EnvdMetadata{Version: version}},
			Runtime: sandboxtypes.RuntimeMetadata{SandboxID: "sbx", TemplateID: "tpl", ExecutionID: "exec"},
		},
	}
}

// counterPoints returns one counter's points keyed by a single attribute, so a
// test can say which reason was counted rather than only how many were.
func counterPoints(t *testing.T, reader *sdkmetric.ManualReader, name telemetry.CounterType, attr string) map[string]int64 {
	t.Helper()

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))

	out := map[string]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != string(name) {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			require.True(t, ok, "%s is not an int64 sum", name)
			for _, dp := range sum.DataPoints {
				v, ok := dp.Attributes.Value(attribute.Key(attr))
				require.True(t, ok, "%s point carries no %q attribute", name, attr)
				out[v.AsString()] += dp.Value
			}
		}
	}

	return out
}

// warmScratchDir returns a directory for a test's source binary and cache, and
// deliberately NOT t.TempDir().
//
// A warm that FAILS is not drainable from here: WarmAsync strips cancellation by
// design, so the copy outlives the test body, and nothing it publishes says it
// finished. t.TempDir()'s RemoveAll then races the copy and fails an
// already-passed test with "directory not empty" -- which is how this arrived,
// green locally and red on a loaded CI runner. Removed on a tolerant retry
// instead, so the directory goes away once the warm lets go of it.
func warmScratchDir(t *testing.T) string {
	t.Helper()

	// Not t.TempDir(), and the linter's suggestion to use it is the bug this
	// helper exists to avoid -- see above.
	dir, err := os.MkdirTemp("", "envd-cache-gate-*") //nolint:usetesting // t.TempDir()'s RemoveAll races the warm still copying into this directory
	require.NoError(t, err)
	t.Cleanup(func() {
		for range 200 {
			if err := os.RemoveAll(dir); err == nil {
				return
			}
			time.Sleep(50 * time.Millisecond)
		}
		t.Logf("cache scratch dir %s outlived its warm; left behind", dir)
	})

	return dir
}

// histogramCount returns how many samples a histogram recorded, keyed by one
// attribute. A phase that was never entered records nothing, which is what makes
// the histogram the only observable that says whether a gate ran before or after
// the work it guards.
func histogramCount(t *testing.T, reader *sdkmetric.ManualReader, name telemetry.HistogramType, attr string) map[string]uint64 {
	t.Helper()

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))

	out := map[string]uint64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != string(name) {
				continue
			}
			h, ok := m.Data.(metricdata.Histogram[int64])
			require.True(t, ok, "%s is not an int64 histogram", name)
			for _, dp := range h.DataPoints {
				v, ok := dp.Attributes.Value(attribute.Key(attr))
				require.True(t, ok, "%s point carries no %q attribute", name, attr)
				out[v.AsString()] += dp.Count
			}
		}
	}

	return out
}

// TestUpgradeGateRejectsAnOldEnvdBeforeResolvingTheTarget pins the order of the
// two gates, which is not visible in either one's outcome: both decline, both
// return (false, nil), and only the counted reason differs.
//
// The order matters on a cold node. The version compare is a string compare on a
// value already in hand, while the resolve can miss the cache and report
// binary_not_cached; resolving first would relabel every too-old sandbox as a
// cache miss, so the series an operator reads to size the un-upgradable cohort
// would drop to zero on a node that had merely just booted.
//
// The counted reason alone cannot show the order -- old_envd is counted either
// way. The resolve-phase histogram can: a resolve that ran records a sample, and
// this population must produce none, because the whole point is that it costs a
// string compare and nothing else.
func TestUpgradeGateRejectsAnOldEnvdBeforeResolvingTheTarget(t *testing.T) {
	t.Parallel()

	dir := warmScratchDir(t)
	src := filepath.Join(dir, "envd")
	// A target that WOULD resolve: staged, probeable, and newer than the sandbox.
	// An absent one would prove nothing here, since the resolver refuses it before
	// consulting the cache and records no phase either.
	writeStandInEnvd(t, src)

	s, reader := upgradeGateServer(t, src, filepath.Join(dir, "base"), true)
	cache := s.sandboxFactory.EnvdBinCache()
	require.NotNil(t, cache)
	require.Eventually(t, func() bool {
		_, ok := cache.Lookup(src)

		return ok
	}, warmWait, 20*time.Millisecond, "the target must be cached, so a resolve would succeed")

	upgraded, err := s.maybeUpgradeEnvd(t.Context(), upgradeSandbox("0.6.11"))
	require.NoError(t, err, "the upgrade is best-effort; it never fails a resume")
	require.False(t, upgraded)

	require.Equal(t, map[string]int64{"old_envd": 1},
		counterPoints(t, reader, telemetry.OrchestratorEnvdUpgradeGated, "reason"),
		"an envd without the /upgrade endpoint must be counted as old_envd, not as a cache or staging miss")
	require.Empty(t, counterPoints(t, reader, telemetry.OrchestratorEnvdUpgradeAttempts, "result"),
		"nothing was attempted, so the attempts series must record nothing")
	require.Empty(t, histogramCount(t, reader, telemetry.OrchestratorEnvdUpgradePhaseDurationName, "phase"),
		"the version gate must return before the resolve, so no phase was entered")
}

// TestUpgradeDefersWhenTheBinaryIsNotCached is the live-path twin of the offline
// gate's deferral: with the cache on and nothing cached, the upgrade is deferred
// to a later resume rather than probing the source, and it is counted so the cost
// side of that gate is visible.
func TestUpgradeDefersWhenTheBinaryIsNotCached(t *testing.T) {
	t.Parallel()

	dir := warmScratchDir(t)
	src := filepath.Join(dir, "envd")
	// Not an ELF image, so the cache's structural check refuses to publish it and
	// the miss persists for the whole test. The gate under test never runs it.
	require.NoError(t, os.WriteFile(src, []byte("#!/bin/sh\necho 0.9.0\n"), 0o755))

	s, reader := upgradeGateServer(t, src, filepath.Join(dir, "base"), true)

	upgraded, err := s.maybeUpgradeEnvd(t.Context(), upgradeSandbox("0.6.13"))
	require.NoError(t, err)
	require.False(t, upgraded)

	require.Equal(t, map[string]int64{envdbin.ReasonNotCached: 1},
		counterPoints(t, reader, telemetry.OrchestratorEnvdUpgradeGated, "reason"),
		"a miss must defer as binary_not_cached, not read the mount and not report a failed probe")
	require.Empty(t, counterPoints(t, reader, telemetry.OrchestratorEnvdUpgradeAttempts, "result"),
		"a deferral delivered nothing, so it must not appear as an attempt")
}

// TestUpgradeWithTheCacheFlagOffProbesTheSourceItself is the direction that shows
// the gate is not inverted. With the flag off the live path must behave exactly as
// it did before the cache existed: it execs the source to learn its version, and
// an empty cache is not consulted at all. If it were, this resume would be counted
// as binary_not_cached instead of recognising the source as already current.
func TestUpgradeWithTheCacheFlagOffProbesTheSourceItself(t *testing.T) {
	t.Parallel()

	dir := warmScratchDir(t)
	src := filepath.Join(dir, "envd")
	// Runnable, because with the flag off the version comes from a real fork+exec.
	require.NoError(t, os.WriteFile(src, []byte("#!/bin/sh\necho 0.6.13\n"), 0o755))

	s, reader := upgradeGateServer(t, src, filepath.Join(dir, "base"), false)

	upgraded, err := s.maybeUpgradeEnvd(t.Context(), upgradeSandbox("0.6.13"))
	require.NoError(t, err)
	require.False(t, upgraded)

	// same_version: the source was probed and matched. It is the expected
	// per-resume no-op, so it is deliberately counted nowhere.
	require.Empty(t, counterPoints(t, reader, telemetry.OrchestratorEnvdUpgradeGated, "reason"),
		"flag off must probe the source directly; a counted reason here means the cache was consulted")
	require.Empty(t, counterPoints(t, reader, telemetry.OrchestratorEnvdUpgradeAttempts, "result"))
}

// TestUpgradeDefersWhenTheCachedCopyIsRemoved is what a retired copy costs the
// live path. Once the copy is gone, nothing on local disk holds the bytes the
// recorded version describes, and the source is not an alternative -- reading it
// would be both the mount cost the cache exists to avoid and bytes whose version
// was never verified against them. So the resume defers, counted, having sent the
// guest nothing.
//
// It defers as binary_not_cached rather than copy_vanished, and that is the
// behaviour worth pinning: the lookup drops the entry as a phantom instead of
// answering from it, which both re-queues the copy and keeps the node upgrading
// once the warm lands. copy_vanished remains for the narrower window where the
// removal falls between a resolve that hit and the read of the bytes.
func TestUpgradeDefersWhenTheCachedCopyIsRemoved(t *testing.T) {
	t.Parallel()

	dir := warmScratchDir(t)
	src := filepath.Join(dir, "envd")
	writeStandInEnvd(t, src)

	s, reader := upgradeGateServer(t, src, filepath.Join(dir, "base"), true)

	cache := s.sandboxFactory.EnvdBinCache()
	require.NotNil(t, cache)
	require.Eventually(t, func() bool {
		_, ok := cache.Lookup(src)

		return ok
	}, warmWait, 20*time.Millisecond, "the startup warm must publish a copy to retire")

	entry, ok := cache.Lookup(src)
	require.True(t, ok)
	require.Equal(t, standInEnvdVersion, entry.Version)
	// Exactly what a promotion does between two resumes: the copy that carried the
	// resolved version is unlinked while the entry still names it.
	require.NoError(t, os.Remove(entry.LocalPath))

	upgraded, err := s.maybeUpgradeEnvd(t.Context(), upgradeSandbox("0.6.13"))
	require.NoError(t, err)
	require.False(t, upgraded)

	require.Equal(t, map[string]int64{envdbin.ReasonNotCached: 1},
		counterPoints(t, reader, telemetry.OrchestratorEnvdUpgradeGated, "reason"),
		"a removed copy must defer and re-queue the copy, not resolve a version whose bytes are gone")
	require.Empty(t, counterPoints(t, reader, telemetry.OrchestratorEnvdUpgradeAttempts, "result"),
		"no byte reached the guest, so this must not be counted as a delivery attempt")

	// The deferral costs one resume, not the node: the same removal must leave a
	// warm behind that republishes the copy for the resume after this one.
	require.Eventually(t, func() bool {
		e, ok := cache.Lookup(src)
		if !ok {
			return false
		}
		_, err := os.Stat(e.LocalPath)

		return err == nil
	}, warmWait, 20*time.Millisecond, "the deferral must re-warm the copy it found missing")
}

// standInEnvdVersion is the version the stand-in binary reports.
const standInEnvdVersion = "9.9.9"

// warmWait bounds the waits for a background warm to land. It is a liveness
// bound, not a latency assertion -- nothing here claims a warm is fast, only that
// it happens -- so it is deliberately far larger than the work needs. A tight one
// would be a measurement of the machine, and these tests share a runner with the
// rest of the suite.
const warmWait = 60 * time.Second

// The stand-in is compiled ONCE per test binary and then written wherever a test
// needs it. Compiled because this is the one fixture the real code path execs: the
// cache validates a copy's ELF structure and then runs it with -version, so
// nothing short of a working binary exercises it. Once, because a `go build` per
// test put the compiler inside the window a test waits for a background warm to
// land -- so on a loaded machine the wait measured the compiler, and the test
// failed for a reason that had nothing to do with the cache.
var standInEnvd struct {
	once  sync.Once
	bytes []byte
	err   error
}

// writeStandInEnvd writes the stand-in envd at path.
func writeStandInEnvd(t *testing.T, path string) {
	t.Helper()

	standInEnvd.once.Do(func() {
		standInEnvd.bytes, standInEnvd.err = compileStandInEnvd(t.Context())
	})
	require.NoError(t, standInEnvd.err, "compiling the stand-in envd")
	require.NoError(t, os.WriteFile(path, standInEnvd.bytes, 0o755))
}

func compileStandInEnvd(ctx context.Context) ([]byte, error) {
	srcDir, err := os.MkdirTemp("", "standin-envd-src-*")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(srcDir)

	if err := os.WriteFile(filepath.Join(srcDir, "go.mod"),
		[]byte("module standinenvd\n\ngo 1.26\n"), 0o644); err != nil {
		return nil, err
	}
	if err := os.WriteFile(filepath.Join(srcDir, "main.go"),
		[]byte("package main\n\nimport \"fmt\"\n\nfunc main() { fmt.Println(\""+standInEnvdVersion+"\") }\n"), 0o644); err != nil {
		return nil, err
	}

	out := filepath.Join(srcDir, "envd")
	cmd := exec.CommandContext(ctx, "go", "build", "-o", out, ".")
	cmd.Dir = srcDir
	if combined, err := cmd.CombinedOutput(); err != nil {
		return nil, fmt.Errorf("go build: %w: %s", err, combined)
	}

	return os.ReadFile(out)
}
