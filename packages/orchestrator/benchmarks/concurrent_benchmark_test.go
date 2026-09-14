//go:build linux

// Concurrent sandbox creation benchmark.
//
// Measures how many sandboxes can be effectively resumed in parallel on a
// single node by launching N goroutines that each call ResumeSandbox
// simultaneously and recording per-sandbox latency and overall wall-clock time.
//
// Run with something like:
//
//	sudo modprobe nbd
//	echo 1024 | sudo tee /proc/sys/vm/nr_hugepages
//	sudo $(which go) test -run='^$' -bench=BenchmarkConcurrentResume -benchtime=1x -timeout=30m -v
//
// Set CONCURRENCY_LEVELS to override the default levels, e.g.:
//
//	CONCURRENCY_LEVELS=1,2,3,5,8,10,12,15 sudo ...
package main

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/trace"

	"github.com/e2b-dev/infra/packages/clickhouse/pkg/hoststats"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/cfg"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/proxy"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox"
	blockmetrics "github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block/metrics"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/cgroup"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/fc"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/nbd"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/network"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/template"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/template/peerclient"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/tcpfirewall"
	templatebuild "github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build"
	buildconfig "github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/config"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/metrics"
	"github.com/e2b-dev/infra/packages/shared/pkg/dockerhub"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/limit"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	sbxlogger "github.com/e2b-dev/infra/packages/shared/pkg/logger/sandbox"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

// concurrencyResult holds the measured latencies from one concurrent batch.
type concurrencyResult struct {
	sandboxID string
	latency   time.Duration
	err       error
}

// defaultConcurrencyLevels are the concurrency levels to benchmark.
var defaultConcurrencyLevels = []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}

// parseConcurrencyLevels reads CONCURRENCY_LEVELS env var (comma-separated ints)
// or returns the defaults.
func parseConcurrencyLevels() []int {
	env := os.Getenv("CONCURRENCY_LEVELS")
	if env == "" {
		return defaultConcurrencyLevels
	}

	parts := strings.Split(env, ",")
	levels := make([]int, 0, len(parts))

	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}

		n, err := strconv.Atoi(p)
		if err != nil || n <= 0 {
			continue
		}

		levels = append(levels, n)
	}

	if len(levels) == 0 {
		return defaultConcurrencyLevels
	}

	slices.Sort(levels)

	return levels
}

// percentile returns the p-th percentile (0-100) from a sorted slice of durations.
func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}

	idx := max(0, int(float64(len(sorted)-1)*p/100.0))
	idx = min(idx, len(sorted)-1)

	return sorted[idx]
}

// BenchmarkConcurrentResume benchmarks concurrent sandbox resume at various
// concurrency levels. For each level N, it launches N goroutines that call
// ResumeSandbox simultaneously and measures per-sandbox latency.
//
// Network and NBD pool sizes are scaled to the max concurrency level + headroom
// so the benchmark measures sandbox creation overhead, not pool starvation.
func BenchmarkConcurrentResume(b *testing.B) {
	if os.Geteuid() != 0 {
		b.Skip("skipping benchmark because not running as root")
	}

	levels := parseConcurrencyLevels()

	// test configuration - same template/kernel as BenchmarkBaseImageLaunch
	const (
		baseImage       = "e2bdev/base"
		kernelVersion   = "vmlinux-6.1.158"
		fcVersion       = featureflags.DefaultFirecrackerVersion
		templateID      = "fcb33d09-3141-42c4-8d3b-c2df411681db"
		templateVersion = "v2.0.0"

		buildIDNormal    = "ba6aae36-74f7-487a-b6f7-74fd7c94e479"
		buildIDHugePages = "ba6aae36-74f7-487a-b6f7-74fd7c94e480"
	)

	disableHP, _ := strconv.ParseBool(os.Getenv("DISABLE_HUGE_PAGES"))
	useHugePages := !disableHP
	buildID := buildIDHugePages
	if !useHugePages {
		buildID = buildIDNormal
		b.Log("huge pages disabled")
	}

	// cache & ephemeral paths
	persistenceDir := getPersistenceDir()
	kernelsDir := filepath.Join(persistenceDir, "kernels")
	sandboxDir := filepath.Join(persistenceDir, "sandbox")
	require.NoError(b, os.MkdirAll(kernelsDir, 0o755))

	tempDir := b.TempDir()

	abs := func(s string) string {
		return utils.Must(filepath.Abs(s))
	}

	// optional OTEL tracing
	endpoint := os.Getenv("OTEL_COLLECTOR_GRPC_ENDPOINT")
	if endpoint != "" {
		spanExporter, err := telemetry.NewSpanExporter(b.Context(),
			otlptracegrpc.WithEndpoint(endpoint),
		)
		require.NoError(b, err)
		b.Cleanup(func() {
			ctx := context.WithoutCancel(b.Context())
			assert.NoError(b, spanExporter.Shutdown(ctx))
		})

		resource, err := telemetry.GetResource(b.Context(), "node-id", "BenchmarkConcurrentResume", "service-commit", "service-version", "service-instance-id")
		require.NoError(b, err)
		tracerProvider := telemetry.NewTracerProvider(spanExporter, resource)
		otel.SetTracerProvider(tracerProvider)
	}

	// kernel download
	linuxKernelURL, err := url.JoinPath("https://storage.googleapis.com/e2b-artifact-binaries/kernels/", kernelVersion, "vmlinux.bin")
	require.NoError(b, err)
	linuxKernelFilename := filepath.Join(kernelsDir, kernelVersion, "vmlinux.bin")
	downloadKernel(b, linuxKernelFilename, linuxKernelURL)

	// env vars
	b.Setenv("ARTIFACTS_REGISTRY_PROVIDER", "Local")
	b.Setenv("FIRECRACKER_VERSIONS_DIR", abs(filepath.Join("..", "..", "fc-versions", "builds")))
	b.Setenv("HOST_ENVD_PATH", abs(filepath.Join("..", "..", "envd", "bin", "envd")))
	b.Setenv("HOST_KERNELS_DIR", abs(kernelsDir))
	b.Setenv("LOCAL_TEMPLATE_STORAGE_BASE_PATH", abs(filepath.Join(persistenceDir, "templates")))
	b.Setenv("ORCHESTRATOR_BASE_PATH", tempDir)
	b.Setenv("SANDBOX_DIR", abs(sandboxDir))
	b.Setenv("STORAGE_PROVIDER", "Local")

	config, err := cfg.Parse()
	require.NoError(b, err)

	for _, subdir := range []string{"build", "build-templates", "sandbox", "template"} {
		require.NoError(b, os.MkdirAll(filepath.Join(tempDir, subdir), 0o755))
	}

	l, err := logger.NewDevelopmentLogger()
	require.NoError(b, err)
	sbxlogger.SetSandboxLoggerInternal(l)

	slotStorage, err := network.NewStorageLocal(b.Context(), config.NetworkConfig, network.NoopEgressProxy{})
	require.NoError(b, err)
	networkPool := network.NewPool(network.NewSlotsPoolSize, network.ReusedSlotsPoolSize, slotStorage, config.NetworkConfig)
	go func() {
		networkPool.Populate(b.Context())
		l.Info(b.Context(), "network pool populated")
	}()
	b.Cleanup(func() {
		ctx := context.WithoutCancel(b.Context())
		assert.NoError(b, networkPool.Close(ctx))
	})

	devicePool, err := nbd.NewDevicePool(config.NBDPoolSize)
	require.NoError(b, err, "do you have the nbd kernel module installed?")
	go func() {
		devicePool.Populate(b.Context())
		l.Info(b.Context(), "device pool populated")
	}()
	b.Cleanup(func() {
		ctx := context.WithoutCancel(b.Context())
		assert.NoError(b, devicePool.Close(ctx))
	})

	featureFlags, err := featureflags.NewClient()
	require.NoError(b, err)
	b.Cleanup(func() {
		ctx := context.WithoutCancel(b.Context())
		assert.NoError(b, featureFlags.Close(ctx))
	})

	limiter, err := limit.New(b.Context(), featureFlags)
	require.NoError(b, err)

	templateSpec, err := cfg.TemplateStorage()
	require.NoError(b, err)
	persistence, err := storage.NewProvider(b.Context(), templateSpec, storage.WithLimiter(limiter))
	require.NoError(b, err)

	blockMetrics, err := blockmetrics.NewMetrics(&noop.MeterProvider{})
	require.NoError(b, err)

	templateCache, err := template.NewCache(config, featureFlags, persistence, blockMetrics, peerclient.NopResolver())
	require.NoError(b, err)
	templateCache.Start(b.Context())
	b.Cleanup(templateCache.Stop)

	cgroupManager, err := cgroup.NewManager()
	require.NoError(b, err, "cgroups v2 not available - running as root?")
	require.NoError(b, cgroupManager.Initialize(b.Context()), "failed to initialize root cgroup")

	sandboxes := sandbox.NewSandboxesMap()
	sandboxFactory := sandbox.NewFactory(
		b.Context(),
		config.BuilderConfig, networkPool, devicePool,
		featureFlags, hoststats.NewNoopDelivery(), cgroupManager, network.NewNoopEgressProxy(), sandbox.NoopNetworkAssignHook{}, sandboxes,
	)

	dockerhubRepository, err := dockerhub.GetRemoteRepository(b.Context())
	require.NoError(b, err)
	b.Cleanup(func() { assert.NoError(b, dockerhubRepository.Close()) })

	accessToken := "access-token"
	sandboxConfig := sandbox.NewConfig(sandbox.Config{
		BaseTemplateID:  templateID,
		Vcpu:            2,
		RamMB:           512,
		TotalDiskSizeMB: 2 * 1024,
		HugePages:       useHugePages,
		Envd: sandbox.EnvdMetadata{
			Vars:        map[string]string{"HELLO": "WORLD"},
			AccessToken: &accessToken,
			Version:     "1.2.3",
		},
		FirecrackerConfig: fc.Config{
			KernelVersion:      kernelVersion,
			FirecrackerVersion: fcVersion,
		},
	})

	templateSpec2, err := cfg.TemplateStorage()
	require.NoError(b, err)
	persistenceTemplate, err := storage.NewProvider(b.Context(), templateSpec2)
	require.NoError(b, err)

	buildCacheSpec, err := cfg.BuildCacheStorage()
	require.NoError(b, err)
	persistenceBuild, err := storage.NewProvider(b.Context(), buildCacheSpec)
	require.NoError(b, err)

	var proxyPort uint16 = 5007

	tcpFw := tcpfirewall.New(l, config.NetworkConfig, sandboxes, noop.NewMeterProvider(), featureFlags)
	go func() { assert.NoError(b, tcpFw.Start(b.Context())) }()
	b.Cleanup(func() {
		ctx := context.WithoutCancel(b.Context())
		assert.NoError(b, tcpFw.Close(ctx))
	})

	sandboxProxy, err := proxy.NewSandboxProxy(noop.MeterProvider{}, proxyPort, sandboxes, featureFlags)
	require.NoError(b, err)
	go func() {
		err := sandboxProxy.Start(b.Context())
		assert.ErrorIs(b, err, http.ErrServerClosed)
	}()
	b.Cleanup(func() {
		ctx := context.WithoutCancel(b.Context())
		assert.NoError(b, sandboxProxy.Close(ctx))
	})

	buildMetrics, err := metrics.NewBuildMetrics(noop.MeterProvider{})
	require.NoError(b, err)

	builder := templatebuild.NewBuilder(
		config.BuilderConfig, l, featureFlags, sandboxFactory,
		persistenceTemplate, persistenceBuild,
		dockerhubRepository, sandboxProxy, sandboxes, templateCache, buildMetrics,
		nil,
	)

	// build template if not cached
	buildPath := filepath.Join(os.Getenv("LOCAL_TEMPLATE_STORAGE_BASE_PATH"), buildID, "rootfs.ext4")
	if _, err := os.Stat(buildPath); os.IsNotExist(err) {
		force := true
		templateConfig := buildconfig.TemplateConfig{
			Version:            templateVersion,
			TemplateID:         templateID,
			FromImage:          baseImage,
			Force:              &force,
			VCpuCount:          sandboxConfig.Vcpu,
			MemoryMB:           sandboxConfig.RamMB,
			StartCmd:           "echo 'start cmd debug' && sleep .1 && echo 'done starting command debug'",
			DiskSizeMB:         sandboxConfig.TotalDiskSizeMB,
			FreeDiskSizeMB:     sandboxConfig.TotalDiskSizeMB,
			HugePages:          sandboxConfig.HugePages,
			KernelVersion:      kernelVersion,
			FirecrackerVersion: fcVersion,
		}

		metadata := storage.Paths{BuildID: buildID}
		_, err = builder.Build(b.Context(), metadata, templateConfig, l.Detach(b.Context()).Core())
		require.NoError(b, err)
	}

	tmpl, err := templateCache.GetTemplate(b.Context(), buildID, false, false)
	require.NoError(b, err)

	// warm-up: create and destroy one sandbox to prime caches
	b.Log("warming up: creating one sandbox to prime caches...")
	warmupRuntime := sandbox.RuntimeMetadata{
		TemplateID:  templateID,
		SandboxID:   "warmup-" + uuid.NewString()[:8],
		ExecutionID: "warmup-exec",
		TeamID:      "bench-team",
	}
	warmupSbx, err := sandboxFactory.ResumeSandbox(
		b.Context(), tmpl, sandboxConfig, warmupRuntime,
		time.Now(), time.Now().Add(2*time.Minute), nil,
	)
	require.NoError(b, err, "warm-up sandbox creation failed")
	require.NoError(b, warmupSbx.Close(b.Context()))
	b.Log("warm-up complete")

	// run sub-benchmarks per concurrency level
	//
	// Latencies from every iteration are accumulated so that percentiles
	// are computed over the full dataset (e.g. 100x at concurrency-5
	// yields 500 latency samples). Wall-clock times are also accumulated
	// and averaged.
	for _, n := range levels {
		b.Run(fmt.Sprintf("concurrency-%d", n), func(b *testing.B) {
			var allLatencies []time.Duration
			var allWallClocks []time.Duration
			var totalOK, totalFail int

			for b.Loop() {
				results, wall := runConcurrentResume(b, sandboxFactory, tmpl, sandboxConfig, templateID, n)
				allWallClocks = append(allWallClocks, wall)

				for _, r := range results {
					if r.err != nil {
						totalFail++
						b.Logf("  FAIL sandbox %s: %v", r.sandboxID, r.err)
					} else {
						totalOK++
						allLatencies = append(allLatencies, r.latency)
					}
				}
			}

			reportAggregateResults(b, n, allLatencies, allWallClocks, totalOK, totalFail)
		})
	}
}

// runConcurrentResume launches n goroutines that each create a sandbox simultaneously.
// Returns a slice of results (one per goroutine) and the wall-clock duration.
// Sandboxes are cleaned up before returning.
func runConcurrentResume(
	b *testing.B,
	factory *sandbox.Factory,
	tmpl template.Template,
	config *sandbox.Config,
	templateID string,
	n int,
) ([]concurrencyResult, time.Duration) {
	b.Helper()

	results := make([]concurrencyResult, n)
	created := make([]*sandbox.Sandbox, n)

	// Barrier: all goroutines wait until the gate is closed, then start simultaneously.
	gate := make(chan struct{})
	var wg sync.WaitGroup

	b.StopTimer()

	for i := range n {
		wg.Go(func() {
			runtime := sandbox.RuntimeMetadata{
				TemplateID:  templateID,
				SandboxID:   fmt.Sprintf("bench-%d-%s", i, uuid.NewString()[:8]),
				ExecutionID: fmt.Sprintf("bench-exec-%d", i),
				TeamID:      "bench-team",
			}

			// Wait for the barrier.
			<-gate

			ctx, span := tracer.Start(b.Context(), "bench-resume",
				trace.WithAttributes(
					attribute.Int("concurrency", n),
					attribute.Int("sandbox.index", i),
					attribute.String("sandbox.id", runtime.SandboxID),
				),
			)

			start := time.Now()
			sbx, err := factory.ResumeSandbox(
				ctx,
				tmpl,
				config,
				runtime,
				time.Now(),
				time.Now().Add(2*time.Minute),
				nil,
			)
			elapsed := time.Since(start)
			span.End()

			results[i] = concurrencyResult{
				sandboxID: runtime.SandboxID,
				latency:   elapsed,
				err:       err,
			}
			created[i] = sbx
		})
	}

	// Open the gate - start measuring.
	b.StartTimer()
	wallStart := time.Now()
	close(gate)

	// Wait for all goroutines to finish.
	wg.Wait()
	wallDuration := time.Since(wallStart)
	b.StopTimer()

	// Clean up all sandboxes.
	for i, sbx := range created {
		if sbx != nil {
			if err := sbx.Close(b.Context()); err != nil {
				b.Logf("warning: failed to close sandbox %d (%s): %v", i, results[i].sandboxID, err)
			}
		}
	}

	b.StartTimer()

	return results, wallDuration
}

// reportAggregateResults computes and reports latency statistics aggregated
// across all benchmark iterations. This gives stable percentiles even at
// high iteration counts (e.g. -benchtime=100x).
func reportAggregateResults(b *testing.B, concurrency int, latencies []time.Duration, wallClocks []time.Duration, totalOK, totalFail int) {
	b.Helper()

	iterations := len(wallClocks)

	b.ReportMetric(float64(totalOK)/float64(iterations), "ok")
	b.ReportMetric(float64(totalFail)/float64(iterations), "fail")

	// Average wall-clock across iterations.
	if len(wallClocks) > 0 {
		var totalWall time.Duration
		for _, w := range wallClocks {
			totalWall += w
		}
		avgWall := totalWall / time.Duration(len(wallClocks))
		b.ReportMetric(float64(avgWall.Milliseconds()), "wall-clock-ms")
	}

	if len(latencies) == 0 {
		b.Logf("concurrency=%d: all sandboxes failed across %d iterations", concurrency, iterations)

		return
	}

	slices.Sort(latencies)

	fastest := latencies[0]
	slowest := latencies[len(latencies)-1]

	var total time.Duration
	for _, l := range latencies {
		total += l
	}
	avg := total / time.Duration(len(latencies))

	p50 := percentile(latencies, 50)
	p95 := percentile(latencies, 95)
	p99 := percentile(latencies, 99)

	b.ReportMetric(float64(avg.Milliseconds()), "avg-ms")
	b.ReportMetric(float64(p50.Milliseconds()), "p50-ms")
	b.ReportMetric(float64(p95.Milliseconds()), "p95-ms")
	b.ReportMetric(float64(p99.Milliseconds()), "p99-ms")
	b.ReportMetric(float64(fastest.Milliseconds()), "min-ms")
	b.ReportMetric(float64(slowest.Milliseconds()), "max-ms")

	b.Logf("concurrency=%d: %d ok, %d fail across %d iterations (%d samples) | avg=%s p50=%s p95=%s p99=%s min=%s max=%s",
		concurrency, totalOK, totalFail, iterations, len(latencies),
		avg.Truncate(time.Millisecond),
		p50.Truncate(time.Millisecond),
		p95.Truncate(time.Millisecond),
		p99.Truncate(time.Millisecond),
		fastest.Truncate(time.Millisecond),
		slowest.Truncate(time.Millisecond),
	)
}
