//go:build linux

// run with something like:
//
// sudo `which go` test -benchtime=15s -bench=. -v
// sudo modprobe nbd
// echo 1024 | sudo tee /proc/sys/vm/nr_hugepages
package main

import (
	"context"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/metric/noop"

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
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build"
	buildconfig "github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/config"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/metrics"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/metadata"
	"github.com/e2b-dev/infra/packages/shared/pkg/dockerhub"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/limit"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	sbxlogger "github.com/e2b-dev/infra/packages/shared/pkg/logger/sandbox"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

var tracer = otel.Tracer("github.com/e2b-dev/infra/packages/orchestrator/benchmarks")

func BenchmarkBaseImageLaunch(b *testing.B) {
	if os.Geteuid() != 0 {
		b.Skip("skipping benchmark because not running as root")
	}

	// test configuration
	const (
		testType        = onlyStart
		baseImage       = "e2bdev/base"
		kernelVersion   = "vmlinux-6.1.158"
		fcVersion       = featureflags.DefaultFirecrackerVersion
		templateID      = "fcb33d09-3141-42c4-8d3b-c2df411681db"
		buildID         = "ba6aae36-74f7-487a-b6f7-74fd7c94e479"
		useHugePages    = false
		templateVersion = "v2.0.0"
	)

	// cache paths, to speed up test runs. these paths aren't wiped between tests
	persistenceDir := getPersistenceDir()
	kernelsDir := filepath.Join(persistenceDir, "kernels")
	sandboxDir := filepath.Join(persistenceDir, "sandbox")
	err := os.MkdirAll(kernelsDir, 0o755)
	require.NoError(b, err)

	// ephemeral data
	tempDir := b.TempDir()

	abs := func(s string) string {
		return utils.Must(filepath.Abs(s))
	}

	endpoint := os.Getenv("OTEL_COLLECTOR_GRPC_ENDPOINT")
	if endpoint != "" {
		spanExporter, err := telemetry.NewSpanExporter(b.Context(),
			otlptracegrpc.WithEndpoint(endpoint),
		)
		b.Cleanup(func() {
			ctx := context.WithoutCancel(b.Context())
			err := spanExporter.Shutdown(ctx)
			assert.NoError(b, err)
		})

		require.NoError(b, err)
		resource, err := telemetry.GetResource(b.Context(), "node-id", "BenchmarkBaseImageLaunch", "service-commit", "service-version", "service-instance-id")
		require.NoError(b, err)
		tracerProvider := telemetry.NewTracerProvider(spanExporter, resource)
		otel.SetTracerProvider(tracerProvider)
	}

	linuxKernelURL, err := url.JoinPath("https://storage.googleapis.com/e2b-artifact-binaries/kernels/", kernelVersion, "vmlinux.bin")
	require.NoError(b, err)
	linuxKernelFilename := filepath.Join(kernelsDir, kernelVersion, "vmlinux.bin")

	downloadKernel(b, linuxKernelFilename, linuxKernelURL)

	// hacks, these should go away
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

	// prep directories
	for _, subdir := range []string{"build", "build-templates" /*"fc-vm",*/, "sandbox", "template"} {
		fullDirName := filepath.Join(tempDir, subdir)
		err := os.MkdirAll(fullDirName, 0o755)
		require.NoError(b, err)
	}

	l, err := logger.NewDevelopmentLogger()
	require.NoError(b, err)

	sbxlogger.SetSandboxLoggerInternal(l)
	// sbxlogger.SetSandboxLoggerExternal(logger)

	slotStorage, err := network.NewStorageLocal(b.Context(), config.NetworkConfig, network.NoopEgressProxy{})
	require.NoError(b, err)
	networkPool := network.NewPool(8, 8, slotStorage, config.NetworkConfig)
	go func() {
		networkPool.Populate(b.Context())
		l.Info(b.Context(), "network pool populated")
	}()
	b.Cleanup(func() {
		ctx := context.WithoutCancel(b.Context())
		err := networkPool.Close(ctx)
		assert.NoError(b, err)
	})

	devicePool, err := nbd.NewDevicePool(config.NBDPoolSize)
	require.NoError(b, err, "do you have the nbd kernel module installed?")
	go func() {
		devicePool.Populate(b.Context())
		l.Info(b.Context(), "device pool populated")
	}()
	b.Cleanup(func() {
		ctx := context.WithoutCancel(b.Context())
		err := devicePool.Close(ctx)
		assert.NoError(b, err)
	})

	featureFlags, err := featureflags.NewClient()
	require.NoError(b, err)
	b.Cleanup(func() {
		ctx := context.WithoutCancel(b.Context())
		err := featureFlags.Close(ctx)
		assert.NoError(b, err)
	})

	limiter, err := limit.New(b.Context(), featureFlags)
	require.NoError(b, err)

	templateSpec, err := cfg.TemplateStorage()
	require.NoError(b, err)
	persistence, err := storage.NewProvider(b.Context(), templateSpec, storage.WithLimiter(limiter))
	require.NoError(b, err)

	blockMetrics, err := blockmetrics.NewMetrics(&noop.MeterProvider{})
	require.NoError(b, err)

	c, err := cfg.Parse()
	if err != nil {
		b.Fatalf("error parsing config: %v", err)
	}

	templateCache, err := template.NewCache(c, featureFlags, persistence, blockMetrics, peerclient.NopResolver())
	require.NoError(b, err)
	templateCache.Start(b.Context())
	b.Cleanup(templateCache.Stop)

	sandboxes := sandbox.NewSandboxesMap()
	sandboxFactory := sandbox.NewFactory(b.Context(), config.BuilderConfig, networkPool, devicePool, featureFlags, hoststats.NewNoopDelivery(), cgroup.NewNoopManager(), network.NewNoopEgressProxy(), sandbox.NoopNetworkAssignHook{}, sandboxes)

	dockerhubRepository, err := dockerhub.GetRemoteRepository(b.Context())
	require.NoError(b, err)
	b.Cleanup(func() {
		err := dockerhubRepository.Close()
		assert.NoError(b, err)
	})

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

	runtime := sandbox.RuntimeMetadata{
		TemplateID:  templateID,
		SandboxID:   "sandbox-id",
		ExecutionID: "execution-id",
		TeamID:      "team-id",
	}

	templateSpec2, err := cfg.TemplateStorage()
	require.NoError(b, err)
	persistenceTemplate, err := storage.NewProvider(b.Context(), templateSpec2)
	require.NoError(b, err)

	buildCacheSpec, err := cfg.BuildCacheStorage()
	require.NoError(b, err)
	persistenceBuild, err := storage.NewProvider(b.Context(), buildCacheSpec)
	require.NoError(b, err)

	var proxyPort uint16 = 5007

	tcpFirewall := tcpfirewall.New(
		l,
		config.NetworkConfig,
		sandboxes,
		noop.NewMeterProvider(),
		featureFlags,
	)
	go func() {
		err := tcpFirewall.Start(b.Context())
		assert.NoError(b, err)
	}()
	b.Cleanup(func() {
		ctx := context.WithoutCancel(b.Context())
		err := tcpFirewall.Close(ctx)
		assert.NoError(b, err)
	})

	sandboxProxy, err := proxy.NewSandboxProxy(noop.MeterProvider{}, proxyPort, sandboxes, featureFlags)
	require.NoError(b, err)
	go func() {
		err := sandboxProxy.Start(b.Context())
		assert.ErrorIs(b, http.ErrServerClosed, err)
	}()
	b.Cleanup(func() {
		ctx := context.WithoutCancel(b.Context())
		err := sandboxProxy.Close(ctx)
		assert.NoError(b, err)
	})

	buildMetrics, err := metrics.NewBuildMetrics(noop.MeterProvider{})
	require.NoError(b, err)

	builder := build.NewBuilder(
		config.BuilderConfig,
		l,
		featureFlags,
		sandboxFactory,
		persistenceTemplate,
		persistenceBuild,
		dockerhubRepository,
		sandboxProxy,
		sandboxes,
		templateCache,
		buildMetrics,
		nil,
	)

	buildPath := filepath.Join(os.Getenv("LOCAL_TEMPLATE_STORAGE_BASE_PATH"), buildID, "rootfs.ext4")
	if _, err := os.Stat(buildPath); os.IsNotExist(err) {
		// build template
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

		metadata := storage.Paths{
			BuildID: buildID,
		}
		_, err = builder.Build(b.Context(), metadata, templateConfig, l.Detach(b.Context()).Core())
		require.NoError(b, err)
	}

	// retrieve template
	tmpl, err := templateCache.GetTemplate(
		b.Context(),
		buildID,
		false,
		false,
	)
	require.NoError(b, err)

	tc := testContainer{
		sandboxFactory: sandboxFactory,
		testType:       testType,
		tmpl:           tmpl,
		sandboxConfig:  sandboxConfig,
		runtime:        runtime,
	}

	for b.Loop() {
		tc.testOneItem(b, buildID, kernelVersion, fcVersion)
	}
}

func getPersistenceDir() string {
	home := os.Getenv("HOME")
	if home != "" {
		return filepath.Join(home, ".cache", "e2b-orchestrator-benchmark")
	}

	return filepath.Join(os.TempDir(), "e2b-orchestrator-benchmark")
}

type testCycle string

const (
	onlyStart        testCycle = "only-start"
	startAndPause    testCycle = "start-and-pause"
	startPauseResume testCycle = "start-pause-resume"
)

type testContainer struct {
	testType       testCycle
	sandboxFactory *sandbox.Factory
	tmpl           template.Template
	sandboxConfig  *sandbox.Config
	runtime        sandbox.RuntimeMetadata
}

func (tc *testContainer) testOneItem(b *testing.B, buildID, kernelVersion, fcVersion string) {
	b.Helper()

	ctx, span := tracer.Start(b.Context(), "testOneItem")
	defer span.End()

	sbx, err := tc.sandboxFactory.ResumeSandbox(
		ctx,
		tc.tmpl,
		tc.sandboxConfig,
		tc.runtime,
		time.Now(),
		time.Now().Add(time.Second*15),
		nil,
	)
	require.NoError(b, err)

	if tc.testType == onlyStart {
		b.StopTimer()
		err = sbx.Close(ctx)
		require.NoError(b, err)
		b.StartTimer()

		return
	}

	meta, err := sbx.Template.Metadata()
	require.NoError(b, err)

	templateMetadata := meta.SameVersionTemplate(metadata.TemplateMetadata{
		BuildID:            buildID,
		KernelVersion:      kernelVersion,
		FirecrackerVersion: fcVersion,
	})
	snap, err := sbx.Pause(ctx, templateMetadata, sandbox.SnapshotUseCasePause)
	require.NoError(b, err)
	require.NotNil(b, snap)

	if tc.testType == startAndPause {
		b.StopTimer()
		err = sbx.Close(ctx)
		require.NoError(b, err)
		b.StartTimer()
	}

	// resume sandbox
	sbx, err = tc.sandboxFactory.ResumeSandbox(ctx, tc.tmpl, tc.sandboxConfig, tc.runtime, time.Now(), time.Now().Add(time.Second*15), nil)
	require.NoError(b, err)

	// close sandbox
	err = sbx.Close(ctx)
	require.NoError(b, err)
}

func downloadKernel(b *testing.B, filename, url string) {
	b.Helper()

	dirname := filepath.Dir(filename)
	err := os.MkdirAll(dirname, 0o755)
	require.NoError(b, err)

	// kernel already exists
	if _, err := os.Stat(filename); err == nil {
		return
	}

	client := &http.Client{}
	req, err := http.NewRequestWithContext(b.Context(), http.MethodGet, url, nil)
	require.NoError(b, err)
	response, err := client.Do(req)
	require.NoError(b, err)
	require.Equal(b, http.StatusOK, response.StatusCode)
	defer response.Body.Close()

	file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY, 0o644)
	require.NoError(b, err)
	defer file.Close()

	_, err = file.ReadFrom(response.Body)
	require.NoError(b, err)
}
