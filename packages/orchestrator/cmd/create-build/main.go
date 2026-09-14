//go:build linux

package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/launchdarkly/go-sdk-common/v3/ldlog"
	"github.com/launchdarkly/go-sdk-common/v3/ldvalue"
	"go.opentelemetry.io/otel/metric/noop"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/e2b-dev/infra/packages/clickhouse/pkg/hoststats"
	"github.com/e2b-dev/infra/packages/orchestrator/cmd/internal/cmdutil"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/cfg"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/proxy"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/artifact"
	blockmetrics "github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block/metrics"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/cgroup"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/nbd"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/network"
	sbxtemplate "github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/template"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/template/peerclient"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/tcpfirewall"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/config"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/metrics"
	"github.com/e2b-dev/infra/packages/shared/pkg/dockerhub"
	"github.com/e2b-dev/infra/packages/shared/pkg/fcversion"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	templatemanager "github.com/e2b-dev/infra/packages/shared/pkg/grpc/template-manager"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	sbxlogger "github.com/e2b-dev/infra/packages/shared/pkg/logger/sandbox"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
	"github.com/e2b-dev/infra/packages/shared/pkg/templates"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

const baseImage = "e2bdev/base:latest"

// proxyPort is the sandbox proxy listen port. It defaults to 5007 but can be
// overridden via PROXY_PORT so create-build can run alongside a live
// orchestrator (which already holds the default port).
func proxyPort() uint16 {
	if v := os.Getenv("PROXY_PORT"); v != "" {
		if p, err := strconv.ParseUint(v, 10, 16); err == nil {
			return uint16(p)
		}
		log.Printf("warning: ignoring invalid PROXY_PORT=%q, using default 5007", v)
	}

	return 5007
}

func main() {
	templateID := flag.String("template", "local-template", "template id")
	fromBuild := flag.String("from-build", "", "base build ID to build from (incremental build)")
	toBuild := flag.String("to-build", "", "output build ID (UUID, required)")
	storagePath := flag.String("storage", "", "storage: local path or gs://bucket (default: gs://$TEMPLATE_BUCKET_NAME or .local-build)")
	sandboxDir := flag.String("sandbox-dir", "", "override SANDBOX_DIR (the rootfs path baked into the snapshot)")
	kernel := flag.String("kernel", featureflags.DefaultKernelVersion, "kernel version")
	fc := flag.String("firecracker", featureflags.DefaultFirecrackerVersion, "firecracker version")
	vcpu := flag.Int("vcpu", 2, "vCPUs")
	memory := flag.Int("memory", 1024, "memory MB")
	disk := flag.Int("disk", 1024, "disk MB")
	hugePages := flag.Bool("hugepages", true, "use 2MB huge pages for memory (false = 4KB pages)")
	disableMemfd := flag.Bool("disable-memfd", false, "disable memfd-backed guest memory")
	memfileDiffDedup := flag.Bool("memfile-diff-dedup", false, "enable 4KiB-page deduplication of memfile diff against the base template")
	startCmd := flag.String("start-cmd", "", "start command")
	setupCmd := flag.String("setup-cmd", "", "setup command to run during build (e.g., install deps)")
	readyCmd := flag.String("ready-cmd", "", "ready check command")
	timeout := flag.Int("timeout", 5, "build timeout in minutes")
	verbose := flag.Bool("v", false, "verbose output")
	flag.Parse()

	if *disableMemfd {
		featureflags.OverrideBoolFlag(featureflags.UseMemFdFlag, false)
	}

	if *memfileDiffDedup {
		featureflags.OverrideJSONFlag(featureflags.MemfileDiffDedupFlag, ldvalue.FromJSONMarshal(map[string]any{
			"enabled": true,
		}))
	}

	if *toBuild == "" {
		log.Fatal("-to-build required")
	}

	// FPH must be installed at boot — flag is read by Create, not Resume.
	featureflags.NewJSONFlag("free-page-hinting-config", ldvalue.FromJSONMarshal(map[string]any{
		"enabled": true,
	}))

	// Suppress other noisy output unless verbose, but keep std log for fatal errors
	if !*verbose {
		cmdutil.SuppressNoisyLogsKeepStdLog()
	}

	ctx := context.Background()

	// Only run setupEnv when -storage is explicitly passed (for local development)
	// Otherwise, use existing environment variables (like the original code did)
	localMode := false
	if *storagePath != "" {
		localMode = !strings.HasPrefix(*storagePath, "gs://")
		if err := setupEnv(ctx, *storagePath, *sandboxDir, *kernel, *fc, localMode); err != nil {
			log.Fatal(err)
		}
	}

	builderConfig, err := cfg.ParseBuilder()
	if err != nil {
		log.Fatalf("config: %v", err)
	}
	networkConfig, err := network.ParseConfig()
	if err != nil {
		log.Fatalf("network config: %v", err)
	}

	err = doBuild(ctx, *templateID, *toBuild, *fromBuild, *kernel, *fc, *vcpu, *memory, *disk, *hugePages, *startCmd, *setupCmd, *readyCmd, localMode, *verbose, *timeout, builderConfig, networkConfig)
	if err != nil {
		log.Fatal(err)
	}
}

func setupEnv(ctx context.Context, storagePath, sandboxDir, kernel, fc string, localMode bool) error {
	abs := func(s string) string { return utils.Must(filepath.Abs(s)) }

	if sandboxDir != "" {
		os.Setenv("SANDBOX_DIR", sandboxDir)
	}

	if localMode {
		if os.Geteuid() != 0 {
			return errors.New("local mode requires root")
		}

		dataDir := storagePath
		dirs := []string{"kernels", "templates", "build-cache", "sandbox", "orchestrator", "fc-versions"}
		for _, d := range dirs {
			if err := os.MkdirAll(filepath.Join(dataDir, d), 0o755); err != nil {
				return fmt.Errorf("mkdir %s: %w", d, err)
			}
		}
		for _, d := range []string{"build", "build-templates", "sandbox", "template"} {
			if err := os.MkdirAll(filepath.Join(dataDir, "orchestrator", d), 0o755); err != nil {
				return fmt.Errorf("mkdir orchestrator/%s: %w", d, err)
			}
		}

		env := map[string]string{
			"ARTIFACTS_REGISTRY_PROVIDER": "Local",
			"FIRECRACKER_VERSIONS_DIR":    abs(filepath.Join(dataDir, "fc-versions")),
			"HOST_KERNELS_DIR":            abs(filepath.Join(dataDir, "kernels")),
			"ORCHESTRATOR_BASE_PATH":      abs(filepath.Join(dataDir, "orchestrator")),
		}
		for k, v := range env {
			if os.Getenv(k) == "" {
				os.Setenv(k, v)
			}
		}

		// The explicit -storage flag is authoritative: set unconditionally,
		// unlike the fill-the-gap defaults above.
		os.Setenv("TEMPLATE_STORAGE_URL", "file://"+abs(filepath.Join(dataDir, "templates")))
		os.Setenv("BUILD_CACHE_STORAGE_URL", "file://"+abs(filepath.Join(dataDir, "build-cache")))

		if err := setupKernel(ctx, filepath.Join(dataDir, "kernels"), kernel); err != nil {
			return err
		}
		if err := setupFC(ctx, filepath.Join(dataDir, "fc-versions"), fc); err != nil {
			return err
		}

		// HOST_ENVD_PATH: use env if set, otherwise default to local dev path
		envdPath := os.Getenv("HOST_ENVD_PATH")
		if envdPath == "" {
			envdPath = abs("../envd/bin/envd")
			os.Setenv("HOST_ENVD_PATH", envdPath)
		}
		if _, err := os.Stat(envdPath); err == nil {
			fmt.Printf("✓ Envd: %s\n", envdPath)
		}

		// HOST_BUSYBOX_DIR: use env if set, otherwise default to local .busybox dir.
		// Run "make fetch-busybox" in packages/orchestrator to download the binary.
		busyboxDir := os.Getenv("HOST_BUSYBOX_DIR")
		if busyboxDir == "" {
			busyboxDir = abs(".busybox")
			os.Setenv("HOST_BUSYBOX_DIR", busyboxDir)
		}
		busyboxVersion := os.Getenv("BUSYBOX_VERSION")
		if busyboxVersion == "" {
			busyboxVersion = cfg.DefaultBusyboxVersion
		}
		busyboxBin := filepath.Join(busyboxDir, busyboxVersion, runtime.GOARCH, "busybox")
		if _, err := os.Stat(busyboxBin); err == nil {
			fmt.Printf("✓ Busybox: %s\n", busyboxBin)
		} else {
			fmt.Printf("⚠ Busybox not found at %s — run 'make fetch-busybox' in packages/orchestrator\n", busyboxBin)
		}

		fmt.Printf("✓ Storage: %s (local)\n", dataDir)
	} else {
		bucket := strings.TrimPrefix(storagePath, "gs://")
		os.Setenv("TEMPLATE_STORAGE_URL", "gs://"+bucket)
		fmt.Printf("✓ Storage: gs://%s\n", bucket)
	}

	return nil
}

func doBuild(
	parentCtx context.Context,
	templateID, buildID, fromBuild, kernel, fc string,
	vcpu, memory, disk int,
	hugePages bool,
	startCmd, setupCmd, readyCmd string,
	localMode, verbose bool,
	timeout int,
	builderConfig cfg.BuilderConfig,
	networkConfig network.Config,
) error {
	ctx, cancel := context.WithTimeout(parentCtx, time.Duration(timeout)*time.Minute)
	defer cancel()

	var cores []zapcore.Core
	if localMode && !verbose {
		// Only log errors when in local mode without verbose
		cores = append(cores, zapcore.NewCore(
			zapcore.NewConsoleEncoder(zap.NewDevelopmentEncoderConfig()),
			zapcore.AddSync(os.Stderr),
			zap.ErrorLevel,
		))
	}

	l, err := logger.NewLogger(logger.LoggerConfig{
		ServiceName:   "build-template",
		IsInternal:    true,
		IsDebug:       verbose,
		EnableConsole: verbose,
		Cores:         cores,
	})
	if err != nil {
		return fmt.Errorf("logger: %w", err)
	}
	logger.ReplaceGlobals(ctx, l)
	sbxlogger.SetSandboxLoggerExternal(l)
	if verbose {
		sbxlogger.SetSandboxLoggerInternal(l)
	} else {
		sbxlogger.SetSandboxLoggerInternal(logger.NewNopLogger())
	}

	l.Info(ctx, "building template", logger.WithTemplateID(templateID), logger.WithBuildID(buildID))

	logLevel := ldlog.Error
	if verbose {
		logLevel = ldlog.Info
	}
	featureFlags, _ := featureflags.NewClientWithLogLevel(logLevel)

	sandboxes := sandbox.NewSandboxesMap()

	sandboxProxy, err := proxy.NewSandboxProxy(noop.MeterProvider{}, proxyPort(), sandboxes, featureFlags)
	if err != nil {
		return fmt.Errorf("proxy: %w", err)
	}
	go func() {
		if err := sandboxProxy.Start(parentCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
			l.Error(ctx, "proxy error", zap.Error(err))
		}
	}()
	defer sandboxProxy.Close(parentCtx)

	tcpFirewall := tcpfirewall.New(l, networkConfig, sandboxes, noop.NewMeterProvider(), featureFlags)
	go tcpFirewall.Start(ctx)
	defer tcpFirewall.Close(parentCtx)

	templateSpec, err := cfg.TemplateStorage()
	if err != nil {
		return fmt.Errorf("template storage: %w", err)
	}
	persistenceTemplate, err := storage.NewProvider(ctx, templateSpec)
	if err != nil {
		return fmt.Errorf("template storage: %w", err)
	}
	buildCacheSpec, err := cfg.BuildCacheStorage()
	if err != nil {
		return fmt.Errorf("build storage: %w", err)
	}
	persistenceBuild, err := storage.NewProvider(ctx, buildCacheSpec)
	if err != nil {
		return fmt.Errorf("build storage: %w", err)
	}

	blockMetrics, _ := blockmetrics.NewMetrics(noop.NewMeterProvider())

	if os.Getenv("NODE_IP") == "" {
		os.Setenv("NODE_IP", "127.0.0.1")
	}

	c, err := cfg.Parse()
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}

	devicePool, err := nbd.NewDevicePool(c.NBDPoolSize)
	if err != nil {
		return fmt.Errorf("nbd pool: %w", err)
	}
	go devicePool.Populate(ctx)
	defer devicePool.Close(parentCtx)

	slotStorage, err := network.NewStorageLocal(ctx, networkConfig, tcpFirewall)
	if err != nil {
		return fmt.Errorf("network storage: %w", err)
	}
	networkPool := network.NewPool(8, 8, slotStorage, networkConfig)
	go networkPool.Populate(ctx)
	defer networkPool.Close(parentCtx)

	dockerhubRepo, err := dockerhub.GetRemoteRepository(ctx)
	if err != nil {
		return fmt.Errorf("dockerhub: %w", err)
	}
	defer dockerhubRepo.Close()

	templateCache, err := sbxtemplate.NewCache(c, featureFlags, persistenceTemplate, blockMetrics, peerclient.NopResolver())
	if err != nil {
		return fmt.Errorf("template cache: %w", err)
	}
	templateCache.Start(ctx)
	defer templateCache.Stop()

	buildMetrics, _ := metrics.NewBuildMetrics(noop.MeterProvider{})
	sandboxFactory := sandbox.NewFactory(ctx, c.BuilderConfig, networkPool, devicePool, featureFlags, hoststats.NewNoopDelivery(), cgroup.NewNoopManager(), network.NewNoopEgressProxy(), sandbox.NoopNetworkAssignHook{}, sandboxes)

	// Layered V4 builds need the upload coordinator so child layers wait on
	// their parents' header finalization. Redis is nil (CLI is single-host —
	// no cross-orch signaling needed); local same-orch coordination via
	// futures is what matters here.
	uploads := sandbox.NewUploads(templateCache, persistenceTemplate, peerclient.NopResolver(), nil)
	defer uploads.Stop()

	builder := build.NewBuilder(
		builderConfig, l, featureFlags, sandboxFactory,
		persistenceTemplate, persistenceBuild,
		dockerhubRepo, sandboxProxy, sandboxes, templateCache, buildMetrics,
		uploads,
	)

	l = l.With(zap.String("envID", templateID)).With(zap.String("buildID", buildID))

	force := true

	// Build steps list for setup commands
	var steps []*templatemanager.TemplateStep
	if setupCmd != "" {
		fmt.Printf("Setup command (as root): %s\n", setupCmd)
		// Add a RUN step that executes as root
		steps = append(steps, &templatemanager.TemplateStep{
			Type: "RUN",
			Args: []string{setupCmd, "root"}, // command, user
		})
	}

	// Mirror prod gating in pkg/template/server/create_template.go: balloon
	// is rejected on FC <1.14, so we can't unconditionally request FPR.
	fcInfo, err := fcversion.New(fc)
	if err != nil {
		return fmt.Errorf("invalid firecracker version %q: %w", fc, err)
	}

	tmpl := config.TemplateConfig{
		Version:            templates.TemplateV2LatestVersion,
		TemplateID:         templateID,
		Force:              &force,
		VCpuCount:          int64(vcpu),
		MemoryMB:           int64(memory),
		DiskSizeMB:         int64(disk),
		FreeDiskSizeMB:     int64(disk),
		HugePages:          hugePages,
		StartCmd:           startCmd,
		ReadyCmd:           readyCmd,
		KernelVersion:      kernel,
		FirecrackerVersion: fc,
		FreePageReporting:  fcInfo.HasFreePageReporting(),
		FreePageHinting:    fcInfo.HasFreePageHinting(),
		TeamID:             "local",
		Steps:              steps,
	}

	pageSizeStr := "2MB (hugepages)"
	if !hugePages {
		pageSizeStr = "4KB (standard)"
	}
	fmt.Printf("Building with page size: %s\n", pageSizeStr)

	if fromBuild != "" {
		tmpl.FromTemplate = &templatemanager.FromTemplateConfig{BuildID: fromBuild}
		fmt.Printf("Building from: %s\n", fromBuild)
	} else {
		tmpl.FromImage = baseImage
	}

	result, err := builder.Build(ctx, storage.Paths{BuildID: buildID}, tmpl, l.Detach(ctx).Core())
	if err != nil {
		return fmt.Errorf("build: %w", err)
	}

	fmt.Printf("\n✅ Build finished: %s\n", buildID)

	// Print artifact sizes
	printArtifactSizes(ctx, persistenceTemplate, buildID, result)

	return nil
}

func printArtifactSizes(ctx context.Context, persistence storage.StorageProvider, buildID string, _ *build.Result) {
	paths := storage.Paths{BuildID: buildID}
	var basePath string
	if spec, err := cfg.TemplateStorage(); err == nil && spec.Provider == storage.LocalStorageProvider {
		basePath = spec.BasePath
	}

	fmt.Printf("\n📦 Artifacts:\n")

	// For local storage, get actual file sizes on disk
	if basePath != "" {
		printLocalFileSizes(basePath, buildID)
	} else {
		// For remote storage, get sizes from storage provider
		if memfile, err := persistence.OpenSeekable(ctx, paths.Memfile()); err == nil {
			if size, err := memfile.Size(ctx); err == nil {
				fmt.Printf("   Memfile: %d MB\n", size>>20)
			}
		}
	}
}

func printLocalFileSizes(basePath, buildID string) {
	dir := filepath.Join(basePath, buildID)

	for _, a := range cmdutil.MainArtifacts() {
		path := filepath.Join(dir, a.File)
		_, actual, err := cmdutil.GetFileSizes(path)
		if err != nil {
			continue
		}

		headerPath := filepath.Join(dir, a.HeaderFile)
		totalSize, blockSize := cmdutil.GetHeaderInfo(headerPath)
		if totalSize == 0 {
			fmt.Printf("   %s: %d MB (this layer)\n", a.Name, actual>>20)

			continue
		}

		pct := float64(actual) / float64(totalSize) * 100
		fmt.Printf("   %s: %d MB diff / %d MB total (%.1f%%), block size: %d KB\n",
			a.Name, actual>>20, totalSize>>20, pct, blockSize>>10)
	}

	for _, a := range cmdutil.SmallArtifacts() {
		path := filepath.Join(dir, a.File)
		if actual, err := cmdutil.GetActualFileSize(path); err == nil {
			fmt.Printf("   %s: %d KB\n", a.Name, actual>>10)
		}
	}
}

func setupKernel(ctx context.Context, dir, version string) error {
	arch := utils.TargetArch()
	dstPath := filepath.Join(dir, version, arch, artifact.KernelFileName)

	if err := os.MkdirAll(filepath.Dir(dstPath), 0o755); err != nil {
		return fmt.Errorf("mkdir kernel dir: %w", err)
	}

	if _, err := os.Stat(dstPath); err == nil {
		fmt.Printf("✓ Kernel %s (%s) exists\n", version, arch)

		return nil
	}

	// Try arch-specific URL first: {version}/{arch}/vmlinux.bin
	archURL, err := url.JoinPath("https://storage.googleapis.com/e2b-artifact-binaries/kernels/", version, arch, artifact.KernelFileName)
	if err != nil {
		return fmt.Errorf("invalid kernel URL: %w", err)
	}

	fmt.Printf("⬇ Downloading kernel %s (%s)...\n", version, arch)

	if err := download(ctx, archURL, dstPath, 0o644); err == nil {
		return nil
	} else if !errors.Is(err, errNotFound) {
		return fmt.Errorf("failed to download kernel: %w", err)
	}

	// Legacy URLs are x86_64-only; only fall back for amd64.
	if arch != "amd64" {
		return fmt.Errorf("kernel %s not found for %s (no legacy fallback for non-amd64)", version, arch)
	}

	legacyURL, err := url.JoinPath("https://storage.googleapis.com/e2b-artifact-binaries/kernels/", version, artifact.KernelFileName)
	if err != nil {
		return fmt.Errorf("invalid kernel legacy URL: %w", err)
	}

	fmt.Printf("  %s path not found, trying legacy URL...\n", arch)

	return download(ctx, legacyURL, dstPath, 0o644)
}

func setupFC(ctx context.Context, dir, version string) error {
	arch := utils.TargetArch()
	dstPath := filepath.Join(dir, version, arch, artifact.FirecrackerBinaryName)

	if err := os.MkdirAll(filepath.Dir(dstPath), 0o755); err != nil {
		return fmt.Errorf("mkdir firecracker dir: %w", err)
	}

	if _, err := os.Stat(dstPath); err == nil {
		fmt.Printf("✓ Firecracker %s (%s) exists\n", version, arch)

		return nil
	}

	// Download from GCS bucket with {version}/{arch}/firecracker path
	fcURL, err := url.JoinPath("https://storage.googleapis.com/e2b-artifact-binaries/firecrackers/", version, arch, artifact.FirecrackerBinaryName)
	if err != nil {
		return fmt.Errorf("invalid Firecracker URL: %w", err)
	}

	fmt.Printf("⬇ Downloading Firecracker %s (%s)...\n", version, arch)

	if err := download(ctx, fcURL, dstPath, 0o755); err == nil {
		return nil
	} else if !errors.Is(err, errNotFound) {
		return fmt.Errorf("failed to download Firecracker: %w", err)
	}

	// Legacy URLs are x86_64-only; only fall back for amd64.
	if arch != "amd64" {
		return fmt.Errorf("firecracker %s not found for %s (no legacy fallback for non-amd64)", version, arch)
	}

	legacyURL, err := url.JoinPath("https://storage.googleapis.com/e2b-artifact-binaries/firecrackers/", version, artifact.FirecrackerBinaryName)
	if err != nil {
		return fmt.Errorf("invalid Firecracker legacy URL: %w", err)
	}

	fmt.Printf("  %s path not found, trying legacy URL...\n", arch)

	return download(ctx, legacyURL, dstPath, 0o755)
}

var errNotFound = errors.New("not found")

func download(ctx context.Context, rawURL, path string, perm os.FileMode) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return fmt.Errorf("invalid download URL %s: %w", rawURL, err)
	}

	resp, err := (&http.Client{Timeout: 5 * time.Minute}).Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return fmt.Errorf("%w: %s", errNotFound, rawURL)
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, rawURL)
	}

	// Write to a temporary file and rename atomically to avoid partial files
	// on network errors or disk-full conditions.
	tmpPath := path + ".tmp"

	f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, perm)
	if err != nil {
		return err
	}

	if _, err := io.Copy(f, resp.Body); err != nil {
		f.Close()
		os.Remove(tmpPath)

		return err
	}

	if err := f.Close(); err != nil {
		os.Remove(tmpPath)

		return err
	}

	if err := os.Rename(tmpPath, path); err != nil {
		os.Remove(tmpPath)

		return err
	}

	fmt.Printf("✓ Downloaded %s\n", filepath.Base(path))

	return nil
}
