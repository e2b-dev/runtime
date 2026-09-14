//go:build linux

package smoketest

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/launchdarkly/go-sdk-common/v3/ldlog"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	"github.com/e2b-dev/infra/packages/clickhouse/pkg/hoststats"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/cfg"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/proxy"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/artifact"
	blockmetrics "github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block/metrics"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/cgroup"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/fc"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/nbd"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/network"
	sbxtemplate "github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/template"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/template/peerclient"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/tcpfirewall"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/config"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/metrics"
	"github.com/e2b-dev/infra/packages/shared/pkg/consts"
	"github.com/e2b-dev/infra/packages/shared/pkg/dockerhub"
	"github.com/e2b-dev/infra/packages/shared/pkg/fcversion"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	sbxlogger "github.com/e2b-dev/infra/packages/shared/pkg/logger/sandbox"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
	"github.com/e2b-dev/infra/packages/shared/pkg/templates"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

const (
	baseImage = "ubuntu:22.04"
	proxyPort = 5009
)

// TestSmokeAllFCVersions builds a template and resumes from it for every
// Firecracker version in FirecrackerVersionMap. It requires root, Docker,
// the envd binary, KVM, NBD, and hugepages.
func TestSmokeAllFCVersions(t *testing.T) { //nolint:paralleltest // subtests share infra and must run sequentially
	checkPrerequisites(t)

	dataDir := t.TempDir()
	envdPath := findOrBuildEnvd(t)

	setupLocalDirs(t, dataDir)
	setupEnvVars(t, dataDir, envdPath)

	downloadKernel(t, dataDir)
	for _, fcVersion := range featureflags.FirecrackerVersionMap {
		downloadFC(t, dataDir, fcVersion)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Minute)
	defer cancel()

	infra := newTestInfra(t, ctx)
	defer infra.close(ctx)

	for fcMajor, fcVersion := range featureflags.FirecrackerVersionMap { //nolint:paralleltest // sequential by design
		t.Run("fc-"+fcMajor, func(t *testing.T) {
			buildID := uuid.New().String()

			// Phase 1: create build
			t.Logf("creating build %s with FC %s", buildID, fcVersion)
			force := true
			_, err := infra.builder.Build(
				ctx,
				storage.Paths{BuildID: buildID},
				config.TemplateConfig{
					Version:            templates.TemplateV2LatestVersion,
					TemplateID:         "smoke-" + fcMajor,
					Force:              &force,
					VCpuCount:          2,
					MemoryMB:           512,
					DiskSizeMB:         512,
					FreeDiskSizeMB:     512,
					HugePages:          true,
					KernelVersion:      featureflags.DefaultKernelVersion,
					FirecrackerVersion: fcVersion,
					FromImage:          baseImage,
				},
				logger.NewNopLogger().Detach(ctx).Core(),
			)
			require.NoError(t, err, "create build failed for FC %s", fcVersion)
			t.Logf("build %s done", buildID)

			// Phase 2: resume from the build
			t.Logf("resuming build %s", buildID)
			tmpl, err := infra.templateCache.GetTemplate(ctx, buildID, false, false)
			require.NoError(t, err, "load template for FC %s", fcVersion)

			meta, err := tmpl.Metadata()
			require.NoError(t, err)

			token := "smoke-test"
			t0 := time.Now()
			sbx, err := infra.factory.ResumeSandbox(
				ctx,
				tmpl,
				sandbox.NewConfig(sandbox.Config{
					BaseTemplateID: "smoke-" + fcMajor,
					Vcpu:           2,
					RamMB:          512,
					HugePages:      true,
					Envd: sandbox.EnvdMetadata{
						Vars:        map[string]string{},
						AccessToken: &token,
						Version:     "1.0.0",
					},
					FirecrackerConfig: fc.Config{
						KernelVersion:      meta.Template.KernelVersion,
						FirecrackerVersion: meta.Template.FirecrackerVersion,
					},
				}),
				sandbox.RuntimeMetadata{
					TemplateID:  "smoke-" + fcMajor,
					TeamID:      "smoke",
					SandboxID:   fmt.Sprintf("sbx-smoke-%s-%d", fcMajor, time.Now().UnixNano()),
					ExecutionID: uuid.NewString(),
				},
				t0,
				t0.Add(10*time.Minute),
				nil,
			)
			require.NoError(t, err, "resume failed for FC %s", fcVersion)
			t.Logf("resumed in %s", time.Since(t0))

			// Phase 3: freeze and thaw the live guest rootfs. (envd readiness
			// is already guaranteed: ResumeSandbox runs WaitForEnvd before
			// returning unless SkipEnvdWait is set.)
			assertFsFreezeQuiescesRootfs(t, ctx, sbx, token)

			sbx.Close(context.WithoutCancel(ctx))
		})
	}
}

// testInfra holds the shared infrastructure used across all FC version sub-tests.
type testInfra struct {
	builder       *build.Builder
	factory       *sandbox.Factory
	templateCache *sbxtemplate.Cache

	// resources to close
	closers []func(context.Context)
}

func (ti *testInfra) close(ctx context.Context) {
	cleanCtx := context.WithoutCancel(ctx)
	for _, closer := range slices.Backward(ti.closers) {
		closer(cleanCtx)
	}
}

func newTestInfra(t *testing.T, ctx context.Context) *testInfra {
	t.Helper()

	l := logger.NewNopLogger()
	sbxlogger.SetSandboxLoggerInternal(l)
	sbxlogger.SetSandboxLoggerExternal(l)

	flags, _ := featureflags.NewClientWithLogLevel(ldlog.Error)

	builderConfig, err := cfg.ParseBuilder()
	require.NoError(t, err)

	networkConfig, err := network.ParseConfig()
	require.NoError(t, err)

	orcConfig, err := cfg.Parse()
	require.NoError(t, err)

	ti := &testInfra{}

	// Storage
	templateSpec, err := cfg.TemplateStorage()
	require.NoError(t, err)
	persistenceTemplate, err := storage.NewProvider(ctx, templateSpec)
	require.NoError(t, err)

	buildCacheSpec, err := cfg.BuildCacheStorage()
	require.NoError(t, err)
	persistenceBuild, err := storage.NewProvider(ctx, buildCacheSpec)
	require.NoError(t, err)

	// NBD
	devicePool, err := nbd.NewDevicePool(orcConfig.NBDPoolSize)
	require.NoError(t, err)
	go devicePool.Populate(ctx)
	ti.closers = append(ti.closers, func(ctx context.Context) { devicePool.Close(ctx) })

	// Sandbox proxy + TCP firewall
	sandboxes := sandbox.NewSandboxesMap()

	tcpFw := tcpfirewall.New(l, networkConfig, sandboxes, noop.NewMeterProvider(), flags)
	go tcpFw.Start(ctx)
	ti.closers = append(ti.closers, func(ctx context.Context) { tcpFw.Close(ctx) })

	// Network
	slotStorage, err := network.NewStorageLocal(ctx, networkConfig, tcpFw)
	require.NoError(t, err)
	networkPool := network.NewPool(8, 8, slotStorage, networkConfig)
	go networkPool.Populate(ctx)
	ti.closers = append(ti.closers, func(ctx context.Context) { networkPool.Close(ctx) })

	// Artifacts / Docker
	dockerhubRepo, err := dockerhub.GetRemoteRepository(ctx)
	require.NoError(t, err)
	ti.closers = append(ti.closers, func(_ context.Context) { dockerhubRepo.Close() })

	// Template cache
	blockMetrics, _ := blockmetrics.NewMetrics(noop.NewMeterProvider())
	templateCache, err := sbxtemplate.NewCache(orcConfig, flags, persistenceTemplate, blockMetrics, peerclient.NopResolver())
	require.NoError(t, err)
	templateCache.Start(ctx)
	ti.closers = append(ti.closers, func(_ context.Context) { templateCache.Stop() })
	ti.templateCache = templateCache

	sandboxProxy, err := proxy.NewSandboxProxy(noop.MeterProvider{}, proxyPort, sandboxes, flags)
	require.NoError(t, err)
	go sandboxProxy.Start(ctx)
	ti.closers = append(ti.closers, func(ctx context.Context) { sandboxProxy.Close(ctx) })

	// Factory + Builder
	factory := sandbox.NewFactory(ctx, orcConfig.BuilderConfig, networkPool, devicePool, flags, hoststats.NewNoopDelivery(), cgroup.NewNoopManager(), network.NewNoopEgressProxy(), sandbox.NoopNetworkAssignHook{}, sandboxes)
	ti.factory = factory

	buildMetrics, _ := metrics.NewBuildMetrics(noop.MeterProvider{})
	ti.builder = build.NewBuilder(
		builderConfig, l, flags, factory,
		persistenceTemplate, persistenceBuild,
		dockerhubRepo, sandboxProxy, sandboxes, templateCache, buildMetrics,
		nil,
	)

	return ti
}

// --- prerequisites ----------------------------------------------------------

func checkPrerequisites(t *testing.T) {
	t.Helper()

	if os.Geteuid() != 0 {
		t.Skip("requires root")
	}

	if _, err := os.Stat("/dev/kvm"); err != nil {
		t.Skip("/dev/kvm not available")
	}

	// Firecracker host assets, shipped with the orchestrator host image.
	builderConfig, err := cfg.ParseBuilder()
	require.NoError(t, err)

	busybox := filepath.Join(builderConfig.HostBusyboxDir, builderConfig.BusyboxVersion, runtime.GOARCH, "busybox")
	if _, err := os.Stat(busybox); err != nil {
		t.Skipf("busybox binary %q not available; set HOST_BUSYBOX_DIR/BUSYBOX_VERSION", busybox)
	}
}

// --- envd -------------------------------------------------------------------

func findOrBuildEnvd(t *testing.T) string {
	t.Helper()

	if p := os.Getenv("HOST_ENVD_PATH"); p != "" {
		if _, err := os.Stat(p); err == nil {
			t.Logf("using envd from HOST_ENVD_PATH: %s", p)

			return p
		}
	}

	envdDir := locateEnvdSource(t)
	if envdDir == "" {
		t.Skip("cannot locate envd source directory")
	}

	binPath := filepath.Join(envdDir, "bin", "envd")
	if _, err := os.Stat(binPath); err == nil {
		t.Logf("using existing envd binary: %s", binPath)

		return binPath
	}

	t.Logf("building envd from %s", envdDir)
	require.NoError(t, os.MkdirAll(filepath.Join(envdDir, "bin"), 0o755))

	cmd := exec.CommandContext(t.Context(), "go", "build", "-o", binPath, ".") //nolint:gosec // trusted input
	cmd.Dir = envdDir
	cmd.Env = append(os.Environ(), "CGO_ENABLED=0", "GOOS=linux", "GOARCH="+runtime.GOARCH)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Skipf("failed to build envd: %v\n%s", err, out)
	}

	t.Logf("built envd: %s", binPath)

	return binPath
}

func locateEnvdSource(t *testing.T) string {
	t.Helper()

	wd, err := os.Getwd()
	require.NoError(t, err)

	// The monorepo keeps envd at go/oss/envd, the public layout at
	// packages/envd.
	layouts := [][]string{{"go", "oss", "envd"}, {"packages", "envd"}}

	for dir := wd; dir != "/"; dir = filepath.Dir(dir) {
		for _, layout := range layouts {
			candidate := filepath.Join(append([]string{dir}, layout...)...)
			if _, err := os.Stat(filepath.Join(candidate, "main.go")); err == nil {
				return candidate
			}
		}
	}

	return ""
}

// --- local storage setup ----------------------------------------------------

func setupLocalDirs(t *testing.T, dataDir string) {
	t.Helper()
	for _, d := range []string{"kernels", "templates", "sandbox", "orchestrator", "fc-versions", "build-cache"} {
		require.NoError(t, os.MkdirAll(filepath.Join(dataDir, d), 0o755))
	}
	for _, d := range []string{"build", "build-templates", "sandbox", "template"} {
		require.NoError(t, os.MkdirAll(filepath.Join(dataDir, "orchestrator", d), 0o755))
	}
}

func setupEnvVars(t *testing.T, dataDir, envdPath string) {
	t.Helper()

	abs := func(rel string) string {
		a, err := filepath.Abs(rel)
		require.NoError(t, err)

		return a
	}

	vars := map[string]string{
		"ARTIFACTS_REGISTRY_PROVIDER":         "Local",
		"FIRECRACKER_VERSIONS_DIR":            abs(filepath.Join(dataDir, "fc-versions")),
		"HOST_ENVD_PATH":                      envdPath,
		"HOST_KERNELS_DIR":                    abs(filepath.Join(dataDir, "kernels")),
		"LOCAL_TEMPLATE_STORAGE_BASE_PATH":    abs(filepath.Join(dataDir, "templates")),
		"LOCAL_BUILD_CACHE_STORAGE_BASE_PATH": abs(filepath.Join(dataDir, "build-cache")),
		"ORCHESTRATOR_BASE_PATH":              abs(filepath.Join(dataDir, "orchestrator")),
		"SANDBOX_DIR":                         abs(filepath.Join(dataDir, "sandbox")),
		"STORAGE_PROVIDER":                    "Local",
	}

	for k, v := range vars {
		t.Setenv(k, v)
	}
}

// --- binary downloads -------------------------------------------------------

func downloadKernel(t *testing.T, dataDir string) {
	t.Helper()
	dst := filepath.Join(dataDir, "kernels", featureflags.DefaultKernelVersion, artifact.KernelFileName)
	url := fmt.Sprintf("https://storage.googleapis.com/e2b-artifact-binaries/kernels/%s/%s", featureflags.DefaultKernelVersion, artifact.KernelFileName)
	downloadFile(t, url, dst, 0o644)
}

func downloadFC(t *testing.T, dataDir, version string) {
	t.Helper()

	dst := filepath.Join(dataDir, "fc-versions", version, artifact.FirecrackerBinaryName)

	// e2b-format releases (vX.Y-a.b.c) are published by the release pipeline
	// to the public artifact bucket — the corresponding GitHub release
	// holding the same assets is private, so the bucket is the only
	// unauthenticated source. Fetch the HOST's arch (the bucket carries
	// amd64 and arm64) into the arch path that setupFC/the fc config
	// resolve first.
	if info, err := fcversion.New(version); err == nil {
		if _, isE2B := info.E2BVersion(); isE2B {
			arch := utils.TargetArch()
			archDst := filepath.Join(dataDir, "fc-versions", version, arch, artifact.FirecrackerBinaryName)
			url := fmt.Sprintf("https://storage.googleapis.com/e2b-artifact-binaries/firecrackers/%s/%s/firecracker", version, arch)
			downloadFile(t, url, archDst, 0o755)

			return
		}
	}

	// Old releases in https://github.com/e2b-dev/fc-versions/releases don't build
	// x86_64 and aarch64 binaries. They just build the former and the asset's name
	// is just 'firecracker'
	// TODO: Drop this work-around once we remove support for Firecracker v1.10
	assetName := "firecracker-amd64"
	if strings.HasPrefix(version, "v1.10") {
		assetName = artifact.FirecrackerBinaryName
	}

	url := fmt.Sprintf("https://github.com/e2b-dev/fc-versions/releases/download/%s/%s", version, assetName)
	downloadFile(t, url, dst, 0o755)
}

func downloadFile(t *testing.T, url, dst string, perm os.FileMode) {
	t.Helper()

	if _, err := os.Stat(dst); err == nil {
		return
	}

	require.NoError(t, os.MkdirAll(filepath.Dir(dst), 0o755))

	t.Logf("downloading %s", url)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err, "download %s", url)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode, "download %s returned HTTP %d", url, resp.StatusCode)

	f, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, perm)
	require.NoError(t, err)
	defer f.Close()

	_, err = io.Copy(f, resp.Body)
	require.NoError(t, err)
}

// --- fsfreeze ---------------------------------------------------------------

const (
	// rootfsProbeDir is on the guest root filesystem — the one /fsfreeze
	// quiesces. /tmp can be a separate tmpfs, which freezing the rootfs would not
	// touch, so probes have to target the rootfs to observe anything.
	rootfsProbeDir = "/var/tmp"

	// frozenWriteWindow is how long a write is given to prove it is blocked. A
	// write to a frozen filesystem blocks until thaw, so any completion at all
	// means the freeze did not take; the wait only has to outlast an unblocked
	// write.
	frozenWriteWindow = 3 * time.Second

	// thawedWriteTimeout bounds that same write once thawed.
	thawedWriteTimeout = 60 * time.Second

	envdRequestTimeout = 30 * time.Second
)

// assertFsFreezeQuiescesRootfs drives envd's /fsfreeze and /fsthaw against the
// live guest and checks the property the filesystem-only pause rests on: freezing
// does not merely flush the rootfs, it stops writes, so nothing can be
// acknowledged between the flush and the VM pause.
//
// It reaches envd at the sandbox slot IP, the way the orchestrator does. The
// sandbox proxy does not route control-plane routes, so this is the only side
// from which they can be driven — which is why the assertion lives here rather
// than in the integration suite.
//
// Writes and reads go through envd's public file API, so a blocked write is
// observed exactly as a caller would experience one. That envd serves them at all
// while its own root filesystem is frozen is the other half of the claim: it is
// what lets envd answer the thaw on the pause-failure rollback path.
func assertFsFreezeQuiescesRootfs(t *testing.T, ctx context.Context, sbx *sandbox.Sandbox, accessToken string) {
	t.Helper()

	assertFsFreezeQuiescesRootfsAt(t, ctx, fmt.Sprintf("http://%s:%d", sbx.Slot.HostIPString(), consts.DefaultEnvdServerPort), accessToken)
}

// assertFsFreezeQuiescesRootfsAt is the body of assertFsFreezeQuiescesRootfs
// against an arbitrary envd address, so the probe's own control flow — detecting
// a write that never returns, and releasing it on every exit path — is testable
// without a guest. See fsfreeze_probe_test.go.
func assertFsFreezeQuiescesRootfsAt(t *testing.T, ctx context.Context, envdURL, accessToken string) {
	t.Helper()

	// Outlasts the blocked write so the client does not give up before the thaw
	// releases it, which would look like a completion.
	client := &http.Client{Timeout: thawedWriteTimeout + envdRequestTimeout}

	// The pre-freeze write retries transport errors: the first call after a
	// snapshot resume can be reset by the guest. Every observed CI failure
	// was a read-phase reset (the handshake completed, then the connection
	// died) on this first call, while envd itself was up — the resume's own
	// /init, on its own connection, had already succeeded. Production
	// tolerates this window because its first-contact paths retry; a
	// one-shot client turned it into a flake. The frozen write below must
	// NOT retry — blocking is its assertion.
	readable := rootfsProbeDir + "/smoke-before-freeze"
	status, err := retryTransportErrors(ctx, envdRequestTimeout, func() (int, error) {
		return writeGuestFile(ctx, client, envdURL, accessToken, readable, "before freeze")
	})
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, status, "a rootfs write should succeed before the freeze")

	status, err = postEnvd(ctx, client, envdURL, accessToken, "/fsfreeze")
	require.NoError(t, err)
	require.Equal(t, http.StatusNoContent, status, "freezing the guest rootfs should succeed")

	// A write to a frozen filesystem blocks in the kernel and cannot be
	// interrupted, so it is left running and collected after the thaw. Closing
	// after the send lets the cleanup below wait on the same channel whether or
	// not the body already collected the result.
	blocked := make(chan int, 1)

	go func() {
		defer close(blocked)

		blockedStatus, blockedErr := writeGuestFile(ctx, client, envdURL, accessToken, rootfsProbeDir+"/smoke-while-frozen", "released by thaw")
		if blockedErr != nil {
			t.Logf("write released by the thaw failed: %v", blockedErr)
		}

		blocked <- blockedStatus
	}()

	thawed := false

	defer func() {
		// However this exits, leave the rootfs writable: sbx.Close has to tear
		// down a VM whose filesystem is not wedged.
		//
		// On its own context, because the reason for the exit may well be that ctx
		// was cancelled — a thaw that gives up because the deadline already passed
		// is the one case that strands a frozen guest.
		if !thawed {
			cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), envdRequestTimeout)
			defer cancel()

			if _, err := postEnvd(cleanupCtx, client, envdURL, accessToken, "/fsthaw"); err != nil {
				t.Errorf("thawing the guest rootfs during cleanup failed: %v", err)
			}
		}

		select {
		case <-blocked:
		case <-time.After(thawedWriteTimeout):
			t.Error("the write blocked by the freeze never completed after the thaw")
		}
	}()

	select {
	case blockedStatus := <-blocked:
		t.Fatalf("a write to the frozen rootfs completed with HTTP %d; the freeze did not take", blockedStatus)
	case <-time.After(frozenWriteWindow):
	}

	// Reads are unaffected — freezing is a write barrier — and serving this at all
	// shows envd is still responsive with its own filesystem frozen. This read is
	// forced onto a FRESH dial (the client's only pooled connection is held by
	// the blocked write above), making it the next-most-exposed call after the
	// pre-freeze write, so it retries the same way; reads are idempotent. The
	// freeze/thaw POSTs are deliberately not retried: they reuse pooled
	// connections, no observed failure has ever hit anything but the first
	// post-resume call, and blindly retrying a state-flipping POST is unsound
	// (a lost-response retry can observe its own success as an error).
	status, err = retryTransportErrors(ctx, envdRequestTimeout, func() (int, error) {
		return readGuestFile(ctx, client, envdURL, accessToken, readable)
	})
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, status, "reads should still work while the rootfs is frozen")

	status, err = postEnvd(ctx, client, envdURL, accessToken, "/fsthaw")
	require.NoError(t, err)
	require.Equal(t, http.StatusNoContent, status, "thawing the guest rootfs should succeed")

	thawed = true

	select {
	case blockedStatus := <-blocked:
		require.Equal(t, http.StatusOK, blockedStatus, "the write blocked by the freeze should succeed once thawed")
	case <-time.After(thawedWriteTimeout):
		t.Fatal("the write blocked by the freeze did not complete after the thaw")
	}
}

// postEnvd calls one of envd's control routes. These are the calls the sandbox
// proxy refuses; reaching envd directly at the slot IP is the orchestrator's own
// path to them.
//
// Kept free of testing assertions so it is safe to call from a goroutine.
func postEnvd(ctx context.Context, client *http.Client, envdURL, accessToken, route string) (int, error) {
	reqCtx, cancel := context.WithTimeout(ctx, envdRequestTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, envdURL+route, http.NoBody)
	if err != nil {
		return 0, err
	}

	return doEnvd(client, req, accessToken)
}

// retryTransportErrors reruns an envd call while it fails at the transport
// layer, for up to budget. Immediately after a snapshot resume the guest can
// reset connections it has already accepted: every observed CI failure was a
// read-phase reset (handshake completed, so NOT a dial-phase stale-tuple
// refusal) on the first call after a resume, never on a later one. Retrying
// past that window is how production's first-contact paths cross it. Use
// only for calls that are safe to rerun.
func retryTransportErrors(ctx context.Context, budget time.Duration, call func() (int, error)) (int, error) {
	deadline := time.Now().Add(budget)
	for {
		status, err := call()
		if err == nil || time.Now().After(deadline) || ctx.Err() != nil {
			return status, err
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// writeGuestFile uploads content to path on the guest as root, through envd's
// public file API. Writing as root keeps the probe off any assumption about which
// unprivileged user a template ships.
func writeGuestFile(ctx context.Context, client *http.Client, envdURL, accessToken, path, content string) (int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, envdURL+"/files?"+guestFileQuery(path), strings.NewReader(content))
	if err != nil {
		return 0, err
	}

	// envd takes the body verbatim only for application/octet-stream; anything
	// else is parsed as multipart.
	req.Header.Set("Content-Type", "application/octet-stream")

	return doEnvd(client, req, accessToken)
}

func readGuestFile(ctx context.Context, client *http.Client, envdURL, accessToken, path string) (int, error) {
	reqCtx, cancel := context.WithTimeout(ctx, envdRequestTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, envdURL+"/files?"+guestFileQuery(path), http.NoBody)
	if err != nil {
		return 0, err
	}

	return doEnvd(client, req, accessToken)
}

func guestFileQuery(path string) string {
	return url.Values{"path": {path}, "username": {"root"}}.Encode()
}

func doEnvd(client *http.Client, req *http.Request, accessToken string) (int, error) {
	req.Header.Set("X-Access-Token", accessToken)

	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	// Drain so the connection can be reused; the guest is on the other side of a
	// freeze and connections are not free.
	_, _ = io.Copy(io.Discard, resp.Body)

	return resp.StatusCode, nil
}
