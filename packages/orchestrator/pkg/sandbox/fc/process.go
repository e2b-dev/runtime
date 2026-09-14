//go:build linux

package fc

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sync/atomic"
	"syscall"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"go.uber.org/zap/zapio"
	"golang.org/x/sync/errgroup"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/cfg"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/cgroup"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/network"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/rootfs"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/socket"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/template"
	"github.com/e2b-dev/infra/packages/shared/pkg/fc/client/operations"
	"github.com/e2b-dev/infra/packages/shared/pkg/keys"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	sbxlogger "github.com/e2b-dev/infra/packages/shared/pkg/logger/sandbox"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

var tracer = otel.Tracer("github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/fc")

// fcLogFilter wraps an io.Writer and suppresses Firecracker FlushMetrics
// request/response log line pairs that fire every few seconds and create
// excessive noise. The stateful flag is safe because Firecracker's API server
// is single-threaded: request and response logs are always adjacent with no
// interleaving from other actions.
type fcLogFilter struct {
	w            io.Writer
	skipResponse atomic.Bool
}

func (f *fcLogFilter) Write(p []byte) (n int, err error) {
	var filtered []byte

	for _, line := range bytes.SplitAfter(p, []byte("\n")) {
		if len(line) == 0 {
			continue
		}

		if bytes.Contains(line, []byte("FlushMetrics")) {
			f.skipResponse.Store(true)

			continue
		}

		if f.skipResponse.Load() && bytes.Contains(line, []byte("The request was executed successfully")) {
			f.skipResponse.Store(false)

			continue
		}

		filtered = append(filtered, line...)
	}

	if len(filtered) > 0 {
		_, err = f.w.Write(filtered)
	}

	return len(p), err
}

// ext4RootFlags are the ext4 mount flags passed on the kernel cmdline.
// discard: ext4 issues TRIM on freed blocks so they are elided from the
// snapshot diff. It must never include "noload": a filesystem-only snapshot
// resume cold-boots from the snapshot rootfs and relies on ext4 replaying the
// journal on mount.
const ext4RootFlags = "discard"

type ProcessOptions struct {
	// IoEngine is the io engine to use for the rootfs drive.
	IoEngine *string

	// InitScriptPath is the path to the init script that will be executed inside the VM on kernel start.
	InitScriptPath string

	// KernelLogs is a flag to enable kernel logs output to the process stdout.
	KernelLogs bool

	// SystemdToKernelLogs is a flag to enable systemd logs output to the console.
	// It enabled the kernel logs by default too.
	SystemdToKernelLogs bool

	// KvmClock is a flag to enable kvm-clock as the clocksource for the kernel.
	KvmClock bool

	// CmdlineArgs are extra guest kernel command line arguments overlaid on the
	// defaults. Empty is the default command line. Rejected wholesale if they include a key the orchestrator reserves
	// (see ValidateCmdlineArgs).
	//
	// Only boots that produce or restore a template's kernel need to set this: the
	// layer sandbox whose memory becomes the template, and the cold boot of a
	// filesystem-only snapshot. A memory resume never re-reads the command line.
	CmdlineArgs map[string]string

	// AccessToken, when non-nil, makes Create write the guest MMDS metadata
	// (sandbox/template IDs, logs address, and the access-token hash) before the
	// VM boots, so a cold-booted envd can authenticate /init the same way it does
	// after a memory resume. An empty string hashes to the "no token" value,
	// matching Resume. Template-build cold boots leave it nil and skip the write,
	// preserving their existing behavior.
	AccessToken *string

	// Stdout is the writer to which the process stdout will be written.
	Stdout io.Writer

	// Stderr is the writer to which the process stderr will be written.
	Stderr io.Writer
}

// TokenBucketConfig holds parameters for a single Firecracker token bucket.
// BucketSize < 0 disables the bucket.
type TokenBucketConfig struct {
	BucketSize   int64
	OneTimeBurst int64
	RefillTimeMs int64
}

// RateLimiterConfig holds rate limit parameters for a Firecracker device (network or block).
// Mirrors the Firecracker RateLimiter structure: two independent token buckets.
type RateLimiterConfig struct {
	Ops       TokenBucketConfig // packets; effective rate = BucketSize * 1000 / RefillTimeMs ops/s
	Bandwidth TokenBucketConfig // bytes;   effective rate = BucketSize * 1000 / RefillTimeMs bytes/s
}

type Process struct {
	Versions Config

	cmd *exec.Cmd

	config                cfg.BuilderConfig
	firecrackerSocketPath string
	metricsPath           string

	slot           *network.Slot
	rootfsProvider rootfs.Provider
	rootfsPath     string
	kernelPath     string
	files          *storage.SandboxFiles

	Exit *utils.ErrorOnce

	client *apiClient

	// balloonAccum is the cumulative virtio-balloon snapshot summed by the
	// metrics-reader goroutine (FC's SharedIncMetric resets per flush).
	balloonAccum atomic.Pointer[BalloonMetricsSnapshot]
}

func validateFirecrackerBinary(versions Config, config cfg.BuilderConfig) error {
	firecrackerPath := versions.FirecrackerPath(config)
	_, err := os.Stat(firecrackerPath)
	if err == nil {
		return nil
	}

	if errors.Is(err, os.ErrNotExist) {
		archPath, legacyPath := versions.firecrackerPaths(config)

		return fmt.Errorf("firecracker binary not found; checked architecture-specific path %q and legacy path %q: %w", archPath, legacyPath, err)
	}

	return fmt.Errorf("error stating firecracker binary %q: %w", firecrackerPath, err)
}

func NewProcess(
	ctx context.Context,
	execCtx context.Context,
	config cfg.BuilderConfig,
	slot *network.Slot,
	files *storage.SandboxFiles,
	versions Config,
	rootfsProvider rootfs.Provider,
	rootfsPaths RootfsPaths,
) (*Process, error) {
	ctx, childSpan := tracer.Start(ctx, "initialize-fc", trace.WithAttributes(
		attribute.Int("sandbox.slot.index", slot.Idx),
	))
	defer childSpan.End()

	// Build the firecracker start script and get computed paths
	startBuilder := NewStartScriptBuilder(config)
	startScript, err := startBuilder.Build(versions, files, rootfsPaths, slot.NamespaceID())
	if err != nil {
		return nil, err
	}

	telemetry.SetAttributes(ctx,
		attribute.String("sandbox.cmd", startScript.Value),
	)

	if err = validateFirecrackerBinary(versions, config); err != nil {
		return nil, err
	}

	_, err = os.Stat(versions.HostKernelPath(config))
	if err != nil {
		return nil, fmt.Errorf("error stating kernel file: %w", err)
	}

	cmd := exec.CommandContext(execCtx,
		"unshare",
		"-m",
		"--",
		"bash",
		"-c",
		startScript.Value,
	)

	p := &Process{
		Versions:              versions,
		Exit:                  utils.NewErrorOnce(),
		cmd:                   cmd,
		firecrackerSocketPath: files.SandboxFirecrackerSocketPath(),
		metricsPath:           files.SandboxMetricsFifoPath(),
		config:                config,
		client:                newApiClient(files.SandboxFirecrackerSocketPath()),
		rootfsProvider:        rootfsProvider,
		files:                 files,
		slot:                  slot,

		kernelPath: startScript.KernelPath,
		rootfsPath: startScript.RootfsPath,
	}

	cmd.SysProcAttr = &syscall.SysProcAttr{
		Setsid: true, // Create a new session
	}

	return p, nil
}

func (p *Process) configure(
	ctx context.Context,
	sbxMetadata sbxlogger.LoggerMetadata,
	stdoutExternal io.Writer,
	stderrExternal io.Writer,
	cgroupFD int,
) error {
	ctx, childSpan := tracer.Start(ctx, "configure-fc")
	defer childSpan.End()

	stdoutWriter := &zapio.Writer{Log: sbxlogger.I(sbxMetadata).Logger.Detach(ctx), Level: zap.InfoLevel}
	stdoutWriters := []io.Writer{stdoutWriter}
	if stdoutExternal != nil {
		stdoutWriters = append(stdoutWriters, stdoutExternal)
	}
	p.cmd.Stdout = &fcLogFilter{w: io.MultiWriter(stdoutWriters...)}

	stderrWriter := &zapio.Writer{Log: sbxlogger.I(sbxMetadata).Logger.Detach(ctx), Level: zap.ErrorLevel}
	stderrWriters := []io.Writer{stderrWriter}
	if stderrExternal != nil {
		stderrWriters = append(stderrWriters, stderrExternal)
	}
	p.cmd.Stderr = io.MultiWriter(stderrWriters...)

	// Set up cgroup FD for atomic placement via CLONE_INTO_CGROUP.
	// The cgroup is created and owned by the caller (Sandbox); Process only
	// uses the FD during clone.
	if cgroupFD != cgroup.NoCgroupFD {
		p.cmd.SysProcAttr.UseCgroupFD = true
		p.cmd.SysProcAttr.CgroupFD = cgroupFD
	}

	// Create the metrics FIFO before Firecracker starts.
	// Firecracker will open the write end once PUT /metrics is called.
	if err := syscall.Mkfifo(p.metricsPath, 0o600); err != nil {
		return fmt.Errorf("error creating fc metrics FIFO: %w", err)
	}

	err := p.cmd.Start()
	if err != nil {
		// cmd.Process is nil when Start fails, so Stop() won't reach the FIFO cleanup.
		// Remove the FIFO here to avoid leaving it behind.
		_ = os.Remove(p.metricsPath)

		return fmt.Errorf("error starting fc process: %w", err)
	}

	startCtx, cancelStart := context.WithCancelCause(ctx)
	defer cancelStart(errors.New("fc finished starting"))

	go func() {
		defer stderrWriter.Close()
		defer stdoutWriter.Close()

		waitErr := p.cmd.Wait()
		if waitErr != nil {
			if exitErr, ok := errors.AsType[*exec.ExitError](waitErr); ok {
				// Check if the process was killed by a signal
				if status, ok := exitErr.Sys().(syscall.WaitStatus); ok && status.Signaled() && (status.Signal() == syscall.SIGKILL || status.Signal() == syscall.SIGTERM) {
					p.Exit.SetError(nil)

					return
				}
			}

			logger.L().Error(ctx, "error waiting for fc process", zap.Error(waitErr))

			errMsg := fmt.Errorf("error waiting for fc process: %w", waitErr)
			p.Exit.SetError(errMsg)

			cancelStart(errMsg)

			return
		}

		p.Exit.SetError(nil)
	}()

	// Wait for the FC process to start so we can use FC API
	err = socket.Wait(startCtx, p.firecrackerSocketPath)
	if err != nil {
		errMsg := fmt.Errorf("error waiting for fc socket: %w", err)

		fcStopErr := p.Stop(ctx)

		return errors.Join(errMsg, fcStopErr)
	}

	return nil
}

func (p *Process) Create(
	ctx context.Context,
	sbxMetadata sbxlogger.LoggerMetadata,
	vCPUCount int64,
	memoryMB int64,
	hugePages bool,
	freePageReporting bool,
	freePageHinting bool,
	options ProcessOptions,
	txRateLimit RateLimiterConfig,
	driveRateLimit RateLimiterConfig,
	cgroupFD int,
) error {
	ctx, childSpan := tracer.Start(ctx, "create-fc")
	defer childSpan.End()

	// Symlink /dev/null to the rootfs link path, so we can start the FC process without the rootfs and then symlink the real rootfs.
	err := utils.SymlinkForce("/dev/null", p.files.SandboxCacheRootfsLinkPath(p.config.StorageConfig))
	if err != nil {
		return fmt.Errorf("error symlinking rootfs: %w", err)
	}

	err = p.configure(
		ctx,
		sbxMetadata,
		options.Stdout,
		options.Stderr,
		cgroupFD,
	)
	if err != nil {
		fcStopErr := p.Stop(ctx)

		return errors.Join(fmt.Errorf("error starting fc process: %w", err), fcStopErr)
	}

	// Start the metrics reader goroutine before calling setMetrics.
	// The goroutine blocks on open(O_RDONLY) until Firecracker opens the write end,
	// which happens when it processes PUT /metrics below.
	p.startMetricsReader(ctx)

	err = p.client.setMetrics(ctx, p.metricsPath)
	if err != nil {
		fcStopErr := p.Stop(ctx)

		return errors.Join(fmt.Errorf("error setting fc metrics: %w", err), fcStopErr)
	}
	telemetry.ReportEvent(ctx, "set fc metrics")

	// IPv4 configuration - format: [local_ip]::[gateway_ip]:[netmask]:hostname:iface:dhcp_option:[dns]
	ipv4 := fmt.Sprintf("%s::%s:%s:instance:%s:off:%s", p.slot.NamespaceIP(), p.slot.TapIPString(), p.slot.TapMaskString(), p.slot.VpeerName(), p.slot.TapName())
	kernelArgs := buildKernelArgs(ipv4, options).String()
	err = p.client.setBootSource(ctx, kernelArgs, p.kernelPath)
	if err != nil {
		fcStopErr := p.Stop(ctx)

		return errors.Join(fmt.Errorf("error setting fc boot source %q): %w", p.kernelPath, err), fcStopErr)
	}
	telemetry.ReportEvent(ctx, "set fc boot source config")

	// Rootfs
	rootfsPath, err := p.rootfsProvider.Path()
	if err != nil {
		fcStopErr := p.Stop(ctx)

		return errors.Join(fmt.Errorf("error getting rootfs path: %w", err), fcStopErr)
	}
	telemetry.ReportEvent(ctx, "got rootfs path")

	err = utils.SymlinkForce(rootfsPath, p.files.SandboxCacheRootfsLinkPath(p.config.StorageConfig))
	if err != nil {
		fcStopErr := p.Stop(ctx)

		return errors.Join(fmt.Errorf("error symlinking rootfs: %w", err), fcStopErr)
	}
	telemetry.ReportEvent(ctx, "symlinked rootfs")

	err = p.client.setRootfsDrive(ctx, p.rootfsPath, options.IoEngine, buildRateLimiter(driveRateLimit))
	if err != nil {
		fcStopErr := p.Stop(ctx)

		return errors.Join(fmt.Errorf("error setting fc drivers config: %w", err), fcStopErr)
	}
	telemetry.ReportEvent(ctx, "set fc drivers config")

	err = p.client.setNetworkInterface(ctx, p.slot.VpeerName(), p.slot.TapName(), p.slot.TapMAC(), buildRateLimiter(txRateLimit))
	if err != nil {
		fcStopErr := p.Stop(ctx)

		return errors.Join(fmt.Errorf("error setting fc network config: %w", err), fcStopErr)
	}
	telemetry.ReportEvent(ctx, "set fc network config")

	err = p.client.setMachineConfig(ctx, vCPUCount, memoryMB, hugePages)
	if err != nil {
		fcStopErr := p.Stop(ctx)

		return errors.Join(fmt.Errorf("error setting fc machine config: %w", err), fcStopErr)
	}
	telemetry.ReportEvent(ctx, "set fc machine config")

	err = p.client.setEntropyDevice(ctx)
	if err != nil {
		fcStopErr := p.Stop(ctx)

		return errors.Join(fmt.Errorf("error setting fc entropy config: %w", err), fcStopErr)
	}
	telemetry.ReportEvent(ctx, "set fc entropy config")

	if freePageReporting || freePageHinting {
		if err := p.client.installBalloon(ctx, freePageReporting, freePageHinting); err != nil {
			fcStopErr := p.Stop(ctx)

			return errors.Join(fmt.Errorf("error installing balloon device: %w", err), fcStopErr)
		}
		telemetry.ReportEvent(ctx, "installed balloon device",
			attribute.Bool("balloon.free_page_reporting", freePageReporting),
			attribute.Bool("balloon.free_page_hinting", freePageHinting),
		)
	}

	// Write MMDS metadata before boot when an access token is provided (the
	// cold-boot/reboot user path) so the guest envd can authenticate /init the
	// same way it does after a memory resume. The MMDS transport is already
	// configured by setNetworkInterface above. Template-build cold boots leave
	// AccessToken nil and skip this, preserving their existing behavior.
	if options.AccessToken != nil {
		md := sbxMetadata.LoggerMetadata()
		meta := &MmdsMetadata{
			SandboxID:            md.SandboxID,
			TemplateID:           md.TemplateID,
			LogsCollectorAddress: fmt.Sprintf("http://%s/logs", p.config.NetworkConfig.OrchestratorInSandboxIPAddress),
			AccessTokenHash:      keys.HashAccessToken(*options.AccessToken),
		}
		if err := p.client.setMmds(ctx, meta); err != nil {
			fcStopErr := p.Stop(ctx)

			return errors.Join(fmt.Errorf("error setting mmds: %w", err), fcStopErr)
		}
		telemetry.ReportEvent(ctx, "set fc mmds metadata")
	}

	err = p.client.startVM(ctx)
	if err != nil {
		fcStopErr := p.Stop(ctx)

		return errors.Join(fmt.Errorf("error starting fc: %w", err), fcStopErr)
	}

	telemetry.ReportEvent(ctx, "started fc")

	return nil
}

// ResumeInPlace resumes the already-running Firecracker process after an
// in-place snapshot (Pause + CreateSnapshot). Unlike Resume it does not
// reconfigure FC, wait for a UFFD socket or load a snapshot — the process,
// memory and rootfs overlay are all still live; it only un-pauses the VM.
func (p *Process) ResumeInPlace(
	ctx context.Context,
) error {
	ctx, span := tracer.Start(ctx, "resume-fc-in-place")
	defer span.End()

	if err := p.client.resumeVM(ctx); err != nil {
		return fmt.Errorf("could not resume sandbox: %w", err)
	}

	return nil
}

func (p *Process) Resume(
	ctx context.Context,
	sbxMetadata sbxlogger.SandboxMetadata,
	uffdSocketPath string,
	snapfile template.File,
	uffdReady chan struct{},
	accessToken *string,
	cgroupFD int,
	useMemfd bool,
	useSyncWP bool,
	txRateLimit RateLimiterConfig,
	driveRateLimit RateLimiterConfig,
) error {
	ctx, span := tracer.Start(ctx, "resume-fc")
	defer span.End()

	// Symlink /dev/null to the rootfs link path, so we can start the FC process without the rootfs and then symlink the real rootfs.
	err := utils.SymlinkForce("/dev/null", p.files.SandboxCacheRootfsLinkPath(p.config.StorageConfig))
	if err != nil {
		return fmt.Errorf("error symlinking rootfs: %w", err)
	}

	// create errgroup with context that handled socket wait + rootfs symlink
	eg, egCtx := errgroup.WithContext(ctx)

	eg.Go(func() error {
		err := p.configure(
			egCtx,
			sbxMetadata,
			nil,
			nil,
			cgroupFD,
		)
		if err != nil {
			return fmt.Errorf("error starting fc process: %w", err)
		}

		telemetry.ReportEvent(egCtx, "configured fc")

		return nil
	})

	eg.Go(func() error {
		ctx, uffdSpan := tracer.Start(egCtx, "wait-uffd-socket")
		err := socket.Wait(ctx, uffdSocketPath)
		uffdSpan.End()

		if err != nil {
			return fmt.Errorf("error waiting for uffd socket: %w", err)
		}

		telemetry.ReportEvent(egCtx, "uffd socket ready")

		return nil
	})

	eg.Go(func() error {
		_, rootfsSpan := tracer.Start(egCtx, "wait-rootfs-path")
		rootfsPath, err := p.rootfsProvider.Path()
		rootfsSpan.End()

		if err != nil {
			return fmt.Errorf("error getting rootfs path: %w", err)
		}

		err = utils.SymlinkForce(rootfsPath, p.files.SandboxCacheRootfsLinkPath(p.config.StorageConfig))
		if err != nil {
			return fmt.Errorf("error symlinking rootfs: %w", err)
		}

		telemetry.ReportEvent(egCtx, "symlinked rootfs")

		return nil
	})

	err = eg.Wait()
	if err != nil {
		fcStopErr := p.Stop(ctx)

		return errors.Join(fmt.Errorf("error waiting for uffd socket or symlinking rootfs: %w", err), fcStopErr)
	}

	// Start the metrics reader goroutine before calling setMetrics (same ordering as Create).
	p.startMetricsReader(ctx)

	err = p.client.setMetrics(ctx, p.metricsPath)
	if err != nil {
		fcStopErr := p.Stop(ctx)

		return errors.Join(fmt.Errorf("error setting fc metrics: %w", err), fcStopErr)
	}
	telemetry.ReportEvent(ctx, "set fc metrics")

	err = p.client.loadSnapshot(
		ctx,
		uffdSocketPath,
		uffdReady,
		snapfile,
		useMemfd,
		useSyncWP,
	)
	if err != nil {
		fcStopErr := p.Stop(ctx)

		return errors.Join(fmt.Errorf("error loading snapshot: %w", err), fcStopErr)
	}

	// Always apply/reset rate limits before resuming so any limits
	// persisted in the snapshot are overwritten by the current config.
	if setErr := p.client.setTxRateLimit(ctx, p.slot.VpeerName(), txRateLimit); setErr != nil {
		fcStopErr := p.Stop(ctx)

		return errors.Join(fmt.Errorf("error setting TX rate limit: %w", setErr), fcStopErr)
	}
	telemetry.ReportEvent(ctx, "configured tx rate limit")

	if setErr := p.client.setDriveRateLimit(ctx, rootfsDriveID, driveRateLimit); setErr != nil {
		fcStopErr := p.Stop(ctx)

		return errors.Join(fmt.Errorf("error setting drive rate limit: %w", setErr), fcStopErr)
	}
	telemetry.ReportEvent(ctx, "configured drive rate limit")

	err = p.client.resumeVM(ctx)
	if err != nil {
		fcStopErr := p.Stop(ctx)

		return errors.Join(fmt.Errorf("error resuming vm: %w", err), fcStopErr)
	}

	meta := &MmdsMetadata{
		SandboxID:            sbxMetadata.SandboxID,
		TemplateID:           sbxMetadata.TemplateID,
		LogsCollectorAddress: fmt.Sprintf("http://%s/logs", p.config.NetworkConfig.OrchestratorInSandboxIPAddress),
	}

	if accessToken != nil && *accessToken != "" {
		meta.AccessTokenHash = keys.HashAccessToken(*accessToken)
	} else {
		meta.AccessTokenHash = keys.HashAccessToken("")
	}

	err = p.client.setMmds(ctx, meta)
	if err != nil {
		fcStopErr := p.Stop(ctx)

		return errors.Join(fmt.Errorf("error setting mmds: %w", err), fcStopErr)
	}

	telemetry.SetAttributes(
		ctx,
		attribute.String("sandbox.cmd.dir", p.cmd.Dir),
		attribute.String("sandbox.cmd.path", p.cmd.Path),
	)

	return nil
}

func (p *Process) Pid() (int, error) {
	if p.cmd.Process == nil {
		return 0, errors.New("fc process not started")
	}

	return p.cmd.Process.Pid, nil
}

func (p *Process) Stop(ctx context.Context) error {
	if p.cmd.Process == nil {
		return errors.New("fc process not started")
	}

	// Always remove the metrics FIFO, even if the process already exited,
	// to avoid leaving orphaned files behind.
	if removeErr := os.Remove(p.metricsPath); removeErr != nil && !os.IsNotExist(removeErr) {
		logger.L().Warn(ctx, "failed to remove fc metrics FIFO", zap.Error(removeErr), logger.WithSandboxID(p.files.SandboxID))
	}

	pid := p.cmd.Process.Pid

	// Check if the Firecracker leader has already exited. Descendant cleanup is
	// handled by the sandbox cgroup so Stop never signals a numeric process group.
	select {
	case <-p.Exit.Done():
		logger.L().Info(ctx, "fc process already exited", logger.WithSandboxID(p.files.SandboxID))

		return nil
	default:
	}

	// this function should never fail b/c a previous context was canceled.
	ctx = context.WithoutCancel(ctx)

	// On Linux >= 5.4, Go backs os.Process with pidfd, so Signal is safe against PID reuse.
	err := p.cmd.Process.Signal(syscall.SIGTERM)
	if err != nil {
		if errors.Is(err, os.ErrProcessDone) {
			logger.L().Info(ctx, "fc process already exited", logger.WithSandboxID(p.files.SandboxID))

			return nil
		}

		logger.L().Warn(ctx, "failed to send SIGTERM to fc process", zap.Error(err), logger.WithSandboxID(p.files.SandboxID))
	}

	termDeadline := time.NewTimer(10 * time.Second)
	defer termDeadline.Stop()

	select {
	case <-p.Exit.Done():
		return nil
	case <-termDeadline.C:
		killErr := p.cmd.Process.Kill()
		if killErr == nil {
			logger.L().Info(ctx, "sent SIGKILL to fc process because it was not responding to SIGTERM for 10 seconds",
				logger.WithSandboxID(p.files.SandboxID),
			)
		}
		if errors.Is(killErr, os.ErrProcessDone) {
			logger.L().Info(ctx, "fc process already exited", logger.WithSandboxID(p.files.SandboxID))

			return nil
		}
		if killErr != nil && !errors.Is(killErr, os.ErrProcessDone) {
			logger.L().Warn(ctx, "failed to send SIGKILL to fc process", zap.Error(killErr), logger.WithSandboxID(p.files.SandboxID))
		}

		killDeadline := time.NewTimer(time.Second)
		defer killDeadline.Stop()

		select {
		case <-p.Exit.Done():
			return nil
		case <-killDeadline.C:
			return fmt.Errorf("fc process %d still exists after SIGKILL", pid)
		}
	}
}

func (p *Process) Pause(ctx context.Context) error {
	ctx, childSpan := tracer.Start(ctx, "pause-fc")
	defer childSpan.End()

	return p.client.pauseVM(ctx)
}

// freePageHintDone is FC's FREE_PAGE_HINT_DONE: the host_cmd value FC writes
// back after the guest's FREE_PAGE_HINT_STOP when start used acknowledge_on_stop.
const freePageHintDone int64 = 1

// BalloonFreePageReporting reports whether this VM's balloon device runs
// continuous free-page reporting. Boot-time device truth straight from FC:
// a resumed sandbox's Config does not carry it (the device travels with the
// snapshot). False when no balloon is installed.
func (p *Process) BalloonFreePageReporting(ctx context.Context) (bool, error) {
	return p.client.balloonFreePageReporting(ctx)
}

// PauseFreePageReporting defers all balloon free-page-reporting discards
// until ResumeFreePageReporting; see the client method for semantics.
// Requires an FC build with the /balloon/reporting endpoints.
func (p *Process) PauseFreePageReporting(ctx context.Context) error {
	return p.client.pauseFreePageReporting(ctx)
}

// FreePageReportingPaused reports whether the balloon's free-page reporting
// is currently paused (positive confirmation for the CoW window's
// no-discard precondition).
func (p *Process) FreePageReportingPaused(ctx context.Context) (bool, error) {
	return p.client.freePageReportingPaused(ctx)
}

// ResumeFreePageReporting re-enables free-page-reporting discards and
// processes anything deferred while paused. Idempotent.
func (p *Process) ResumeFreePageReporting(ctx context.Context) error {
	return p.client.resumeFreePageReporting(ctx)
}

// DrainBalloon triggers a free-page-hinting run and blocks until the cycle
// completes or ctx fires. No-op on FC < v1.14 and when no balloon is configured.
func (p *Process) DrainBalloon(ctx context.Context) error {
	ctx, span := tracer.Start(ctx, "drain-balloon")
	outcome := "ok"
	defer func() {
		span.SetAttributes(attribute.String("drain-balloon.outcome", outcome))
		span.End()
	}()

	if !FCSupportsFreePageHinting(p.Versions.FirecrackerVersion) {
		outcome = "fc-unsupported"

		return nil
	}

	if err := p.client.startBalloonHinting(ctx, true); err != nil {
		if _, ok := errors.AsType[*operations.StartBalloonHintingBadRequest](err); ok {
			outcome = "not-configured"

			return nil
		}

		outcome = "start-failed"

		return fmt.Errorf("start balloon hinting: %w", err)
	}

	if err := pollFphDone(ctx, p.client.describeBalloonHinting); err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			outcome = "timeout"
		} else {
			outcome = "describe-failed"
		}

		return err
	}

	return nil
}

func pollFphDone(ctx context.Context, describe func(ctx context.Context) (int64, error)) error {
	backoff := 5 * time.Millisecond
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}

		host, err := describe(ctx)
		if err != nil {
			return fmt.Errorf("balloon hinting status: %w", err)
		}
		if host == freePageHintDone {
			return nil
		}
		backoff = min(backoff*2, 50*time.Millisecond)
	}
}

// CreateSnapshot VM needs to be paused before creating a snapshot.
func (p *Process) CreateSnapshot(ctx context.Context, snapfilePath string) error {
	ctx, childSpan := tracer.Start(ctx, "create-snapshot-fc")
	defer childSpan.End()

	return p.client.createSnapshot(ctx, snapfilePath)
}
