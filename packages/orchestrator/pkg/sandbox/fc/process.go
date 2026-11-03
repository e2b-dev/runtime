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
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"go.uber.org/zap/zapio"
	"golang.org/x/sync/errgroup"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/cfg"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/cgroup"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/fc/cputemplate"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/network"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/rootfs"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/socket"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/template"
	"github.com/e2b-dev/infra/packages/shared/pkg/fc/client/operations"
	"github.com/e2b-dev/infra/packages/shared/pkg/fc/models"
	"github.com/e2b-dev/infra/packages/shared/pkg/keys"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	sbxlogger "github.com/e2b-dev/infra/packages/shared/pkg/logger/sandbox"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

var tracer = otel.Tracer("github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/fc")

var errFirecrackerExitedBeforeSocketReadiness = errors.New("fc process exited before API socket became ready")

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

	// BootCpus caps the CPUs the guest kernel brings up (maxcpus=); 0 is all. Create sets it.
	BootCpus int64

	// ACPIPowerOff declares the ACPI power-off device, so a guest power-off
	// ends the process instead of halting the vCPUs. A Firecracker without
	// the device refuses it, and the boot goes on without.
	ACPIPowerOff bool

	// CPUTemplate is the custom Firecracker CPU template sent before boot. Nil is none. Only
	// a cold boot needs it; a memory resume restores the vCPU state the template produced.
	CPUTemplate *cputemplate.Template

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
	// teamID labels the per-sandbox Firecracker metrics.
	teamID string
	// vmVcpus is the VM's vCPU count as Resume resolved it; 0 for a booted VM or before Resume.
	vmVcpus int64

	Exit *utils.ErrorOnce

	// exitInfo is how the leader was reaped. Published before Exit resolves,
	// so anything woken by Exit reads a populated value.
	exitInfo atomic.Pointer[ExitInfo]

	// reaped closes once the leader is reaped, before Exit resolves.
	reaped chan struct{}

	// sentSignals has bit N set before Stop sends signal N. The reap matches
	// the terminating signal against it: a zombie still accepts signals and
	// the reap races Stop, so no single flag can be ordered against the death.
	sentSignals atomic.Uint64

	client *apiClient

	// mmdsStoreMissing is set when Resume found the restored microVM without an
	// MMDS store; a pause records it, since its envd then keeps its token.
	mmdsStoreMissing atomic.Bool

	// acpiPowerOff is set once Firecracker accepted the ACPI power-off
	// device, on the cold boot's machine config or the restore's PATCH.
	acpiPowerOff atomic.Bool

	// hintCmd is the command id of the last hinting cycle this process
	// started, and hintGuestSeen the last guest_cmd a cycle observed; the
	// stop's acknowledgement wait keys on them.
	hintCmd       atomic.Int64
	hintGuestSeen atomic.Int64
	hintSeenInit  atomic.Bool
	balloonCaps   atomic.Pointer[BalloonCaps]

	// balloonAccum is the cumulative virtio-balloon snapshot summed by the
	// metrics-reader goroutine (FC's SharedIncMetric resets per flush);
	// metricsLineMs is the utc_timestamp_ms of the last line it ingested.
	balloonAccum  atomic.Pointer[BalloonMetricsSnapshot]
	metricsLineMs atomic.Int64

	// guestResets sums i8042.reset_count, guestPowerOffs
	// acpi_pm.poweroff_count and guestCrashes the count field of the guest
	// profile's crash report group over the process's life; guestCrashCode
	// keeps the last value of that group's code field.
	// metricsStarted is set once the metrics reader is started, and
	// metricsDone closes when it has stopped reading.
	guestResets    atomic.Uint64
	guestPowerOffs atomic.Uint64
	guestCrashes   atomic.Uint64
	guestCrashCode atomic.Uint64
	metricsStarted atomic.Bool
	// drainOnExit makes the exit wait for the metrics reader, so the guest
	// counters above are complete when the exit is classified; drainWanted,
	// asked at the exit, can ask for the wait too.
	drainOnExit atomic.Bool
	drainWanted atomic.Pointer[func() bool]
	metricsDone chan struct{}
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
	teamID string,
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
		reaped:                make(chan struct{}),
		metricsDone:           make(chan struct{}),
		cmd:                   cmd,
		firecrackerSocketPath: files.SandboxFirecrackerSocketPath(),
		metricsPath:           files.SandboxMetricsFifoPath(),
		config:                config,
		client:                newApiClient(files.SandboxFirecrackerSocketPath()),
		rootfsProvider:        rootfsProvider,
		files:                 files,
		teamID:                teamID,
		slot:                  slot,

		kernelPath: startScript.KernelPath,
		rootfsPath: startScript.RootfsPath,
	}

	cmd.SysProcAttr = &syscall.SysProcAttr{
		Setsid: true, // Create a new session
	}

	return p, nil
}

// refuseReapedProcess refuses readiness for a process the wait owner already
// reaped: its API socket can accept a connection just before it exits.
func refuseReapedProcess(exit *utils.ErrorOnce) error {
	select {
	case <-exit.Done():
		if err := exit.Error(); err != nil {
			return err
		}

		return errFirecrackerExitedBeforeSocketReadiness
	default:
		return nil
	}
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

		exitErr := p.handleExit(ctx, p.cmd.Wait())
		// Exit intentionally resolves cleanly for exit 0 and SIGTERM/SIGKILL,
		// but every reaped process still makes API-socket startup impossible.
		if exitErr == nil {
			exitErr = errFirecrackerExitedBeforeSocketReadiness
		}
		cancelStart(exitErr)
	}()

	// Wait for the FC process to start so we can use FC API
	err = socket.WaitAccepting(startCtx, p.firecrackerSocketPath)
	if err == nil {
		err = refuseReapedProcess(p.Exit)
	}
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
	memoryMB int64,
	hugePages bool,
	freePageReporting bool,
	freePageHinting bool,
	options ProcessOptions,
	txRateLimit RateLimiterConfig,
	driveRateLimit RateLimiterConfig,
	cgroupHandle *cgroup.CgroupHandle,
	targetOnlineVcpus int64,
	configuredMaxVcpus int64,
) error {
	ctx, childSpan := tracer.Start(ctx, "create-fc")
	defer childSpan.End()

	// Symlink /dev/null to the rootfs link path, so we can start the FC process without the rootfs and then symlink the real rootfs.
	err := utils.SymlinkForce("/dev/null", p.files.SandboxCacheRootfsLinkPath(p.config.StorageConfig))
	if err != nil {
		return fmt.Errorf("error symlinking rootfs: %w", err)
	}

	// Firecracker creates the full VM capacity. Limit the whole cgroup until its vCPU threads exist.
	firecrackerVcpuCount := max(targetOnlineVcpus, configuredMaxVcpus)
	cpuQuota := bootCpuQuota(targetOnlineVcpus, firecrackerVcpuCount)
	// Keep this before configure starts Firecracker and fatal: no CI test pins it (the cgroup tests need root).
	if cpuQuota > 0 {
		if err := cgroupHandle.SetSandboxCpuLimit(ctx, cpuQuota); err != nil {
			recordVcpuLimit(ctx, "boot", "failed")

			return fmt.Errorf("error limiting sandbox cgroup before boot: %w", err)
		}
		options.BootCpus = targetOnlineVcpus
	}

	err = p.configure(
		ctx,
		sbxMetadata,
		options.Stdout,
		options.Stderr,
		cgroupHandle.GetFD(),
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

	err = p.configureGuestDevices(ctx, firecrackerVcpuCount, memoryMB, hugePages, freePageReporting, freePageHinting, options, txRateLimit, driveRateLimit)
	if err != nil {
		return err
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
	p.moveLimitToVcpuThreads(ctx, cgroupHandle, cpuQuota, firecrackerVcpuCount)

	return nil
}

// configureGuestDevices sends Firecracker the boot source, the disk, the
// network interface with its MMDS binding, the machine, the CPU template, the
// entropy device and the balloon: the part of Create the guest profile decides.
// Every error already carries the Stop of the process.
func (p *Process) configureGuestDevices(
	ctx context.Context,
	firecrackerVcpuCount int64,
	memoryMB int64,
	hugePages bool,
	freePageReporting bool,
	freePageHinting bool,
	options ProcessOptions,
	txRateLimit RateLimiterConfig,
	driveRateLimit RateLimiterConfig,
) error {
	boot := p.Versions.GuestProfile().Boot()
	// Built before the first request, so a misconfigured profile never half-configures the VM.
	machineConfig, err := newMachineConfigBody(firecrackerVcpuCount, memoryMB, hugePages, boot.MachineConfigExtra, options.ACPIPowerOff)
	if err != nil {
		fcStopErr := p.Stop(ctx)

		return errors.Join(fmt.Errorf("invalid guest profile machine config: %w", err), fcStopErr)
	}

	// IPv4 configuration - format: [local_ip]::[gateway_ip]:[netmask]:hostname:iface:dhcp_option:[dns]
	ipv4 := fmt.Sprintf("%s::%s:%s:instance:%s:off:%s", p.slot.NamespaceIP(), p.slot.TapIPString(), p.slot.TapMaskString(), p.slot.VpeerName(), p.slot.TapName())
	kernelArgs := buildKernelArgs(ipv4, options).String()
	if boot.NoKernelArgs {
		kernelArgs = ""
	}
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

	err = p.client.setRootfsDrive(ctx, p.rootfsPath, options.IoEngine, buildRateLimiter(driveRateLimit), !boot.NoRootDevice, boot.BlockSectorSize)
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

	err = p.client.setMachineConfig(ctx, machineConfig)
	if err != nil && options.ACPIPowerOff {
		logger.L().Warn(ctx, "firecracker refused the ACPI power-off device, booting without it",
			zap.String("firecracker_version", p.Versions.FirecrackerVersion), zap.Error(err))
		var withoutPowerOff any
		withoutPowerOff, err = newMachineConfigBody(firecrackerVcpuCount, memoryMB, hugePages, boot.MachineConfigExtra, false)
		if err == nil {
			err = p.client.setMachineConfig(ctx, withoutPowerOff)
		}
	} else if err == nil && options.ACPIPowerOff {
		p.acpiPowerOff.Store(true)
	}
	if err != nil {
		fcStopErr := p.Stop(ctx)

		return errors.Join(fmt.Errorf("error setting fc machine config: %w", err), fcStopErr)
	}
	telemetry.ReportEvent(ctx, "set fc machine config", attribute.Int("machine.config_extra_fields", len(boot.MachineConfigExtra)))

	// A template this host rejects is corrupt metadata or a placement bug.
	if options.CPUTemplate != nil && !options.CPUTemplate.IsEmpty() && !boot.NoCPUTemplate {
		err = p.client.setCPUConfig(ctx, *options.CPUTemplate)
		if err != nil {
			fcStopErr := p.Stop(ctx)

			return errors.Join(fmt.Errorf("error setting fc cpu config: %w", err), fcStopErr)
		}
		telemetry.ReportEvent(ctx, "set fc cpu config",
			attribute.String("fc.cpu_template", options.CPUTemplate.Digest()),
		)
	}

	if !boot.NoEntropy {
		err = p.client.setEntropyDevice(ctx)
		if err != nil {
			fcStopErr := p.Stop(ctx)

			return errors.Join(fmt.Errorf("error setting fc entropy config: %w", err), fcStopErr)
		}
		telemetry.ReportEvent(ctx, "set fc entropy config")
	}

	if (freePageReporting || freePageHinting) && !boot.NoBalloon {
		if err := p.client.installBalloon(ctx, freePageReporting, freePageHinting); err != nil {
			fcStopErr := p.Stop(ctx)

			return errors.Join(fmt.Errorf("error installing balloon device: %w", err), fcStopErr)
		}
		telemetry.ReportEvent(ctx, "installed balloon device",
			attribute.Bool("balloon.free_page_reporting", freePageReporting),
			attribute.Bool("balloon.free_page_hinting", freePageHinting),
		)
	}

	return nil
}

// bootCpuQuota is the host CPU quota needed before a larger VM boots; 0 means no quota.
func bootCpuQuota(targetOnlineVcpus, firecrackerVcpuCount int64) int64 {
	if firecrackerVcpuCount > targetOnlineVcpus {
		return targetOnlineVcpus
	}

	return 0
}

// recordVcpuLimit counts how a start whose VM has spare vCPUs got its CPU limit (SandboxVcpuLimit).
func recordVcpuLimit(ctx context.Context, path, outcome string) {
	vcpuLimitStarts.Add(ctx, 1, metric.WithAttributes(attribute.String("path", path), attribute.String("outcome", outcome)))
}

// ResumeVcpus sizes a resume: Target CPUs online in a VM of Snapshot vCPUs, Recorded in the snapshot or a
// guess the loaded threads replace. RefuseSmallerVM fails a resume whose VM is smaller than Target; it is
// set when a VM size was given, so a resume without one runs as before.
type ResumeVcpus struct {
	Target          int64
	Snapshot        int64
	Recorded        bool
	RefuseSmallerVM bool
}

// vcpuCapResumeErr is whether the cap step's error fails the resume: a VM smaller than the target has
// nothing to cap and fails it only when refuseSmallerVM; any other error always does.
func vcpuCapResumeErr(err error, refuseSmallerVM bool) error {
	if errors.Is(err, ErrVcpuExceedsSnapshot) && !refuseSmallerVM {
		return nil
	}

	return err
}

// ErrVcpuExceedsSnapshot is a memory resume asking for more vCPUs than the snapshot's VM has; a resume cannot add any.
var ErrVcpuExceedsSnapshot = errors.New("requested vcpu exceeds snapshot capacity")

// resolveVmVcpus is the loaded VM's vCPU count: the recorded one, or for a snapshot from before it was
// recorded, the fc_vcpu threads Firecracker restored. If they cannot be counted, the caller's guess stands.
func (p *Process) resolveVmVcpus(ctx context.Context, guess int64, recorded bool) int64 {
	p.vmVcpus = guess
	if recorded {
		return guess
	}
	n, err := vcpuThreadCount(fmt.Sprintf("/proc/%d", p.cmd.Process.Pid))
	if err != nil {
		telemetry.ReportError(ctx, "failed to count the snapshot's vcpu threads, using the requested size", err,
			telemetry.WithSandboxID(p.files.SandboxID))

		return guess
	}
	p.vmVcpus = n

	return n
}

// VmVcpus is how many vCPUs the running VM has, once Resume resolved it; 0 before.
func (p *Process) VmVcpus() int64 {
	return p.vmVcpus
}

// vcpuThreadCount counts the fc_vcpu threads of the process at procDir. Walking that one process's
// task list takes no host-wide lock, unlike reading the sandbox cgroup's cgroup.threads (css_set_lock).
func vcpuThreadCount(procDir string) (int64, error) {
	tasks, err := os.ReadDir(filepath.Join(procDir, "task"))
	if err != nil {
		return 0, fmt.Errorf("failed to list threads: %w", err)
	}
	var n int64
	for _, task := range tasks {
		comm, err := os.ReadFile(filepath.Join(procDir, "task", task.Name(), "comm"))
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return 0, fmt.Errorf("failed to read thread name: %w", err)
		}
		if strings.HasPrefix(string(comm), cgroup.VcpuThreadPrefix) {
			n++
		}
	}
	if n == 0 {
		return 0, errors.New("no firecracker vcpu threads")
	}

	return n, nil
}

// limitVcpusBeforeResume confines paused vCPU threads when the snapshot's VM is larger than the online target.
// If that fails, limit the whole cgroup; failing both aborts the resume.
func (p *Process) limitVcpusBeforeResume(ctx context.Context, cgroupHandle *cgroup.CgroupHandle, targetOnlineVcpus, snapshotVcpuCount int64) error {
	if snapshotVcpuCount < targetOnlineVcpus {
		return fmt.Errorf("%w: snapshot has %d vcpus, request asks for %d", ErrVcpuExceedsSnapshot, snapshotVcpuCount, targetOnlineVcpus)
	}
	if snapshotVcpuCount == targetOnlineVcpus {
		return nil
	}

	// Its own span: cgroup writes can stall on one node (RCU grace period, a fork stuck in reclaim).
	return telemetry.Observe0(ctx, tracer, "limit-vcpus-before-resume", func(ctx context.Context) error {
		outcome := "failed"
		defer func() { recordVcpuLimit(ctx, "resume", outcome) }()

		err := cgroupHandle.ConfineVcpuThreads(ctx, targetOnlineVcpus, snapshotVcpuCount)
		if err == nil {
			outcome = "confined"

			return nil
		}
		telemetry.ReportError(ctx, "failed to confine vcpu threads, limiting the whole sandbox cgroup", err,
			telemetry.WithSandboxID(p.files.SandboxID))
		if err := cgroupHandle.SetSandboxCpuLimit(ctx, targetOnlineVcpus); err != nil {
			return fmt.Errorf("error limiting sandbox cgroup before resume: %w", err)
		}
		outcome = "cgroup_fallback"

		return nil
	})
}

// moveLimitToVcpuThreads confines the VM's vCPU threads, then lifts the cgroup-wide limit; if confining fails it stays.
func (p *Process) moveLimitToVcpuThreads(ctx context.Context, h *cgroup.CgroupHandle, cpuQuota, firecrackerVcpuCount int64) {
	if cpuQuota <= 0 {
		return
	}
	// The error only marks the span: any failure leaves the cgroup-wide limit, so the VMM threads share the quota.
	_ = telemetry.Observe0(ctx, tracer, "move-limit-to-vcpu-threads", func(ctx context.Context) error {
		outcome := "cgroup_fallback"
		defer func() { recordVcpuLimit(ctx, "boot", outcome) }()

		if err := h.ConfineVcpuThreads(ctx, cpuQuota, firecrackerVcpuCount); err != nil {
			telemetry.ReportError(ctx, "failed to confine vcpu threads, vmm threads share the cpu limit", err,
				telemetry.WithSandboxID(p.files.SandboxID))

			return err
		}
		if err := h.SetSandboxCpuLimit(ctx, 0); err != nil {
			telemetry.ReportError(ctx, "failed to lift the cgroup-wide cpu limit, vmm threads stay limited", err,
				telemetry.WithSandboxID(p.files.SandboxID))

			return err
		}
		outcome = "confined"

		return nil
	})
}

// ResumeInPlace runs the vCPUs of the live Firecracker process after an
// in-place snapshot (Pause + CreateSnapshot) or RestorePaused. It loads
// nothing, so a retry after an uncertain result acts on the same VM, and a
// running VM answers it as resumed.
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

// GuestOptions is what the guest decides about a restore. The zero value is
// the restore every snapshot of the default guest takes.
type GuestOptions struct {
	// ACPIPowerOff declares the ACPI power-off device before the load; the
	// process counts the device as declared once Firecracker accepts it.
	ACPIPowerOff bool
}

// Resume restores the snapshot with RestorePaused, runs its vCPUs and then
// sets the MMDS metadata. A failure stops the process.
func (p *Process) Resume(
	ctx context.Context,
	sbxMetadata sbxlogger.SandboxMetadata,
	uffdSocketPath string,
	snapfile template.File,
	uffdReady chan struct{},
	accessToken *string,
	cgroupHandle *cgroup.CgroupHandle,
	useMemfd bool,
	useSyncWP bool,
	guest GuestOptions,
	cpuTemplate string,
	txRateLimit RateLimiterConfig,
	driveRateLimit RateLimiterConfig,
	vcpus ResumeVcpus,
) error {
	ctx, span := tracer.Start(ctx, "resume-fc")
	defer span.End()

	err := p.RestorePaused(ctx, sbxMetadata, uffdSocketPath, snapfile, uffdReady, cgroupHandle, useMemfd, useSyncWP, guest, cpuTemplate, txRateLimit, driveRateLimit, vcpus)
	if err != nil {
		return err
	}

	err = p.client.resumeVM(ctx)
	if err != nil {
		fcStopErr := p.Stop(ctx)

		return errors.Join(fmt.Errorf("error resuming vm: %w", err), fcStopErr)
	}

	err = p.SetMmds(ctx, sbxMetadata, accessToken)
	if err != nil {
		fcStopErr := p.Stop(ctx)

		return errors.Join(err, fcStopErr)
	}

	telemetry.SetAttributes(
		ctx,
		attribute.String("sandbox.cmd.dir", p.cmd.Dir),
		attribute.String("sandbox.cmd.path", p.cmd.Path),
	)

	return nil
}

// RestorePaused starts the process and loads the snapshot into a paused VM,
// configured except for the MMDS metadata, so the guest has executed nothing
// when it returns. The sandbox package's Factory.ResumeVM runs it, sets the
// metadata and waits for envd. A failure stops the process.
func (p *Process) RestorePaused(
	ctx context.Context,
	sbxMetadata sbxlogger.SandboxMetadata,
	uffdSocketPath string,
	snapfile template.File,
	uffdReady chan struct{},
	cgroupHandle *cgroup.CgroupHandle,
	useMemfd bool,
	useSyncWP bool,
	guest GuestOptions,
	cpuTemplate string,
	txRateLimit RateLimiterConfig,
	driveRateLimit RateLimiterConfig,
	vcpus ResumeVcpus,
) error {
	ifaceID, netOverride := p.resumeNetworkOverride()

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
			cgroupHandle.GetFD(),
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

	p.declareRestorePowerOff(ctx, guest)

	err = p.client.loadSnapshot(
		ctx,
		uffdSocketPath,
		uffdReady,
		snapfile,
		useMemfd,
		useSyncWP,
		p.Versions.GuestProfile().Restore().ClockRealtime,
		netOverride,
		cpuTemplate,
		p.teamID,
	)
	if err != nil {
		fcStopErr := p.Stop(ctx)

		return errors.Join(fmt.Errorf("error loading snapshot: %w", err), fcStopErr)
	}

	// Always apply/reset rate limits before resuming so any limits
	// persisted in the snapshot are overwritten by the current config.
	if setErr := p.client.setTxRateLimit(ctx, ifaceID, txRateLimit); setErr != nil {
		fcStopErr := p.Stop(ctx)

		return errors.Join(fmt.Errorf("error setting TX rate limit: %w", setErr), fcStopErr)
	}
	telemetry.ReportEvent(ctx, "configured tx rate limit")

	if setErr := p.client.setDriveRateLimit(ctx, p.Versions.RootfsDriveIDOrDefault(), driveRateLimit); setErr != nil {
		fcStopErr := p.Stop(ctx)

		return errors.Join(fmt.Errorf("error setting drive rate limit: %w", setErr), fcStopErr)
	}
	telemetry.ReportEvent(ctx, "configured drive rate limit")

	// The snapshot is loaded and the vCPU threads exist, paused: limit them before the guest runs.
	// Keep this before any resume and fatal: no CI test pins it (the cgroup tests need root), so rerun them on a VM.
	err = p.limitVcpusBeforeResume(ctx, cgroupHandle, vcpus.Target, p.resolveVmVcpus(ctx, vcpus.Snapshot, vcpus.Recorded))
	if err := vcpuCapResumeErr(err, vcpus.RefuseSmallerVM); err != nil {
		fcStopErr := p.Stop(ctx)

		return errors.Join(err, fcStopErr)
	}

	return nil
}

// SetMmds writes the sandbox's MMDS metadata into the running process. A
// failure leaves the process to the caller.
func (p *Process) SetMmds(ctx context.Context, sbxMetadata sbxlogger.SandboxMetadata, accessToken *string) error {
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

	if err := p.setRestoredMmds(ctx, meta); err != nil {
		return fmt.Errorf("error setting mmds: %w", err)
	}

	return nil
}

// setRestoredMmds writes the metadata a restored guest reads. An image made
// outside this runtime can carry no MMDS store, nor does any snapshot paused
// from it, and Firecracker cannot add one after a restore: for a profile whose
// restore declares the store optional, that refusal is tolerated and
// remembered; any other refusal, and any under another profile, fails.
func (p *Process) setRestoredMmds(ctx context.Context, meta *MmdsMetadata) error {
	err := p.client.setMmds(ctx, meta)
	if err == nil || !p.Versions.GuestProfile().Restore().MmdsStoreOptional || !mmdsStoreMissing(err) {
		return err
	}

	p.mmdsStoreMissing.Store(true)
	logger.L().Warn(ctx, "the snapshot carries no MMDS store, skipping the metadata put",
		logger.WithSandboxID(meta.SandboxID),
		zap.Error(err))
	telemetry.ReportEvent(ctx, "mmds store missing in the snapshot")

	return nil
}

// DrainMetricsOnExit makes the exit wait, up to metricsDrainTimeout, for the
// metrics reader to reach Firecracker's last line, so a guest reset or
// power-off is counted before the exit is classified. Without it the exit is
// classified on the counters read so far.
func (p *Process) DrainMetricsOnExit() {
	p.drainOnExit.Store(true)
}

// SetExitDrain asks want at the exit whether to wait for the metrics reader,
// so a setting turned on while the process runs still counts its last line.
// A process that took the ACPI power-off device always waits.
func (p *Process) SetExitDrain(want func() bool) {
	p.drainWanted.Store(&want)
}

// drainsOnExit is whether the exit waits for the metrics reader.
func (p *Process) drainsOnExit() bool {
	if p.drainOnExit.Load() || p.ACPIPowerOffDeclared() {
		return true
	}
	want := p.drainWanted.Load()

	return want != nil && (*want)()
}

// declareRestorePowerOff sends a restore's ACPI power-off declaration when the
// guest asks for it. A Firecracker that refuses it restores without the device.
func (p *Process) declareRestorePowerOff(ctx context.Context, guest GuestOptions) {
	if !guest.ACPIPowerOff {
		return
	}
	if err := p.client.setACPIPowerOff(ctx); err != nil {
		logger.L().Warn(ctx, "firecracker refused the ACPI power-off device, restoring without it",
			zap.String("firecracker_version", p.Versions.FirecrackerVersion), zap.Error(err))

		return
	}
	p.acpiPowerOff.Store(true)
}

// ACPIPowerOffDeclared reports whether Firecracker accepted the ACPI
// power-off device for this process, so a guest power-off ends it.
func (p *Process) ACPIPowerOffDeclared() bool {
	return p != nil && p.acpiPowerOff.Load()
}

// DeclarePowerOffForTest records the device as accepted, as
// configureGuestDevices does once Firecracker takes it.
func (p *Process) DeclarePowerOffForTest() {
	p.acpiPowerOff.Store(true)
}

// MmdsStoreMissing reports whether this microVM was restored without an MMDS
// store, so its envd accepts only the token it already holds.
func (p *Process) MmdsStoreMissing() bool {
	return p != nil && p.mmdsStoreMissing.Load()
}

func (p *Process) Pid() (int, error) {
	if p.cmd.Process == nil {
		return 0, errors.New("fc process not started")
	}

	return p.cmd.Process.Pid, nil
}

// handleExit records how the reaped process ended and resolves Exit with the
// error it returns. waitErr is what cmd.Wait returned.
func (p *Process) handleExit(ctx context.Context, waitErr error) error {
	close(p.reaped)
	drain := p.drainsOnExit()
	metricsRead := drain && p.waitMetricsDrained(ctx)

	// Record the status before resolving Exit: the branches below flatten a
	// signalled kill and a clean exit into the same nil error.
	if state := p.cmd.ProcessState; state != nil {
		if status, ok := state.Sys().(syscall.WaitStatus); ok {
			info := exitInfoFromStatus(status, p.sentSignals.Load())
			info.MetricsRead = metricsRead
			info.MetricsAwaited = drain
			info.GuestResets = p.guestResets.Load()
			info.GuestPowerOffs = p.guestPowerOffs.Load()
			info.GuestCrashes = p.guestCrashes.Load()
			info.GuestCrashCode = p.guestCrashCode.Load()
			p.exitInfo.Store(info)
		}
	}

	if waitErr == nil {
		p.Exit.SetError(nil)

		return nil
	}

	if exitErr, ok := errors.AsType[*exec.ExitError](waitErr); ok {
		// A signalled teardown is how a sandbox normally ends, so Exit
		// resolves clean whether or not the signal was ours; ExitInfo carries
		// the difference.
		if status, ok := exitErr.Sys().(syscall.WaitStatus); ok && status.Signaled() && (status.Signal() == syscall.SIGKILL || status.Signal() == syscall.SIGTERM) {
			p.Exit.SetError(nil)

			return nil
		}
	}

	logger.L().Error(ctx, "error waiting for fc process", zap.Error(waitErr))

	err := fmt.Errorf("error waiting for fc process: %w", waitErr)
	p.Exit.SetError(err)

	return err
}

// waitMetricsDrained waits for the metrics reader to read Firecracker's last
// line, metricsDrainTimeout at most, and reports whether it did. A process
// whose reader never started has no counters to wait for.
func (p *Process) waitMetricsDrained(ctx context.Context) bool {
	if !p.metricsStarted.Load() {
		return false
	}

	select {
	case <-p.metricsDone:
		return true
	case <-time.After(metricsDrainTimeout):
		logger.L().Warn(ctx, "fc metrics reader did not reach the end of the FIFO after the exit",
			zap.Duration("timeout", metricsDrainTimeout),
			logger.WithSandboxID(p.files.SandboxID),
		)

		return false
	}
}

// ExitInfo reports how the Firecracker leader was reaped, and nil before that.
// Nil-safe: crash reporting runs for sandboxes that never got a process.
func (p *Process) ExitInfo() *ExitInfo {
	if p == nil {
		return nil
	}

	return p.exitInfo.Load()
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
	err := p.signal(syscall.SIGTERM)
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
		killErr := p.signal(syscall.SIGKILL)
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

// signal records sig as sent before sending it, so a reap that our signal
// causes always sees the record.
func (p *Process) signal(sig syscall.Signal) error {
	p.sentSignals.Or(signalBit(sig))

	return p.cmd.Process.Signal(sig)
}

func (p *Process) Pause(ctx context.Context) error {
	ctx, childSpan := tracer.Start(ctx, "pause-fc")
	defer childSpan.End()

	return p.client.pauseVM(ctx)
}

const (
	// freePageHintStop is FC's FREE_PAGE_HINT_STOP: what the guest writes back
	// when it has finished or been stopped, and what an idle guest reads as.
	freePageHintStop int64 = 0
	// freePageHintDone is FC's FREE_PAGE_HINT_DONE: the host_cmd value FC writes
	// back after the guest's FREE_PAGE_HINT_STOP when start used acknowledge_on_stop.
	freePageHintDone int64 = 1
	// freePageHintFirstCmd is the lowest id FC assigns to a cycle: 0 and 1 are
	// reserved for STOP and DONE, so any host_cmd or guest_cmd at or above it
	// names a real cycle.
	freePageHintFirstCmd int64 = 2
	// hintConfigReadTimeout bounds the config read that classifies a start
	// refusal. It must not inherit a drain deadline the start already spent.
	hintConfigReadTimeout = 500 * time.Millisecond
)

// BalloonCaps is what the balloon device was installed with. Both false when
// no balloon is installed.
type BalloonCaps struct {
	Reporting bool
	Hinting   bool
}

// BalloonCaps reads the balloon config once for both free-page mechanisms.
// Device truth is fixed at boot, so a successful read is kept for the process.
func (p *Process) BalloonCaps(ctx context.Context) (BalloonCaps, error) {
	if caps := p.balloonCaps.Load(); caps != nil {
		return *caps, nil
	}
	cfg, err := p.client.describeBalloonConfig(ctx)
	if err != nil {
		return BalloonCaps{}, err
	}
	caps := BalloonCaps{}
	if cfg != nil {
		caps = BalloonCaps{Reporting: cfg.FreePageReporting, Hinting: cfg.FreePageHinting}
	}
	p.balloonCaps.Store(&caps)

	return caps, nil
}

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

// ErrHintingNotConfigured means the VM has no balloon, or its balloon was installed
// without free-page hinting. Callers that only want a best-effort drain treat
// it as a no-op; the periodic hinter exits on it.
var ErrHintingNotConfigured = errors.New("balloon free-page hinting not configured")

// ErrHintingInFlight wraps a DrainBalloon failure after which the guest may
// still be hinting: the cycle started and was not confirmed done, or the start
// round-trip itself ended in a context error and FC may have applied it. The
// caller must StopBalloonHinting before anything else starts a cycle or
// snapshots.
var ErrHintingInFlight = errors.New("balloon hinting cycle possibly in flight")

// ErrHintingStartRefused wraps a 400 from the start on a balloon that does have
// hinting (the guest has not activated the device yet), or whose config could
// not be read in time. The cycle did not start.
var ErrHintingStartRefused = errors.New("balloon hinting start refused")

// ErrHintingConfigUnknown additionally qualifies a refusal whose config read
// failed: the refusal says nothing about the guest until the config is read.
var ErrHintingConfigUnknown = errors.New("balloon config unreadable")

// ErrHintingGuestSilent qualifies an ErrHintingInFlight timeout: the guest
// never echoed the cycle's command, so it is not hinting slowly, it is not
// hinting at all (no driver, feature not negotiated, balloon never activated).
var ErrHintingGuestSilent = errors.New("guest did not engage in the hinting cycle")

// ErrHintingStopUnacked means the stop reached FC but a guest that had engaged in
// the cycle had not written its FREE_PAGE_HINT_STOP back before ctx ended. No
// further discard can land, but that late stop may acknowledge the next cycle
// as done before it ran.
var ErrHintingStopUnacked = errors.New("balloon hinting stop not acknowledged by guest")

// DrainBalloon triggers a free-page-hinting run and blocks until the cycle
// completes or ctx fires. ErrHintingNotConfigured when the balloon cannot hint,
// a Firecracker without the hinting API included.
func (p *Process) DrainBalloon(ctx context.Context) error {
	ctx, span := tracer.Start(ctx, "drain-balloon")
	outcome := "ok"
	defer func() {
		span.SetAttributes(attribute.String("drain-balloon.outcome", outcome))
		span.End()
	}()

	if !FCSupportsFreePageHinting(p.Versions.FirecrackerVersion) {
		outcome = "fc-unsupported"

		return fmt.Errorf("%w: firecracker %s", ErrHintingNotConfigured, p.Versions.FirecrackerVersion)
	}

	// A balloon without hinting answers nothing useful to any of the calls
	// below; the cached config settles it without a request.
	if caps, err := p.BalloonCaps(ctx); err == nil && !caps.Hinting {
		outcome = "not-configured"

		return ErrHintingNotConfigured
	}

	// Unknown until a status read shows it: a start that times out may have
	// landed with an id this process has not seen.
	p.hintCmd.Store(0)
	if !p.hintSeenInit.Load() {
		// guest_cmd travels with the snapshot: what the guest last wrote in
		// some earlier life must not read as engagement in this one. Retried
		// on the next run if the read fails.
		if st, err := p.client.describeBalloonHinting(ctx); err == nil {
			p.hintGuestSeen.Store(st.guestCmd)
			p.hintSeenInit.Store(true)
		}
	}
	if err := p.client.startBalloonHinting(ctx, true); err != nil {
		if _, ok := errors.AsType[*operations.StartBalloonHintingBadRequest](err); ok {
			// FC answers 400 both for "no hinting on this balloon" and for a
			// balloon the guest has not activated; only the device config says
			// which it was.
			cfgCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), hintConfigReadTimeout)
			caps, cfgErr := p.BalloonCaps(cfgCtx)
			cancel()
			configured := caps.Hinting
			switch {
			case cfgErr != nil:
				// The one certain fact is that no cycle started.
				outcome = "start-refused-config-unknown"

				return fmt.Errorf("%w: %w: %w", ErrHintingStartRefused, ErrHintingConfigUnknown, errors.Join(err, cfgErr))
			case !configured:
				outcome = "not-configured"

				return ErrHintingNotConfigured
			}
			outcome = "start-refused"

			return fmt.Errorf("%w: %w", ErrHintingStartRefused, err)
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			// The request may have reached FC before the context ended.
			outcome = "start-uncertain"

			return fmt.Errorf("%w: start balloon hinting: %w", ErrHintingInFlight, err)
		}

		outcome = "start-failed"

		return fmt.Errorf("start balloon hinting: %w", err)
	}

	// A status read while the cycle runs carries its command id; the guest
	// echoes it back as guest_cmd once it starts hinting, which is how a slow
	// guest is told apart from one that will never answer.
	var cmd int64
	engaged := false
	err := p.pollHinting(ctx, func(st hintingStatus) bool {
		p.hintGuestSeen.Store(st.guestCmd)
		if cmd == 0 && st.hostCmd >= freePageHintFirstCmd {
			cmd = st.hostCmd
			p.hintCmd.Store(cmd)
		}
		engaged = engaged || (cmd >= freePageHintFirstCmd && st.guestCmd == cmd)

		return st.hostCmd == freePageHintDone
	})
	if err != nil {
		switch {
		case !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded):
			outcome = "describe-failed"
		case engaged || cmd == 0:
			// A budget spent on the start itself says nothing about the guest.
			outcome = "timeout"
		default:
			outcome = "timeout-guest-silent"
			err = fmt.Errorf("%w: %w", ErrHintingGuestSilent, err)
		}

		return fmt.Errorf("%w: %w", ErrHintingInFlight, err)
	}

	return nil
}

// StopBalloonHinting ends an in-flight hinting cycle. FC publishes
// FREE_PAGE_HINT_DONE synchronously and skips any chain the guest still sends
// for the old command, so once the stop lands no further discard can land.
// FC's done-acknowledgement is not scoped to a command, though: a guest stop
// arriving after the next start would mark that cycle done before it ran. So
// this then waits for the guest's side of the handshake. guest_cmd is sticky
// (it still reads the previous cycle's STOP until the guest echoes the new
// command), so a guest that has echoed the command is waited for until ctx
// ends, and one that has not is given ackGrace to do so before it is taken to
// have never read it. A 400 (hinting not enabled, device not active) means
// nothing was running. Needed because a second start restarts a cycle rather
// than refusing it.
func (p *Process) StopBalloonHinting(ctx context.Context, ackGrace time.Duration) error {
	if !FCSupportsFreePageHinting(p.Versions.FirecrackerVersion) {
		return nil
	}
	if err := p.client.stopBalloonHinting(ctx); err != nil {
		if errors.Is(err, errHintingStopRefused) {
			return nil
		}

		return fmt.Errorf("stop balloon hinting: %w", err)
	}

	// 0 when the start was never observed to land (its round-trip timed out);
	// then any command the guest has echoed since the last one this process
	// saw is this cycle's. guest_cmd is sticky, so an unchanged value is not.
	cmd := p.hintCmd.Load()
	// Without a baseline (the first read failed) a live-looking guest_cmd may
	// be one persisted in the snapshot: only the grace applies.
	seen, haveSeen := p.hintGuestSeen.Load(), p.hintSeenInit.Load()
	grace := time.After(max(ackGrace, 0))
	engaged := false
	var last int64
	err := p.pollHinting(ctx, func(st hintingStatus) bool {
		last = st.guestCmd
		p.hintGuestSeen.Store(st.guestCmd)
		engaged = engaged || (st.guestCmd >= freePageHintFirstCmd && (st.guestCmd == cmd || (cmd == 0 && haveSeen && st.guestCmd != seen)))
		if engaged {
			return st.guestCmd == freePageHintStop
		}
		select {
		case <-grace:
			return true
		default:
			return false
		}
	})
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			// The stop itself landed; only the acknowledgement is in doubt.
			return fmt.Errorf("%w: guest_cmd=%d engaged=%t: %w", ErrHintingStopUnacked, last, engaged, err)
		}

		return fmt.Errorf("stop balloon hinting: acknowledgement poll: %w", err)
	}

	return nil
}

// HintFreedBytes is FC's cumulative free_page_hint_freed as of its last
// metrics flush.
func (p *Process) HintFreedBytes() uint64 {
	return p.BalloonMetrics().HintFreed
}

// FlushHintFreedBytes asks FC to flush its metrics and returns the cumulative
// free_page_hint_freed once the reader has ingested the flush. FC otherwise
// flushes on a cadence of seconds, so the value read right after a run would
// almost never include that run. On error the last observed value is returned
// with the error.
func (p *Process) FlushHintFreedBytes(ctx context.Context) (uint64, error) {
	snap, err := p.FlushAndReadBalloonMetrics(ctx)

	return snap.HintFreed, err
}

// pollHinting reads the hinting status with a short backoff until done
// returns true or ctx ends; a status error ends it early.
func (p *Process) pollHinting(ctx context.Context, done func(hintingStatus) bool) error {
	backoff := 5 * time.Millisecond
	for {
		st, err := p.client.describeBalloonHinting(ctx)
		if err != nil {
			return fmt.Errorf("balloon hinting status: %w", err)
		}
		if done(st) {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, 50*time.Millisecond)
	}
}

// Exited reports whether the Firecracker process has already terminated.
func (p *Process) Exited() bool {
	select {
	case <-p.Exit.Done():
		return true
	default:
		return false
	}
}

// CreateSnapshot VM needs to be paused before creating a snapshot.
func (p *Process) CreateSnapshot(ctx context.Context, snapfilePath string) error {
	ctx, childSpan := tracer.Start(ctx, "create-snapshot-fc")
	defer childSpan.End()

	return p.client.createSnapshot(ctx, snapfilePath)
}

// resumeNetworkOverride names the interface a restore addresses (the device
// name the snapshot bakes; see Config) and, for a snapshot baked with a
// foreign name, the override that reopens it on the slot's tap.
func (p *Process) resumeNetworkOverride() (ifaceID string, override *models.NetworkOverride) {
	ifaceID = p.Versions.NetIfaceIDOrDefault(p.slot.VpeerName())
	if p.Versions.NetIfaceID == "" {
		return ifaceID, nil
	}

	tapName := p.slot.TapName()

	return ifaceID, &models.NetworkOverride{IfaceID: &ifaceID, HostDevName: &tapName}
}
