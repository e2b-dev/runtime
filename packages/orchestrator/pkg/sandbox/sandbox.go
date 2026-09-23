//go:build linux

package sandbox

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/google/uuid"
	"github.com/launchdarkly/go-sdk-common/v3/ldcontext"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/clickhouse/pkg/hoststats"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/cfg"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/build"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/cgroup"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/envdbin"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/fc"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/nbd"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/network"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/rootfs"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/template"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/uffd"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/uffd/prefetch"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/uffd/userfaultfd"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/scheduling"
	buildenvd "github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/core/envd"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/metadata"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	sbxlogger "github.com/e2b-dev/infra/packages/shared/pkg/logger/sandbox"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxtypes"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

var (
	meter                         = otel.Meter("github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox")
	envdInitCalls                 = utils.Must(telemetry.GetCounter(meter, telemetry.EnvdInitCalls))
	waitForEnvdDurationHistogram  = utils.Must(telemetry.GetHistogram(meter, telemetry.WaitForEnvdDurationHistogramName))
	envdCollapseDurationHistogram = utils.Must(telemetry.GetHistogram(meter, telemetry.EnvdCollapseDurationHistogramName))
	envdFreezeDurationHistogram   = utils.Must(telemetry.GetHistogram(meter, telemetry.EnvdFreezeDurationHistogramName))
	envdFreezeSweepHistogram      = utils.Must(telemetry.GetHistogram(meter, telemetry.EnvdFreezeSweepHistogramName))
	envdFreezeWaitHistogram       = utils.Must(telemetry.GetHistogram(meter, telemetry.EnvdFreezeWaitHistogramName))
	envdFreezeVisitedHistogram    = utils.Must(telemetry.GetHistogram(meter, telemetry.EnvdFreezeVisitedHistogramName))
	envdFreezeAuditHistogram      = utils.Must(telemetry.GetHistogram(meter, telemetry.EnvdFreezeAuditHistogramName))
	envdMemoryProtectionHistogram = utils.Must(telemetry.GetHistogram(meter, telemetry.EnvdMemoryProtectionHistogramName))
	envdDefaultsApplied           = utils.Must(telemetry.GetCounter(meter, telemetry.EnvdDefaultsApplied))
	envdDefaultsMismatch          = utils.Must(telemetry.GetCounter(meter, telemetry.EnvdDefaultsMismatch))
	envdDefaultsWorkdirWithheld   = utils.Must(telemetry.GetCounter(meter, telemetry.EnvdDefaultsWorkdirWithheld))
	envdDefaultsBuiltinFallback   = utils.Must(telemetry.GetCounter(meter, telemetry.EnvdDefaultsBuiltinFallback))
	envdFreezeCgroupsHistogram    = utils.Must(telemetry.GetHistogram(meter, telemetry.EnvdFreezeCgroupsHistogramName))
	envdUnfreezeDurationHistogram = utils.Must(telemetry.GetHistogram(meter, telemetry.EnvdUnfreezeDurationHistogramName))
	envdCollapseChunks            = utils.Must(telemetry.GetCounter(meter, telemetry.EnvdCollapseChunks))
	guestSyncDurationHistogram    = utils.Must(telemetry.GetHistogram(meter, telemetry.GuestSyncDurationHistogramName))
	fsQuiescedPauseCounter        = utils.Must(telemetry.GetCounter(meter, telemetry.SandboxPauseFsQuiescedCounterName))
	resumeWPModeCounter           = utils.Must(telemetry.GetCounter(meter, telemetry.SandboxResumeWPModeCounterName))

	processMemoryDurationHistogram = utils.Must(telemetry.GetHistogram(meter, telemetry.SnapshotProcessMemoryDurationName))
	processRootfsDurationHistogram = utils.Must(telemetry.GetHistogram(meter, telemetry.SnapshotProcessRootfsDurationName))
	guestFreezeDurationHistogram   = utils.Must(telemetry.GetHistogram(meter, telemetry.SnapshotGuestFreezeDurationName))
	fprResumeCounter               = utils.Must(telemetry.GetCounter(meter, telemetry.OrchestratorFPRResumeCounterName))
	rootfsSealDurationHistogram    = utils.Must(telemetry.GetHistogram(meter, telemetry.SnapshotRootfsSealDurationName))
	memorySealDurationHistogram    = utils.Must(telemetry.GetHistogram(meter, telemetry.SnapshotMemorySealDurationName))

	uffdStartupPagesHistogram       = utils.Must(telemetry.GetHistogram(meter, telemetry.UffdStartupPagesHistogramName))
	uffdStartupSourcePagesHistogram = utils.Must(telemetry.GetHistogram(meter, telemetry.UffdStartupSourcePagesHistogramName))
	uffdStartupBytesHistogram       = utils.Must(telemetry.GetHistogram(meter, telemetry.UffdStartupBytesHistogramName))
)

// Sandbox start types recorded on sandbox start/init metrics via the
// start_type attribute.
type StartType string

const (
	StartTypeCreate StartType = "create" // cold boot (template build)
	StartTypeResume StartType = "resume" // resume from a snapshot (the common runtime path)
	StartTypeReboot StartType = "reboot" // cold boot from a snapshot rootfs (filesystem-only resume)
)

// ErrWaitForEnvdTimeout is the cancel cause used when WaitForEnvd exceeds its timeout.
var ErrWaitForEnvdTimeout = errors.New("syncing took too long")

// ErrFcProcessExited is the cancel cause used when the Firecracker process exits during WaitForEnvd.
var ErrFcProcessExited = errors.New("fc process exited prematurely")

var SandboxHttpTransport = otelhttp.NewTransport(
	&http.Transport{
		DisableKeepAlives: true,
		ForceAttemptHTTP2: false,
	},
)

// Http client that should be used for requests to sandboxes.
var sandboxHttpClient = http.Client{
	Timeout:   10 * time.Second,
	Transport: SandboxHttpTransport,
}

type Config struct {
	// TODO: Remove when the rootfs path is constant.
	// Only used for v1 rootfs paths format.
	BaseTemplateID string

	Vcpu  int64
	RamMB int64

	// TotalDiskSizeMB optional, now used only for metrics.
	TotalDiskSizeMB   int64
	HugePages         bool
	FreePageReporting bool
	FreePageHinting   bool

	Envd EnvdMetadata

	FirecrackerConfig fc.Config

	// SkipEnvdWait skips the post-resume wait for envd readiness. Used by the
	// resume-build gdb debugging flow: the guest is held at a gdb entry
	// breakpoint and never boots envd, so the readiness wait would otherwise
	// time out and tear the sandbox down before a debugger can attach.
	SkipEnvdWait bool

	VolumeMounts []VolumeMountConfig

	MaxSandboxLengthHours int64

	// mu protects mutable sub-fields of Network (Egress, Ingress).
	// The Network pointer itself is set once at construction and never replaced.
	mu      *sync.RWMutex
	Network *orchestrator.SandboxNetworkConfig
}

// NewConfig creates a Config, normalizing a nil Network to an empty config
// so that Network is never nil.
func NewConfig(c Config) *Config {
	if c.Network == nil {
		c.Network = &orchestrator.SandboxNetworkConfig{}
	}

	c.mu = &sync.RWMutex{}

	return &c
}

// GetNetworkEgress returns the egress config in a thread-safe manner.
func (c *Config) GetNetworkEgress() *orchestrator.SandboxNetworkEgressConfig {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.Network.GetEgress()
}

// SetNetworkEgress updates the egress config in a thread-safe manner.
func (c *Config) SetNetworkEgress(egress *orchestrator.SandboxNetworkEgressConfig) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.Network.Egress = egress
}

// GetNetworkIngress returns the ingress config in a thread-safe manner.
func (c *Config) GetNetworkIngress() *orchestrator.SandboxNetworkIngressConfig {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.Network.GetIngress()
}

type VolumeMountConfig struct {
	ID   uuid.UUID
	Name string
	Path string
	Type string
}

type EnvdMetadata struct {
	Vars           map[string]string
	DefaultUser    *string
	DefaultWorkdir *string
	AccessToken    *string
	Version        string
}

// inPlaceStateFlipTimeout bounds the FC pause and resume calls of an in-place
// checkpoint. The PATCH is a state flip that normally completes in
// milliseconds, but the bound must exceed Firecracker's own internal 30s
// vcpu-ack deadline (RECV_TIMEOUT_SEC — FC's deadlock detector on the same
// wait), so a Go-side trip means FC itself has given up on the flip, not that
// we abandoned one still in progress on FC's serial API loop. Tripping it is
// not just a failed call: the pause path answers it with the cleanup resume,
// and a failed resume tears the sandbox down (ErrSandboxLost).
const inPlaceStateFlipTimeout = 40 * time.Second

// StopReason says why a sandbox execution ended. It is a metric label, so the
// set of values has to stay small and closed.
type StopReason string

const (
	// StopReasonKilled covers Delete and the teardowns the orchestrator does
	// itself after an operation leaves the sandbox unusable.
	StopReasonKilled             StopReason = "killed"
	StopReasonPaused             StopReason = "paused"
	StopReasonCheckpointing      StopReason = "checkpointing"
	StopReasonRegistrationFailed StopReason = "registration_failed"
	// StopReasonCrashed is the absence of a recorded reason: nothing asked the
	// sandbox to stop and it went down anyway.
	StopReasonCrashed StopReason = "crashed"
)

// sandboxLDContext builds an LD context with envd/kernel/FC-version attributes for
// per-sandbox flag targeting. Team/template targeting comes from the team and
// template contexts the caller embeds in ctx.
func sandboxLDContext(runtime sandboxtypes.RuntimeMetadata, config *Config) ldcontext.Context {
	return ldcontext.NewBuilder(runtime.SandboxID).
		Kind(featureflags.SandboxKind).
		SetString(featureflags.SandboxTemplateAttribute, runtime.TemplateID).
		SetString(featureflags.SandboxKernelVersionAttribute, config.FirecrackerConfig.KernelVersion).
		SetString(featureflags.SandboxFirecrackerVersionAttribute, config.FirecrackerConfig.FirecrackerVersion).
		SetString(featureflags.SandboxEnvdVersionAttribute, config.Envd.Version).
		SetString(featureflags.SandboxTypeAttribute, runtime.SandboxType.String()).
		Build()
}

type Resources struct {
	Slot   *network.Slot
	rootfs rootfs.Provider
	memory uffd.MemoryBackend
}

type internalConfig struct {
	EnvdInitRequestTimeout time.Duration

	// envdServerURLOverride, when non-empty, replaces the default
	// http://<slot-ip>:<envd-port> base address used for envd HTTP calls.
	// Test-only: it lets unit tests point envd ops (e.g. fsfreeze/fsthaw) at an
	// httptest server.
	envdServerURLOverride string
}

type Metadata struct {
	internalConfig internalConfig
	Config         *Config
	Runtime        sandboxtypes.RuntimeMetadata

	rwmu               sync.RWMutex // protects timing and stop reason
	startedAt          time.Time
	endAt              time.Time
	stoppedAt          time.Time
	stopReason         StopReason
	executionStartedAt time.Time
}

// GetEndAt returns the sandbox end time in a thread-safe manner.
func (m *Metadata) GetEndAt() time.Time {
	m.rwmu.RLock()
	defer m.rwmu.RUnlock()

	return m.endAt
}

// SetEndAt sets the sandbox end time in a thread-safe manner.
func (m *Metadata) SetEndAt(t time.Time) {
	m.rwmu.Lock()
	defer m.rwmu.Unlock()

	m.endAt = t
}

type Sandbox struct {
	*Resources
	*Metadata

	updateMu sync.Mutex

	// LifecycleID is a unique identifier for each Firecracker process.
	// It is used internally by the orchestrator for map eviction guards
	// and proxy connection pooling. Unlike ExecutionID (which is stable
	// across checkpoints and shared with the API), LifecycleID changes
	// every time a new Firecracker VM is started.
	LifecycleID string

	// Fresh host timestamp marking lifecycle start.
	LifecycleStartedAt time.Time

	config  cfg.BuilderConfig
	files   *storage.SandboxFiles
	cleanup *Cleanup

	sandboxes *Map

	featureFlags *featureflags.Client

	process      *fc.Process
	cgroupHandle *cgroup.CgroupHandle

	// inPlaceCheckpointInFlight excludes concurrent in-place checkpoints of
	// this sandbox: it stays live and addressable through one (no MarkStopping),
	// so nothing else prevents a second Checkpoint RPC from racing
	// Pause/CreateSnapshot/ResumeInPlace on the same FC process. See
	// Server.checkpointInPlace.
	inPlaceCheckpointInFlight atomic.Bool

	// useSyncWP records whether this sandbox was resumed with synchronous
	// userfault write-protect delivery (use_sync_wp on snapshot load). Only
	// then can the page tracker serve as the pause-time dirty source.
	// Written once during resume, before the sandbox is published; read-only
	// afterwards (see UseSyncWP).
	useSyncWP bool

	Template template.Template

	// liveEnvdVersion is the version the running envd reported on its most recent
	// /init response (X-Envd-Version). Empty until first captured. The resume-time
	// upgrade trigger uses this ground truth — not the template built-with, which
	// never changes across live upgrades — to decide, label, and confirm upgrades.
	liveEnvdVersion atomic.Pointer[string]
	// handoverResult is the live-upgrade handover outcome the running envd
	// reported on its most recent /init (X-Envd-Handover header), or nil if the
	// running envd did not boot from a handover.
	handoverResult atomic.Pointer[EnvdHandoverResult]
	// envdMemory is the memory protection the running envd most recently reported on
	// its cgroup chain (X-Envd-Memory on /init); nil until a report has decoded, which a
	// start that never got a response stays at. It is what the init instruments derive
	// their protection cohort attribute from.
	envdMemory atomic.Pointer[EnvdMemoryProtection]
	// envdReportedDefaults is what the running envd said it is effectively serving
	// with, from the X-Envd-Defaults header on its most recent /init. Nil means the
	// running envd never reported any — which is a CAPABILITY signal, not an empty
	// value: only an envd that carries the exec context across a handover emits this
	// header, so its absence says the blob cannot preserve the identity either.
	envdReportedDefaults atomic.Pointer[EnvdEffectiveDefaults]

	// envdWorkdirWithheld records that this sandbox's metadata names a default workdir
	// that the start path did NOT re-send, so the value survives only in the running
	// envd's memory. Written once during resume, before the sandbox is published;
	// read-only afterwards (see EnvdWorkdirWithheld).
	//
	// Only the memory-resume path sets it. A filesystem-only cold boot reconstructs and
	// sends the recorded workdir, so nothing is withheld there and the zero value is
	// correct — which matters, because both paths reach the live-upgrade gate.
	envdWorkdirWithheld bool

	Checks *Checks

	hostStatsCollector *HostStatsCollector

	// Deprecated: to be removed in the future
	// It was used to store the config to allow API restarts
	APIStoredConfig *orchestrator.SandboxConfig

	CABundle string

	exit *utils.ErrorOnce

	stop utils.Lazy[error]

	// inPlaceExportedDirty accumulates every page exported by this FC
	// lifetime's in-place memory checkpoints. In-place diffs always parent on
	// the ORIGINAL template header (the sandbox's Template does not advance
	// across in-place checkpoints), so each export must be cumulative — a
	// page captured by a previous checkpoint stays write-protect-armed under
	// the deferred (CoW) export and reads as pagemap-clean at the next
	// pause, yet the base template does not hold its content. Guarded by
	// memSealMu.
	inPlaceExportedDirty *roaring.Bitmap

	// fprPauseGen counts free-page-reporting pauses on this sandbox. A
	// detached FPR-resume retry loop captures the generation it belongs to
	// and stops the moment a newer window pauses reporting again, so a stray
	// late resume cannot undo the newer window's pause.
	fprPauseGen atomic.Uint64

	// memSealMu guards memSealDone and inPlaceExportedDirty.
	memSealMu sync.Mutex
	// memSealDone is resolved when the most recent in-place background CoW
	// memory capture of this sandbox has completed (see waitForMemorySeal).
	memSealDone *utils.SetOnce[struct{}]

	// rootfsSealMu guards rootfsSealDone.
	rootfsSealMu sync.Mutex
	// rootfsSealDone is resolved when the most recent in-place background rootfs
	// seal (swap + reflink + fold) finishes. A subsequent in-place checkpoint
	// waits on it so the writable COW cache is a complete diff before it swaps
	// again. nil until the first in-place seal runs.
	rootfsSealDone *utils.SetOnce[struct{}]

	// startupRecorded guards ALL first-WaitForEnvd recording — the envd-init
	// duration + uffd.startup.* histograms, the envd-init call counter and the
	// envd memory-protection histogram (both in initEnvd), and SetStartedAt — so
	// they fire only on the actual sandbox start. A later WaitForEnvd on the same
	// handler (the post-upgrade readiness
	// re-check, or the envd-binary swap + restart in a template build) re-runs
	// /init to re-capture state but must not re-record these: ServeStats() is
	// lifetime-cumulative, the duration/counter would double-count the resume
	// KPI, and SetStartedAt would overwrite the real start with a later time.
	// CAS'd true by the first WaitForEnvd; later calls see false and skip.
	startupRecorded atomic.Bool

	// skipStartupMetrics suppresses the per-start KPI histograms (envd-init
	// duration, uffd startup pages/source-pages/bytes) for a throwaway resume,
	// so the warm harvest never pollutes the customer resume distributions. Set
	// from the WithoutLiveRegistration resume option.
	skipStartupMetrics bool
}

// BeginInPlaceCheckpoint marks an in-place checkpoint in flight; false means
// one is already running and the caller must refuse. Pair with
// EndInPlaceCheckpoint.
func (s *Sandbox) BeginInPlaceCheckpoint() bool {
	return s.inPlaceCheckpointInFlight.CompareAndSwap(false, true)
}

// EndInPlaceCheckpoint clears the in-flight marker set by BeginInPlaceCheckpoint.
func (s *Sandbox) EndInPlaceCheckpoint() {
	s.inPlaceCheckpointInFlight.Store(false)
}

func (s *Sandbox) RunUpdate(update func() error) error {
	s.updateMu.Lock()
	defer s.updateMu.Unlock()

	return update()
}

// UseSyncWP reports whether this sandbox was resumed with synchronous
// userfault write-protect delivery (use_sync_wp on snapshot load). Set once
// during resume before the sandbox is published, so it needs no locking.
func (s *Sandbox) UseSyncWP() bool {
	return s.useSyncWP
}

func (s *Sandbox) LoggerMetadata() sbxlogger.SandboxMetadata {
	return sbxlogger.SandboxMetadata{
		SandboxID:  s.Runtime.SandboxID,
		TemplateID: s.Runtime.TemplateID,
		TeamID:     s.Runtime.TeamID,
	}
}

// log returns the internal orchestrator logger tagged with this sandbox's
// identity. Not to be confused with LoggerMetadata, which feeds the separate,
// customer-visible sandbox log stream.
func (s *Sandbox) log() logger.Logger {
	return s.Runtime.Logger()
}

// GetStartedAt returns the sandbox start time in a thread-safe manner.
func (m *Metadata) GetStartedAt() time.Time {
	m.rwmu.RLock()
	defer m.rwmu.RUnlock()

	return m.startedAt
}

func (m *Metadata) GetExecutionStartedAt() time.Time {
	m.rwmu.RLock()
	defer m.rwmu.RUnlock()

	return m.executionStartedAt
}

func (m *Metadata) SetExecutionStartedAt(t time.Time) {
	m.rwmu.Lock()
	defer m.rwmu.Unlock()

	if m.executionStartedAt.IsZero() {
		m.executionStartedAt = t
	}
}

// SetStartedAt sets the sandbox start time in a thread-safe manner.
func (m *Metadata) SetStartedAt(t time.Time) {
	m.rwmu.Lock()
	defer m.rwmu.Unlock()

	m.startedAt = t
}

// SetStoppedAt records when the guest stopped executing. The first call wins:
// a pause suspends the VM before it snapshots and uploads, and that tail is
// not time the sandbox was running.
func (m *Metadata) SetStoppedAt(t time.Time) {
	m.rwmu.Lock()
	defer m.rwmu.Unlock()

	if !m.stoppedAt.IsZero() {
		return
	}

	m.stoppedAt = t
}

// SetStopReason records why this execution is being torn down. Call it before
// triggering the stop, or the stop wins the race and the execution reads as a
// crash. The first call wins, so a teardown landing on an already-ending
// sandbox does not relabel it.
func (m *Metadata) SetStopReason(reason StopReason) {
	m.rwmu.Lock()
	defer m.rwmu.Unlock()

	if m.stopReason != "" {
		return
	}

	m.stopReason = reason
}

// GetStopReason returns the recorded teardown reason. An execution that ended
// with none was never asked to stop, so it crashed. Only meaningful once the
// execution has ended.
func (m *Metadata) GetStopReason() StopReason {
	m.rwmu.RLock()
	defer m.rwmu.RUnlock()

	if m.stopReason == "" {
		return StopReasonCrashed
	}

	return m.stopReason
}

// ExecutionDuration returns how long the guest ran, from being ready to serve
// until it stopped executing. It reports false when that span is unknown: no
// start time (a failed envd init records none), no stop time yet, or the two
// out of order.
func (m *Metadata) ExecutionDuration() (time.Duration, bool) {
	m.rwmu.RLock()
	defer m.rwmu.RUnlock()

	if m.startedAt.IsZero() || m.stoppedAt.IsZero() || m.stoppedAt.Before(m.startedAt) {
		return 0, false
	}

	return m.stoppedAt.Sub(m.startedAt), true
}

type Factory struct {
	Sandboxes         *Map
	config            cfg.BuilderConfig
	networkPool       network.PoolInterface
	devicePool        *nbd.DevicePool
	featureFlags      *featureflags.Client
	hostStatsDelivery hoststats.Delivery
	cgroupManager     cgroup.Manager
	egressProxy       network.EgressProxy
	networkAssignHook NetworkAssignHook
	envdBinCache      *envdbin.Cache

	// swapEnvdBinary is the offline envd upgrade's one call into the host. Nil
	// means rootfs.SwapEnvdBinary; a test substitutes it to drive the decision
	// flow around the call -- which refusal defers, which failure still boots the
	// original, which one fails the boot -- without debugfs, NBD, systemd or
	// privileges. It mirrors the seam rootfs.swapIO provides one layer down, and
	// for the same reason: every one of those outcomes returns the same nil to the
	// resume, so nothing else can tell them apart.
	swapEnvdBinary func(ctx context.Context, rootfsPath, srcPath string) (rootfs.SwapResult, error)
}

// offlineSwap is the offline swap implementation this Factory should use.
func (f *Factory) offlineSwap() func(ctx context.Context, rootfsPath, srcPath string) (rootfs.SwapResult, error) {
	if f.swapEnvdBinary != nil {
		return f.swapEnvdBinary
	}

	return func(ctx context.Context, rootfsPath, srcPath string) (rootfs.SwapResult, error) {
		return rootfs.SwapEnvdBinary(ctx, rootfsPath, srcPath, f.config.OrchestratorBaseDir)
	}
}

// NewFactory builds the sandbox factory. It takes a context because it does
// context-carrying work of its own: it resolves the envd-binary-cache flag and,
// when that is on, starts the background warm of the promoted envd binary.
func NewFactory(
	ctx context.Context,
	config cfg.BuilderConfig,
	networkPool network.PoolInterface,
	devicePool *nbd.DevicePool,
	featureFlags *featureflags.Client,
	hostStatsDelivery hoststats.Delivery,
	cgroupManager cgroup.Manager,
	egressProxy network.EgressProxy,
	networkAssignHook NetworkAssignHook,
	sandboxes *Map,
) *Factory {
	if networkAssignHook == nil {
		networkAssignHook = NoopNetworkAssignHook{}
	}

	f := &Factory{
		Sandboxes:         sandboxes,
		config:            config,
		networkPool:       networkPool,
		devicePool:        devicePool,
		featureFlags:      featureFlags,
		hostStatsDelivery: hostStatsDelivery,
		cgroupManager:     cgroupManager,
		egressProxy:       egressProxy,
		networkAssignHook: networkAssignHook,
		// Constructed from config rather than injected: its only inputs are the
		// cache directory and the version probe, so every caller would pass the
		// same thing. It touches no filesystem until first used.
		//
		// nil when there is no directory to put copies in, so the call sites'
		// existing `cache != nil` guard disengages the resolver. A cache built on an
		// empty dir would instead pass that guard and then miss forever while
		// WarmAsync returned before it could even count the attempt -- both upgrade
		// paths silently off, with warms{} flat at zero, which is the one state the
		// warm counter exists to make distinguishable from a working node.
		envdBinCache: newEnvdBinCache(config.OrchestratorBaseDir),
	}

	// Warm the promoted binary now rather than leaving it to the first resume that
	// wants it. A node boots long before it resumes anything, so this is where the
	// 13 MB read belongs: nothing waits on it here, and by the time an upgrade
	// needs the version the copy is already local.
	//
	// Gated, because with the flag off this would still read 13 MB and create a
	// directory on every node at boot -- small, but an observable difference where
	// there should be none. The cost of gating it is one DEFERRED upgrade per node
	// when the flag is first enabled: that first resume misses, skips the upgrade,
	// and the warm it kicks off makes the next one a hit.
	//
	// WarmAsync strips ctx's cancellation and gives the work its own deadline, so
	// the warm outlives whatever built the factory.
	//
	// Deciding WHAT to warm is on the same side of that boundary as the copy
	// itself, and deliberately so: resolving a version-pinned target stats the
	// artifact mount, which is the one thing here that can block for as long as
	// the mount wants to. This constructor runs on the orchestrator's startup
	// path, so doing it inline would let a wedged mount hold startup open with no
	// deadline -- the same read this cache exists to keep off the paths that wait.
	// Nothing consumes the result, so nothing needs it before the node starts.
	if featureFlags != nil {
		go func() {
			ctx := context.WithoutCancel(ctx)
			if !featureFlags.BoolFlag(ctx, featureflags.EnvdBinaryCacheFlag) {
				return
			}
			for _, p := range envdWarmTargets(ctx, featureFlags, config.HostEnvdPath) {
				f.envdBinCache.WarmAsync(ctx, p)
			}
		}()
	}

	return f
}

// envdBinCacheDir is where the host envd binary's node-local copies live. It is
// derived rather than configured: the directory is bounded and disposable, so
// nothing needs to relocate it independently of the orchestrator's base path.
//
// An unset base yields no directory rather than a relative one — filepath.Join
// would return "envdbin", a path under whatever the process's working
// directory happens to be, and a Factory built from a zero-value config (as
// tests do) would then create it.
func envdBinCacheDir(base string) string {
	if base == "" {
		return ""
	}

	return filepath.Join(base, "envdbin")
}

// envdWarmTargets is what the boot warm should prime: the binaries the two
// upgrade paths will actually ask for, deduplicated.
//
// Not simply HostEnvdPath. Either flag may name a concrete version, which resolves
// to a staged sibling (envd.<id>, or <id>/envd) rather than to the promoted path --
// and pinning a version is the normal way to canary a build. Warming only the
// promoted path on such a ramp gives it no pre-warm at all, while still paying a
// warm's cost and holding one of the cache's four slots for bytes nobody asks for.
//
// Resolved through the same helper the resolver uses, so the two cannot disagree
// about what a target names, and deliberately WITHOUT probing: the version does
// not matter here, only which file to copy.
//
// The promoted path is always included, even when both targets are off. It is what
// a target flag turning on will most often name, so warming it is what makes that
// first upgrade a hit instead of a deferral -- and the flags can ramp long after
// this node booted, when nothing re-runs this.
func envdWarmTargets(ctx context.Context, ff *featureflags.Client, hostEnvdPath string) []string {
	seen := map[string]struct{}{hostEnvdPath: {}}
	targets := []string{hostEnvdPath}
	for _, flag := range []featureflags.StringFlag{
		featureflags.EnvdUpgradeTargetFlag,
		featureflags.EnvdOfflineUpgradeTargetFlag,
	} {
		target := ff.StringFlag(ctx, flag)
		// Only a version-pinned target can name a file this list does not already
		// hold: "" and "off" name nothing, and "promoted" resolves to hostEnvdPath.
		// Skipped rather than resolved, because resolving stats the mount to
		// establish what these three values already tell us.
		switch target {
		case "", "off", "promoted":
			continue
		}

		candidate, _ := featureflags.EnvdUpgradeCandidate(target, hostEnvdPath)
		if candidate == "" {
			continue
		}
		if _, dup := seen[candidate]; dup {
			continue
		}
		seen[candidate] = struct{}{}
		targets = append(targets, candidate)
	}

	return targets
}

// newEnvdBinCache returns the node-local envd binary cache, or nil when there is
// no base path to derive a directory from -- a Config built as a struct literal,
// or ORCHESTRATOR_BASE_PATH set to the empty string, which env parsing treats as
// a value rather than as absent and so does not replace with the default.
func newEnvdBinCache(base string) *envdbin.Cache {
	dir := envdBinCacheDir(base)
	if dir == "" {
		return nil
	}

	return envdbin.NewCache(dir, buildenvd.GetEnvdVersion)
}

// EnvdBinCache is the node-local cache of the host envd binary, shared by the
// live-upgrade path (which delivers the binary to a running envd) and the
// offline swap path (which stages it into a rootfs before boot).
func (f *Factory) EnvdBinCache() *envdbin.Cache {
	return f.envdBinCache
}

// runNetworkAssignHook calls the configured NetworkAssignHook.OnNetworkAssign
// synchronously and imposes no timeout. The implementation is responsible for
// constraining its own duration. The code recovers any panic here to keep it from
// crashing the whole process because there is no other recovery layer between
// this call and the process's main goroutine.
//
// The synchronous call ensures the hook finishes before the resource becomes
// active, preserving that ordering guarantee.
func (f *Factory) runNetworkAssignHook(ctx context.Context, sbx *Sandbox, reason NetworkAssignReason) {
	defer func() {
		if r := recover(); r != nil {
			sbx.log().Error(ctx, "sandbox network-assign hook panicked, continuing",
				logger.WithLifecycleID(sbx.LifecycleID),
				zap.Any("panic", r))
		}
	}()

	if err := f.networkAssignHook.OnNetworkAssign(ctx, sbx, reason); err != nil {
		sbx.log().Warn(ctx, "sandbox network-assign hook failed, continuing",
			logger.WithLifecycleID(sbx.LifecycleID),
			zap.Error(err))
	}
}

func (f *Factory) EgressProxy() network.EgressProxy {
	return f.egressProxy
}

// NewDirectPathMount opens host-side NBD access without a Firecracker VM.
func (f *Factory) NewDirectPathMount(backend block.Device, lg logger.Logger) *nbd.DirectPathMount {
	return nbd.NewDirectPathMount(backend, f.devicePool, f.featureFlags, lg)
}

// PreBootFn is an optional callback invoked after the rootfs is ready but before
// Firecracker boots. It receives the rootfs device path (e.g., a file path for
// DirectProvider or /dev/nbdX for NBDProvider) and may modify the filesystem
// on the host side.
type PreBootFn func(ctx context.Context, rootfsPath string) error

type createOptions struct {
	deferMarkRunning    bool
	networkAssignReason NetworkAssignReason
}

type CreateOption func(*createOptions)

// WithDeferredMarkRunning skips marking the sandbox running inside CreateSandbox
// so the caller can mark it only after envd is ready, matching ResumeSandbox.
// Used by the reboot path, where the guest is cold-booting and must not be
// routable until envd answers.
func WithDeferredMarkRunning() CreateOption {
	return func(o *createOptions) { o.deferMarkRunning = true }
}

func withNetworkAssignReason(reason NetworkAssignReason) CreateOption {
	return func(o *createOptions) { o.networkAssignReason = reason }
}

// CreateSandbox creates the sandbox.
// IMPORTANT: You must Close() the sandbox after you are done with it.
func (f *Factory) CreateSandbox(
	ctx context.Context,
	config *Config,
	runtime sandboxtypes.RuntimeMetadata,
	template template.Template,
	sandboxTimeout time.Duration,
	rootfsCachePath string,
	processOptions fc.ProcessOptions,
	apiConfigToStore *orchestrator.SandboxConfig,
	preBootFn PreBootFn,
	opts ...CreateOption,
) (s *Sandbox, e error) {
	ctx, span := tracer.Start(ctx, "create sandbox")
	defer span.End()
	defer handleSpanError(span, &e)

	createOpts := createOptions{networkAssignReason: NetworkAssignReasonCreate}
	for _, opt := range opts {
		opt(&createOpts)
	}

	execCtx, execSpan := startExecutionSpan(ctx)

	exit := utils.NewErrorOnce()

	cleanup := NewCleanup()
	defer func() {
		if e != nil {
			cleanupErr := cleanup.Run(ctx)
			e = errors.Join(e, cleanupErr)
			handleSpanError(execSpan, &e)
			execSpan.End()
		}
	}()

	lifecycleID := uuid.NewString()

	ipsPromise := getNetworkSlot(ctx, f.networkPool, cleanup, config.Network, f.Sandboxes.NetworkReleased, runtime.SandboxType.EgressClass())

	sandboxFiles := template.Files().NewSandboxFiles(runtime.SandboxID)
	cleanup.Add(ctx, cleanupFiles(f.config, sandboxFiles))

	rootFS, err := template.Rootfs()
	if err != nil {
		return nil, fmt.Errorf("failed to get rootfs: %w", err)
	}

	sbxLogger := runtime.Logger()

	var rootfsProvider rootfs.Provider
	if rootfsCachePath == "" {
		rootfsProvider, err = rootfs.NewNBDProvider(
			ctx,
			rootFS,
			sandboxFiles.SandboxCacheRootfsPath(f.config.StorageConfig),
			f.devicePool,
			f.featureFlags,
			sbxLogger,
		)
	} else {
		rootfsProvider, err = rootfs.NewDirectProvider(
			ctx,
			rootFS,
			// Populate direct cache directly from the source file
			// This is needed for marking all blocks as dirty and being able to read them directly
			rootfsCachePath,
			sbxLogger,
		)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to create rootfs overlay: %w", err)
	}
	cleanup.Add(ctx, rootfsProvider.Close)
	go func() {
		runErr := rootfsProvider.Start(execCtx)
		if runErr != nil {
			sbxLogger.Error(ctx, "rootfs overlay error", zap.Error(runErr))
		}
	}()

	memfile, err := template.Memfile(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get memfile: %w", err)
	}

	memfileSize, err := memfile.Size(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get memfile size: %w", err)
	}

	// / ==== END of resources initialization ====
	ips, err := ipsPromise.Wait(ctx)
	if err != nil {
		return nil, err
	}

	// Run the optional pre-boot hook before Firecracker starts.
	// This allows host-side filesystem changes before the guest kernel takes charge.
	if preBootFn != nil {
		rootfsPath, pathErr := rootfsProvider.Path()
		if pathErr != nil {
			return nil, fmt.Errorf("failed to get rootfs path for pre-boot hook: %w", pathErr)
		}

		if hookErr := preBootFn(ctx, rootfsPath); hookErr != nil {
			return nil, fmt.Errorf("pre-boot hook failed: %w", hookErr)
		}
	}

	cgroupHandle, cgroupFD := createCgroup(ctx, f.cgroupManager, sandboxFiles.SandboxCgroupName())
	defer releaseCgroupFD(ctx, cgroupHandle, runtime.SandboxID)

	cleanup.Add(ctx, func(ctx context.Context) error {
		return cgroupHandle.Remove(ctx)
	})

	fcHandle, err := fc.NewProcess(
		ctx,
		execCtx,
		f.config,
		ips,
		sandboxFiles,
		config.FirecrackerConfig,
		rootfsProvider,
		fc.ConstantRootfsPaths,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to init FC: %w", err)
	}

	throttleConfig := featureflags.GetTCPFirewallEgressThrottleConfig(ctx, f.featureFlags)
	driveThrottleConfig := featureflags.GetBlockDriveThrottleConfig(ctx, f.featureFlags)

	telemetry.ReportEvent(ctx, "created fc client")

	fcPageSize := int64(header.PageSize)
	if config.HugePages {
		fcPageSize = int64(header.HugepageSize)
	}
	resources := &Resources{
		Slot:   ips,
		rootfs: rootfsProvider,
		memory: uffd.NewNoopMemory(memfileSize, fcPageSize),
	}

	metadata := &Metadata{
		internalConfig: internalConfig{
			EnvdInitRequestTimeout: f.GetEnvdInitRequestTimeout(ctx),
		},

		Config:  config,
		Runtime: runtime,

		startedAt: time.Now(),
		endAt:     time.Now().Add(sandboxTimeout),
	}

	sbx := &Sandbox{
		LifecycleID:        lifecycleID,
		LifecycleStartedAt: time.Now().UTC(),

		Resources:    resources,
		Metadata:     metadata,
		cgroupHandle: cgroupHandle,

		Template:  template,
		config:    f.config,
		files:     sandboxFiles,
		process:   fcHandle,
		sandboxes: f.Sandboxes,

		cleanup:      cleanup,
		featureFlags: f.featureFlags,

		APIStoredConfig: apiConfigToStore,

		CABundle: f.egressProxy.CABundle(),

		exit: exit,
	}

	f.Sandboxes.AssignNetwork(ctx, sbx)
	f.Sandboxes.reclaimLiveEntryOnCleanup(ctx, cleanup, runtime.SandboxID, sbx.LifecycleID)

	// Do not move this call: it must run after AssignNetwork above and
	// before fcHandle.Create below, so OnNetworkAssign always runs before
	// the guest can execute.
	f.runNetworkAssignHook(ctx, sbx, createOpts.networkAssignReason)

	initializeHostStatsCollector(execCtx, sbx, runtime, config, f.hostStatsDelivery)

	// Collect a final stats sample on cleanup while the cgroup is still alive.
	cleanup.Add(ctx, func(ctx context.Context) error {
		if sbx.hostStatsCollector != nil {
			sbx.hostStatsCollector.Stop(ctx)
		}

		return nil
	})

	freePageHinting := fc.FCSupportsFreePageHinting(config.FirecrackerConfig.FirecrackerVersion) && config.FreePageHinting

	err = fcHandle.Create(
		ctx,
		sbxlogger.SandboxMetadata{
			SandboxID:  runtime.SandboxID,
			TemplateID: runtime.TemplateID,
			TeamID:     runtime.TeamID,
		},
		config.Vcpu,
		config.RamMB,
		config.HugePages,
		config.FreePageReporting,
		freePageHinting,
		processOptions,
		fc.RateLimiterConfig{
			Ops:       fc.TokenBucketConfig(throttleConfig.Ops),
			Bandwidth: fc.TokenBucketConfig(throttleConfig.Bandwidth),
		},
		fc.RateLimiterConfig{
			Ops:       fc.TokenBucketConfig(driveThrottleConfig.Ops),
			Bandwidth: fc.TokenBucketConfig(driveThrottleConfig.Bandwidth),
		},
		cgroupFD,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create FC: %w", err)
	}
	telemetry.ReportEvent(ctx, "created fc process")

	sbx.Checks = NewChecks(sbx)

	// Stop the sandbox first if it is still running, otherwise do nothing
	cleanup.AddPriority(ctx, sbx.Stop)

	go func() {
		defer execSpan.End()

		ctx, span := tracer.Start(execCtx, "sandbox-exit-wait")
		defer span.End()

		// If the process exists, stop the sandbox properly
		fcErr := fcHandle.Exit.Wait()
		err := sbx.Stop(ctx)

		exit.SetError(errors.Join(err, fcErr))
	}()

	if !createOpts.deferMarkRunning {
		if err := f.Sandboxes.MarkRunning(ctx, sbx); err != nil {
			return nil, err
		}
	}

	return sbx, nil
}

// Usage: defer handleSpanError(span, &err)
func handleSpanError(span trace.Span, err *error) {
	defer span.End()
	if err != nil && *err != nil {
		span.RecordError(*err)
		span.SetStatus(codes.Error, (*err).Error())
	}
}

// resumeOptions carries the optional knobs of ResumeSandbox.
type resumeOptions struct {
	executionStartedAt time.Time

	// denyEgress isolates the resumed sandbox from the network (except the
	// orchestrator control path) before it is resumed.
	denyEgress bool
	// skipLiveRegistration keeps the resumed sandbox out of the live registry
	// (not addressable, not counted, no health checks) for throwaways the caller
	// reaps itself.
	skipLiveRegistration bool
	// deferMarkRunning skips only MarkRunning and health-check startup inside
	// ResumeSandbox, leaving everything else (metrics, host stats, network
	// assignment) intact, so the caller can promote the sandbox to live itself
	// once a post-resume step has completed. Used by the resume-time envd
	// live-upgrade path so the sandbox is not routable during the sub-second
	// pre-/init auth window after the upgrade re-exec. Unlike skipLiveRegistration
	// the sandbox IS meant to go live — just later.
	deferMarkRunning bool
}

// describesCustomerStart reports whether this resume's start-path telemetry describes a
// customer start, and so belongs in the fleet's startup numbers.
//
// One definition for every such counter, because they are read against each other: the
// prefetch harvest resumes EVERY memory pause with a copy of the original config, so a
// counter that includes throwaways and a counter that does not cannot be divided. It loses
// no signal — the sandbox's own resume reports the same thing moments later.
//
// The counters that follow it, exhaustively: the per-start KPI histograms (via
// skipStartupMetrics), the two envd-defaults volume counters recordEnvdDefaults emits,
// builtin_fallback in compareEnvdDefaults, and the resume wp_mode counter. Deliberately NOT
// the mismatch counter, which reports a defect rather than sizing a population — a defect on
// a throwaway resume is still a defect. Also deliberately NOT the envd memory-protection
// histogram, which follows the envd.init.calls predicate instead so that it can be divided
// by that counter's exit_type=success series over the starts whose header decoded;
// init.calls counts throwaways, so the histogram must too.
func (o *resumeOptions) describesCustomerStart() bool {
	return !o.skipLiveRegistration
}

// ResumeOption customizes a ResumeSandbox call.
type ResumeOption func(*resumeOptions)

func WithExecutionStartedAt(startedAt time.Time) ResumeOption {
	return func(o *resumeOptions) { o.executionStartedAt = startedAt }
}

// WithDenyEgress denies all network egress for the resumed sandbox — except the
// orchestrator control path — before Firecracker is resumed, so neither envd
// init nor any briefly unfrozen workload can reach the network. It is used for
// the throwaway pause-resume prefetch harvest sandbox, which is reaped as soon
// as its resume working set has been recorded.
func WithDenyEgress() ResumeOption {
	return func(o *resumeOptions) { o.denyEgress = true }
}

// WithoutLiveRegistration resumes the sandbox without adding it to the live
// registry and without starting health checks. The sandbox is not addressable
// via the sandbox map, is not counted in the node's reported allocation, and
// emits no per-sandbox metrics — for throwaways (e.g. the pause-resume prefetch
// harvest) that the caller reaps itself rather than promoting to a live
// sandbox. The network IP mapping is still assigned so the resume's own
// teardown stays symmetric.
func WithoutLiveRegistration() ResumeOption {
	return func(o *resumeOptions) { o.skipLiveRegistration = true }
}

// WithDeferredLiveRegistration resumes the sandbox but defers MarkRunning and
// health-check startup to the caller, so the sandbox is not addressable via the
// sandbox map until the caller promotes it with Sandboxes.MarkRunning +
// Checks.Start. Used by the resume-time envd live-upgrade path to keep the
// sandbox out of routing during the sub-second pre-/init window after the
// upgrade re-exec. Everything else (metrics, host stats, network assignment)
// runs as normal, so — unlike WithoutLiveRegistration — the sandbox is a real,
// soon-to-be-live sandbox, not a throwaway.
func WithDeferredLiveRegistration() ResumeOption {
	return func(o *resumeOptions) { o.deferMarkRunning = true }
}

// ThrowawayResumeOptions are the resume options for a caller-reaped throwaway
// (e.g. the pause-resume prefetch harvest): network-isolated and kept out of the
// live registry. It is the single source of truth for that option set so callers
// can't drift, and so the set can be asserted in one place.
func ThrowawayResumeOptions() []ResumeOption {
	return []ResumeOption{WithDenyEgress(), WithoutLiveRegistration()}
}

// ResumeSandbox resumes the sandbox from already saved template or snapshot.
// IMPORTANT: You must Close() the sandbox after you are done with it.
func (f *Factory) ResumeSandbox(
	ctx context.Context,
	t template.Template,
	config *Config,
	runtime sandboxtypes.RuntimeMetadata,
	startedAt time.Time,
	endAt time.Time,
	apiConfigToStore *orchestrator.SandboxConfig,
	opts ...ResumeOption,
) (s *Sandbox, e error) {
	ctx, span := tracer.Start(ctx, "resume sandbox")
	defer span.End()
	defer handleSpanError(span, &e)

	var ropts resumeOptions
	for _, opt := range opts {
		opt(&ropts)
	}

	execCtx, execSpan := startExecutionSpan(ctx)

	exit := utils.NewErrorOnce()

	cleanup := NewCleanup()
	defer func() {
		if e != nil {
			cleanupErr := cleanup.Run(ctx)
			e = errors.Join(e, cleanupErr)
			handleSpanError(execSpan, &e)
			execSpan.End()
		}
	}()

	lifecycleID := uuid.NewString()

	sandboxFiles := t.Files().NewSandboxFiles(runtime.SandboxID)
	cleanup.Add(ctx, cleanupFiles(f.config, sandboxFiles))

	telemetry.ReportEvent(ctx, "created sandbox files")

	// Identity shared by everything this resume logs on the sandbox's behalf:
	// the uffd backend (and its serve loop) and the prefetcher.
	sbxLogger := runtime.Logger()

	// Uffd initialization
	fcUffdPath := sandboxFiles.SandboxUffdSocketPath()
	uffdPromise := utils.NewPromise(func() (*uffd.Uffd, error) {
		memfile, err := t.Memfile(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to get memfile: %w", err)
		}

		telemetry.ReportEvent(ctx, "got template memfile")

		return uffd.New(memfile, fcUffdPath, sbxLogger), nil
	})

	// Prefetching. Derive the prefetch context and register its cancel with the
	// cleanup manager *synchronously*, before the goroutine below. execCtx is
	// non-cancelable (context.WithoutCancel) and the fetch-only last-cycle path
	// has no copy worker to observe uffd close, so without an explicit cancel a
	// torn-down sandbox would keep draining a (potentially multi-GiB) diff from
	// object storage. Registering here rather than inside the goroutine also
	// avoids racing cleanup.Run: a goroutine-side Add could lose the hasRun
	// check to a concurrent teardown and never register.
	//
	// Register as PRIORITY so teardown aborts the fetch first: priority handlers
	// run before the normal cleanup list, and (LIFO) this one runs before the
	// priority Stop — otherwise the fetchers keep issuing large memfile reads
	// through Stop and the rest of the normal cleanup until a late cancel.
	prefetchCtx, cancelPrefetch := context.WithCancel(execCtx)
	cleanup.AddPriority(ctx, func(context.Context) error {
		cancelPrefetch()

		return nil
	})

	go func() {
		memfile, err := t.Memfile(ctx)
		if err != nil {
			return
		}

		meta, err := t.Metadata()
		if err != nil {
			return
		}

		telemetry.ReportEvent(ctx, "got metadata")

		// Start background prefetchers as early as possible. Fetching from
		// source starts immediately; copying (when prefaulting) waits for uffd.
		//
		// Up to two independent mappings are replayed on a resume, chosen by the
		// resume-prefetch-source flag (see selectResumePrefetch):
		//  - The init trace (meta.Prefetch.Memory): the build-time
		//    create-from-template / checkpoint read-hot startup working set.
		//    Prefaulted, exactly as today.
		//  - The last-cycle diff: the pages this sandbox's last resume→pause
		//    cycle wrote — its own pause diff, derived from the memfile header
		//    (see buildDiffMemoryPrefetchMapping) — a good predictor of the
		//    next cycle's working set. Replayed FETCH-ONLY: it warms the cache
		//    and lets the guest fault, because prefaulting a multi-GiB diff
		//    would load UFFDIO_COPY onto the resume-critical path for no
		//    workload gain and would regress warm resumes.
		// The default source "init" selects only the init trace, preserving
		// today's behavior. Pause/resume normally has no init trace
		// (SameVersionTemplate drops it), so with source=last-cycle/both the
		// last-cycle diff usually runs alone. When both exist, the small init
		// trace runs first and last-cycle follows it (a barrier), keeping the
		// large last-cycle fetch off the resume-critical path.
		source := f.featureFlags.StringFlag(ctx, featureflags.ResumePrefetchSourceFlag, sandboxLDContext(runtime, config))
		useInit, useLastCycle := selectResumePrefetch(source)

		var initMapping *metadata.MemoryPrefetchMapping
		if useInit && meta.Prefetch != nil {
			initMapping = meta.Prefetch.Memory
		}

		var lastCycleMapping *metadata.MemoryPrefetchMapping
		if useLastCycle {
			lastCycleMapping = buildDiffMemoryPrefetchMapping(memfile.Header())
			// Bound the last-cycle volume against the shared object-store pool;
			// resume-last-cycle-prefetch-max-mib=-1 (default) is uncapped.
			maxMiB := f.featureFlags.IntFlag(ctx, featureflags.ResumeLastCyclePrefetchMaxMiBFlag, sandboxLDContext(runtime, config))
			lastCycleMapping = capResumePrefetch(lastCycleMapping, maxMiB)
		}

		// Record the chosen source and the sizes it resolved to, so a resume can
		// be cohorted by prefetch source (guards against flag misconfiguration)
		// and the last-cycle set size is visible per resume.
		execSpan.SetAttributes(
			attribute.String("resume.prefetch.source", source),
			attribute.Int("resume.prefetch.init_blocks", initMapping.Count()),
			attribute.Int("resume.prefetch.last_cycle_blocks", lastCycleMapping.Count()),
		)

		if initMapping == nil && lastCycleMapping == nil {
			return
		}

		fcUffd, err := uffdPromise.Wait(ctx)
		if err != nil {
			return
		}

		telemetry.ReportEvent(ctx, "starting prefetcher")

		go func() {
			// Init trace first, prefaulted (prod behavior). Start blocks until
			// its fetch+copy complete, so it acts as a barrier before the
			// last-cycle fetch begins.
			if initMapping != nil {
				p := prefetch.New(sbxLogger, memfile, fcUffd, initMapping, f.featureFlags)
				if err := p.Start(prefetchCtx); err != nil {
					sbxLogger.Error(ctx, "failed to start init prefetcher", zap.Error(err))
				}
			}

			// Last-cycle diff, fetch-only.
			if lastCycleMapping != nil {
				p := prefetch.New(sbxLogger, memfile, fcUffd, lastCycleMapping, f.featureFlags)
				p.Prefault = false
				if err := p.Start(prefetchCtx); err != nil {
					sbxLogger.Error(ctx, "failed to start last-cycle prefetcher", zap.Error(err))
				}
			}
		}()
	}()

	// Slot initialization
	ipsPromise := getNetworkSlot(ctx, f.networkPool, cleanup, config.Network, f.Sandboxes.NetworkReleased, runtime.SandboxType.EgressClass())

	// Rootfs initialization
	overlayPromise := utils.NewPromise(func() (rootfs.Provider, error) {
		readonlyRootfs, err := t.Rootfs()
		if err != nil {
			return nil, fmt.Errorf("failed to get rootfs: %w", err)
		}

		telemetry.ReportEvent(ctx, "got template rootfs")

		overlay, err := rootfs.NewNBDProvider(
			ctx,
			readonlyRootfs,
			sandboxFiles.SandboxCacheRootfsPath(f.config.StorageConfig),
			f.devicePool,
			f.featureFlags,
			sbxLogger,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create rootfs overlay: %w", err)
		}

		cleanup.Add(ctx, overlay.Close)

		telemetry.ReportEvent(ctx, "created rootfs overlay")

		go func() {
			runErr := overlay.Start(execCtx)
			if runErr != nil {
				sbxLogger.Error(ctx, "rootfs overlay error", zap.Error(runErr))
			}
		}()

		return overlay, nil
	})

	// Memory initialization
	memoryPromise := utils.NewPromise(func() (struct{}, error) {
		fcUffd, err := uffdPromise.Wait(ctx)
		if err != nil {
			return struct{}{}, err
		}

		err = serveMemory(
			execCtx,
			cleanup,
			fcUffd,
		)
		if err != nil {
			return struct{}{}, fmt.Errorf("failed to serve memory: %w", err)
		}

		telemetry.ReportEvent(ctx, "started serving memory")

		return struct{}{}, nil
	})

	// Wait for all resources to be initialized
	ips, err := ipsPromise.Wait(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get network slot: %w", err)
	}

	telemetry.ReportEvent(ctx, "got network slot")

	// Isolate the sandbox from the network before it is resumed, so that neither
	// envd init nor any briefly unfrozen workload can egress while it runs. This
	// must happen before fcHandle.Resume below — denying on the returned handle
	// would be too late, as ResumeSandbox blocks until envd init has completed.
	if ropts.denyEgress {
		if err := ips.DenyEgress(ctx); err != nil {
			return nil, fmt.Errorf("failed to deny egress for resumed sandbox: %w", err)
		}

		telemetry.ReportEvent(ctx, "denied egress for resumed sandbox")
	}

	overlay, err := overlayPromise.Wait(ctx)
	if err != nil {
		return nil, err
	}

	_, err = memoryPromise.Wait(ctx)
	if err != nil {
		return nil, err
	}
	// ==== END of resources initialization ====

	rootfs, err := t.Rootfs()
	if err != nil {
		return nil, fmt.Errorf("failed to get rootfs overlay: %w", err)
	}

	meta, err := t.Metadata()
	if err != nil {
		return nil, fmt.Errorf("failed to get metadata: %w", err)
	}

	// The default user lives only in the restored envd's memory, so a resume that sends it
	// empty is correct only for as long as that process survives. Replace it — a live
	// upgrade re-execs into a new image with no such state — and every request that omits a
	// user resolves to the built-in fallback instead of the template's user, silently, for
	// the rest of the sandbox's life. Re-send it, so the identity is delivered rather than
	// inherited.
	//
	// Only when the metadata establishes what the build sent (see EnvdDefaultUser). When it
	// cannot, the restored process still holds the right value and sending nothing preserves
	// it; sending a guess would overwrite it permanently and without a signal.
	//
	// The workdir is never re-sent, for the same reason and deliberately: it is unprovable
	// from the metadata. That leaves a resumed sandbox's cwd falling back to the resolved
	// user's home, which is what it already did — identical to sending nothing, as today.
	//
	// A config arriving with a user already set does not skip the counter, it changes its
	// source label. No external caller supplies one (finalize, the one build phase that
	// does, goes through CreateSandbox), but this function MUTATES the config it is given
	// and the checkpoint path hands that same config back for the sandbox's next life, so
	// the second resume finds the first one's value in place. Counting only the derived
	// starts would drop those resumes from applied and workdir_withheld while the
	// live-upgrade gate still reads the withheld term off the sandbox — letting
	// gated{unprovable_workdir} exceed its own denominator, which is the divergence the
	// single derivation below exists to prevent.
	//
	// Whether a recorded workdir is being left in the running process rather than re-sent:
	// replacing the process drops a workdir that only this metadata remembers and that
	// nothing afterwards can restore. Computed ONCE, and taken by both readers — the counter
	// below and the live-upgrade gate, which reads it back off the sandbox. Two derivations
	// would let gated{unprovable_workdir} exceed workdir_withheld, and that ratio is how the
	// gate's cost is meant to be read.
	workdirWithheld := envdWorkdirWithheld(meta, config.Envd.DefaultWorkdir)

	defaultUser, defaultsSource := resolveEnvdDefaultUser(meta, config.Envd.DefaultUser)
	config.Envd.DefaultUser = defaultUser

	if ropts.describesCustomerStart() {
		recordEnvdDefaults(ctx, defaultsSource, runtime.SandboxType, workdirWithheld)
	}

	// Create cgroup for sandbox resource accounting
	cgroupHandle, cgroupFD := createCgroup(ctx, f.cgroupManager, sandboxFiles.SandboxCgroupName())
	defer releaseCgroupFD(ctx, cgroupHandle, runtime.SandboxID)

	cleanup.Add(ctx, func(ctx context.Context) error {
		return cgroupHandle.Remove(ctx)
	})

	fcHandle, fcErr := fc.NewProcess(
		ctx,
		execCtx,
		f.config,
		ips,
		sandboxFiles,
		// The versions need to base exactly the same as the paused sandbox template because of the FC compatibility.
		config.FirecrackerConfig,
		overlay,
		fc.RootfsPaths{
			TemplateVersion: meta.Version,
			TemplateID:      config.BaseTemplateID,
			BuildID:         rootfs.Header().Metadata.BaseBuildId.String(),
		},
	)
	if fcErr != nil {
		return nil, fmt.Errorf("failed to create FC: %w", fcErr)
	}

	resumeThrottleConfig := featureflags.GetTCPFirewallEgressThrottleConfig(ctx, f.featureFlags)
	resumeDriveThrottleConfig := featureflags.GetBlockDriveThrottleConfig(ctx, f.featureFlags)

	telemetry.ReportEvent(ctx, "created FC process")

	// todo: check if kernel, firecracker, and envd versions exist
	snapfile, err := t.Snapfile()
	if err != nil {
		return nil, fmt.Errorf("failed to get snapfile: %w", err)
	}

	telemetry.ReportEvent(ctx, "got snapfile")

	fcUffd, err := uffdPromise.Wait(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get uffd: %w", err)
	}

	resources := &Resources{
		Slot:   ips,
		rootfs: overlay,
		memory: fcUffd,
	}

	metadata := &Metadata{
		internalConfig: internalConfig{
			EnvdInitRequestTimeout: f.GetEnvdInitRequestTimeout(ctx),
		},

		Config:  config,
		Runtime: runtime,

		startedAt:          startedAt,
		endAt:              endAt,
		executionStartedAt: ropts.executionStartedAt,
	}

	sbx := &Sandbox{
		LifecycleID:        lifecycleID,
		LifecycleStartedAt: time.Now().UTC(),

		Resources:    resources,
		Metadata:     metadata,
		cgroupHandle: cgroupHandle,

		Template:  t,
		config:    f.config,
		files:     sandboxFiles,
		process:   fcHandle,
		sandboxes: f.Sandboxes,

		cleanup:      cleanup,
		featureFlags: f.featureFlags,

		APIStoredConfig: apiConfigToStore,
		CABundle:        f.egressProxy.CABundle(),

		exit: exit,

		// A throwaway resume keeps its warm, customer-indistinguishable start out
		// of the per-resume KPI histograms (see WaitForEnvd).
		skipStartupMetrics: !ropts.describesCustomerStart(),
	}

	useMemfd := fc.FCSupportsMemfd(config.FirecrackerConfig.FirecrackerVersion) &&
		f.featureFlags.BoolFlag(ctx, featureflags.UseMemFdFlag, sandboxLDContext(runtime, config))

	// Synchronous WP fault delivery (vs the kernel's in-place WP_ASYNC clears).
	// The serve loop resolves WP faults in both modes (inert under WP_ASYNC).
	// The operator must only enable the flag where the deployed FC accepts
	// use_sync_wp: FC's MemBackendConfig is deny_unknown_fields, so a mismatch
	// fails the snapshot load loudly instead of silently downgrading.
	useSyncWP := f.featureFlags.BoolFlag(ctx, featureflags.UseSyncWPFlag, sandboxLDContext(runtime, config))
	// Throwaway resumes (e.g. the pause-resume prefetch harvest) promise "no
	// per-sandbox metrics" and never pause, so counting them would inflate the
	// wp_mode denominator with resumes that can never contribute wp_resolve or
	// divergence samples — the same rule, and now the same predicate, as the
	// other start-population counters.
	if ropts.describesCustomerStart() {
		wpMode := "async"
		if useSyncWP {
			wpMode = "sync"
		}
		resumeWPModeCounter.Add(ctx, 1, metric.WithAttributes(attribute.String("mode", wpMode)))
	}
	// Remembered for pause: only a sync-WP sandbox may use the page tracker
	// as its dirty source (see processMemorySnapshot).
	sbx.useSyncWP = useSyncWP
	sbx.envdWorkdirWithheld = workdirWithheld
	// The backend records its own mode so DiffMetadata can refuse the
	// tracker dirty source for a WP_ASYNC sandbox (fail closed).
	fcUffd.SetSyncWP(useSyncWP)

	// Part of the sandbox as we need to stop Checks before pausing the sandbox
	// This is to prevent race condition of reporting unhealthy sandbox
	sbx.Checks = NewChecks(sbx)

	cleanup.AddPriority(ctx, func(ctx context.Context) error {
		// Stop the sandbox first if it is still running, otherwise do nothing
		return sbx.Stop(ctx)
	})

	// Register the sandbox IP before Resume so it is findable by source address
	// during the resume (e.g. for TCP firewall lookups). On failure the deferred cleanup
	// will remove it.
	f.Sandboxes.AssignNetwork(ctx, sbx)
	f.Sandboxes.reclaimLiveEntryOnCleanup(ctx, cleanup, runtime.SandboxID, sbx.LifecycleID)

	reason := NetworkAssignReasonResume
	if ropts.skipLiveRegistration {
		reason = NetworkAssignReasonThrowawayResume
	}
	// Do not move this call: it must run after AssignNetwork above and
	// before fcHandle.Resume below, so OnNetworkAssign always runs before
	// the guest can resume (and, with it, resume any live connection).
	f.runNetworkAssignHook(ctx, sbx, reason)

	// A throwaway also skips host-stats collection, so it emits no per-sandbox
	// host stats under its (unregistered) identity — consistent with not being in
	// the live registry. The cleanup below is nil-safe when the collector is unset.
	if !ropts.skipLiveRegistration {
		initializeHostStatsCollector(execCtx, sbx, runtime, config, f.hostStatsDelivery)
	}

	// Collect a final stats sample on cleanup while the cgroup is still alive.
	cleanup.Add(ctx, func(ctx context.Context) error {
		if sbx.hostStatsCollector != nil {
			sbx.hostStatsCollector.Stop(ctx)
		}

		return nil
	})

	uffdStartCtx, cancelUffdStartCtx := context.WithCancelCause(ctx)
	defer cancelUffdStartCtx(errors.New("uffd finished starting"))
	go func() {
		uffdWaitErr := fcUffd.Exit().Wait()

		cancelUffdStartCtx(fmt.Errorf("uffd process exited: %w", errors.Join(uffdWaitErr, context.Cause(uffdStartCtx))))
	}()
	fcStartErr := fcHandle.Resume(
		uffdStartCtx,
		sbxlogger.SandboxMetadata{
			SandboxID:  runtime.SandboxID,
			TemplateID: runtime.TemplateID,
			TeamID:     runtime.TeamID,
		},
		fcUffdPath,
		snapfile,
		fcUffd.Ready(),
		config.Envd.AccessToken,
		cgroupFD,
		useMemfd,
		useSyncWP,
		fc.RateLimiterConfig{
			Ops:       fc.TokenBucketConfig(resumeThrottleConfig.Ops),
			Bandwidth: fc.TokenBucketConfig(resumeThrottleConfig.Bandwidth),
		},
		fc.RateLimiterConfig{
			Ops:       fc.TokenBucketConfig(resumeDriveThrottleConfig.Ops),
			Bandwidth: fc.TokenBucketConfig(resumeDriveThrottleConfig.Bandwidth),
		},
	)

	if fcStartErr != nil {
		return nil, fmt.Errorf("failed to start FC: %w", fcStartErr)
	}

	telemetry.ReportEvent(ctx, "initialized FC")

	if config.SkipEnvdWait {
		// gdb debugging: the guest is frozen at the entry breakpoint and never
		// boots envd, so skip the readiness wait (it would time out and tear the
		// sandbox down). The caller drives the VM via the gdb stub instead.
		telemetry.ReportEvent(execCtx, "skipping envd wait (gdb mode)")
	} else {
		telemetry.ReportEvent(execCtx, "waiting for envd")

		err = sbx.WaitForEnvd(
			ctx,
			StartTypeResume,
			f.GetEnvdTimeout(ctx),
		)
		if err != nil {
			return nil, fmt.Errorf("failed to wait for sandbox start: %w", err)
		}
	}

	// A throwaway (e.g. the pause-resume prefetch harvest) is never promoted to a
	// live sandbox: keep it out of the live registry so it is not addressable and
	// does not inflate the node's reported allocation or emit per-sandbox metrics,
	// and skip health checks it would never need.
	if !ropts.skipLiveRegistration && !ropts.deferMarkRunning {
		if err := f.Sandboxes.MarkRunning(ctx, sbx); err != nil {
			return nil, err
		}
	}

	telemetry.ReportEvent(execCtx, "envd initialized")

	if !ropts.skipLiveRegistration && !ropts.deferMarkRunning {
		go sbx.Checks.Start(execCtx)
	}

	go func() {
		defer execSpan.End()

		ctx, span := tracer.Start(execCtx, "sandbox-exit-wait")
		defer span.End()

		// Wait for either uffd or fc process to exit
		select {
		case <-fcUffd.Exit().Done():
		case <-fcHandle.Exit.Done():
		}

		err := sbx.Stop(ctx)

		uffdWaitErr := fcUffd.Exit().Wait()
		fcErr := fcHandle.Exit.Wait()
		exit.SetError(errors.Join(err, fcErr, uffdWaitErr))
	}()

	return sbx, nil
}

func startExecutionSpan(ctx context.Context) (context.Context, trace.Span) {
	parentSpan := trace.SpanFromContext(ctx)

	ctx = context.WithoutCancel(ctx)
	ctx, span := tracer.Start(ctx, "execute sandbox", //nolint:spancheck // this is still just a helper method
		trace.WithNewRoot(),
	)

	parentSpan.AddLink(trace.LinkFromContext(ctx))

	return ctx, span //nolint:spancheck // this is still just a helper method
}

func (s *Sandbox) Wait(ctx context.Context) error {
	return s.exit.WaitWithContext(ctx)
}

func (s *Sandbox) Close(ctx context.Context) error {
	// The live-map entry is reclaimed inside the chain, by the callback
	// Map.reclaimLiveEntryOnCleanup registers. Close must not reclaim it here:
	// a writer at this point runs after the whole chain, which would move the
	// moment the sandbox leaves Get, Items and Count to the very end of teardown,
	// past every step the registration point was chosen to precede.
	err := s.cleanup.Run(ctx)
	if s.sandboxes != nil {
		s.sandboxes.MarkStopped(context.WithoutCancel(ctx), s)
	}

	if err != nil {
		return fmt.Errorf("failed to cleanup sandbox: %w", err)
	}

	return nil
}

// Stop kills the sandbox. It is safe to call multiple times; only the first
// call will actually perform the stop operation.
func (s *Sandbox) Stop(ctx context.Context) error {
	return s.stop.GetOrInit(func() error {
		return s.doStop(ctx)
	})
}

// doStop performs the actual stop operation.
func (s *Sandbox) doStop(ctx context.Context) error {
	ctx, span := tracer.Start(ctx, "sandbox-close")
	defer span.End()

	var errs []error

	// Stop the health checks before stopping the sandbox
	s.Checks.Stop()

	fcStopErr := s.process.Stop(ctx)
	if fcStopErr != nil {
		errs = append(errs, fmt.Errorf("failed to stop FC: %w", fcStopErr))
	}

	cgroupKillErr := s.cgroupHandle.Kill(ctx)
	if cgroupKillErr != nil {
		errs = append(errs, fmt.Errorf("failed to kill sandbox cgroup: %w", cgroupKillErr))
	}

	// The process should exit before the rest of cleanup, but memory shutdown
	// must still run if the wait context is canceled so UFFD can exit.
	// FC's own exit error is reported via the exit waiters, not as a stop
	// failure, so only a canceled wait counts as an error here.
	select {
	case <-s.process.Exit.Done():
	case <-ctx.Done():
		errs = append(errs, fmt.Errorf("failed waiting for FC exit: %w", ctx.Err()))
	}

	uffdStopErr := s.Resources.memory.Stop()
	if uffdStopErr != nil {
		errs = append(errs, fmt.Errorf("failed to stop uffd: %w", uffdStopErr))
	}

	return errors.Join(errs...)
}

func (s *Sandbox) Shutdown(ctx context.Context) error {
	ctx, span := tracer.Start(ctx, "shutdown sandbox")
	defer span.End()

	// Stop the health check before pausing the VM
	s.Checks.Stop()

	if err := s.process.Pause(ctx); err != nil {
		return fmt.Errorf("failed to pause VM: %w", err)
	}

	// This is required because the FC API doesn't support passing /dev/null
	cachePaths, err := storage.Paths{
		BuildID: uuid.New().String(),
	}.Cache(s.config.StorageConfig)
	if err != nil {
		return fmt.Errorf("failed to create cache paths: %w", err)
	}
	defer cachePaths.Close()

	// The snapfile is required only because the FC API doesn't support passing /dev/null
	snapfile := template.NewLocalFileLink(cachePaths.CacheSnapfile())
	defer snapfile.Close()

	err = s.process.CreateSnapshot(ctx, snapfile.Path())
	if err != nil {
		return fmt.Errorf("error creating snapshot: %w", err)
	}

	// This should properly flush rootfs to the underlying device.
	err = s.Close(ctx)
	if err != nil {
		return fmt.Errorf("error stopping sandbox: %w", err)
	}

	return nil
}

type pauseOptions struct {
	filesystemSnapshot bool
	deferRootfsExport  bool
	maintainSandbox    bool
}

type PauseOption func(*pauseOptions)

// WithMaintainSandbox keeps the sandbox process and all its resources alive
// through the snapshot and resumes it in place afterwards (an in-place
// checkpoint), instead of stopping it. Combined with WithDeferredRootfsExport it
// swaps a fresh COW cache onto the live overlay and seals the old one in the
// background; without it the in-place export is synchronous.
func WithMaintainSandbox() PauseOption {
	return func(o *pauseOptions) { o.maintainSandbox = true }
}

// WithFilesystemSnapshot makes the pause produce a filesystem-only snapshot:
// guest memory is not snapshotted, only the filesystem (rootfs) is persisted.
// Resuming such a snapshot reboots the guest instead of restoring memory state.
// The default (no option) is a full memory snapshot.
func WithFilesystemSnapshot() PauseOption {
	return func(o *pauseOptions) { o.filesystemSnapshot = true }
}

// WithDeferredRootfsExport seals the rootfs diff off the critical path: the
// sandbox is ejected/stopped and the diff is reflinked in the background, so the
// pause returns without the host->NVMe writeback stall. Only safe when nothing
// reads the diff before the background seal completes — i.e. the suspend (pause)
// path, not a resume-fresh checkpoint.
func WithDeferredRootfsExport() PauseOption {
	return func(o *pauseOptions) { o.deferRootfsExport = true }
}

// ErrSandboxLost reports that an operation destroyed the sandbox in the
// process of failing (e.g. the final in-place resume failed and the frozen VM
// was torn down). Callers translating errors into API status codes must map
// this to a fatal code: the "sandbox still healthy, restore to Running"
// treatment that fits every other in-place checkpoint failure would leave a
// phantom row routed, billed and holding a concurrency slot until expiry.
var ErrSandboxLost = errors.New("sandbox lost")

// Pause creates a snapshot of the sandbox.
//
// Currently the memory snapshotting works like this:
//  1. We pause FC VM
//  2. We call FC snapshot endpoint without specifying memfile path. With our custom FC,
//     this only creates the snapfile and drains and flushes the disk.
//  3. We call custom FC endpoint that returns memory addresses of the sandbox memory, that we will process after.
//  4. In case of NoopMemory (the sandbox was not a resume) we also call the custom FC endpoint,
//     that returns info about resident memory pages and about empty memory pages.
//  5. Base on the info from the custom FC endpoint or from Uffd we copy the pages directly from the FC process to a local cache.
//  6. We then can either close the sandbox or resume it.
//
// With WithFilesystemSnapshot(), steps 3-5 are skipped: a guest sync flushes
// the page cache to disk before pause, CreateSnapshot is still called for its
// disk drain+flush side effect (the snapfile is not uploaded), and the memfile
// diff is empty (NoDiff).
func (s *Sandbox) Pause(
	ctx context.Context,
	m metadata.Template,
	useCase SnapshotUseCase,
	opts ...PauseOption,
) (st *Snapshot, e error) {
	var pauseOpts pauseOptions
	for _, opt := range opts {
		opt(&pauseOpts)
	}

	ctx, span := tracer.Start(ctx, "sandbox-snapshot", trace.WithAttributes(
		attribute.Bool("fs-only-snapshot", pauseOpts.filesystemSnapshot),
	))
	defer span.End()

	cleanup := NewCleanup()
	defer func() {
		// Cleanup the snapshot if an error occurs
		if e != nil {
			err := cleanup.Run(ctx)
			e = errors.Join(e, err)
		}
	}()

	cachePaths, err := storage.Paths{BuildID: m.Template.BuildID}.Cache(s.config.StorageConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create cache paths: %w", err)
	}
	cleanup.AddNoContext(ctx, cachePaths.Close)

	buildID, err := uuid.Parse(cachePaths.BuildID)
	if err != nil {
		return nil, fmt.Errorf("failed to parse build id: %w", err)
	}

	// Serialize against a still-running background seal from a PRIOR in-place
	// snapshot of this sandbox: its fold makes the writable COW cache a complete
	// diff again, which this snapshot's export relies on. UNCONDITIONAL (the
	// seal state lives on the long-lived Sandbox, so a plain autopause can race
	// a seal too — the destroy-path export never consults the sealing layer),
	// and FIRST: waiting here, before checks stop and before the FC pause,
	// keeps an overrunning reflink+fold out of the guest-frozen window, and a
	// latched seal error aborts while the sandbox is still fully intact.
	// Server.Pause additionally checks this BEFORE arming its teardown, so a
	// latched error fails a plain pause cleanly instead of destroying the
	// sandbox with nothing persisted.
	if err := s.waitForRootfsSeal(ctx); err != nil {
		return nil, fmt.Errorf("previous rootfs seal did not complete: %w", err)
	}

	// Serialize against a still-running CoW memory capture from a PRIOR
	// in-place snapshot, for the same two reasons and at the same spot as the
	// rootfs wait above: waiting HERE keeps an overrunning sweep out of the
	// guest-frozen window (the sandbox is still running and serving while it
	// waits), and a release failure aborts while the sandbox is fully intact.
	// UNCONDITIONAL, not just for maintainSandbox: the window state lives on
	// the long-lived Sandbox, so a normal pause (e.g. autopause) can also
	// race a window left running by a prior in-place checkpoint — its dirty
	// readout would race the tracker rebaseline the capture owns. Nothing
	// between here and the dirty readout can install a new window (windows
	// are only installed further down this same function).
	if err := s.waitForMemorySeal(ctx); err != nil {
		return nil, fmt.Errorf("previous memory capture did not complete: %w", err)
	}

	// Stop the health check before pausing the VM
	s.Checks.Stop()

	// Best-effort pre-pause guest reclaim (fstrim, sync, drop_caches,
	// compact_memory) on the live VM via envd. Per-step caps are LD-flag-driven;
	// all default to 0 which disables the chain entirely. Non-fatal.
	s.bestEffortReclaim(ctx)
	// reclaim freezes user cgroups; if pause/snapshot fails the sandbox stays
	// live, so unfreeze on error to avoid a permanently frozen live VM.
	// Only runs via cleanup.Run on the error path; success leaves the frozen
	// state intact so it persists into the snapshot.
	cleanup.Add(ctx, func(ctx context.Context) error {
		s.bestEffortUnfreeze(ctx)

		return nil
	})

	// frozen records whether the fs-only pause quiesced the rootfs with a real
	// FIFREEZE (vs a plain sync fallback); false for a memory pause.
	frozen := false
	// Count the eligible-snapshot population only for pauses that actually mint a
	// snapshot: record on the success path (deferred, e == nil) so a pause that
	// aborts after the freeze — process pause, snapshot creation, rootfs export,
	// metadata write — doesn't inflate quiesced/total. The span attribute is set
	// inline (the span already carries the outcome), the counter is not.
	defer func() {
		if e == nil && pauseOpts.filesystemSnapshot {
			fsQuiescedPauseCounter.Add(ctx, 1, metric.WithAttributes(attribute.Bool("quiesced", frozen)))
		}
	}()
	if pauseOpts.filesystemSnapshot {
		// FC never flushes the guest page cache and no memory snapshot will
		// preserve it, so the rootfs must be quiesced before pause or it would
		// persist missing acknowledged writes. This is mandatory, unlike the
		// best-effort reclaim above.
		frozen, err = s.guestPrepareFsForPause(ctx, cleanup)
		if err != nil {
			return nil, err
		}

		// Memory prefetch refers to the memfile, which is not persisted.
		m.Prefetch = nil

		span.SetAttributes(attribute.Bool("fs_quiesced", frozen))
	}

	// Record the snapshot kind in metadata so the resume path picks reboot vs
	// memory-resume from the snapshot's own metadata (see metadata.IsFilesystemOnly).
	// Set unconditionally so a memory pause of a previously-rebooted (fs-only)
	// sandbox correctly clears the flag. MarkFilesystemOnly also upgrades the
	// metadata version when needed so the flag survives deserialize for snapshots
	// taken from a V1 template.
	m = m.MarkFilesystemOnly(pauseOpts.filesystemSnapshot)
	// Persist whether the rootfs was frozen so a later feature can safely decide
	// this snapshot is one it may cold-boot / rewrite without journal repair.
	m = m.MarkFsQuiesced(pauseOpts.filesystemSnapshot && frozen)

	// Drain free-page-hinting before pause so the snapshot doesn't capture
	// pages the guest already considers free. Timeout per use case; 0 disables.
	if t := featureflags.GetFreePageHintingTimeout(ctx, s.featureFlags, string(useCase), sandboxLDContext(s.Runtime, s.Config)); t > 0 {
		drainCtx, cancel := context.WithTimeout(ctx, t)
		if err := s.process.DrainBalloon(drainCtx); err != nil {
			telemetry.ReportError(ctx, "balloon hinting drain failed (continuing pause)", err)
		}
		cancel()
	}

	// For an in-place checkpoint the VM must come back up even if the snapshot
	// fails, and health checks — stopped at the top of Pause — must restart, or
	// the still-live sandbox is left with checks permanently off. Registered
	// BEFORE the FC pause, and the resume runs on EVERY outcome — the pre-arm
	// rule: a pause whose round-trip failed may still have landed in FC, and
	// resuming a never-paused VM is an idempotent no-op (a running vCPU
	// answers Resumed), so skipping the resume on any "failed" pause is what
	// would hand the API a frozen VM labeled healthy. pauseLanded gates only
	// the freeze metric and the clock re-sync: a pause KNOWN to have failed
	// before reaching the guest froze nothing and drifted nothing.
	// resumeOnError is cleared just before the real ResumeInPlace on the
	// success path.
	pauseLanded := false
	// memExportDeferred records whether the memory export took the deferred
	// (CoW window) path — the treated arm of the ramp. Set after
	// processMemorySnapshot; read by the guest_freeze records below (the
	// cleanup closure captures the variable, and it runs strictly after the
	// assignment).
	memExportDeferred := false
	var freezeStart time.Time
	resumeOnError := pauseOpts.maintainSandbox
	if pauseOpts.maintainSandbox {
		cleanup.Add(ctx, func(ctx context.Context) error {
			if !resumeOnError {
				return nil
			}

			// WithoutCancel: this runs when the pause is already failing —
			// often BECAUSE the request context died (client disconnect).
			// The resume must not inherit that cancellation or the VM is
			// left frozen forever. The fresh timeout is the call's ONLY
			// bound (nothing in the FC client stack sets one): without it
			// a wedged FC API socket would hang this cleanup forever.
			resumeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), inPlaceStateFlipTimeout)
			err := s.process.ResumeInPlace(resumeCtx)
			cancel()
			if err != nil {
				// Same failure mode and same handling as the success-path
				// resume below: the VM is stuck paused and unrecoverable,
				// so tear it down and tag ErrSandboxLost. Returning a
				// plain error instead would surface as FailedPrecondition,
				// which the API answers by restoring a permanently frozen
				// VM to Running — routed, billed, checks off, unreapable.
				// The stop reason must land BEFORE the Close or the
				// lifecycle goroutine reads this orchestrator-chosen
				// teardown as a guest crash.
				s.SetStopReason(StopReasonKilled)

				return fmt.Errorf(
					"resume in place failed during pause cleanup, sandbox torn down: %w",
					errors.Join(ErrSandboxLost, err, s.Close(context.WithoutCancel(ctx))))
			}

			if pauseLanded {
				// The freeze window ended on the failure path; success=false
				// keeps these samples separable from clean checkpoints.
				guestFreezeDurationHistogram.Record(ctx, time.Since(freezeStart).Milliseconds(),
					metric.WithAttributes(
						attribute.Bool("deferred", memExportDeferred),
						attribute.Bool("success", false),
					))

				// The failed checkpoint restores a live sandbox, so it needs
				// the same clock re-sync as the success path — the guest was
				// paused just as long.
				go s.bestEffortEnvdReinit(ctx)
			}

			s.Checks = NewChecks(s)
			go s.Checks.Start(context.WithoutCancel(ctx))

			return nil
		})
	}

	freezeStart = time.Now()
	if pauseOpts.maintainSandbox {
		// The pause PATCH is the one state flip whose failure is AMBIGUOUS: a
		// request-ctx cancellation (client disconnect) can kill the round-trip
		// after FC already applied it. So it runs immune to request
		// cancellation under the same state-flip bound as the resume, and the
		// cleanup above resumes on EVERY outcome (see the pre-arm rule at its
		// registration). pauseLanded — the metric/clock gate — is set only on
		// a successful return, the one case the guest is KNOWN to have frozen.
		pauseCtx, cancelPause := context.WithTimeout(context.WithoutCancel(ctx), inPlaceStateFlipTimeout)
		err := s.process.Pause(pauseCtx)
		cancelPause()
		if err != nil {
			return nil, fmt.Errorf("failed to pause VM: %w", err)
		}
		pauseLanded = true
	} else {
		// Destroy path: no resume cleanup exists (resumeOnError is false), so
		// the ambiguity above has no consumer; keep the plain request-scoped
		// call.
		if err := s.process.Pause(ctx); err != nil {
			return nil, fmt.Errorf("failed to pause VM: %w", err)
		}
	}

	// The guest stops executing here; the snapshot, rootfs export and upload
	// that follow run against a paused VM. Not recorded for an in-place
	// checkpoint: the sandbox resumes and keeps running, and SetStoppedAt is
	// first-call-wins — recording now would freeze the stop time at the
	// checkpoint and undercount the execution.
	if !pauseOpts.maintainSandbox {
		s.SetStoppedAt(time.Now())
	}

	// Best-effort flush before the rootfs export goroutine closes the FC API
	// socket. Non-blocking on the reader; trades precision for pause latency.
	_ = s.process.FlushMetrics(ctx)

	// Snapfile is not closed as it's returned and cached for later use (like resume)
	snapfile := template.NewLocalFileLink(cachePaths.CacheSnapfile())
	cleanup.AddNoContext(ctx, snapfile.Close)

	// CreateSnapshot also drains and flushes the virtio disk in our custom FC, so
	// it runs even for a filesystem-only pause (which needs the disk flush); the
	// resulting snapfile is just not uploaded in that case.
	err = s.process.CreateSnapshot(ctx, snapfile.Path())
	if err != nil {
		return nil, fmt.Errorf("error creating snapshot: %w", err)
	}

	// Gather data for postprocessing
	originalRootfs, err := s.Template.Rootfs()
	if err != nil {
		return nil, fmt.Errorf("failed to get original rootfs: %w", err)
	}

	// Start POSTPROCESSING
	//
	// For a filesystem-only pause the memory snapshot is skipped entirely: the
	// memfile diff stays NoDiff with no header, and the memfile-derived fields
	// stay zero so the snapshot and scheduling metadata carry rootfs only.
	mem := MemorySnapshot{
		Diff:       build.Diff(&build.NoDiff{}),
		DiffHeader: NewResolvedDiffHeader(nil),
	}
	// startMemSeal, when non-nil, starts the background CoW memory capture;
	// invoked after the guest has resumed in place (next to startSeal).
	var startMemSeal func(context.Context)
	if !pauseOpts.filesystemSnapshot {
		mem, startMemSeal, err = s.processMemorySnapshot(ctx, buildID, pauseOpts.maintainSandbox, cleanup)
		if err != nil {
			return nil, err
		}
		memExportDeferred = startMemSeal != nil
	}
	var (
		rootfsDiff   build.Diff
		rootfsHeader *header.Header
		// startSeal, when non-nil, reflinks the frozen cache into the diff in the
		// background; the caller invokes it after the metadata is written (and,
		// for an in-place checkpoint, after the guest has resumed).
		startSeal func(context.Context)
	)

	rootfsDiff, rootfsHeader, startSeal, err = s.processRootfsSnapshot(
		ctx,
		buildID,
		originalRootfs.Header(),
		&pauseOpts,
		cleanup,
	)
	if err != nil {
		return nil, fmt.Errorf("error while post processing: %w", err)
	}

	rootfsDiffHeader := NewResolvedDiffHeader(rootfsHeader)
	// Derive scheduling metadata synchronously so Pause never blocks on the
	// async memfile-dedup header: the memfile chain comes from the resolved
	// parent header plus the new build, whose exact bytes aren't known yet, so
	// we pass the pre-dedup dirty size as an upper bound. It is block-granular
	// (dirty blocks * diff block size) and counts pages before dedup drops the
	// base-identical ones, so it over-estimates. The rootfs header is known
	// synchronously even with deferred export — the diff metadata (chain + exact
	// bytes) is read up front at pause time and only the reflink seal is deferred —
	// so the rootfs half of the scheduling metadata is exact.
	// mem.header is nil for a filesystem-only pause → rootfs-only metadata.
	schedulingMetadata := scheduling.FromHeaders(buildID, mem.header, rootfsHeader, mem.newBytes)

	metadataFileLink := template.NewLocalFileLink(cachePaths.CacheMetadata())
	cleanup.AddNoContext(ctx, metadataFileLink.Close)

	err = m.ToFile(metadataFileLink.Path())
	if err != nil {
		return nil, err
	}

	// In-place checkpoint: resume the same VM now that the snapshot's metadata is
	// written, BEFORE the (possibly deferred) rootfs seal runs, so the reflink
	// stall stays off the resume critical path. The destroy path skips this — its
	// sandbox is already stopped.
	if pauseOpts.maintainSandbox {
		resumeOnError = false

		// WithoutCancel: a client disconnect that cancels the request context
		// must not fail this resume — the VM is paused and a failure here tears
		// down a sandbox whose snapshot already succeeded. The fresh timeout is
		// the call's ONLY bound (the FC client stack sets none): request-ctx
		// cancellation used to be the one thing that could end a wedged FC API
		// call, and dropping it without a deadline would hang the Checkpoint
		// handler forever — leaking the start permit and the in-flight
		// checkpoint guard, with a frozen VM nothing reaps. On timeout the
		// ErrSandboxLost branch below tears the sandbox down instead.
		resumeCtx, cancelResume := context.WithTimeout(context.WithoutCancel(ctx), inPlaceStateFlipTimeout)
		err := s.process.ResumeInPlace(resumeCtx)
		cancelResume()
		if err != nil {
			// Final resume failed -> VM stuck paused and unrecoverable. Tear it
			// down so we don't leak a frozen VM, and fail the checkpoint —
			// tagged ErrSandboxLost so the RPC layer reports a FATAL code
			// instead of the healthy-sandbox FailedPrecondition. Stop reason
			// BEFORE the Close, or this orchestrator-chosen teardown reads as
			// a guest crash (ERROR log + stop_reason="crashed" sample).
			s.SetStopReason(StopReasonKilled)

			return nil, fmt.Errorf(
				"resume in place failed, sandbox torn down: %w",
				errors.Join(ErrSandboxLost, err, s.Close(context.WithoutCancel(ctx))))
		}

		// The guest-visible freeze window: from the FC pause call to the in-place
		// resume. This is the number the in-place feature exists to move —
		// process_memory/process_rootfs cover their exports but also run against
		// a paused VM only partially, so no other series isolates the freeze.
		guestFreezeDurationHistogram.Record(ctx, time.Since(freezeStart).Milliseconds(),
			metric.WithAttributes(
				// deferred marks the treated arm of the memory-export ramp on
				// the series the feature exists to move.
				attribute.Bool("deferred", memExportDeferred),
				attribute.Bool("success", true),
			))

		// The live VM keeps running, so undo anything the pause froze. Unlike the
		// destroy path (VM discarded with its frozen state), an in-place resume
		// must actually thaw, matching how guestPrepareFsForPause froze the rootfs:
		// native /fsthaw when envd supports it, otherwise the exec fsfreeze -u path
		// (a no-op if the guest was only sync'd). A native-only thaw would leave an
		// exec-frozen guest's filesystem frozen after resume.
		s.bestEffortUnfreeze(ctx)
		if pauseOpts.filesystemSnapshot {
			if s.envdSupportsFsFreeze(ctx) {
				s.bestEffortFsthaw(ctx)
			} else {
				s.bestEffortFsthawViaExec(ctx)
			}
		}

		go s.bestEffortEnvdReinit(ctx)

		s.Checks = NewChecks(s)
		go s.Checks.Start(context.WithoutCancel(ctx))
	}

	// Seal the rootfs diff in the background: for the destroy path the cache is
	// ejected and the sandbox stopped; for the in-place path the guest has already
	// resumed onto a fresh cache. Either way the pause returns without paying the
	// reflink/writeback stall.
	if startSeal != nil {
		startSeal(context.WithoutCancel(ctx))
	}
	// Start the CoW memory capture now that the guest is running again: from
	// here on its writes to armed pages are pre-image-copied by the fault
	// path while the sweep drains the rest.
	if startMemSeal != nil {
		startMemSeal(context.WithoutCancel(ctx))
	}

	return &Snapshot{
		MemoryExportDeferred: memExportDeferred,
		Snapfile:             snapfile,
		Metafile:             metadataFileLink,
		MemorySnapshot:       mem,
		RootfsDiff:           rootfsDiff,
		RootfsDiffHeader:     rootfsDiffHeader,
		SchedulingMetadata:   schedulingMetadata,
		FilesystemSnapshot:   pauseOpts.filesystemSnapshot,
		RootfsBlockSize:      originalRootfs.Header().Metadata.BlockSize,

		BuildID: buildID,

		cleanup: cleanup,
	}, nil
}

// MemorySnapshot bundles the products of memory postprocessing during a Pause:
// the memfile diff, its (async-resolved) header, and the block size. It is
// embedded in Snapshot. For a filesystem-only pause it is zero-valued except for
// an empty NoDiff and a resolved-nil header (see Snapshot.FilesystemSnapshot).
type MemorySnapshot struct {
	Diff       build.Diff
	DiffHeader *DiffHeader
	// ProvisionalDiffHeader + ProvisionalDiff, when non-nil, let the local
	// template serve immediately from the still-mapped memfd via a distinct
	// provisional build id while dedup runs, instead of blocking a concurrent
	// resume in storage-template-memfile on the deduped header. They feed only
	// the local AddSnapshot path; the upload still uses DiffHeader (deduped).
	ProvisionalDiffHeader *header.Header
	ProvisionalDiff       build.Diff
	// ProvisionalSwapDone, when non-nil, is invoked by the AddSnapshot swap
	// goroutine once it has swapped the deduped header in; it lets the dedup
	// goroutine release the memfd the provisional source was serving from.
	ProvisionalSwapDone func()
	// ProvisionalCreatedAt, when non-zero, is the provisional header's birth at
	// pause; the AddSnapshot swap goroutine records the dedup-duration metric
	// from it.
	ProvisionalCreatedAt time.Time
	// BlockSize is captured synchronously at Pause time because NewUpload's
	// compression validation needs it before the async dedup header resolves;
	// the dedup memfile path produces a page-granular Diff.BlockSize() that
	// doesn't match the chunker-read granularity on restore.
	BlockSize uint64

	// header (base memfile) and newBytes (pre-dedup dirty-byte upper bound) are
	// scheduling inputs consumed only at Pause time, so they stay unexported.
	header   *header.Header
	newBytes uint64

	// waitSealed, when non-nil (deferred CoW export), blocks until the
	// background seal settles and returns its outcome — the earliest point
	// the artifact bytes are known to exist in the local cache. Consumed via
	// Snapshot.WaitMemorySealed.
	waitSealed func(ctx context.Context) error
}

// processMemorySnapshot copies the dirty guest memory pages into a local diff
// and builds its header — steps 3-5 of Pause. Only called for a full memory
// snapshot; a filesystem-only pause skips it. The returned diff's Close must be
// registered for cleanup by the caller.
// processMemorySnapshot copies the dirty guest memory to a local diff. When
// keepMemfdOpen is set (an in-place snapshot that resumes the same VM) it borrows
// the memfd without consuming it and skips dedup/provisional serving, since the
// running guest still faults on that fd.
//
// On the in-place path with defer-memory-export enabled, the copy is deferred
// through a CoW window instead: the returned startMemSeal (nil otherwise) is
// invoked by the caller after ResumeInPlace and drives the background
// capture; the memory diff is a DeferredDiff resolving when the capture
// completes.
func (s *Sandbox) processMemorySnapshot(ctx context.Context, buildID uuid.UUID, keepMemfdOpen bool, cleanup *Cleanup) (MemorySnapshot, func(context.Context), error) {
	originalMemfile, err := s.Template.Memfile(ctx)
	if err != nil {
		return MemorySnapshot{}, nil, fmt.Errorf("failed to get original memfile: %w", err)
	}
	// Parent off the durable (deduped) header, never a provisional (local-only)
	// one: a provisional header maps dirty pages to a synthetic build id with no
	// storage object, so an uploaded header inheriting those mappings would be
	// unreadable on a cold or cross-node resume. DurableHeader waits for the
	// deduped header if a provisional swap is still pending; devices without one
	// return their current header immediately.
	memfileHeader := originalMemfile.Header()
	if dh, ok := originalMemfile.(interface {
		DurableHeader(ctx context.Context) (*header.Header, error)
	}); ok {
		memfileHeader, err = dh.DurableHeader(ctx)
		if err != nil {
			return MemorySnapshot{}, nil, fmt.Errorf("failed to resolve durable memfile header: %w", err)
		}
	}

	// Dirty-source selection: a sandbox resumed with use_sync_wp can derive
	// the dirty set from the page tracker and skip FC's pagemap RPC. The kill
	// switch is evaluated fresh at each pause, so flipping it off reverts
	// running sandboxes to the pagemap source without a redeploy.
	useTrackerDirty := s.useSyncWP &&
		s.featureFlags.BoolFlag(ctx, featureflags.SyncWPTrackerDirtyFlag, sandboxLDContext(s.Runtime, s.Config))

	// Deferred (CoW) memory export preflight — BEFORE the dirty readout.
	// When the VM's balloon runs continuous free-page REPORTING, reporting is
	// PAUSED for the window's lifetime: a REMOVE zapping an uncaptured page
	// would export zeros where pause-time content is owed, and the UFFD
	// handler cannot intercept it (reading the REMOVE event is what releases
	// the madvising thread). The pause must land before the readout because
	// pausing the VM stops vCPUs, not FC's balloon worker — a report already
	// in flight could otherwise zap a page the readout just listed as dirty,
	// leaving the arm covering a hole. The pause is then POSITIVELY
	// CONFIRMED via GET /balloon/reporting/status rather than inferred from
	// the config field: that also gates on FC capability for free — an FC
	// build without /balloon/reporting fails the query/pause/confirm and the
	// export falls back to the synchronous copy. (The pre-pause
	// free-page-hinting drain is synchronous and its REMOVEs settle before
	// this point, so hinting needs no pause. The guest is unaffected beyond
	// a deferred RSS reduction: its driver holds reported pages isolated
	// until the ACK.)
	deferOK := keepMemfdOpen &&
		s.featureFlags.BoolFlag(ctx, featureflags.DeferMemoryExportFlag, sandboxLDContext(s.Runtime, s.Config))
	fprPaused := false
	if deferOK {
		reporting, fprErr := s.process.BalloonFreePageReporting(ctx)
		switch {
		case fprErr != nil:
			sbxlogger.I(s).Warn(ctx, "defer-memory-export: balloon query failed; using sync copy", zap.Error(fprErr))
			deferOK = false
		case reporting:
			if pauseErr := s.process.PauseFreePageReporting(ctx); pauseErr != nil {
				sbxlogger.I(s).Warn(ctx, "defer-memory-export: pausing free-page reporting failed; using sync copy",
					zap.Error(pauseErr))
				deferOK = false
			} else if paused, stErr := s.process.FreePageReportingPaused(ctx); stErr != nil || !paused {
				sbxlogger.I(s).Warn(ctx, "defer-memory-export: free-page reporting pause not confirmed; using sync copy",
					zap.Bool("paused", paused), zap.Error(stErr))
				s.resumeFreePageReportingBestEffort(ctx)
				deferOK = false
			} else {
				fprPaused = true
				// Fence out any detached resume-retry loop a PREVIOUS
				// window's exit left running: from this generation on, the
				// resume belongs to this window's exit paths.
				s.fprPauseGen.Add(1)
			}
		}
	}

	// Pause ownership: once window setup succeeds, the window's exit paths
	// own the resume. On EVERY other exit of this function — sync fallback,
	// any error — the pause is ours to undo, and it must happen AFTER the
	// synchronous copy, not before it: the resume immediately drains the
	// reports held while paused, so resuming first would let the balloon
	// worker zap pages the readout below just listed as dirty, and the sync
	// path has no tripwire — the checkpoint would succeed with zeros stored
	// as pause-time RAM. A function-exit defer is exactly that placement.
	windowOwnsPause := false
	defer func() {
		if fprPaused && !windowOwnsPause {
			s.resumeFreePageReportingBestEffort(ctx)
		}
	}()

	memfileDiffMetadata, err := s.Resources.memory.DiffMetadata(ctx, s.process, useTrackerDirty)
	if err != nil {
		return MemorySnapshot{}, nil, fmt.Errorf("failed to get memfile metadata: %w", err)
	}

	// In-place checkpoints parent every diff on the ORIGINAL template header,
	// so the export must be cumulative across this FC lifetime: pages a
	// previous in-place checkpoint exported may read as clean now (the CoW
	// sweep leaves them armed; the tracker rebaselines them), but the base
	// template does not hold their content. Their live content is identical
	// to the previous capture unless rewritten — and then they are dirty
	// anyway — so re-exporting from the running memfd is always correct.
	// Chaining diffs on the previous checkpoint's header (which would make
	// minimal diffs sound) is the follow-up; see the CoW window notes.
	// The union applies to EVERY memory export of this sandbox, not only the
	// in-place ones: a final destroy-path pause (e.g. autopause) after prior
	// in-place checkpoints also parents the original template, so without the
	// union it would silently drop every page those checkpoints rebaselined
	// (tracker Clean / pagemap not-dirty, content living only in checkpoint
	// artifacts the new snapshot does not parent). Only the in-place path
	// advances the baseline — a destroyed sandbox has no next interval.
	s.applyInPlaceExportUnion(memfileDiffMetadata, keepMemfdOpen)
	recordSnapshotDiff(ctx, "memfile", memfileDiffMetadata, memfileHeader)

	// Deferred (CoW) memory export: install the window over the dirty set
	// read above. EVERY setup failure falls back to the synchronous copy —
	// not just an unsupported backend: nothing has been consumed at this
	// point (the memfd is only borrowed, and a partially armed set degrades
	// harmlessly to tracking-only resolves), so a transient arm or
	// cache-creation error must not turn a checkpoint the sync path would
	// complete into a customer-visible failure.
	if deferOK {
		if ce, ok := s.Resources.memory.(uffd.CoWExporter); ok {
			setupStart := time.Now()
			mem, startMemSeal, err := s.setupDeferredMemoryExport(ctx, buildID, memfileHeader, memfileDiffMetadata, ce, fprPaused, cleanup)
			if err == nil {
				windowOwnsPause = true
				// The deferred path bypasses pauseProcessMemory, so emit its
				// (setup-only) process_memory.duration sample here — without
				// it the treated arm goes silent in the ramp's headline panel
				// and reads as "no change" instead of "no data". deferred
				// follows the seal: an empty dirty set installs no window
				// (nil startMemSeal) and defers nothing.
				processMemoryDurationHistogram.Record(ctx, time.Since(setupStart).Milliseconds(),
					metric.WithAttributes(
						attribute.Bool("in_place", true),
						attribute.Bool("deferred", startMemSeal != nil),
						attribute.Bool("success", true),
					))

				return mem, startMemSeal, nil
			}
			if errors.Is(err, uffd.ErrCoWExportUnsupported) {
				sbxlogger.I(s).Warn(ctx, "defer-memory-export unsupported by backend; using sync copy", zap.Error(err))
			} else {
				sbxlogger.I(s).Error(ctx, "defer-memory-export setup failed; using sync copy", zap.Error(err))
			}
		}
		// Falling back to the sync copy: the window never took ownership of
		// the pause. The resume happens in the function-exit defer above —
		// AFTER the synchronous copy, which must run under the same
		// no-discard window as the readout.
	}

	var dedupBase block.ReadonlyDevice
	var dedupBestEffort, dedupDirectIO bool
	var dedupBudget block.DedupBudget
	dedupCfg := s.featureFlags.JSONFlag(ctx, featureflags.MemfileDiffDedupFlag, sandboxLDContext(s.Runtime, s.Config)).AsValueMap()
	if dedupCfg.Get("enabled").BoolValue() {
		dedupBase = originalMemfile
		dedupBestEffort = dedupCfg.Get("bestEffort").BoolValue()
		dedupDirectIO = dedupCfg.Get("directIO").BoolValue()
		dedupBudget = block.DedupBudget{
			MaxFetchWindowsPerBlock:        dedupCfg.Get("maxFetchWindowsPerBlock").IntValue(),
			MaxPromotedParentPagesPerBlock: dedupCfg.Get("maxPromotedParentPagesPerBlock").IntValue(),
			MaxPagesPerPromotedFrame:       dedupCfg.Get("maxPagesPerPromotedFrame").IntValue(),
			BlockFaultPct:                  dedupCfg.Get("blockFaultPct").IntValue(),
			FetchRunWindowPages:            dedupCfg.Get("fetchRunWindowPages").IntValue(),
		}
	}

	// In-place snapshot borrows the memfd via PeekMemfd (the running VM keeps
	// using it) and turns off dedup + inflight-serve, which would consume or
	// double-manage it. The destroy path consumes the memfd via Memfd. We must NOT
	// call Memfd on the in-place path: it swaps the memfd out of uffd, which both
	// leaks the fd and makes the subsequent PeekMemfd return nil.
	var memfd *block.Memfd
	dedupInflightServe := s.featureFlags.BoolFlag(ctx, featureflags.MemfdDedupInflightServeFlag, sandboxLDContext(s.Runtime, s.Config))
	if keepMemfdOpen {
		memfd = s.memory.PeekMemfd(ctx)
		dedupBase = nil
		dedupInflightServe = false
	} else {
		memfd = s.memory.Memfd(ctx)
	}

	memfileDiff, memfileDiffHeader, provMemfileHeader, provMemfileDiff, provMemfileSwapDone, err := pauseProcessMemory(
		ctx,
		buildID,
		memfileHeader,
		memfileDiffMetadata,
		s.config.DefaultCacheDir,
		s.process,
		memfd,
		s.featureFlags.BoolFlag(ctx, featureflags.MemfdBackgroundCopyFlag, sandboxLDContext(s.Runtime, s.Config)),
		dedupBase,
		dedupBestEffort,
		dedupDirectIO,
		dedupBudget,
		dedupInflightServe,
		keepMemfdOpen,
	)
	if err != nil {
		return MemorySnapshot{}, nil, fmt.Errorf("error while post processing: %w", err)
	}

	// Each path owns its diff's cleanup registration (the deferred path MUST
	// interleave Close with its promise resolver — see setupDeferredMemoryExport);
	// this is the synchronous path's.
	cleanup.AddNoContext(ctx, memfileDiff.Close)

	var provCreatedAt time.Time
	if provMemfileHeader != nil {
		provCreatedAt = time.Now()
	}

	return MemorySnapshot{
		Diff:                  memfileDiff,
		DiffHeader:            memfileDiffHeader,
		ProvisionalDiffHeader: provMemfileHeader,
		ProvisionalDiff:       provMemfileDiff,
		ProvisionalSwapDone:   provMemfileSwapDone,
		ProvisionalCreatedAt:  provCreatedAt,
		BlockSize:             memfileHeader.Metadata.BlockSize,
		header:                memfileHeader,
		newBytes:              memfileDiffMetadata.Dirty.GetCardinality() * uint64(memfileDiffMetadata.BlockSize),
	}, nil, nil
}

// applyInPlaceExportUnion folds the cumulative in-place export baseline into a
// pause-time dirty readout (see the comment at the call site for why the union
// exists), and advances the baseline when this export starts a new interval.
// Pages the baseline holds but the CURRENT readout reports empty are excluded:
// a previously-exported page the guest has since freed (FPR/balloon REMOVE →
// tracker Removed / pagemap non-present) owes ZEROS, which Empty already
// encodes with no storage — unioning it into Dirty would arm and copy a hole
// (UFFDIO_WRITEPROTECT over an unmapped range can fail the whole checkpoint,
// and a successful copy would store zero pages the mapping expresses for
// free). Dropping such a page from the advanced baseline is safe in every
// later cell: still absent → Empty again; re-touched → the serve loop
// installs zeros for a Removed page (tracker Zero → empty) or the write makes
// it tracker-dirty.
func (s *Sandbox) applyInPlaceExportUnion(meta *header.DiffMetadata, advanceBaseline bool) {
	s.memSealMu.Lock()
	defer s.memSealMu.Unlock()

	if s.inPlaceExportedDirty != nil {
		reexport := s.inPlaceExportedDirty.Clone()
		reexport.AndNot(meta.Empty)
		meta.Dirty.Or(reexport)
		// Disjoint by construction now; enforced anyway because downstream
		// MergeMappings lets EMPTY win on overlap.
		meta.Empty.AndNot(meta.Dirty)
	}
	if advanceBaseline {
		s.inPlaceExportedDirty = meta.Dirty.Clone()
	}
}

// sharedCacheWriter routes writes through Cache.WriteAtShared. Safe only for
// writers that guarantee a single write per block — the CoW window's claim
// map does — where the exclusive lock would serialize faulting vCPUs against
// the sweep and each other. The shared lock still excludes Cache.Close, so a
// straggler write fails with ErrCacheClosed instead of hitting unmapped
// memory (a process-fatal SIGBUS).
type sharedCacheWriter struct{ c *block.Cache }

func (w sharedCacheWriter) WriteAt(p []byte, off int64) (int, error) {
	return w.c.WriteAtShared(p, off)
}

// setupDeferredMemoryExport prepares the CoW-window memory export for an
// in-place checkpoint, all while the VM is paused: the diff header is built
// synchronously (the dirty set is fixed at pause, exactly like rootfs), an
// empty export cache is created, and the dirty set is write-protect-armed
// with the window installed. It returns a startMemSeal closure the caller
// invokes AFTER the guest has resumed in place, which drives the background
// sweep. Mirrors setupDeferredRootfsExport, including the started-guard
// cleanup choreography for a pause that aborts before startMemSeal runs.
func (s *Sandbox) setupDeferredMemoryExport(
	ctx context.Context,
	buildID uuid.UUID,
	memfileHeader *header.Header,
	diffMetadata *header.DiffMetadata,
	ce uffd.CoWExporter,
	fprPaused bool,
	cleanup *Cleanup,
) (MemorySnapshot, func(context.Context), error) {
	memfileDiffHeader, err := diffMetadata.ToDiffHeader(ctx, memfileHeader, buildID)
	if err != nil {
		return MemorySnapshot{}, nil, fmt.Errorf("building memfile diff header: %w", err)
	}

	mem := MemorySnapshot{
		DiffHeader: NewResolvedDiffHeader(memfileDiffHeader),
		BlockSize:  memfileHeader.Metadata.BlockSize,
		header:     memfileHeader,
		newBytes:   diffMetadata.Dirty.GetCardinality() * uint64(diffMetadata.BlockSize),
	}

	// No dirty pages: nothing to capture — same NoDiff shape as the sync
	// path, and no window to arm (so nothing will resume FPR later; undo the
	// pause here). startMemSeal is NIL, not a noop: nothing about this
	// export is deferred, so it must not flip Snapshot.MemoryExportDeferred
	// (which would mislabel the guest_freeze/process_memory cohorts and send
	// a plain NoDiff through the upload's seal gate).
	if diffMetadata.Dirty.IsEmpty() {
		if fprPaused {
			s.resumeFreePageReportingBestEffort(ctx)
		}
		mem.Diff = &build.NoDiff{}
		cleanup.AddNoContext(ctx, mem.Diff.Close)

		return mem, nil, nil
	}

	// The diff artifact stores the dirty pages CONCATENATED in ascending
	// page order (the packed layout header.CreateMapping addresses); the
	// window writes identity offsets, so its sink is the packing adapter.
	// The cache is sized to the packed artifact, not the memfile.
	packedBytes := int64(diffMetadata.Dirty.GetCardinality()) * diffMetadata.BlockSize
	cachePath := build.GenerateDiffCachePath(s.config.DefaultCacheDir, buildID.String(), build.Memfile)
	cache, err := block.NewCache(packedBytes, diffMetadata.BlockSize, cachePath, false)
	if err != nil {
		return MemorySnapshot{}, nil, fmt.Errorf("creating memory export cache: %w", err)
	}
	// sharedCacheWriter: the window's claim map guarantees exactly one
	// writer per page — the single-writer-per-block precondition — and the
	// exclusive Cache lock would otherwise serialize every faulting vCPU
	// against the background sweep and against each other, on the per-fault
	// path. The shared mode keeps Close excluded.
	sink := userfaultfd.NewPackedPageSink(diffMetadata.Dirty, diffMetadata.BlockSize, sharedCacheWriter{cache})

	// Arm + install. A partial arm on failure is harmless: armed pages
	// without a window take the plain tracking-only resolve.
	window, err := ce.BeginCoWExport(ctx, diffMetadata.Dirty, sink)
	if err != nil {
		return MemorySnapshot{}, nil, errors.Join(err, cache.Close())
	}

	diffPromise := utils.NewSetOnce[build.Diff]()
	mem.Diff = build.NewDeferredDiff(build.GetDiffStoreKey(buildID.String(), build.Memfile), diffMetadata.BlockSize, diffPromise)
	mem.waitSealed = func(ctx context.Context) error {
		_, waitErr := diffPromise.WaitWithContext(ctx)

		return waitErr
	}

	// If Pause aborts before startMemSeal runs (e.g. m.ToFile or
	// ResumeInPlace fails), the capture goroutine never starts: cancel the
	// window (the fault path degrades to tracking-only), close the cache and
	// poison the promise so nothing blocks on a capture that will never run.
	// Guarded by `started` so the running capture, once it owns them, is
	// never double-closed or raced. The deferred diff's Close is registered
	// HERE, between the window cleanup and the promise resolver — exactly
	// like setupDeferredRootfsExport — so the LIFO cleanup run poisons the
	// promise BEFORE Close waits on it; Close registered by a caller (i.e.
	// after the resolver) would run first and deadlock the whole cleanup
	// stack on the never-resolved promise.
	var started atomic.Bool
	cleanup.Add(ctx, func(cleanupCtx context.Context) error {
		if started.Load() {
			return nil
		}
		window.RecordCancelReason(cleanupCtx, "pause_abort")
		window.CancelAndDrain(errors.New("pause aborted before deferred memory export ran"))
		ce.EndCoWExport(window)
		if fprPaused {
			s.resumeFreePageReportingBestEffort(cleanupCtx)
		}

		return cache.Close()
	})
	cleanup.AddNoContext(ctx, mem.Diff.Close)
	cleanup.Add(ctx, func(context.Context) error {
		if started.Load() {
			return nil
		}
		_ = diffPromise.SetError(errors.New("pause aborted before deferred memory export ran"))

		return nil
	})

	startMemSeal := func(sealCtx context.Context) {
		started.Store(true)

		sealDone := utils.NewSetOnce[struct{}]()
		s.memSealMu.Lock()
		s.memSealDone = sealDone
		s.memSealMu.Unlock()

		go s.runDeferredMemoryExport(sealCtx, window, ce, cache, buildID, diffPromise, sealDone, fprPaused)
	}

	return mem, startMemSeal, nil
}

// runDeferredMemoryExport drives the CoW window to completion in the
// background: the sweep captures every remaining page (guest writes race it
// through the fault path), then the cache is wrapped into the promised diff.
// The upload waits on the deferred diff, gating shutdown via the server's
// upload WaitGroup — exactly like the deferred rootfs seal.
func (s *Sandbox) runDeferredMemoryExport(
	ctx context.Context,
	window *userfaultfd.CoWWindow,
	ce uffd.CoWExporter,
	cache *block.Cache,
	buildID uuid.UUID,
	diffPromise *utils.SetOnce[build.Diff],
	sealDone *utils.SetOnce[struct{}],
	fprPaused bool,
) {
	ctx, span := tracer.Start(ctx, "deferred-memory-export")
	defer span.End()

	start := time.Now()
	err := window.Sweep(ctx)
	ce.EndCoWExport(window)
	// EndCoWExport serializes behind the serve loop's whole read→parse→cancel
	// section (readSerial), so after it returns, every REMOVE whose read could
	// have released a zap has also finished its tripwire pass. Re-check before
	// trusting a successful sweep: a tripwire cancel that raced the final
	// copy's completion is authoritative — that copy may have captured
	// post-punch zeros.
	if err == nil {
		err = window.CancelCause()
	}
	// Record BEFORE the FPR resume: this series is "time for the background
	// CoW-window memory capture", and the resume's inline attempt can burn a
	// full 2s on a stuck FC API — folding that in would make a resume
	// failure read as a slow sweep. The resume has its own outcome counter.
	memorySealDurationHistogram.Record(ctx, time.Since(start).Milliseconds(),
		metric.WithAttributes(attribute.Bool("success", err == nil)))
	// The window no longer owns any page: free-page reporting may discard
	// again. Resume on success AND failure — a leaked pause would block the
	// guest's reporting worker for the sandbox's lifetime.
	if fprPaused {
		s.resumeFreePageReportingBestEffort(ctx)
	}

	fail := func(err error) {
		// ErrDeferredSealFailed tells the upload retry loop this is permanent:
		// the capture never re-runs, so the diff can never materialize.
		sealErr := fmt.Errorf("%w: %w", build.ErrDeferredSealFailed, err)
		_ = diffPromise.SetError(sealErr)
		// sbxlogger, not logger.L(): this is the primary on-call diagnostic
		// when a window fails or the tripwire fires, and trace ids are
		// sampled — the sandbox/build identity must be on the line itself.
		sbxlogger.I(s).Error(ctx, "deferred memory export failed",
			zap.String("build_id", buildID.String()), zap.Error(err))
		// Drain in-flight fault-path copies before closing the cache: Sweep
		// returns on cancel WITHOUT waiting them out. sharedCacheWriter's
		// lock makes a straggler fail cleanly (ErrCacheClosed) rather than
		// SIGBUS, but the drain keeps the ordering deterministic — no copy
		// racing the artifact file's removal. Idempotent: the window is
		// already canceled on every path into fail.
		window.CancelAndDrain(err)
		if closeErr := cache.Close(); closeErr != nil {
			sbxlogger.I(s).Warn(ctx, "closing memory export cache",
				zap.String("build_id", buildID.String()), zap.Error(closeErr))
		}
		// The SANDBOX is unharmed: a failed or cancelled capture loses only
		// THIS checkpoint's artifact. Unlike the rootfs seal (whose failure
		// can leave the writable cache missing blocks), memory has nothing to
		// fold back — the source of truth is live guest RAM, every export
		// parents the ORIGINAL template header, and inPlaceExportedDirty was
		// advanced at pause time (before this capture ran), so the next pause
		// or checkpoint re-exports every page this one rebaselined. The seal
		// signal therefore resolves SUCCESS: what waitForMemorySeal guards is
		// window OCCUPANCY (one window at a time; the memfd and WP armings
		// must be released before the next pause touches memory), and by this
		// point EndCoWExport has run and reporting is resumed on every path
		// through this function. Latching an error here would let the next
		// autopause kill a fully exportable sandbox.
		_ = sealDone.SetValue(struct{}{})
	}

	if err != nil {
		fail(err)

		return
	}

	diff, err := build.NewLocalDiffFromCache(build.GetDiffStoreKey(buildID.String(), build.Memfile), cache)
	if err != nil {
		fail(fmt.Errorf("materialize memory diff: %w", err))

		return
	}

	if err := diffPromise.SetValue(diff); err != nil {
		// The promise was already settled (pause aborted); drop the diff so
		// its cache file doesn't leak. The window is fully released, so the
		// occupancy signal resolves success (see fail above).
		_ = diff.Close()
		_ = sealDone.SetValue(struct{}{})

		return
	}
	_ = sealDone.SetValue(struct{}{})
	telemetry.ReportEvent(ctx, "memfile diff captured (deferred CoW)")
}

// resumeFreePageReportingBestEffort re-enables balloon free-page reporting
// after a CoW window paused it. A resume that never lands is a permanent
// degradation — the guest's reporting worker stays blocked on its one
// in-flight request and reported pages stay isolated from host reclamation
// for the sandbox's remaining lifetime. But most call sites run while the VM
// is PAUSED (and the pause-abort cleanup runs ahead of the ResumeInPlace
// cleanup), so blocking retries here would hand their full budget to the
// guest-frozen window. Split accordingly: ONE bounded inline attempt — the
// common transient failure costs nothing more — and on failure a detached
// retry loop that is fenced by the FPR pause generation, so a stray late
// resume cannot undo a NEWER window's pause. (If a fenced-out retry's final
// PATCH still crosses a new pause on the wire, the REMOVE tripwire cancels
// that checkpoint cleanly — belt and braces.) Failures are never propagated:
// the caller has resolved or is resolving the window either way. Every
// terminal outcome lands on the fpr_resume counter — outcome="abandoned" is
// the alertable signal for a leaked pause, which would otherwise only show
// as free_page_report_freed going quiet (indistinguishable from a guest that
// stopped freeing memory).
func (s *Sandbox) resumeFreePageReportingBestEffort(ctx context.Context) {
	runFPRResume(context.WithoutCancel(ctx), fprResumeDeps{
		resume:         s.process.ResumeFreePageReporting,
		fcExited:       s.process.Exit.Done(),
		pauseGen:       s.fprPauseGen.Load,
		attemptTimeout: 2 * time.Second,
		retryDelay:     500 * time.Millisecond,
		record: func(outcome string) {
			fprResumeCounter.Add(context.WithoutCancel(ctx), 1,
				metric.WithAttributes(attribute.String("outcome", outcome)))
		},
		logInfo: func(msg string, fields ...zap.Field) {
			sbxlogger.I(s).Info(ctx, msg, fields...)
		},
		logError: func(msg string, fields ...zap.Field) {
			sbxlogger.I(s).Error(ctx, msg, fields...)
		},
	})
}

// fprResumeDeps injects the seams runFPRResume needs so the retry/fence
// choreography is unit-testable without a live FC process (the pollFphDone
// pattern).
type fprResumeDeps struct {
	resume         func(ctx context.Context) error
	fcExited       <-chan struct{}
	pauseGen       func() uint64
	attemptTimeout time.Duration
	retryDelay     time.Duration
	record         func(outcome string)
	logInfo        func(msg string, fields ...zap.Field)
	logError       func(msg string, fields ...zap.Field)
}

// runFPRResume performs one bounded inline resume attempt and, on failure,
// hands the remaining retries to a detached goroutine fenced by the FPR
// pause generation. ctx must already be detached from request cancellation.
func runFPRResume(ctx context.Context, deps fprResumeDeps) {
	attemptCtx, cancel := context.WithTimeout(ctx, deps.attemptTimeout)
	err := deps.resume(attemptCtx)
	cancel()
	if err == nil {
		deps.record("inline")

		return
	}

	gen := deps.pauseGen()
	go func() {
		const attempts = 4

		retryErr := err
		for i := range attempts {
			select {
			case <-deps.fcExited:
				// FC is gone; the pause died with it.
				deps.record("fc_exited")

				return
			case <-time.After(deps.retryDelay):
			}
			if deps.pauseGen() != gen {
				// A newer window paused reporting again; its own exit paths
				// now own the resume.
				deps.record("fenced")

				return
			}

			attemptCtx, cancel := context.WithTimeout(ctx, deps.attemptTimeout)
			retryErr = deps.resume(attemptCtx)
			cancel()
			if retryErr == nil {
				deps.record("retry")
				deps.logInfo("free-page reporting resumed after retry", zap.Int("attempt", i+2))

				return
			}
		}

		deps.record("abandoned")
		deps.logError("resuming free-page reporting failed; reporting stays paused for this sandbox's lifetime",
			zap.Int("attempts", attempts+1), zap.Error(retryErr))
	}()
}

// waitForMemorySeal blocks until the most recent in-place background CoW
// memory capture of this sandbox has finished RELEASING the window — success
// or failure. Returns immediately if none has run. This wait is about window
// occupancy, not artifact outcome: a failed capture loses only its own
// checkpoint's diff (memory exports parent the original template and union
// inPlaceExportedDirty, so later exports stay complete), and the runner
// resolves the signal SUCCESS on every path that released the window. An
// error here therefore means the window's release itself went wrong — the
// one state where snapshotting on top would race a half-owned window.
func (s *Sandbox) waitForMemorySeal(ctx context.Context) error {
	s.memSealMu.Lock()
	done := s.memSealDone
	s.memSealMu.Unlock()

	if done == nil {
		return nil
	}

	_, err := done.WaitWithContext(ctx)

	return err
}

// FlushAndReadBalloonMetrics triggers an FC metrics flush and returns the
// updated cumulative virtio-balloon counters. Used by the FPH bench.
func (s *Sandbox) FlushAndReadBalloonMetrics(ctx context.Context) (fc.BalloonMetricsSnapshot, error) {
	return s.process.FlushAndReadBalloonMetrics(ctx)
}

// MemoryPrefetchData returns the ordered page fault data for prefetch mapping.
func (s *Sandbox) MemoryPrefetchData(ctx context.Context) (block.PrefetchData, error) {
	prefetchData, err := s.Resources.memory.PrefetchData(ctx)
	if err != nil {
		return block.PrefetchData{}, fmt.Errorf("failed to get prefetch data: %w", err)
	}

	return prefetchData, nil
}

func pauseProcessMemory(
	ctx context.Context,
	buildID uuid.UUID,
	originalHeader *header.Header,
	diffMetadata *header.DiffMetadata,
	cacheDir string,
	fc *fc.Process,
	memfd *block.Memfd,
	bgCopy bool,
	originalMemfile block.ReadonlyDevice,
	dedupBestEffort bool,
	dedupDirectIO bool,
	dedupBudget block.DedupBudget,
	dedupInflightServe bool,
	keepMemfdOpen bool,
) (d build.Diff, h *DiffHeader, provisionalHeader *header.Header, provisionalDiff build.Diff, provisionalSwapDone func(), e error) {
	ctx, span := tracer.Start(ctx, "process-memory")
	defer span.End()

	// Duration of the synchronous memory export+diff (memory pauses only; fs-only
	// pauses skip this). The async header dedup goroutine below outlives this span.
	start := time.Now()
	defer func() {
		processMemoryDurationHistogram.Record(ctx, time.Since(start).Milliseconds(),
			metric.WithAttributes(
				// in_place splits the cohorts: an in-place checkpoint (VM
				// resumes after) vs a destroy-path pause / resume-fresh flow.
				// deferred=false: this is the synchronous copy — the deferred
				// path records its own (setup-only) sample at its return, so
				// the two are separable after the fact. It reflects the path
				// actually taken, not the flag.
				attribute.Bool("in_place", keepMemfdOpen),
				attribute.Bool("deferred", false),
				attribute.Bool("success", e == nil),
			))
	}()

	memfileDiffPath := build.GenerateDiffCachePath(cacheDir, buildID.String(), build.Memfile)
	metaOut := utils.NewSetOnce[*header.DiffMetadata]()
	// ExportMemory owns memfd and closes it on all paths, EXCEPT when
	// keepMemfdOpen is set (in-place snapshot): then it borrows the fd and the
	// running VM keeps ownership.
	cache, err := fc.ExportMemory(
		ctx, diffMetadata.Dirty, memfileDiffPath, diffMetadata.BlockSize, memfd, bgCopy,
		originalMemfile, dedupBestEffort, dedupDirectIO, dedupBudget, diffMetadata.Empty, metaOut,
		dedupInflightServe, keepMemfdOpen,
	)
	if err != nil {
		return nil, nil, nil, nil, nil, fmt.Errorf("failed to export memory: %w", err)
	}

	diff, err := build.NewLocalDiffFromCache(
		build.GetDiffStoreKey(buildID.String(), build.Memfile),
		cache,
	)
	if err != nil {
		return nil, nil, nil, nil, nil, fmt.Errorf("failed to create local diff from cache: %w", errors.Join(err, cache.Close()))
	}

	// Provisional local header: while the deduped header is still
	// being computed, let a same-node resume serve dirty pages from the memfd via
	// a distinct provisional build id at identity offsets. Gated on the memfd
	// dedup path + the inflight-serve flag; best-effort (fall back to the deduped
	// header on any error). The upload always uses the deduped header below.
	// Skipped entirely when keepMemfdOpen: the running in-place VM owns the memfd,
	// so a provisional source serving from (and later releasing) it would
	// double-manage the live fd.
	if !keepMemfdOpen {
		provisionalHeader, provisionalDiff, provisionalSwapDone = buildProvisionalMemfile(ctx, cache, dedupInflightServe, originalMemfile, originalHeader, diffMetadata)
	}

	// Build the diff header on a goroutine so Pause returns without waiting
	// on memfd-dedup compare. ExportMemory resolves metaOut sync for every
	// other path, so Wait there is non-blocking; the goroutine is harmless.
	headerOut := utils.NewSetOnce[*header.Header]()
	go func() {
		setHeader := func(h *header.Header, err error) {
			if setErr := headerOut.SetResult(h, err); setErr != nil {
				logger.L().Warn(ctx, "set memfile diff header", zap.Error(setErr))
			}
		}
		meta, err := metaOut.Wait()
		if err != nil {
			setHeader(nil, err)

			return
		}
		// post == nil signals "no dedup ran" to the metric so it records
		// kind="none" with zero savings.
		post := meta
		if originalMemfile == nil {
			post = nil
		}
		recordSnapshotDedup(ctx, "memfile", diffMetadata, post, dedupBestEffort)
		setHeader(meta.ToDiffHeader(ctx, originalHeader, buildID))
	}()

	return diff, headerOut, provisionalHeader, provisionalDiff, provisionalSwapDone, nil
}

// buildProvisionalMemfile builds the provisional local header + its memfd-backed
// diff source, plus a swap-done callback the AddSnapshot swap goroutine invokes
// once it has swapped the deduped header in (it lets the dedup goroutine release
// the memfd). Returns (nil, nil, nil) — falling back to the deduped header —
// when the path doesn't apply or on any error, so it never blocks a pause. The
// provisional source is keyed by a fresh build id so a header swap to the
// deduped header (after dedup) is race-free (see MemfdIdentitySource).
func buildProvisionalMemfile(
	ctx context.Context,
	cache block.DiffSource,
	enabled bool,
	originalMemfile block.ReadonlyDevice,
	originalHeader *header.Header,
	diffMetadata *header.DiffMetadata,
) (*header.Header, build.Diff, func()) {
	dc, ok := cache.(*block.DedupedMemfdCache)
	if !ok {
		return nil, nil, nil
	}
	// From here dc != nil. On every path that declines to build a provisional
	// source, signal MarkSwapped so runDedup's inflight memfd-hold releases at
	// drain-time instead of waiting out the swap grace — nothing will serve from
	// the memfd, so there's no reason to hold it.
	if !enabled || originalMemfile == nil || originalHeader == nil || diffMetadata == nil {
		dc.MarkSwapped()

		return nil, nil, nil
	}

	provisionalBuildID := uuid.New()
	provisionalHeader, err := diffMetadata.ToProvisionalDiffHeader(ctx, originalHeader, provisionalBuildID)
	if err != nil {
		logger.L().Warn(ctx, "build provisional memfile header; using deduped header", zap.Error(err))
		dc.MarkSwapped()

		return nil, nil, nil
	}

	provisionalSource := block.NewMemfdIdentitySource(dc, int64(originalHeader.Metadata.Size))
	provisionalDiff, err := build.NewLocalDiffFromCache(build.GetDiffStoreKey(provisionalBuildID.String(), build.Memfile), provisionalSource)
	if err != nil {
		logger.L().Warn(ctx, "build provisional memfile diff; using deduped header", zap.Error(err))
		dc.MarkSwapped()

		return nil, nil, nil
	}

	return provisionalHeader, provisionalDiff, dc.MarkSwapped
}

func (s *Sandbox) processRootfsSnapshot(
	ctx context.Context,
	buildID uuid.UUID,
	originalHeader *header.Header,
	pauseOpts *pauseOptions,
	cleanup *Cleanup,
) (d build.Diff, h *header.Header, startSeal func(context.Context), e error) {
	ctx, span := tracer.Start(ctx, "process-rootfs")
	defer span.End()
	// Duration of the rootfs export+diff, split by fs_only (runs for both pause
	// kinds) so the fs-only pause latency can be decomposed into quiesce + rootfs.
	// This is the pause CRITICAL-PATH rootfs cost: the full export for the
	// synchronous path, but only the eject/setup for the deferred path — the
	// background reflink seal (runDeferredRootfsExport) is intentionally excluded.
	//
	// The `deferred` attribute records which of those two populations a sample
	// belongs to: without it the (much smaller) deferred setup-only timings would
	// be indistinguishable from full synchronous exports. It reflects the path
	// actually taken — the fall-through below flips it off when a provider can't
	// defer. `success` here is the critical-path outcome only; on the deferred
	// path the actual export success/failure is recorded by the seal metric
	// (rootfsSealDurationHistogram), so a deferred success=true is setup-only.
	start := time.Now()
	defer func() {
		processRootfsDurationHistogram.Record(ctx, time.Since(start).Milliseconds(),
			metric.WithAttributes(
				attribute.Bool("fs_only", pauseOpts.filesystemSnapshot),
				attribute.Bool("deferred", pauseOpts.deferRootfsExport),
				// in_place splits the cohorts: an in-place checkpoint (VM
				// resumes after) vs a destroy-path pause / resume-fresh flow.
				attribute.Bool("in_place", pauseOpts.maintainSandbox),
				attribute.Bool("success", e == nil),
			))
	}()

	// In-place checkpoint: the sandbox keeps running. With deferred export, swap a
	// fresh COW cache onto the live overlay and seal the frozen one in the
	// background (then fold it back); otherwise export in place synchronously.
	// Both keep the VM alive; the destroy-path branches below never run for it.
	if pauseOpts.maintainSandbox {
		if pauseOpts.deferRootfsExport {
			rootfsDiff, rootfsHeader, startSeal, err := s.setupInPlaceRootfsExport(ctx, buildID, originalHeader, cleanup)
			switch {
			case errors.Is(err, rootfs.ErrDeferredExportNotSupported):
				// Provider can't swap (e.g. DirectProvider); fall through to the
				// synchronous in-place export below.
				pauseOpts.deferRootfsExport = false
			case err != nil:
				return nil, nil, nil, fmt.Errorf("in-place rootfs export setup failed: %w", err)
			default:
				return rootfsDiff, rootfsHeader, startSeal, nil
			}
		}

		// Synchronous in-place export: reflink on the critical path but keep the VM
		// alive (closeHook nil -> ExportDiffInPlace).
		rootfsDiff, rootfsHeader, err := pauseProcessRootfs(
			ctx,
			buildID,
			originalHeader,
			&RootfsDiffCreator{rootfs: s.rootfs},
			s.config.DefaultCacheDir,
		)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("synchronous in-place rootfs export failed: %w", err)
		}
		cleanup.AddNoContext(ctx, rootfsDiff.Close)

		return rootfsDiff, rootfsHeader, nil, nil
	}

	if pauseOpts.deferRootfsExport {
		rootfsDiff, rootfsHeader, startSeal, err := s.setupDeferredRootfsExport(ctx, buildID, originalHeader, cleanup)
		switch {
		case errors.Is(err, rootfs.ErrDeferredExportNotSupported):
			// The provider (e.g. DirectProvider) can't defer; fall through to the
			// synchronous export below. Safe because PrepareExportDiff returns this
			// sentinel before ejecting/stopping anything.
			pauseOpts.deferRootfsExport = false
		case err != nil:
			return nil, nil, nil, fmt.Errorf("deferred rootfs export setup failed: %w", err)
		default:
			return rootfsDiff, rootfsHeader, startSeal, nil
		}
	}

	rootfsDiff, rootfsHeader, err := pauseProcessRootfs(
		ctx,
		buildID,
		originalHeader,
		&RootfsDiffCreator{
			rootfs:    s.rootfs,
			closeHook: s.Close,
		},
		s.config.DefaultCacheDir,
	)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("synchronous rootfs export failed: %w", err)
	}
	cleanup.AddNoContext(ctx, rootfsDiff.Close)

	return rootfsDiff, rootfsHeader, nil, nil
}

func pauseProcessRootfs(
	ctx context.Context,
	buildID uuid.UUID,
	originalHeader *header.Header,
	diffCreator DiffCreator,
	cacheDir string,
) (d build.Diff, h *header.Header, e error) {
	rootfsDiffFile, err := build.NewLocalDiffFile(cacheDir, buildID.String(), build.Rootfs)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create rootfs diff: %w", err)
	}

	rootfsDiffMetadata, err := diffCreator.process(ctx, rootfsDiffFile.File)
	if err != nil {
		err = errors.Join(err, rootfsDiffFile.Close())

		return nil, nil, fmt.Errorf("error creating diff: %w", err)
	}
	telemetry.ReportEvent(ctx, "exported rootfs")
	recordSnapshotDiff(ctx, "rootfs", rootfsDiffMetadata, originalHeader)

	rootfsDiff, err := rootfsDiffFile.CloseToDiff(int64(originalHeader.Metadata.BlockSize))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to convert rootfs diff file to local diff: %w", err)
	}
	telemetry.ReportEvent(ctx, "converted rootfs diff file to local diff")

	rootfsHeader, err := rootfsDiffMetadata.ToDiffHeader(ctx, originalHeader, buildID)
	if err != nil {
		err = errors.Join(err, rootfsDiff.Close())

		return nil, nil, fmt.Errorf("failed to create rootfs header: %w", err)
	}

	return rootfsDiff, rootfsHeader, nil
}

// setupDeferredRootfsExport ejects the writable cache and stops the sandbox
// (destroy path), then prepares the deferred rootfs diff + header from the frozen
// ejected cache. It returns a startSeal closure that reflinks the cache into the
// diff in the background, so the pause returns without paying the reflink stall.
// Only safe on the suspend path, where nothing reads the diff before the seal
// completes.
// rootfsSealPrep carries the artifacts of the setup steps shared by both
// deferred-seal flows (destroy-path eject and in-place swap). When empty is
// set, the frozen cache has no dirty blocks and only rootfsHeader is valid.
type rootfsSealPrep struct {
	diffMetadata *header.DiffMetadata
	rootfsHeader *header.Header
	blockSize    int64
	diffPromise  *utils.SetOnce[build.Diff]
	rootfsDiff   build.Diff
	empty        bool
}

// prepareRootfsSeal runs the setup shared by setupDeferredRootfsExport and
// setupInPlaceRootfsExport: read the frozen cache's diff metadata, emit the
// same rootfs size/ratio metrics the synchronous path does, build the diff
// header, detect the empty-diff case, and construct the deferred-diff
// plumbing. Disposing of the frozen cache on error is deliberately the
// caller's job — that is exactly where the two flows differ (Close for the
// ejected cache vs fold-back for the swapped one).
func (s *Sandbox) prepareRootfsSeal(
	ctx context.Context,
	sealCache *block.Cache,
	originalHeader *header.Header,
	buildID uuid.UUID,
) (*rootfsSealPrep, error) {
	diffMetadata, err := sealCache.DiffMetadata()
	if err != nil {
		return nil, fmt.Errorf("reading frozen cache metadata: %w", err)
	}
	recordSnapshotDiff(ctx, "rootfs", diffMetadata, originalHeader)

	rootfsHeader, err := diffMetadata.ToDiffHeader(ctx, originalHeader, buildID)
	if err != nil {
		return nil, fmt.Errorf("building rootfs diff header: %w", err)
	}

	prep := &rootfsSealPrep{diffMetadata: diffMetadata, rootfsHeader: rootfsHeader}

	// No dirty filesystem blocks: the seal would produce an empty diff, i.e. the
	// same *NoDiff the synchronous path returns. The caller returns NoDiff (and
	// skips the background seal — nothing to reflink) so AddSnapshot omits it
	// from the DiffStore and peer LookupDiff keeps returning ErrNotAvailable
	// rather than an entry whose Slice/Size yield NoDiffError.
	if diffMetadata.Dirty.IsEmpty() {
		prep.empty = true

		return prep, nil
	}

	prep.blockSize = int64(originalHeader.Metadata.BlockSize)
	prep.diffPromise = utils.NewSetOnce[build.Diff]()
	prep.rootfsDiff = build.NewDeferredDiff(build.GetDiffStoreKey(buildID.String(), build.Rootfs), prep.blockSize, prep.diffPromise)

	return prep, nil
}

// runRootfsSealCore is the timed reflink shared by both background seal
// runners: seal the frozen cache into the diff, resolve the promise, and
// record the seal histogram split by flow (in_place) and outcome. What happens
// to the frozen cache afterwards — Close for the destroy path, fold-back for
// in-place — stays with the caller.
func (s *Sandbox) runRootfsSealCore(
	ctx context.Context,
	sealCache *block.Cache,
	buildID uuid.UUID,
	blockSize int64,
	meta *header.DiffMetadata,
	diffPromise *utils.SetOnce[build.Diff],
	inPlace bool,
) error {
	start := time.Now()
	err := s.sealCacheToDiff(ctx, sealCache, buildID, blockSize, meta, diffPromise)
	rootfsSealDurationHistogram.Record(ctx, time.Since(start).Milliseconds(),
		metric.WithAttributes(
			attribute.Bool("in_place", inPlace),
			attribute.Bool("success", err == nil),
		))

	return err
}

func (s *Sandbox) setupDeferredRootfsExport(
	ctx context.Context,
	buildID uuid.UUID,
	originalHeader *header.Header,
	cleanup *Cleanup,
) (d build.Diff, h *header.Header, startSeal func(context.Context), e error) {
	sealCache, err := s.rootfs.PrepareExportDiff(ctx, s.Close)
	if err != nil {
		return nil, nil, nil, err
	}

	prep, err := s.prepareRootfsSeal(ctx, sealCache, originalHeader, buildID)
	if err != nil {
		return nil, nil, nil, errors.Join(err, sealCache.Close())
	}

	// Empty diff: close the ejected cache now since no seal will own it.
	if prep.empty {
		if err := sealCache.Close(); err != nil {
			return nil, nil, nil, fmt.Errorf("closing empty ejected cache: %w", err)
		}

		return &build.NoDiff{}, prep.rootfsHeader, func(context.Context) {}, nil
	}

	diffPromise := prep.diffPromise
	rootfsDiff := prep.rootfsDiff

	// The ejected cache and the deferred diff's promise are both owned by the
	// background seal once it starts. If Pause aborts before startSeal runs (e.g.
	// m.ToFile fails), the goroutine never runs, so on that path we close the
	// cache here (else its mmap + backing file leak) and poison the promise (else
	// the deferred diff's Close and any waiter block forever on a seal that will
	// never resolve). Both are guarded by `started`: once the seal owns them, this
	// cleanup is a no-op, so we never double-close the cache nor race a spurious
	// SetError against the seal's SetValue (which would drop the sealed diff).
	// atomic because startSeal writes it on the pause goroutine and these cleanups
	// read it on the cleanup goroutine.
	var started atomic.Bool
	cleanup.Add(ctx, func(context.Context) error {
		if started.Load() {
			return nil
		}

		return sealCache.Close()
	})

	// LIFO: the abort resolver (added last) runs before rootfsDiff.Close, so on the
	// error path the deferred diff's Close never blocks on a seal that won't run.
	cleanup.AddNoContext(ctx, rootfsDiff.Close)
	cleanup.Add(ctx, func(context.Context) error {
		if started.Load() {
			return nil
		}
		_ = diffPromise.SetError(errors.New("pause aborted before deferred rootfs export ran"))

		return nil
	})

	startSeal = func(sealCtx context.Context) {
		started.Store(true)
		go s.runDeferredRootfsExport(sealCtx, sealCache, buildID, prep.blockSize, prep.diffMetadata, diffPromise)
	}

	return rootfsDiff, prep.rootfsHeader, startSeal, nil
}

// runDeferredRootfsExport reflinks the ejected cache into the rootfs diff and
// closes the cache. The sandbox is already stopped, so there is nothing to fold
// or serialize; the upload waits on the deferred diff, gating shutdown via the
// server's upload WaitGroup.
func (s *Sandbox) runDeferredRootfsExport(
	ctx context.Context,
	sealCache *block.Cache,
	buildID uuid.UUID,
	blockSize int64,
	meta *header.DiffMetadata,
	diffPromise *utils.SetOnce[build.Diff],
) {
	ctx, span := tracer.Start(ctx, "deferred-rootfs-export")
	defer span.End()

	// The seal core records the background reflink latency separately from the
	// critical-path process_rootfs.duration, so the deferred export's off-path
	// cost stays visible.
	err := s.runRootfsSealCore(ctx, sealCache, buildID, blockSize, meta, diffPromise, false)
	if err != nil {
		s.log().Error(ctx, "deferred rootfs export failed", zap.Error(err))
	} else {
		telemetry.ReportEvent(ctx, "rootfs diff sealed (deferred)")
	}

	// The sandbox is torn down; the ejected cache is ours to close regardless of
	// the export outcome.
	if err := sealCache.Close(); err != nil {
		s.log().Warn(ctx, "closing ejected rootfs cache", zap.Error(err))
	}
}

// sealCacheToDiff reflinks the frozen cache into a fresh local diff file and
// resolves diffPromise with the materialized diff.
func (s *Sandbox) sealCacheToDiff(
	ctx context.Context,
	sealCache *block.Cache,
	buildID uuid.UUID,
	blockSize int64,
	meta *header.DiffMetadata,
	diffPromise *utils.SetOnce[build.Diff],
) error {
	diffFile, err := build.NewLocalDiffFile(s.config.DefaultCacheDir, buildID.String(), build.Rootfs)
	if err != nil {
		return s.failRootfsSeal(diffPromise, fmt.Errorf("create rootfs diff file: %w", err))
	}

	// Export using the metadata captured at setup so the sealed data matches the
	// header built from the same bitmap read (rather than re-reading the tracker).
	if _, err := sealCache.ExportToDiffWithMetadata(ctx, diffFile.File, meta); err != nil {
		return s.failRootfsSeal(diffPromise, errors.Join(fmt.Errorf("export rootfs diff: %w", err), diffFile.Close()))
	}
	telemetry.ReportEvent(ctx, "exported rootfs")

	diff, err := diffFile.CloseToDiff(blockSize)
	if err != nil {
		return s.failRootfsSeal(diffPromise, fmt.Errorf("materialize rootfs diff: %w", err))
	}

	if err := diffPromise.SetValue(diff); err != nil {
		// The promise was already settled (pause aborted); drop the diff so its
		// cache file doesn't leak.
		return errors.Join(err, diff.Close())
	}

	return nil
}

// failRootfsSeal settles the deferred diff with err and returns it.
func (s *Sandbox) failRootfsSeal(diffPromise *utils.SetOnce[build.Diff], err error) error {
	// Tag the failure with ErrDeferredSealFailed so the upload retry loop can tell
	// this permanent, one-shot seal failure apart from transient upload errors and
	// stop retrying (the seal never re-runs, so the diff can never materialize).
	sealErr := fmt.Errorf("%w: %w", build.ErrDeferredSealFailed, err)
	_ = diffPromise.SetError(sealErr)

	return sealErr
}

// EnsurePausable surfaces any latched in-place seal failure before the pause
// machinery spins up: a latched error means the writable COW cache is missing
// blocks and no valid snapshot can ever be produced for this sandbox again.
// The caller uses it to fail fast WITH the real cause — it cannot use it to
// spare the sandbox, because the API pause chain deletes routing and removes
// the store record regardless of this RPC's result (and the sandbox state
// machine has no Pausing→Running edge), so a "refused" pause just leaves a
// live VM for the orphan reconciler to kill ~20s later, misattributed. It
// intentionally does NOT wait for a healthy in-flight seal — Pause itself
// does that.
func (s *Sandbox) EnsurePausable() error {
	s.rootfsSealMu.Lock()
	done := s.rootfsSealDone
	s.rootfsSealMu.Unlock()

	if done != nil {
		// Result is non-blocking: NotSetError means the seal is still healthy
		// and in flight, which is Pause's job to wait out, not a reason to
		// refuse.
		if _, err := done.Result(); err != nil && !errors.Is(err, utils.NotSetError{}) {
			return fmt.Errorf("sandbox cannot be paused: %w", err)
		}
	}

	// Same check for the deferred MEMORY seal — defensively: no current path
	// latches an error here (a failed or cancelled CoW window resolves the
	// signal SUCCESS once the window is released, because the artifact loss
	// is confined to that checkpoint — see runDeferredMemoryExport). A
	// latched error would mean the window RELEASE itself failed, i.e. the
	// memfd/WP armings may still be half-owned, which is the one memory
	// state a pause must not export on top of.
	s.memSealMu.Lock()
	memDone := s.memSealDone
	s.memSealMu.Unlock()

	if memDone != nil {
		if _, err := memDone.Result(); err != nil && !errors.Is(err, utils.NotSetError{}) {
			return fmt.Errorf("sandbox cannot be paused: %w", err)
		}
	}

	return nil
}

// SnapshotAdmissionOutcome labels how a snapshot-admission pre-flight ended.
type SnapshotAdmissionOutcome string

const (
	// SnapshotAdmissionReady: admitted without waiting.
	SnapshotAdmissionReady SnapshotAdmissionOutcome = "ready"
	// SnapshotAdmissionReadyAfterWait: the parent header became durable inside
	// the grace wait.
	SnapshotAdmissionReadyAfterWait SnapshotAdmissionOutcome = "ready_after_wait"
	// SnapshotAdmissionRefused: still deduplicating when the grace elapsed.
	SnapshotAdmissionRefused SnapshotAdmissionOutcome = "refused"
	// SnapshotAdmissionLatchedError: a latched seal failure means no valid
	// snapshot can ever be produced; not retryable.
	SnapshotAdmissionLatchedError SnapshotAdmissionOutcome = "latched_error"
)

// ErrSnapshotAdmissionPending marks a retryable admission refusal: the parent
// memfile header was still deduplicating when the grace elapsed.
var ErrSnapshotAdmissionPending = errors.New("parent memfile header is still deduplicating")

// AwaitSnapshotAdmission is the pre-destructive snapshot-admission pre-flight:
// "can this sandbox produce a valid snapshot right now?". It folds the
// EnsurePausable latched-error checks together with the durable-parent
// readiness check, so the Pause/Checkpoint handlers ask once, BEFORE any
// destructive step. Readiness is monotonic per resumed sandbox (the
// provisional->durable swap never reverts), so a positive answer cannot be
// invalidated between this check and the snapshot.
//
// memorySnapshot=false (a filesystem-only pause) skips the durable-parent
// wait entirely — an fs-only snapshot has no memory parent, so it can never
// be refused here; only the latched checks apply.
//
// The wait runs on its own timer, never the RPC deadline. Returns:
//   - (Ready|ReadyAfterWait, waited, nil): admitted, proceed.
//   - (Refused, waited, ErrSnapshotAdmissionPending): refuse retryably.
//   - (LatchedError, waited, err): permanently unpersistable; the caller's
//     existing terminal handling applies.
//   - ("", waited, ctx.Err()): the caller's context ended mid-wait; nothing
//     was decided and the sandbox is untouched.
func (s *Sandbox) AwaitSnapshotAdmission(ctx context.Context, grace time.Duration, memorySnapshot bool) (SnapshotAdmissionOutcome, time.Duration, error) {
	ctx, span := tracer.Start(ctx, "snapshot-admission")
	defer span.End()

	outcome, waited, err := s.awaitSnapshotAdmission(ctx, grace, memorySnapshot)
	span.SetAttributes(
		attribute.String("outcome", string(outcome)),
		attribute.Int64("waited_ms", waited.Milliseconds()),
	)

	return outcome, waited, err
}

func (s *Sandbox) awaitSnapshotAdmission(ctx context.Context, grace time.Duration, memorySnapshot bool) (SnapshotAdmissionOutcome, time.Duration, error) {
	if err := s.EnsurePausable(); err != nil {
		return SnapshotAdmissionLatchedError, 0, err
	}

	if !memorySnapshot {
		return SnapshotAdmissionReady, 0, nil
	}

	memfile, err := s.Template.Memfile(ctx)
	if err != nil {
		// No memory parent to wait on (e.g. a filesystem-only boot); the
		// snapshot path tolerates a missing memfile.
		return SnapshotAdmissionReady, 0, nil
	}

	dh, ok := memfile.(interface {
		DurableHeaderNow() (*header.Header, bool)
		DurableHeader(ctx context.Context) (*header.Header, error)
	})
	if !ok {
		return SnapshotAdmissionReady, 0, nil
	}

	if _, ready := dh.DurableHeaderNow(); ready {
		return SnapshotAdmissionReady, 0, nil
	}

	// grace <= 0 yields an already-expired waitCtx: an instant probe. It must
	// still go through DurableHeader, which returns an already-resolved error
	// even under an expired context — a failed dedup classifies as latched,
	// never as retryable-pending.
	waitCtx, cancel := context.WithTimeout(ctx, grace)
	defer cancel()

	start := time.Now()
	_, err = dh.DurableHeader(waitCtx)
	waited := time.Since(start)

	switch {
	case err == nil:
		return SnapshotAdmissionReadyAfterWait, waited, nil
	case ctx.Err() != nil:
		return "", waited, ctx.Err()
	//nolint:errorlint // identity on purpose: timer expiry returns the raw
	// sentinel, while a permanent dedup failure stored in the future WRAPS a
	// context error — errors.Is would misread it as a pending swap.
	case err == context.DeadlineExceeded && waitCtx.Err() != nil:
		return SnapshotAdmissionRefused, waited, ErrSnapshotAdmissionPending
	default:
		// The durable future resolved with an error: the deduped header will
		// never exist, so no valid memory snapshot can ever be produced —
		// permanent, like a latched seal failure, not retryable.
		return SnapshotAdmissionLatchedError, waited, err
	}
}

// bestEffortEnvdReinit re-runs the envd /init a real resume makes, after an
// in-place resume (success or error-cleanup path). The FC-paused window
// stopped the guest's clocks; envd steps CLOCK_REALTIME from /init's host
// timestamp when it lags by >50ms — less than a single checkpoint's pause
// window — so without this every checkpoint leaves the guest's wall clock
// behind by the paused duration and repeated checkpoints accumulate the
// drift. Best-effort: it is the same /init every resume already makes against
// a live envd, and a failure only leaves the clock lagging until the next
// resume. WithoutCancel so a dying request ctx can't skip it.
//
// The episode budget is the envd-timeout flag (~10s) — the same bound a real
// resume gives WaitForEnvd — NOT EnvdInitRequestTimeout: that 50ms value is
// the PER-ATTEMPT deadline inside initEnvd's retry loop, calibrated for
// fail-fast-and-retry, and using it as the total would allow exactly one
// attempt against a guest that is busy working through its post-freeze
// backlog. Callers run this on a goroutine: a sick envd must cost the
// checkpoint nothing, and a missed sync only lasts until the next /init.
func (s *Sandbox) bestEffortEnvdReinit(ctx context.Context) {
	budget := time.Duration(s.featureFlags.IntFlag(ctx, featureflags.EnvdTimeoutMilliseconds)) * time.Millisecond
	initCtx, cancel := context.WithCancelCause(context.WithoutCancel(ctx))
	defer cancel(nil)

	// The same race WaitForEnvd runs around this exact retry loop: bound it on
	// sandbox LIVENESS, not just time. Detached from the request, a re-init
	// that only times out would outlive a sandbox killed mid-budget — and
	// outlive the network pool's 3s slot drain, POSTing this sandbox's /init
	// body (access token, env vars, CA bundle) at whichever tenant is handed
	// the recycled slot IP next.
	go func() {
		select {
		case <-time.After(budget):
			cancel(errors.New("envd re-init budget exceeded"))
		case <-initCtx.Done():
			return
		case <-s.process.Exit.Done():
			// Exit.Error() is nil for every orchestrator-initiated teardown
			// (SIGTERM/SIGKILL both store nil); a nil %w renders as
			// %!w(<nil>), garbling the dominant cause string.
			if exitErr := s.process.Exit.Error(); exitErr != nil {
				cancel(fmt.Errorf("%w: %w", ErrFcProcessExited, exitErr))
			} else {
				cancel(ErrFcProcessExited)
			}
		}
	}()

	if err := s.initEnvd(initCtx, StartTypeResume, false); err != nil {
		s.log().Warn(ctx, "envd re-init after in-place resume failed (guest clock may lag)", zap.Error(err))
	}
}

// waitForRootfsSeal blocks until the most recent in-place background rootfs seal
// of this sandbox has completed (its fold made the writable COW cache whole
// again). Returns immediately if none has run. A failed prior seal surfaces its
// error so the caller aborts rather than exporting an incomplete writable cache.
func (s *Sandbox) waitForRootfsSeal(ctx context.Context) error {
	s.rootfsSealMu.Lock()
	done := s.rootfsSealDone
	s.rootfsSealMu.Unlock()

	if done == nil {
		return nil
	}

	_, err := done.WaitWithContext(ctx)

	return err
}

// foldAndCloseSeal folds the just-swapped sealing cache back into the live
// writable cache and closes it, undoing a SwapForBackgroundSeal. Used on the
// setup error / empty-diff paths (VM still paused) so the writable cache stays a
// complete diff and the sealing slot is freed.
func (s *Sandbox) foldAndCloseSeal(ctx context.Context, sealCache *block.Cache) error {
	detached, err := s.rootfs.FoldSealed(ctx)
	if err != nil {
		return fmt.Errorf("fold sealed cache: %w", err)
	}
	if detached != nil {
		// The fold has already done everything correctness depends on: the
		// writable cache is a complete diff and the sealing slot is free.
		// Close only unmaps and unlinks a file nothing reads any more, so a
		// failure here is a leaked file — not an incomplete cache — and must
		// not feed the callers' latch. Mirrors the success-path seal close.
		if closeErr := detached.Close(); closeErr != nil {
			s.log().Warn(ctx, "closing folded seal cache failed (leaked file)", zap.Error(closeErr))
		}

		return nil
	}

	// Nothing was sealing: either the swap was already undone, or a teardown
	// beat this cleanup to it — on a ResumeInPlace failure the sandbox Close
	// tears down the overlay (closing the sealing cache) BEFORE the Pause
	// cleanup stack runs. Closing what we hold is then a double-close;
	// tolerate it rather than misreport the fold-back as failed.
	if err := sealCache.Close(); err != nil {
		if _, ok := errors.AsType[*block.CacheClosedError](err); ok {
			return nil
		}

		return err
	}

	return nil
}

// failSealSetup unwinds a failed in-place seal setup: it folds the
// just-swapped cache back so the writable cache stays a complete diff, and —
// when the fold-back ITSELF fails — latches the state into rootfsSealDone.
// These setup failures happen BEFORE rootfsSealDone is assigned, so without
// the latch a stuck sealing slot would be invisible: waitForRootfsSeal would
// succeed and a later destroy-path export would silently drop every block
// still only in the sealing layer, while the next in-place swap failed
// forever on the occupied slot. With the latch, every later pause aborts
// loudly instead (the sandbox itself survives via Pause's resume-on-error).
func (s *Sandbox) failSealSetup(ctx context.Context, sealCache *block.Cache, cause error) error {
	foldErr := s.foldAndCloseSeal(ctx, sealCache)
	if foldErr == nil {
		return cause
	}

	latched := utils.NewSetOnce[struct{}]()
	_ = latched.SetError(fmt.Errorf("%w: seal setup failed and the fold-back failed too: %w",
		build.ErrDeferredSealFailed, errors.Join(cause, foldErr)))
	s.rootfsSealMu.Lock()
	s.rootfsSealDone = latched
	s.rootfsSealMu.Unlock()

	return errors.Join(cause, foldErr)
}

// setupInPlaceRootfsExport swaps a fresh COW cache onto the live overlay and
// prepares the deferred rootfs diff + header from the now-frozen previous cache,
// all synchronously (the VM is paused). It returns a startSeal closure the caller
// invokes AFTER the guest has resumed in place, which reflinks the frozen cache
// off the critical path and folds it back into the writable cache. Returns
// rootfs.ErrDeferredExportNotSupported (via SwapForBackgroundSeal) when the
// provider can't swap, so the caller falls back to a synchronous in-place export.
func (s *Sandbox) setupInPlaceRootfsExport(
	ctx context.Context,
	buildID uuid.UUID,
	originalHeader *header.Header,
	cleanup *Cleanup,
) (d build.Diff, h *header.Header, startSeal func(context.Context), e error) {
	sealCache, err := s.rootfs.SwapForBackgroundSeal(ctx)
	if err != nil {
		return nil, nil, nil, err
	}
	// From here the overlay is swapped: any failure must fold the frozen cache
	// back so the writable cache stays complete (the caller then aborts).

	prep, err := s.prepareRootfsSeal(ctx, sealCache, originalHeader, buildID)
	if err != nil {
		return nil, nil, nil, s.failSealSetup(ctx, sealCache, err)
	}

	sealDone := utils.NewSetOnce[struct{}]()
	s.rootfsSealMu.Lock()
	s.rootfsSealDone = sealDone
	s.rootfsSealMu.Unlock()

	// Empty dirty set: nothing to seal. Fold the (empty) swapped cache back now,
	// resolve the seal signal, and return NoDiff (matching the synchronous path).
	if prep.empty {
		if err := s.foldAndCloseSeal(ctx, sealCache); err != nil {
			_ = sealDone.SetError(err)

			return nil, nil, nil, fmt.Errorf("folding empty swapped cache: %w", err)
		}
		_ = sealDone.SetValue(struct{}{})

		return &build.NoDiff{}, prep.rootfsHeader, func(context.Context) {}, nil
	}

	diffPromise := prep.diffPromise
	rootfsDiff := prep.rootfsDiff

	// If Pause aborts before startSeal runs (e.g. m.ToFile or ResumeInPlace fails),
	// the seal goroutine never runs. Guarded by `started`: settle the deferred
	// diff's promise so nothing blocks forever, and FOLD the frozen cache back
	// into the writable one. Leaving it attached is not an option: under
	// maintainSandbox a failed pause resumes the sandbox and it KEEPS RUNNING,
	// so an occupied sealing slot plus an errored seal signal would fail every
	// later in-place checkpoint of this sandbox (waitForRootfsSeal surfaces the
	// error; SwapCache refuses a second swap). After a successful fold-back the
	// writable cache is a complete diff again — only the aborted checkpoint's
	// artifact failed — so the seal signal resolves SUCCESS; a failed fold-back
	// keeps it errored, correctly poisoning exports of a genuinely incomplete
	// cache. The fold is safe with the guest running: FillMissingFrom only
	// fills blocks the writable cache lacks, so concurrent guest writes win.
	var started atomic.Bool
	cleanup.AddNoContext(ctx, rootfsDiff.Close)
	cleanup.Add(ctx, func(context.Context) error {
		if started.Load() {
			return nil
		}
		abortErr := errors.New("pause aborted before in-place rootfs export ran")
		_ = diffPromise.SetError(abortErr)
		if foldErr := s.foldAndCloseSeal(ctx, sealCache); foldErr != nil {
			_ = sealDone.SetError(fmt.Errorf("%w; folding the seal back also failed: %w", abortErr, foldErr))

			return fmt.Errorf("aborted in-place rootfs export fold-back: %w", foldErr)
		}
		_ = sealDone.SetValue(struct{}{})

		return nil
	})

	startSeal = func(sealCtx context.Context) {
		started.Store(true)
		go s.runInPlaceRootfsExport(sealCtx, sealCache, buildID, prep.blockSize, prep.diffMetadata, diffPromise, sealDone)
	}

	return rootfsDiff, prep.rootfsHeader, startSeal, nil
}

// runInPlaceRootfsExport reflinks the frozen sealing cache into the rootfs diff,
// resolves the deferred diff, then folds the sealing cache back into the live
// writable cache and releases it, freeing the sealing slot for the next
// checkpoint. It runs on its own goroutine after the guest has resumed; the
// upload waits on the deferred diff, so the server's upload WaitGroup transitively
// gates graceful shutdown on this seal.
func (s *Sandbox) runInPlaceRootfsExport(
	ctx context.Context,
	sealCache *block.Cache,
	buildID uuid.UUID,
	blockSize int64,
	meta *header.DiffMetadata,
	diffPromise *utils.SetOnce[build.Diff],
	sealDone *utils.SetOnce[struct{}],
) {
	ctx, span := tracer.Start(ctx, "in-place-rootfs-export")
	defer span.End()

	err := s.runRootfsSealCore(ctx, sealCache, buildID, blockSize, meta, diffPromise, true)
	if err != nil {
		s.log().Error(ctx, "in-place rootfs export failed", zap.Error(err))
		// The checkpoint's artifact is lost (diffPromise already carries
		// ErrDeferredSealFailed), but the SANDBOX must stay serviceable:
		// sealDone is a write-once field on the long-lived Sandbox, and an
		// error latched here would fail waitForRootfsSeal on every later
		// checkpoint AND every later pause — where Server.Pause has already
		// armed its deferred stop, so the first autopause would destroy the
		// sandbox with nothing persisted. The frozen cache is intact (the
		// failed reflink never closes it), so recover exactly like the
		// abort path: fold it back into the writable cache and resolve the
		// seal signal SUCCESS; latch the error only if the fold-back itself
		// fails, which is the genuinely unrecoverable-cache case.
		if foldErr := s.foldAndCloseSeal(ctx, sealCache); foldErr != nil {
			s.log().Error(ctx, "in-place rootfs export fold-back failed", zap.Error(foldErr))
			_ = sealDone.SetError(fmt.Errorf("%w; folding the seal back also failed: %w", err, foldErr))

			return
		}
		_ = sealDone.SetValue(struct{}{})

		return
	}
	telemetry.ReportEvent(ctx, "rootfs diff sealed (in-place)")

	// Fold the sealed cache into the live writable cache so it becomes a complete
	// diff again and the sealing slot frees for the next checkpoint.
	detached, err := s.rootfs.FoldSealed(ctx)
	if err != nil {
		s.log().Error(ctx, "folding sealed rootfs cache failed", zap.Error(err))
		_ = sealDone.SetError(fmt.Errorf("fold sealed rootfs cache: %w", err))

		return
	}
	if detached != nil {
		if closeErr := detached.Close(); closeErr != nil {
			s.log().Warn(ctx, "closing folded rootfs cache", zap.Error(closeErr))
		}
	}

	telemetry.ReportEvent(ctx, "rootfs seal folded")
	_ = sealDone.SetValue(struct{}{})
}

// createCgroup creates a cgroup for sandbox resource accounting.
// The caller is responsible for registering cleanup to remove the cgroup.
//
// Returns the CgroupHandle and the cgroup directory FD to pass to the
// Firecracker process or (nil, cgroup.NoCgroupFD) on error.
func createCgroup(ctx context.Context, cgroupManager cgroup.Manager, cgroupName string) (*cgroup.CgroupHandle, int) {
	ctx, span := tracer.Start(ctx, "sandbox-create-cgroup", trace.WithAttributes(
		attribute.String("cgroup_name", cgroupName),
	))
	defer span.End()

	handle, err := cgroupManager.Create(ctx, cgroupName)
	if err != nil {
		logger.L().Warn(ctx, "failed to create cgroup, continuing without cgroup accounting",
			zap.String("cgroup_name", cgroupName),
			zap.Error(err))

		telemetry.ReportEvent(ctx, "cgroup creation failed, continuing without accounting")

		return nil, cgroup.NoCgroupFD
	}

	return handle, handle.GetFD()
}

func getNetworkSlot(
	ctx context.Context,
	networkPool network.PoolInterface,
	cleanup *Cleanup,
	networkConfig *orchestrator.SandboxNetworkConfig,
	networkReleased network.ReleaseNotify,
	egressClass sandboxtypes.EgressClass,
) *utils.Promise[*network.Slot] {
	return utils.NewPromise(func() (*network.Slot, error) {
		ctx, span := tracer.Start(ctx, "get network-slot")
		defer span.End()

		slot, err := networkPool.Get(ctx, networkConfig, egressClass)
		if err != nil {
			return nil, fmt.Errorf("failed to get network slot: %w", err)
		}

		cleanup.Add(ctx, func(ctx context.Context) error {
			ctx, span := tracer.Start(ctx, "clean network-slot")
			defer span.End()

			// Async so sandbox cleanup doesn't block on the return delay or
			// network teardown; the pool's Close waits for in-flight returns.
			return networkPool.ReturnAsync(ctx, slot, networkReleased, network.ReturnDelay)
		})

		return slot, nil
	})
}

func serveMemory(
	ctx context.Context,
	cleanup *Cleanup,
	fcUffd *uffd.Uffd,
) error {
	ctx, span := tracer.Start(ctx, "serve-memory")
	defer span.End()

	telemetry.ReportEvent(ctx, "created uffd")

	if err := fcUffd.Start(ctx); err != nil {
		return fmt.Errorf("failed to start uffd: %w", err)
	}

	telemetry.ReportEvent(ctx, "started uffd")

	cleanup.Add(ctx, func(ctx context.Context) error {
		_, span := tracer.Start(ctx, "uffd-stop")
		defer span.End()

		if err := fcUffd.Stop(); err != nil {
			return fmt.Errorf("failed to stop uffd: %w", err)
		}

		return nil
	})

	return nil
}

func (s *Sandbox) WaitForEnvd(
	ctx context.Context,
	startType StartType,
	timeout time.Duration,
) (e error) {
	start := time.Now()
	ctx, span := tracer.Start(ctx, "sandbox-wait-for-start")
	defer span.End()

	// Record the per-start KPIs, the envd-init counter and protection histogram,
	// and StartedAt only on the FIRST WaitForEnvd for this handler (see
	// startupRecorded). A later call — the
	// post-upgrade readiness re-check, or the envd-binary swap during a template
	// build — re-runs /init to re-capture state but must not re-record.
	firstStart := s.startupRecorded.CompareAndSwap(false, true)

	defer func() {
		if !firstStart {
			return
		}

		// A throwaway (the pause-resume prefetch harvest) is warm by construction
		// and must not pollute the customer resume KPIs (envd-init duration,
		// startup pages/source-pages — the consume-side payoff signals) or even be
		// distinguishable in them, so it records none of these. It is otherwise
		// kept out of Prometheus (registration-skip); the harvest's own metrics
		// cover its timing/size.
		if !s.skipStartupMetrics {
			duration := time.Since(start).Milliseconds()
			waitForEnvdDurationHistogram.Record(ctx, duration,
				metric.WithAttributes(s.waitForEnvdDurationAttrs(startType, e)...))

			// The demand-fault working set the guest needed to reach this point.
			// ServeStats() is cumulative since resume, so at this instant it equals
			// the startup counts. Recorded for both outcomes (success label) so
			// slow/failed starts can be correlated with page volume.
			stats := s.memory.ServeStats()
			startupAttrs := metric.WithAttributes(
				attribute.String("start_type", string(startType)),
				attribute.Bool("success", e == nil),
			)
			uffdStartupPagesHistogram.Record(ctx, stats.Pages, startupAttrs)
			uffdStartupSourcePagesHistogram.Record(ctx, stats.SourcePages, startupAttrs)
			uffdStartupBytesHistogram.Record(ctx, stats.Bytes, startupAttrs)
		}

		if e != nil {
			return
		}

		// Update the sandbox as started now
		s.SetStartedAt(time.Now())
	}()
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)

	go func() {
		select {
		// Ensure the syncing takes at most timeout seconds.
		case <-time.After(timeout):
			cancel(ErrWaitForEnvdTimeout)
		case <-ctx.Done():
			return
		case <-s.process.Exit.Done():
			// Exit.Error() is nil for orchestrator-initiated teardowns; a nil
			// %w renders as %!w(<nil>).
			if exitErr := s.process.Exit.Error(); exitErr != nil {
				cancel(fmt.Errorf("%w: %w", ErrFcProcessExited, exitErr))
			} else {
				cancel(ErrFcProcessExited)
			}
		}
	}()

	if err := s.initEnvd(ctx, startType, firstStart); err != nil {
		return fmt.Errorf("failed to init new envd: %w", err)
	}

	telemetry.ReportEvent(ctx, fmt.Sprintf("[sandbox %s]: initialized new envd", s.Metadata.Runtime.SandboxID))

	return nil
}

// waitForEnvdDurationAttrs is the attribute set the envd-init duration histogram is
// recorded with, for a start of startType that ended with e.
func (s *Sandbox) waitForEnvdDurationAttrs(startType StartType, e error) []attribute.KeyValue {
	attrs := []attribute.KeyValue{
		telemetry.WithEnvdVersion(s.Config.Envd.Version),
		attribute.Int64("timeout_ms", s.internalConfig.EnvdInitRequestTimeout.Milliseconds()),
		// success is kept for backward compatibility until consumers move to exit_type.
		attribute.Bool("success", e == nil),
		attribute.String("start_type", string(startType)),
		attribute.String("exit_type", string(classifyEnvdInitExit(e))),
	}

	return append(attrs, s.envdProtectionAttrs()...)
}

func releaseCgroupFD(ctx context.Context, cgroupHandle *cgroup.CgroupHandle, sandboxID string) {
	if releaseErr := cgroupHandle.ReleaseCgroupFD(); releaseErr != nil {
		logger.L().Warn(ctx, "failed to release cgroup directory FD",
			logger.WithSandboxID(sandboxID),
			zap.Error(releaseErr))
	}
}

func (f *Factory) GetEnvdInitRequestTimeout(ctx context.Context) time.Duration {
	envdInitRequestTimeoutMs := f.featureFlags.IntFlag(ctx, featureflags.EnvdInitTimeoutMilliseconds)

	return time.Duration(envdInitRequestTimeoutMs) * time.Millisecond
}

func (f *Factory) GetEnvdTimeout(ctx context.Context) time.Duration {
	envdTimeoutMs := f.featureFlags.IntFlag(ctx, featureflags.EnvdTimeoutMilliseconds)

	return time.Duration(envdTimeoutMs) * time.Millisecond
}
