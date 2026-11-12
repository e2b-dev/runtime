//go:build linux

package server

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"net/http"
	"slices"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/jellydator/ttlcache/v3"
	"github.com/launchdarkly/go-sdk-common/v3/ldcontext"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/build"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/envdbin"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/fc"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/guest"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/rootfs"
	sbxtemplate "github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/template"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/uffd/userfaultfd"
	buildenvd "github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/core/envd"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/metadata"
	"github.com/e2b-dev/infra/packages/shared/pkg/events"
	"github.com/e2b-dev/infra/packages/shared/pkg/fcversion"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	orchestratorinfo "github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator-info"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	sbxlogger "github.com/e2b-dev/infra/packages/shared/pkg/logger/sandbox"
	"github.com/e2b-dev/infra/packages/shared/pkg/retry"
	catalog "github.com/e2b-dev/infra/packages/shared/pkg/sandbox-catalog"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxtypes"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

var tracer = otel.Tracer("github.com/e2b-dev/infra/packages/orchestrator/pkg/server")

const (
	requestTimeout = 60 * time.Second
	// acquireTimeout is the max time to wait for a semaphore for resuming sandboxes snapshot.
	acquireTimeout = 15 * time.Second

	// uploadTimeout is the max time allowed for a single upload attempt to
	// remote storage. The overall retry window is uploadTotalBudget.
	uploadTimeout = 20 * time.Minute
	// checkpointSnapshotTimeout bounds the snapshot phase of a resume-fresh
	// checkpoint (pause, memory seal, synchronous export), which runs
	// detached from the caller because the old VM is lost on any failure
	// once the sandbox is reserved. The budget is observed by the steps
	// that take the context (the Firecracker pause on this route does) and
	// between steps; a step that does not take it outlives the budget.
	checkpointSnapshotTimeout = 10 * time.Minute
	// resumeAfterCheckpointTimeout bounds the phase after the snapshot
	// (template, resume, upgrade, registration), also detached from the
	// caller. The resume waits for the memory snapshot to be processed,
	// which takes minutes for large sandboxes on a busy node.
	resumeAfterCheckpointTimeout = 10 * time.Minute
	// uploadTotalBudget bounds how long a snapshot upload is retried before it
	// is given up. Covers a long GCS outage without retrying forever.
	uploadTotalBudget = 2 * time.Hour
	// redisPeerKeyTTL keeps the peer routing key valid across the whole retry
	// window so a long retry doesn't drop peer routing mid-upload. A retained
	// snapshot with a failed upload keeps the key until this TTL expires.
	redisPeerKeyTTL = uploadTotalBudget + 2*time.Minute

	// uploadRetryInitialBackoff is the wait before the first retry; it grows
	// exponentially up to uploadRetryMaxBackoff.
	uploadRetryInitialBackoff = 5 * time.Second
	// uploadRetryMaxBackoff caps the backoff between attempts.
	uploadRetryMaxBackoff = 2 * time.Minute
	// uploadRetryBackoffMultiplier is the exponential growth factor between
	// retry attempts.
	uploadRetryBackoffMultiplier = 2

	// executionEventDataKey is the key used in webhook event data for sandbox execution metrics.
	executionEventDataKey = "execution"

	killReasonUnknown = "unknown"
	// killReasonSealFailed: the pause-path kill of a sandbox that can never be
	// validly persisted again — a latched in-place seal failure, or a parent
	// memfile dedup that failed for good.
	killReasonSealFailed = "seal_failed"
	// killReasonResumeFailed: the in-place resume after a checkpoint failed
	// and the orchestrator tore the stuck-paused VM down (ErrSandboxLost).
	// Accurate for BOTH teardown sites even when the chain started with a
	// pause failure: a failed pause alone is survived via the cleanup resume,
	// so the resume failing is always the lethal step; the full chain is in
	// the joined error.
	killReasonResumeFailed = "resume_failed"
	// killReasonThawFailed: the resume succeeded but the rootfs stayed frozen
	// through every thaw attempt (ErrSandboxLost tagged ErrRootfsThawFailed),
	// so the orchestrator tore down a guest that ran but could not write.
	killReasonThawFailed = "thaw_failed"
	// killReasonUploadFailed: the sandbox resumed after a checkpoint went
	// live and was torn down because its snapshot upload failed, leaving it
	// unable to be paused or resumed later.
	killReasonUploadFailed = "upload_failed"

	// pause_mode on paused events: what the snapshot holds. The values are the
	// SDK's snapshot mode, so a customer reads back the mode they passed.
	pauseModeFull       = "full"
	pauseModeFilesystem = "filesystem"
	// resume_mode on resumed events: how the sandbox started, in the values of
	// the SDK's onResume option.
	resumeModeRestore = "restore"
	resumeModeReboot  = "reboot"
)

// lostSandboxKillReason names the kill behind an ErrSandboxLost from the
// in-place checkpoint: thaw_failed when the VM resumed but its rootfs stayed
// frozen, resume_failed for every other lethal step.
func lostSandboxKillReason(err error) string {
	if errors.Is(err, sandbox.ErrRootfsThawFailed) {
		return killReasonThawFailed
	}

	return killReasonResumeFailed
}

// filesystemBoot reports whether a snapshot resumes by cold-booting (rebooting)
// from its rootfs instead of restoring memory: when the artifact has no memory
// to restore, or when the request demands a cold boot of one that does. The
// request can only widen toward the no-memory path — it can never force a
// memory restore of a snapshot that has none.
func filesystemBoot(meta metadata.Template, req *orchestrator.SandboxCreateRequest) bool {
	return meta.IsFilesystemOnly() || req.GetFilesystemBoot()
}

func validateColdBootVcpus(vcpu, maxVcpus, recordedVcpus int64) error {
	// An explicit maximum replaces the snapshot's size; otherwise inherit it.
	if maxVcpus == 0 {
		maxVcpus = recordedVcpus
	}
	machineVcpus := max(vcpu, maxVcpus)
	if !fc.ValidMachineVcpus(machineVcpus) {
		return status.Errorf(codes.InvalidArgument, "cannot cold-boot a VM with %d vcpus on this host", machineVcpus)
	}

	return nil
}

// firecrackerSupports reports whether the sandbox's RUNNING Firecracker
// carries a version-gated feature, per the given fcversion predicate. The
// version is fixed at resume, so the answer cannot change under a running
// sandbox. An unparsable version fails closed — version-gated features must
// never engage on a build outside the release contract.
func firecrackerSupports(ctx context.Context, sbx *sandbox.Sandbox, feature string, has func(*fcversion.Info) bool) bool {
	version := sbx.Config.FirecrackerConfig.FirecrackerVersion
	info, err := fcversion.New(version)
	if err != nil {
		sbxlogger.I(sbx).Warn(ctx, "unparsable firecracker version; refusing version-gated feature",
			zap.String("firecracker_version", version), zap.String("feature", feature), zap.Error(err))

		return false
	}
	if !has(&info) {
		sbxlogger.I(sbx).Info(ctx, "firecracker release predates version-gated feature",
			zap.String("firecracker_version", version), zap.String("feature", feature))

		return false
	}

	return true
}

// admitEgressProxy is the BYOP egress proxy kill-switch for a start that
// names one; it mirrors the API gate for direct gRPC callers and snapshot
// resumes, and a migration's restore runs it too, since the target may lack
// the dialer or the team's flag the source had.
func (s *Server) admitEgressProxy(ctx context.Context, config *orchestrator.SandboxConfig) error {
	if config.GetNetwork().GetEgress().GetEgressProxyAddress() == "" {
		return nil
	}
	if !s.featureFlags.BoolFlag(ctx, featureflags.BYOPProxyEnabledFlag) {
		telemetry.ReportEvent(ctx, "egressProxy rejected by BYOPProxyEnabledFlag")

		return status.Error(codes.PermissionDenied, "egress proxy is not enabled for this team")
	}
	if !s.sandboxFactory.EgressProxy().SupportsBYOP() {
		telemetry.ReportEvent(ctx, "egressProxy rejected: orchestrator build has no BYOP dialer")

		return status.Error(codes.Unimplemented, "egress proxy is not supported by this orchestrator build")
	}

	return nil
}

func (s *Server) Create(ctx context.Context, req *orchestrator.SandboxCreateRequest) (_ *orchestrator.SandboxCreateResponse, createErr error) {
	releaseWork := s.info.TrackWork()
	defer releaseWork()

	// set max request timeout for this request. The pre-boot journal replay runs
	// within this budget (a successful replay is fast; the cancel-immune worst case
	// is bounded well under it), so the orchestrator still times out before the
	// caller does and a recovery overrun surfaces as a retryable per-node error.
	ctx, cancel := context.WithTimeoutCause(ctx, requestTimeout, errors.New("request timed out"))
	defer cancel()

	// set up tracing
	ctx, childSpan := tracer.Start(ctx, "sandbox-create")
	defer childSpan.End()

	isResume := req.GetSandbox().GetSnapshot()
	vcpu, maxVcpus := req.GetSandbox().GetVcpu(), req.GetSandbox().GetMaxVcpus()
	if err := fc.ValidateVcpus(vcpu, maxVcpus, "vcpu", "max_vcpus"); err != nil {
		return nil, createRefused(ctx, codes.InvalidArgument, "%s", err.Error())
	}
	// fsOnly reports the ARTIFACT kind (filesystem-only snapshot), mirroring the
	// fs_only pause label, so the historical fs-only latency cohort stays pure.
	// Combined with fs_boot_requested the population decomposes fully: the boot
	// path taken is fs_only OR fs_boot_requested; a rescue of a memory snapshot
	// is fs_only=false, fs_boot_requested=true. Set after metadata loads below.
	var fsOnly bool
	// filesystemBooted is the dispatch outcome (the reboot path ran); echoed in
	// the response so a demanding caller can verify the demand was honored.
	var filesystemBooted bool
	// Paired with success on the metric below so a replayed-then-hung start
	// (fs_recovery=replayed, success=false) is countable; neither signal shows it
	// alone. Stays "none" when no recovery ran (memory resume, flag off).
	fsRecovery := rootfs.RecoverOutcomeNone
	createStart := time.Now()
	// Set by maybeUpgradeEnvd below; labels the resume-latency histogram so the
	// treated (upgraded) vs untreated cohorts can be compared during the rollout.
	var envdUpgraded bool
	// Empty when the start failed before the sandbox had a network.
	var networkMode catalog.NetworkMode
	defer func() {
		s.sandboxCreateDuration.Record(ctx, time.Since(createStart).Milliseconds(),
			metric.WithAttributes(
				attribute.Bool("sandbox.resume", isResume),
				attribute.Bool("fs_only", fsOnly),
				attribute.Bool("fs_boot_requested", req.GetFilesystemBoot()),
				attribute.Bool("success", createErr == nil),
				attribute.Bool("envd.upgraded", envdUpgraded),
				attribute.String("fs_recovery", string(fsRecovery)),
				attribute.String("network_mode", string(networkMode)),
				telemetry.WithTeamID(req.GetSandbox().GetTeamId()),
			),
		)
	}()

	childSpan.SetAttributes(
		telemetry.WithBuildID(req.GetSandbox().GetBuildId()),
		telemetry.WithTeamID(req.GetSandbox().GetTeamId()),
		telemetry.WithTemplateID(req.GetSandbox().GetTemplateId()),
		telemetry.WithKernelVersion(req.GetSandbox().GetKernelVersion()),
		telemetry.WithSandboxID(req.GetSandbox().GetSandboxId()),
		telemetry.WithEnvdVersion(req.GetSandbox().GetEnvdVersion()),
	)

	// setup launch darkly
	ctx = featureflags.AddToContext(ctx, startFlagContexts(req.GetSandbox())...)

	if err := s.admitEgressProxy(ctx, req.GetSandbox()); err != nil {
		return nil, err
	}

	// A node that is not Healthy admits nothing new. The ResourceExhausted
	// makes the caller retry elsewhere without excluding this node; its detail
	// tells a status refusal, which lasts until the node is set Healthy, from
	// a capacity refusal, which the caller retries on the same node. Only the
	// SANDBOX_CREATE_REFUSED detail makes the caller give up.
	if snap := s.info.Snapshot(); snap.Status != orchestratorinfo.ServiceInfoStatus_Healthy {
		telemetry.ReportEvent(ctx, "create refused by node status")

		return nil, nodeNotAcceptingStatus(snap.Status).Err()
	}

	// Check the limit while taking the ID so in-flight starts count too.
	maxRunningSandboxesPerNode := s.info.MaxSandboxes.Load()
	reservation, err := s.sandboxFactory.Sandboxes.ReserveWithin(req.GetSandbox().GetSandboxId(), maxRunningSandboxesPerNode)
	if errors.Is(err, sandbox.ErrNodeAtCapacity) {
		telemetry.ReportEvent(ctx, "max number of running sandboxes reached")

		return nil, status.Errorf(codes.ResourceExhausted, "max number of running sandboxes on node reached (%d), please retry", maxRunningSandboxesPerNode)
	}
	if err != nil {
		return nil, s.sandboxAlreadyRunning(ctx, req.GetSandbox().GetSandboxId(), req.GetSandbox().GetExecutionId(), err)
	}
	var rollback *sandbox.Cleanup
	defer func() {
		s.finishSandboxStart(ctx, reservation, rollback, createErr)
	}()

	// Check if we've reached the max number of starting instances on this node
	if req.GetSandbox().GetSnapshot() {
		err := s.waitForAcquire(ctx, startGateResume)
		if err != nil {
			return nil, err
		}
	} else {
		acquired := s.tryAcquireStart(ctx, startGateCreate)
		if !acquired {
			telemetry.ReportEvent(ctx, "too many starting sandboxes on node")

			return nil, status.Errorf(codes.ResourceExhausted, "too many sandboxes starting on this node, please retry")
		}
	}
	defer s.startingSandboxes.Release(1)

	releaseHugePages, err := s.admitHugePages(ctx, req.GetSandbox())
	if err != nil {
		return nil, err
	}
	defer releaseHugePages()

	// Pinned: eviction Closes a template, which deletes its snapfile/metafile
	// from disk, so a template backing a running sandbox must not be evictable.
	// The pin is taken atomically with the lookup — taking it afterwards can
	// race an eviction already in flight. Released either by the rollback below
	// (if we never reach the lifecycle goroutine) or by that goroutine once the
	// sandbox has closed; releaseTemplate is idempotent, so registering it on
	// both paths still releases exactly one pin.
	template, meta, profile, releaseTemplate, err := s.pinTemplateWithProfile(ctx, req.GetSandbox().GetBuildId(), req.GetSandbox().GetSnapshot(), req.GetSandbox().GetMaxSandboxLength())
	if err != nil {
		if refusal := guestProfileRefusal(err); refusal != nil {
			return nil, refusal
		}

		return nil, err
	}

	rollback = sandbox.NewCleanup()
	rollback.AddNoContext(ctx, func() error {
		releaseTemplate()

		return nil
	})

	if err := tlsInterceptionRefusal(ctx, profile, req.GetSandbox().GetNetwork().GetEgress(), isResume); err != nil {
		return nil, err
	}
	if err := autoPauseRefusal(ctx, profile, req.GetSandbox()); err != nil {
		return nil, err
	}

	resolvedFCVersion := resolveSandboxFirecrackerVersion(ctx, s.featureFlags, meta, profile, req.GetSandbox().GetFirecrackerVersion())
	if err := firecrackerVersionRefusal(ctx, profile, resolvedFCVersion); err != nil {
		return nil, err
	}
	config, err := sandboxConfigOf(req.GetSandbox(), firecrackerConfigFor(meta, profile, req.GetSandbox().GetKernelVersion(), resolvedFCVersion), s.config.NFSMountNconnect)
	if err != nil {
		return nil, err
	}
	if err := firecrackerFeaturesRefusal(ctx, config.FirecrackerConfig, s.config.BuilderConfig); err != nil {
		return nil, err
	}
	childSpan.SetAttributes(
		telemetry.WithFirecrackerVersion(config.FirecrackerConfig.FirecrackerVersion),
		attribute.String("guest_os", profile.Label()),
	)

	runtime := sandboxtypes.RuntimeMetadata{
		TemplateID:  req.GetSandbox().GetTemplateId(),
		SandboxID:   req.GetSandbox().GetSandboxId(),
		ExecutionID: req.GetSandbox().GetExecutionId(),
		TeamID:      req.GetSandbox().GetTeamId(),
		BuildID:     req.GetSandbox().GetBuildId(),
		SandboxType: sandboxtypes.SandboxTypeSandbox,
	}

	fsOnly = meta.IsFilesystemOnly()
	filesystemBooted = filesystemBoot(meta, req)
	if filesystemBooted {
		if err := validateColdBootVcpus(vcpu, maxVcpus, meta.VcpuCount); err != nil {
			return nil, err
		}
	}

	// Read before the start too: a teardown that ends during it gave its pages
	// back after the mmap failed, so a retry would find them.
	stoppingBefore := s.sandboxFactory.Sandboxes.Stopping()

	// A guest that resets before envd first answers was paused while shutting
	// down or restarting; it is booted again instead of failing the start.
	rebootOnReset := s.featureFlags.BoolFlag(ctx, featureflags.GuestRebootInPlaceFlag)

	var sbx *sandbox.Sandbox
	if filesystemBooted {
		sbx, err = s.sandboxFactory.RebootSandbox(
			ctx,
			template,
			config,
			runtime,
			req.GetEndTime().AsTime(),
			req.GetSandbox(),
			// Defer routing until after the resume-time envd upgrade's
			// post-/init, so the sandbox isn't reachable during its pre-init
			// auth window. Promoted below via markSandboxLive.
			true,
			req.GetFilesystemBoot(),
			func(o rootfs.RecoverOutcome) { fsRecovery = o },
		)
	} else {
		// Defer routing until after the resume-time envd upgrade's post-/init
		// (see markSandboxLive below).
		opts := []sandbox.ResumeOption{sandbox.WithDeferredLiveRegistration()}
		if rebootOnReset {
			opts = append(opts, sandbox.WithRebootOnGuestReset(s.startRebootReport(guestOSLabel(config))))
		}

		sbx, err = s.sandboxFactory.ResumeSandbox(
			ctx,
			template,
			config,
			runtime,
			req.GetStartTime().AsTime(),
			req.GetEndTime().AsTime(),
			req.GetSandbox(),
			opts...,
		)
	}
	if err != nil {
		if !rebootOnReset && errors.Is(err, sandbox.ErrGuestResetOnStart) {
			s.countGuestReboot(ctx, guestOSLabel(config), guestRebootOutcomeFlagOff, guestRebootPathStart)
		}

		if errors.Is(err, storage.ErrObjectNotExist) {
			// Snapshot data not found, let the API know the data aren't probably upload yet
			telemetry.ReportError(ctx, "sandbox files not found", err, telemetry.WithSandboxID(req.GetSandbox().GetSandboxId()))

			return nil, status.Errorf(codes.FailedPrecondition, "sandbox files for '%s' not found", req.GetSandbox().GetSandboxId())
		}
		if errors.Is(err, sandbox.ErrVcpuExceedsSnapshot) {
			logger.L().Warn(ctx, "resume refused: vcpu exceeds snapshot", zap.Error(err),
				zap.Int64("vcpu", vcpu),
				zap.Int64("max_vcpus", maxVcpus),
				logger.WithSandboxID(runtime.SandboxID),
				logger.WithBuildID(runtime.BuildID),
				logger.WithTemplateID(runtime.TemplateID))

			return nil, createRefused(ctx, codes.InvalidArgument, "%s", err.Error())
		}
		if errors.Is(err, sandbox.ErrEnvdTokenNotReplaceable) || errors.Is(err, sandbox.ErrColdBootVcpus) {
			logger.L().Warn(ctx, "start refused: the guest cannot run this request", zap.Error(err),
				logger.WithSandboxID(runtime.SandboxID),
				logger.WithBuildID(runtime.BuildID),
				logger.WithTemplateID(runtime.TemplateID))

			return nil, createRefused(ctx, codes.FailedPrecondition, "%s", err.Error())
		}
		if refusal := volumeRefusal(ctx, err); refusal != nil {
			// The user's volume is unusable; the orchestrator itself is fine.
			telemetry.ReportError(ctx, "sandbox volume not usable", err, telemetry.WithSandboxID(req.GetSandbox().GetSandboxId()))

			return nil, refusal
		}
		if refusal := s.memoryRefusal(ctx, err, req.GetSandbox().GetRamMb(), stoppingBefore, runtime); refusal != nil {
			// Placement may retry this node with the same sandbox ID within a
			// millisecond, and the ID stays reserved until the rollback has run
			// (finishSandboxStart runs it on a goroutine), so run it now. It
			// holds only the template pin here, normally a refcount drop.
			if cleanupErr := rollback.Run(ctx); cleanupErr != nil {
				telemetry.ReportCriticalError(ctx, "failed to clean up sandbox start", cleanupErr)
			}
			rollback = nil

			return nil, refusal
		}

		err = errors.Join(err, context.Cause(ctx))
		telemetry.ReportCriticalError(ctx, "failed to create sandbox", err)
		logger.L().Error(ctx, "failed to create sandbox", zap.Error(err),
			zap.Bool("filesystem_boot_requested", req.GetFilesystemBoot()),
			logger.WithSandboxID(runtime.SandboxID),
			logger.WithBuildID(runtime.BuildID),
			logger.WithTemplateID(runtime.TemplateID),
			logger.WithTeamID(runtime.TeamID),
			logger.WithEnvdVersion(config.Envd.Version),
			logger.WithKernelVersion(config.FirecrackerConfig.KernelVersion),
			logger.WithFirecrackerVersion(config.FirecrackerConfig.FirecrackerVersion),
		)

		return nil, status.Errorf(codes.Internal, "failed to create sandbox: %s", err)
	}
	networkMode = sbx.NetworkMode()

	rollback.Add(ctx, func(ctx context.Context) error { return stopAndCloseSandbox(ctx, sbx) })
	s.setupSandboxLifecycle(ctx, sbx, releaseTemplate)

	// Resume-time envd live-upgrade. The API /resume maps to Create
	// with snapshot=true, so this is the real resume path. Flag-driven,
	// best-effort + recover-wrapped (see maybeUpgradeEnvd) so it can't disrupt
	// resume. ctx already carries the LD context (envd-version/team/template).
	if req.GetSandbox().GetSnapshot() {
		var upErr error
		envdUpgraded, upErr = s.maybeUpgradeEnvd(ctx, sbx)
		if upErr != nil {
			sbx.SetStopReason(sandbox.StopReasonKilled)

			return nil, upErr
		}
	}

	// Promote to the live registry only now — after any resume-time envd upgrade
	// has run its post-/init and restored the access token — so the sandbox is
	// never routable during the upgrade's sub-second pre-init auth window. Both
	// the resume and reboot paths above defer this.
	if err := s.markSandboxLive(ctx, sbx, reservation, false); err != nil {
		return nil, s.registrationRefused(ctx, req.GetSandbox().GetSandboxId(), req.GetSandbox().GetExecutionId(), err)
	}
	// Read off the start path; unknown here means the read has not landed yet.
	childSpan.SetAttributes(attribute.String("balloon_mode", sbx.BalloonMode()))

	// Read scheduling metadata after the sandbox resumed so the template's
	// memfile/rootfs devices (and their headers) are resolved.
	var schedulingMetadata *orchestrator.SchedulingMetadata
	if provider, ok := template.(interface {
		SchedulingMetadata(ctx context.Context) *orchestrator.SchedulingMetadata
	}); ok {
		schedulingMetadata = provider.SchedulingMetadata(ctx)
	}

	teamID, buildId, eventsTTLDays, eventData := s.prepareSandboxEventData(ctx, sbx)

	eventType := events.SandboxCreatedEventPair
	if req.GetSandbox().GetSnapshot() {
		eventType = events.SandboxResumedEventPair
		addResumeMode(eventData, filesystemBooted)
	}

	s.publishEventAsync(
		ctx,
		teamID,
		events.SandboxEvent{
			Version:   events.StructureVersionV2,
			ID:        uuid.New(),
			Type:      eventType.Type,
			Timestamp: time.Now().UTC(),

			EventData:          eventData,
			SandboxID:          sbx.Runtime.SandboxID,
			SandboxExecutionID: sbx.Runtime.ExecutionID,
			SandboxTemplateID:  sbx.Config.BaseTemplateID,
			SandboxBuildID:     buildId,
			SandboxTeamID:      teamID,
			EventsTTLDays:      eventsTTLDays,
		},
	)

	sbx.SetExecutionStartedAt(time.Now())

	return &orchestrator.SandboxCreateResponse{
		ClientId:              s.info.ClientId,
		SchedulingMetadata:    schedulingMetadata,
		FilesystemBootApplied: filesystemBooted,
		// The resolved Firecracker version the sandbox actually runs, frozen
		// for its lifetime. The API stores it so a version-gated feature can
		// key off the running binary exactly instead of re-resolving the
		// flag, which drifts from this frozen value whenever the flag moves.
		ResolvedFirecrackerVersion: resolvedFCVersion,
		// The placement of the network this execution started on; none when it
		// uses the worker's own network.
		NetworkPlacement: sbx.NetworkPlacement,
	}, nil
}

// pinTemplateWithProfile pins buildID's template in the cache and returns it
// with its metadata, its guest profile and the call that releases the pin.
// On an error it holds no pin.
func (s *Server) pinTemplateWithProfile(ctx context.Context, buildID string, isSnapshot bool, maxSandboxLengthHours int64) (sbxtemplate.Template, metadata.Template, guest.Profile, func(), error) {
	template, release, err := s.templateCache.GetTemplatePinned(ctx, buildID, isSnapshot, false,
		sbxtemplate.GetTemplateOpts{MaxSandboxLengthHours: maxSandboxLengthHours})
	if err != nil {
		return nil, metadata.Template{}, nil, nil, fmt.Errorf("failed to get template snapshot data: %w", err)
	}

	// The template's own record of what it runs. The request carries the
	// build's versions and machine size, which the database knows; the guest
	// OS is lineage state only the template metadata carries, and its profile
	// decides how the VMM is driven before the first request reaches
	// Firecracker, starting with which Firecracker binary that is.
	meta, err := template.Metadata()
	if err != nil {
		release()

		return nil, metadata.Template{}, nil, nil, fmt.Errorf("failed to read template metadata: %w", err)
	}
	profile, err := meta.ServingGuestProfile(ctx, s.featureFlags)
	if err != nil {
		release()

		return nil, metadata.Template{}, nil, nil, fmt.Errorf("failed to resolve the guest profile: %w", err)
	}

	return template, meta, profile, release, nil
}

// sandboxConfigOf is the configuration a start of the sandbox api describes
// runs with on firecracker. The start may change it, never api.
// nconnectDefault is the node's nconnect for volume mounts that request none.
func sandboxConfigOf(api *orchestrator.SandboxConfig, firecracker fc.Config, nconnectDefault int) (*sandbox.Config, error) {
	volumeMounts, err := createVolumeMountModelsFromAPI(api.GetVolumeMounts(), nconnectDefault)
	if err != nil {
		return nil, fmt.Errorf("failed to convert volume mounts: %w", err)
	}

	return sandbox.NewConfig(sandbox.Config{
		BaseTemplateID: api.GetBaseTemplateId(),

		Vcpu:              api.GetVcpu(),
		ConfiguredVmVcpus: api.GetMaxVcpus(),
		RamMB:             api.GetRamMb(),
		TotalDiskSizeMB:   api.GetTotalDiskSizeMb(),
		HugePages:         true,

		Network: proto.CloneOf(api.GetNetwork()),

		Envd: sandbox.EnvdMetadata{
			Version:     api.GetEnvdVersion(),
			AccessToken: api.EnvdAccessToken,
			Vars:        api.GetEnvVars(),
		},

		FirecrackerConfig: firecracker,

		VolumeMounts:          volumeMounts,
		MaxSandboxLengthHours: api.GetMaxSandboxLength(),
	}), nil
}

func createVolumeMountModelsFromAPI(volumeMounts []*orchestrator.SandboxVolumeMount, nconnectDefault int) ([]sandbox.VolumeMountConfig, error) {
	var errs []error

	results := make([]sandbox.VolumeMountConfig, 0, len(volumeMounts))

	for _, v := range volumeMounts {
		volumeID, err := uuid.Parse(v.GetId())
		if err != nil {
			errs = append(errs, fmt.Errorf("invalid volume id %q: %w", v.GetId(), err))

			continue
		}

		// A mount that requests no nconnect takes the node's default; the
		// default itself may be 0 to mount without the option.
		nconnect := int(v.GetNconnect())
		if nconnect == 0 {
			nconnect = nconnectDefault
		}
		if nconnect < 0 || nconnect > 16 {
			errs = append(errs, fmt.Errorf("invalid nconnect %d for volume %q: must be between 0 and 16", nconnect, v.GetName()))

			continue
		}

		results = append(results, sandbox.VolumeMountConfig{
			ID:       volumeID,
			Name:     v.GetName(),
			Path:     v.GetPath(),
			Type:     v.GetType(),
			Nconnect: nconnect,
		})
	}

	return results, errors.Join(errs...)
}

func (s *Server) Update(ctx context.Context, req *orchestrator.SandboxUpdateRequest) (*emptypb.Empty, error) {
	releaseWork := s.info.TrackWork()
	defer releaseWork()

	ctx, childSpan := tracer.Start(ctx, "sandbox-update")
	defer childSpan.End()

	childSpan.SetAttributes(
		telemetry.WithSandboxID(req.GetSandboxId()),
	)

	sbx, ok, awaitErr := s.sandboxFactory.Sandboxes.AwaitBoot(ctx, req.GetSandboxId())
	if awaitErr != nil {
		return nil, status.FromContextError(awaitErr).Err()
	}
	if !ok {
		telemetry.ReportCriticalError(ctx, "sandbox not found", nil)

		return nil, status.Error(codes.NotFound, "sandbox not found")
	}

	// Every flag the update and its rollback evaluate, the firewall's
	// non-DNS UDP mode included, sees the sandbox and its team.
	ctx = featureflags.AddToContext(ctx, sandboxFlagContexts(sbx)...)

	childSpan.SetAttributes(
		telemetry.WithTeamID(sbx.Runtime.TeamID),
		telemetry.WithTemplateID(sbx.Runtime.TemplateID),
		telemetry.WithBuildID(sbx.Runtime.BuildID),
		telemetry.WithFirecrackerVersion(sbx.Config.FirecrackerConfig.FirecrackerVersion),
		telemetry.WithKernelVersion(sbx.Config.FirecrackerConfig.KernelVersion),
		telemetry.WithEnvdVersion(sbx.Config.Envd.Version),
		attribute.String("balloon_mode", sbx.BalloonMode()),
	)

	// Mirror the Create-side BYOP gates; defense-in-depth for direct gRPC
	// callers.
	if req.GetEgress().GetEgressProxyAddress() != "" {
		if !s.featureFlags.BoolFlag(ctx, featureflags.BYOPProxyEnabledFlag) {
			telemetry.ReportEvent(ctx, "egressProxy update rejected by BYOPProxyEnabledFlag")

			return nil, status.Error(codes.PermissionDenied,
				"egress proxy is not enabled for this team")
		}
		if !s.sandboxFactory.EgressProxy().SupportsBYOP() {
			telemetry.ReportEvent(ctx, "egressProxy update rejected: orchestrator build has no BYOP dialer")

			return nil, status.Error(codes.Unimplemented,
				"egress proxy is not supported by this orchestrator build")
		}
	}
	// Refused before anything is applied; the API stores nothing on this answer.
	if err := sandbox.RefuseTLSInterception(sbx.Config.GuestProfile(), req.GetEgress()); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	var updates []utils.UpdateFunc

	if req.GetEndTime() != nil {
		updates = append(updates, func(_ context.Context) (func(context.Context), error) {
			old := sbx.GetEndAt()
			sbx.SetEndAt(req.GetEndTime().AsTime())

			return func(_ context.Context) { sbx.SetEndAt(old) }, nil
		})
	}

	if req.GetEgress() != nil {
		updates = append(updates, func(ctx context.Context) (func(context.Context), error) {
			oldEgress := sbx.Config.GetNetworkEgress()
			egress := req.GetEgress()

			if err := transitionEgress(ctx, sbx.Config, sbx.Slot.UpdateInternet, oldEgress, egress); err != nil {
				return nil, err
			}

			return func(ctx context.Context) {
				// Fails closed: a failed kernel re-drop never publishes the
				// weaker config. Rollbacks cannot propagate errors, so report.
				if err := transitionEgress(ctx, sbx.Config, sbx.Slot.UpdateInternet, sbx.Config.GetNetworkEgress(), oldEgress); err != nil {
					telemetry.ReportCriticalError(ctx, "failed to roll back sandbox egress update", err)
				}
			}, nil
		})
	}

	// The factory refuses an egress proxy to a sandbox on a placed network
	// before anything commits, and tells that network a committed update.
	err := s.sandboxFactory.RunUpdate(ctx, sbx, req.GetEgress(), func() error {
		if err := utils.ApplyAllOrNone(ctx, updates); err != nil {
			telemetry.ReportCriticalError(ctx, "failed to update sandbox", err)

			return status.Errorf(codes.Internal, "failed to update sandbox: %s", err)
		}

		// Publish event if any updates were applied.
		if len(updates) > 0 {
			teamID, buildId, eventsTTLDays, eventData := s.prepareSandboxEventData(ctx, sbx)
			if req.GetEndTime() != nil {
				eventData["set_timeout"] = req.GetEndTime().AsTime().Format(time.RFC3339)
			}
			if egress := req.GetEgress(); egress != nil {
				eventData["network_egress"] = map[string]any{
					"allowed_cidrs":   egress.GetAllowedCidrs(),
					"denied_cidrs":    egress.GetDeniedCidrs(),
					"allowed_domains": egress.GetAllowedDomains(),
				}
			}

			s.publishEventAsync(
				ctx,
				teamID,
				events.SandboxEvent{
					Version:   events.StructureVersionV2,
					ID:        uuid.New(),
					Type:      events.SandboxUpdatedEventPair.Type,
					Timestamp: time.Now().UTC(),

					EventData:          eventData,
					SandboxID:          sbx.Runtime.SandboxID,
					SandboxExecutionID: sbx.Runtime.ExecutionID,
					SandboxTemplateID:  sbx.Config.BaseTemplateID,
					SandboxBuildID:     buildId,
					SandboxTeamID:      teamID,
					EventsTTLDays:      eventsTTLDays,
				},
			)
		}

		return nil
	})
	if err != nil {
		return nil, mapSandboxUpdateError(err)
	}

	return &emptypb.Empty{}, nil
}

func mapSandboxUpdateError(err error) error {
	if errors.Is(err, sandbox.ErrSandboxUpdateClosed) {
		return status.Error(codes.NotFound, "sandbox is closing or closed")
	}
	if errors.Is(err, sandbox.ErrEgressProxyUnsupported) {
		return status.Error(codes.FailedPrecondition, err.Error())
	}
	if errors.Is(err, sandbox.ErrSandboxMigrating) {
		return status.Error(codes.FailedPrecondition, err.Error())
	}

	return err
}

// migratingStatus answers a checkpoint of a sandbox being migrated, with
// the checkpoint-in-progress detail a checkpoint refused for a migration
// begun after its read carries, so the API reads one state one way.
func migratingStatus(sandboxID string) error {
	return checkpointInProgressStatus(codes.FailedPrecondition, "sandbox '%s' is being migrated", sandboxID).Err()
}

// migratingRetryStatus answers a kill or a pause of a sandbox being migrated,
// as a running checkpoint does. The API keeps the sandbox record only on this
// answer and removes it on any other. A migrating VM must keep its record:
// once retained, its network belongs to the target.
func migratingRetryStatus(sandboxID string) error {
	return checkpointInProgressStatus(codes.ResourceExhausted, "sandbox '%s' is being migrated, please retry", sandboxID).Err()
}

// transitionEgress moves the in-memory egress config (read per-connection by
// the userspace proxy) and the in-netns kernel firewall from one state to
// another. Loosen last, tighten first: the kernel must never be more relaxed
// than the config the proxy sees. On failure both layers stay consistent.
// updateInternet is a parameter so tests can observe the ordering.
func transitionEgress(
	ctx context.Context,
	cfg *sandbox.Config,
	updateInternet func(context.Context, *orchestrator.SandboxNetworkEgressConfig) error,
	from, to *orchestrator.SandboxNetworkEgressConfig,
) error {
	if to.GetEgressProxyAddress() != "" {
		// BYOP loosens the kernel firewall: internal-destined TCP is handed
		// to the userspace proxy instead of dropped, so the proxy must see
		// the config before the kernel lets that traffic through.
		applyNetworkEgress(cfg, to)
		if err := updateInternet(ctx, to); err != nil {
			// The kernel kept its rules (atomic flush); undo the publish.
			applyNetworkEgress(cfg, from)

			return fmt.Errorf("failed to update sandbox network: %w", err)
		}

		return nil
	}

	if err := updateInternet(ctx, to); err != nil {
		return fmt.Errorf("failed to update sandbox network: %w", err)
	}
	applyNetworkEgress(cfg, to)

	return nil
}

// applyNetworkEgress publishes the egress config to the in-memory sandbox
// config, collapsing an all-empty egress (no CIDRs, domains, rules, or proxy)
// to nil so readers treat it as "no custom egress".
func applyNetworkEgress(cfg *sandbox.Config, egress *orchestrator.SandboxNetworkEgressConfig) {
	if len(egress.GetAllowedCidrs()) == 0 && len(egress.GetDeniedCidrs()) == 0 && len(egress.GetAllowedDomains()) == 0 && len(egress.GetRules()) == 0 && egress.GetEgressProxyAddress() == "" {
		cfg.SetNetworkEgress(nil)
	} else {
		cfg.SetNetworkEgress(egress)
	}
}

func (s *Server) List(ctx context.Context, _ *emptypb.Empty) (*orchestrator.SandboxListResponse, error) {
	_, childSpan := tracer.Start(ctx, "sandbox-list")
	defer childSpan.End()

	items := s.sandboxFactory.Sandboxes.Items()

	sandboxes := make([]*orchestrator.RunningSandbox, 0, len(items))

	for _, sbx := range items {
		if sbx == nil {
			continue
		}

		// Build sandboxes are not owned by the API and must never show up here,
		// or the API would treat them as orphans and kill them. They are the only
		// sandboxes created without an APIStoredConfig.
		if sbx.APIStoredConfig == nil {
			continue
		}

		startedAt := sbx.GetStartedAt()
		entry := &orchestrator.RunningSandbox{
			ClientId:    s.info.ClientId,
			StartTime:   timestamppb.New(startedAt),
			EndTime:     timestamppb.New(sbx.GetEndAt()),
			SandboxId:   sbx.Runtime.SandboxID,
			TeamId:      sbx.Runtime.TeamID,
			ExecutionId: sbx.Runtime.ExecutionID,
			Vcpu:        sbx.Config.Vcpu,
			RamMb:       sbx.Config.RamMB,
		}
		if sbx.GuestPoweredOff() {
			entry.Ended = true
			entry.GuestEvent = guestEventProto(fc.GuestEventPowerOff)
			entry.DiskKept = true
		}
		sandboxes = append(sandboxes, entry)
	}

	for _, e := range s.sandboxFactory.Sandboxes.EndedItems(time.Now()) {
		sandboxes = append(sandboxes, &orchestrator.RunningSandbox{
			ClientId:    s.info.ClientId,
			StartTime:   timestamppb.New(e.StartedAt),
			EndTime:     timestamppb.New(e.EndAt),
			SandboxId:   e.SandboxID,
			TeamId:      e.TeamID,
			ExecutionId: e.ExecutionID,
			Vcpu:        e.Vcpu,
			RamMb:       e.RamMB,
			Ended:       true,
			GuestEvent:  guestEventProto(e.GuestEvent),
		})
	}

	return &orchestrator.SandboxListResponse{
		Sandboxes: sandboxes,
	}, nil
}

func (s *Server) Delete(ctxConn context.Context, in *orchestrator.SandboxDeleteRequest) (*emptypb.Empty, error) {
	releaseWork := s.info.TrackWork()
	defer releaseWork()

	ctx, cancel := context.WithTimeoutCause(ctxConn, requestTimeout, errors.New("request timed out"))
	defer cancel()

	ctx, childSpan := tracer.Start(ctx, "sandbox-delete")
	defer childSpan.End()

	childSpan.SetAttributes(
		telemetry.WithSandboxID(in.GetSandboxId()),
	)

	killReason := in.GetKillReason()
	if killReason == "" {
		killReason = killReasonUnknown
	}

	read, _ := s.sandboxFactory.Sandboxes.Get(in.GetSandboxId())
	sbx, err := s.takeForDelete(ctx, in, killReason, read)
	if err != nil {
		return nil, err
	}
	if sbx == nil {
		return &emptypb.Empty{}, nil
	}

	childSpan.SetAttributes(
		telemetry.WithTeamID(sbx.Runtime.TeamID),
		telemetry.WithTemplateID(sbx.Runtime.TemplateID),
		telemetry.WithBuildID(sbx.Runtime.BuildID),
		telemetry.WithFirecrackerVersion(sbx.Config.FirecrackerConfig.FirecrackerVersion),
		telemetry.WithKernelVersion(sbx.Config.FirecrackerConfig.KernelVersion),
		telemetry.WithEnvdVersion(sbx.Config.Envd.Version),
		attribute.String("balloon_mode", sbx.BalloonMode()),
	)

	sbxlogger.E(sbx).Info(ctx, "Killing sandbox", zap.String("kill_reason", killReason))

	sbx.SetStopReason(sandbox.StopReasonKilled)

	// Check health metrics before stopping the sandbox
	sbx.Checks().Healthcheck(ctx, true)

	// Start the cleanup in a goroutine—the initial kill request should be send as the first thing in stop, and at this point you cannot route to the sandbox anymore.
	// We don't wait for the whole cleanup to finish here.
	go func() {
		err := sbx.Stop(context.WithoutCancel(ctx))
		if err != nil {
			sbxlogger.I(sbx).Error(ctx, "error stopping sandbox",
				logger.WithSandboxID(in.GetSandboxId()),
				zap.String("kill_reason", killReason),
				zap.Error(err),
			)
		}
	}()

	s.emitSandboxKilled(ctx, sbx, killReason)

	return &emptypb.Empty{}, nil
}

// takeForDelete marks the sandbox Delete read from the live map as stopping,
// so it is excluded from live queries (Get, Items, Count) but remains
// findable by IP (GetByHostPort) while the Firecracker process finishes
// shutting down, and is not synced to the API again. A miss, or a read that
// lost the marking to a checkpoint's reservation meanwhile, is a sandbox a
// checkpoint may hold out of the live map: the checkpoint is ended on the
// Delete's behalf and waited for, so its rollback is this delete and no VM
// the API stopped tracking goes live later; one that went live first is
// handed back to be killed like any other. nil with no error means the
// checkpoint ended with the kill and there is nothing left to do. A sandbox
// being migrated is refused retryably (migratingRetryStatus).
func (s *Server) takeForDelete(ctx context.Context, in *orchestrator.SandboxDeleteRequest, killReason string, read *sandbox.Sandbox) (*sandbox.Sandbox, error) {
	if read != nil && s.sandboxFactory.Sandboxes.MarkStopping(ctx, read.Runtime.SandboxID, read.LifecycleID) {
		return read, nil
	}
	if read != nil && read.MigrationInFlight() {
		return nil, migratingRetryStatus(read.Runtime.SandboxID)
	}

	live, ended := s.endCheckpointForDelete(ctx, in.GetSandboxId(), killReason)
	if live != nil {
		if !s.sandboxFactory.Sandboxes.MarkStopping(ctx, live.Runtime.SandboxID, live.LifecycleID) {
			telemetry.ReportCriticalError(ctx, "failed to mark sandbox as stopping", nil, telemetry.WithSandboxID(in.GetSandboxId()))

			return nil, status.Errorf(codes.Internal, "failed to delete sandbox '%s'", in.GetSandboxId())
		}

		return live, nil
	}
	if ended {
		return nil, nil
	}
	// A kill during a reboot in place ends the boot, which tears its
	// lifecycle down; a boot that registered first is killed as a live one.
	booting, publish, err := s.sandboxFactory.Sandboxes.CancelBoot(ctx, in.GetSandboxId(), killReason)
	if err != nil {
		logger.L().Info(ctx, "delete answered before the boot it cancelled finished its teardown; the boot publishes the kill",
			logger.WithSandboxID(in.GetSandboxId()), zap.String("kill_reason", killReason), zap.Error(context.Cause(ctx)))

		return nil, status.FromContextError(err).Err()
	}
	if booted, ok := s.sandboxFactory.Sandboxes.Get(in.GetSandboxId()); ok {
		if !s.sandboxFactory.Sandboxes.MarkStopping(ctx, booted.Runtime.SandboxID, booted.LifecycleID) {
			telemetry.ReportCriticalError(ctx, "failed to mark sandbox as stopping", nil, telemetry.WithSandboxID(in.GetSandboxId()))

			return nil, status.Errorf(codes.Internal, "failed to delete sandbox '%s'", in.GetSandboxId())
		}

		return booted, nil
	}
	if booting != nil {
		// The kill is published once: by the first Delete that waited the
		// boot out, or by the boot when a Delete stopped waiting.
		if publish {
			sbxlogger.E(booting).Info(ctx, "Killing sandbox", zap.String("kill_reason", killReason))
			s.emitSandboxKilled(ctx, booting, killReason)
		}

		return nil, nil
	}

	// The API ending a sandbox it learned had ended: nothing is left to
	// stop, and the API accepts NotFound.
	if s.sandboxFactory.Sandboxes.DropEnded(in.GetSandboxId()) {
		return nil, status.Errorf(codes.NotFound, "sandbox '%s' already ended", in.GetSandboxId())
	}

	// A read that lost the marking to a reservation and was not a boot: the
	// lifecycle is gone and nothing replaced it.
	if read != nil {
		telemetry.ReportCriticalError(ctx, "failed to mark sandbox as stopping", nil, telemetry.WithSandboxID(in.GetSandboxId()))

		return nil, status.Errorf(codes.Internal, "failed to delete sandbox '%s'", in.GetSandboxId())
	}

	telemetry.ReportCriticalError(ctx, "sandbox not found", nil, telemetry.WithSandboxID(in.GetSandboxId()))

	return nil, status.Errorf(codes.NotFound, "sandbox '%s' not found", in.GetSandboxId())
}

// markResumedSandboxEnded records why a resumed sandbox the checkpoint tears
// down before it went live ended: killed when a Delete ended the checkpoint,
// registration failed otherwise (a budget or the server's shutdown), as for a
// resumed sandbox whose registration itself failed. A budget or shutdown is
// not a kill, and a sandbox with no reason reads as crashed once its
// lifecycle goroutine sees it exit.
func markResumedSandboxEnded(ctx context.Context, resumedSbx *sandbox.Sandbox) {
	if errors.Is(context.Cause(ctx), errCheckpointDeleted) {
		resumedSbx.SetStopReason(sandbox.StopReasonKilled)

		return
	}
	resumedSbx.SetStopReason(sandbox.StopReasonRegistrationFailed)
}

// reportCheckpointStepError reports a failed step of a detached checkpoint.
// An end a Delete or the server's shutdown asked for is expected, and is
// logged rather than reported as critical.
func (s *Server) reportCheckpointStepError(ctx context.Context, msg string, err error, sandboxID string) {
	cause := context.Cause(ctx)
	if errors.Is(cause, errCheckpointDeleted) || errors.Is(cause, errCheckpointServerShutdown) {
		logger.L().Info(ctx, msg, logger.WithSandboxID(sandboxID), zap.Error(err))

		return
	}

	telemetry.ReportCriticalError(ctx, msg, err, telemetry.WithSandboxID(sandboxID))
}

// emitSandboxKilled publishes the terminal surfaces of a sandbox kill — the
// killed lifecycle event and the kill counter, with a bounded reason. Callers
// are the Delete RPC and the paths that destroy a live-registered sandbox
// in-request without passing through it: the pause-path seal-failure kill and
// the in-place resume-failure teardown.
func (s *Server) emitSandboxKilled(ctx context.Context, sbx *sandbox.Sandbox, killReason string) {
	teamID, buildId, eventsTTLDays, eventData := s.prepareSandboxEventData(ctx, sbx)
	eventData[executionEventDataKey] = s.getSandboxExecutionData(sbx)
	addKillReason(eventData, killReason)
	recordSandboxKill(ctx, s.sandboxKilledCounter, sbx.Runtime.TeamID, killReason)
	s.publishEventAsync(
		ctx,
		teamID,
		events.SandboxEvent{
			Version:   events.StructureVersionV2,
			ID:        uuid.New(),
			Type:      events.SandboxKilledEventPair.Type,
			Timestamp: time.Now().UTC(),

			EventData:          eventData,
			SandboxID:          sbx.Runtime.SandboxID,
			SandboxExecutionID: sbx.Runtime.ExecutionID,
			SandboxTemplateID:  sbx.Config.BaseTemplateID,
			SandboxBuildID:     buildId,
			SandboxTeamID:      teamID,
			EventsTTLDays:      eventsTTLDays,
		},
	)
}

// addKillReason records the kill reason on killed events. Empty input is
// normalized to "unknown" so killed events always carry a kill_reason key.
func addKillReason(eventData map[string]any, killReason string) {
	if killReason == "" {
		killReason = killReasonUnknown
	}

	eventData["kill_reason"] = killReason
}

// addPauseMode records on paused events whether the snapshot is full (memory
// and filesystem) or filesystem-only, whose resume cold-boots. Every paused
// event carries the key, so its absence means the event predates it.
func addPauseMode(eventData map[string]any, filesystemOnly bool) {
	pauseMode := pauseModeFull
	if filesystemOnly {
		pauseMode = pauseModeFilesystem
	}

	eventData["pause_mode"] = pauseMode
}

// addResumeMode records on resumed events the boot path that actually ran:
// restore when memory was restored, reboot when the sandbox cold-booted from
// its filesystem, either because the snapshot has no memory or because the
// resume asked to drop it.
func addResumeMode(eventData map[string]any, filesystemBooted bool) {
	resumeMode := resumeModeRestore
	if filesystemBooted {
		resumeMode = resumeModeReboot
	}

	eventData["resume_mode"] = resumeMode
}

// recordSandboxKill increments the kill counter with a bounded reason label.
func recordSandboxKill(ctx context.Context, counter metric.Int64Counter, teamID, killReason string) {
	if killReason == "" {
		killReason = killReasonUnknown
	}

	counter.Add(ctx, 1, metric.WithAttributes(
		attribute.String("kill_reason", killReason),
		telemetry.WithTeamID(teamID),
	))
}

// recordExecutionDuration samples how long one sandbox execution ran, labeled
// by why it ended. An operation that fails after the guest has already stopped
// — a pause whose snapshot fails — is still labeled by its intent; its failure
// is counted by that operation's own metric.
func (s *Server) recordExecutionDuration(ctx context.Context, sbx *sandbox.Sandbox) {
	duration, ok := sbx.ExecutionDuration()
	if !ok {
		return
	}

	s.sandboxExecutionDuration.Record(ctx, duration.Milliseconds(),
		metric.WithAttributes(
			attribute.String("stop_reason", string(sbx.GetStopReason())),
			telemetry.WithTeamID(sbx.Runtime.TeamID),
		))
}

// Labels of the guest reboots counter.
const (
	guestRebootOutcomeFlagOff = "flag_off"
	guestRebootPathRunning    = "running"
	guestRebootPathStart      = "start"
)

// recordCrash reports an execution that ended with no stop reason and returns
// its cause. The cause separates a guest that halted itself from a
// Firecracker somebody else killed: both leave a nil wait error.
func (s *Server) recordCrash(ctx context.Context, sbx *sandbox.Sandbox, waitErr error) fc.CrashCause {
	exitInfo := sbx.FirecrackerExit()
	memoryHandlerErr := sbx.MemoryHandlerErr()
	cause := crashCause(exitInfo, memoryHandlerErr)
	s.countCrash(ctx, cause, errors.Join(waitErr, memoryHandlerErr), sbx.Runtime.TeamID)

	fields := append(
		[]zap.Field{zap.Error(waitErr), zap.String("crash_cause", string(cause))},
		exitInfo.LogFields()...,
	)

	if isCleanFirecrackerExit(cause, waitErr) {
		sbxlogger.I(sbx).Warn(ctx, "sandbox stopped without a stop reason", fields...)

		return cause
	}

	sbxlogger.I(sbx).Error(ctx, "sandbox crashed", fields...)

	return cause
}

// countCrash counts an execution that ended with no stop reason.
func (s *Server) countCrash(ctx context.Context, cause fc.CrashCause, uffdErr error, teamID string) {
	s.sandboxCrashedCounter.Add(ctx, 1, metric.WithAttributes(
		attribute.String("cause", string(cause)),
		attribute.String("uffd_copy_error", string(classifyUffdCopyError(uffdErr))),
		telemetry.WithTeamID(teamID),
	))
}

// countGuestReboot counts a guest reset by what was done about it.
func (s *Server) countGuestReboot(ctx context.Context, guestOS, outcome, path string) {
	s.guestRebootsCounter.Add(ctx, 1, metric.WithAttributes(
		attribute.String("outcome", outcome),
		attribute.String("guest_os", guestOS),
		attribute.String("path", path),
	))
}

// rebootsOnReset reports whether an execution that ended with no stop reason
// is a guest reset that boots the sandbox again: a reset, whatever caused
// it, since the guest asked for the restart. A wait error means something
// else failed alongside the exit.
func rebootsOnReset(cause fc.CrashCause, waitErr error) bool {
	return cause.GuestEvent().Restarts() && waitErr == nil
}

// keepsOnPowerOff reports whether an execution that ended with no stop reason
// is a guest power-off with nothing else failing, which keeps the sandbox for
// its timeout action when the flag is on.
func keepsOnPowerOff(cause fc.CrashCause, waitErr error) bool {
	return cause.GuestEvent() == fc.GuestEventPowerOff && waitErr == nil
}

// holdPoweredOff keeps a sandbox whose guest powered off, disk included, and
// listed as powered off until the API ends it with the sandbox's timeout
// action: a kill, or a filesystem-only pause. It returns the sandbox log line
// once that action has stopped it. With the flag off it returns at once and
// the sandbox stops as a crash does.
func (s *Server) holdPoweredOff(ctx context.Context, sbx *sandbox.Sandbox) string {
	// A process that took the device is held whatever the flag says now: it
	// was booted to end on a power-off, and turning the flag off must not
	// cost its disk.
	flagCtx := featureflags.AddToContext(ctx, sandboxFlagContexts(sbx)...)
	if !sbx.PowerOffDeviceDeclared() && !s.featureFlags.BoolFlag(flagCtx, featureflags.GuestPowerOffEndFlag) {
		return stoppedLineFor(sbx.FirecrackerExit(), fc.GuestEventPowerOff, 0)
	}

	sbx.Checks().Stop()
	released := sbx.HoldPoweredOff()
	sbxlogger.I(sbx).Info(ctx, "guest powered off, sandbox kept for its timeout action")

	<-released

	return poweredOffLine(sbx.GetStopReason())
}

// poweredOffLine is the sandbox log line for a powered-off sandbox, once its
// timeout action has ended it.
func poweredOffLine(reason sandbox.StopReason) string {
	switch reason {
	case sandbox.StopReasonPaused:
		return "The guest powered off. The sandbox was paused."
	case sandbox.StopReasonKilled:
		return "The guest powered off. The sandbox was killed."
	default:
		return crashedSandboxLine(fc.GuestEventPowerOff, 0)
	}
}

// rebootGuest handles the reset of a running sandbox's guest. event says
// whether the guest reported a crash first, with the crash's code. It returns
// the next lifecycle, live, when the sandbox was booted again, and the
// sandbox log line saying what happened.
func (s *Server) rebootGuest(ctx context.Context, sbx *sandbox.Sandbox, event fc.GuestEvent, crashCode uint64, resetAt time.Time) (*sandbox.Sandbox, string) {
	ctx = featureflags.AddToContext(ctx, sandboxFlagContexts(sbx)...)
	if !s.featureFlags.BoolFlag(ctx, featureflags.GuestRebootInPlaceFlag) {
		s.countGuestReboot(ctx, guestOSLabel(sbx.Config), guestRebootOutcomeFlagOff, guestRebootPathRunning)

		return nil, stoppedLineFor(sbx.FirecrackerExit(), event, crashCode)
	}

	res := s.sandboxFactory.RebootInPlace(ctx, sbx)
	s.countGuestReboot(ctx, guestOSLabel(sbx.Config), string(res.Outcome), guestRebootPathRunning)

	return s.rebootEnded(ctx, sbx, res, event, crashCode, resetAt)
}

// rebootEnded publishes what a reboot in place of sbx ended with: the kill a
// Delete left it (bootKillToPublish), the next lifecycle's start, and the
// sandbox log line. It is rebootGuest after RebootInPlace returned.
func (s *Server) rebootEnded(ctx context.Context, sbx *sandbox.Sandbox, res sandbox.GuestRebootResult, event fc.GuestEvent, crashCode uint64, resetAt time.Time) (*sandbox.Sandbox, string) {
	fields := []zap.Field{
		zap.String("reboot_outcome", string(res.Outcome)),
		zap.Int("consecutive_resets", res.ConsecutiveResets),
	}
	if res.Sandbox == nil {
		sbxlogger.I(sbx).Warn(ctx, "guest reset, sandbox not booted again", append(fields, zap.Error(res.Err))...)

		if killReason, ok := bootKillToPublish(res); ok {
			// The kill is what ended the sandbox: an unset reason reads it,
			// so the lifecycle lists nothing as ended beside the event.
			sbx.SetStopReason(sandbox.StopReasonKilled)
			sbxlogger.E(sbx).Info(ctx, "Killing sandbox", zap.String("kill_reason", killReason))
			s.emitSandboxKilled(ctx, sbx, killReason)

			return nil, "Sandbox stopped"
		}
		if errors.Is(res.Err, sandbox.ErrBootCancelled) {
			return nil, "Sandbox stopped"
		}

		return nil, guestRebootLine(res.Outcome, event, crashCode)
	}
	if res.PendingKill != "" {
		sbxlogger.I(sbx).Info(ctx, "a delete stopped waiting for the boot it cancelled after the rebooted sandbox registered; it is live, and a retry or the API's orphan sync ends it",
			zap.String("kill_reason", res.PendingKill), zap.String("next_lifecycle_id", res.Sandbox.LifecycleID))
	}

	took := time.Since(resetAt)
	s.guestRebootDuration.Record(ctx, took.Milliseconds(), metric.WithAttributes(
		attribute.String("guest_os", guestOSLabel(sbx.Config)),
		attribute.String("path", guestRebootPathRunning),
	))
	sbxlogger.I(sbx).Info(ctx, "guest reset, sandbox booted again",
		append(fields, zap.String("next_lifecycle_id", res.Sandbox.LifecycleID), zap.Duration("reset_to_ready", took))...)

	go res.Sandbox.Checks().Start(context.WithoutCancel(ctx))

	return res.Sandbox, guestRebootLine(res.Outcome, event, crashCode)
}

// bootKillToPublish names the kill a reboot in place publishes as it ends:
// the one of a Delete that cancelled the boot and stopped waiting before the
// teardown finished, when the boot ended with nothing live. That Delete
// answered its deadline, and its retry finds no sandbox, no boot and nothing
// listed as ended, so nothing else publishes the kill. A boot that went live
// despite the cancel leaves it to the retry, which finds the sandbox.
func bootKillToPublish(res sandbox.GuestRebootResult) (string, bool) {
	if res.Sandbox != nil || res.PendingKill == "" {
		return "", false
	}

	return res.PendingKill, true
}

// reportStartReboot records what a create or resume did about a guest reset
// before envd first answered. A rebooted sandbox is not live yet; the
// create registers it.
func (s *Server) reportStartReboot(ctx context.Context, guestOS string, res sandbox.GuestRebootResult, resetAt time.Time) {
	s.countGuestReboot(ctx, guestOS, string(res.Outcome), guestRebootPathStart)

	fields := []zap.Field{
		zap.String("reboot_outcome", string(res.Outcome)),
		zap.Int("consecutive_resets", res.ConsecutiveResets),
	}
	if res.Sandbox == nil {
		logger.L().Warn(ctx, "guest reset during the start, sandbox not booted again", append(fields, zap.Error(res.Err))...)

		return
	}

	took := time.Since(resetAt)
	s.guestRebootDuration.Record(ctx, took.Milliseconds(), metric.WithAttributes(
		attribute.String("guest_os", guestOS),
		attribute.String("path", guestRebootPathStart),
	))
	sbxlogger.I(res.Sandbox).Info(ctx, "guest reset during the start, sandbox booted again",
		append(fields, zap.Duration("reset_to_ready", took))...)
	sbxlogger.E(res.Sandbox).Info(ctx, guestRebootLine(res.Outcome, fc.GuestEventRebootOrCrash, 0))
}

// startRebootReport is reportStartReboot for a sandbox of the given guest OS.
func (s *Server) startRebootReport(guestOS string) sandbox.StartRebootReport {
	return func(ctx context.Context, res sandbox.GuestRebootResult, resetAt time.Time) {
		s.reportStartReboot(ctx, guestOS, res, resetAt)
	}
}

// guestOSLabel is the guest_os label of a sandbox's guest reboots.
func guestOSLabel(config *sandbox.Config) string {
	return config.GuestProfile().Label()
}

// guestRebootLine is the sandbox log line for what a reboot in place did
// about a guest reset. It says only what the host observed.
func guestRebootLine(outcome sandbox.GuestRebootOutcome, event fc.GuestEvent, crashCode uint64) string {
	switch outcome {
	case sandbox.GuestRebootRebooted:
		return guestRestartPhrase(event, crashCode) + ". The sandbox was booted again from its disk."
	case sandbox.GuestRebootRefusedLoop:
		return "The guest kept restarting without starting. The sandbox stopped."
	case sandbox.GuestRebootRefusedTimeout:
		return "The guest restarted too close to the sandbox's timeout to boot again. The sandbox stopped."
	default:
		return guestRestartPhrase(event, crashCode) + ", and booting it again failed. The sandbox stopped."
	}
}

// guestRestartPhrase says how a guest restarted, as far as the host saw: a
// crash the guest reported, with its crash code when it gave one, or a
// reset that may be either a reboot or a crash.
func guestRestartPhrase(event fc.GuestEvent, crashCode uint64) string {
	switch {
	case event == fc.GuestEventCrashWithRestart && crashCode != 0:
		return fmt.Sprintf("The guest crashed (code %#x) and restarted", crashCode)
	case event == fc.GuestEventCrashWithRestart:
		return "The guest crashed and restarted"
	default:
		return "The guest restarted (a reboot or a kernel crash)"
	}
}

// guestEventProto is the wire form of a guest event.
func guestEventProto(event fc.GuestEvent) orchestrator.GuestEvent {
	switch event {
	case fc.GuestEventPowerOff:
		return orchestrator.GuestEvent_GUEST_EVENT_POWER_OFF
	case fc.GuestEventRebootOrCrash:
		return orchestrator.GuestEvent_GUEST_EVENT_REBOOT_OR_CRASH
	case fc.GuestEventCrashWithRestart:
		return orchestrator.GuestEvent_GUEST_EVENT_CRASH_WITH_RESTART
	case fc.GuestEventCrashNoRestart:
		return orchestrator.GuestEvent_GUEST_EVENT_CRASH_NO_RESTART
	case fc.GuestEventFailure:
		return orchestrator.GuestEvent_GUEST_EVENT_FAILURE
	default:
		return orchestrator.GuestEvent_GUEST_EVENT_UNKNOWN
	}
}

// stoppedSandboxLine is the sandbox log line of a lifecycle that ended with
// no stop reason. A line that names what the guest did is written only when
// the exit waited for the guest counters it is read from: without the wait
// the counters are whatever had been read, and a plain reboot would be called
// a crash. An end the host classified without them keeps its line.
func stoppedSandboxLine(exit *fc.ExitInfo, cause fc.CrashCause, crashCode uint64) string {
	return stoppedLineFor(exit, cause.GuestEvent(), crashCode)
}

// stoppedLineFor is stoppedSandboxLine for an event already read from the
// cause; the flag-off returns of the reboot and power-off paths use it so
// they do not overwrite the plain line with one read from unawaited counters.
func stoppedLineFor(exit *fc.ExitInfo, event fc.GuestEvent, crashCode uint64) string {
	if exit != nil && !exit.MetricsAwaited && guestCountedEvent(event) {
		return "Sandbox stopped"
	}

	return crashedSandboxLine(event, crashCode)
}

// guestCountedEvent reports whether an event is read from the guest's exit
// counters rather than from how Firecracker ended.
func guestCountedEvent(event fc.GuestEvent) bool {
	switch event {
	case fc.GuestEventPowerOff, fc.GuestEventRebootOrCrash, fc.GuestEventCrashWithRestart, fc.GuestEventCrashNoRestart:
		return true
	default:
		return false
	}
}

// crashedSandboxLine is the sandbox log line that tells the user why a
// sandbox with no stop reason stopped. It says only what the host observed;
// crashCode is the code of a crash the guest reported, 0 for none.
func crashedSandboxLine(event fc.GuestEvent, crashCode uint64) string {
	switch event {
	case fc.GuestEventPowerOff:
		return "The guest powered off. The sandbox stopped."
	case fc.GuestEventRebootOrCrash, fc.GuestEventCrashWithRestart:
		return guestRestartPhrase(event, crashCode) + ". The sandbox stopped."
	case fc.GuestEventCrashNoRestart:
		return "The guest crashed and did not restart. The sandbox stopped."
	default:
		return "The sandbox stopped because of an infrastructure error."
	}
}

// crashCause classifies an execution that ended with no stop reason. The
// resume path stops Firecracker as soon as the memory handler exits, so a
// signal we sent after the handler failed is that failure, not a missing stop
// reason. Only requested_signal is overridden: a Firecracker killed from
// outside can take the handler down with it.
func crashCause(exit *fc.ExitInfo, memoryHandlerErr error) fc.CrashCause {
	cause := fc.ClassifyExit(exit)
	if cause == fc.CrashCauseRequestedSignal && memoryHandlerErr != nil {
		return fc.CrashCauseMemoryHandlerFailed
	}

	return cause
}

// uffdCopyError labels a crash whose memory handler could not install a
// faulted page. A node with no free hugepage fails UFFDIO_COPY with ENOMEM,
// and an alert on hugepage exhaustion needs to count those crashes alone:
// the crash cause folds them in with every other handler failure.
type uffdCopyError string

const (
	uffdCopyErrorNone   uffdCopyError = "none"
	uffdCopyErrorENOMEM uffdCopyError = "enomem"
	uffdCopyErrorOther  uffdCopyError = "other"
)

// classifyUffdCopyError reads the errors a crashed execution ended with.
func classifyUffdCopyError(err error) uffdCopyError {
	if !errors.Is(err, userfaultfd.ErrCopyFailed) {
		return uffdCopyErrorNone
	}
	if errors.Is(err, syscall.ENOMEM) {
		return uffdCopyErrorENOMEM
	}

	return uffdCopyErrorOther
}

// isCleanFirecrackerExit reports whether Firecracker exited 0 and nothing else
// failed: the guest reset itself, powered off, or ended without either. A wait
// error means something else failed alongside the exit, so that stays a crash.
func isCleanFirecrackerExit(cause fc.CrashCause, waitErr error) bool {
	switch cause {
	case fc.CrashCauseGuestPowerOff, fc.CrashCauseGuestReset, fc.CrashCauseGuestCrash, fc.CrashCauseCleanExit:
		return waitErr == nil
	default:
		return false
	}
}

// admitEnvd probes the guest before a snapshot and records the decision
// under rpc: a nil error admits, the typed retryable refusal refuses, the
// caller's context status means nothing was decided. A miss during a
// checkpoint is answered with held, the rpc's own answer for that hold; a
// miss on an execution that exited is NotFound, or the retryable refusal
// while the lifecycle may still keep it; a guest that powered off while the
// probe waited is refused retryably whatever strict says, since the capture
// that follows is its held disk, which no mark describes. Any other miss
// refuses when strict; otherwise the snapshot is admitted and
// envdUnresponsive reports that it is being taken over an agent that did not
// answer.
func (s *Server) admitEnvd(ctx context.Context, sbx *sandbox.Sandbox, rpc string, timeout time.Duration, held error, strict bool) (envdUnresponsive bool, err error) {
	outcome, waited, admitErr := sbx.AwaitEnvdAdmission(ctx, timeout)
	if !errors.Is(admitErr, sandbox.ErrSnapshotAdmissionEnvdUnhealthy) {
		s.recordPauseAdmission(ctx, sbx.Runtime.TeamID, rpc, outcome, waited)
		if admitErr != nil {
			return false, status.FromContextError(admitErr).Err()
		}

		return false, nil
	}

	// A guest that powered off while the probe waited is kept with its
	// disk, not gone: the retryable refusal keeps the record for the
	// disk-only pause that follows, whatever the caller allows.
	if sbx.GuestPoweredOff() {
		s.recordPauseAdmission(ctx, sbx.Runtime.TeamID, rpc, outcome, waited)

		return false, envdUnresponsiveStatus(sbx.Runtime.SandboxID).Err()
	}
	// An in-place checkpoint keeps the entry listed with the guest frozen,
	// so its marker is read first. Cleanup keeps the entry until its end,
	// so the exit is read with the map; only this execution's exit counts.
	// Neither is a hung agent, so neither is proceeded past.
	if sbx.CheckpointInFlight() {
		s.recordPauseAdmission(ctx, sbx.Runtime.TeamID, rpc, outcome, waited)

		return false, held
	}
	cur, live := s.sandboxFactory.Sandboxes.Get(sbx.Runtime.SandboxID)
	if !live || (cur == sbx && sbx.Exited() && !s.exitMayKeepSandbox(ctx, sbx)) {
		s.recordPauseAdmission(ctx, sbx.Runtime.TeamID, rpc, outcome, waited)
		// A reboot in place that took the sandbox while the probe waited
		// is retryable too, with the detail the API restores the record
		// on whatever its restore flag says.
		if s.sandboxFactory.Sandboxes.Rebooting(sbx.Runtime.SandboxID) {
			return false, checkpointInProgressStatus(codes.ResourceExhausted, "sandbox '%s' is rebooting, please retry", sbx.Runtime.SandboxID).Err()
		}
		if s.detachedCheckpointFor(sbx.Runtime.SandboxID) != nil {
			return false, held
		}
		// The claim can end between the two reads, leaving a live
		// successor and no claim; a second read tells it from a gone one.
		if cur, live := s.sandboxFactory.Sandboxes.Get(sbx.Runtime.SandboxID); live && (cur != sbx || !sbx.Exited()) {
			return false, held
		}

		return false, status.Error(codes.NotFound, "sandbox not found")
	}
	// Only a live execution that is this one is proceeded past: a successor
	// under the same ID means the probed one is gone, and the retryable
	// refusal keeps the API's record of the successor.
	if strict || cur != sbx || sbx.Exited() {
		s.recordPauseAdmission(ctx, sbx.Runtime.TeamID, rpc, outcome, waited)
		sbxlogger.E(sbx).Warn(ctx, "Refusing "+rpc+": envd is not answering health checks", zap.Duration("probe", waited))

		return false, envdUnresponsiveStatus(sbx.Runtime.SandboxID).Err()
	}
	s.recordPauseAdmission(ctx, sbx.Runtime.TeamID, rpc, sandbox.SnapshotAdmissionEnvdUnhealthyProceeded, waited)
	sbxlogger.E(sbx).Warn(ctx, "Proceeding with "+rpc+" although envd is not answering health checks; the snapshot is recorded as taken over an unresponsive guest agent", zap.Duration("probe", waited))

	return true, nil
}

// exitMayKeepSandbox reports whether an execution's exit can leave its
// sandbox listed rather than gone: a guest held after it powered off, or
// booted again in place, each behind its flag, and a process that took the
// ACPI power-off device, which holdPoweredOff keeps whatever the flag says
// now. Until the lifecycle has decided, a listed execution that exited is
// answered retryably, not as not found. Otherwise an exit is the end, as on
// a default guest.
func (s *Server) exitMayKeepSandbox(ctx context.Context, sbx *sandbox.Sandbox) bool {
	return sbx.PowerOffDeviceDeclared() ||
		s.featureFlags.BoolFlag(ctx, featureflags.GuestPowerOffEndFlag) ||
		s.featureFlags.BoolFlag(ctx, featureflags.GuestRebootInPlaceFlag)
}

// recordPauseAdmission records one admission decision. The wait histogram
// samples only the outcomes that actually waited or probed; an empty outcome
// (the caller's context ended mid-wait) records nothing — no decision was made.
func (s *Server) recordPauseAdmission(ctx context.Context, teamID, rpc string, outcome sandbox.SnapshotAdmissionOutcome, waited time.Duration) {
	if outcome == "" {
		return
	}

	s.pauseAdmissionCounter.Add(ctx, 1, metric.WithAttributes(
		attribute.String("outcome", string(outcome)),
		attribute.String("rpc", rpc),
		telemetry.WithTeamID(teamID),
	))

	if outcome == sandbox.SnapshotAdmissionReadyAfterWait || outcome == sandbox.SnapshotAdmissionRefused ||
		outcome == sandbox.SnapshotAdmissionEnvdHealthy || outcome == sandbox.SnapshotAdmissionEnvdUnhealthy ||
		outcome == sandbox.SnapshotAdmissionEnvdUnhealthyProceeded {
		s.pauseAdmissionWaitDuration.Record(ctx, waited.Milliseconds(),
			metric.WithAttributes(
				attribute.String("outcome", string(outcome)),
				telemetry.WithTeamID(teamID),
			))
	}
}

func (s *Server) Pause(ctx context.Context, in *orchestrator.SandboxPauseRequest) (*orchestrator.SandboxPauseResponse, error) {
	return s.pauseWith(ctx, in, serverPauseSteps{s: s})
}

// pauseWith is Pause with the VM steps given.
func (s *Server) pauseWith(ctx context.Context, in *orchestrator.SandboxPauseRequest, steps pauseSteps) (resp *orchestrator.SandboxPauseResponse, err error) {
	releaseWork := s.info.TrackWork()
	defer releaseWork()

	ctx, childSpan := tracer.Start(ctx, "sandbox-pause")
	defer childSpan.End()

	// Record pause duration split by fs_only vs memory (the gRPC RPC metric
	// can't distinguish them) and success, so dashboards can scope pause
	// call-count / error-rate / latency to filesystem-only pauses.
	pauseStart := time.Now()
	// Unknown until the sandbox is looked up; a pause of a missing sandbox
	// records an empty team.
	var sandboxTeamID string
	filesystemOnly := in.GetFilesystemOnly()
	defer func() {
		s.sandboxPauseDuration.Record(ctx, time.Since(pauseStart).Milliseconds(),
			metric.WithAttributes(
				attribute.Bool("fs_only", filesystemOnly),
				attribute.Bool("success", err == nil),
				telemetry.WithTeamID(sandboxTeamID),
			),
		)
	}()

	childSpan.SetAttributes(
		telemetry.WithSandboxID(in.GetSandboxId()),
		telemetry.WithTemplateID(in.GetTemplateId()),
		telemetry.WithBuildID(in.GetBuildId()),
	)

	// A pause during a reboot in place, the API's eviction at the end time
	// included, runs once the guest is up again.
	// The wait for a reboot ends shortly before the caller's deadline: a boot
	// still running then is a retryable refusal the API restores the record
	// on, not an expired RPC.
	awaitCtx, cancelAwait := s.pauseRebootWaitCtx(ctx)
	sbx, ok, waitedForBoot, awaitErr := s.sandboxFactory.Sandboxes.AwaitBootWaited(awaitCtx, in.GetSandboxId())
	cancelAwait()
	if awaitErr != nil {
		if ctx.Err() == nil {
			return nil, checkpointInProgressStatus(codes.ResourceExhausted, "sandbox '%s' is rebooting, please retry", in.GetSandboxId()).Err()
		}

		return nil, status.FromContextError(awaitErr).Err()
	}
	if !ok {
		// A checkpoint or a reboot holds the sandbox out of the live map
		// until its successor is live. ResourceExhausted with the
		// checkpoint-in-progress detail, not the FailedPrecondition the
		// Checkpoint RPC answers: the API's pause path restores the record
		// on that detail whatever its restore flag says, and removes it on
		// anything else, after which the successor is killed as an orphan.
		if err := s.pauseHeldElsewhere(in.GetSandboxId()); err != nil {
			return nil, err
		}
		telemetry.ReportCriticalError(ctx, "sandbox not found", nil, telemetry.WithSandboxID(in.GetSandboxId()))

		return nil, status.Error(codes.NotFound, "sandbox not found")
	}
	// A pause that waited for a reboot goes on only with time left for the
	// capture: a boot that ended near the cutoff is a retryable refusal, not
	// a capture that expires halfway through.
	if waitedForBoot && !s.pauseHasCaptureBudget(ctx) {
		return nil, checkpointInProgressStatus(codes.ResourceExhausted, "sandbox '%s' just rebooted, please retry", in.GetSandboxId()).Err()
	}
	sandboxTeamID = sbx.Runtime.TeamID
	if sbx.MigrationInFlight() {
		return nil, migratingRetryStatus(sbx.Runtime.SandboxID)
	}

	ctx = featureflags.AddToContext(ctx, sandboxFlagContexts(sbx)...)

	// A sandbox whose guest powered off has no memory to keep, whatever the
	// request asked for, a request that requires a healthy guest included:
	// there is no agent to ask, and the held disk is the only capture there
	// is, so it is taken rather than refused until the timeout action.
	filesystemOnly = filesystemOnly || sbx.GuestPoweredOff()

	childSpan.SetAttributes(
		attribute.Bool("guest_powered_off", sbx.GuestPoweredOff()),
		telemetry.WithTeamID(sbx.Runtime.TeamID),
		telemetry.WithFirecrackerVersion(sbx.Config.FirecrackerConfig.FirecrackerVersion),
		telemetry.WithKernelVersion(sbx.Config.FirecrackerConfig.KernelVersion),
		telemetry.WithEnvdVersion(sbx.Config.Envd.Version),
		attribute.String("balloon_mode", sbx.BalloonMode()),
	)

	// A guest without a filesystem quiesce would persist nothing in a
	// filesystem-only capture: refuse before the sandbox is marked stopping
	// and the deferred stop is armed, so it keeps running.
	// A powered-off guest has nothing left to quiesce: its disk-only capture is
	// the only capture there is, whatever the guest.
	if err := refuseUnsupportedFilesystemOnly(sbx, in.GetFilesystemOnly() && !sbx.GuestPoweredOff()); err != nil {
		return nil, err
	}

	// Flag-gated admission pre-flight: refuse retryably BEFORE any destructive
	// step while the parent memfile header is still deduplicating.
	var latchedErr error
	if graceMs := s.featureFlags.IntFlag(ctx, featureflags.PauseAdmissionGraceMs); graceMs >= 0 {
		outcome, waited, admitErr := sbx.AwaitSnapshotAdmission(ctx, time.Duration(graceMs)*time.Millisecond, !filesystemOnly)
		s.recordPauseAdmission(ctx, sbx.Runtime.TeamID, "pause", outcome, waited)
		switch {
		case errors.Is(admitErr, sandbox.ErrSnapshotAdmissionPending):
			sbxlogger.E(sbx).Warn(ctx, "Refusing pause: parent memfile header is still deduplicating", zap.Duration("waited", waited))

			return nil, status.Errorf(codes.ResourceExhausted, "node is busy persisting sandbox '%s', please retry", in.GetSandboxId())
		case admitErr != nil && outcome == "":
			// The caller's context ended mid-wait; nothing was decided and the
			// sandbox is untouched.
			return nil, status.FromContextError(admitErr).Err()
		case admitErr != nil:
			// Permanent: carried into the kill path below, which needs
			// MarkStopping first.
			latchedErr = admitErr
		}
	}

	// Refuse retryably, still before anything destructive, a capture the build
	// filesystem cannot hold (pause_disk_admission.go). A sandbox that can never
	// be persisted again is not refused but killed below, which a refusal would
	// only defer. The estimate stays reserved until the capture is on disk and
	// its upload has ended, or until this call fails before that.
	persistable := latchedErr == nil && sbx.EnsurePausable() == nil

	// Flag-gated envd pre-flight, every snapshot, after the latched checks.
	// A filesystem-only capture needs the agent to freeze the rootfs, which
	// fails past MarkStopping, so a miss refuses it; so does a caller that
	// did not say it can persist the mark. A memory pause that allows it
	// proceeds past a miss and the snapshot is recorded as taken over an
	// unresponsive agent, for the resume to act on. Retryable, never the kill
	// path; before the disk reservation so a waiting probe holds no headroom.
	// Without the flag the request field is ignored. A powered-off guest has
	// no agent to ask and nothing left to freeze: its disk-only capture is
	// not probed.
	envdUnresponsive := false
	if healthMs := s.featureFlags.IntFlag(ctx, featureflags.PauseEnvdHealthTimeoutMs); persistable && healthMs > 0 && !sbx.GuestPoweredOff() {
		strict := in.GetFilesystemOnly() || !in.GetAllowEnvdUnresponsive()
		unresponsive, refuseErr := s.admitEnvd(ctx, sbx, "pause", time.Duration(healthMs)*time.Millisecond,
			checkpointInProgressStatus(codes.ResourceExhausted, "sandbox '%s' is being checkpointed, please retry", in.GetSandboxId()).Err(), strict)
		if refuseErr != nil {
			return nil, refuseErr
		}
		envdUnresponsive = unresponsive
	}

	var diskReservation *pauseDiskReservation
	if persistable {
		reservation, refuseErr := s.admitPauseDisk(ctx, sbx, filesystemOnly)
		if refuseErr != nil {
			return nil, refuseErr
		}
		diskReservation = reservation
	}
	diskHandedOff := false
	defer func() {
		if !diskHandedOff {
			diskReservation.Release()
		}
	}()

	marked := s.sandboxFactory.Sandboxes.MarkStopping(ctx, sbx.Runtime.SandboxID, sbx.LifecycleID)
	if !marked && sbx.MigrationInFlight() {
		return nil, migratingRetryStatus(sbx.Runtime.SandboxID)
	}
	if !marked {
		// A checkpoint or a reboot in place that took the sandbox between
		// the read above and the marking gets the same retryable answer as a
		// read that missed.
		if err := s.pauseHeldElsewhere(in.GetSandboxId()); err != nil {
			return nil, err
		}
		telemetry.ReportCriticalError(ctx, "failed to mark sandbox as stopping", nil, telemetry.WithSandboxID(in.GetSandboxId()))

		return nil, status.Error(codes.Internal, "failed to pause sandbox")
	}

	sbxlogger.E(sbx).Info(ctx, "Pausing sandbox")

	// A latched in-place seal failure — or a parent memfile dedup that failed
	// for good, found by the pre-flight — means this sandbox can never be
	// validly persisted again. Kill it promptly and name the cause: refusing would NOT
	// preserve it — the API pause chain has already deleted the routing entry
	// and removes the store record regardless of this RPC's result, so a
	// refused pause leaves a live VM that the orphan reconciler kills ~20s
	// later, attributed to `orphaned` instead of the seal failure. An
	// in-request kill keeps the API's record consistent with reality and puts
	// the real cause in the error and the stop reason. The same policy covers
	// a seal that only fails while Pause waits on it below: the deferred stop
	// tears the sandbox down and the seal error is returned.
	persistErr := latchedErr
	if persistErr == nil {
		persistErr = sbx.EnsurePausable()
	}
	if persistErr != nil {
		telemetry.ReportCriticalError(ctx, "sandbox cannot be persisted, killing it", persistErr, telemetry.WithSandboxID(in.GetSandboxId()))
		sbx.SetStopReason(sandbox.StopReasonKilled)
		s.stopSandboxAsync(context.WithoutCancel(ctx), sbx)

		// This kill replaces a Delete-RPC death, so it must produce the same
		// terminal surfaces Delete does: the killed lifecycle event and the
		// kill counter, with a reason naming the seal failure. Without them
		// the event stream shows the sandbox start and then simply go silent
		// (MarkStopping above hides it from the orphan reconciler, so nothing
		// downstream fills the gap).
		s.emitSandboxKilled(ctx, sbx, killReasonSealFailed)

		return nil, status.Errorf(codes.Internal, "sandbox '%s' cannot be persisted, killing it: %s", in.GetSandboxId(), persistErr)
	}

	// Set before the snapshot, not after it succeeds: snapshotting suspends the
	// guest and can close the sandbox, which would read as a crash.
	sbx.SetStopReason(sandbox.StopReasonPaused)

	// Stop the old sandbox in background after we're done
	defer steps.stopAsync(context.WithoutCancel(ctx), sbx)

	// Defer the rootfs reflink off the pause critical path when enabled: pause is a
	// suspend, so nothing reads the diff until a later resume (which waits on the
	// upload anyway). NBD provider only; falls back to synchronous export otherwise.
	deferRootfsExport := s.featureFlags.BoolFlag(ctx, featureflags.DeferRootfsExportFlag)

	// Fire and forget - upload completes in the background
	res, err := steps.snapshot(ctx, sbx, in, filesystemOnly, deferRootfsExport, envdUnresponsive)
	if err != nil {
		telemetry.ReportCriticalError(ctx, "error snapshotting sandbox", err, telemetry.WithSandboxID(in.GetSandboxId()))

		return nil, status.Errorf(codes.Internal, "error snapshotting sandbox '%s': %s", in.GetSandboxId(), err)
	}
	// A guest that powered off after the read above was captured disk-only.
	filesystemOnly = res.filesystemOnly

	// The disk reservation follows the capture from here: most of it goes
	// once the diffs are on disk, the upload's copy once the upload ends.
	s.releasePauseDiskWhenWritten(context.WithoutCancel(ctx), res, diskReservation)
	completeUpload := res.completeUpload
	res.completeUpload = func(ctx context.Context, err error) {
		completeUpload(ctx, err)
		diskReservation.Release()
	}
	diskHandedOff = true

	steps.upload(ctx, sbx, res)

	// Best-effort: the local snapshot is now in the cache and the remote upload
	// has been kicked off above (still in flight). Harvest a resume page-fault
	// trace from a throwaway warm resume of the local snapshot and (when enabled)
	// persist it as a prefetch mapping for the next resume. Runs in the
	// background; never affects the pause result, and waits for the upload before
	// touching metadata. No-op unless the harvest flag is on. Reuse the object
	// metadata the snapshot was uploaded with so the re-upload can't drift.
	//
	// Skip it for a filesystem-only pause: that snapshot has no memory diff, so a
	// memory resume of it would just fail (the resume is reserved for memory
	// snapshots; fs-only is a reboot) — there is no memory working set to harvest.
	if !filesystemOnly {
		s.harvestResumePrefetchAsync(ctx, sbx, res, in.GetBuildId(), res.objectMetadata, harvestSourcePause)
	}

	teamID, buildId, eventsTTLDays, eventData := s.prepareSandboxEventData(ctx, sbx)
	eventData[executionEventDataKey] = s.getSandboxExecutionData(sbx)
	addPauseMode(eventData, res.filesystemOnly)

	eventType := events.SandboxPausedEventPair
	s.publishEventAsync(
		ctx,
		teamID,
		events.SandboxEvent{
			Version:   events.StructureVersionV2,
			ID:        uuid.New(),
			Type:      eventType.Type,
			Timestamp: time.Now().UTC(),

			EventData:          eventData,
			SandboxID:          sbx.Runtime.SandboxID,
			SandboxExecutionID: sbx.Runtime.ExecutionID,
			SandboxTemplateID:  sbx.Config.BaseTemplateID,
			SandboxBuildID:     buildId,
			SandboxTeamID:      teamID,
			EventsTTLDays:      eventsTTLDays,
		},
	)

	return &orchestrator.SandboxPauseResponse{
		SchedulingMetadata: res.schedulingMetadata,
		// A guest that powered off between the probe and the capture was
		// captured disk-only, which carries no memory and no mark.
		EnvdUnresponsive: envdUnresponsive && !filesystemOnly,
		FilesystemOnly:   filesystemOnly,
	}, nil
}

// sandboxFlagContexts returns the LaunchDarkly contexts for flag evaluations
// made on behalf of a sandbox already running on this node: the sandbox and
// its team, the same two kinds Create evaluates with.
func sandboxFlagContexts(sbx *sandbox.Sandbox) []ldcontext.Context {
	return []ldcontext.Context{
		ldcontext.NewBuilder(sbx.Runtime.SandboxID).
			Kind(featureflags.SandboxKind).
			SetString(featureflags.SandboxTemplateAttribute, sbx.Runtime.TemplateID).
			SetString(featureflags.SandboxKernelVersionAttribute, sbx.Config.FirecrackerConfig.KernelVersion).
			SetString(featureflags.SandboxFirecrackerVersionAttribute, sbx.Config.FirecrackerConfig.FirecrackerVersion).
			SetString(featureflags.SandboxEnvdVersionAttribute, sbx.Config.Envd.Version).
			Build(),
		featureflags.TeamContext(sbx.Runtime.TeamID),
	}
}

// startFlagContexts returns the LaunchDarkly contexts for flag evaluations
// made on behalf of a start of the sandbox api describes: the sandbox and its
// team.
func startFlagContexts(api *orchestrator.SandboxConfig) []ldcontext.Context {
	return []ldcontext.Context{
		ldcontext.NewBuilder(api.GetSandboxId()).
			Kind(featureflags.SandboxKind).
			SetString(featureflags.SandboxTemplateAttribute, api.GetTemplateId()).
			SetString(featureflags.SandboxKernelVersionAttribute, api.GetKernelVersion()).
			SetString(featureflags.SandboxFirecrackerVersionAttribute, api.GetFirecrackerVersion()).
			SetString(featureflags.SandboxEnvdVersionAttribute, api.GetEnvdVersion()).
			Build(),
		ldcontext.NewBuilder(api.GetTeamId()).
			Kind(featureflags.TeamKind).
			Build(),
	}
}

// The route a checkpoint took: in place, or the first gate condition that
// sent it resume-fresh, in gate order. The counter and the span carry it so
// a resume-fresh fraction is attributable per template.
const (
	routeInPlace          = "in_place"
	routeSyncWPOff        = "sync_wp_off"
	routeFlagOff          = "flag_off"
	routeFCUnsupported    = "fc_unsupported"
	routeBalloonReporting = "balloon_reporting"
	routeBalloonUnknown   = "balloon_unknown"
)

// inPlaceEarlyRoute is the cheap half of the gate, in order; "" means the
// balloon decides. fcSupported is called only when the earlier conditions
// hold, as it logs when it refuses.
func inPlaceEarlyRoute(syncWP, flagOn bool, fcSupported func() bool) string {
	switch {
	case !syncWP:
		return routeSyncWPOff
	case !flagOn:
		return routeFlagOff
	case !fcSupported():
		return routeFCUnsupported
	default:
		return ""
	}
}

// inPlaceBalloonRoute is the balloon half. Only an allow-listed mode goes in
// place. Reporting matters only when the export would pause it: the CoW
// window does, the synchronous copy does not, so with the deferred export
// off a reporting or unread balloon goes in place too. With it on, reporting
// and unknown go in place only when admit says so. A value this build does
// not know fails closed. deferred and admit are called only when they
// decide, so neither flag is evaluated for cohorts it never applies to.
func inPlaceBalloonRoute(mode userfaultfd.BalloonMode, deferred, admit func() bool) string {
	switch mode {
	case userfaultfd.BalloonModeHinting, userfaultfd.BalloonModeNone:
		return routeInPlace
	case userfaultfd.BalloonModeReporting:
		if !deferred() || admit() {
			return routeInPlace
		}

		return routeBalloonReporting
	case userfaultfd.BalloonModeUnknown:
		if !deferred() || admit() {
			return routeInPlace
		}

		return routeBalloonUnknown
	default:
		return routeBalloonUnknown
	}
}

// checkpointRoute decides the route under the flags in ctx. The device read
// behind ResolveBalloonMode runs only once the cheap conditions hold; a route
// decided before it carries the stamped mode as its label.
func (s *Server) checkpointRoute(ctx context.Context, sbx *sandbox.Sandbox) (string, userfaultfd.BalloonMode) {
	early := inPlaceEarlyRoute(
		sbx.UseSyncWP(),
		s.featureFlags.BoolFlag(ctx, featureflags.InPlaceCheckpointFlag),
		func() bool {
			return firecrackerSupports(ctx, sbx, "in-place checkpoint", (*fcversion.Info).HasInPlaceCheckpoint)
		},
	)
	if early != "" {
		return early, sbx.BalloonModeValue()
	}
	mode := sbx.ResolveBalloonMode(ctx)
	deferred := func() bool { return sbx.DeferredMemoryExport(ctx) }
	admit := func() bool { return s.featureFlags.BoolFlag(ctx, featureflags.InPlaceCheckpointReportingFlag) }

	return inPlaceBalloonRoute(mode, deferred, admit), mode
}

func (s *Server) Checkpoint(ctx context.Context, in *orchestrator.SandboxCheckpointRequest) (*orchestrator.SandboxCheckpointResponse, error) {
	releaseWork := s.info.TrackWork()
	defer releaseWork()

	ctx, childSpan := tracer.Start(ctx, "sandbox-checkpoint")
	defer childSpan.End()

	childSpan.SetAttributes(
		telemetry.WithSandboxID(in.GetSandboxId()),
		telemetry.WithBuildID(in.GetBuildId()),
	)

	sbx, ok, awaitErr := s.sandboxFactory.Sandboxes.AwaitBoot(ctx, in.GetSandboxId())
	if awaitErr != nil {
		return nil, status.FromContextError(awaitErr).Err()
	}
	if !ok {
		if s.heldByCheckpoint(in.GetSandboxId()) {
			return nil, errSandboxOperationInProgress(in.GetSandboxId())
		}
		telemetry.ReportCriticalError(ctx, "sandbox not found", nil, telemetry.WithSandboxID(in.GetSandboxId()))

		return nil, status.Errorf(codes.NotFound, "sandbox '%s' not found", in.GetSandboxId())
	}
	// A migration also holds the checkpoint marker, so the routes below
	// refuse a migration begun after this read too.
	if sbx.MigrationInFlight() {
		return nil, migratingStatus(sbx.Runtime.SandboxID)
	}

	ctx = featureflags.AddToContext(ctx, sandboxFlagContexts(sbx)...)

	childSpan.SetAttributes(
		telemetry.WithTeamID(sbx.Runtime.TeamID),
		telemetry.WithTemplateID(sbx.Runtime.TemplateID),
		telemetry.WithFirecrackerVersion(sbx.Config.FirecrackerConfig.FirecrackerVersion),
		telemetry.WithKernelVersion(sbx.Config.FirecrackerConfig.KernelVersion),
		telemetry.WithEnvdVersion(sbx.Config.Envd.Version),
		// The stamp, so a refusal below still carries the cohort; the route
		// decision overwrites it with the device's answer.
		attribute.String("balloon_mode", sbx.BalloonMode()),
	)

	// Check envd version before snapshotting.
	if err := utils.CheckEnvdVersionForSnapshot(sbx.Config.Envd.Version); err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "%s", err.Error())
	}

	// A checkpoint keeps the sandbox running, which a powered-off guest
	// cannot do; only its pause or kill ends it.
	if sbx.GuestPoweredOff() {
		return nil, status.Errorf(codes.FailedPrecondition, "the guest of sandbox '%s' powered off", in.GetSandboxId())
	}

	fsOnly := in.GetFilesystemOnly()
	if err := refuseUnsupportedFilesystemOnly(sbx, fsOnly); err != nil {
		return nil, err
	}

	// A filesystem-only checkpoint behind its flag is refused first, before
	// any admission wait or probe: the answer is permanent and deterministic,
	// and a probe miss must not turn it into a retryable guest-agent refusal.
	if fsOnly && !s.featureFlags.BoolFlag(ctx, featureflags.FilesystemOnlyCheckpointFlag) {
		return nil, filesystemOnlyDisabledStatus(in.GetSandboxId()).Err()
	}

	// The same flag-gated admission pre-flight as Pause; before waitForAcquire
	// so a grace wait never holds a start slot. A filesystem-only checkpoint
	// has no memory parent to wait for, only the latched checks apply.
	if graceMs := s.featureFlags.IntFlag(ctx, featureflags.PauseAdmissionGraceMs); graceMs >= 0 {
		outcome, waited, admitErr := sbx.AwaitSnapshotAdmission(ctx, time.Duration(graceMs)*time.Millisecond, !fsOnly)
		s.recordPauseAdmission(ctx, sbx.Runtime.TeamID, "checkpoint", outcome, waited)
		switch {
		case errors.Is(admitErr, sandbox.ErrSnapshotAdmissionPending):
			sbxlogger.E(sbx).Warn(ctx, "Refusing checkpoint: parent memfile header is still deduplicating", zap.Duration("waited", waited))

			return nil, status.Errorf(codes.ResourceExhausted, "node is busy persisting sandbox '%s', please retry", in.GetSandboxId())
		case admitErr != nil && outcome == "":
			return nil, status.FromContextError(admitErr).Err()
		case admitErr != nil:
			// Unlike Pause, nothing downstream re-checks a latched seal.
			sbxlogger.E(sbx).Warn(ctx, "Refusing checkpoint: sandbox cannot be persisted", zap.Error(admitErr))

			return nil, status.Errorf(codes.FailedPrecondition, "sandbox '%s' cannot be persisted: %s", in.GetSandboxId(), admitErr)
		}
	}

	// Acquire the starting semaphore before resuming, same as Create/Pause.
	if err := s.waitForAcquire(ctx, startGateCheckpoint); err != nil {
		return nil, err
	}
	releaseSlot := sync.OnceFunc(func() { s.startingSandboxes.Release(1) })
	defer releaseSlot()

	// The same envd pre-flight as Pause, always strict: a fork or a template
	// of a hung guest is born dead, a filesystem-only one needs the guest to
	// freeze its rootfs, and the resume-fresh route destroys the original.
	// After the start gate, so the answer is current when the snapshot
	// begins; before the route decision so a refusal claims nothing.
	if healthMs := s.featureFlags.IntFlag(ctx, featureflags.PauseEnvdHealthTimeoutMs); healthMs > 0 {
		if _, refuseErr := s.admitEnvd(ctx, sbx, "checkpoint", time.Duration(healthMs)*time.Millisecond, errSandboxOperationInProgress(in.GetSandboxId()), true); refuseErr != nil {
			return nil, refuseErr
		}
	}

	sbxlogger.E(sbx).Info(ctx, "Checkpointing sandbox", zap.Bool("fs_only", fsOnly))

	// In-place checkpoint (pause, snapshot, resume the SAME FC process) is
	// only honored for sandboxes resumed with use_sync_wp: it skips the
	// snapshot re-load that re-arms write-protection, so dirty tracking
	// across repeated checkpoints relies on the sync-WP serve loop. It also
	// requires the running Firecracker's release contract to include the
	// feature (e2b releases >= 0.2.0 — the CoW memory window drives the
	// balloon free-page-reporting pause API). Everything else takes the
	// resume-fresh flow, so an older FC degrades gracefully rather than
	// erroring.
	//
	// Sandboxes whose balloon runs free-page REPORTING are excluded as well:
	// the CoW window pauses reporting for its lifetime and the deferred
	// reports drain onto the serve loop the moment it resumes, so on these
	// sandboxes the in-place path costs the latency it exists to remove.
	// The mode is the device's, from one read cached per process, so the gate
	// cannot disagree with the running VM or with the memory export, which
	// reads the same cache; the template stamp is only the label. The
	// exclusion applies only while the deferred export would pause reporting:
	// with defer-memory-export off the synchronous copy never touches it and
	// every balloon goes in place. A mode unknown after that read goes
	// resume-fresh: unknown device truth
	// never buys the in-place path. in-place-checkpoint-reporting re-admits
	// reporting balloons, per team, as the way back without a redeploy.
	//
	// A filesystem-only checkpoint exports no memory, so neither the sync-WP
	// dirty tracking, the Firecracker in-place release nor the balloon is
	// involved, and in-place-checkpoint is not consulted: it goes in place
	// behind filesystem-only-checkpoint alone. It has no resume-fresh
	// fallback (resuming a fresh sandbox from a memoryless build would
	// cold-boot it, turning "snapshot my running sandbox" into a reboot), so
	// with its flag off it was refused at the top, before anything is
	// touched, and the API restores the sandbox to Running.
	//
	// Decided here, after admission, so a refused RPC records no route; one
	// decision feeds the span, the gate and the counter.
	var route string
	var balloonMode userfaultfd.BalloonMode
	if fsOnly {
		route, balloonMode = routeInPlace, sbx.BalloonModeValue()
	} else {
		route, balloonMode = s.checkpointRoute(ctx, sbx)
	}
	inPlace := route == routeInPlace
	childSpan.SetAttributes(
		attribute.String("balloon_mode", balloonMode.String()),
		attribute.String("route", route),
	)
	// A caller that went away during the device read must not be answered
	// with resume-fresh under its dead context: that path stops the sandbox
	// on the failure it is about to hit, while the API restores the record.
	if err := ctx.Err(); err != nil {
		sbxlogger.E(sbx).Info(ctx, "checkpoint abandoned by the caller after the route decision, before either path started",
			zap.String("route", route), zap.String("balloon_mode", balloonMode.String()), zap.Error(err))

		return nil, status.FromContextError(err).Err()
	}

	var res *orchestrator.SandboxCheckpointResponse
	var deferred bool
	var err error
	start := time.Now()
	if inPlace {
		res, deferred, err = s.checkpointInPlace(ctx, sbx, in)
	} else {
		res, err = s.checkpointResumeFresh(ctx, sbx, in, releaseSlot)
	}
	// deferred splits the in-place arm by export mechanism (CoW window or
	// synchronous copy), so the three arms of the reporting A/B are one
	// series apart; it is always false on resume-fresh.
	attrs := metric.WithAttributes(
		attribute.Bool("in_place", inPlace),
		attribute.Bool("fs_only", fsOnly),
		attribute.String("route", route),
		attribute.String("balloon_mode", balloonMode.String()),
		attribute.Bool("deferred", deferred),
		attribute.Bool("success", err == nil),
		telemetry.WithTeamID(sbx.Runtime.TeamID),
	)
	s.sandboxCheckpointDuration.Record(ctx, time.Since(start).Milliseconds(), attrs)

	// The denominator for the in_place-labeled duration histograms: what
	// fraction of checkpoints went in-place, at what success rate, and for
	// the rest which gate condition sent them resume-fresh.
	s.sandboxCheckpointCounter.Add(ctx, 1, attrs)

	return res, err
}

// runCheckpointUpload persists the checkpoint snapshot according to the
// PeerToPeerAsyncCheckpointFlag: async returns immediately (peer nodes can
// pull chunks from us during the upload window), sync waits so a failed
// upload is surfaced to the caller — invoking onUploadFailure (nil for the
// in-place flow, whose sandbox stays running regardless: only the checkpoint
// template failed to persist) before returning the error. failureCode is the
// gRPC code an upload failure maps to: the in-place caller passes
// FailedPrecondition (the sandbox is alive and healthy; the API must restore
// it to Running, not kill it), resume-fresh passes Internal (its failure
// closure tears the resumed sandbox down, so a kill describes reality).
func (s *Server) runCheckpointUpload(ctx context.Context, sbx *sandbox.Sandbox, res *snapshotResult, in *orchestrator.SandboxCheckpointRequest, failureCode codes.Code, onUploadFailure func()) error {
	async := s.featureFlags.BoolFlag(ctx, featureflags.PeerToPeerAsyncCheckpointFlag)
	if async && res.memoryExportDeferred {
		// A deferred (CoW window) memory export can still FAIL after this
		// function would have answered success: a cancelled window poisons
		// the promise-backed memfile diff with a permanent
		// ErrDeferredSealFailed, the upload retry classifies it
		// non-retryable and gives up — leaving a build record the control
		// plane believes in with no artifact behind it, by construction
		// forever (there is no build-invalidation path). That hazard ends
		// with the SWEEP, not the upload: once the seal settles successfully
		// the artifact bytes are in the local cache and the deferred diff is
		// exactly as durable as a synchronously copied one — the async
		// path's normal risk profile, peer pull window included. So gate on
		// the seal outcome instead of forcing the whole upload synchronous:
		// wait it out (bounded like the sync path), surface a seal failure
		// to the caller BEFORE any success is reported, and detach the
		// upload once sealed.
		waitCtx, cancelWait := context.WithTimeout(context.WithoutCancel(ctx), uploadTimeout)
		sealErr := res.waitMemorySealed(waitCtx)
		cancelWait()
		if sealErr != nil {
			// The same shape as the sync path failing fast (Run's first act
			// is waiting this promise): end the registered upload with the
			// error — freeing the build's upload future — and surface it
			// under the caller's failure code (FailedPrecondition for
			// in-place: the sandbox is alive, the API restores it to
			// Running). The memfile diff is unusable, so the upload is
			// abandoned rather than finished: nothing will read the
			// snapshot.
			telemetry.ReportCriticalError(ctx, "deferred memory seal failed before checkpoint upload", sealErr, telemetry.WithSandboxID(in.GetSandboxId()))
			res.abandonUpload(ctx, sealErr)
			if onUploadFailure != nil {
				onUploadFailure()
			}

			return status.Errorf(failureCode, "deferred memory export for checkpoint '%s' failed: %s", in.GetSandboxId(), sealErr)
		}
		sbxlogger.I(sbx).Info(ctx, "deferred memory export sealed; detaching checkpoint upload")
	}
	if async {
		// Async: return immediately; peer nodes can pull chunks from us
		// during the upload window.
		s.uploadSnapshotAsync(ctx, sbx, res)

		return nil
	}

	uploadCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), uploadTimeout)
	defer cancel()

	err := res.upload.Run(uploadCtx)
	// A checkpoint whose upload failed is discarded — the API keeps an
	// in-place sandbox running, and onUploadFailure kills a resume-fresh one —
	// so its upload is abandoned rather than finished: nothing will read the
	// snapshot.
	uploadErr := err
	defer func() {
		if uploadErr != nil {
			res.abandonUpload(uploadCtx, uploadErr)

			return
		}
		res.completeUpload(uploadCtx, nil)
	}()

	if err != nil {
		telemetry.ReportCriticalError(ctx, "error uploading snapshot for checkpoint", err, telemetry.WithSandboxID(in.GetSandboxId()))
		if onUploadFailure != nil {
			onUploadFailure()
		}

		return status.Errorf(failureCode, "error uploading snapshot for checkpoint '%s': %s", in.GetSandboxId(), err)
	}

	return nil
}

// checkpointInPlace pauses the sandbox, caches the checkpoint, and resumes it
// IN PLACE — the sandbox keeps running (same ExecutionID and FC process), so
// there is no MarkStopping / stopSandboxAsync / resume-fresh here; Pause
// resumes it before returning, and no SetStopReason / SetStoppedAt is
// recorded for it (both are first-call-wins, and recording them for a
// sandbox that keeps running would mislabel its eventual real stop).
//
// The ONE exception to all of the above is ErrSandboxLost: when the in-place
// resume itself fails, Pause tears the sandbox down (Close, with
// StopReasonKilled set by Pause before the Close) — this function then emits
// the killed event/counter itself, because the teardown's cleanup has already
// MarkStopping-ed the sandbox out of the live map, so the API's follow-up
// Delete finds nothing to attribute.
// checkpointInPlace reports, next to the reply, whether the memory export
// went through the CoW window (true) or the synchronous copy (false); false
// as well when the snapshot failed before that was decided.
func (s *Server) checkpointInPlace(ctx context.Context, sbx *sandbox.Sandbox, in *orchestrator.SandboxCheckpointRequest) (_ *orchestrator.SandboxCheckpointResponse, deferred bool, _ error) {
	// The sandbox stays live and addressable through an in-place checkpoint —
	// that is the point — so there is no MarkStopping to naturally exclude
	// concurrent lifecycle RPCs. The API already serializes them (checkpoint,
	// kill and pause all pass through the sandbox state machine: a second
	// snapshot gets a conflict, a kill waits on the transition), but guard
	// here too so a misbehaving caller cannot race two checkpoints into
	// Pause/CreateSnapshot/ResumeInPlace on the same FC process. The marker
	// is the sandbox's, shared with the resume-fresh route, which holds it
	// from its claim until its resumed sandbox is live: a filesystem-only
	// checkpoint (always in place) and a memory checkpoint routed resume-
	// fresh on the same sandbox refuse each other. FailedPrecondition tells
	// the API the sandbox is still healthy.
	if !sbx.BeginCheckpoint() {
		return nil, false, checkpointInProgressStatus(codes.FailedPrecondition, "a checkpoint is already in progress for sandbox '%s'", in.GetSandboxId()).Err()
	}
	defer sbx.EndCheckpoint()

	// Gate the rootfs seal deferral on the existing defer-rootfs-export flag: when
	// on, the rootfs is swapped to a fresh COW cache and sealed (reflinked) in the
	// background after the guest resumes; when off, the in-place export is
	// synchronous.
	deferRootfsExport := s.featureFlags.BoolFlag(ctx, featureflags.DeferRootfsExportFlag)
	res, err := s.snapshotAndCacheSandbox(
		ctx,
		sbx,
		in.GetBuildId(),
		in.GetMetadata(),
		storage.ObjectOriginSnapshotTemplate,
		in.GetFilesystemOnly(),
		deferRootfsExport,
		false,
		sandboxKeepsRunning,
	)
	if err != nil {
		telemetry.ReportCriticalError(ctx, "error snapshotting sandbox for checkpoint", err, telemetry.WithSandboxID(in.GetSandboxId()))

		// The one path where the orchestrator itself destroyed the sandbox —
		// the final in-place resume failing, which tears the frozen VM down —
		// must report a FATAL code so the API cleans up the row, routing and
		// concurrency slot, instead of restoring a phantom to Running until
		// its expiry.
		if errors.Is(err, sandbox.ErrSandboxLost) {
			// The teardown happened inside Pause (Close, with the stop reason
			// already set); its cleanup MarkStopping-ed the sandbox out of
			// the live map — so the API's follow-up Delete finds nothing and
			// would publish nothing. Emit the terminal surfaces here, like
			// the seal-failure kill above: without them this death has no
			// killed event and no kill-counter sample.
			s.emitSandboxKilled(ctx, sbx, lostSandboxKillReason(err))

			return nil, false, status.Errorf(codes.Internal, "error snapshotting sandbox '%s': %s", in.GetSandboxId(), err)
		}

		// FailedPrecondition, not Internal, for everything else: Pause's
		// resume-on-error cleanup has resumed the VM and restarted health
		// checks, so the sandbox is alive and healthy — only the checkpoint
		// artifact failed. Internal would make both API callers kill it (a
		// transient storage or disk error destroying a running sandbox). A
		// failing resume-on-error cleanup no longer lands here: it tears the
		// sandbox down and tags ErrSandboxLost, taking the branch above.
		return nil, false, status.Errorf(codes.FailedPrecondition, "error snapshotting sandbox '%s': %s", in.GetSandboxId(), err)
	}

	// res.meta.Prefetch stays nil here: the live PrefetchTracker holds the whole
	// workload's fault history (not a cold-start working set), and embedding it
	// made launches from the produced template over-prefetch. The resume-fresh
	// checkpoint gets its mapping from the real resume it performs; in place
	// there is none (the sandbox was already resumed inside Pause, so there is no
	// resume-fresh / lifecycle setup / envd-upgrade / markSandboxLive here — the
	// original sandbox keeps running), so the mapping is harvested below from a
	// throwaway warm resume, exactly as the pause path does. Without it a
	// template produced in place launches with no prefetch at all under the
	// default resume-prefetch-source, and every sandbox from it demand-faults its
	// working set.

	deferred = res.memoryExportDeferred
	if err := s.runCheckpointUpload(ctx, sbx, res, in, codes.FailedPrecondition, nil); err != nil {
		return nil, deferred, err
	}

	s.harvestCheckpointPrefetchAsync(ctx, sbx, res, in)

	s.publishSandboxEvent(ctx, sbx, events.SandboxCheckpointedEvent)

	telemetry.ReportEvent(ctx, "Checkpoint completed")

	return &orchestrator.SandboxCheckpointResponse{
		SchedulingMetadata: res.schedulingMetadata,
		Network:            &orchestrator.SandboxCheckpointNetwork{Placement: sbx.NetworkPlacement},
	}, deferred, nil
}

// harvestCheckpointPrefetchAsync schedules the resume-prefetch harvest for a
// checkpoint taken in place, once its upload has been kicked off or completed:
// the local snapshot is in the cache, the deferred seals (memfile through the
// CoW window, rootfs when deferred rootfs export is on) are awaited by the
// harvest itself, and the consume path waits for the upload before touching
// metadata. Best-effort and off the checkpoint's critical path: the source
// sandbox is already running again and the RPC result is not affected. Skipped
// for a filesystem-only checkpoint, which has no memfile and whose template
// cold-boots — there is no resume working set to harvest.
func (s *Server) harvestCheckpointPrefetchAsync(ctx context.Context, sbx *sandbox.Sandbox, res *snapshotResult, in *orchestrator.SandboxCheckpointRequest) {
	if in.GetFilesystemOnly() {
		return
	}

	s.harvestResumePrefetchAsync(ctx, sbx, res, in.GetBuildId(), res.objectMetadata, harvestSourceCheckpoint)
}

// checkpointResumeFresh snapshots the sandbox and resumes a FRESH sandbox
// from the produced build (new FC process, same ExecutionID). This is the
// pre-in-place checkpoint flow and the fallback whenever in-place is not
// available (async-WP sandbox or in-place-checkpoint flag off).
func (s *Server) checkpointResumeFresh(ctx context.Context, sbx *sandbox.Sandbox, in *orchestrator.SandboxCheckpointRequest, releaseSlot func()) (*orchestrator.SandboxCheckpointResponse, error) {
	return s.checkpointResumeFreshWith(ctx, serverResumeFreshSteps{s: s}, sbx, in, releaseSlot)
}

// checkpointResumeFreshWith is checkpointResumeFresh with the VM steps given.
// releaseSlot returns the starting-sandboxes slot once the resumed sandbox
// is live, so the upload does not hold it; the caller releases it on every
// other path.
func (s *Server) checkpointResumeFreshWith(callerCtx context.Context, steps resumeFreshSteps, sbx *sandbox.Sandbox, in *orchestrator.SandboxCheckpointRequest, releaseSlot func()) (_ *orchestrator.SandboxCheckpointResponse, checkpointErr error) {
	// From the reservation on the old VM is lost on any failure, and the
	// resume is the only way the sandbox survives: a caller giving up during
	// a long snapshot or resume must not decide that. Everything past the
	// reservation runs detached from the caller, on per-phase budgets, ended
	// only by a Delete for the sandbox or by server shutdown. The claim is
	// taken first so no interleaving finds the sandbox neither live nor in
	// flight.
	ctx, op, ok := s.registerDetachedCheckpoint(callerCtx, sbx)
	if !ok {
		return nil, errSandboxOperationInProgress(in.GetSandboxId())
	}

	var (
		reservation *sandbox.Reservation
		rollback    *sandbox.Cleanup
		handoff     *checkpointHandoff
		wentLive    bool
	)
	// A panic must not leak the claim, the reservation or the VMs: it is
	// turned into the error that drives the rollback and raised again once
	// the cleanup is on its way.
	defer func() {
		if r := recover(); r != nil {
			checkpointErr = fmt.Errorf("checkpoint of sandbox '%s' panicked: %v", in.GetSandboxId(), r)
			defer panic(r)
		}
		// The checkpoint's own end is its cause unless a Delete's landed
		// first; what a Delete reads and what is decided here is the same.
		op.cancel(checkpointErr)
		// A checkpoint a Delete ended is that kill: it records the reason
		// and publishes the kill's terminal surfaces, as the in-place path
		// does for its own teardown.
		if killReason, ok := deleteKillToPublish(op.cause(), checkpointErr, op.reservedSandbox(), wentLive); ok {
			sbx.MarkKilledDuringCheckpoint()
			sbxlogger.E(sbx).Info(ctx, "Killed sandbox during checkpoint", zap.String("kill_reason", killReason))
			s.emitSandboxKilled(ctx, sbx, killReason)
		}
		if handoff != nil {
			handoff.settle(context.WithoutCancel(ctx), checkpointErr)
		}
		// A sandbox of this checkpoint's live after a Delete answered with
		// none in hand (the original the map put back after a panic in the
		// reservation) is this checkpoint's to kill.
		s.killForAcknowledgedDelete(ctx, op, steps, in.GetSandboxId())
		if reservation == nil {
			op.release()

			return
		}
		s.finishSandboxStartThen(ctx, reservation, rollback, checkpointErr, op.release)
	}()

	// The resume needs the guest's memory, which a powered-off guest no
	// longer has: refuse while the sandbox is still live and untouched. A
	// power-off after the reservation fails the snapshot instead
	// (WithMemoryRequired), never a disk-only capture.
	if sbx.GuestPoweredOff() {
		return nil, status.Errorf(codes.FailedPrecondition, "the guest of sandbox '%s' powered off", in.GetSandboxId())
	}

	reservation, err := op.reserve(ctx, s.sandboxFactory.Sandboxes, sbx)
	if err != nil {
		if errors.Is(err, sandbox.ErrSandboxOperationInProgress) {
			return nil, errSandboxOperationInProgress(in.GetSandboxId())
		}
		telemetry.ReportCriticalError(ctx, "failed to mark sandbox as stopping", err, telemetry.WithSandboxID(in.GetSandboxId()))

		return nil, status.Errorf(codes.Internal, "failed to checkpoint sandbox '%s'", in.GetSandboxId())
	}

	rollback = sandbox.NewCleanup()
	rollback.Add(ctx, func(ctx context.Context) error { return steps.stopAndClose(ctx, sbx) })

	// Always stop the old sandbox when done — on success the resumed sandbox
	// takes over, on failure this prevents a leaked sandbox that is running
	// but no longer addressable through the map. Stop is idempotent.
	// A sandbox the snapshot gave back to the live map because its guest
	// powered off (ErrPoweredOffHasNoMemory below) is not stopped.
	restored := false
	defer func() {
		if !restored {
			steps.stopAsync(context.WithoutCancel(ctx), sbx)
		}
	}()

	// Set before the snapshot, as in Pause.
	sbx.SetStopReason(sandbox.StopReasonCheckpointing)

	snapshotCtx, cancelSnapshotBudget := context.WithTimeoutCause(ctx, checkpointSnapshotTimeout, errCheckpointSnapshotTimeout)
	res, err := steps.snapshot(snapshotCtx, sbx, in)
	err = withContextCause(snapshotCtx, err)
	cancelSnapshotBudget()
	// A guest that powered off after the check above had nothing captured:
	// the sandbox goes back to the live map as it was, held for its timeout
	// action, instead of through the rollback that would close it.
	if errors.Is(err, sandbox.ErrPoweredOffHasNoMemory) {
		// Cleared before the entry is published: a Delete that takes it must
		// record its kill, which the checkpointing reason would suppress.
		sbx.ClearCheckpointingStopReason()
		restoreErr := reservation.MarkRunning(ctx, sbx)
		if restoreErr == nil {
			rollback = nil
			restored = true
			// Live again: a Delete that ended this checkpoint, or its
			// acknowledged-delete cleanup, owns the one terminal event.
			wentLive = true

			return nil, status.Errorf(codes.FailedPrecondition, "the guest of sandbox '%s' powered off", in.GetSandboxId())
		}
		// Not published: the stop below ends it as the checkpoint's.
		sbx.SetStopReason(sandbox.StopReasonCheckpointing)
		err = errors.Join(err, restoreErr)
	}
	if err != nil {
		s.reportCheckpointStepError(ctx, "error snapshotting sandbox for checkpoint", err, in.GetSandboxId())

		return nil, status.Errorf(codes.Internal, "error snapshotting sandbox '%s': %s", in.GetSandboxId(), err)
	}

	// Every return before runCheckpointUpload leaves the registered upload
	// unrun. The settle above abandons it, so its waiters fail instead of
	// hanging and its template cache pin is returned.
	handoff = &checkpointHandoff{res: res}

	if err := checkpointEnded(ctx, in.GetSandboxId()); err != nil {
		return nil, err
	}

	resumeCtx, cancelResumeBudget := context.WithTimeoutCause(ctx, resumeAfterCheckpointTimeout, errResumeAfterCheckpointTimeout)
	defer cancelResumeBudget()
	ctx = resumeCtx

	// Get the template for resume
	template, releaseTemplate, err := steps.template(ctx, sbx, in.GetBuildId())
	if err != nil {
		err = withContextCause(ctx, err)
		s.reportCheckpointStepError(ctx, "error getting template for resume after checkpoint", err, in.GetSandboxId())

		return nil, status.Errorf(codes.Internal, "error getting template for resume: %s", err)
	}

	rollback.AddNoContext(ctx, func() error {
		releaseTemplate()

		return nil
	})

	resumedSbx, err := steps.resume(ctx, template, sbx)
	if err != nil {
		err = withContextCause(ctx, err)
		s.reportCheckpointStepError(ctx, "error resuming sandbox after checkpoint", err, in.GetSandboxId())

		return nil, status.Errorf(codes.Internal, "error resuming sandbox after checkpoint: %s", err)
	}
	rollback.Add(ctx, func(ctx context.Context) error { return steps.stopAndClose(ctx, resumedSbx) })

	if err := checkpointEnded(ctx, in.GetSandboxId()); err != nil {
		markResumedSandboxEnded(ctx, resumedSbx)

		return nil, err
	}

	// Collect prefetch data immediately after resume while it's most accurate
	prefetchData, prefetchErr := resumedSbx.MemoryPrefetchData(ctx)
	if prefetchErr != nil {
		sbxlogger.I(resumedSbx).Warn(ctx, "failed to get prefetch data for checkpoint", zap.Error(prefetchErr))
	}

	// Setup lifecycle for the resumed sandbox
	s.setupSandboxLifecycle(ctx, resumedSbx, releaseTemplate)

	// resume-time envd live-upgrade. Best-effort and tightly gated so
	// it can never disrupt the universal resume path — except an unrecoverable
	// post-execve failure (new envd left uninitialized), which fails the
	// checkpoint rather than leave a bricked sandbox.
	if _, upErr := s.maybeUpgradeEnvd(ctx, resumedSbx); upErr != nil {
		resumedSbx.SetStopReason(sandbox.StopReasonKilled)
		upErr = withContextCause(ctx, upErr)
		s.reportCheckpointStepError(ctx, "error upgrading envd after checkpoint", upErr, in.GetSandboxId())

		return nil, status.Errorf(codes.Internal, "error upgrading envd after checkpoint: %s", upErr)
	}

	if err := checkpointEnded(ctx, in.GetSandboxId()); err != nil {
		markResumedSandboxEnded(ctx, resumedSbx)

		return nil, err
	}

	// Promote to the live registry now that any resume-time upgrade's post-/init
	// has restored auth — the sandbox was resumed with routing deferred. The
	// lifecycle is the claim's before it is published, so a Delete that finds
	// it live while the registration notifies subscribers owns it; the
	// registration refuses once a Delete's cancellation has landed.
	op.setRegistered(resumedSbx.LifecycleID)
	if err := s.markSandboxLive(ctx, resumedSbx, reservation, true); err != nil {
		err = withContextCause(ctx, err)
		s.reportCheckpointStepError(ctx, "error registering resumed sandbox after checkpoint", err, in.GetSandboxId())

		return nil, status.Errorf(codes.Internal, "error registering resumed sandbox after checkpoint: %s", err)
	}
	// The sandbox is live: the start is over, as Create defines it, and a
	// Delete finds it in the live map from here on. The detached phase is
	// over with it, so the rest runs on no budget, as the snapshot's did.
	wentLive = true
	cancelResumeBudget()
	ctx = context.WithoutCancel(ctx)
	releaseSlot()
	// A Delete that answered before this publication, with nothing live at
	// its final read, is honoured here: the claim is still held, so no
	// waiting Delete can take the sandbox meanwhile.
	s.killForAcknowledgedDelete(ctx, op, steps, in.GetSandboxId())
	op.release()

	// Embed prefetch data into the metadata so it's uploaded with the snapshot files in a single pass.
	if prefetchErr == nil {
		prefetchMapping := metadata.PrefetchEntriesToMapping(slices.Collect(maps.Values(prefetchData.BlockEntries)), prefetchData.BlockSize)
		if prefetchMapping != nil {
			res.meta = res.meta.WithPrefetch(&metadata.Prefetch{
				Memory: prefetchMapping,
				Origin: metadata.PrefetchOriginCheckpoint,
			})

			if err := s.templateCache.UpdateMetadata(ctx, in.GetBuildId(), res.meta); err != nil {
				sbxlogger.I(resumedSbx).Warn(ctx, "failed to update local metadata with prefetch", zap.Error(err))
			}
		}
	}

	// On upload failure, tear down the resumed sandbox — without a persisted
	// snapshot it cannot be paused or resumed later.
	handoff.handOff()
	if err := s.runCheckpointUpload(ctx, resumedSbx, res, in, codes.Internal, func() {
		s.killResumedOnUploadFailure(ctx, resumedSbx)
	}); err != nil {
		return nil, err
	}

	s.publishSandboxEvent(ctx, resumedSbx, events.SandboxCheckpointedEvent)

	telemetry.ReportEvent(ctx, "Checkpoint completed")

	// The resumed VM was placed again, possibly on another network.
	return &orchestrator.SandboxCheckpointResponse{
		SchedulingMetadata: res.schedulingMetadata,
		Network:            &orchestrator.SandboxCheckpointNetwork{Placement: resumedSbx.NetworkPlacement},
	}, nil
}

// Extracts common data needed for sandbox events
func (s *Server) prepareSandboxEventData(ctx context.Context, sbx *sandbox.Sandbox) (uuid.UUID, string, int64, map[string]any) {
	teamID, err := uuid.Parse(sbx.Runtime.TeamID)
	if err != nil {
		sbxlogger.I(sbx).Error(ctx, "error parsing team ID", logger.WithSandboxID(sbx.Runtime.SandboxID), zap.Error(err))
	}

	buildId := ""
	eventsTTLDays := int64(0)
	eventData := make(map[string]any)
	if sbx.APIStoredConfig != nil {
		buildId = sbx.APIStoredConfig.GetBuildId()
		eventsTTLDays = sbx.APIStoredConfig.GetEventsTtlDays()
		if sbx.APIStoredConfig.Metadata != nil {
			// Copy the map to avoid race conditions
			eventData["sandbox_metadata"] = utils.ShallowCopyMap(sbx.APIStoredConfig.GetMetadata())
		}
	}

	return teamID, buildId, eventsTTLDays, eventData
}

func (s *Server) getSandboxExecutionData(sbx *sandbox.Sandbox) map[string]any {
	// ExecutionStartedAt survives Checkpoint's handler rebuild, which resets StartedAt, so the earlier of the two is the execution start.
	startedAt := sbx.GetStartedAt()
	if executionStartedAt := sbx.GetExecutionStartedAt(); !executionStartedAt.IsZero() && executionStartedAt.Before(startedAt) {
		startedAt = executionStartedAt
	}

	return map[string]any{
		"started_at":     startedAt.UTC().Format(time.RFC3339),
		"vcpu_count":     sbx.Config.Vcpu,
		"memory_mb":      sbx.Config.RamMB,
		"execution_time": time.Since(startedAt).Milliseconds(),
	}
}

// killResumedOnUploadFailure takes a resumed sandbox whose snapshot upload
// failed out of the live map and publishes the kill's terminal surfaces: the
// sandbox is live and the API's follow-up Delete will find nothing to kill.
// A Delete that took it out first has published them.
func (s *Server) killResumedOnUploadFailure(ctx context.Context, resumedSbx *sandbox.Sandbox) {
	resumedSbx.SetStopReason(sandbox.StopReasonKilled)
	if s.sandboxFactory.Sandboxes.MarkStopping(ctx, resumedSbx.Runtime.SandboxID, resumedSbx.LifecycleID) {
		s.emitSandboxKilled(ctx, resumedSbx, killReasonUploadFailed)
	}
}

// checkpointHandoff owns a checkpoint's registered upload until it is handed
// to runCheckpointUpload, and abandons it if the checkpoint returns first.
type checkpointHandoff struct {
	res    *snapshotResult
	handed bool
}

// handOff passes the upload on; settle leaves it alone from then on.
func (h *checkpointHandoff) handOff() { h.handed = true }

// settle abandons the upload unless it was handed off, failing its waiters
// with cause, the error the checkpoint returns.
func (h *checkpointHandoff) settle(ctx context.Context, cause error) {
	if h.handed {
		return
	}
	if cause == nil {
		cause = errors.New("no error returned")
	}

	h.res.abandonUpload(ctx, fmt.Errorf("checkpoint abandoned before its upload started: %w", cause))
}

// snapshotResult holds the data produced by snapshotAndCacheSandbox that
// callers need to start the background remote storage upload.
type snapshotResult struct {
	meta               metadata.Template
	schedulingMetadata *orchestrator.SchedulingMetadata
	upload             *sandbox.Upload
	// completeUpload finishes the upload, reporting it landed or failed, and
	// only uploadSnapshotAsync, whose pause or checkpoint the API already
	// relies on, calls it with an error. abandonUpload ends an upload whose
	// snapshot nothing will read — one that will never run, or a checkpoint's
	// that failed: it fails the upload's waiters with the error and reports it
	// abandoned. Both return the template cache pin.
	completeUpload func(ctx context.Context, uploadErr error)
	abandonUpload  func(ctx context.Context, err error)
	// rootfsDiff is the snapshot's rootfs diff. With deferred export it is a
	// promise-backed diff that resolves only once the background seal finishes,
	// so the prefetch harvest waits on its CachePath before its throwaway resume
	// (a warm resume reads the rootfs). Nil-safe callers only: always set here.
	rootfsDiff build.Diff
	// memoryDiff is the snapshot's memory diff. With the memfd background copy
	// its CachePath resolves only once the copy has finished streaming into the
	// build directory, which is when the capture's bytes are all on disk.
	memoryDiff build.Diff
	// objectMetadata is the storage object metadata the snapshot was uploaded
	// with. The prefetch harvest reuses it verbatim when re-uploading the
	// metadata object, so the two can never drift.
	objectMetadata storage.ObjectMetadata
	// filesystemOnly records whether this was a filesystem-only (memoryless)
	// pause, so the async upload can label its failure counter with fs_only.
	filesystemOnly bool
	// memoryExportDeferred records that the memfile diff is promise-backed
	// (CoW window) and can still fail after Pause returned — see
	// runCheckpointUpload, which must not answer async success before
	// waitMemorySealed (Snapshot.WaitMemorySealed) settles.
	memoryExportDeferred bool
	waitMemorySealed     func(ctx context.Context) error
	// advertised records that peers were offered the snapshot from this
	// node's cache while its upload runs.
	advertised bool
}

// afterSnapshot is what happens to a sandbox once its snapshot is taken.
type afterSnapshot int

const (
	// sandboxStops stops the sandbox (pause, resume-fresh checkpoint).
	sandboxStops afterSnapshot = iota
	// sandboxKeepsRunning resumes it in place (sandbox.WithMaintainSandbox).
	sandboxKeepsRunning
	// sandboxStaysPaused leaves it paused for the caller (sandbox.WithRetainedPause).
	sandboxStaysPaused
)

// snapshotAndCacheSandbox creates a snapshot of a sandbox and adds it to the
// local template cache. The caller is responsible for starting the remote
// storage upload via uploadSnapshotAsync.
//
// With sandboxStaysPaused, the caller owns the paused sandbox whenever Pause
// succeeded, including when this returns a cache or upload setup error.
func (s *Server) snapshotAndCacheSandbox(
	ctx context.Context,
	sbx *sandbox.Sandbox,
	buildID string,
	provenance map[string]string,
	buildOrigin storage.ObjectOrigin,
	filesystemOnly bool,
	deferRootfsExport bool,
	envdUnresponsive bool,
	after afterSnapshot,
	extra ...sandbox.PauseOption,
) (*snapshotResult, error) {
	meta, err := sbx.Template.Metadata()
	if err != nil {
		return nil, fmt.Errorf("no metadata found in template: %w", err)
	}

	meta = meta.SameVersionTemplate(metadata.TemplateMetadata{
		BuildID:            buildID,
		KernelVersion:      sbx.Config.FirecrackerConfig.KernelVersion,
		FirecrackerVersion: sbx.Config.FirecrackerConfig.FirecrackerVersion,
	})

	var pauseOpts []sandbox.PauseOption
	if filesystemOnly {
		pauseOpts = append(pauseOpts, sandbox.WithFilesystemSnapshot())
	}
	if deferRootfsExport {
		pauseOpts = append(pauseOpts, sandbox.WithDeferredRootfsExport())
	}
	if envdUnresponsive {
		pauseOpts = append(pauseOpts, sandbox.WithEnvdUnresponsive())
	}
	switch after {
	case sandboxKeepsRunning:
		pauseOpts = append(pauseOpts, sandbox.WithMaintainSandbox())
	case sandboxStaysPaused:
		pauseOpts = append(pauseOpts, sandbox.WithRetainedPause())
	}
	pauseOpts = append(pauseOpts, extra...)

	snapshot, err := sbx.Pause(ctx, meta, sandbox.SnapshotUseCasePause, pauseOpts...)
	if err != nil {
		return nil, fmt.Errorf("error snapshotting sandbox: %w", err)
	}
	// The prefetch writers rewrite the metafile from res.meta, so it must carry
	// what Pause stamped (vCPU count, balloon mode), not the pre-pause copy.
	meta = snapshot.Metadata

	// The caller owns a retained guest from here, failed or not, and must
	// know it is paused.
	pausedFor := func(err error) error {
		if after == sandboxStaysPaused {
			return errors.Join(errRetainedPaused, err)
		}

		return err
	}

	finishUpload, err := s.templateCache.AddSnapshot(
		ctx,
		meta.Template.BuildID,
		snapshotLineage(buildOrigin, sbx.Template, after != sandboxStops),
		snapshot.MemorySnapshot.DiffHeader,
		snapshot.RootfsDiffHeader,
		snapshot.Snapfile,
		snapshot.Metafile,
		snapshot.MemorySnapshot.Diff,
		snapshot.RootfsDiff,
		snapshot.MemorySnapshot.ProvisionalDiffHeader,
		snapshot.MemorySnapshot.ProvisionalDiff,
		snapshot.MemorySnapshot.ProvisionalCreatedAt,
		snapshot.MemorySnapshot.ProvisionalSwapDone,
	)
	if err != nil {
		return nil, pausedFor(fmt.Errorf("error adding snapshot to template cache: %w", err))
	}

	// Caller-supplied provenance (e.g. template_id) is forwarded as-is; team and
	// origin are orchestrator-authoritative and set last so they always win.
	objectMetadata := storage.ObjectMetadata{}
	maps.Copy(objectMetadata, provenance)
	objectMetadata[storage.ObjectMetadataTeamID] = sbx.Runtime.TeamID
	objectMetadata[storage.ObjectMetadataBuildOrigin] = string(buildOrigin)

	// Register the upload only after the snapshot is in the local cache, so a
	// failed AddSnapshot doesn't leave an orphan future blocking re-registration.
	upload, err := sandbox.NewUpload(ctx, s.uploads, snapshot, s.persistence, s.config.StorageConfig.CompressConfig, s.featureFlags, storage.UseCasePause, objectMetadata, finishUpload)
	if err != nil {
		return nil, pausedFor(fmt.Errorf("register upload: %w", err))
	}

	telemetry.ReportEvent(ctx, "added snapshot to template cache")

	completeUpload, abandonUpload, advertised := s.advertiseSnapshot(ctx, upload, meta.Template.BuildID, after == sandboxStaysPaused)

	return &snapshotResult{
		meta:                 meta,
		schedulingMetadata:   snapshot.SchedulingMetadata,
		upload:               upload,
		completeUpload:       completeUpload,
		abandonUpload:        abandonUpload,
		objectMetadata:       objectMetadata,
		filesystemOnly:       filesystemOnly || snapshot.FilesystemSnapshot,
		rootfsDiff:           snapshot.RootfsDiff,
		memoryDiff:           snapshot.MemorySnapshot.Diff,
		memoryExportDeferred: snapshot.MemoryExportDeferred,
		waitMemorySealed:     snapshot.WaitMemorySealed,
		advertised:           advertised,
	}, nil
}

// advertiseSnapshot offers buildID's cached snapshot to peers while its upload
// runs, when peer transfer is on, and returns the calls that end the upload:
// complete with its outcome, or abandon when nothing will read the snapshot,
// and whether the offer was registered. Either withdraws the offer, except
// after a retained capture's failed upload: this node may hold the only copy,
// so peers can read it while the key and the local cache entry last.
func (s *Server) advertiseSnapshot(ctx context.Context, upload *sandbox.Upload, buildID string, retained bool) (complete, abandon func(context.Context, error), advertised bool) {
	// Capture once so Register and the symmetric Unregister inside
	// completeUpload don't drift if the flag flips mid-upload.
	peerEnabled := s.featureFlags.BoolFlag(ctx, featureflags.PeerToPeerChunkTransferFlag)

	finishPeer := func(ctx context.Context, landed bool) {
		if !peerEnabled {
			return
		}

		// Only advertise the build as fully uploaded when it actually landed.
		// On abandon/failure the bytes are not in storage, so marking it would
		// make chunk-serving falsely report "already uploaded".
		if landed {
			s.uploadedBuilds.Set(buildID, struct{}{}, ttlcache.DefaultTTL)
		}

		if err := s.peerRegistry.Unregister(ctx, buildID); err != nil {
			logger.L().Warn(ctx, "failed to unregister peer address from routing", logger.WithBuildID(buildID), zap.Error(err))
		}
	}

	complete = func(ctx context.Context, uploadErr error) {
		upload.Finish(ctx, uploadErr)
		if uploadErr != nil && retained {
			return
		}
		finishPeer(ctx, uploadErr == nil)
	}

	abandon = func(ctx context.Context, err error) {
		upload.Abandon(ctx, err)
		finishPeer(ctx, false)
	}

	if peerEnabled {
		if err := s.peerRegistry.Register(ctx, buildID, redisPeerKeyTTL); err != nil {
			logger.L().Warn(ctx, "failed to register peer address for routing", logger.WithBuildID(buildID), zap.Error(err))
		} else {
			advertised = true
		}
	}

	return complete, abandon, advertised
}

// snapshotLineage describes the snapshot of a sandbox running on predecessor.
// The operation leaves predecessor behind unless it maintains the sandbox, as
// an in-place checkpoint does.
func snapshotLineage(origin storage.ObjectOrigin, predecessor sbxtemplate.Template, maintainSandbox bool) sbxtemplate.SnapshotLineage {
	return sbxtemplate.SnapshotLineage{
		Origin:              origin,
		Predecessor:         predecessor.Files().CacheKey(),
		AbandonsPredecessor: !maintainSandbox,
	}
}

// uploadSnapshotAsync uploads snapshot files to remote storage in the
// background and cleans up the Redis peer key once done. Used by the Pause
// handler where no prefetch data is available.
func (s *Server) uploadSnapshotAsync(ctx context.Context, sbx *sandbox.Sandbox, res *snapshotResult) {
	// Detach from the request: the upload retries for up to uploadTotalBudget.
	// A graceful shutdown waits for it to finish (see Server.Close via uploadsWG)
	// rather than cancelling, so an in-flight snapshot isn't dropped on restart.
	uploadCtx := context.WithoutCancel(ctx)

	releaseWork := s.info.TrackWork()
	s.uploadsInFlight.Add(1)
	s.uploadsWG.Go(func() {
		defer releaseWork()
		defer s.uploadsInFlight.Add(-1)

		spanCtx, span := tracer.Start(uploadCtx, "upload snapshot")
		defer span.End()

		err := retry.Do(
			spanCtx,
			defaultUploadRetryPolicy(),
			isRetryableUploadErr,
			res.upload.Run,
			func(attempt int, backoff time.Duration, err error) {
				sbxlogger.I(sbx).Warn(spanCtx, "snapshot upload attempt failed, retrying",
					zap.Int("attempt", attempt),
					zap.Duration("backoff", backoff),
					zap.Error(err),
				)
			},
		)
		if err != nil {
			sbxlogger.I(sbx).Error(spanCtx, "snapshot upload did not durably land", zap.Error(err))
			s.uploadFailedCounter.Add(spanCtx, 1, metric.WithAttributes(
				attribute.Bool("fs_only", res.filesystemOnly),
				telemetry.WithTeamID(sbx.Runtime.TeamID),
			))
		} else {
			sbxlogger.I(sbx).Info(spanCtx, "snapshot finished uploading successfully")
		}

		res.completeUpload(spanCtx, err)
	})
}

type sandboxTeardown interface {
	Stop(ctx context.Context) error
	Wait(ctx context.Context) error
	Close(ctx context.Context) error
}

func stopAndCloseSandbox(ctx context.Context, sbx sandboxTeardown) error {
	ctx = context.WithoutCancel(ctx)
	stopErr := sbx.Stop(ctx)
	// Stop only signals UFFD; drain must wait for its exit.
	waitErr := sbx.Wait(ctx)
	closeErr := sbx.Close(ctx)

	return errors.Join(stopErr, waitErr, closeErr)
}

func (s *Server) finishSandboxStart(ctx context.Context, reservation *sandbox.Reservation, rollback *sandbox.Cleanup, operationErr error) {
	s.finishSandboxStartThen(ctx, reservation, rollback, operationErr, nil)
}

// finishSandboxStartThen is finishSandboxStart with then run once the
// reservation is released, after the rollback when one runs.
func (s *Server) finishSandboxStartThen(ctx context.Context, reservation *sandbox.Reservation, rollback *sandbox.Cleanup, operationErr error, then func()) {
	if then == nil {
		then = func() {}
	}
	if operationErr == nil || rollback == nil {
		reservation.Release()
		then()

		return
	}

	releaseWork := s.info.TrackWork()
	go func() {
		defer releaseWork()
		defer then()
		defer reservation.Release()
		ctx := context.WithoutCancel(ctx)
		if err := rollback.Run(ctx); err != nil {
			telemetry.ReportCriticalError(ctx, "failed to clean up sandbox start", err)
		}
	}()
}

// markSandboxLive publishes sbx under its reservation. unlessEnded refuses
// the publication once ctx has ended, for an operation a Delete may have
// ended and answered on (checkpoint_detached.go); a sandbox refused there
// ended for the operation's cause, a Delete's kill included, as on the
// checkpoint's exits before it.
func (s *Server) markSandboxLive(ctx context.Context, sbx *sandbox.Sandbox, reservation *sandbox.Reservation, unlessEnded bool) error {
	register := reservation.MarkRunning
	if unlessEnded {
		register = reservation.MarkRunningUnlessEnded
	}
	if err := register(ctx, sbx); err != nil {
		if unlessEnded {
			markResumedSandboxEnded(ctx, sbx)
		} else {
			sbx.SetStopReason(sandbox.StopReasonRegistrationFailed)
		}

		return err
	}

	go sbx.Checks().Start(context.WithoutCancel(ctx))

	return nil
}

// registrationRefused is a Create's answer to a refused registration: an ID
// taken on this node answers AlreadyExists, as it does at the reservation.
func (s *Server) registrationRefused(ctx context.Context, sandboxID, executionID string, err error) error {
	// Also matches the lost-reservation wrap, unreachable while the start holds its reservation.
	if errors.Is(err, sandbox.ErrSandboxAlreadyRunning) {
		return s.sandboxAlreadyRunning(ctx, sandboxID, executionID, err)
	}

	return status.Errorf(codes.Internal, "failed to register sandbox: %s", err)
}

func (s *Server) sandboxAlreadyRunning(ctx context.Context, sandboxID, executionID string, cause error) error {
	telemetry.ReportCriticalError(ctx, "refusing to create sandbox: its ID is already taken on this node", cause,
		telemetry.WithSandboxID(sandboxID),
		attribute.String("requested_execution_id", executionID),
	)

	return status.Errorf(codes.AlreadyExists, "sandbox '%s' is already running on this node: %s", sandboxID, cause)
}

// setupSandboxLifecycle starts the goroutine that waits for the sandbox to end
// and tears it down. releaseTemplate lifts this sandbox's pin on its template;
// it runs after the sandbox is closed, on every ending (kill, pause, checkpoint
// hand-off, crash), and is idempotent so the caller's error rollback may also
// hold a reference to it.
func (s *Server) setupSandboxLifecycle(ctx context.Context, sbx *sandbox.Sandbox, releaseTemplate func()) {
	s.sandboxFactory.Sandboxes.TrackLifecycle(ctx, sbx)
	releaseWork := s.info.TrackWork()
	go func() {
		defer releaseWork()

		// Deferred so it runs only after the body below has closed the sandbox:
		// the template must not become evictable while the sandbox is still
		// using it, since eviction deletes the snapfile. A reboot in place
		// hands the pin to the next lifecycle instead.
		release := releaseTemplate
		defer func() {
			if release != nil {
				release()
			}
		}()

		ctx, childSpan := tracer.Start(context.WithoutCancel(ctx), "stop sandbox-lifecycle", trace.WithNewRoot())
		defer childSpan.End()

		waitErr := sbx.Wait(ctx)
		if waitErr != nil {
			sbxlogger.I(sbx).Error(ctx, "failed to wait for sandbox, cleaning up", zap.Error(waitErr))
		}

		stoppedAt := time.Now()
		sbx.SetStoppedAt(stoppedAt)

		// A guest that dies cleanly leaves no wait error, so the log above
		// misses it.
		stoppedLine := "Sandbox stopped"
		var cause fc.CrashCause
		if sbx.GetStopReason() == sandbox.StopReasonCrashed {
			cause = s.recordCrash(ctx, sbx, waitErr)
			crashCode := guestCrashCode(sbx.FirecrackerExit())
			stoppedLine = stoppedSandboxLine(sbx.FirecrackerExit(), cause, crashCode)
		}
		// A lifecycle that never went live is not the API's: it is neither
		// kept for the API's timeout action, nor booted again, nor listed as
		// ended. A restore the migration never promoted shares its execution
		// with the source, which an ended entry would have the API kill.
		if sbx.GetStopReason() == sandbox.StopReasonCrashed && sbx.WentLive() {
			crashCode := guestCrashCode(sbx.FirecrackerExit())
			if keepsOnPowerOff(cause, waitErr) {
				stoppedLine = s.holdPoweredOff(ctx, sbx)
			}

			// Before Close: the next lifecycle takes this one's slot and
			// overlay, and Close then releases only what it kept.
			if rebootsOnReset(cause, waitErr) {
				var next *sandbox.Sandbox
				next, stoppedLine = s.rebootGuest(ctx, sbx, cause.GuestEvent(), crashCode, stoppedAt)
				if next != nil {
					s.setupSandboxLifecycle(ctx, next, release)
					release = nil
				}
			}
		}

		// Every ending — kill, pause, checkpoint hand-off, crash — passes here.
		s.recordExecutionDuration(ctx, sbx)

		// The proxy's connections end with the lifecycle: reset when it stopped,
		// joined when its network slot is released.
		cleanupErr := sbx.Close(ctx)
		if cleanupErr != nil {
			sbxlogger.I(sbx).Error(ctx, "failed to cleanup sandbox, will remove from cache", zap.Error(cleanupErr))
		}

		// List reports the end now that Close has released everything the
		// sandbox held.
		if s.listsAsEnded(ctx, sbx) {
			s.sandboxFactory.Sandboxes.AddEnded(sbx, cause.GuestEvent(), time.Now())
		}

		sbxlogger.E(sbx).Info(ctx, stoppedLine)
	}()
}

// guestCrashCode is the code of the last crash the guest reported, 0 for
// none or an exit not yet reaped.
func guestCrashCode(exit *fc.ExitInfo) uint64 {
	if exit == nil {
		return 0
	}

	return exit.GuestCrashCode
}

// endsUnrequested reports whether a closed lifecycle ended with nobody asking
// for it: no kill, pause or checkpoint, no reboot in place, no power-off hold
// ended by the API. Build sandboxes are not the API's, so never.
func endsUnrequested(sbx *sandbox.Sandbox) bool {
	return sbx.GetStopReason() == sandbox.StopReasonCrashed && sbx.APIStoredConfig != nil
}

// listsAsEnded reports whether the closed lifecycle sbx is listed as ended
// for the API: it ended unrequested while the API had it running, so a
// lifecycle that never went live is not. A held power-off is listed under
// its own flag, and every other unrequested end only when the API is meant
// to end it. A restore a migration never promoted shares its execution with the
// source, which an ended entry would have the API kill.
func (s *Server) listsAsEnded(ctx context.Context, sbx *sandbox.Sandbox) bool {
	if !endsUnrequested(sbx) || !sbx.WentLive() {
		return false
	}

	return sbx.GuestPoweredOff() || s.featureFlags.BoolFlag(ctx, featureflags.SandboxEndedOnNodeFlag)
}

// stopSandboxAsync stops the sandbox in a background goroutine.
func (s *Server) stopSandboxAsync(ctx context.Context, sbx *sandbox.Sandbox) {
	releaseWork := s.info.TrackWork()
	go func() {
		defer releaseWork()

		ctx, childSpan := tracer.Start(context.WithoutCancel(ctx), "stop sandbox-async", trace.WithNewRoot())
		defer childSpan.End()

		err := sbx.Stop(ctx)
		if err != nil {
			sbxlogger.I(sbx).Error(ctx, "error stopping sandbox", zap.Error(err))
		}
	}()
}

// publishSandboxEvent publishes a sandbox event in the background.
func (s *Server) publishSandboxEvent(ctx context.Context, sbx *sandbox.Sandbox, eventType string) {
	teamID, buildId, eventsTTLDays, eventData := s.prepareSandboxEventData(ctx, sbx)

	s.publishEventAsync(
		ctx,
		teamID,
		events.SandboxEvent{
			Version:   events.StructureVersionV2,
			ID:        uuid.New(),
			Type:      eventType,
			Timestamp: time.Now().UTC(),

			EventData:          eventData,
			SandboxID:          sbx.Runtime.SandboxID,
			SandboxExecutionID: sbx.Runtime.ExecutionID,
			SandboxTemplateID:  sbx.Config.BaseTemplateID,
			SandboxBuildID:     buildId,
			SandboxTeamID:      teamID,
			EventsTTLDays:      eventsTTLDays,
		},
	)
}

func (s *Server) publishEventAsync(ctx context.Context, teamID uuid.UUID, event events.SandboxEvent) {
	releaseWork := s.info.TrackWork()
	go func() {
		defer releaseWork()

		s.sbxEventsService.Publish(context.WithoutCancel(ctx), teamID, event)
	}()
}

// recordUpgradePhase records one live-upgrade phase's wall-time. The phases hold
// independent budgets, so the combined upgrade.duration histogram cannot say
// which of them a slow upgrade is approaching; this can. result is the phase's
// own outcome, not the upgrade's.
//
// It covers `resolve` as well as the two budgeted phases, because resolve is
// where the target's version is probed and that probe is the largest cost on a
// cold node: exec'ing the binary off the gcsfuse mount demand-pages it in small
// reads, which costs far more than reading the same bytes sequentially. It also
// sits BEFORE the combined histogram starts timing, so without
// this phase the dominant cost of a cold upgrade appears in no metric at all.
func (s *Server) recordUpgradePhase(ctx context.Context, teamID, phase, result string, start time.Time) {
	s.envdUpgradePhaseDuration.Record(ctx, time.Since(start).Milliseconds(),
		metric.WithAttributes(
			attribute.String("phase", phase),
			attribute.String("result", result),
			telemetry.WithTeamID(teamID),
		))
}

// resolvePhaseResult labels the resolve phase, and reports whether it is worth
// recording at all.
//
// recorded is false for the reasons the resolver returns WITHOUT calling
// getVersion: those samples time a string comparison and a stat, and mixing them
// with the ones that paid for an exec off the mount makes the distribution
// meaningless. Every reason that did probe -- or that deliberately did not, having
// consulted the cache -- is labelled as itself, so the series can be cut by what
// actually happened.
func resolvePhaseResult(path, reason string) (string, bool) {
	if path != "" {
		return "success", true
	}
	switch reason {
	case "off", "not_staged", "invalid_target":
		// Returned before getVersion; nothing was measured.
		return "", false
	default:
		// same_version, downgrade, getversion_failed all probed; binary_not_cached
		// consulted the cache and deliberately did not; source_stalled waited on the
		// mount up to its stat budget.
		return reason, true
	}
}

// phaseResult labels a phase by whether it errored.
func phaseResult(err error) string {
	if err != nil {
		return "failed"
	}

	return "success"
}

// recordDeliveryPhase records the delivery phase. It takes CallEnvdUpgrade's two
// return values rather than a label, so the delivery phase cannot be recorded
// with the two-outcome phaseResult -- which read an expired budget as a success
// and hid the one stall this phase was added to expose. The mistake is now a
// compile error rather than something a test has to catch.
func (s *Server) recordDeliveryPhase(ctx context.Context, teamID string, execConfirmed bool, err error, start time.Time) {
	s.recordUpgradePhase(ctx, teamID, "deliver", deliveryResult(execConfirmed, err), start)
}

// deliveryResult labels the delivery phase, which has three outcomes rather than
// two. CallEnvdUpgrade returns a nil error when the deadline fired or the parent
// was cancelled -- deliberately, because envd may still be mid-handover, so the
// caller must keep a follow-up not-ready recoverable. Labelling that "success"
// would hide the exact stall this phase exists to expose: a delivery that ran out
// its budget looks identical to one that finished.
func deliveryResult(execConfirmed bool, err error) string {
	switch {
	case err != nil:
		return "failed"
	case !execConfirmed:
		return "unconfirmed"
	default:
		return "success"
	}
}

// maybeUpgradeEnvd is the orchestrator's resume-time envd live-upgrade trigger
// . At resume it asks EnvdUpgradeTargetFlag whether the sandbox's envd
// should be swapped for a newer node-local build and, if so, delivers that
// binary into the guest and triggers envd's same-PID self-upgrade so the
// workload is preserved.
//
// Fully best-effort: recover()-wrapped, bounded timeouts, and every failure is
// logged-and-swallowed so it can never disrupt the universal resume path. The
// flag fallback is "off", so with no LaunchDarkly (e.g. dev) this is inert.
//
// It returns whether an upgrade actually completed (for the resume-latency
// label) and emits the rollout metrics: orchestrator.envd.upgrade.attempts
// {result,from_version,to_version}, .duration{result}, .gated{reason}, and
// .phase.duration{phase,result}, which splits the duration into the resolve,
// deliver and ready phases so a stalled delivery is not read as a slow probe.
func (s *Server) maybeUpgradeEnvd(ctx context.Context, sbx *sandbox.Sandbox) (upgraded bool, fatalErr error) {
	// Decide, label, and confirm against the version the running envd actually
	// reports (captured on the resume-path /init) — not the template built-with,
	// which never changes across live upgrades and would otherwise re-trigger the
	// handover on every resume. Fall back to built-with only when no live version
	// was captured (an envd too old to report it — which the gate then rejects).
	from := sbx.LiveEnvdVersion()
	if from == "" {
		from = sbx.Config.Envd.Version
	}

	var (
		attempted bool
		toVersion string
		result    = "success"
		start     = time.Now()
	)
	defer func() {
		if r := recover(); r != nil {
			sbxlogger.I(sbx).Error(ctx, "envd auto-upgrade panic (recovered)", zap.Any("panic", r))
			result = "panic"
			attempted = true
			upgraded = false
		}
		if attempted {
			s.envdUpgradeAttempts.Add(ctx, 1, metric.WithAttributes(
				attribute.String("result", result),
				attribute.String("from_version", from),
				attribute.String("to_version", toVersion),
			))
			s.envdUpgradeDuration.Record(ctx, time.Since(start).Milliseconds(),
				metric.WithAttributes(
					attribute.String("result", result),
					telemetry.WithTeamID(sbx.Runtime.TeamID),
				))
		}
	}()

	// The flag first, and on its own: resolving a target stats up to three paths and execs
	// the candidate for its version, and a cluster with the flag off must pay none of that.
	target := featureflags.EnvdUpgradeTarget(ctx, s.featureFlags)
	if featureflags.EnvdUpgradeTargetDisabled(target) {
		return false, nil
	}

	// A guest the staged envd binary does not run on keeps the envd its
	// template carries.
	if sbx.Config.GuestProfile().Capabilities().NoEnvdLiveUpgrade {
		return false, nil
	}

	// The *running* envd must already have the /upgrade endpoint + handover code,
	// else the delivery POST would 404 or hang. Count the skip so a ramp can see
	// the gated population.
	//
	// Ahead of the resolver because it is a comparison against a version already in memory.
	// It also makes the counter mean what its comment says: behind the resolver, a not_staged
	// node or an already-current sandbox pre-empted this label, so the "gated population" it
	// claims to size was systematically undercounted. With the binary cache in front of the
	// probe there is a second reason: resolving first would let a transient cache miss report
	// binary_not_cached for this whole population, so the series an operator reads to size the
	// too-old cohort would go to zero on a cold node while nothing about that cohort changed.
	if ok, err := utils.IsGTEVersion(from, utils.MinEnvdVersionForUpgrade); err != nil || !ok {
		s.envdUpgradeGated.Add(ctx, 1, metric.WithAttributes(attribute.String("reason", "old_envd")))

		return false, nil
	}

	// Resolve the target: "" path => no upgrade, with a reason. off / same_version are the
	// expected per-resume no-op (a re-resume of an already-upgraded sandbox), deliberately
	// not counted as noise; the misconfigurations (not_staged — e.g. a bad SHA / rubbish flag
	// value, invalid_target, getversion_failed, downgrade) are worth a counted, logged signal
	// so a broken target is distinguishable from "already current". binary_not_cached is in
	// the same set but is counted WITHOUT a line: it is this node's own expected state on a
	// cold cache rather than anything an operator can fix, and it recurs on every resume
	// until a warm lands -- see the case below.
	//
	// This has to precede the exec-context gate below, even though that gate needs nothing
	// from it. The offline swap advances a sandbox's envd to the promoted binary at cold
	// boot, so a large share of the fleet resolves to same_version — and those sandboxes are
	// not gated by anything, they simply have nowhere to upgrade to. Declining them first
	// would count every one of their resumes as an exec-context decline and make that
	// counter a measure of the fleet rather than of the population the gate holds back.
	//
	// Probe the target's version through the node-local binary cache when
	// enabled. Uncached, the probe is a fork+exec of a 13 MB binary on the gcsfuse
	// mount, and it runs on every sampled resume before any decision is made. That
	// is far more expensive than its size suggests on a cold node: exec
	// demand-pages the binary in small reads, each one a round trip to the mount,
	// which is more than an order of magnitude slower than reading the same bytes
	// sequentially off a cold mount.
	//
	// So a hit answers from a local copy, and a MISS DEFERS THE UPGRADE rather than
	// reading the source. That is the point of the cache and not a limitation of
	// it: the probe carries no budget of its own beyond the resume's own deadline,
	// and delivery carries 30 s, so an upgrade that reaches the mount can add tens
	// of seconds to a resume a customer is waiting on. The warm lands in the
	// background, the upgrade is idempotent and re-fires on the next resume, so
	// deferring costs one cycle. Off, it is the direct fork+exec, unchanged.
	resolveStart := time.Now()
	getVersion := buildenvd.GetEnvdVersion
	var binCache *envdbin.Resolver
	cache := s.sandboxFactory.EnvdBinCache()
	// The candidate checks stat the mount before any version probe, so they are
	// bounded whether or not the cache is engaged.
	stat := cache.BoundedStat
	// The nil check is not redundant with the flag: a Factory built as a struct
	// literal (as tests do) has no cache.
	if cache != nil && s.featureFlags.BoolFlag(ctx, featureflags.EnvdBinaryCacheFlag) {
		binCache = envdbin.NewResolver(cache, envdbin.OpLive)
		getVersion = binCache.Version
		stat = binCache.Stat
	}

	path, tv, reason := featureflags.ResolveEnvdUpgrade(ctx, target, from, s.config.HostEnvdPath,
		s.sandboxFactory.RecordingEnvdTargetVersion(getVersion), stat)
	reason = envdbin.GatedReason(reason, binCache.DeferralOutcome())
	// Labelled by the resolver's own reason, not by success/no_upgrade, and skipped
	// entirely where the resolver returned before probing anything.
	//
	// This histogram exists to expose the version probe, which is the dominant cost
	// of a cold upgrade and is invisible to the combined duration histogram. A
	// success/no_upgrade label defeats that: "off" and "not_staged" return before
	// getVersion is called and so measure nothing, while "same_version" -- the
	// steady-state majority -- returns after paying for it in full. Averaged
	// together the series is dominated by samples where no probe ran, and cannot
	// answer the one question it was added for.
	if resolveResult, recorded := resolvePhaseResult(path, reason); recorded {
		s.recordUpgradePhase(ctx, sbx.Runtime.TeamID, "resolve", resolveResult, resolveStart)
	}
	toVersion = tv
	if path == "" {
		switch reason {
		case "off", "same_version":
			// expected no-op — not counted
		case envdbin.ReasonNotCached, featureflags.ReasonSourceStalled:
			// Counted, because it is the cost side of gating on the cache, but not
			// warned: on a node that has just booted this is the expected state for
			// one resume, and the background warm clears it. A stall is counted
			// under its own reason and warned once per path per window by the
			// cache, not here on every resume.
			s.envdUpgradeGated.Add(ctx, 1, metric.WithAttributes(attribute.String("reason", reason)))
		default:
			s.envdUpgradeGated.Add(ctx, 1, metric.WithAttributes(attribute.String("reason", reason)))
			sbxlogger.I(sbx).Warn(ctx, "envd auto-upgrade: target not resolved",
				zap.String("reason", reason), zap.String("from", from))
		}

		return false, nil
	}

	// Never replace the envd process without a user to re-send afterwards. The new image
	// rebuilds its exec context from scratch, so a post-upgrade /init carrying nothing
	// leaves it on the compiled-in fallback for the rest of the sandbox's life —
	// silently, with a 200 on every RPC.
	//
	// This is not defensive. The metadata cannot establish what a build sent whenever it
	// records anything other than config.TemplateDefaultUser (see
	// metadata.Template.EnvdDefaultUser), and a template that named its own user is in
	// exactly that set while genuinely serving that name. Replacing its process is what
	// would drop it to the fallback.
	//
	// The handover blob carries the identity independently, which is what will retire this
	// gate. The condition is per-swap and not fleet-wide: what matters is the envd THIS
	// sandbox is running, never whether every sandbox has the field. Do not read it as the
	// latter and conclude the gate is permanent — a snapshot paused before the field
	// existed keeps that envd in its RAM for as long as it stays paused, so "every sandbox
	// has it" has no completion date, while "this one does" resolves on its own for
	// anything that cold-boots.
	//
	// It retires itself per swap: the resume's own /init precedes this call and returns
	// X-Envd-Defaults, whose fallback field is !UserDelivered in the running process, so a
	// sandbox whose envd is holding a delivered identity is no longer declined. No release
	// number is involved in either term — the header answers the question from the running
	// process, and a constant guessed now would rot.
	if reason := envdUpgradeDeclineReason(
		utils.DerefOrDefault(sbx.Config.Envd.DefaultUser, ""),
		sbx.EnvdWorkdirWithheld(),
		envdPreservesExecContext(sbx.EnvdReportedDefaults()),
	); reason != "" {
		s.envdUpgradeGated.Add(ctx, 1, metric.WithAttributes(attribute.String("reason", reason)))
		// Counted, not logged, and that is the whole of the volume control. Both reasons are
		// stable properties of the template and the running envd, so a per-resume warn
		// repeats for the whole life of every sandbox in a population that is expected
		// rather than broken -- and there is nowhere to dedupe it: a Sandbox is constructed
		// fresh on every resume, so per-instance state cannot span two of them. The sibling
		// old_envd gate above is counter-only for the same reason. Naming the templates the
		// gate holds back needs a signal that outlives one resume, which this is not.

		return false, nil
	}

	// Resolved before the attempt is marked, so a refusal is counted as the
	// deferral it is rather than as an upgrade that was tried and failed. Delivery
	// hands back the very bytes the resolver decided on: when the cache holds them
	// this reads local disk instead of streaming 13 MB off gcsfuse, which is what
	// pushed delivery past its budget on a contended node, and it removes the
	// second independent read of the source, so a promotion landing mid-upgrade can
	// no longer deliver a binary whose version was never the one recorded.
	deliverPath, deliverable := binCache.SourcePath(ctx, path)
	if !deliverable {
		// The copy that carried the resolved version is gone, so nothing on local
		// disk holds the bytes that version describes. Skip rather than read the
		// source: that would be the mount cost this cache exists to avoid,
		// delivering bytes whose version was never verified against them. Nothing
		// has been sent to the guest, so this is a deferral -- counted beside the
		// other reasons a resume chose not to upgrade -- and the next resume retries.
		s.envdUpgradeGated.Add(ctx, 1, metric.WithAttributes(attribute.String("reason", envdbin.ReasonCopyVanished)))
		sbxlogger.I(sbx).Warn(ctx, "envd auto-upgrade: cached binary went away after its version was resolved; deferring",
			zap.String("from", from), zap.String("to", toVersion), zap.String("path", path))

		return false, nil
	}

	attempted = true
	start = time.Now()

	upCtx, span := tracer.Start(ctx, "envd-upgrade", trace.WithAttributes(
		attribute.String("envd.from_version", from),
		attribute.String("envd.to_version", toVersion),
	))
	defer span.End()

	// Delivery and readiness get INDEPENDENT budgets, not a shared cap: a slow
	// binary upload must not eat into the time envd has to re-adopt and answer
	// /init (which would mislabel a would-be success as delivery_failed/not_ready).
	const (
		deliverTimeout = 30 * time.Second
		readyTimeout   = 15 * time.Second
	)

	sbxlogger.I(sbx).Info(upCtx, "envd auto-upgrade: delivering+triggering",
		zap.String("from", from), zap.String("to", toVersion), zap.String("path", path),
		zap.String("bin_cache", string(binCache.Outcome())))

	// Stream the new binary over the authenticated /upgrade endpoint (delivery
	// + trigger in one call) and let envd same-PID re-exec into it. NB: not the
	// build-time /files CopyFile path — a live, post-/init sandbox rejects that.
	deliverStart := time.Now()
	execConfirmed, err := sbx.CallEnvdUpgrade(upCtx, deliverPath, "/usr/bin/envd.next", deliverTimeout)
	// A retired copy is a deferral, not a delivery, so it gets no phase sample:
	// the open fails in microseconds, and a ~0 ms observation would drag down the
	// quantiles of the histogram added to expose delivery STALLS -- while the
	// attempts series deliberately records nothing for it, so deliver{failed}
	// would also outnumber the attempts it is read against.
	deferredCopyGone := binCache != nil && errors.Is(err, fs.ErrNotExist)
	if !deferredCopyGone {
		s.recordDeliveryPhase(ctx, sbx.Runtime.TeamID, execConfirmed, err, deliverStart)
	}
	if err != nil {
		// A promotion can retire the copy between the resolver's stat and this
		// open. Nothing reached the guest, so that is the same deferral the stat
		// reports and not a failed delivery: labelling it delivery_failed would say
		// the trigger failed when no byte was ever sent.
		// Gated on the cache being ENGAGED, not just on the errno. With the flag off
		// deliverPath is the source itself, so a genuinely missing promoted binary
		// would take this branch: relabelled as a cache event that cannot happen
		// without a cache, dropped from the attempts series, and demoted from Error
		// to Warn. That is the flag-off path changing behaviour, which it must not.
		if deferredCopyGone {
			// Nothing was attempted, so the deferred recorder must not count an
			// attempt or a duration for it -- that is what keeps the attempts series
			// free of a population that delivered nothing.
			attempted = false
			s.envdUpgradeGated.Add(ctx, 1, metric.WithAttributes(attribute.String("reason", envdbin.ReasonCopyVanished)))
			sbxlogger.I(sbx).Warn(upCtx, "envd auto-upgrade: cached binary went away before delivery; deferring",
				zap.String("from", from), zap.String("to", toVersion), zap.String("path", path))

			return false, nil
		}

		result = "delivery_failed"
		span.RecordError(err)
		sbxlogger.I(sbx).Error(upCtx, "envd auto-upgrade: trigger failed", zap.Error(err))

		// Delivery/trigger failed BEFORE execve, so the old envd is still running
		// and serving — best-effort: let the resume proceed on the old version.
		return false, nil
	}
	// A confirmed exec is taken to run the staged binary: the rest of this life routes its
	// /init by that version, the re-check below included, and the mark stands even if the
	// version check below finds a mismatch.
	if execConfirmed {
		sbx.MarkEnvdDelivered(toVersion)
	}

	// WaitForEnvd re-runs /init, which re-captures the now-running version.
	//
	// Detach from upCtx (WithoutCancel) but keep the bounded readyTimeout: the
	// exec is a fait accompli by this point, so if the parent resume is cancelled
	// in this window we must still drive /init to completion. Running it on the
	// cancellable parent would let an ambiguous cancellation skip /init yet still
	// return recoverably, publishing a promoted-but-uninitialized sandbox (the
	// exec'd envd never gets its auth/env restored). Version confirmation below
	// then correctly distinguishes an actual exec from an untouched old envd.
	readyCtx := context.WithoutCancel(upCtx)
	readyStart := time.Now()
	readyErr := sbx.WaitForEnvd(readyCtx, sandbox.StartTypeResume, readyTimeout)
	s.recordUpgradePhase(ctx, sbx.Runtime.TeamID, "ready", phaseResult(readyErr), readyStart)
	if readyErr != nil {
		result = "not_ready"
		span.RecordError(readyErr)
		sbxlogger.I(sbx).Error(upCtx, "envd auto-upgrade: envd not ready after upgrade", zap.Error(readyErr))

		if !execConfirmed {
			// The delivery deadline fired without a confirmed exec — envd may
			// still be mid-handover on the OLD binary, which will thaw and keep
			// serving. Don't tear down a recoverable sandbox: best-effort, let the
			// resume proceed (a false-positive brick is worse than a missed
			// upgrade).
			return false, nil
		}

		// The exec is confirmed (the old envd is gone) but the new envd never
		// completed /init — its access token is unrestored and the
		// WithAuthorization handover gate fail-closes every RPC. It can't be made
		// both usable and secure without /init, so fail the resume (unrecoverable)
		// rather than return a live-but-permanently-bricked sandbox.
		return false, fmt.Errorf("envd live-upgrade left the sandbox uninitialized (post-upgrade /init failed): %w", readyErr)
	}

	// Confirm by ground truth: the running envd must now report the target
	// version. This is the arbiter — a transport quirk (e.g. a slow exec that
	// never answered) can't mislabel a non-swap as success.
	if now := sbx.LiveEnvdVersion(); now != toVersion {
		result = "version_mismatch"
		span.SetAttributes(attribute.String("envd.observed_version", now))
		sbxlogger.I(sbx).Error(upCtx, "envd auto-upgrade: version did not flip",
			zap.String("observed", now), zap.String("expected", toVersion))

		// /init succeeded (envd is initialized and serving), it just isn't the
		// expected version — usable, not bricked, so don't fail the resume.
		return false, nil
	}

	// The version flipped, but if the in-guest handover itself failed post-exec
	// (ResumeFromHandover errored/panicked, so the workload was never re-adopted —
	// orphaned and unreaped), the old envd is gone and the sandbox is broken. Fail
	// the resume so the caller tears it down rather than hand back a live-but-
	// broken sandbox that merely reports the target version.
	if h := sbx.HandoverResult(); h != nil && h.Failed {
		result = "handover_failed"
		span.SetAttributes(attribute.Bool("envd.handover.failed", true))
		sbxlogger.I(sbx).Error(upCtx, "envd auto-upgrade: in-guest handover failed post-exec; workload not re-adopted")

		return false, errors.New("envd live-upgrade handover failed post-exec (workload not re-adopted)")
	}

	span.SetAttributes(attribute.Bool("envd.upgraded", true))

	// Record the handover outcome the new envd reported on /init (fleet
	// visibility into what it re-adopted). Per item (proc|retained|watcher) as
	// ok/failed so failed/(ok+failed) is the handover error rate — a non-zero
	// failed means the swap dropped or degraded something (a lost watch, an
	// unrecoverable exit code, a bad process config), which envd otherwise only
	// logs in-guest.
	if h := sbx.HandoverResult(); h != nil {
		recordItem := func(item string, ok, failed int) {
			s.envdUpgradeHandover.Add(upCtx, int64(ok), metric.WithAttributes(
				attribute.String("item", item), attribute.String("result", "ok")))
			s.envdUpgradeHandover.Add(upCtx, int64(failed), metric.WithAttributes(
				attribute.String("item", item), attribute.String("result", "failed")))
		}
		recordItem("proc", h.Procs-h.ProcsFailed, h.ProcsFailed)
		recordItem("retained", h.Retained-h.RetainedFailed, h.RetainedFailed)
		recordItem("watcher", h.Watchers-h.WatchersFailed, h.WatchersFailed)

		span.SetAttributes(
			attribute.Int("envd.handover.procs", h.Procs),
			attribute.Int("envd.handover.procs_failed", h.ProcsFailed),
			attribute.Int("envd.handover.retained", h.Retained),
			attribute.Int("envd.handover.retained_failed", h.RetainedFailed),
			attribute.Int("envd.handover.watchers", h.Watchers),
			attribute.Int("envd.handover.watchers_failed", h.WatchersFailed),
		)
		sbxlogger.I(sbx).Info(upCtx, "envd auto-upgrade: complete",
			zap.String("to", toVersion),
			zap.Int("procs", h.Procs), zap.Int("procs_failed", h.ProcsFailed),
			zap.Int("retained", h.Retained), zap.Int("retained_failed", h.RetainedFailed),
			zap.Int("watchers", h.Watchers), zap.Int("watchers_failed", h.WatchersFailed))
	} else {
		sbxlogger.I(sbx).Info(upCtx, "envd auto-upgrade: complete", zap.String("to", toVersion))
	}

	return true, nil
}

// The gated reasons a live upgrade is declined. Named constants because they are dashboard
// values a rollout reads, and separate values because they cost different things: one
// protects an identity the host cannot reconstruct, the other a workdir it cannot prove.
const (
	envdUpgradeGateNoDefaultUser     = "no_default_user"
	envdUpgradeGateUnprovableWorkdir = "unprovable_workdir"
)

// envdPreservesExecContext reports whether the running envd will carry its exec context
// across its own replacement without the host re-sending anything.
//
// Both terms are required, and the second is not redundant. A reporting envd that was never
// told a user writes NO defaults into the handover blob at all — handoverDefaults refuses to
// pass the compiled-in fallback off as a delivered identity — so its blob would carry the
// exec context no further than a silent envd's. Testing only for a reported header would let
// that swap through.
func envdPreservesExecContext(reported *sandbox.EnvdEffectiveDefaults) bool {
	return reported != nil && !reported.Fallback
}

// envdUpgradeDeclineReason reports why a live envd upgrade must not be attempted, or "" when
// it may proceed. One rule: never replace the process without being able to restore
// everything it was serving.
//
// A pure function over the three facts it needs, so the policy and its emitted labels are
// pinned by a test rather than only by the call site: the whole point of the gate is that
// its failure mode is silent in both directions — let a swap through and the sandbox loses
// state with a 200 on every RPC, decline one that could have proceeded and it simply never
// upgrades, which nothing else reports.
//
// envdPreserves is a capability, not a value: the header and the handover blob's defaults
// field ship in the same envd, so an envd that reports a delivered exec context is also one
// whose blob carries it across the swap by itself. That is what makes BOTH rules
// self-retiring — each declines only while the running envd cannot preserve the value without
// help, never on the shape of the template alone, so no template is excluded permanently.
func envdUpgradeDeclineReason(sentUser string, workdirWithheld, envdPreserves bool) string {
	// Both terms, so this rule retires itself per swap exactly as the workdir rule does. The
	// host having nothing to send is not sufficient to decline: once the running envd
	// reports a delivered identity, its blob restores that identity across the execve, and
	// the post-upgrade /init carrying an empty user is a no-op in SetData rather than an
	// overwrite. Declining on the sent user alone would exclude every template recording a
	// non-default user for as long as it stays a memory snapshot, which is stricter than the
	// mechanism needs and has no end date.
	//
	// It makes the guest's own report load-bearing, and the exposure is bounded to the
	// sandbox that lies: a guest reporting fallback=false when it was never told gets a swap
	// whose blob then carries nothing, so it loses its own default user. It cannot reach any
	// other sandbox, and the default user is not a privilege boundary — any holder of the
	// sandbox token can already run as any user that exists in the guest.
	if sentUser == "" && !envdPreserves {
		return envdUpgradeGateNoDefaultUser
	}

	// A recorded workdir that was not re-sent lives only in the process being replaced,
	// and the metadata cannot establish it (finalize sends it above the template version
	// gate and nothing below it, and that version is not in the snapshot), so nothing can
	// put it back afterwards. Declining keeps it; upgrading trades it for a version bump.
	if workdirWithheld && !envdPreserves {
		return envdUpgradeGateUnprovableWorkdir
	}

	return ""
}

// createRefused is a create refusal no other node and no retry changes, with
// a typed detail so the API reports it as a refusal rather than a failure to
// retry; a transient refusal (this host's limits, draining, files still on
// their way) carries none. The detail's HTTP status is the client's fault
// for an invalid argument and the server's otherwise.
func createRefused(ctx context.Context, code codes.Code, format string, args ...any) error {
	return withCreateRefusedDetail(ctx, status.Newf(code, format, args...))
}

// withCreateRefusedDetail attaches the create-refused detail to a status that
// may already carry another, as createRefused does.
func withCreateRefusedDetail(ctx context.Context, st *status.Status) error {
	httpStatus := http.StatusInternalServerError
	if st.Code() == codes.InvalidArgument {
		httpStatus = http.StatusBadRequest
	}
	withDetails, err := st.WithDetails(&orchestrator.UserError{
		Code:       orchestrator.UserErrorCode_SANDBOX_CREATE_REFUSED,
		Message:    st.Message(),
		HttpStatus: uint32(httpStatus),
	})
	if err != nil {
		logger.L().Error(ctx, "failed to attach the create refusal detail", zap.Error(err), zap.String("refusal", st.Message()))

		return st.Err()
	}

	return withDetails.Err()
}

// volumeRefusal is the create refusal for a volume envd could not mount, or nil when
// err is not one. Every node mounts the same volume, so no other node is asked.
func volumeRefusal(ctx context.Context, err error) error {
	volumeErr, ok := errors.AsType[sandbox.VolumeMountError](err)
	if !ok {
		return nil
	}

	return createRefused(ctx, codes.FailedPrecondition, "%s", volumeErr.Error())
}

// memoryRefusal is the answer to a create whose guest memory the hugepage pool
// could not back, or nil when err is not that failure or the flag is off, in
// which case the create fails as any other. While a sandbox on this node is
// tearing down, now or when the create began (the failed start's own teardown
// runs before the error gets here), its pages are on their way back, so the
// create is refused for placement to retry here; otherwise the pool is too
// small or held by running sandboxes, and the create fails at once with that
// said. Neither carries the for-good detail: another node may have a larger
// pool. The chain is logged; the client's message leaves it out.
func (s *Server) memoryRefusal(ctx context.Context, err error, ramMB int64, stoppingBefore int, runtime sandboxtypes.RuntimeMetadata) error {
	if _, ok := errors.AsType[sandbox.GuestMemoryError](err); !ok || !s.featureFlags.BoolFlag(ctx, featureflags.CreateMemoryRefusalFlag) {
		return nil
	}
	stopping := max(stoppingBefore, s.sandboxFactory.Sandboxes.Stopping())
	telemetry.ReportError(ctx, "create refused: guest memory not mappable", err,
		telemetry.WithSandboxID(runtime.SandboxID),
		telemetry.WithBuildID(runtime.BuildID),
		telemetry.WithTemplateID(runtime.TemplateID),
		telemetry.WithTeamID(runtime.TeamID),
		attribute.Int("stopping", stopping),
		attribute.Int64("ram_mb", ramMB))
	if stopping > 0 {
		return status.Error(codes.ResourceExhausted, "not enough memory to map the guest memory while a sandbox is still stopping on this node, please retry")
	}

	return status.Errorf(codes.FailedPrecondition, "this node cannot map %d MiB of guest memory: its memory pool is too small or in use by running sandboxes", ramMB)
}

// checkpointInProgressStatus is the refusal of a snapshot while another
// checkpoint of the same sandbox is in flight, with a typed detail so the API
// tells it from a busy node on Pause and from a rejected artifact on
// Checkpoint. The code is the one each RPC answered before the detail existed.
func checkpointInProgressStatus(code codes.Code, format string, sandboxID string) *status.Status {
	st := status.Newf(code, format, sandboxID)
	withDetails, err := st.WithDetails(&orchestrator.UserError{
		Code:       orchestrator.UserErrorCode_CHECKPOINT_IN_PROGRESS,
		Message:    st.Message(),
		HttpStatus: http.StatusServiceUnavailable,
	})
	if err != nil {
		return st
	}

	return withDetails
}

// nodeNotAcceptingStatus is the refusal of a create on a node whose status
// admits nothing new, with the detail the API skips the node on.
func nodeNotAcceptingStatus(nodeStatus orchestratorinfo.ServiceInfoStatus) *status.Status {
	st := status.Newf(codes.ResourceExhausted, "node is not accepting new sandboxes (status %s), please retry", nodeStatus)
	withDetails, err := st.WithDetails(&orchestrator.UserError{
		Code:       orchestrator.UserErrorCode_NODE_NOT_ACCEPTING_SANDBOXES,
		Message:    st.Message(),
		HttpStatus: http.StatusServiceUnavailable,
	})
	if err != nil {
		return st
	}

	return withDetails
}

// envdUnresponsiveStatus is the refusal of a memory pause whose guest agent
// did not answer the health probe, with a typed detail so the API tells it
// from a busy node and restores the sandbox regardless of its restore flag.
func envdUnresponsiveStatus(sandboxID string) *status.Status {
	st := status.Newf(codes.ResourceExhausted, "sandbox '%s' guest agent is not responding, please retry", sandboxID)
	withDetails, err := st.WithDetails(&orchestrator.UserError{
		Code:       orchestrator.UserErrorCode_ENVD_UNRESPONSIVE,
		Message:    st.Message(),
		HttpStatus: http.StatusServiceUnavailable,
	})
	if err != nil {
		return st
	}

	return withDetails
}

// filesystemOnlyDisabledStatus is the flag refusal with a typed detail, so the
// API can answer it as its own pre-flight does instead of a generic failure
// when the two flag evaluations disagree.
func filesystemOnlyDisabledStatus(sandboxID string) *status.Status {
	st := status.Newf(codes.FailedPrecondition, "filesystem-only checkpoint of sandbox '%s' is disabled", sandboxID)
	withDetails, err := st.WithDetails(&orchestrator.UserError{
		Code:       orchestrator.UserErrorCode_FILESYSTEM_ONLY_CHECKPOINT_DISABLED,
		Message:    st.Message(),
		HttpStatus: http.StatusBadRequest,
	})
	if err != nil {
		return st
	}

	return withDetails
}

// firecrackerConfigFor is the Firecracker configuration a served sandbox gets from
// its build's metadata: the guest profile and the device names the memory image
// bakes, the default profile and no names for every snapshot this runtime made.
// A pinning profile's build also names its kernel slot payload, which stands
// over the request's: the build row of a template built from such a parent
// carries the versions the API resolved for the default guest.
func firecrackerConfigFor(meta metadata.Template, profile guest.Profile, kernelVersion, firecrackerVersion string) fc.Config {
	if profile.Firecracker().PinToBuild && meta.Template.KernelVersion != "" {
		kernelVersion = meta.Template.KernelVersion
	}

	return fc.Config{
		KernelVersion:      kernelVersion,
		FirecrackerVersion: firecrackerVersion,
		Guest:              profile,
		RootfsDriveID:      meta.RootfsDriveID,
		NetIfaceID:         meta.NetIfaceID,
		RootfsFile:         meta.RootfsFile,
	}
}

// defaultPauseReplyMargin is what a pause that waited for a reboot keeps of
// its deadline to answer a retryable refusal in time.
const defaultPauseReplyMargin = 5 * time.Second

// defaultPauseCaptureBudget is the time a pause that waited for a reboot
// needs left to capture the sandbox; the earlier fixed 30 s wait left 50 s
// of the API's 80.
const defaultPauseCaptureBudget = 30 * time.Second

// pauseHasCaptureBudget reports whether ctx leaves a pause the time a capture
// needs; without a deadline it does.
func (s *Server) pauseHasCaptureBudget(ctx context.Context) bool {
	deadline, ok := ctx.Deadline()
	if !ok {
		return true
	}

	budget := defaultPauseCaptureBudget
	if s.pauseCaptureBudget > 0 {
		budget = s.pauseCaptureBudget
	}

	return time.Until(deadline) >= budget
}

// pauseRebootWaitCtx bounds a pause's wait for a reboot in place to the
// caller's deadline less the time its answer needs; without a deadline the
// wait is the boot's own.
func (s *Server) pauseRebootWaitCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	deadline, ok := ctx.Deadline()
	if !ok {
		return ctx, func() {}
	}

	margin := defaultPauseReplyMargin
	if s.pauseReplyMargin > 0 {
		margin = s.pauseReplyMargin
	}

	return context.WithDeadline(ctx, deadline.Add(-margin))
}

// pauseHeldElsewhere is the retryable refusal of a pause whose sandbox a
// reboot in place or a checkpoint holds out of the live map, or made live
// again since the pause read it; nil means the sandbox is gone. Every answer
// carries the checkpoint-in-progress detail: a bare ResourceExhausted is a
// busy node to the API, which kills the sandbox unless its restore flag is
// on, while the detail restores the record whatever the flag says.
func (s *Server) pauseHeldElsewhere(sandboxID string) error {
	switch {
	case s.sandboxFactory.Sandboxes.Rebooting(sandboxID):
		return checkpointInProgressStatus(codes.ResourceExhausted, "sandbox '%s' is rebooting, please retry", sandboxID).Err()
	case s.detachedCheckpointFor(sandboxID) != nil:
		return checkpointInProgressStatus(codes.ResourceExhausted, "sandbox '%s' is being checkpointed, please retry", sandboxID).Err()
	default:
		// Read last: a reboot or a checkpoint makes the sandbox live before
		// it lets go of it, so one that ended since the checks above is
		// seen here.
		if _, ok := s.sandboxFactory.Sandboxes.Get(sandboxID); ok {
			return checkpointInProgressStatus(codes.ResourceExhausted, "sandbox '%s' is live again, please retry", sandboxID).Err()
		}

		return nil
	}
}
