//go:build linux

package server

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"slices"
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
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/rootfs"
	sbxtemplate "github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/template"
	buildenvd "github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/core/envd"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/metadata"
	"github.com/e2b-dev/infra/packages/shared/pkg/events"
	"github.com/e2b-dev/infra/packages/shared/pkg/fcversion"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	sbxlogger "github.com/e2b-dev/infra/packages/shared/pkg/logger/sandbox"
	"github.com/e2b-dev/infra/packages/shared/pkg/retry"
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
	// uploadTotalBudget bounds how long a snapshot upload is retried before it
	// is given up. Covers a long GCS outage without retrying forever.
	uploadTotalBudget = 2 * time.Hour
	// redisPeerKeyTTL keeps the peer routing key valid across the whole retry
	// window so a long retry doesn't drop peer routing mid-upload. It is
	// unregistered promptly once the upload finishes (success or give-up).
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
)

// filesystemBoot reports whether a snapshot resumes by cold-booting (rebooting)
// from its rootfs instead of restoring memory: when the artifact has no memory
// to restore, or when the request demands a cold boot of one that does. The
// request can only widen toward the no-memory path — it can never force a
// memory restore of a snapshot that has none.
func filesystemBoot(meta metadata.Template, req *orchestrator.SandboxCreateRequest) bool {
	return meta.IsFilesystemOnly() || req.GetFilesystemBoot()
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
	defer func() {
		s.sandboxCreateDuration.Record(ctx, time.Since(createStart).Milliseconds(),
			metric.WithAttributes(
				attribute.Bool("sandbox.resume", isResume),
				attribute.Bool("fs_only", fsOnly),
				attribute.Bool("fs_boot_requested", req.GetFilesystemBoot()),
				attribute.Bool("success", createErr == nil),
				attribute.Bool("envd.upgraded", envdUpgraded),
				attribute.String("fs_recovery", string(fsRecovery)),
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
	ctx = featureflags.AddToContext(
		ctx,
		ldcontext.NewBuilder(req.GetSandbox().GetSandboxId()).
			Kind(featureflags.SandboxKind).
			SetString(featureflags.SandboxTemplateAttribute, req.GetSandbox().GetTemplateId()).
			SetString(featureflags.SandboxKernelVersionAttribute, req.GetSandbox().GetKernelVersion()).
			SetString(featureflags.SandboxFirecrackerVersionAttribute, req.GetSandbox().GetFirecrackerVersion()).
			SetString(featureflags.SandboxEnvdVersionAttribute, req.GetSandbox().GetEnvdVersion()).
			Build(),
		ldcontext.NewBuilder(req.GetSandbox().GetTeamId()).
			Kind(featureflags.TeamKind).
			Build(),
	)

	// BYOP egress proxy kill-switch; mirrors the API gate for direct gRPC
	// callers and snapshot resumes.
	if req.GetSandbox().GetNetwork().GetEgress().GetEgressProxyAddress() != "" {
		if !s.featureFlags.BoolFlag(ctx, featureflags.BYOPProxyEnabledFlag) {
			telemetry.ReportEvent(ctx, "egressProxy rejected by BYOPProxyEnabledFlag")

			return nil, status.Error(codes.PermissionDenied,
				"egress proxy is not enabled for this team")
		}
		if !s.sandboxFactory.EgressProxy().SupportsBYOP() {
			telemetry.ReportEvent(ctx, "egressProxy rejected: orchestrator build has no BYOP dialer")

			return nil, status.Error(codes.Unimplemented,
				"egress proxy is not supported by this orchestrator build")
		}
	}

	reservation, err := s.sandboxFactory.Sandboxes.Reserve(req.GetSandbox().GetSandboxId())
	if err != nil {
		return nil, s.sandboxAlreadyRunning(ctx, req.GetSandbox().GetSandboxId(), req.GetSandbox().GetExecutionId(), err)
	}
	var rollback *sandbox.Cleanup
	defer func() {
		s.finishSandboxStart(ctx, reservation, rollback, createErr)
	}()

	maxRunningSandboxesPerNode := s.info.MaxSandboxes.Load()

	runningSandboxes := int64(s.sandboxFactory.Sandboxes.Count())
	if runningSandboxes >= maxRunningSandboxesPerNode {
		telemetry.ReportEvent(ctx, "max number of running sandboxes reached")

		return nil, status.Errorf(codes.ResourceExhausted, "max number of running sandboxes on node reached: current=%d, max=%d, please retry", runningSandboxes, maxRunningSandboxesPerNode)
	}

	// Check if we've reached the max number of starting instances on this node
	if req.GetSandbox().GetSnapshot() {
		err := s.waitForAcquire(ctx)
		if err != nil {
			return nil, err
		}
	} else {
		acquired := s.startingSandboxes.TryAcquire(1)
		if !acquired {
			telemetry.ReportEvent(ctx, "too many starting sandboxes on node")

			return nil, status.Errorf(codes.ResourceExhausted, "too many sandboxes starting on this node: current=%d, max=%d, please retry", s.startingSandboxes.Current(), s.startingSandboxes.Limit())
		}
	}
	defer s.startingSandboxes.Release(1)

	template, err := s.templateCache.GetTemplate(
		ctx,
		req.GetSandbox().GetBuildId(),
		req.GetSandbox().GetSnapshot(),
		false,
		sbxtemplate.GetTemplateOpts{MaxSandboxLengthHours: req.GetSandbox().GetMaxSandboxLength()},
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get template snapshot data: %w", err)
	}

	// Clone the network config to avoid modifying the original request
	network := proto.CloneOf(req.GetSandbox().GetNetwork())

	resolvedFCVersion := featureflags.ResolveFirecrackerVersion(ctx, s.featureFlags, req.GetSandbox().GetFirecrackerVersion())
	volumeMounts, err := createVolumeMountModelsFromAPI(req.GetSandbox().GetVolumeMounts())
	if err != nil {
		return nil, fmt.Errorf("failed to convert volume mounts: %w", err)
	}

	config := sandbox.NewConfig(sandbox.Config{
		BaseTemplateID: req.GetSandbox().GetBaseTemplateId(),

		Vcpu:            req.GetSandbox().GetVcpu(),
		RamMB:           req.GetSandbox().GetRamMb(),
		TotalDiskSizeMB: req.GetSandbox().GetTotalDiskSizeMb(),
		HugePages:       req.GetSandbox().GetHugePages(),

		Network: network,

		Envd: sandbox.EnvdMetadata{
			Version:     req.GetSandbox().GetEnvdVersion(),
			AccessToken: req.GetSandbox().EnvdAccessToken,
			Vars:        req.GetSandbox().GetEnvVars(),
		},

		FirecrackerConfig: fc.Config{
			KernelVersion:      req.GetSandbox().GetKernelVersion(),
			FirecrackerVersion: resolvedFCVersion,
		},

		VolumeMounts:          volumeMounts,
		MaxSandboxLengthHours: req.GetSandbox().GetMaxSandboxLength(),
	})
	childSpan.SetAttributes(
		telemetry.WithFirecrackerVersion(config.FirecrackerConfig.FirecrackerVersion),
	)

	runtime := sandboxtypes.RuntimeMetadata{
		TemplateID:  req.GetSandbox().GetTemplateId(),
		SandboxID:   req.GetSandbox().GetSandboxId(),
		ExecutionID: req.GetSandbox().GetExecutionId(),
		TeamID:      req.GetSandbox().GetTeamId(),
		BuildID:     req.GetSandbox().GetBuildId(),
		SandboxType: sandboxtypes.SandboxTypeSandbox,
	}

	meta, err := template.Metadata()
	if err != nil {
		return nil, fmt.Errorf("failed to read template metadata: %w", err)
	}

	fsOnly = meta.IsFilesystemOnly()
	filesystemBooted = filesystemBoot(meta, req)

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
		sbx, err = s.sandboxFactory.ResumeSandbox(
			ctx,
			template,
			config,
			runtime,
			req.GetStartTime().AsTime(),
			req.GetEndTime().AsTime(),
			req.GetSandbox(),
			// Defer routing until after the resume-time envd upgrade's
			// post-/init (see markSandboxLive below).
			sandbox.WithDeferredLiveRegistration(),
		)
	}
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotExist) {
			// Snapshot data not found, let the API know the data aren't probably upload yet
			telemetry.ReportError(ctx, "sandbox files not found", err, telemetry.WithSandboxID(req.GetSandbox().GetSandboxId()))

			return nil, status.Errorf(codes.FailedPrecondition, "sandbox files for '%s' not found", req.GetSandbox().GetSandboxId())
		}

		err = errors.Join(err, context.Cause(ctx))
		telemetry.ReportCriticalError(ctx, "failed to create sandbox", err)
		logger.L().Error(ctx, "failed to create sandbox", zap.Error(err),
			zap.Bool("filesystem_boot_requested", req.GetFilesystemBoot()),
			logger.WithSandboxID(runtime.SandboxID),
			logger.WithBuildID(runtime.BuildID),
			logger.WithTemplateID(runtime.TemplateID),
			logger.WithEnvdVersion(config.Envd.Version),
			logger.WithKernelVersion(config.FirecrackerConfig.KernelVersion),
			logger.WithFirecrackerVersion(config.FirecrackerConfig.FirecrackerVersion),
		)

		return nil, status.Errorf(codes.Internal, "failed to create sandbox: %s", err)
	}

	rollback = sandbox.NewCleanup()
	rollback.Add(ctx, func(ctx context.Context) error { return stopAndCloseSandbox(ctx, sbx) })
	s.setupSandboxLifecycle(ctx, sbx)

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
	if err := s.markSandboxLive(ctx, sbx, reservation); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to register sandbox: %s", err)
	}

	// Read scheduling metadata after the sandbox resumed so the template's
	// memfile/rootfs devices (and their headers) are resolved.
	var schedulingMetadata *orchestrator.SchedulingMetadata
	if provider, ok := template.(interface {
		SchedulingMetadata(ctx context.Context) *orchestrator.SchedulingMetadata
	}); ok {
		schedulingMetadata = provider.SchedulingMetadata(ctx)
	}

	eventType := events.SandboxCreatedEventPair
	if req.GetSandbox().GetSnapshot() {
		eventType = events.SandboxResumedEventPair
	}

	teamID, buildId, eventsTTLDays, eventData := s.prepareSandboxEventData(ctx, sbx)
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
	}, nil
}

func createVolumeMountModelsFromAPI(volumeMounts []*orchestrator.SandboxVolumeMount) ([]sandbox.VolumeMountConfig, error) {
	var errs []error

	results := make([]sandbox.VolumeMountConfig, 0, len(volumeMounts))

	for _, v := range volumeMounts {
		volumeID, err := uuid.Parse(v.GetId())
		if err != nil {
			errs = append(errs, fmt.Errorf("invalid volume id %q: %w", v.GetId(), err))

			continue
		}

		results = append(results, sandbox.VolumeMountConfig{
			ID:   volumeID,
			Name: v.GetName(),
			Path: v.GetPath(),
			Type: v.GetType(),
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

	sbx, ok := s.sandboxFactory.Sandboxes.Get(req.GetSandboxId())
	if !ok {
		telemetry.ReportCriticalError(ctx, "sandbox not found", nil)

		return nil, status.Error(codes.NotFound, "sandbox not found")
	}

	childSpan.SetAttributes(
		telemetry.WithTeamID(sbx.Runtime.TeamID),
		telemetry.WithTemplateID(sbx.Runtime.TemplateID),
		telemetry.WithBuildID(sbx.Runtime.BuildID),
		telemetry.WithFirecrackerVersion(sbx.Config.FirecrackerConfig.FirecrackerVersion),
		telemetry.WithKernelVersion(sbx.Config.FirecrackerConfig.KernelVersion),
		telemetry.WithEnvdVersion(sbx.Config.Envd.Version),
	)

	// Mirror the Create-side BYOP gates; defense-in-depth for direct gRPC
	// callers.
	if req.GetEgress().GetEgressProxyAddress() != "" {
		ctx = featureflags.AddToContext(ctx,
			ldcontext.NewBuilder(sbx.Runtime.TeamID).
				Kind(featureflags.TeamKind).
				Build(),
		)
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

	err := sbx.RunUpdate(func() error {
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
		return nil, err
	}

	return &emptypb.Empty{}, nil
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
		sandboxes = append(sandboxes, &orchestrator.RunningSandbox{
			Config:      sbx.APIStoredConfig,
			ClientId:    s.info.ClientId,
			StartTime:   timestamppb.New(startedAt),
			EndTime:     timestamppb.New(sbx.GetEndAt()),
			SandboxId:   sbx.Runtime.SandboxID,
			TeamId:      sbx.Runtime.TeamID,
			ExecutionId: sbx.Runtime.ExecutionID,
			Vcpu:        sbx.Config.Vcpu,
			RamMb:       sbx.Config.RamMB,
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

	sbx, ok := s.sandboxFactory.Sandboxes.Get(in.GetSandboxId())
	if !ok {
		telemetry.ReportCriticalError(ctx, "sandbox not found", nil, telemetry.WithSandboxID(in.GetSandboxId()))

		return nil, status.Errorf(codes.NotFound, "sandbox '%s' not found", in.GetSandboxId())
	}

	childSpan.SetAttributes(
		telemetry.WithTeamID(sbx.Runtime.TeamID),
		telemetry.WithTemplateID(sbx.Runtime.TemplateID),
		telemetry.WithBuildID(sbx.Runtime.BuildID),
		telemetry.WithFirecrackerVersion(sbx.Config.FirecrackerConfig.FirecrackerVersion),
		telemetry.WithKernelVersion(sbx.Config.FirecrackerConfig.KernelVersion),
		telemetry.WithEnvdVersion(sbx.Config.Envd.Version),
	)

	// Mark the sandbox as stopping so it is excluded from live queries (Get, Items,
	// Count) but remains findable by IP (GetByHostPort) while the Firecracker
	// process finishes shutting down.
	// This prevents the sandbox from being synced to API again.
	marked := s.sandboxFactory.Sandboxes.MarkStopping(ctx, sbx.Runtime.SandboxID, sbx.LifecycleID)
	if !marked {
		telemetry.ReportCriticalError(ctx, "failed to mark sandbox as stopping", nil, telemetry.WithSandboxID(in.GetSandboxId()))

		return nil, status.Errorf(codes.Internal, "failed to delete sandbox '%s'", in.GetSandboxId())
	}

	killReason := in.GetKillReason()
	if killReason == "" {
		killReason = killReasonUnknown
	}

	sbxlogger.E(sbx).Info(ctx, "Killing sandbox", zap.String("kill_reason", killReason))

	sbx.SetStopReason(sandbox.StopReasonKilled)

	// Check health metrics before stopping the sandbox
	sbx.Checks.Healthcheck(ctx, true)

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

// emitSandboxKilled publishes the terminal surfaces of a sandbox kill — the
// killed lifecycle event and the kill counter, with a bounded reason. Callers
// are the Delete RPC and the paths that destroy a live-registered sandbox
// in-request without passing through it: the pause-path seal-failure kill and
// the in-place resume-failure teardown.
func (s *Server) emitSandboxKilled(ctx context.Context, sbx *sandbox.Sandbox, killReason string) {
	teamID, buildId, eventsTTLDays, eventData := s.prepareSandboxEventData(ctx, sbx)
	eventData[executionEventDataKey] = s.getSandboxExecutionData(sbx)
	addKillReason(eventData, killReason)
	recordSandboxKill(ctx, s.sandboxKilledCounter, killReason)
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

// recordSandboxKill increments the kill counter with a bounded reason label.
func recordSandboxKill(ctx context.Context, counter metric.Int64Counter, killReason string) {
	if killReason == "" {
		killReason = killReasonUnknown
	}

	counter.Add(ctx, 1, metric.WithAttributes(attribute.String("kill_reason", killReason)))
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
		metric.WithAttributes(attribute.String("stop_reason", string(sbx.GetStopReason()))))
}

// recordPauseAdmission records one admission decision. The wait histogram
// samples only the outcomes that actually waited; an empty outcome (the
// caller's context ended mid-wait) records nothing — no decision was made.
func (s *Server) recordPauseAdmission(ctx context.Context, rpc string, outcome sandbox.SnapshotAdmissionOutcome, waited time.Duration) {
	if outcome == "" {
		return
	}

	s.pauseAdmissionCounter.Add(ctx, 1, metric.WithAttributes(
		attribute.String("outcome", string(outcome)),
		attribute.String("rpc", rpc),
	))

	if outcome == sandbox.SnapshotAdmissionReadyAfterWait || outcome == sandbox.SnapshotAdmissionRefused {
		s.pauseAdmissionWaitDuration.Record(ctx, waited.Milliseconds(),
			metric.WithAttributes(attribute.String("outcome", string(outcome))))
	}
}

func (s *Server) Pause(ctx context.Context, in *orchestrator.SandboxPauseRequest) (resp *orchestrator.SandboxPauseResponse, err error) {
	releaseWork := s.info.TrackWork()
	defer releaseWork()

	ctx, childSpan := tracer.Start(ctx, "sandbox-pause")
	defer childSpan.End()

	// Record pause duration split by fs_only vs memory (the gRPC RPC metric
	// can't distinguish them) and success, so dashboards can scope pause
	// call-count / error-rate / latency to filesystem-only pauses.
	pauseStart := time.Now()
	defer func() {
		s.sandboxPauseDuration.Record(ctx, time.Since(pauseStart).Milliseconds(),
			metric.WithAttributes(
				attribute.Bool("fs_only", in.GetFilesystemOnly()),
				attribute.Bool("success", err == nil),
			),
		)
	}()

	childSpan.SetAttributes(
		telemetry.WithSandboxID(in.GetSandboxId()),
		telemetry.WithTemplateID(in.GetTemplateId()),
		telemetry.WithBuildID(in.GetBuildId()),
	)

	sbx, ok := s.sandboxFactory.Sandboxes.Get(in.GetSandboxId())
	if !ok {
		telemetry.ReportCriticalError(ctx, "sandbox not found", nil, telemetry.WithSandboxID(in.GetSandboxId()))

		return nil, status.Error(codes.NotFound, "sandbox not found")
	}

	ctx = featureflags.AddToContext(
		ctx,
		ldcontext.NewBuilder(in.GetSandboxId()).
			Kind(featureflags.SandboxKind).
			SetString(featureflags.SandboxTemplateAttribute, sbx.Runtime.TemplateID).
			SetString(featureflags.SandboxKernelVersionAttribute, sbx.Config.FirecrackerConfig.KernelVersion).
			SetString(featureflags.SandboxFirecrackerVersionAttribute, sbx.Config.FirecrackerConfig.FirecrackerVersion).
			SetString(featureflags.SandboxEnvdVersionAttribute, sbx.Config.Envd.Version).
			Build(),
	)

	childSpan.SetAttributes(
		telemetry.WithTeamID(sbx.Runtime.TeamID),
		telemetry.WithFirecrackerVersion(sbx.Config.FirecrackerConfig.FirecrackerVersion),
		telemetry.WithKernelVersion(sbx.Config.FirecrackerConfig.KernelVersion),
		telemetry.WithEnvdVersion(sbx.Config.Envd.Version),
	)

	// Flag-gated admission pre-flight: refuse retryably BEFORE any destructive
	// step while the parent memfile header is still deduplicating.
	var latchedErr error
	if graceMs := s.featureFlags.IntFlag(ctx, featureflags.PauseAdmissionGraceMs); graceMs >= 0 {
		outcome, waited, admitErr := sbx.AwaitSnapshotAdmission(ctx, time.Duration(graceMs)*time.Millisecond, !in.GetFilesystemOnly())
		s.recordPauseAdmission(ctx, "pause", outcome, waited)
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

	marked := s.sandboxFactory.Sandboxes.MarkStopping(ctx, sbx.Runtime.SandboxID, sbx.LifecycleID)
	if !marked {
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
	defer s.stopSandboxAsync(context.WithoutCancel(ctx), sbx)

	// Defer the rootfs reflink off the pause critical path when enabled: pause is a
	// suspend, so nothing reads the diff until a later resume (which waits on the
	// upload anyway). NBD provider only; falls back to synchronous export otherwise.
	deferRootfsExport := s.featureFlags.BoolFlag(ctx, featureflags.DeferRootfsExportFlag)

	// Fire and forget - upload completes in the background
	res, err := s.snapshotAndCacheSandbox(ctx, sbx, in.GetBuildId(), map[string]string{storage.ObjectMetadataTemplateID: in.GetTemplateId()}, storage.ObjectOriginPause, in.GetFilesystemOnly(), deferRootfsExport, false)
	if err != nil {
		telemetry.ReportCriticalError(ctx, "error snapshotting sandbox", err, telemetry.WithSandboxID(in.GetSandboxId()))

		return nil, status.Errorf(codes.Internal, "error snapshotting sandbox '%s': %s", in.GetSandboxId(), err)
	}

	s.uploadSnapshotAsync(ctx, sbx, res)

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
	if !in.GetFilesystemOnly() {
		s.harvestResumePrefetchAsync(ctx, sbx, res, in.GetBuildId(), res.objectMetadata)
	}

	teamID, buildId, eventsTTLDays, eventData := s.prepareSandboxEventData(ctx, sbx)
	eventData[executionEventDataKey] = s.getSandboxExecutionData(sbx)

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
	}, nil
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

	sbx, ok := s.sandboxFactory.Sandboxes.Get(in.GetSandboxId())
	if !ok {
		telemetry.ReportCriticalError(ctx, "sandbox not found", nil, telemetry.WithSandboxID(in.GetSandboxId()))

		return nil, status.Errorf(codes.NotFound, "sandbox '%s' not found", in.GetSandboxId())
	}

	ctx = featureflags.AddToContext(
		ctx,
		ldcontext.NewBuilder(in.GetSandboxId()).
			Kind(featureflags.SandboxKind).
			SetString(featureflags.SandboxTemplateAttribute, sbx.Runtime.TemplateID).
			SetString(featureflags.SandboxKernelVersionAttribute, sbx.Config.FirecrackerConfig.KernelVersion).
			SetString(featureflags.SandboxFirecrackerVersionAttribute, sbx.Config.FirecrackerConfig.FirecrackerVersion).
			SetString(featureflags.SandboxEnvdVersionAttribute, sbx.Config.Envd.Version).
			Build(),
	)

	childSpan.SetAttributes(
		telemetry.WithTeamID(sbx.Runtime.TeamID),
		telemetry.WithTemplateID(sbx.Runtime.TemplateID),
		telemetry.WithFirecrackerVersion(sbx.Config.FirecrackerConfig.FirecrackerVersion),
		telemetry.WithKernelVersion(sbx.Config.FirecrackerConfig.KernelVersion),
		telemetry.WithEnvdVersion(sbx.Config.Envd.Version),
	)

	// Check envd version before snapshotting.
	if err := utils.CheckEnvdVersionForSnapshot(sbx.Config.Envd.Version); err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "%s", err.Error())
	}

	// The same flag-gated admission pre-flight as Pause (a checkpoint always
	// takes a full memory snapshot, on both the in-place and resume-fresh
	// paths); before waitForAcquire so a grace wait never holds a start slot.
	if graceMs := s.featureFlags.IntFlag(ctx, featureflags.PauseAdmissionGraceMs); graceMs >= 0 {
		outcome, waited, admitErr := sbx.AwaitSnapshotAdmission(ctx, time.Duration(graceMs)*time.Millisecond, true)
		s.recordPauseAdmission(ctx, "checkpoint", outcome, waited)
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
	if err := s.waitForAcquire(ctx); err != nil {
		return nil, err
	}
	defer s.startingSandboxes.Release(1)

	sbxlogger.E(sbx).Info(ctx, "Checkpointing sandbox")

	// In-place checkpoint (pause, snapshot, resume the SAME FC process) is
	// only honored for sandboxes resumed with use_sync_wp: it skips the
	// snapshot re-load that re-arms write-protection, so dirty tracking
	// across repeated checkpoints relies on the sync-WP serve loop. It also
	// requires the running Firecracker's release contract to include the
	// feature (e2b releases >= 0.2.0 — the CoW memory window drives the
	// balloon free-page-reporting pause API). Everything else takes the
	// resume-fresh flow, so an older FC degrades gracefully rather than
	// erroring.
	inPlace := sbx.UseSyncWP() &&
		s.featureFlags.BoolFlag(ctx, featureflags.InPlaceCheckpointFlag) &&
		firecrackerSupports(ctx, sbx, "in-place checkpoint", (*fcversion.Info).HasInPlaceCheckpoint)

	var res *orchestrator.SandboxCheckpointResponse
	var err error
	if inPlace {
		res, err = s.checkpointInPlace(ctx, sbx, in)
	} else {
		res, err = s.checkpointResumeFresh(ctx, sbx, in)
	}

	// The denominator for the in_place-labeled duration histograms: what
	// fraction of checkpoints went in-place, at what success rate.
	s.sandboxCheckpointCounter.Add(ctx, 1, metric.WithAttributes(
		attribute.Bool("in_place", inPlace),
		attribute.Bool("success", err == nil),
	))

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
			// is waiting this promise): finish the registered upload with
			// the error — freeing the build's upload future — and surface
			// it under the caller's failure code (FailedPrecondition for
			// in-place: the sandbox is alive, the API restores it to
			// Running).
			telemetry.ReportCriticalError(ctx, "deferred memory seal failed before checkpoint upload", sealErr, telemetry.WithSandboxID(in.GetSandboxId()))
			res.completeUpload(ctx, sealErr)
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
	defer res.completeUpload(uploadCtx, err)

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
func (s *Server) checkpointInPlace(ctx context.Context, sbx *sandbox.Sandbox, in *orchestrator.SandboxCheckpointRequest) (*orchestrator.SandboxCheckpointResponse, error) {
	// The sandbox stays live and addressable through an in-place checkpoint —
	// that is the point — so unlike resume-fresh there is no MarkStopping to
	// naturally exclude concurrent lifecycle RPCs. The API already serializes
	// them (checkpoint, kill and pause all pass through the sandbox state
	// machine: a second snapshot gets a conflict, a kill waits on the
	// transition), but guard here too so a misbehaving caller cannot race two
	// checkpoints into Pause/CreateSnapshot/ResumeInPlace on the same FC
	// process. FailedPrecondition tells the API the sandbox is still healthy.
	if !sbx.BeginInPlaceCheckpoint() {
		return nil, status.Errorf(codes.FailedPrecondition, "a checkpoint is already in progress for sandbox '%s'", in.GetSandboxId())
	}
	defer sbx.EndInPlaceCheckpoint()

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
		false, // filesystemOnly: full-memory checkpoint (fs-only in-place is a follow-up)
		deferRootfsExport,
		true, // maintainSandbox: resume in place
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
			s.emitSandboxKilled(ctx, sbx, killReasonResumeFailed)

			return nil, status.Errorf(codes.Internal, "error snapshotting sandbox '%s': %s", in.GetSandboxId(), err)
		}

		// FailedPrecondition, not Internal, for everything else: Pause's
		// resume-on-error cleanup has resumed the VM and restarted health
		// checks, so the sandbox is alive and healthy — only the checkpoint
		// artifact failed. Internal would make both API callers kill it (a
		// transient storage or disk error destroying a running sandbox). A
		// failing resume-on-error cleanup no longer lands here: it tears the
		// sandbox down and tags ErrSandboxLost, taking the branch above.
		return nil, status.Errorf(codes.FailedPrecondition, "error snapshotting sandbox '%s': %s", in.GetSandboxId(), err)
	}

	// Prefetch mapping is intentionally omitted for an in-place checkpoint: the
	// live PrefetchTracker holds the whole workload's fault history (not a
	// cold-start working set), and embedding it made launches from the produced
	// template over-prefetch. res.meta.Prefetch stays nil. The sandbox was already
	// resumed in place inside Pause, so there is no resume-fresh / lifecycle setup
	// / envd-upgrade / markSandboxLive here — the original sandbox keeps running.

	if err := s.runCheckpointUpload(ctx, sbx, res, in, codes.FailedPrecondition, nil); err != nil {
		return nil, err
	}

	s.publishSandboxEvent(ctx, sbx, events.SandboxCheckpointedEvent)

	telemetry.ReportEvent(ctx, "Checkpoint completed")

	return &orchestrator.SandboxCheckpointResponse{
		SchedulingMetadata: res.schedulingMetadata,
	}, nil
}

// checkpointResumeFresh snapshots the sandbox and resumes a FRESH sandbox
// from the produced build (new FC process, same ExecutionID). This is the
// pre-in-place checkpoint flow and the fallback whenever in-place is not
// available (async-WP sandbox or in-place-checkpoint flag off).
func (s *Server) checkpointResumeFresh(ctx context.Context, sbx *sandbox.Sandbox, in *orchestrator.SandboxCheckpointRequest) (_ *orchestrator.SandboxCheckpointResponse, checkpointErr error) {
	reservation, err := s.sandboxFactory.Sandboxes.MarkStoppingReserved(ctx, sbx.Runtime.SandboxID, sbx.LifecycleID)
	if errors.Is(err, sandbox.ErrSandboxOperationInProgress) {
		return nil, status.Errorf(codes.FailedPrecondition, "an operation is already in progress for sandbox '%s'", in.GetSandboxId())
	}
	if err != nil {
		telemetry.ReportCriticalError(ctx, "failed to mark sandbox as stopping", err, telemetry.WithSandboxID(in.GetSandboxId()))

		return nil, status.Errorf(codes.Internal, "failed to checkpoint sandbox '%s'", in.GetSandboxId())
	}
	rollback := sandbox.NewCleanup()
	rollback.Add(ctx, func(ctx context.Context) error { return stopAndCloseSandbox(ctx, sbx) })
	defer func() {
		s.finishSandboxStart(ctx, reservation, rollback, checkpointErr)
	}()

	// Always stop the old sandbox when done — on success the resumed sandbox
	// takes over, on failure this prevents a leaked sandbox that is running
	// but no longer addressable through the map. Stop is idempotent.
	defer s.stopSandboxAsync(context.WithoutCancel(ctx), sbx)

	// Set before the snapshot, as in Pause.
	sbx.SetStopReason(sandbox.StopReasonCheckpointing)

	// Checkpoint always takes a full memory snapshot; filesystem-only checkpoint
	// (resume-in-place would need to reboot) is not supported yet.
	// Checkpoint resumes a fresh sandbox from the new build immediately, so the
	// diff must be materialized synchronously — never defer the rootfs export
	// here, and never maintain the paused sandbox.
	res, err := s.snapshotAndCacheSandbox(ctx, sbx, in.GetBuildId(), in.GetMetadata(), storage.ObjectOriginSnapshotTemplate, false, false, false)
	if err != nil {
		telemetry.ReportCriticalError(ctx, "error snapshotting sandbox for checkpoint", err, telemetry.WithSandboxID(in.GetSandboxId()))

		return nil, status.Errorf(codes.Internal, "error snapshotting sandbox '%s': %s", in.GetSandboxId(), err)
	}

	// Get the template for resume
	template, err := s.templateCache.GetTemplate(ctx, in.GetBuildId(), true, false,
		sbxtemplate.GetTemplateOpts{MaxSandboxLengthHours: sbx.Config.MaxSandboxLengthHours})
	if err != nil {
		telemetry.ReportCriticalError(ctx, "error getting template for resume after checkpoint", err, telemetry.WithSandboxID(in.GetSandboxId()))

		return nil, status.Errorf(codes.Internal, "error getting template for resume: %s", err)
	}

	// Resume the sandbox keeping the same ExecutionID (stable identity for
	// the API, routing catalog, and analytics) but with a fresh LifecycleID
	// so the old sandbox's cleanup goroutine won't
	// accidentally evict the resumed sandbox from the map.
	resumedSbx, err := s.sandboxFactory.ResumeSandbox(
		ctx,
		template,
		sbx.Config,
		sandboxtypes.RuntimeMetadata{
			TemplateID:  sbx.Runtime.TemplateID,
			SandboxID:   sbx.Runtime.SandboxID,
			ExecutionID: sbx.Runtime.ExecutionID,
			TeamID:      sbx.Runtime.TeamID,
			BuildID:     sbx.Runtime.BuildID,
			SandboxType: sbx.Runtime.SandboxType,
		},
		sbx.GetStartedAt(),
		sbx.GetEndAt(),
		sbx.APIStoredConfig,
		// Defer routing until after the upgrade's post-/init (markSandboxLive below).
		sandbox.WithDeferredLiveRegistration(),
		sandbox.WithExecutionStartedAt(sbx.GetExecutionStartedAt()),
	)
	if err != nil {
		telemetry.ReportCriticalError(ctx, "error resuming sandbox after checkpoint", err, telemetry.WithSandboxID(in.GetSandboxId()))

		return nil, status.Errorf(codes.Internal, "error resuming sandbox after checkpoint: %s", err)
	}
	rollback.Add(ctx, func(ctx context.Context) error { return stopAndCloseSandbox(ctx, resumedSbx) })

	// Collect prefetch data immediately after resume while it's most accurate
	prefetchData, prefetchErr := resumedSbx.MemoryPrefetchData(ctx)
	if prefetchErr != nil {
		sbxlogger.I(resumedSbx).Warn(ctx, "failed to get prefetch data for checkpoint", zap.Error(prefetchErr))
	}

	// Setup lifecycle for the resumed sandbox
	s.setupSandboxLifecycle(ctx, resumedSbx)

	// resume-time envd live-upgrade. Best-effort and tightly gated so
	// it can never disrupt the universal resume path — except an unrecoverable
	// post-execve failure (new envd left uninitialized), which fails the
	// checkpoint rather than leave a bricked sandbox.
	if _, upErr := s.maybeUpgradeEnvd(ctx, resumedSbx); upErr != nil {
		resumedSbx.SetStopReason(sandbox.StopReasonKilled)

		return nil, upErr
	}

	// Promote to the live registry now that any resume-time upgrade's post-/init
	// has restored auth — the sandbox was resumed with routing deferred.
	if err := s.markSandboxLive(ctx, resumedSbx, reservation); err != nil {
		telemetry.ReportCriticalError(ctx, "error registering resumed sandbox after checkpoint", err, telemetry.WithSandboxID(in.GetSandboxId()))

		return nil, status.Errorf(codes.Internal, "error registering resumed sandbox after checkpoint: %s", err)
	}

	// Embed prefetch data into the metadata so it's uploaded with the snapshot files in a single pass.
	if prefetchErr == nil {
		prefetchMapping := metadata.PrefetchEntriesToMapping(slices.Collect(maps.Values(prefetchData.BlockEntries)), prefetchData.BlockSize)
		if prefetchMapping != nil {
			res.meta = res.meta.WithPrefetch(&metadata.Prefetch{
				Memory: prefetchMapping,
			})

			if err := s.templateCache.UpdateMetadata(in.GetBuildId(), res.meta); err != nil {
				sbxlogger.I(resumedSbx).Warn(ctx, "failed to update local metadata with prefetch", zap.Error(err))
			}
		}
	}

	// On upload failure, tear down the resumed sandbox — without a persisted
	// snapshot it cannot be paused or resumed later.
	if err := s.runCheckpointUpload(ctx, resumedSbx, res, in, codes.Internal, func() {
		resumedSbx.SetStopReason(sandbox.StopReasonKilled)
		s.sandboxFactory.Sandboxes.MarkStopping(ctx, resumedSbx.Runtime.SandboxID, resumedSbx.LifecycleID)
	}); err != nil {
		return nil, err
	}

	s.publishSandboxEvent(ctx, resumedSbx, events.SandboxCheckpointedEvent)

	telemetry.ReportEvent(ctx, "Checkpoint completed")

	return &orchestrator.SandboxCheckpointResponse{
		SchedulingMetadata: res.schedulingMetadata,
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
	startedAt := sbx.GetStartedAt()

	return map[string]any{
		"started_at":     startedAt.UTC().Format(time.RFC3339),
		"vcpu_count":     sbx.Config.Vcpu,
		"memory_mb":      sbx.Config.RamMB,
		"execution_time": time.Since(startedAt).Milliseconds(),
	}
}

// snapshotResult holds the data produced by snapshotAndCacheSandbox that
// callers need to start the background remote storage upload.
type snapshotResult struct {
	meta               metadata.Template
	schedulingMetadata *orchestrator.SchedulingMetadata
	upload             *sandbox.Upload
	completeUpload     func(ctx context.Context, uploadErr error)
	// rootfsDiff is the snapshot's rootfs diff. With deferred export it is a
	// promise-backed diff that resolves only once the background seal finishes,
	// so the prefetch harvest waits on its CachePath before its throwaway resume
	// (a warm resume reads the rootfs). Nil-safe callers only: always set here.
	rootfsDiff build.Diff
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
}

// snapshotAndCacheSandbox creates a snapshot of a sandbox and adds it to the
// local template cache. The caller is responsible for starting the remote
// storage upload via uploadSnapshotAsync.
func (s *Server) snapshotAndCacheSandbox(
	ctx context.Context,
	sbx *sandbox.Sandbox,
	buildID string,
	provenance map[string]string,
	buildOrigin storage.ObjectOrigin,
	filesystemOnly bool,
	deferRootfsExport bool,
	maintainSandbox bool,
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
	if maintainSandbox {
		pauseOpts = append(pauseOpts, sandbox.WithMaintainSandbox())
	}

	snapshot, err := sbx.Pause(ctx, meta, sandbox.SnapshotUseCasePause, pauseOpts...)
	if err != nil {
		return nil, fmt.Errorf("error snapshotting sandbox: %w", err)
	}

	err = s.templateCache.AddSnapshot(
		ctx,
		meta.Template.BuildID,
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
		return nil, fmt.Errorf("error adding snapshot to template cache: %w", err)
	}

	// Caller-supplied provenance (e.g. template_id) is forwarded as-is; team and
	// origin are orchestrator-authoritative and set last so they always win.
	objectMetadata := storage.ObjectMetadata{}
	maps.Copy(objectMetadata, provenance)
	objectMetadata[storage.ObjectMetadataTeamID] = sbx.Runtime.TeamID
	objectMetadata[storage.ObjectMetadataBuildOrigin] = string(buildOrigin)

	// Register the upload only after the snapshot is in the local cache, so a
	// failed AddSnapshot doesn't leave an orphan future blocking re-registration.
	upload, err := sandbox.NewUpload(ctx, s.uploads, snapshot, s.persistence, s.config.StorageConfig.CompressConfig, s.featureFlags, storage.UseCasePause, objectMetadata)
	if err != nil {
		return nil, fmt.Errorf("register upload: %w", err)
	}

	telemetry.ReportEvent(ctx, "added snapshot to template cache")

	// Capture once so Register and the symmetric Unregister inside
	// completeUpload don't drift if the flag flips mid-upload.
	peerEnabled := s.featureFlags.BoolFlag(ctx, featureflags.PeerToPeerChunkTransferFlag)

	completeUpload := func(ctx context.Context, uploadErr error) {
		upload.Finish(ctx, uploadErr)

		if !peerEnabled {
			return
		}

		// Only advertise the build as fully uploaded when it actually landed.
		// On abandon/failure the bytes are not in storage, so marking it would
		// make chunk-serving falsely report "already uploaded".
		if uploadErr == nil {
			s.uploadedBuilds.Set(meta.Template.BuildID, struct{}{}, ttlcache.DefaultTTL)
		}

		if err := s.peerRegistry.Unregister(ctx, meta.Template.BuildID); err != nil {
			logger.L().Warn(ctx, "failed to unregister peer address from routing", zap.String("build_id", meta.Template.BuildID), zap.Error(err))
		}
	}

	if peerEnabled {
		if err := s.peerRegistry.Register(ctx, meta.Template.BuildID, redisPeerKeyTTL); err != nil {
			logger.L().Warn(ctx, "failed to register peer address for routing", zap.String("build_id", meta.Template.BuildID), zap.Error(err))
		}
	}

	return &snapshotResult{
		meta:                 meta,
		schedulingMetadata:   snapshot.SchedulingMetadata,
		upload:               upload,
		completeUpload:       completeUpload,
		objectMetadata:       objectMetadata,
		filesystemOnly:       filesystemOnly,
		rootfsDiff:           snapshot.RootfsDiff,
		memoryExportDeferred: snapshot.MemoryExportDeferred,
		waitMemorySealed:     snapshot.WaitMemorySealed,
	}, nil
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
			s.uploadFailedCounter.Add(spanCtx, 1, metric.WithAttributes(attribute.Bool("fs_only", res.filesystemOnly)))
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
	if operationErr == nil || rollback == nil {
		reservation.Release()

		return
	}

	releaseWork := s.info.TrackWork()
	go func() {
		defer releaseWork()
		defer reservation.Release()
		ctx := context.WithoutCancel(ctx)
		if err := rollback.Run(ctx); err != nil {
			telemetry.ReportCriticalError(ctx, "failed to clean up sandbox start", err)
		}
	}()
}

func (s *Server) markSandboxLive(ctx context.Context, sbx *sandbox.Sandbox, reservation *sandbox.Reservation) error {
	if err := reservation.MarkRunning(ctx, sbx); err != nil {
		sbx.SetStopReason(sandbox.StopReasonKilled)

		return err
	}

	go sbx.Checks.Start(context.WithoutCancel(ctx))

	return nil
}

func (s *Server) sandboxAlreadyRunning(ctx context.Context, sandboxID, executionID string, cause error) error {
	telemetry.ReportCriticalError(ctx, "refusing to create sandbox: its ID is already taken on this node", cause,
		telemetry.WithSandboxID(sandboxID),
		attribute.String("requested_execution_id", executionID),
	)

	return status.Errorf(codes.AlreadyExists, "sandbox '%s' is already running on this node: %s", sandboxID, cause)
}

// setupSandboxLifecycle sets up the cleanup goroutine for a sandbox.
func (s *Server) setupSandboxLifecycle(ctx context.Context, sbx *sandbox.Sandbox) {
	s.sandboxFactory.Sandboxes.TrackLifecycle(ctx, sbx)
	releaseWork := s.info.TrackWork()
	go func() {
		defer releaseWork()

		ctx, childSpan := tracer.Start(context.WithoutCancel(ctx), "stop sandbox-lifecycle", trace.WithNewRoot())
		defer childSpan.End()

		waitErr := sbx.Wait(ctx)
		if waitErr != nil {
			sbxlogger.I(sbx).Error(ctx, "failed to wait for sandbox, cleaning up", zap.Error(waitErr))
		}

		sbx.SetStoppedAt(time.Now())

		// A guest that dies cleanly leaves no wait error, so the log above
		// misses it.
		if sbx.GetStopReason() == sandbox.StopReasonCrashed {
			sbxlogger.I(sbx).Error(ctx, "sandbox crashed", zap.Error(waitErr))
		}

		// Every ending — kill, pause, checkpoint hand-off, crash — passes here.
		s.recordExecutionDuration(ctx, sbx)

		cleanupErr := sbx.Close(ctx)
		if cleanupErr != nil {
			sbxlogger.I(sbx).Error(ctx, "failed to cleanup sandbox, will remove from cache", zap.Error(cleanupErr))
		}

		closeErr := s.proxy.RemoveFromPool(sbx.LifecycleID)
		if closeErr != nil {
			sbxlogger.I(sbx).Warn(ctx, "errors when manually closing connections to sandbox", zap.Error(closeErr))
		}

		sbxlogger.E(sbx).Info(ctx, "Sandbox stopped")
	}()
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
func (s *Server) recordUpgradePhase(ctx context.Context, phase, result string, start time.Time) {
	s.envdUpgradePhaseDuration.Record(ctx, time.Since(start).Milliseconds(),
		metric.WithAttributes(
			attribute.String("phase", phase),
			attribute.String("result", result),
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
		// consulted the cache and deliberately did not.
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
func (s *Server) recordDeliveryPhase(ctx context.Context, execConfirmed bool, err error, start time.Time) {
	s.recordUpgradePhase(ctx, "deliver", deliveryResult(execConfirmed, err), start)
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
				metric.WithAttributes(attribute.String("result", result)))
		}
	}()

	// The flag first, and on its own: resolving a target stats up to three paths and execs
	// the candidate for its version, and a cluster with the flag off must pay none of that.
	target := featureflags.EnvdUpgradeTarget(ctx, s.featureFlags)
	if featureflags.EnvdUpgradeTargetDisabled(target) {
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
	// The nil check is not redundant with the flag: a Factory built as a struct
	// literal (as tests do) has no cache.
	if cache := s.sandboxFactory.EnvdBinCache(); cache != nil && s.featureFlags.BoolFlag(ctx, featureflags.EnvdBinaryCacheFlag) {
		binCache = envdbin.NewResolver(cache, envdbin.OpLive)
		getVersion = binCache.Version
	}

	path, tv, reason := featureflags.ResolveEnvdUpgrade(ctx, target, from, s.config.HostEnvdPath, getVersion)
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
		s.recordUpgradePhase(ctx, "resolve", resolveResult, resolveStart)
	}
	toVersion = tv
	if path == "" {
		switch reason {
		case "off", "same_version":
			// expected no-op — not counted
		case envdbin.ReasonNotCached:
			// Counted, because it is the cost side of gating on the cache, but not
			// warned: on a node that has just booted this is the expected state for
			// one resume, and the background warm clears it.
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
		s.recordDeliveryPhase(ctx, execConfirmed, err, deliverStart)
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
	s.recordUpgradePhase(ctx, "ready", phaseResult(readyErr), readyStart)
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
