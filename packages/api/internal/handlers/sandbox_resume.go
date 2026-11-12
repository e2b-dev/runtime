package handlers

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	snapshotcache "github.com/e2b-dev/infra/packages/api/internal/cache/snapshots"
	"github.com/e2b-dev/infra/packages/api/internal/orchestrator"
	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
	"github.com/e2b-dev/infra/packages/api/internal/utils"
	"github.com/e2b-dev/infra/packages/auth/pkg/auth"
	"github.com/e2b-dev/infra/packages/db/pkg/types"
	"github.com/e2b-dev/infra/packages/db/queries"
	"github.com/e2b-dev/infra/packages/shared/pkg/apierrors"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/ginutils"
	orchestratorgrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	proxygrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc/proxy"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	sbxlogger "github.com/e2b-dev/infra/packages/shared/pkg/logger/sandbox"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

// resumeWaitOrchestrator is the slice of *orchestrator.Orchestrator the resume
// handler consults before it commits to resuming: the record read and the
// wait on an in-flight transition.
type resumeWaitOrchestrator interface {
	GetSandbox(ctx context.Context, teamID uuid.UUID, sandboxID string) (sandbox.Sandbox, error)
	WaitForStateChange(ctx context.Context, teamID uuid.UUID, sandboxID string) error
}

func (a *APIStore) resumeBackend() resumeWaitOrchestrator {
	if a.resumeBackendOverride != nil {
		return a.resumeBackendOverride
	}

	return a.orchestrator
}

func (a *APIStore) PostSandboxesSandboxIDResume(c *gin.Context, sandboxID api.SandboxID) {
	ctx := c.Request.Context()

	// Get team from context, use TeamContextKey
	teamInfo := auth.MustGetTeamInfo(c)

	span := trace.SpanFromContext(ctx)
	traceID := span.SpanContext().TraceID().String()
	c.Set("traceID", traceID)

	sandboxID, err := utils.ShortID(sandboxID)
	if err != nil {
		a.sendAPIStoreError(c, http.StatusBadRequest, "Invalid sandbox ID")

		return
	}

	span.SetAttributes(telemetry.WithSandboxID(sandboxID))

	// The body is optional: every field defaults, so tolerate an absent one.
	body, err := ginutils.ParseOptionalBody[api.PostSandboxesSandboxIDResumeJSONRequestBody](ctx, c)
	if err != nil {
		a.sendAPIStoreError(c, http.StatusBadRequest, fmt.Sprintf("Error when parsing request: %s", err))

		telemetry.ReportCriticalError(ctx, "error when parsing request", err)

		return
	}

	telemetry.ReportEvent(ctx, "Parsed body")

	timeout, apiErr := validateAndParseTimeout(body.Timeout, teamInfo.Limits.MaxLengthHours)
	if apiErr != nil {
		a.sendAPIStoreError(c, apiErr.Code, apiErr.ClientMsg)

		return
	}

	teamID := teamInfo.Team.ID
	backend := a.resumeBackend()

	sandboxData, err := backend.GetSandbox(ctx, teamID, sandboxID)
	if err == nil {
		if sandboxData.TeamID != teamID {
			logger.L().Debug(ctx, "Sandbox team mismatch on resume", logger.WithSandboxID(sandboxID), logger.WithTeamID(teamID.String()))
			a.sendAPIStoreError(c, http.StatusNotFound, utils.SandboxNotFoundMsg(sandboxID))

			return
		}

		switch sandboxData.State {
		case sandbox.StatePausing:
			logger.L().Debug(ctx, "Waiting for sandbox to pause", logger.WithSandboxID(sandboxID))
			err = backend.WaitForStateChange(ctx, teamID, sandboxID)
			if errors.Is(err, sandbox.ErrTransitionRestored) {
				a.sendAPIStoreError(c, http.StatusConflict, fmt.Sprintf("Sandbox %s is already running", sandboxID))

				return
			}
			if err != nil {
				telemetry.ReportCriticalError(ctx, "error waiting for sandbox to pause", err,
					telemetry.WithSandboxID(sandboxID),
					telemetry.WithTeamID(teamID.String()),
				)
				a.sendAPIStoreError(c, http.StatusInternalServerError, "Error waiting for sandbox to pause")

				return
			}
			// The transition can complete before the wait looks: re-read, a
			// refused-and-restored pause leaves the sandbox running.
			if current, getErr := backend.GetSandbox(ctx, teamID, sandboxID); getErr == nil && current.State == sandbox.StateRunning {
				a.sendAPIStoreError(c, http.StatusConflict, fmt.Sprintf("Sandbox %s is already running", sandboxID))

				return
			}
		case sandbox.StateKilling:
			logger.L().Debug(ctx, "Sandbox is being killed, cannot resume", logger.WithSandboxID(sandboxID))
			a.sendAPIStoreError(c, http.StatusNotFound, utils.SandboxNotFoundMsg(sandboxID))

			return
		case sandbox.StateSnapshotting:
			a.sendAPIStoreError(c, http.StatusConflict, fmt.Sprintf("Sandbox snapshot is currently being created for sandbox '%s'", sandboxID))

			return
		case sandbox.StateRunning:
			a.sendAPIStoreError(c, http.StatusConflict, fmt.Sprintf("Sandbox %s is already running", sandboxID))

			logger.L().Debug(ctx, "Sandbox is already running",
				logger.WithSandboxID(sandboxID),
				logger.Time("end_time", sandboxData.EndTime),
				logger.Time("start_time", sandboxData.StartTime),
				zap.String("node_id", sandboxData.NodeID),
			)

			return
		default:
			telemetry.ReportCriticalError(ctx, "Sandbox is in an unknown state", fmt.Errorf("state: %s", sandboxData.State),
				telemetry.WithSandboxID(sandboxID),
				telemetry.WithTeamID(teamID.String()),
			)
			a.sendAPIStoreError(c, http.StatusInternalServerError, "Sandbox is in an unknown state")

			return
		}
	}

	// TODO: ENG-3544 scope GetLastSnapshot query by teamID to avoid post-fetch ownership check.
	lastSnapshot, err := a.snapshotCache.Get(ctx, sandboxID)
	if err != nil {
		if errors.Is(err, snapshotcache.ErrSnapshotNotFound) {
			logger.L().Debug(ctx, "Snapshot not found", logger.WithSandboxID(sandboxID))
			a.sendAPIStoreError(c, http.StatusNotFound, utils.SandboxNotFoundMsg(sandboxID))

			return
		}

		telemetry.ReportCriticalError(ctx, "Error getting last snapshot", err,
			telemetry.WithSandboxID(sandboxID),
			telemetry.WithTeamID(teamID.String()),
		)
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Error when getting snapshot")

		return
	}

	if lastSnapshot.Snapshot.TeamID != teamID {
		telemetry.ReportError(ctx, fmt.Sprintf("snapshot for sandbox '%s' doesn't belong to team '%s'", sandboxID, teamID.String()), nil)
		a.sendAPIStoreError(c, http.StatusNotFound, utils.SandboxNotFoundMsg(sandboxID))

		return
	}

	// Pre-flight of the fetcher's authoritative gate so a disabled flag answers
	// 400 even when the start would otherwise join an in-flight one (409).
	if _, apiErr := resolveFilesystemBoot(ctx, a.featureFlags, body.Memory, lastSnapshot.Snapshot, lastSnapshot.EnvBuild); apiErr != nil {
		setMemoryOverrideOutcome(c, body.Memory, apiErr)
		setResumeGateOutcome(c, body.Memory, lastSnapshot.EnvBuild, apiErr)
		apierrors.SendAPIError(c, apiErr)

		return
	}

	// Refuse a cpuCount the snapshot or the team cannot honour before the start
	// is reserved, so the 400 reaches only this request and never a connect or
	// auto-resume joined to the same start.
	if apiErr := orchestrator.CheckSandboxCPUCount(ctx, a.featureFlags, teamInfo, lastSnapshot.EnvBuild, body.CpuCount); apiErr != nil {
		setMemoryOverrideOutcome(c, body.Memory, apiErr)
		apierrors.SendAPIError(c, apiErr)

		return
	}

	sbxlogger.E(&sbxlogger.SandboxMetadata{
		SandboxID:  sandboxID,
		TemplateID: lastSnapshot.Snapshot.EnvID,
		TeamID:     teamID.String(),
	}).Debug(ctx, "Started resuming sandbox")

	sbx, createErr := a.startSandbox(
		ctx,
		sandboxID,
		timeout,
		teamInfo,
		a.buildResumeSandboxData(sandboxID, body.AutoPause, body.Memory),
		&c.Request.Header,
		true,
		demandsFilesystemBoot(body.Memory, lastSnapshot.Snapshot),
		nil, // mcp
		orchestrator.WithCPUCount(body.CpuCount),
	)
	setMemoryOverrideOutcome(c, body.Memory, createErr)
	setResumeGateOutcome(c, body.Memory, lastSnapshot.EnvBuild, createErr)
	if createErr != nil {
		responseErr := *createErr
		responseErr.Retryable = apierrors.UndefinedRetry
		apierrors.SendAPIError(c, &responseErr)

		return
	}

	c.JSON(http.StatusCreated, &sbx)
}

func convertDatabaseMountsToOrchestratorMounts(volumes []*types.SandboxVolumeMountConfig) []*orchestratorgrpc.SandboxVolumeMount {
	results := make([]*orchestratorgrpc.SandboxVolumeMount, 0, len(volumes))

	for _, item := range volumes {
		results = append(results, &orchestratorgrpc.SandboxVolumeMount{
			Id:       item.ID,
			Type:     item.Type,
			Name:     item.Name,
			Path:     item.Path,
			Nconnect: uint32(item.Nconnect),
		})
	}

	return results
}

// buildResumeSandboxData returns a SandboxDataFetcher for resuming a sandbox
// from its own snapshot. memory is the request's optional memory field; nil
// (the implicit paths: auto-resume, fork) means a plain resume.
func (a *APIStore) buildResumeSandboxData(sandboxID string, autoPauseOverride, memory *bool) orchestrator.SandboxDataFetcher {
	return a.buildResumeSandboxDataFromSnapshot(sandboxID, sandboxID, autoPauseOverride, memory)
}

const (
	errCodeMemoryOverrideDisabled = "sandbox_memory_override_disabled"
	// errCodeSnapshotGuestAgentUnresponsive refuses a memory resume of a
	// snapshot the node took while the sandbox's guest agent did not answer;
	// the proxy's copy, so auto-resume renders it as the refusal it is.
	errCodeSnapshotGuestAgentUnresponsive = proxygrpc.SnapshotGuestAgentUnresponsive
)

// setMemoryOverrideOutcome labels the request metric with the fate of an
// explicit memory:false so the ramp is measurable on http.server.duration:
// served, or rejected (flag off, join refused, unconfirmed echo, other).
func setMemoryOverrideOutcome(c *gin.Context, memory *bool, createErr *api.APIError) {
	if memory == nil || *memory {
		return
	}

	outcome := "served"
	switch {
	case createErr == nil:
	case createErr.ErrorCode == errCodeMemoryOverrideDisabled:
		outcome = "rejected_flag_off"
	case createErr.ErrorCode == orchestrator.ErrCodeStartInFlight:
		outcome = "rejected_in_flight_start"
	case createErr.ErrorCode == orchestrator.ErrCodeFilesystemBootUnconfirmed:
		outcome = "rejected_unconfirmed"
	default:
		outcome = "error"
	}
	c.Set(metricMemoryOverride, outcome)
}

// snapshotIsFilesystemOnly reports whether the stored snapshot persists only
// the rootfs. Rows written before the kind was recorded have a nil config and
// are memory snapshots.
func snapshotIsFilesystemOnly(snap queries.Snapshot) bool {
	return snap.Config != nil && snap.Config.FilesystemOnly
}

// setResumeGateOutcome labels the request metric with the fate of a resume
// of a snapshot taken over an unresponsive guest agent, so the refusals and
// the memory-less recoveries are countable on http.server.duration: refused
// (a plain resume answered the conflict) or served without memory. Any other
// resume is unlabeled; metricMemoryOverride keeps labeling memory:false.
func setResumeGateOutcome(c *gin.Context, memory *bool, build queries.EnvBuild, createErr *api.APIError) {
	switch {
	case createErr != nil && createErr.ErrorCode == errCodeSnapshotGuestAgentUnresponsive:
		c.Set(metricResumeGate, "refused_guest_agent_unresponsive")
	case createErr == nil && memory != nil && !*memory && snapshotEnvdUnresponsive(build):
		c.Set(metricResumeGate, "served_without_memory")
	}
}

// snapshotGuestAgentUnresponsiveError is the refusal of a memory resume of a
// snapshot taken over an unresponsive guest agent: a conflict with its own
// code, so the explicit resume paths and the auto-resume answer alike.
func snapshotGuestAgentUnresponsiveError(sandboxID string) *api.APIError {
	return &api.APIError{
		Code:      http.StatusConflict,
		ErrorCode: errCodeSnapshotGuestAgentUnresponsive,
		ClientMsg: fmt.Sprintf("Sandbox '%s' cannot be resumed with its memory: its snapshot was taken while the sandbox guest agent was not responding, and resuming the memory would bring that back. Resume it without memory (memory: false) to cold-boot it from its disk", sandboxID),
		Err:       fmt.Errorf("memory resume of snapshot '%s' taken over an unresponsive guest agent refused", sandboxID),
	}
}

// snapshotEnvdUnresponsive reports whether the build a sandbox would resume
// from was taken while its guest agent did not answer the pause's health
// probe. Builds from before the mark, and builds whose agent answered, carry
// no value and are memory snapshots like any other.
func snapshotEnvdUnresponsive(build queries.EnvBuild) bool {
	return build.EnvdUnresponsive != nil && *build.EnvdUnresponsive
}

// envdUnresponsiveAtPause is the API's optional field for the mark: set only
// when it is true, so a running sandbox and an ordinary snapshot omit it.
func envdUnresponsiveAtPause(mark *bool) *api.EnvdUnresponsiveAtPause {
	if mark == nil || !*mark {
		return nil
	}

	return new(true)
}

// demandsFilesystemBoot reports whether the request explicitly demands a cold
// boot that an in-flight start might not honor: memory:false on a snapshot not
// already filesystem-only (an fs-only snapshot cold-boots on any start, so a
// join is safe for it).
func demandsFilesystemBoot(memory *bool, snap queries.Snapshot) bool {
	if memory == nil || *memory {
		return false
	}

	return !snapshotIsFilesystemOnly(snap)
}

// resolveFilesystemBoot maps the request's optional memory field (default
// true) to the create RPC's filesystem-boot demand. Flag off rejects rather
// than silently memory-restoring; a filesystem-only snapshot already
// cold-boots from its own metadata, so the RPC stays unchanged for it. A
// snapshot taken while the guest agent did not answer is refused a memory
// resume, since that memory holds the unresponsive agent, and is cold-booted
// on memory:false for every team: the flag gates a choice, not a rescue.
func resolveFilesystemBoot(ctx context.Context, flags featureFlagsClient, memory *bool, snap queries.Snapshot, build queries.EnvBuild) (bool, *api.APIError) {
	if snapshotIsFilesystemOnly(snap) {
		return false, nil
	}

	if memory == nil || *memory {
		if snapshotEnvdUnresponsive(build) {
			return false, snapshotGuestAgentUnresponsiveError(snap.SandboxID)
		}

		return false, nil
	}

	if snapshotEnvdUnresponsive(build) {
		return true, nil
	}

	if !flags.BoolFlag(ctx, featureflags.FsOnlyResumeAPIFlag,
		featureflags.TeamContext(snap.TeamID.String()),
		featureflags.SandboxContext(snap.SandboxID),
	) {
		return false, &api.APIError{
			Code:      http.StatusBadRequest,
			ErrorCode: errCodeMemoryOverrideDisabled,
			ClientMsg: "Resuming without memory (memory: false) is not enabled for this team; a plain resume still restores memory",
			Err:       fmt.Errorf("fs-only resume of memory snapshot '%s' rejected: feature disabled", snap.SandboxID),
		}
	}

	return true, nil
}

// buildResumeSandboxDataFromSnapshot returns a SandboxDataFetcher that fetches
// snapshot data for snapshotSandboxID from the cache and builds SandboxMetadata
// for resume operations. sandboxID is the ID the sandbox will run under — it
// differs from snapshotSandboxID when forking — and scopes the envd access token.
// The returned callback is called inside the sandbox lock to prevent race conditions.
func (a *APIStore) buildResumeSandboxDataFromSnapshot(snapshotSandboxID, sandboxID string, autoPauseOverride, memory *bool) orchestrator.SandboxDataFetcher {
	return func(ctx context.Context) (orchestrator.SandboxMetadata, *api.APIError) {
		lastSnapshot, err := a.snapshotCache.Get(ctx, snapshotSandboxID)
		if err != nil {
			return orchestrator.SandboxMetadata{}, &api.APIError{
				Code:      http.StatusInternalServerError,
				ClientMsg: "Error when getting snapshot",
				Err:       fmt.Errorf("error getting last snapshot for sandbox '%s': %w", snapshotSandboxID, err),
			}
		}

		snap := lastSnapshot.Snapshot
		build := lastSnapshot.EnvBuild

		// Resolved here rather than in the handler so the decision reads the
		// same locked snapshot fetch the create request is built from.
		filesystemBoot, apiErr := resolveFilesystemBoot(ctx, a.featureFlags, memory, snap, build)
		if apiErr != nil {
			return orchestrator.SandboxMetadata{}, apiErr
		}

		nodeID := snap.OriginNodeID

		alias := ""
		if len(lastSnapshot.Aliases) > 0 {
			alias = lastSnapshot.Aliases[0]
		}

		var envdAccessToken *string
		if snap.EnvSecure {
			accessToken, tokenErr := a.getEnvdAccessToken(build.EnvdVersion, sandboxID)
			if tokenErr != nil {
				return orchestrator.SandboxMetadata{}, tokenErr
			}
			envdAccessToken = &accessToken
		}

		autoPause := snap.AutoPause
		if autoPauseOverride != nil {
			autoPause = *autoPauseOverride
		}

		var network *types.SandboxNetworkConfig
		var autoResume *types.SandboxAutoResumeConfig
		var volumes []*types.SandboxVolumeMountConfig
		// Unlike auto_pause (which resume can override via the request body), the
		// auto-pause snapshot kind is intentionally always inherited from the
		// snapshot: there is no resume-time override for it. Changing the kind
		// requires creating a new sandbox with the desired autoPauseMemory.
		var autoPauseFilesystemOnly bool
		// A fork (snapshotSandboxID != sandboxID) inherits the parent sandbox's IAM
		// configuration from the snapshot, the same as a resume. The resumed/forked
		// execution still gets a freshly generated execution ID upstream, so no
		// stored identity subject is carried across.
		var iam *types.SandboxIam
		if snap.Config != nil {
			network = snap.Config.Network
			autoResume = snap.Config.AutoResume
			volumes = snap.Config.VolumeMounts
			autoPauseFilesystemOnly = snap.Config.AutoPauseFilesystemOnly
			iam = snap.Config.Iam
		}

		return orchestrator.SandboxMetadata{
			Metadata:                snap.Metadata,
			Build:                   build,
			AllowInternetAccess:     snap.AllowInternetAccess,
			Network:                 network,
			Alias:                   alias,
			TemplateID:              snap.EnvID,
			BaseTemplateID:          snap.BaseEnvID,
			AutoPause:               autoPause,
			AutoPauseFilesystemOnly: autoPauseFilesystemOnly,
			AutoResume:              autoResume,
			VolumeMounts:            convertDatabaseMountsToOrchestratorMounts(volumes),
			EnvdAccessToken:         envdAccessToken,
			Iam:                     iam,
			NodeID:                  &nodeID,
			SnapshotSandboxID:       snapshotSandboxID,
			FilesystemBoot:          filesystemBoot,
			FilesystemOnlySnapshot:  snapshotIsFilesystemOnly(snap),
		}, nil
	}
}
