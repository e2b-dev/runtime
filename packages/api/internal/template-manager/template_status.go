package template_manager

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/flowchartsman/retry"
	"github.com/google/uuid"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/e2b-dev/infra/packages/api/internal/clusters"
	"github.com/e2b-dev/infra/packages/db/pkg/types"
	"github.com/e2b-dev/infra/packages/db/queries"
	templatemanagergrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc/template-manager"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

var (
	buildTimeout             = time.Hour
	syncWaitingStateDeadline = time.Minute * 40

	// transientErrorGracePeriod bounds how long the poller tolerates transient
	// status errors — the build keeps running on the builder while we retry.
	transientErrorGracePeriod = 5 * time.Minute
)

// errTransientStatus marks a status error worth retrying instead of failing the build.
var errTransientStatus = errors.New("transient error")

// terminalWriteTimeout bounds a write that records a build's final status.
const terminalWriteTimeout = 30 * time.Second

// terminalWriteContext returns the context for recording a build's final
// status. It is detached from ctx because the poll context expires at the build
// deadline, and pgx fast-fails an expired context while the retry wrapper does
// not retry context errors: a terminal write made on it never lands, leaving
// the build in progress forever.
func terminalWriteContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), terminalWriteTimeout)
}

func (tm *TemplateManager) BuildStatusSync(ctx context.Context, buildID uuid.UUID, templateID string, clusterID uuid.UUID, nodeID *string) error {
	if tm.createInProcessingQueue(buildID, templateID) {
		// already processing, skip
		return nil
	}

	// remove from processing queue when done
	defer tm.removeFromProcessingQueue(buildID)

	result, err := tm.sqlcDB.GetTemplateBuildWithTemplate(ctx, queries.GetTemplateBuildWithTemplateParams{
		TemplateID: templateID,
		BuildID:    buildID,
	})
	if err != nil {
		return fmt.Errorf("failed to get env build: %w", err)
	}

	envBuild := result.EnvBuild
	// waiting for build to start, local docker build and push can take some time
	// so just check if it's not too long
	if envBuild.StatusGroup == types.BuildStatusGroupPending {
		// if waiting for too long, fail the build
		if time.Since(envBuild.CreatedAt) > syncWaitingStateDeadline {
			err = tm.SetStatus(ctx, buildID, types.BuildStatusGroupFailed, &templatemanagergrpc.TemplateBuildStatusReason{
				Message: "build is in waiting state for too long",
			})
			if err != nil {
				logger.L().Error(ctx, "error when setting build status to failed after waiting for too long", zap.Error(err), logger.WithBuildID(buildID.String()), logger.WithTemplateID(templateID))
			}

			return errors.New("build is in waiting state for too long, failing it")
		}

		// just wait for next sync
		return nil
	}

	if nodeID == nil {
		return errors.New("build is not assigned to a node, but it should be")
	}

	checker := &PollBuildStatus{
		client: tm,
		logger: logger.L().With(logger.WithBuildID(buildID.String()), logger.WithTemplateID(templateID)),

		templateID: templateID,
		buildID:    buildID,

		clusterID: clusterID,
		nodeID:    *nodeID,
	}

	// context for the building phase
	ctx, buildCancel := context.WithTimeout(ctx, buildTimeout)
	defer buildCancel()

	checker.poll(ctx)

	return nil
}

type templateManagerClient interface {
	SetTerminalStatus(ctx context.Context, buildID uuid.UUID, statusGroup types.BuildStatusGroup, reason *templatemanagergrpc.TemplateBuildStatusReason) (bool, error)
	SetFinished(ctx context.Context, buildID uuid.UUID, rootfsSize int64, envdVersion, kernelVersion, firecrackerVersion string) error
	GetStatus(ctx context.Context, buildId uuid.UUID, templateID string, clusterID uuid.UUID, nodeID string) (*templatemanagergrpc.TemplateBuildStatusResponse, error)
	DeleteBuild(ctx context.Context, buildID uuid.UUID, templateID string, clusterID uuid.UUID, nodeID string) error
}

type PollBuildStatus struct {
	logger logger.Logger
	client templateManagerClient

	templateID string
	buildID    uuid.UUID

	clusterID uuid.UUID
	nodeID    string

	status *templatemanagergrpc.TemplateBuildStatusResponse

	// transientErrorsSince is when the current run of transient status errors
	// started. Zero while the builder is answering.
	transientErrorsSince time.Time
}

func (c *PollBuildStatus) poll(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			// A cancelled window means the poller is going away, not the build.
			// The periodical sync re-adopts the build, so nothing is recorded.
			if !errors.Is(ctx.Err(), context.DeadlineExceeded) {
				c.logger.Debug(ctx, "Build status polling stopped, leaving the build in progress", zap.Error(ctx.Err()))

				return
			}

			c.logger.Debug(ctx, "Build status polling timed out, stopping polling")

			if c.setFailed(ctx, fmt.Sprintf("build status polling timed out. Maximum build time is %s.", buildTimeout)) {
				c.cancelBuildOnNode(ctx)
			}

			return
		case <-ticker.C:
			buildCompleted, err := c.checkBuildStatus(ctx)
			if err != nil {
				// The check was cut short by the window closing, which says nothing
				// about the build. The ctx.Done() case decides what to record.
				if ctx.Err() != nil {
					c.logger.Debug(ctx, "Build status polling window closed mid-check",
						zap.Error(errors.Join(ctx.Err(), err)))

					continue
				}

				waitStarting := c.transientErrorsSince.IsZero()
				failure := c.buildFailure(err)
				if failure == nil {
					// Logged once per run of errors, not on every tick.
					if waitStarting {
						c.logger.Warn(ctx, "Build status polling received a transient error, keeping the build alive",
							zap.Error(err), zap.Duration("grace_period", transientErrorGracePeriod))
					}

					continue
				}

				c.logger.Error(ctx, "Build status polling failed", zap.Error(failure))

				c.setFailed(ctx, failure.Error())

				return
			}

			if !c.transientErrorsSince.IsZero() {
				c.logger.Info(ctx, "Build status polling recovered", zap.Duration("failing_for", time.Since(c.transientErrorsSince)))
				c.transientErrorsSince = time.Time{}
			}

			// build status can return empty error when build is still in progress
			// this will cause fast return to avoid pooling when build is already finished
			if buildCompleted {
				return
			}
		}
	}
}

// buildFailure turns a polling error into the reason the build should be failed
// with, or nil when the error is transient and the poller should just try again.
func (c *PollBuildStatus) buildFailure(err error) error {
	if !errors.Is(err, errTransientStatus) {
		return fmt.Errorf("polling received unrecoverable error: %w", err)
	}

	if c.transientErrorsSince.IsZero() {
		c.transientErrorsSince = time.Now()
	}

	if time.Since(c.transientErrorsSince) >= transientErrorGracePeriod {
		return fmt.Errorf("polling kept failing for %s: %w", transientErrorGracePeriod, err)
	}

	return nil
}

// setFailed records the build as failed. It reports false when another poller
// ended the build first, in which case this write changed nothing.
func (c *PollBuildStatus) setFailed(ctx context.Context, message string) bool {
	writeCtx, cancel := terminalWriteContext(ctx)
	defer cancel()

	recorded, err := c.client.SetTerminalStatus(writeCtx, c.buildID, types.BuildStatusGroupFailed, &templatemanagergrpc.TemplateBuildStatusReason{
		Message: message,
	})
	if err != nil {
		c.logger.Error(writeCtx, "error when setting build status", zap.Error(err))
	}

	return recorded
}

// cancelBuildOnNode stops a build the poller has just failed for running out of
// time. The builder has no deadline of its own, and a build that is no longer in
// progress is watched by neither the periodical sync nor the admin cancel
// endpoint, so left alone it holds its node slot to completion. The delete takes
// the build's artifacts with it, so it must follow a write that ended the build
// rather than one that lost to another poller.
func (c *PollBuildStatus) cancelBuildOnNode(ctx context.Context) {
	writeCtx, cancel := terminalWriteContext(ctx)
	defer cancel()

	err := c.client.DeleteBuild(writeCtx, c.buildID, c.templateID, c.clusterID, c.nodeID)
	if err != nil {
		c.logger.Error(writeCtx, "error when cancelling the timed-out build on the node", zap.Error(err))
	}
}

// isTransientStatusError reports whether a status RPC failed for a reason that
// says nothing about the build itself. gRPC status errors do not unwrap to
// context.DeadlineExceeded, so the status code has to be inspected explicitly.
func isTransientStatusError(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}

	// The builder lookup also fails while the instance flaps Unhealthy after a few
	// missed syncs — as recoverable as the RPC-level errors below.
	if errors.Is(err, clusters.ErrTemplateBuilderNotFound) {
		return true
	}

	st, ok := grpcstatus.FromError(err)
	if !ok {
		return false
	}

	switch st.Code() {
	case codes.DeadlineExceeded, codes.Unavailable, codes.ResourceExhausted:
		return true
	default:
		return false
	}
}

func (c *PollBuildStatus) setStatus(ctx context.Context) error {
	status, err := c.client.GetStatus(ctx, c.buildID, c.templateID, c.clusterID, c.nodeID)
	if err != nil && isTransientStatusError(err) {
		return fmt.Errorf("%w when polling build status: %w", errTransientStatus, err)
	} else if err != nil { // retry only on transient errors
		c.logger.Error(ctx, "terminal error when polling build status", zap.Error(err))

		return retry.Stop(err)
	}

	if status == nil {
		return errors.New("nil status") // this should never happen
	}

	// debug log the status
	c.logger.Debug(ctx, "setting status pointer", zap.String("status", status.GetStatus().String()))

	c.status = status

	return nil
}

func (c *PollBuildStatus) dispatchBasedOnStatus(ctx context.Context, status *templatemanagergrpc.TemplateBuildStatusResponse) (bool, error) {
	if status == nil {
		return false, errors.New("nil status")
	}
	switch status.GetStatus() {
	case templatemanagergrpc.TemplateBuildState_Failed:
		// build failed
		writeCtx, cancel := terminalWriteContext(ctx)
		defer cancel()

		_, err := c.client.SetTerminalStatus(writeCtx, c.buildID, types.BuildStatusGroupFailed, status.GetReason())
		if err != nil {
			return false, fmt.Errorf("error when setting build status: %w", err)
		}

		return true, nil
	case templatemanagergrpc.TemplateBuildState_Completed:
		// build completed
		meta := status.GetMetadata()
		if meta == nil {
			return false, errors.New("nil metadata")
		}

		writeCtx, cancel := terminalWriteContext(ctx)
		defer cancel()

		err := c.client.SetFinished(
			writeCtx,
			c.buildID,
			int64(meta.GetRootfsSizeKey()),
			meta.GetEnvdVersionKey(),
			meta.GetKernelVersion(),
			meta.GetFirecrackerVersion(),
		)
		if err != nil {
			return false, fmt.Errorf("error when finishing build: %w", err)
		}

		return true, nil
	default:
		c.logger.Debug(ctx, "skipping status", zap.String("status", status.GetStatus().String()))

		return false, nil
	}
}

func (c *PollBuildStatus) checkBuildStatus(ctx context.Context) (bool, error) {
	c.logger.Debug(ctx, "Checking template build status")

	retrier := retry.NewRetrier(
		10,
		100*time.Millisecond,
		time.Second,
	)

	err := retrier.RunContext(ctx, c.setStatus)
	if err != nil {
		return false, err
	}

	c.logger.Debug(ctx, "dispatching based on status", zap.String("status", c.status.GetStatus().String()))

	buildCompleted, err := c.dispatchBasedOnStatus(ctx, c.status)
	if err != nil {
		return false, fmt.Errorf("error when dispatching build status: %w", err)
	}

	return buildCompleted, nil
}

func (tm *TemplateManager) removeFromProcessingQueue(buildID uuid.UUID) {
	tm.lock.Lock()
	delete(tm.processing, buildID)
	tm.lock.Unlock()
}

func (tm *TemplateManager) createInProcessingQueue(buildID uuid.UUID, templateID string) bool {
	tm.lock.Lock()
	defer tm.lock.Unlock()

	_, exists := tm.processing[buildID]
	if exists {
		// already in processing queue, skip
		return true
	}

	tm.processing[buildID] = processingBuilds{templateID: templateID}

	return false
}

func (tm *TemplateManager) SetStatus(ctx context.Context, buildID uuid.UUID, statusGroup types.BuildStatusGroup, reason *templatemanagergrpc.TemplateBuildStatusReason) error {
	if statusGroup.IsTerminal() {
		_, err := tm.SetTerminalStatus(ctx, buildID, statusGroup, reason)

		return err
	}

	now := time.Now()

	err := tm.sqlcDB.UpdateEnvBuildStatus(ctx, queries.UpdateEnvBuildStatusParams{
		Status:     buildStatus(statusGroup),
		FinishedAt: &now,
		Reason:     buildReasonFrom(reason),
		BuildID:    buildID,
	})

	tm.buildCache.Invalidate(ctx, buildID)

	return err
}

// SetTerminalStatus records a build's final outcome and reports whether this
// write is the one that ended it: several pollers watch the same build, and the
// build keeps the first outcome recorded.
func (tm *TemplateManager) SetTerminalStatus(ctx context.Context, buildID uuid.UUID, statusGroup types.BuildStatusGroup, reason *templatemanagergrpc.TemplateBuildStatusReason) (bool, error) {
	buildReason := buildReasonFrom(reason)

	logger.L().Warn(ctx, "Setting template build status to terminal failure",
		logger.WithBuildID(buildID.String()),
		zap.String("reason", buildReason.Message),
	)

	now := time.Now()

	recorded, err := tm.sqlcDB.FailTemplateBuildAndDeactivate(ctx, queries.FailTemplateBuildAndDeactivateParams{
		Status:     buildStatus(statusGroup),
		FinishedAt: &now,
		Reason:     buildReason,
		BuildID:    buildID,
	})

	tm.buildCache.Invalidate(ctx, buildID)

	return recorded, err
}

func buildReasonFrom(reason *templatemanagergrpc.TemplateBuildStatusReason) types.BuildReason {
	if reason == nil {
		return types.BuildReason{}
	}

	buildReason := types.BuildReason{
		Message: reason.GetMessage(),
	}
	if step := reason.GetStep(); step != "" {
		buildReason.Step = &step
	}

	return buildReason
}

func (tm *TemplateManager) SetFinished(ctx context.Context, buildID uuid.UUID, rootfsSize int64, envdVersion, kernelVersion, firecrackerVersion string) error {
	// first do database update to prevent race condition while calling status
	// TODO(ENG-3469): Switch to types.BuildStatusReady once all consumers are migrated.
	err := tm.sqlcDB.FinishTemplateBuild(ctx, queries.FinishTemplateBuildParams{
		TotalDiskSizeMb:    &rootfsSize,
		Status:             types.BuildStatusUploaded,
		EnvdVersion:        &envdVersion,
		KernelVersion:      kernelVersion,
		FirecrackerVersion: firecrackerVersion,
		BuildID:            buildID,
	})

	tm.buildCache.Invalidate(ctx, buildID)

	return err
}

// buildStatus maps a status group to a default build status for the database.
func buildStatus(g types.BuildStatusGroup) types.BuildStatus {
	switch g {
	case types.BuildStatusGroupPending:
		return types.BuildStatusPending
	case types.BuildStatusGroupInProgress:
		return types.BuildStatusBuilding
	case types.BuildStatusGroupReady:
		return types.BuildStatusUploaded
	case types.BuildStatusGroupFailed:
		return types.BuildStatusFailed
	default:
		return types.BuildStatusFailed
	}
}
