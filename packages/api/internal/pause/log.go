package pause

import (
	"context"

	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

type Reason string

const (
	ReasonRequest Reason = "request"
	ReasonTimeout Reason = "timeout"
)

type SkipReason string

const (
	SkipReasonAlreadyPaused SkipReason = "already_paused"
	SkipReasonNotEvictable  SkipReason = "not_evictable"
	SkipReasonNotFound      SkipReason = "not_found"
	// SkipReasonStateChanged: the sandbox moved, between the expiry scan and
	// the removal, into a state pause cannot start from.
	SkipReasonStateChanged     SkipReason = "state_changed"
	SkipReasonAdmissionRefused SkipReason = "admission_refused"
	// SkipReasonDraining: this replica is shutting down and stopped admitting
	// pauses, so the sandbox is left running for a retry elsewhere.
	SkipReasonDraining SkipReason = "draining"
)

// fsOnly rides on every pause event so the snapshot kind can be joined to the
// pause reason — the split that sizes the blast radius of fs-only behavior
// changes (an explicit memory:false request refuses hard, a timeout
// auto-pause degrades silently). Initiated logs the REQUESTED kind; the
// result events log the EFFECTIVE kind, which differs when a timeout
// auto-pause was degraded to a memory snapshot.
func LogInitiated(ctx context.Context, sandboxID string, teamID string, reason Reason, fsOnly bool) {
	logger.L().Info(ctx, "sandbox_pause_initiated",
		logger.WithSandboxID(sandboxID),
		logger.WithTeamID(teamID),
		zap.String("pause_reason", string(reason)),
		zap.Bool("fs_only", fsOnly),
	)
}

func LogSuccess(ctx context.Context, sandboxID string, teamID string, reason Reason, fsOnly bool) {
	logger.L().Info(ctx, "sandbox_pause_result",
		logger.WithSandboxID(sandboxID),
		logger.WithTeamID(teamID),
		zap.String("pause_reason", string(reason)),
		zap.String("pause_result", "success"),
		zap.Bool("fs_only", fsOnly),
	)
}

func LogFailure(ctx context.Context, sandboxID string, teamID string, reason Reason, fsOnly bool, err error) {
	fields := []zap.Field{
		logger.WithSandboxID(sandboxID),
		logger.WithTeamID(teamID),
		zap.String("pause_reason", string(reason)),
		zap.String("pause_result", "failure"),
		zap.Bool("fs_only", fsOnly),
	}
	if err != nil {
		fields = append(fields, zap.Error(err))
	}

	logger.L().Warn(ctx, "sandbox_pause_result", fields...)
}

// LogRefused is not a result: the node refused a memory snapshot and the same
// sweep goes on to request a filesystem-only one, whose result row follows.
func LogRefused(ctx context.Context, sandboxID string, teamID string, reason Reason, fsOnly bool) {
	logger.L().Info(ctx, "sandbox_pause_refused",
		logger.WithSandboxID(sandboxID),
		logger.WithTeamID(teamID),
		zap.String("pause_reason", string(reason)),
		zap.Bool("fs_only", fsOnly),
	)
}

func LogSkipped(ctx context.Context, sandboxID string, teamID string, reason Reason, skipReason SkipReason, fsOnly bool) {
	logger.L().Info(ctx, "sandbox_pause_result",
		logger.WithSandboxID(sandboxID),
		logger.WithTeamID(teamID),
		zap.String("pause_reason", string(reason)),
		zap.String("pause_result", "skipped"),
		zap.String("pause_skip_reason", string(skipReason)),
		zap.Bool("fs_only", fsOnly),
	)
}
