package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
	teamtypes "github.com/e2b-dev/infra/packages/auth/pkg/types"
	"github.com/e2b-dev/infra/packages/shared/pkg/apierrors"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/middleware/otel/joined"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

// errCodeSizedStartFailed is what the requests that joined a start see when
// that start named a memory size and the size was refused: the failure is the
// size's alone, so each of them starts the sandbox again on its own terms
// instead of taking it.
const errCodeSizedStartFailed = "sandbox_sized_start_failed"

// joinExtensionFailedCounter counts connects that joined a start, got its
// sandbox running, and could not extend its timeout for a reason other than
// the sandbox having stopped. The connect still answers with the sandbox; only
// the extension failed.
var joinExtensionFailedCounter, _ = memoryMeter.Int64Counter("api.sandbox.connect_join_extension_failed",
	metric.WithDescription("Number of connects that joined a running start but could not extend the sandbox's timeout, by error code"))

// isSizeRefusal reports whether a start's error is a refusal of the memory size
// the request named, by the API's check under the start. No sandbox was created.
func isSizeRefusal(apiErr *api.APIError) bool {
	switch apiErr.ErrorCode {
	case errCodeSandboxMemoryDisabled,
		errCodeSandboxMemoryNotResizable,
		errCodeSandboxMemoryOutOfRange,
		errCodeSandboxMemoryShrinkUnsupported,
		errCodeSandboxMemoryTeamLimit,
		errCodeSandboxMemoryEnvdOutdated:
		return true
	}

	return false
}

// StartErrorKeepsRetryHint reports whether a start's error carries its own retry
// hint that a handler must pass through: the refusals of a requested memory size,
// which no retry changes, and a joined sized start that failed, which a retry may
// pass.
func StartErrorKeepsRetryHint(apiErr *api.APIError) bool {
	if apiErr == nil {
		return false
	}

	return isSizeRefusal(apiErr) || apiErr.ErrorCode == errCodeSizedStartFailed
}

// publishedStartError is what a failed start hands the requests that joined
// it. When the start named a size and the size was refused, it hands them
// errCodeSizedStartFailed, since the refusal is the size's alone. Every other
// failure reaches them unchanged: one that came after the sandbox was created
// keeps its own retry hint, so no joiner starts the sandbox again while it is
// being removed.
func publishedStartError(apiErr *api.APIError, sized bool) error {
	if !sized || !isSizeRefusal(apiErr) {
		return apiErr
	}

	return &api.APIError{
		Code:      http.StatusServiceUnavailable,
		ErrorCode: errCodeSizedStartFailed,
		ClientMsg: "The sandbox's start with a requested memory size failed; retry",
		Err:       fmt.Errorf("a start of the sandbox with a requested memory size failed: %w", apiErr.Err),
		Retryable: apierrors.CanRetry,
	}
}

// reserveStart takes the sandbox's start, or the wait for the start already in
// flight (or for the sandbox already running) when another request holds it.
func (o *Orchestrator) reserveStart(ctx context.Context, team *teamtypes.Team, sandboxID string, limit int) (func(sandbox.Sandbox, error), func(context.Context) (sandbox.Sandbox, error), *api.APIError) {
	finishStart, waitForStart, err := o.sandboxStore.Reserve(ctx, team.Team.ID, sandboxID, limit)
	if err == nil {
		return finishStart, waitForStart, nil
	}

	var limitErr *sandbox.LimitExceededError
	switch {
	case errors.As(err, &limitErr):
		return nil, nil, &api.APIError{
			Code: http.StatusTooManyRequests,
			ClientMsg: fmt.Sprintf(
				"you have reached the maximum number of concurrent E2B sandboxes (%d). If you need more, "+
					"please visit 'https://e2b.dev/docs/billing'", limit),
			Err:       fmt.Errorf("team '%s' has reached the maximum number of instances (%d)", team.ID, limit),
			Retryable: apierrors.CanRetry,
		}
	case errors.Is(err, sandbox.ErrSandboxKilled):
		return nil, nil, &api.APIError{
			Code:      http.StatusNotFound,
			ClientMsg: fmt.Sprintf("Sandbox '%s' was deleted", sandboxID),
			Err:       fmt.Errorf("resume of '%s' refused: %w", sandboxID, err),
		}
	default:
		logger.L().Error(ctx, "failed to reserve sandbox for team", logger.WithSandboxID(sandboxID), zap.Error(err))

		return nil, nil, &api.APIError{
			Code:      http.StatusInternalServerError,
			ClientMsg: fmt.Sprintf("Failed to create sandbox: %s", err),
			Err:       err,
			Retryable: apierrors.CanRetry,
		}
	}
}

// joinStart rides the start another request holds, or takes the sandbox it
// finds running. retry reports that the start joined named a memory size that
// was refused, and that the caller may start the sandbox itself once
// (mayRetry).
func (o *Orchestrator) joinStart(
	ctx context.Context,
	team *teamtypes.Team,
	sandboxID string,
	timeout time.Duration,
	demandFilesystemBoot bool,
	opts createOptions,
	waitForStart func(context.Context) (sandbox.Sandbox, error),
	mayRetry bool,
) (sbx sandbox.Sandbox, apiErr *api.APIError, retry bool) {
	ignoresMemory := opts.connect && !demandFilesystemBoot && opts.cpuCount == nil
	if opts.memoryMB != nil && !ignoresMemory {
		// Every branch below refuses this request.
		o.recordMemoryOverride(ctx, memoryOverrideRejected, ErrCodeStartInFlight, memorySourceNamed)
	}

	// A joined request rides whatever start is already in flight — which may
	// be a memory restore (e.g. a traffic-triggered auto-resume) that an
	// explicit memory:false must never be silently answered with.
	if demandFilesystemBoot {
		return sandbox.Sandbox{}, &api.APIError{
			Code:      http.StatusConflict,
			ErrorCode: ErrCodeStartInFlight,
			ClientMsg: "Sandbox is already starting; memory: false cannot be applied to a start already in flight — retry once it is running or paused",
			Err:       fmt.Errorf("filesystem-boot resume of '%s' cannot join an in-flight start", sandboxID),
		}, false
	}

	if opts.cpuCount != nil {
		return sandbox.Sandbox{}, &api.APIError{
			Code:      http.StatusConflict,
			ErrorCode: ErrCodeStartInFlight,
			ClientMsg: "Sandbox is already starting; cpuCount cannot be applied to a start already in flight",
			Err:       fmt.Errorf("CPU override for sandbox %q cannot join an in-flight start", sandboxID),
			Retryable: apierrors.NoRetry,
		}, false
	}

	if opts.memoryMB != nil && !ignoresMemory {
		return sandbox.Sandbox{}, &api.APIError{
			Code:      http.StatusConflict,
			ErrorCode: ErrCodeStartInFlight,
			ClientMsg: "Sandbox is already starting; memoryMB cannot be applied to a start already in flight",
			Err:       fmt.Errorf("memory override for sandbox %q cannot join an in-flight start", sandboxID),
			Retryable: apierrors.NoRetry,
		}, false
	}

	logger.L().Info(ctx, "sandbox is already being started, waiting for it to be ready", logger.WithSandboxID(sandboxID))

	sbx, err := waitForStart(ctx)
	if err != nil {
		logger.L().Warn(ctx, "Error waiting for sandbox to start", zap.Error(err), logger.WithSandboxID(sandboxID))

		apiErr, ok := errors.AsType[*api.APIError](err)
		if !ok {
			apiErr = &api.APIError{
				Code:      http.StatusInternalServerError,
				ClientMsg: "Error waiting for sandbox to start",
				Err:       err,
				Retryable: apierrors.NoRetry,
			}
		}
		if apiErr.ErrorCode == errCodeSizedStartFailed {
			if mayRetry {
				// Not marked as joined: this request goes on to start the
				// sandbox itself.
				logger.L().Info(ctx, "the start joined named a memory size that was refused; starting the sandbox again", logger.WithSandboxID(sandboxID))

				return sandbox.Sandbox{}, nil, true
			}
			apiErr.Retryable = apierrors.CanRetry
		}
		joined.Mark(ctx)
		if opts.memoryMB != nil {
			o.recordMemoryOverride(ctx, memoryOverrideFailed, apiErr.ErrorCode, memorySourceNamed)
		}

		return sandbox.Sandbox{}, apiErr, false
	}

	joined.Mark(ctx)

	if opts.connect {
		// A connect gets the sandbox for its own timeout, as it would if it had
		// found the sandbox running; the timeout only ever moves out. The
		// extension is the optional part: the sandbox the connect joined is
		// running, so a failed extension is logged and counted, not answered.
		extended, keepErr := o.KeepAliveFor(ctx, team.ID, sandboxID, timeout, false)
		switch {
		case keepErr == nil:
			sbx = *extended
		case sandboxLeftRunning(keepErr):
			logger.L().Info(ctx, "joined sandbox stopped running before its timeout could be extended", logger.WithSandboxID(sandboxID), zap.Error(keepErr.Err))
		default:
			logger.L().Warn(ctx, "could not extend the timeout of the sandbox a connect joined; answering with the sandbox", logger.WithSandboxID(sandboxID), zap.Error(keepErr.Err))
			o.recordJoinExtensionFailed(ctx, keepErr.ErrorCode)
		}
	}

	if opts.memoryMB != nil {
		o.recordMemoryOverride(ctx, memoryOverrideIgnored, "", memorySourceNamed)
	}

	return sbx, nil, false
}

// sandboxLeftRunning reports a keep-alive refused because the sandbox is no
// longer running or no longer in the store.
func sandboxLeftRunning(apiErr *api.APIError) bool {
	var notRunning *sandbox.NotRunningError

	return errors.Is(apiErr.Err, sandbox.ErrNotFound) || errors.As(apiErr.Err, &notRunning)
}

// recordJoinExtensionFailed counts a joined connect whose timeout extension
// failed although the sandbox it joined runs.
func (o *Orchestrator) recordJoinExtensionFailed(ctx context.Context, errorCode string) {
	if errorCode == "" {
		errorCode = "keep_alive_failed"
	}
	attrs := []attribute.KeyValue{attribute.String("error_code", errorCode)}
	telemetry.ReportEvent(ctx, "joined connect timeout extension failed", attrs...)
	counter := joinExtensionFailedCounter
	if o != nil && o.joinExtensionFailedCounter != nil {
		counter = o.joinExtensionFailedCounter
	}
	if counter != nil {
		counter.Add(ctx, 1, metric.WithAttributes(attrs...))
	}
}
