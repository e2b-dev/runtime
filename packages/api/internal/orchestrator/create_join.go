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
// that start named a memoryMB or a cpuCount and failed before any sandbox was
// created, on a cause the joiners need not share (see joinersMayRestart): each
// of them starts the sandbox again on its own terms instead of taking it.
const errCodeSizedStartFailed = "sandbox_sized_start_failed"

// joinerRestartCounter counts the requests that joined a start that named a
// cpuCount or memoryMB and failed before creating the sandbox
// (errCodeSizedStartFailed), by route and outcome: restarted when the request
// went on to start the sandbox itself, gave_up when it had already done so
// once and answered the 503.
var joinerRestartCounter, _ = memoryMeter.Int64Counter("api.sandbox.start_joiner_restart",
	metric.WithDescription("Number of requests that joined a start with a requested cpuCount or memoryMB that failed before creating the sandbox, by route and outcome (restarted, gave_up)"))

// The outcomes api.sandbox.start_joiner_restart counts.
const (
	joinerRestarted = "restarted"
	joinerGaveUp    = "gave_up"
)

// joinExtensionFailedCounter counts connects that joined a start, got its
// sandbox running, and could not extend its timeout for a reason other than
// the sandbox having stopped. The connect still answers with the sandbox; only
// the extension failed.
var joinExtensionFailedCounter, _ = memoryMeter.Int64Counter("api.sandbox.connect_join_extension_failed",
	metric.WithDescription("Number of connects that joined a running start but could not extend the sandbox's timeout, by error code"))

// isMemoryRefusal reports a refusal of a memory size, by the API's check under
// the start or by the node asked to create the sandbox. No sandbox is created.
func isMemoryRefusal(code string) bool {
	switch code {
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

// isCPUCountRefusal reports a refusal of a cpuCount by the API's check under
// the start. No sandbox is created.
func isCPUCountRefusal(code string) bool {
	switch code {
	case errCodeSandboxCpuCountDisabled,
		errCodeSandboxCpuCountNotResizable,
		errCodeSandboxCpuCountOutOfRange,
		errCodeSandboxCpuCountTeamLimit:
		return true
	}

	return false
}

// joinersMayRestart reports whether the requests that joined a failed start may
// start the sandbox again themselves. Only a start that named a memoryMB or a
// cpuCount qualifies, and only for a failure before any sandbox was created
// whose cause may be that request's own: the refusal of a field it named, or a
// placement that found no node for it (the three placement codes are answered
// only when no create ran). Any other failure, one after the sandbox was
// created included, reaches the joiners unchanged, so none starts a sandbox
// that may still be being removed.
func joinersMayRestart(apiErr *api.APIError, opts createOptions) bool {
	switch {
	case opts.memoryMB != nil && isMemoryRefusal(apiErr.ErrorCode),
		opts.cpuCount != nil && isCPUCountRefusal(apiErr.ErrorCode):
		return true
	case opts.memoryMB == nil && opts.cpuCount == nil:
		return false
	}

	switch apiErr.ErrorCode {
	case errCodeCapacityUnavailable, errCodeNoCompatibleNode, errCodeFeatureUnsupported:
		return true
	}

	return false
}

// StartErrorKeepsRetryHint reports whether a start's error carries its own retry
// hint that a handler must pass through: the refusals of a requested memoryMB
// or cpuCount, which no retry changes, and a joined start that named one and
// failed, which a retry may pass.
func StartErrorKeepsRetryHint(apiErr *api.APIError) bool {
	if apiErr == nil {
		return false
	}

	return isMemoryRefusal(apiErr.ErrorCode) || isCPUCountRefusal(apiErr.ErrorCode) || apiErr.ErrorCode == errCodeSizedStartFailed
}

// publishedStartError is what a failed start hands the requests that joined
// it: errCodeSizedStartFailed when they may start the sandbox again
// (joinersMayRestart), and the start's own error otherwise. The error reaches
// them through the start result in the store, which keeps its code and message
// but not its retry hint.
func publishedStartError(apiErr *api.APIError, opts createOptions) error {
	if !joinersMayRestart(apiErr, opts) {
		return apiErr
	}

	return &api.APIError{
		Code:      http.StatusServiceUnavailable,
		ErrorCode: errCodeSizedStartFailed,
		ClientMsg: "The sandbox's start with a requested cpuCount or memoryMB failed; retry",
		Err:       fmt.Errorf("a start of the sandbox with a requested cpuCount or memoryMB failed: %w", apiErr.Err),
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
			Err:       fmt.Errorf("team '%s' has reached the maximum number of sandboxes (%d)", team.ID, limit),
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
// finds running. retry reports that the start joined failed with
// errCodeSizedStartFailed, and that the caller may start the sandbox itself
// once (mayRetry).
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
	// A connect rides the start in flight with its size and count ignored,
	// unless it demands a cold boot, which is refused below.
	ignoresSize := opts.connect && !demandFilesystemBoot
	if opts.cpuCount != nil && !ignoresSize {
		// Every branch below refuses this request.
		o.recordCPUCountOverride(ctx, cpuCountOverrideRejected, ErrCodeStartInFlight)
	}
	if opts.memoryMB != nil && !ignoresSize {
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

	if opts.cpuCount != nil && !ignoresSize {
		return sandbox.Sandbox{}, &api.APIError{
			Code:      http.StatusConflict,
			ErrorCode: ErrCodeStartInFlight,
			ClientMsg: "Sandbox is already starting; cpuCount cannot be applied to a start already in flight",
			Err:       fmt.Errorf("CPU override for sandbox %q cannot join an in-flight start", sandboxID),
			Retryable: apierrors.NoRetry,
		}, false
	}

	if opts.memoryMB != nil && !ignoresSize {
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
				logger.L().Info(ctx, "the start joined named a cpuCount or memoryMB and failed before creating the sandbox; starting the sandbox again", logger.WithSandboxID(sandboxID))
				o.recordJoinerRestart(ctx, joinerRestarted)

				return sandbox.Sandbox{}, nil, true
			}
			o.recordJoinerRestart(ctx, joinerGaveUp)
			apiErr.Retryable = apierrors.CanRetry
		}
		joined.Mark(ctx)
		if opts.memoryMB != nil {
			o.recordMemoryOverride(ctx, memoryOverrideFailed, apiErr.ErrorCode, memorySourceNamed)
		}
		if opts.cpuCount != nil {
			o.recordCPUCountOverride(ctx, cpuCountOverrideFailed, apiErr.ErrorCode)
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
	if opts.cpuCount != nil {
		o.RecordConnectCPUCountIgnored(ctx)
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

// recordJoinerRestart counts a request that joined a start which failed with
// errCodeSizedStartFailed, under the route ctx carries, on the orchestrator's
// counter, the package counter unless one was set; o may be nil.
func (o *Orchestrator) recordJoinerRestart(ctx context.Context, outcome string) {
	attrs := []attribute.KeyValue{attribute.String("outcome", outcome), attribute.String("route", memoryRoute(ctx))}
	telemetry.ReportEvent(ctx, "joined start failed on its size", attrs...)
	counter := joinerRestartCounter
	if o != nil && o.joinerRestartCounter != nil {
		counter = o.joinerRestartCounter
	}
	if counter != nil {
		counter.Add(ctx, 1, metric.WithAttributes(attrs...))
	}
}
