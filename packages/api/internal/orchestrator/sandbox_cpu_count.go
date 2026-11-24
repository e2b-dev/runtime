package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/db/queries"
	teamtypes "github.com/e2b-dev/infra/packages/auth/pkg/types"
	"github.com/e2b-dev/infra/packages/shared/pkg/apierrors"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

const (
	errCodeSandboxCpuCountDisabled     = "sandbox_cpu_count_disabled"
	errCodeSandboxCpuCountNotResizable = "sandbox_cpu_count_not_resizable"
	errCodeSandboxCpuCountOutOfRange   = "sandbox_cpu_count_out_of_range"
	errCodeSandboxCpuCountTeamLimit    = "sandbox_cpu_count_team_limit"
)

// cpuCountOverrideCounter counts each create, resume and connect request that
// names a cpuCount, by route and outcome, as api.sandbox.memory_override does
// for memoryMB: accepted once the start resolved the count, rejected (with the
// error code), failed when a joined start failed, and ignored when a connect
// did not resume the sandbox itself (it was running, or it joined another
// request's start), so the count did not apply. ignored is not a refusal: a
// rate of refusals leaves it out. A count inherited above the team's limit and
// lowered to it is clamped_to_team_limit.
var cpuCountOverrideCounter, _ = otel.Meter("github.com/e2b-dev/infra/packages/api/internal/orchestrator").
	Int64Counter("api.sandbox.cpu_count_override", metric.WithDescription("Number of sandbox create, resume and connect requests with an explicit cpuCount, and inherited counts lowered to the team limit, by route, outcome (ignored is not a refusal) and error code"))

// The outcomes api.sandbox.cpu_count_override counts.
const (
	cpuCountOverrideAccepted = "accepted"
	cpuCountOverrideRejected = "rejected"
	cpuCountOverrideFailed   = "failed"
	// cpuCountOverrideClamped: an inherited count above the team's limit was
	// lowered to it.
	cpuCountOverrideClamped = "clamped_to_team_limit"
	// cpuCountOverrideIgnored: a connect named a cpuCount but did not resume
	// the sandbox itself, so the count did not apply. Not a refusal.
	cpuCountOverrideIgnored = "ignored"
)

// resolveSandboxVcpu picks the sandbox's online vCPU target: the build's
// cpuCount unless the request names one, which must fit the build's
// capacity (1..maxCpuCount; a fixed-size build accepts only its cpuCount).
func (o *Orchestrator) resolveSandboxVcpu(
	ctx context.Context,
	flags *featureflags.Client,
	team *teamtypes.Team,
	sandboxID, templateID string,
	build queries.EnvBuild,
	requested *int32,
) (int64, *api.APIError) {
	if requested == nil && build.MaxVcpu == nil {
		return build.Vcpu, nil
	}

	vcpu, clamped, apiErr := pickSandboxVcpu(ctx, flags, team, build, requested)

	outcome, errorCode := cpuCountOverrideAccepted, ""
	switch {
	case apiErr != nil:
		outcome, errorCode = cpuCountOverrideRejected, apiErr.ErrorCode
	case clamped:
		outcome = cpuCountOverrideClamped
	}
	if requested != nil || clamped {
		o.recordCPUCountOverride(ctx, outcome, errorCode)
	}

	switch {
	case apiErr != nil:
		return 0, apiErr
	case clamped:
		logger.L().Info(ctx, "sandbox cpuCount lowered to the team limit",
			logger.WithTeamID(team.ID.String()),
			logger.WithSandboxID(sandboxID),
			logger.WithTemplateID(templateID),
			logger.WithBuildID(build.ID.String()),
			zap.Int64("cpu_count", build.Vcpu),
			zap.Int64("team_max_vcpu", team.Limits.MaxVcpu),
		)
	case requested != nil:
		logger.L().Info(ctx, "sandbox cpuCount set by request",
			logger.WithTeamID(team.ID.String()),
			logger.WithSandboxID(sandboxID),
			logger.WithTemplateID(templateID),
			logger.WithBuildID(build.ID.String()),
			zap.Int64("cpu_count", vcpu),
			zap.Int64p("max_cpu_count", build.MaxVcpu),
		)
	}

	return vcpu, nil
}

// CheckSandboxCPUCount refuses a requested cpuCount that the build or the
// team cannot honour. The resume and connect handlers run it before the start
// is reserved, so the refusal reaches only the request that named the count.
// A start reserved first publishes its result to every request joined to it,
// including a connect or auto-resume that sent no count. CreateSandbox still
// decides the count from the build it fetches under the reservation; this
// counts only its refusals, and that path counts the rest.
func (o *Orchestrator) CheckSandboxCPUCount(
	ctx context.Context,
	flags *featureflags.Client,
	team *teamtypes.Team,
	build queries.EnvBuild,
	requested *int32,
) *api.APIError {
	if requested == nil {
		return nil
	}

	if _, _, apiErr := pickSandboxVcpu(ctx, flags, team, build, requested); apiErr != nil {
		o.recordCPUCountOverride(ctx, cpuCountOverrideRejected, apiErr.ErrorCode)
		// The handler answers this refusal directly; it reaches no other error
		// report on the span.
		trace.SpanFromContext(ctx).RecordError(apiErr.Err, trace.WithAttributes(
			attribute.String("error_code", apiErr.ErrorCode),
			attribute.Int64("cpu_count", int64(*requested)),
			attribute.Int64("build_cpu_count", build.Vcpu),
		))

		return apiErr
	}

	return nil
}

// RecordConnectCPUCountIgnored counts a connect that named cpuCount but did not
// resume the sandbox itself: the sandbox was running, or the connect joined
// another request's start.
func (o *Orchestrator) RecordConnectCPUCountIgnored(ctx context.Context) {
	o.recordCPUCountOverride(ctx, cpuCountOverrideIgnored, "")
}

// recordCPUCountOverride counts a cpuCount decision under the route ctx
// carries (WithMemoryRoute), on the orchestrator's counter, the package counter
// unless one was set; o may be nil.
func (o *Orchestrator) recordCPUCountOverride(ctx context.Context, outcome, errorCode string) {
	attrs := []attribute.KeyValue{attribute.String("outcome", outcome), attribute.String("route", memoryRoute(ctx))}
	if errorCode != "" {
		attrs = append(attrs, attribute.String("error_code", errorCode))
	}
	counter := cpuCountOverrideCounter
	if o != nil && o.cpuCountOverrideCounter != nil {
		counter = o.cpuCountOverrideCounter
	}
	if counter != nil {
		counter.Add(ctx, 1, metric.WithAttributes(attrs...))
	}
}

// pickSandboxVcpu is the decision behind resolveSandboxVcpu and
// CheckSandboxCPUCount, free of logs and counters so both can share it.
// clamped reports an inherited count lowered to the team's limit.
func pickSandboxVcpu(
	ctx context.Context,
	flags *featureflags.Client,
	team *teamtypes.Team,
	build queries.EnvBuild,
	requested *int32,
) (vcpu int64, clamped bool, apiErr *api.APIError) {
	if requested != nil {
		if apiErr := cpuCountDisabled(ctx, flags, team); apiErr != nil {
			return 0, false, apiErr
		}
	}

	want := build.Vcpu
	if requested != nil {
		want = int64(*requested)
	}
	switch {
	case build.MaxVcpu == nil && want != build.Vcpu:
		return 0, false, &api.APIError{
			Code:      http.StatusBadRequest,
			ErrorCode: errCodeSandboxCpuCountNotResizable,
			ClientMsg: fmt.Sprintf("cpuCount %d is not available: the template is not resizable and runs its cpuCount %d", want, build.Vcpu),
			Err:       fmt.Errorf("cpuCount %d requested on a fixed-size build with %d vcpus", want, build.Vcpu),
			Retryable: apierrors.NoRetry,
		}
	case build.MaxVcpu != nil && (want < 1 || want > *build.MaxVcpu):
		return 0, false, &api.APIError{
			Code:      http.StatusBadRequest,
			ErrorCode: errCodeSandboxCpuCountOutOfRange,
			ClientMsg: fmt.Sprintf("cpuCount %d must be between 1 and the template's maxCpuCount %d", want, *build.MaxVcpu),
			Err:       fmt.Errorf("cpuCount %d requested on a build with max %d vcpus", want, *build.MaxVcpu),
			Retryable: apierrors.NoRetry,
		}
	}

	if want > team.Limits.MaxVcpu && requested == nil && team.Limits.MaxVcpu >= 1 {
		// No count was asked for (connect, auto-resume, a plain resume or
		// create): the caller cannot pick a smaller one, so start at the
		// team's current limit rather than refuse. The build's capacity is
		// unchanged, so the count can be raised again within it.
		return team.Limits.MaxVcpu, true, nil
	}

	if want > team.Limits.MaxVcpu {
		return 0, false, &api.APIError{
			Code:      http.StatusBadRequest,
			ErrorCode: errCodeSandboxCpuCountTeamLimit,
			ClientMsg: fmt.Sprintf("cpuCount must be at most your team limit (%d)", team.Limits.MaxVcpu),
			Err:       fmt.Errorf("cpuCount %d exceeds team limit %d", want, team.Limits.MaxVcpu),
			Retryable: apierrors.NoRetry,
		}
	}

	return want, false, nil
}

// cpuCountDisabled refuses an explicit cpuCount when resizable templates are
// off for the team. The resume and connect handlers (CheckSandboxCPUCount) and,
// for create and resume, CreateSandbox run it before a start is reserved or
// joined, so a disabled feature answers 400 rather than a retryable-looking
// 409. A connect that holds the start meets it again in pickSandboxVcpu.
func cpuCountDisabled(ctx context.Context, flags *featureflags.Client, team *teamtypes.Team) *api.APIError {
	if flags.BoolFlag(ctx, featureflags.ResizableTemplatesFlag, featureflags.TeamContext(team.ID.String())) {
		return nil
	}

	return &api.APIError{
		Code:      http.StatusBadRequest,
		ErrorCode: errCodeSandboxCpuCountDisabled,
		ClientMsg: "Resizable templates are not enabled for your team",
		Err:       errors.New("sandbox cpuCount requested while resizable templates are off for the team"),
		Retryable: apierrors.NoRetry,
	}
}
