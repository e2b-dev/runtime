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
	"github.com/e2b-dev/infra/packages/api/internal/constants"
	"github.com/e2b-dev/infra/packages/api/internal/db/queries"
	teamtypes "github.com/e2b-dev/infra/packages/auth/pkg/types"
	"github.com/e2b-dev/infra/packages/shared/pkg/apierrors"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	proxygrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc/proxy"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
	sharedUtils "github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

const (
	errCodeSandboxMemoryDisabled          = "sandbox_memory_disabled"
	errCodeSandboxMemoryNotResizable      = "sandbox_memory_not_resizable"
	errCodeSandboxMemoryOutOfRange        = "sandbox_memory_out_of_range"
	errCodeSandboxMemoryShrinkUnsupported = "sandbox_memory_shrink_unsupported"
	errCodeSandboxMemoryTeamLimit         = "sandbox_memory_team_limit"
	errCodeSandboxMemoryEnvdOutdated      = "sandbox_memory_envd_outdated"
	// errCodeSandboxMemoryRecordMismatch is a node refusing a size the API
	// sent from its own record, not from the request: the record disagrees
	// with the node's snapshot.
	errCodeSandboxMemoryRecordMismatch = proxygrpc.SandboxMemoryRecordMismatch
)

var memoryMeter = otel.Meter("github.com/e2b-dev/infra/packages/api/internal/orchestrator")

// memoryOverrideCounter counts each create, resume and connect request that
// names a memoryMB once, by route and outcome: accepted once a node took a
// start at that size, rejected by the API or the node (with the error code),
// failed for any other error, and ignored when a connect did not resume the
// sandbox itself (it was running, or it joined another request's start), so
// the size did not apply. ignored is not a refusal: a rate of refusals leaves
// it out. It also counts the refusal of a size a new sandbox inherits from its
// build, on create and fork (source=inherited); a request's own size is
// source=named.
var memoryOverrideCounter, _ = memoryMeter.Int64Counter("api.sandbox.memory_override",
	metric.WithDescription("Number of sandbox create, resume and connect requests with an explicit memoryMB, and refusals of a size inherited on create or fork, by route, outcome (ignored is not a refusal), error code and source"))

// memorySizeUnknownCounter counts starts and pauses of a sandbox whose build
// has a memory maximum but whose size the API does not know, by path. It
// stays at zero unless a writer without the size field (an older API or
// scheduling-api) touched the sandbox's record.
var memorySizeUnknownCounter, _ = memoryMeter.Int64Counter("api.sandbox.memory_size_unknown",
	metric.WithDescription("Number of starts and pauses of a sandbox whose memory size the API does not know, by path"))

// resolveSandboxMemory picks the memory a sandbox runs with: the request's
// memoryMB, else the size the build records for a start (BuildStartMemoryMB).
// known reports whether the API knows that size; when it does and the build has a
// memory maximum, the size is sent to the orchestrator, which then enforces the
// number the API accounts and bills. create is a new sandbox, not a resume of
// the caller's own one (see pickSandboxMemory).
func (o *Orchestrator) resolveSandboxMemory(
	ctx context.Context,
	flags *featureflags.Client,
	team *teamtypes.Team,
	sandboxID, templateID string,
	build queries.EnvBuild,
	requested *int32,
	create bool,
) (memoryMB int64, known bool, apiErr *api.APIError) {
	memoryMB, known, apiErr = pickSandboxMemory(ctx, flags, team, build, requested, create)
	setMemorySpanAttributes(ctx, build, requested)
	if apiErr == nil && known && build.MaxRamMb != nil {
		telemetry.SetAttributes(ctx, attribute.Int64("memory.resolved_mb", memoryMB))
	}
	if requested == nil {
		if apiErr != nil {
			inherited, _ := BuildStartMemoryMB(build.RamMb, build.MaxRamMb, build.StartMemoryMb)
			logger.L().Warn(ctx, "sandbox memory inherited from its build refused",
				logger.WithTeamID(team.ID.String()),
				logger.WithSandboxID(sandboxID),
				logger.WithTemplateID(templateID),
				logger.WithBuildID(build.ID.String()),
				zap.String("error_code", apiErr.ErrorCode),
				zap.Int64("inherited_memory_mb", inherited),
				zap.Int64("boot_memory_mb", build.RamMb),
				zap.Int64("team_max_memory_mb", team.Limits.MaxRamMb),
				zap.Stringp("envd_version", build.EnvdVersion),
			)
			o.recordMemoryOverride(ctx, memoryOverrideRejected, apiErr.ErrorCode, memorySourceInherited)
		}

		return memoryMB, known, apiErr
	}

	if apiErr != nil {
		o.recordMemoryOverride(ctx, memoryOverrideRejected, apiErr.ErrorCode, memorySourceNamed)

		return 0, false, apiErr
	}

	logger.L().Info(ctx, "sandbox memoryMB set by request",
		logger.WithTeamID(team.ID.String()),
		logger.WithSandboxID(sandboxID),
		logger.WithTemplateID(templateID),
		logger.WithBuildID(build.ID.String()),
		zap.Int64("memory_mb", memoryMB),
		zap.Int64p("start_memory_mb", build.StartMemoryMb),
		zap.Int64("boot_memory_mb", build.RamMb),
		zap.Int64p("max_memory_mb", build.MaxRamMb),
	)

	return memoryMB, true, nil
}

// CheckSandboxMemory refuses a requested memoryMB that the build or the team
// cannot honour. The resume and connect handlers run it before the start is
// reserved, for the reason CheckSandboxCPUCount gives. CreateSandbox decides the size again
// from the build it fetches under the reservation; this counts only its
// refusals.
func (o *Orchestrator) CheckSandboxMemory(
	ctx context.Context,
	flags *featureflags.Client,
	team *teamtypes.Team,
	build queries.EnvBuild,
	requested *int32,
) *api.APIError {
	if requested == nil {
		return nil
	}

	if _, _, apiErr := pickSandboxMemory(ctx, flags, team, build, requested, false); apiErr != nil {
		setMemorySpanAttributes(ctx, build, requested)
		o.recordMemoryOverride(ctx, memoryOverrideRejected, apiErr.ErrorCode, memorySourceNamed)
		// The handler answers this refusal directly; it reaches no other error
		// report on the span.
		trace.SpanFromContext(ctx).RecordError(apiErr.Err, trace.WithAttributes(attribute.String("error_code", apiErr.ErrorCode)))

		return apiErr
	}

	return nil
}

// BuildStartMemoryMB is the memory a start from a build runs with when it names
// no size, and whether the API knows it. A build without a memory maximum runs
// at boot RAM. One with a maximum records the size in start_memory_mb: boot RAM
// for a template build, the size the sandbox ran with for a snapshot build. A
// build that does not record it was made without the size; the orchestrator
// then keeps the size its own snapshot recorded, which the API does not know.
func BuildStartMemoryMB(ramMB int64, maxRAMMB, startMemoryMB *int64) (int64, bool) {
	if maxRAMMB == nil {
		return ramMB, true
	}
	if startMemoryMB == nil {
		return ramMB, false
	}

	return max(ramMB, *startMemoryMB), true
}

// The source of the size a memory decision is about: one the request named, or
// one a new sandbox inherits from its build (a snapshot template or a fork).
const (
	memorySourceNamed     = "named"
	memorySourceInherited = "inherited"
)

const (
	memoryOverrideAccepted = "accepted"
	memoryOverrideRejected = "rejected"
	memoryOverrideFailed   = "failed"
	// memoryOverrideIgnored: a connect named a size but did not resume the
	// sandbox itself (it was running, or it joined another request's start),
	// so the size did not apply, as memory does not. Not a refusal.
	memoryOverrideIgnored = "ignored"
)

// The request route a memory decision is counted under.
const (
	MemoryRouteCreate  = "create"
	MemoryRouteFork    = "fork"
	MemoryRouteResume  = "resume"
	MemoryRouteConnect = "connect"
	memoryRouteOther   = "other"
)

type memoryRouteKey struct{}

// WithMemoryRoute names the route whose memory decisions ctx carries, for the
// route label on api.sandbox.memory_override.
func WithMemoryRoute(ctx context.Context, route string) context.Context {
	return context.WithValue(ctx, memoryRouteKey{}, route)
}

func memoryRoute(ctx context.Context) string {
	if route, ok := ctx.Value(memoryRouteKey{}).(string); ok {
		return route
	}

	return memoryRouteOther
}

// RecordConnectMemoryIgnored counts a connect that named memoryMB for a sandbox
// that was already running, so the size did not apply.
func (o *Orchestrator) RecordConnectMemoryIgnored(ctx context.Context) {
	o.recordMemoryOverride(ctx, memoryOverrideIgnored, "", memorySourceNamed)
}

// recordMemoryOverride counts a memory decision on the orchestrator's counter,
// the package counter unless one was set; o may be nil.
func (o *Orchestrator) recordMemoryOverride(ctx context.Context, outcome, errorCode, source string) {
	attrs := []attribute.KeyValue{attribute.String("outcome", outcome), attribute.String("source", source), attribute.String("route", memoryRoute(ctx))}
	if errorCode != "" {
		attrs = append(attrs, attribute.String("error_code", errorCode))
	}
	telemetry.ReportEvent(ctx, "sandbox memory override", attrs...)
	counter := memoryOverrideCounter
	if o != nil && o.memoryOverrideCounter != nil {
		counter = o.memoryOverrideCounter
	}
	if counter != nil {
		counter.Add(ctx, 1, metric.WithAttributes(attrs...))
	}
}

// setMemorySpanAttributes puts the sizes a start's memory is decided from on its
// span, for a build that can grow or a request that names a size.
func setMemorySpanAttributes(ctx context.Context, build queries.EnvBuild, requested *int32) {
	if build.MaxRamMb == nil && requested == nil {
		return
	}
	attrs := []attribute.KeyValue{attribute.Int64("memory.boot_mb", build.RamMb)}
	if requested != nil {
		attrs = append(attrs, attribute.Int64("memory.requested_mb", int64(*requested)))
	}
	if build.MaxRamMb != nil {
		attrs = append(attrs, attribute.Int64("memory.max_mb", *build.MaxRamMb))
	}
	if build.StartMemoryMb != nil {
		attrs = append(attrs, attribute.Int64("memory.start_mb", *build.StartMemoryMb))
	}
	telemetry.SetAttributes(ctx, attrs...)
}

// placementMemoryOutcome is how a start that named memoryMB is counted once
// placement answered: accepted when a node took it, rejected with the memory
// code when the node refused the size, failed with the API code otherwise.
func placementMemoryOutcome(placementErr error, apiErr *api.APIError) (outcome, errorCode string) {
	if placementErr == nil {
		return memoryOverrideAccepted, ""
	}
	if code := memoryRefusalCodeOf(placementErr); code != "" {
		return memoryOverrideRejected, code
	}

	return memoryOverrideFailed, apiErr.ErrorCode
}

// placementMemoryMiB is the memory placement accounts a start at. A build
// with a memory maximum whose start size the API does not know is accounted
// at the maximum: the orchestrator keeps its snapshot's size, which is at
// most that, and over-counting a node only delays a placement while
// under-counting it lets the node take more than its huge pages hold.
func placementMemoryMiB(build queries.EnvBuild, memoryMB int64, known bool) int64 {
	if build.MaxRamMb != nil && !known {
		return *build.MaxRamMb
	}

	return memoryMB
}

// reportMemorySizeUnknown logs and counts a start or pause of a sandbox
// whose build has a memory maximum but whose size the API does not know.
func reportMemorySizeUnknown(ctx context.Context, path string, bootMB, maxMB int64, fields ...zap.Field) {
	logger.L().Warn(ctx, "sandbox memory size unknown for a build with a memory maximum; an older API or scheduling-api may have dropped it",
		append(fields, zap.String("path", path), zap.Int64("boot_memory_mb", bootMB), zap.Int64("max_memory_mb", maxMB))...)
	telemetry.ReportEvent(ctx, "sandbox memory size unknown",
		attribute.String("path", path), attribute.Int64("boot_memory_mb", bootMB), attribute.Int64("max_memory_mb", maxMB))
	if memorySizeUnknownCounter != nil {
		memorySizeUnknownCounter.Add(ctx, 1, metric.WithAttributes(attribute.String("path", path)))
	}
}

// reportMemoryRecordMismatch logs, and records on the start's span, a node refusing
// the memory size the API sent from its own record, with every size involved.
func reportMemoryRecordMismatch(ctx context.Context, err error, sandboxID string, sbxData SandboxMetadata, refusal string, sentMB int64) {
	attrs := []attribute.KeyValue{
		telemetry.WithSandboxID(sandboxID),
		telemetry.WithTemplateID(sbxData.TemplateID),
		telemetry.WithBuildID(sbxData.Build.ID.String()),
		attribute.String("refusal", refusal),
		attribute.Int64("sent_memory_mb", sentMB),
		attribute.Int64("boot_memory_mb", sbxData.Build.RamMb),
	}
	if sbxData.Build.StartMemoryMb != nil {
		attrs = append(attrs, attribute.Int64("start_memory_mb", *sbxData.Build.StartMemoryMb))
	}
	if sbxData.Build.MaxRamMb != nil {
		attrs = append(attrs, attribute.Int64("max_memory_mb", *sbxData.Build.MaxRamMb))
	}
	telemetry.ReportCriticalError(ctx, "Node refused the memory size recorded for the sandbox", err, attrs...)
}

// pickSandboxMemory is the decision behind resolveSandboxMemory and
// CheckSandboxMemory, free of logs and counters so both can share it. A
// sandbox's memory never shrinks: unplugging needs the guest to give the
// memory back, which a start cannot wait for.
func pickSandboxMemory(
	ctx context.Context,
	flags *featureflags.Client,
	team *teamtypes.Team,
	build queries.EnvBuild,
	requested *int32,
	create bool,
) (int64, bool, *api.APIError) {
	current, known := BuildStartMemoryMB(build.RamMb, build.MaxRamMb, build.StartMemoryMb)
	// A size a sandbox already has survives a lowered team limit, but a new
	// sandbox (from a snapshot template, perhaps another team's public one) does
	// not have it yet: its growth above the template's boot RAM is held to the
	// caller's limit, inherited or named.
	limitBase := current
	if create {
		limitBase = build.RamMb
	}
	if requested == nil {
		if create && known && current > limitBase && current > team.Limits.MaxRamMb {
			return 0, false, memoryError(errCodeSandboxMemoryTeamLimit,
				fmt.Sprintf("the recorded memory this sandbox starts with (%d MiB) exceeds your team limit (%d MiB)", current, team.Limits.MaxRamMb),
				fmt.Errorf("inherited start size %d MiB exceeds team limit %d MiB", current, team.Limits.MaxRamMb))
		}
		if create && known && current > limitBase {
			if apiErr := envdOutdatedForGrowth(build, current); apiErr != nil {
				return 0, false, apiErr
			}
		}

		return current, known, nil
	}

	if apiErr := memoryDisabled(ctx, flags, team); apiErr != nil {
		return 0, false, apiErr
	}

	want := int64(*requested)
	switch {
	case build.MaxRamMb == nil && want != build.RamMb:
		return 0, false, memoryError(errCodeSandboxMemoryNotResizable,
			fmt.Sprintf("memoryMB %d is not available: the template's memory cannot grow and is %d MiB", want, build.RamMb),
			fmt.Errorf("memoryMB %d requested on a build with fixed memory %d MiB", want, build.RamMb))
	case build.MaxRamMb != nil && (want < build.RamMb || want > *build.MaxRamMb):
		return 0, false, memoryError(errCodeSandboxMemoryOutOfRange,
			fmt.Sprintf("memoryMB %d must be between the template's memoryMB %d and its maxMemoryMB %d", want, build.RamMb, *build.MaxRamMb),
			fmt.Errorf("memoryMB %d requested on a build with %d to %d MiB", want, build.RamMb, *build.MaxRamMb))
	case (want-build.RamMb)%constants.MemoryHotplugSlotMiB != 0:
		return 0, false, memoryError(errCodeSandboxMemoryOutOfRange,
			fmt.Sprintf("memoryMB %d must be the template's memoryMB %d plus a multiple of %d MiB", want, build.RamMb, constants.MemoryHotplugSlotMiB),
			fmt.Errorf("memoryMB %d is not whole %d MiB slots above %d MiB", want, constants.MemoryHotplugSlotMiB, build.RamMb))
	case want < current:
		return 0, false, memoryError(errCodeSandboxMemoryShrinkUnsupported,
			fmt.Sprintf("memoryMB %d is below the sandbox's current %d MiB; a sandbox's memory can only grow", want, current),
			fmt.Errorf("memoryMB %d requested on a build that starts with %d MiB", want, current))
	case want > limitBase && want > team.Limits.MaxRamMb:
		return 0, false, memoryError(errCodeSandboxMemoryTeamLimit,
			fmt.Sprintf("memoryMB must be at most your team limit (%d MiB)", team.Limits.MaxRamMb),
			fmt.Errorf("memoryMB %d exceeds team limit %d MiB", want, team.Limits.MaxRamMb))
	case want > limitBase:
		if apiErr := envdOutdatedForGrowth(build, want); apiErr != nil {
			return 0, false, apiErr
		}
	}

	return want, true, nil
}

// minEnvdBuildForMemoryGrowth is the first envd build that raises the user and PTY
// memory limit as hotplugged memory comes online (#6499); an older envd leaves the
// guest's processes capped at boot RAM however much is plugged. It is a timestamped
// build, so a release on the 0.10 line does not meet it and a later line does.
const minEnvdBuildForMemoryGrowth = "0.10.202611131208-b1bd7be92bd"

// envdOutdatedForGrowth refuses memory growth on a build whose envd cannot let the
// guest's processes use it.
func envdOutdatedForGrowth(build queries.EnvBuild, want int64) *api.APIError {
	version := ""
	if build.EnvdVersion != nil {
		version = *build.EnvdVersion
	}
	if version != "" {
		if ok, err := sharedUtils.IsGTEVersionWithTimestamp(version, minEnvdBuildForMemoryGrowth); err == nil && ok {
			return nil
		}
	}

	return memoryError(errCodeSandboxMemoryEnvdOutdated,
		fmt.Sprintf("memory of %d MiB needs a template built with a newer envd (template envd %q, at least %s); rebuild the template", want, version, minEnvdBuildForMemoryGrowth),
		fmt.Errorf("memory growth to %d MiB on a build with envd %q, below %s", want, version, minEnvdBuildForMemoryGrowth))
}

func memoryError(code, clientMsg string, err error) *api.APIError {
	return &api.APIError{
		Code:      http.StatusBadRequest,
		ErrorCode: code,
		ClientMsg: clientMsg,
		Err:       err,
		Retryable: apierrors.NoRetry,
	}
}

// memoryDisabled refuses an explicit memoryMB when resizable templates are off
// for the team, before a start is reserved or joined, as cpuCountDisabled does.
func memoryDisabled(ctx context.Context, flags *featureflags.Client, team *teamtypes.Team) *api.APIError {
	if flags.BoolFlag(ctx, featureflags.ResizableTemplatesFlag, featureflags.TeamContext(team.ID.String())) {
		return nil
	}

	return memoryError(errCodeSandboxMemoryDisabled,
		"Resizable templates are not enabled for your team",
		errors.New("sandbox memoryMB requested while resizable templates are off for the team"))
}
