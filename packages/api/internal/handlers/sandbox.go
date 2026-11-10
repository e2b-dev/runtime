package handlers

import (
	"context"
	"maps"
	"net/http"
	"slices"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/orchestrator"
	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
	typesteam "github.com/e2b-dev/infra/packages/auth/pkg/types"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

var tracer = otel.Tracer("github.com/e2b-dev/infra/packages/api/internal/handlers")

func (a *APIStore) startSandbox(
	ctx context.Context,
	sandboxID string,
	timeout time.Duration,
	team *typesteam.Team,
	getSandboxData orchestrator.SandboxDataFetcher,
	requestHeader *http.Header,
	isResume bool,
	demandFilesystemBoot bool,
	mcp api.Mcp,
	options ...orchestrator.CreateOption,
) (*api.Sandbox, *api.APIError) {
	sbx, apiErr := a.startSandboxInternal(
		ctx,
		sandboxID,
		timeout,
		team,
		getSandboxData,
		requestHeader,
		isResume,
		demandFilesystemBoot,
		mcp,
		options...,
	)
	if apiErr != nil {
		return nil, apiErr
	}

	return sbx.ToAPISandbox(), nil
}

// startSandboxInternal starts the sandbox and returns the internal sandbox model (includes routing info).
func (a *APIStore) startSandboxInternal(
	ctx context.Context,
	sandboxID string,
	timeout time.Duration,
	team *typesteam.Team,
	getSandboxData orchestrator.SandboxDataFetcher,
	requestHeader *http.Header,
	isResume bool,
	demandFilesystemBoot bool,
	mcp api.Mcp,
	options ...orchestrator.CreateOption,
) (sandbox.Sandbox, *api.APIError) {
	startTime := time.Now()
	endTime := startTime.Add(timeout)

	// Unique ID for the execution (from start/resume to stop/pause)
	executionID := uuid.New().String()

	creationMeta := buildCreationMetadata(team, requestHeader, isResume, mcp)

	sbx, instanceErr := a.orchestrator.CreateSandbox(
		ctx,
		sandboxID,
		executionID,
		team,
		getSandboxData,
		startTime,
		endTime,
		timeout,
		isResume,
		demandFilesystemBoot,
		creationMeta,
		options...,
	)
	if instanceErr != nil {
		telemetry.ReportError(ctx, "error when creating instance", instanceErr.Err)

		return sandbox.Sandbox{}, instanceErr
	}

	telemetry.ReportEvent(ctx, "Created sandbox")

	go func() {
		a.templateSpawnCounter.IncreaseTemplateSpawnCount(sbx.BaseTemplateID, time.Now())
	}()

	telemetry.SetAttributes(ctx,
		attribute.String("instance.id", sbx.SandboxID),
	)

	return sbx, nil
}

func buildCreationMetadata(
	team *typesteam.Team,
	requestHeader *http.Header,
	isResume bool,
	mcp api.Mcp,
) sandbox.CreationMetadata {
	meta := sandbox.CreationMetadata{
		IsResume: isResume,
		TeamName: team.Name,
	}

	if requestHeader != nil {
		meta.RequestHeader = *requestHeader
	}

	if mcp != nil {
		meta.MCPServerNames = slices.Collect(maps.Keys(mcp))
	}

	return meta
}
