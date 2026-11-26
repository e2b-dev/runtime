package clusters

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/grafana/loki/v3/pkg/logproto"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	templatemanagergrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc/template-manager"
	edgeapi "github.com/e2b-dev/infra/packages/shared/pkg/http/edge"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/logs"
	"github.com/e2b-dev/infra/packages/shared/pkg/smap"
)

type ClusterResource interface {
	// A rig is a cloud scaling group behind an edge deployment, so only a
	// remote cluster can serve these; the local one answers 501.
	GetRigs(ctx context.Context) ([]api.Rig, *api.APIError)
	SetRigCapacity(ctx context.Context, rigID string, desired int32) *api.APIError
	GetRigInstances(ctx context.Context, rigID string) ([]api.RigInstance, *api.APIError)
	GetRigErrors(ctx context.Context, rigID string, limit *int32) ([]api.RigError, *api.APIError)
	TerminateRigInstance(ctx context.Context, instanceID string, decrementDesired bool) *api.APIError

	GetSandboxMetrics(ctx context.Context, teamID string, sandboxID string, qStart *int64, qEnd *int64) ([]api.SandboxMetric, *api.APIError)
	GetSandboxesMetrics(ctx context.Context, teamID string, sandboxIDs []string) (map[string]api.SandboxMetric, *api.APIError)
	GetSandboxLogs(ctx context.Context, teamID string, sandboxID string, start *int64, end *int64, limit *int32, direction *api.LogsDirection, level *logs.LogLevel, search *string) (api.SandboxLogs, *api.APIError)
	GetBuildLogs(ctx context.Context, nodeID *string, teamID uuid.UUID, templateID string, buildID string, offset int32, limit int32, level *logs.LogLevel, cursor *time.Time, direction api.LogsDirection, source *api.LogsSource) ([]logs.LogEntry, *api.APIError)
}

const (
	logsOldestLimit = 7 * 24 * time.Hour // 7 days

	defaultLogsLimit = 1000
)

// rigsUnsupported is a 501 rather than a 404: the cluster exists, the
// capability does not.
func rigsUnsupported() *api.APIError {
	return &api.APIError{
		Err:       errors.New("rig management is not available for this cluster"),
		ClientMsg: "Rig management is not available for this cluster",
		Code:      http.StatusNotImplemented,
	}
}

func LogQueryWindow(cursor *time.Time, direction api.LogsDirection) (time.Time, time.Time) {
	now := time.Now()
	oldestAllowedStart := now.Add(-logsOldestLimit)
	start, end := oldestAllowedStart, now

	if cursor != nil {
		if direction == api.LogsDirectionForward {
			start = *cursor
			end = start.Add(logsOldestLimit)
		} else {
			end = *cursor
			start = end.Add(-logsOldestLimit)
		}
	}

	// Ensure start time respects the log retention limit
	if start.Before(oldestAllowedStart) {
		start = oldestAllowedStart
	}

	// Ensure end time respects the log retention limit
	// (can happen if cursor is very old and results in end < oldestAllowed)
	if end.Before(oldestAllowedStart) {
		end = oldestAllowedStart
	}

	// Ensure start is never after end
	if start.After(end) {
		start = end
	}

	return start, end
}

func logDirectionToTemplateManagerDirection(direction api.LogsDirection) templatemanagergrpc.LogsDirection {
	switch direction {
	case api.LogsDirectionForward:
		return templatemanagergrpc.LogsDirection_Forward
	case api.LogsDirectionBackward:
		return templatemanagergrpc.LogsDirection_Backward
	default:
		return templatemanagergrpc.LogsDirection_Forward
	}
}

func apiLogDirectionToLokiProtoDirection(direction *api.LogsDirection) logproto.Direction {
	if direction == nil {
		return logproto.FORWARD
	}

	if *direction == api.LogsDirectionBackward {
		return logproto.BACKWARD
	}

	return logproto.FORWARD
}

func apiLogDirectionToEdgeSandboxLogsDirection(direction *api.LogsDirection) *edgeapi.V1SandboxLogsParamsDirection {
	if direction == nil {
		return nil
	}

	if *direction == api.LogsDirectionBackward {
		return new(edgeapi.V1SandboxLogsParamsDirectionBackward)
	}

	return new(edgeapi.V1SandboxLogsParamsDirectionForward)
}

func apiLogDirectionToEdgeBuildLogsDirection(direction *api.LogsDirection) *edgeapi.V1TemplateBuildLogsParamsDirection {
	if direction == nil {
		return nil
	}

	if *direction == api.LogsDirectionBackward {
		return new(edgeapi.V1TemplateBuildLogsParamsDirectionBackward)
	}

	return new(edgeapi.V1TemplateBuildLogsParamsDirectionForward)
}

func logToEdgeLevel(level *logs.LogLevel) *edgeapi.LogLevel {
	if level == nil {
		return nil
	}

	value := edgeapi.LogLevel(logs.LevelToString(*level))

	return &value
}

func logCheckSourceType(source *api.LogsSource, sourceType api.LogsSource) bool {
	return source == nil || *source == sourceType
}

type logSourceFunc func() ([]logs.LogEntry, *api.APIError)

func logsFromBuilderInstance(ctx context.Context, instance *Instance, templateID string, buildID string, offset int32, limit int32, level *logs.LogLevel, start time.Time, end time.Time, direction api.LogsDirection) logSourceFunc {
	return func() ([]logs.LogEntry, *api.APIError) {
		var lvlReq *templatemanagergrpc.LogLevel
		if level != nil {
			lvlReq = templatemanagergrpc.LogLevel(*level).Enum()
		}

		res, err := instance.GetClient().Template.TemplateBuildStatus(
			ctx, &templatemanagergrpc.TemplateStatusRequest{
				TemplateID: templateID,
				BuildID:    buildID,
				Offset:     &offset,
				Limit:      new(uint32(limit)),
				Level:      lvlReq,
				Start:      timestamppb.New(start),
				End:        timestamppb.New(end),
				Direction:  new(logDirectionToTemplateManagerDirection(direction)),
			},
		)
		if err != nil {
			return nil, &api.APIError{
				Err:       err,
				ClientMsg: "Failed to fetch build logs from builder instance",
				Code:      http.StatusInternalServerError,
			}
		}

		raw := res.GetLogEntries()
		entries := make([]logs.LogEntry, len(raw))
		for i, entry := range raw {
			entries[i] = logs.LogEntry{
				Timestamp: entry.GetTimestamp().AsTime(),
				Message:   entry.GetMessage(),
				Level:     logs.LogLevel(entry.GetLevel()),
				Fields:    entry.GetFields(),
			}
		}

		return entries, nil
	}
}

// getBuildLogsWithSources implements the shared logic for fetching build logs from multiple sources.
// This function extracts the common pattern used by both local and remote cluster resource providers,
// avoiding code duplication between the two implementations.
//
// The function tries log sources in order based on availability and configuration:
// 1. Temporary logs from the builder instance (if nodeID is provided and source allows)
// 2. Persistent logs from backend storage (strategy provided by caller)
//
// It returns the first successful result, logging warnings for any failures encountered.
// This unified approach ensures consistent behavior and makes maintenance easier by centralizing
// the source selection and fallback logic.
func getBuildLogsWithSources(
	ctx context.Context,
	instances *smap.Map[*Instance],
	nodeID *string,
	templateID string,
	buildID string,
	offset int32,
	limit int32,
	level *logs.LogLevel,
	cursor *time.Time,
	direction api.LogsDirection,
	source *api.LogsSource,
	persistentLogFetcher logSourceFunc, // Backend-specific strategy for persistent logs (Loki for local, Edge API for remote)
) ([]logs.LogEntry, *api.APIError) {
	ctx, span := tracer.Start(ctx, "get build-logs")
	defer span.End()

	start, end := LogQueryWindow(cursor, direction)

	var sources []logSourceFunc

	// Handle temporary logs from builder instance
	if nodeID != nil && logCheckSourceType(source, api.LogsSourceTemporary) {
		instance, found := instanceOnNode(instances, *nodeID)
		if found {
			sourceCallback := logsFromBuilderInstance(ctx, instance, templateID, buildID, offset, limit, level, start, end, direction)
			sources = append(sources, sourceCallback)
		} else {
			logger.L().Warn(
				ctx, "Node instance not found for build logs, falling back to other sources",
				logger.WithNodeID(*nodeID),
				logger.WithTemplateID(templateID),
				logger.WithBuildID(buildID),
			)
		}
	}

	// Handle persistent logs (backend-specific implementation provided by caller)
	if logCheckSourceType(source, api.LogsSourcePersistent) {
		sources = append(sources, persistentLogFetcher)
	}

	// Iterate through sources and return the first successful fetch.
	var lastErr *api.APIError
	for _, sourceFetch := range sources {
		entries, err := sourceFetch()
		if err != nil {
			logger.L().Warn(ctx, "Error fetching build logs", logger.WithTemplateID(templateID), logger.WithBuildID(buildID), zap.Error(err))
			lastErr = err

			continue
		}

		return entries, nil
	}

	return nil, lastErr
}
