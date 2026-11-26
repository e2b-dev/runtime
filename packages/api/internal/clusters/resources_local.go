package clusters

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/grafana/loki/v3/pkg/logproto"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/cfg"
	clickhouse "github.com/e2b-dev/infra/packages/clickhouse/pkg"
	"github.com/e2b-dev/infra/packages/clickhouse/pkg/sandboxlogs"
	clickhouseutils "github.com/e2b-dev/infra/packages/clickhouse/pkg/utils"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/logs"
	"github.com/e2b-dev/infra/packages/shared/pkg/logs/loki"
	"github.com/e2b-dev/infra/packages/shared/pkg/smap"
)

// ClickhouseLogsReader is the narrow ClickHouse sandbox_logs reader the local
// cluster needs.
type ClickhouseLogsReader interface {
	QuerySandboxLogs(ctx context.Context, teamID uuid.UUID, sandboxID string, start, end time.Time, limit int, order sandboxlogs.SortOrder, level *logs.LogLevel, search *string) ([]logs.LogEntry, error)
	QueryBuildLogs(ctx context.Context, teamID uuid.UUID, templateID, buildID string, start, end time.Time, limit int, offset int32, level *logs.LogLevel, order sandboxlogs.SortOrder) ([]logs.LogEntry, error)
}

var _ ClickhouseLogsReader = (*sandboxlogs.Reader)(nil)

// logReadMeter/logReadErrorCount count ClickHouse log read failures so
// operators can alert on them, broken down by log kind.
var (
	logReadMeter    = otel.Meter("github.com/e2b-dev/infra/packages/api/internal/clusters")
	logReadErrCount = mustLogReadCounter(
		"log_read_clickhouse_error_count",
		"Number of local-cluster ClickHouse log read failures by log kind",
	)
)

func mustLogReadCounter(name, description string) metric.Int64Counter {
	counter, err := logReadMeter.Int64Counter(name, metric.WithDescription(description))
	if err != nil {
		return nil
	}

	return counter
}

func recordClickhouseLogReadError(ctx context.Context, kind string) {
	if logReadErrCount == nil {
		return
	}

	logReadErrCount.Add(ctx, 1, metric.WithAttributes(attribute.String("kind", kind)))
}

type LocalClusterResourceProvider struct {
	config                      cfg.Config
	querySandboxMetricsProvider clickhouse.SandboxQueriesProvider
	queryLogsProvider           *loki.LokiQueryProvider
	sandboxLogsReader           ClickhouseLogsReader
	featureFlags                *featureflags.Client
	instances                   *smap.Map[*Instance]
}

func newLocalClusterResourceProvider(
	querySandboxMetricsProvider clickhouse.SandboxQueriesProvider,
	queryLogsProvider *loki.LokiQueryProvider,
	sandboxLogsReader ClickhouseLogsReader,
	featureFlags *featureflags.Client,
	instances *smap.Map[*Instance],
	config cfg.Config,
) ClusterResource {
	return &LocalClusterResourceProvider{
		config:                      config,
		querySandboxMetricsProvider: querySandboxMetricsProvider,
		queryLogsProvider:           queryLogsProvider,
		sandboxLogsReader:           sandboxLogsReader,
		featureFlags:                featureFlags,
		instances:                   instances,
	}
}

// errNoLogStore is the error of a read that did not route to ClickHouse on an
// api started without LOKI_URL: there is nothing to read from until
// logs-read-config is on and a ClickHouse reader is configured.
var errNoLogStore = errors.New("no log store for this read: LOKI_URL is unset and the read did not route to ClickHouse (logs-read-config off, or no CLICKHOUSE_CONNECTION_STRING to read from)")

// readFromClickhouse reports whether log reads should hit ClickHouse. It is
// true only when the logs-read-config flag is enabled AND a ClickHouse reader
// is configured; otherwise reads stay on Loki (default behavior).
func (l *LocalClusterResourceProvider) readFromClickhouse(ctx context.Context) bool {
	if l.sandboxLogsReader == nil || l.featureFlags == nil {
		return false
	}

	return l.featureFlags.BoolFlag(ctx, featureflags.LogsReadConfigFlag)
}

// apiLogDirectionToSandboxLogsSortOrder converts an API log direction into the
// ClickHouse reader's SortOrder (Forward by default).
func apiLogDirectionToSandboxLogsSortOrder(direction *api.LogsDirection) sandboxlogs.SortOrder {
	if direction != nil && *direction == api.LogsDirectionBackward {
		return sandboxlogs.SortOrderBackward
	}

	return sandboxlogs.SortOrderForward
}

func (l *LocalClusterResourceProvider) GetSandboxMetrics(ctx context.Context, teamID string, sandboxID string, qStart *int64, qEnd *int64) ([]api.SandboxMetric, *api.APIError) {
	start, end, err := clickhouseutils.GetSandboxStartEndTime(ctx, l.querySandboxMetricsProvider, teamID, sandboxID, qStart, qEnd)
	if err != nil {
		return nil, &api.APIError{
			Err:       fmt.Errorf(`error when getting metrics time range: %w`, err),
			ClientMsg: "Failed to fetch sandbox metrics",
			Code:      http.StatusInternalServerError,
		}
	}

	start, end, err = clickhouseutils.ValidateRange(start, end)
	if err != nil {
		return nil, &api.APIError{
			Err:       fmt.Errorf(`error when validating range of metrics: %w`, err),
			ClientMsg: "Invalid time range for metrics",
			Code:      http.StatusBadRequest,
		}
	}

	// Calculate the step size
	step := clickhouseutils.CalculateStep(start, end)

	rawMetrics, err := l.querySandboxMetricsProvider.QuerySandboxMetrics(ctx, sandboxID, teamID, start, end, step)
	if err != nil {
		return nil, &api.APIError{
			Err:       fmt.Errorf(`error when querying sandbox metrics: %w`, err),
			ClientMsg: "Failed to fetch sandbox metrics",
			Code:      http.StatusInternalServerError,
		}
	}

	metrics := make([]api.SandboxMetric, len(rawMetrics))
	for i, m := range rawMetrics {
		metrics[i] = api.SandboxMetric{
			Timestamp:     m.Timestamp,
			TimestampUnix: m.Timestamp.Unix(),
			CpuUsedPct:    float32(m.CPUUsedPercent),
			CpuCount:      int32(m.CPUCount),
			MemTotal:      int64(m.MemTotal),
			MemUsed:       int64(m.MemUsed),
			MemCache:      int64(m.MemCache),
			DiskTotal:     int64(m.DiskTotal),
			DiskUsed:      int64(m.DiskUsed),
		}
	}

	return metrics, nil
}

func (l *LocalClusterResourceProvider) GetSandboxesMetrics(ctx context.Context, teamID string, sandboxIDs []string) (map[string]api.SandboxMetric, *api.APIError) {
	rawMetrics, err := l.querySandboxMetricsProvider.QueryLatestMetrics(ctx, sandboxIDs, teamID)
	if err != nil {
		return nil, &api.APIError{
			Err:       err,
			ClientMsg: "Failed to fetch sandbox metrics",
			Code:      http.StatusInternalServerError,
		}
	}

	metrics := make(map[string]api.SandboxMetric)
	for _, m := range rawMetrics {
		metrics[m.SandboxID] = api.SandboxMetric{
			Timestamp:     m.Timestamp,
			TimestampUnix: m.Timestamp.Unix(),
			CpuUsedPct:    float32(m.CPUUsedPercent),
			CpuCount:      int32(m.CPUCount),
			MemTotal:      int64(m.MemTotal),
			MemUsed:       int64(m.MemUsed),
			MemCache:      int64(m.MemCache),
			DiskTotal:     int64(m.DiskTotal),
			DiskUsed:      int64(m.DiskUsed),
		}
	}

	return metrics, nil
}

func (l *LocalClusterResourceProvider) GetSandboxLogs(ctx context.Context, teamID string, sandboxID string, qStart *int64, qEnd *int64, qLimit *int32, qDirection *api.LogsDirection, level *logs.LogLevel, search *string) (api.SandboxLogs, *api.APIError) {
	start, end := time.Now().Add(-logsOldestLimit), time.Now()
	if qStart != nil {
		start = time.UnixMilli(*qStart)
	}

	if qEnd != nil {
		end = time.UnixMilli(*qEnd)
	}

	limit := defaultLogsLimit
	if qLimit != nil {
		limit = int(*qLimit)
	}

	var (
		raw []logs.LogEntry
		err error
	)
	switch {
	case l.readFromClickhouse(ctx):
		teamUUID, parseErr := uuid.Parse(teamID)
		if parseErr != nil {
			return api.SandboxLogs{}, &api.APIError{
				Err:       fmt.Errorf("invalid team ID %q: %w", teamID, parseErr),
				ClientMsg: "Invalid team ID",
				Code:      http.StatusBadRequest,
			}
		}

		raw, err = l.sandboxLogsReader.QuerySandboxLogs(ctx, teamUUID, sandboxID, start, end, limit, apiLogDirectionToSandboxLogsSortOrder(qDirection), level, search)
		if err != nil {
			recordClickhouseLogReadError(ctx, "sandbox")
		}
	case l.queryLogsProvider == nil:
		err = errNoLogStore
	default:
		raw, err = l.queryLogsProvider.QuerySandboxLogs(ctx, teamID, sandboxID, start, end, limit, apiLogDirectionToLokiProtoDirection(qDirection), level, search)
	}
	if err != nil {
		return api.SandboxLogs{}, &api.APIError{
			Err:       fmt.Errorf("error when fetching sandbox logs: %w", err),
			ClientMsg: "Failed to fetch sandbox logs",
			Code:      http.StatusInternalServerError,
		}
	}

	ll := make([]api.SandboxLog, len(raw))
	for i, row := range raw {
		ll[i] = api.SandboxLog{Line: row.Raw, Timestamp: row.Timestamp}
	}

	le := make([]api.SandboxLogEntry, len(raw))
	for i, row := range raw {
		le[i] = api.SandboxLogEntry{
			Timestamp: row.Timestamp,
			Level:     api.LogLevel(logs.LevelToString(row.Level)),
			Message:   row.Message,
			Fields:    row.Fields,
		}
	}

	return api.SandboxLogs{Logs: ll, LogEntries: le}, nil
}

func (l *LocalClusterResourceProvider) GetBuildLogs(
	ctx context.Context,
	nodeID *string,
	teamID uuid.UUID,
	templateID string,
	buildID string,
	offset int32,
	limit int32,
	level *logs.LogLevel,
	cursor *time.Time,
	direction api.LogsDirection,
	source *api.LogsSource,
) ([]logs.LogEntry, *api.APIError) {
	// The persistent log backend is Loki by default, ClickHouse when the
	// logs-read-config flag is enabled and a ClickHouse reader is configured.
	start, end := LogQueryWindow(cursor, direction)

	var persistentFetcher logSourceFunc
	if l.readFromClickhouse(ctx) {
		persistentFetcher = l.logsFromClickhouse(ctx, teamID, templateID, buildID, start, end, int(limit), offset, level, apiLogDirectionToSandboxLogsSortOrder(&direction))
	} else {
		persistentFetcher = l.logsFromLocalLoki(ctx, templateID, buildID, start, end, int(limit), offset, level, apiLogDirectionToLokiProtoDirection(&direction))
	}

	return getBuildLogsWithSources(ctx, l.instances, nodeID, templateID, buildID, offset, limit, level, cursor, direction, source, persistentFetcher)
}

func (l *LocalClusterResourceProvider) logsFromClickhouse(ctx context.Context, teamID uuid.UUID, templateID string, buildID string, start time.Time, end time.Time, limit int, offset int32, level *logs.LogLevel, order sandboxlogs.SortOrder) logSourceFunc {
	return func() ([]logs.LogEntry, *api.APIError) {
		entries, err := l.sandboxLogsReader.QueryBuildLogs(ctx, teamID, templateID, buildID, start, end, limit, offset, level, order)
		if err != nil {
			recordClickhouseLogReadError(ctx, "build")

			return nil, &api.APIError{
				Err:       fmt.Errorf("error when fetching build logs from ClickHouse: %w", err),
				ClientMsg: "Failed to fetch build logs",
				Code:      http.StatusInternalServerError,
			}
		}

		return entries, nil
	}
}

func (l *LocalClusterResourceProvider) logsFromLocalLoki(ctx context.Context, templateID string, buildID string, start time.Time, end time.Time, limit int, offset int32, level *logs.LogLevel, direction logproto.Direction) logSourceFunc {
	return func() ([]logs.LogEntry, *api.APIError) {
		if l.queryLogsProvider == nil {
			return nil, &api.APIError{
				Err:       errNoLogStore,
				ClientMsg: "Failed to fetch build logs",
				Code:      http.StatusInternalServerError,
			}
		}

		entries, err := l.queryLogsProvider.QueryBuildLogs(ctx, templateID, buildID, start, end, limit, offset, level, direction)
		if err != nil {
			return nil, &api.APIError{
				Err:       fmt.Errorf("error when fetching build logs from Loki: %w", err),
				ClientMsg: "Failed to fetch build logs",
				Code:      http.StatusInternalServerError,
			}
		}

		return entries, nil
	}
}

// The local cluster has no edge deployment and no scaling group behind it.

func (l *LocalClusterResourceProvider) GetRigs(context.Context) ([]api.Rig, *api.APIError) {
	return nil, rigsUnsupported()
}

func (l *LocalClusterResourceProvider) SetRigCapacity(context.Context, string, int32) *api.APIError {
	return rigsUnsupported()
}

func (l *LocalClusterResourceProvider) GetRigInstances(context.Context, string) ([]api.RigInstance, *api.APIError) {
	return nil, rigsUnsupported()
}

func (l *LocalClusterResourceProvider) GetRigErrors(context.Context, string, *int32) ([]api.RigError, *api.APIError) {
	return nil, rigsUnsupported()
}

func (l *LocalClusterResourceProvider) TerminateRigInstance(context.Context, string, bool) *api.APIError {
	return rigsUnsupported()
}
