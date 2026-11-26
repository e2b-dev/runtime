package clusters

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/launchdarkly/go-server-sdk/v7/testhelpers/ldtestdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/clickhouse/pkg/sandboxlogs"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/logs"
	"github.com/e2b-dev/infra/packages/shared/pkg/smap"
)

type stubClickhouseLogsReader struct {
	sandbox func(context.Context, uuid.UUID, string, time.Time, time.Time, int, sandboxlogs.SortOrder, *logs.LogLevel, *string) ([]logs.LogEntry, error)
	build   func(context.Context, uuid.UUID, string, string, time.Time, time.Time, int, int32, *logs.LogLevel, sandboxlogs.SortOrder) ([]logs.LogEntry, error)
}

func (s *stubClickhouseLogsReader) QuerySandboxLogs(ctx context.Context, teamID uuid.UUID, sandboxID string, start, end time.Time, limit int, order sandboxlogs.SortOrder, level *logs.LogLevel, search *string) ([]logs.LogEntry, error) {
	return s.sandbox(ctx, teamID, sandboxID, start, end, limit, order, level, search)
}

func (s *stubClickhouseLogsReader) QueryBuildLogs(ctx context.Context, teamID uuid.UUID, templateID, buildID string, start, end time.Time, limit int, offset int32, level *logs.LogLevel, order sandboxlogs.SortOrder) ([]logs.LogEntry, error) {
	return s.build(ctx, teamID, templateID, buildID, start, end, limit, offset, level, order)
}

// TestBuildLogsFromClickhouseFailsFast asserts that a ClickHouse read error
// during GetBuildLogs is propagated directly (no automatic Loki fallback).
// The migration relies on the logs-read-config flag plus alerting on
// log_read_clickhouse_error_count to drive rollback decisions, rather than
// per-request fallback masking a degraded ClickHouse.
func TestBuildLogsFromClickhouseFailsFast(t *testing.T) {
	t.Parallel()

	clickhouseErr := errors.New("clickhouse unavailable")
	start, end := time.Now().Add(-time.Minute), time.Now()
	level := logs.LevelInfo
	teamID := uuid.New()
	want := []logs.LogEntry{{Message: "from ClickHouse"}}

	tests := []struct {
		name             string
		clickhouseResult []logs.LogEntry
		clickhouseErr    error
		want             []logs.LogEntry
		wantErr          bool
	}{
		{name: "success returns ClickHouse result", clickhouseResult: want, want: want},
		{name: "ClickHouse error propagates without fallback", clickhouseErr: clickhouseErr, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			provider := &LocalClusterResourceProvider{
				sandboxLogsReader: &stubClickhouseLogsReader{build: func(_ context.Context, gotTeamID uuid.UUID, templateID, buildID string, gotStart, gotEnd time.Time, limit int, offset int32, gotLevel *logs.LogLevel, order sandboxlogs.SortOrder) ([]logs.LogEntry, error) {
					assert.Equal(t, teamID, gotTeamID)
					assert.Equal(t, "template-id", templateID)
					assert.Equal(t, "build-id", buildID)
					assert.Equal(t, start, gotStart)
					assert.Equal(t, end, gotEnd)
					assert.Equal(t, 42, limit)
					assert.Equal(t, int32(7), offset)
					assert.Same(t, &level, gotLevel)
					assert.Equal(t, sandboxlogs.SortOrderBackward, order)

					return tt.clickhouseResult, tt.clickhouseErr
				}},
			}

			got, apiErr := provider.logsFromClickhouse(t.Context(), teamID, "template-id", "build-id", start, end, 42, 7, &level, sandboxlogs.SortOrderBackward)()
			if tt.wantErr {
				require.NotNil(t, apiErr)
				require.ErrorIs(t, apiErr.Err, clickhouseErr)

				return
			}
			require.Nil(t, apiErr)
			assert.Equal(t, tt.want, got)
		})
	}
}

// flagClient builds an isolated flag client whose logs-read-config flag has
// the given value, the way a deployment without LaunchDarkly sees it after the
// LOGS_READ_CONFIG override.
func flagClient(t *testing.T, logsReadConfig bool) *featureflags.Client {
	t.Helper()

	source := ldtestdata.DataSource()
	source.Update(source.Flag("logs-read-config").VariationForAll(logsReadConfig))
	client, err := featureflags.NewClientWithDatasource(source)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close(context.WithoutCancel(t.Context())) })

	return client
}

func TestReadFromClickhouseNeedsReaderAndFlag(t *testing.T) {
	t.Parallel()

	reader := &stubClickhouseLogsReader{}

	tests := []struct {
		name     string
		provider *LocalClusterResourceProvider
		want     bool
	}{
		{name: "flag on with a reader reads ClickHouse", provider: &LocalClusterResourceProvider{sandboxLogsReader: reader, featureFlags: flagClient(t, true)}, want: true},
		{name: "flag off keeps Loki", provider: &LocalClusterResourceProvider{sandboxLogsReader: reader, featureFlags: flagClient(t, false)}, want: false},
		{name: "no reader keeps Loki whatever the flag says", provider: &LocalClusterResourceProvider{featureFlags: flagClient(t, true)}, want: false},
		{name: "no flag client keeps Loki", provider: &LocalClusterResourceProvider{sandboxLogsReader: reader}, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, tt.provider.readFromClickhouse(t.Context()))
		})
	}
}

// Without LOKI_URL the api has no Loki client. A read that still routes to
// Loki must fail with a server error naming the cause, not dereference nil.
func TestLogReadsWithoutAnyStoreFail(t *testing.T) {
	t.Parallel()

	provider := &LocalClusterResourceProvider{featureFlags: flagClient(t, false)}
	start, end := time.Now().Add(-time.Minute), time.Now()

	_, apiErr := provider.logsFromLocalLoki(t.Context(), "template-id", "build-id", start, end, 10, 0, nil, 0)()
	require.NotNil(t, apiErr)
	require.ErrorIs(t, apiErr.Err, errNoLogStore)
	assert.Equal(t, http.StatusInternalServerError, apiErr.Code)
	assert.Contains(t, apiErr.Err.Error(), "LOKI_URL is unset")
	assert.Contains(t, apiErr.Err.Error(), "CLICKHOUSE_CONNECTION_STRING", "the message names both ways a read can end up with no store")

	_, apiErr = provider.GetSandboxLogs(t.Context(), uuid.NewString(), "sandbox-id", nil, nil, nil, nil, nil, nil)
	require.NotNil(t, apiErr)
	require.ErrorIs(t, apiErr.Err, errNoLogStore)
	assert.Equal(t, http.StatusInternalServerError, apiErr.Code)
}

// The path this change exists for: flag on, ClickHouse reader present, no Loki
// client at all. Both read entry points must serve from ClickHouse.
func TestLogReadsUseClickhouseWithoutAnyLokiClient(t *testing.T) {
	t.Parallel()

	want := []logs.LogEntry{{Message: "from ClickHouse", Raw: "raw line"}}
	teamID := uuid.New()
	reader := &stubClickhouseLogsReader{
		sandbox: func(context.Context, uuid.UUID, string, time.Time, time.Time, int, sandboxlogs.SortOrder, *logs.LogLevel, *string) ([]logs.LogEntry, error) {
			return want, nil
		},
		build: func(_ context.Context, gotTeamID uuid.UUID, _, _ string, _, _ time.Time, _ int, _ int32, _ *logs.LogLevel, _ sandboxlogs.SortOrder) ([]logs.LogEntry, error) {
			assert.Equal(t, teamID, gotTeamID)

			return want, nil
		},
	}
	provider := &LocalClusterResourceProvider{sandboxLogsReader: reader, featureFlags: flagClient(t, true), instances: smap.New[*Instance]()}

	sandboxLogs, apiErr := provider.GetSandboxLogs(t.Context(), uuid.NewString(), "sandbox-id", nil, nil, nil, nil, nil, nil)
	require.Nil(t, apiErr)
	require.Len(t, sandboxLogs.Logs, 1)
	assert.Equal(t, "raw line", sandboxLogs.Logs[0].Line)

	buildLogs, apiErr := provider.GetBuildLogs(t.Context(), nil, teamID, "template-id", "build-id", 0, 100, nil, nil, api.LogsDirectionForward, nil)
	require.Nil(t, apiErr)
	assert.Equal(t, want, buildLogs)
}
