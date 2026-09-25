// Package sandboxlogs is a minimal ClickHouse reader for the sandbox_logs
// table, used by the API local cluster to serve sandbox/build logs from
// ClickHouse behind the logs-read-config LaunchDarkly flag.
package sandboxlogs

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/clickhouse/pkg/timestamp"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/logs"
)

// SortOrder controls the timestamp ordering of returned log rows.
type SortOrder int

const (
	SortOrderForward SortOrder = iota
	SortOrderBackward
)

// Reader queries the ClickHouse sandbox_logs table.
type Reader struct {
	conn driver.Conn
}

// NewReader builds a Reader over an existing ClickHouse driver connection.
func NewReader(conn driver.Conn) *Reader {
	return &Reader{conn: conn}
}

// Close closes the underlying ClickHouse connection.
func (r *Reader) Close(_ context.Context) error {
	return r.conn.Close()
}

// row maps a single sandbox_logs table row.
type row struct {
	Timestamp  time.Time
	TeamID     uuid.UUID
	SandboxID  string
	TemplateID string
	BuildID    string
	Service    string
	Category   string
	Level      string
	Message    string
	Raw        string
	Fields     string
}

// toLogEntry converts a row into a logs.LogEntry. The Fields column holds a
// JSON-encoded map[string]string; on unmarshal failure onParseErr is invoked
// (when non-nil), an empty map is used, and Raw is preserved.
func (r row) toLogEntry(onParseErr func(err error)) logs.LogEntry {
	fields := map[string]string{}
	if r.Fields != "" {
		if err := json.Unmarshal([]byte(r.Fields), &fields); err != nil {
			if onParseErr != nil {
				onParseErr(err)
			}
			fields = map[string]string{}
		}
	}

	return logs.LogEntry{
		Timestamp: r.Timestamp,
		Raw:       r.Raw,
		Level:     logs.StringToLevel(r.Level),
		Message:   r.Message,
		Fields:    fields,
	}
}

// orderSQL renders the ORDER BY direction keyword for a SortOrder.
func orderSQL(o SortOrder) string {
	if o == SortOrderBackward {
		return "DESC"
	}

	return "ASC"
}

// atLeastLevels returns the set of stored level strings at or above minLevel,
// mirroring the Loki minimum-level filter semantics.
func atLeastLevels(minLevel logs.LogLevel) []string {
	switch minLevel {
	case logs.LevelError:
		return []string{"error"}
	case logs.LevelWarn:
		return []string{"warn", "error"}
	case logs.LevelInfo:
		return []string{"", "info", "warn", "error"}
	default:
		return []string{"", "debug", "info", "warn", "error"}
	}
}

const sandboxLogsSelect = `
SELECT
    timestamp,
    team_id,
    sandbox_id,
    template_id,
    build_id,
    service,
    category,
    level,
    message,
    raw,
    fields
FROM sandbox_logs
`

func (r *Reader) QuerySandboxLogs(ctx context.Context, teamID uuid.UUID, sandboxID string, start, end time.Time, limit int, order SortOrder, level *logs.LogLevel, search *string) ([]logs.LogEntry, error) {
	filters := []string{
		"team_id = {team_id:String}",
		"sandbox_id = {sandbox_id:String}",
		"timestamp >= fromUnixTimestamp64Nano({start:Int64})",
		"timestamp <= fromUnixTimestamp64Nano({end:Int64})",
		"category != 'metrics'",
	}
	args := []any{
		clickhouse.Named("team_id", teamID.String()),
		clickhouse.Named("sandbox_id", sandboxID),
		clickhouse.Named("start", timestamp.UnixNano(start)),
		clickhouse.Named("end", timestamp.UnixNano(end)),
	}

	if level != nil {
		filters = append(filters, "level IN {levels:Array(String)}")
		args = append(args, clickhouse.Named("levels", atLeastLevels(*level)))
	}
	if search != nil && *search != "" {
		filters = append(filters, "position(message, {search:String}) > 0")
		args = append(args, clickhouse.Named("search", *search))
	}

	q := sandboxLogsSelect +
		"WHERE " + strings.Join(filters, "\n  AND ") + "\n" +
		"ORDER BY timestamp " + orderSQL(order) + "\n" +
		fmt.Sprintf("LIMIT %d", limit)

	out, err := r.scanSandboxLogs(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("error querying sandbox logs: %w", err)
	}

	return out, nil
}

func (r *Reader) QueryBuildLogs(ctx context.Context, templateID, buildID string, start, end time.Time, limit int, offset int32, level *logs.LogLevel, order SortOrder) ([]logs.LogEntry, error) {
	filters := []string{
		"build_id = {build_id:String}",
		"template_id = {template_id:String}",
		"timestamp >= fromUnixTimestamp64Nano({start:Int64})",
		"timestamp <= fromUnixTimestamp64Nano({end:Int64})",
		"service = 'template-manager'",
	}
	args := []any{
		clickhouse.Named("build_id", buildID),
		clickhouse.Named("template_id", templateID),
		clickhouse.Named("start", timestamp.UnixNano(start)),
		clickhouse.Named("end", timestamp.UnixNano(end)),
	}

	if level != nil {
		filters = append(filters, "level IN {levels:Array(String)}")
		args = append(args, clickhouse.Named("levels", atLeastLevels(*level)))
	}

	q := sandboxLogsSelect +
		"WHERE " + strings.Join(filters, "\n  AND ") + "\n" +
		"ORDER BY timestamp " + orderSQL(order) + "\n" +
		fmt.Sprintf("LIMIT %d, %d", offset, limit)

	out, err := r.scanSandboxLogs(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("error querying build logs: %w", err)
	}

	return out, nil
}

func (r *Reader) scanSandboxLogs(ctx context.Context, q string, args ...any) ([]logs.LogEntry, error) {
	rows, err := r.conn.Query(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("error querying sandbox logs: %w", err)
	}
	defer rows.Close()

	out := make([]logs.LogEntry, 0)
	for rows.Next() {
		var rr row
		if err := rows.Scan(
			&rr.Timestamp,
			&rr.TeamID,
			&rr.SandboxID,
			&rr.TemplateID,
			&rr.BuildID,
			&rr.Service,
			&rr.Category,
			&rr.Level,
			&rr.Message,
			&rr.Raw,
			&rr.Fields,
		); err != nil {
			return nil, fmt.Errorf("error scanning sandbox log row: %w", err)
		}

		entry := rr.toLogEntry(func(err error) {
			logger.L().Warn(ctx, "failed to parse sandbox log fields",
				zap.String("sandbox_id", rr.SandboxID),
				logger.Time("timestamp", rr.Timestamp),
				zap.Error(err),
			)
		})
		out = append(out, entry)
	}

	return out, rows.Err()
}
