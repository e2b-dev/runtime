package clickhouse

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"

	"github.com/e2b-dev/infra/packages/clickhouse/pkg/timestamp"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

type Metrics struct {
	SandboxID      string    `ch:"sandbox_id"`
	TeamID         string    `ch:"team_id"`
	Timestamp      time.Time `ch:"ts"`
	CPUCount       float64   `ch:"cpu_total"`
	CPUUsedPercent float64   `ch:"cpu_used"`
	MemTotal       float64   `ch:"ram_total"`
	MemUsed        float64   `ch:"ram_used"`
	MemCache       float64   `ch:"ram_cache"`
	DiskTotal      float64   `ch:"disk_total"`
	DiskUsed       float64   `ch:"disk_used"`
}

var latestMetricsSelectQuery = fmt.Sprintf(`
SELECT sandbox_id,
       team_id,
       argMaxIf(value, timestamp, metric_name = '%s')  AS cpu_total,
       argMaxIf(value, timestamp, metric_name = '%s')  AS cpu_used,
       argMaxIf(value, timestamp, metric_name = '%s')  AS ram_total,
       argMaxIf(value, timestamp, metric_name = '%s')  AS ram_used,
       argMaxIf(value, timestamp, metric_name = '%s')  AS ram_cache,
       argMaxIf(value, timestamp, metric_name = '%s')  AS disk_total,
       argMaxIf(value, timestamp, metric_name = '%s')  AS disk_used,
       -- All metrics are recorded at the same time, so we can use max(timestamp) to get the latest one
       max(timestamp) as ts
FROM   sandbox_metrics_gauge
WHERE  sandbox_id IN ?
       AND team_id = ?
GROUP  BY sandbox_id,
          team_id; 
`, telemetry.SandboxCpuTotalGaugeName, telemetry.SandboxCpuUsedGaugeName, telemetry.SandboxRamTotalGaugeName, telemetry.SandboxRamUsedGaugeName, telemetry.SandboxRamCacheGaugeName, telemetry.SandboxDiskTotalGaugeName, telemetry.SandboxDiskUsedGaugeName)

// QueryLatestMetrics returns rows ordered by timestamp, paged by limit.
func (c *Client) QueryLatestMetrics(ctx context.Context, sandboxIDs []string, teamID string) ([]Metrics, error) {
	if len(sandboxIDs) == 0 {
		return make([]Metrics, 0), nil
	}

	rows, err := c.conn.Query(ctx, latestMetricsSelectQuery,
		sandboxIDs,
		teamID,
	)
	if err != nil {
		return nil, fmt.Errorf("query metrics: %w", err)
	}
	defer rows.Close()

	var out []Metrics
	for rows.Next() {
		var m Metrics
		if err := rows.ScanStruct(&m); err != nil {
			return nil, fmt.Errorf("error scaning metrics: %w", err)
		}
		out = append(out, m)
	}

	return out, rows.Err()
}

const sandboxMetricsTimeRangeSelectQuery = `
SELECT Min(timestamp) AS start_time,
       Max(timestamp) AS end_time
FROM   sandbox_metrics_gauge m
WHERE  sandbox_id = {sandbox_id:String}
       AND team_id = {team_id:String};
`

var sandboxMetricsSelectQuery = fmt.Sprintf(`
SELECT   toStartOfInterval(timestamp, interval {step:UInt32} second) AS ts,
         maxIf(value, metric_name = '%s')         					 AS cpu_total,
         maxIf(value, metric_name = '%s')				          	 AS cpu_used,
         maxIf(value, metric_name = '%s')         					 AS ram_total,
         maxIf(value, metric_name = '%s')          					 AS ram_used,
         maxIf(value, metric_name = '%s')          					 AS ram_cache,
         maxIf(value, metric_name = '%s')        					 AS disk_total,
         maxIf(value, metric_name = '%s')         					 AS disk_used
FROM     sandbox_metrics_gauge s
WHERE    sandbox_id = {sandbox_id:String}
AND      team_id = {team_id:String}
AND      timestamp >= fromUnixTimestamp64Nano({start_time:Int64})
AND      timestamp <= fromUnixTimestamp64Nano({end_time:Int64})
GROUP BY ts
ORDER BY ts;
`, telemetry.SandboxCpuTotalGaugeName, telemetry.SandboxCpuUsedGaugeName, telemetry.SandboxRamTotalGaugeName, telemetry.SandboxRamUsedGaugeName, telemetry.SandboxRamCacheGaugeName, telemetry.SandboxDiskTotalGaugeName, telemetry.SandboxDiskUsedGaugeName)

func (c *Client) QuerySandboxTimeRange(ctx context.Context, sandboxID string, teamID string) (time.Time, time.Time, error) {
	var start, end time.Time

	err := c.conn.QueryRow(ctx, sandboxMetricsTimeRangeSelectQuery,
		clickhouse.Named("sandbox_id", sandboxID),
		clickhouse.Named("team_id", teamID),
	).Scan(&start, &end)
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("query time range: %w", err)
	}

	return start, end, nil
}

func (c *Client) QuerySandboxMetrics(ctx context.Context, sandboxID string, teamID string, start time.Time, end time.Time, step time.Duration) ([]Metrics, error) {
	rows, err := c.conn.Query(ctx, sandboxMetricsSelectQuery,
		clickhouse.Named("sandbox_id", sandboxID),
		clickhouse.Named("team_id", teamID),
		clickhouse.Named("start_time", timestamp.UnixNano(start)),
		clickhouse.Named("end_time", timestamp.UnixNano(end)),
		clickhouse.Named("step", strconv.Itoa(int(step.Seconds()))),
	)
	if err != nil {
		return nil, fmt.Errorf("query metrics: %w", err)
	}

	defer rows.Close()
	var out []Metrics
	for rows.Next() {
		var m Metrics
		if err := rows.ScanStruct(&m); err != nil {
			return nil, fmt.Errorf("error scaning metrics: %w", err)
		}
		out = append(out, m)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over metrics rows: %w", err)
	}

	return out, nil
}
