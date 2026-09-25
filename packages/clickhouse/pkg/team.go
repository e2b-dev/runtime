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

type TeamMetrics struct {
	Timestamp           time.Time `ch:"ts"`
	SandboxStartedRate  float64   `ch:"started_sandboxes_rate"`
	ConcurrentSandboxes int64     `ch:"concurrent_sandboxes"`
}

var teamMetricsSelectQuery = fmt.Sprintf(`
WITH
  created AS (
    SELECT
      toStartOfInterval(timestamp, interval {step:UInt32} second) AS ts,
      sum(value) as created_sandboxes
    FROM team_metrics_sum
    WHERE metric_name = '%s'
      AND team_id = {team_id:String}
      AND timestamp BETWEEN fromUnixTimestamp64Nano({start_time:Int64}) AND fromUnixTimestamp64Nano({end_time:Int64})
	GROUP BY ts
  ),
  concurrent AS (
    SELECT
      toStartOfInterval(timestamp, interval {step:UInt32} second) AS ts,
      toInt64(max(value)) AS concurrent_sandboxes
    FROM team_metrics_gauge
    WHERE metric_name = '%s'
      AND team_id = {team_id:String}
      AND timestamp BETWEEN fromUnixTimestamp64Nano({start_time:Int64}) AND fromUnixTimestamp64Nano({end_time:Int64})
	GROUP BY ts
  ),
  all_ts AS (
    SELECT ts FROM created
    UNION DISTINCT
    SELECT ts FROM concurrent
  )
SELECT
  all_ts.ts AS ts,
  COALESCE(created_sandboxes / {step:UInt32}::Float32, 0.0) AS started_sandboxes_rate,
  COALESCE(concurrent_sandboxes, 0)                         AS concurrent_sandboxes
FROM all_ts
LEFT JOIN created cr      ON cr.ts = all_ts.ts
LEFT JOIN concurrent con ON con.ts = all_ts.ts
ORDER BY all_ts.ts ASC;
`, telemetry.TeamSandboxCreated, telemetry.TeamSandboxRunningGaugeName)

func (c *Client) QueryTeamMetrics(ctx context.Context, teamID string, start time.Time, end time.Time, step time.Duration) ([]TeamMetrics, error) {
	rows, err := c.conn.Query(ctx, teamMetricsSelectQuery,
		clickhouse.Named("team_id", teamID),
		clickhouse.Named("start_time", timestamp.UnixNano(start)),
		clickhouse.Named("end_time", timestamp.UnixNano(end)),
		clickhouse.Named("step", strconv.Itoa(int(step.Seconds()))),
	)
	if err != nil {
		return nil, fmt.Errorf("query team metrics: %w", err)
	}

	defer rows.Close()
	var out []TeamMetrics
	for rows.Next() {
		var m TeamMetrics
		if err := rows.ScanStruct(&m); err != nil {
			return nil, fmt.Errorf("error scanning team metrics: %w", err)
		}
		out = append(out, m)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over team metrics rows: %w", err)
	}

	return out, nil
}

type MaxTeamMetric struct {
	Timestamp time.Time `ch:"ts"`
	Value     float64   `ch:"max_value"`
}

var maxStartRateTeamMetricsSelectQuery = fmt.Sprintf(`
WITH 
	aggregated AS (
		SELECT 
			toStartOfInterval(timestamp, interval {step:UInt32} second) AS agg_ts,
  			sum(value) AS agg_value
	FROM team_metrics_sum
	WHERE metric_name = '%s'
	  AND team_id = {team_id:String}
	  AND timestamp BETWEEN fromUnixTimestamp64Nano({start_time:Int64}) AND fromUnixTimestamp64Nano({end_time:Int64})
	GROUP BY agg_ts
	)
SELECT
	argMax(agg_ts, agg_value) AS ts,
	max(agg_value) / {step:UInt32}::Float32 AS max_value
FROM aggregated
`, telemetry.TeamSandboxCreated)

func (c *Client) QueryMaxStartRateTeamMetrics(ctx context.Context, teamID string, start time.Time, end time.Time, step time.Duration) (MaxTeamMetric, error) {
	rows, err := c.conn.Query(ctx, maxStartRateTeamMetricsSelectQuery,
		clickhouse.Named("team_id", teamID),
		clickhouse.Named("step", strconv.Itoa(int(step.Seconds()))),
		clickhouse.Named("start_time", timestamp.UnixNano(start)),
		clickhouse.Named("end_time", timestamp.UnixNano(end)),
	)
	if err != nil {
		return MaxTeamMetric{}, fmt.Errorf("query max start rate team metrics: %w", err)
	}
	defer rows.Close()

	// No data -> return 0
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return MaxTeamMetric{}, fmt.Errorf("error iterating max start rate team metrics: %w", err)
		}

		return MaxTeamMetric{
			Value:     0,
			Timestamp: time.Now(),
		}, nil
	}

	var out MaxTeamMetric
	err = rows.Scan(&out.Timestamp, &out.Value)
	if err != nil {
		return MaxTeamMetric{}, fmt.Errorf("error scanning max start rate team metrics: %w", err)
	}

	return out, nil
}

var maxConcurrentTeamMetricsSelectQuery = fmt.Sprintf(`
SELECT
  argMax(timestamp, value) AS ts,
  max(value) as max_value
FROM team_metrics_gauge
WHERE metric_name = '%s'
  AND team_id = {team_id:String}
  AND timestamp BETWEEN fromUnixTimestamp64Nano({start_time:Int64}) AND fromUnixTimestamp64Nano({end_time:Int64});
`, telemetry.TeamSandboxRunningGaugeName)

func (c *Client) QueryMaxConcurrentTeamMetrics(ctx context.Context, teamID string, start time.Time, end time.Time) (MaxTeamMetric, error) {
	rows, err := c.conn.Query(ctx, maxConcurrentTeamMetricsSelectQuery,
		clickhouse.Named("team_id", teamID),
		clickhouse.Named("start_time", timestamp.UnixNano(start)),
		clickhouse.Named("end_time", timestamp.UnixNano(end)),
	)
	if err != nil {
		return MaxTeamMetric{}, fmt.Errorf("query max concurrent team metrics: %w", err)
	}

	defer rows.Close()

	// No data -> return 0
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return MaxTeamMetric{}, fmt.Errorf("error iterating max concurrent team metrics: %w", err)
		}

		return MaxTeamMetric{
			Value:     0,
			Timestamp: time.Now(),
		}, nil
	}

	var out MaxTeamMetric
	err = rows.Scan(&out.Timestamp, &out.Value)
	if err != nil {
		return MaxTeamMetric{}, fmt.Errorf("error scanning max concurrent team metrics: %w", err)
	}

	return out, nil
}
