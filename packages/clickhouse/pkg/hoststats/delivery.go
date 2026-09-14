package hoststats

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/clickhouse/pkg/batcher"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

const InsertSandboxHostStatQuery = `INSERT INTO sandbox_host_stats
(
    timestamp,
    sandbox_id,
    sandbox_execution_id,
    sandbox_template_id,
    sandbox_build_id,
    sandbox_team_id,
    sandbox_vcpu_count,
    sandbox_memory_mb,
    cgroup_cpu_usage_usec,
    cgroup_cpu_user_usec,
    cgroup_cpu_system_usec,
    cgroup_memory_usage_bytes,
    cgroup_memory_peak_bytes,
    delta_cgroup_cpu_usage_usec,
    delta_cgroup_cpu_user_usec,
    delta_cgroup_cpu_system_usec,
    interval_us,
    sandbox_type
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

type ClickhouseDelivery struct {
	batcher *batcher.Batcher[SandboxHostStat]
	conn    driver.Conn
	ff      *featureflags.Client
}

type GatedClickhouseDelivery struct {
	*ClickhouseDelivery

	ff *featureflags.Client
}

var tracer = otel.Tracer("github.com/e2b-dev/infra/packages/clickhouse/pkg/hoststats")

const DefaultBatcherName = "sandbox-host-stats"

func NewDefaultClickhouseHostStatsDelivery(
	ctx context.Context,
	conn driver.Conn,
	featureFlags *featureflags.Client,
	batcherName string,
) (*ClickhouseDelivery, error) {
	maxBatchSize := featureFlags.IntFlag(ctx, featureflags.ClickhouseBatcherMaxBatchSize)
	maxDelay := time.Duration(featureFlags.IntFlag(ctx, featureflags.ClickhouseBatcherMaxDelay)) * time.Millisecond
	batcherQueueSize := featureFlags.IntFlag(ctx, featureflags.ClickhouseBatcherQueueSize)

	return NewClickhouseHostStatsDelivery(
		ctx, conn, batcher.BatcherOptions{
			Name:         batcherName,
			MaxBatchSize: maxBatchSize,
			MaxDelay:     maxDelay,
			QueueSize:    batcherQueueSize,
			ErrorHandler: func(err error) {
				logger.L().Error(ctx, "error batching sandbox host stats", zap.Error(err))
			},
		},
		featureFlags,
	)
}

func NewGatedDelivery(inner *ClickhouseDelivery, featureFlags *featureflags.Client) *GatedClickhouseDelivery {
	return &GatedClickhouseDelivery{ClickhouseDelivery: inner, ff: featureFlags}
}

func NewClickhouseHostStatsDelivery(
	ctx context.Context,
	conn driver.Conn,
	opts batcher.BatcherOptions,
	featureFlags *featureflags.Client,
) (*ClickhouseDelivery, error) {
	delivery := &ClickhouseDelivery{conn: conn, ff: featureFlags}

	var err error
	delivery.batcher, err = batcher.NewBatcher(delivery.batchInserter, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to create batcher: %w", err)
	}

	if err = delivery.batcher.Start(ctx); err != nil {
		return nil, fmt.Errorf("failed to start batcher: %w", err)
	}

	return delivery, nil
}

func (c *ClickhouseDelivery) Push(stat SandboxHostStat) error {
	return c.batcher.Push(stat)
}

func (c *GatedClickhouseDelivery) Push(stat SandboxHostStat) error {
	if c.ff != nil && c.ff.BoolFlag(context.Background(), featureflags.ClickhouseWriteFanoutFlag) {
		return c.ClickhouseDelivery.Push(stat)
	}

	return nil
}

// Close drains the batcher. ctx is ignored to avoid leaking the flush goroutine.
func (c *ClickhouseDelivery) Close(_ context.Context) error {
	return c.batcher.Stop()
}

// insertSettings returns the per-query ClickHouse settings for one flush, or
// nil when the server defaults apply.
func (c *ClickhouseDelivery) insertSettings(ctx context.Context) clickhouse.Settings {
	if c.ff == nil || !c.ff.BoolFlag(ctx, featureflags.ClickhouseHostStatsAsyncInsertFlag) {
		return nil
	}

	return clickhouse.Settings{"async_insert": 1}
}

func (c *ClickhouseDelivery) batchInserter(ctx context.Context, stats []SandboxHostStat) error {
	attrs := trace.WithAttributes(attribute.Int("batch.size", len(stats)))
	ctx, span := tracer.Start(ctx, "Flush host stats batch to Clickhouse", attrs)
	defer span.End()

	if settings := c.insertSettings(ctx); settings != nil {
		ctx = clickhouse.Context(ctx, clickhouse.WithSettings(settings))
	}

	batch, err := c.conn.PrepareBatch(ctx, InsertSandboxHostStatQuery, driver.WithReleaseConnection())
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "prepare batch failed")

		return fmt.Errorf("error preparing batch: %w", err)
	}
	defer batch.Close()

	for _, stat := range stats {
		err := batch.Append(
			stat.Timestamp,
			stat.SandboxID,
			stat.SandboxExecutionID,
			stat.SandboxTemplateID,
			stat.SandboxBuildID,
			stat.SandboxTeamID,
			stat.SandboxVCPUCount,
			stat.SandboxMemoryMB,
			stat.CgroupCPUUsageUsec,
			stat.CgroupCPUUserUsec,
			stat.CgroupCPUSystemUsec,
			stat.CgroupMemoryUsage,
			stat.CgroupMemoryPeak,
			stat.DeltaCgroupCPUUsageUsec,
			stat.DeltaCgroupCPUUserUsec,
			stat.DeltaCgroupCPUSystemUsec,
			stat.IntervalUs,
			stat.SandboxType,
		)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "append failed")

			return fmt.Errorf("error appending %d host stat to batch: %w", len(stats), err)
		}
	}

	if err = batch.Send(); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "send failed")

		return fmt.Errorf("error sending %d host stats batch: %w", len(stats), err)
	}

	return nil
}
