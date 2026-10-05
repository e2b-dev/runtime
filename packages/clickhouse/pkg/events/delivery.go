package events

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	chconfig "github.com/e2b-dev/infra/packages/clickhouse/pkg"
	"github.com/e2b-dev/infra/packages/clickhouse/pkg/batcher"
	"github.com/e2b-dev/infra/packages/shared/pkg/events"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

const InsertSandboxEventQuery = `INSERT INTO sandbox_events
(
    timestamp,
    sandbox_id,
    sandbox_execution_id,
    sandbox_template_id,
    sandbox_build_id,
    sandbox_team_id,
    event_data,
    type,
    version,
    id,
    events_ttl_days
)
VALUES (
    ?,
    ?,
    ?,
    ?,
    ?,
    ?,
    ?,
    ?,
    ?,
    ?,
    ?
)`

type ClickhouseDelivery struct {
	batcher     *batcher.Batcher[SandboxEvent]
	conn        driver.Conn
	ff          *featureflags.Client
	batcherName string
}

type GatedClickhouseDelivery struct {
	*ClickhouseDelivery

	ff *featureflags.Client
}

var tracer = otel.Tracer("github.com/e2b-dev/infra/packages/clickhouse/pkg/events")

const DefaultBatcherName = "sandbox-events"

func NewDefaultClickhouseSandboxEventsDelivery(ctx context.Context, conn driver.Conn, featureFlags *featureflags.Client, batcherName string) (*ClickhouseDelivery, error) {
	batcherQueueSize := featureFlags.IntFlag(ctx, featureflags.ClickhouseBatcherQueueSize, featureflags.BatcherContext(batcherName))

	return NewClickhouseSandboxEventsDelivery(
		ctx, conn, batcher.BatcherOptions{
			Name:         batcherName,
			FeatureFlags: featureFlags,
			QueueSize:    batcherQueueSize,
			ErrorHandler: func(err error) {
				logger.L().Error(ctx, "error batching sandbox events", zap.Error(err))
			},
		},
	)
}

func NewGatedDelivery(inner *ClickhouseDelivery, featureFlags *featureflags.Client) *GatedClickhouseDelivery {
	return &GatedClickhouseDelivery{ClickhouseDelivery: inner, ff: featureFlags}
}

func NewClickhouseSandboxEventsDelivery(ctx context.Context, conn driver.Conn, opts batcher.BatcherOptions) (*ClickhouseDelivery, error) {
	var err error

	delivery := &ClickhouseDelivery{conn: conn, ff: opts.FeatureFlags, batcherName: opts.Name}
	delivery.batcher, err = batcher.NewBatcher(delivery.batchInserter, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to create batcher: %w", err)
	}

	if err = delivery.batcher.Start(ctx); err != nil {
		return nil, fmt.Errorf("failed to start batcher: %w", err)
	}

	return delivery, nil
}

func (c *ClickhouseDelivery) Publish(_ context.Context, _ string, event events.SandboxEvent) error {
	eventDataJson, err := json.Marshal(event.EventData)
	if err != nil {
		return err
	}

	eventData := string(eventDataJson)

	ttlDays := event.EventsTTLDays
	if ttlDays <= 0 {
		ttlDays = events.DefaultEventsTTLDays
	}
	if ttlDays > events.MaxEventsTTLDays {
		ttlDays = events.MaxEventsTTLDays
	}

	return c.batcher.Push(SandboxEvent{
		Version:   event.Version,
		ID:        event.ID,
		Type:      event.Type,
		Timestamp: event.Timestamp,

		EventData:          sql.NullString{String: eventData, Valid: eventData != ""},
		SandboxID:          event.SandboxID,
		SandboxTemplateID:  event.SandboxTemplateID,
		SandboxBuildID:     event.SandboxBuildID,
		SandboxTeamID:      event.SandboxTeamID,
		SandboxExecutionID: event.SandboxExecutionID,
		EventsTTLDays:      ttlDays,
	})
}

func (c *GatedClickhouseDelivery) Publish(ctx context.Context, key string, event events.SandboxEvent) error {
	if c.ff != nil && c.ff.BoolFlag(ctx, featureflags.ClickhouseWriteFanoutFlag) {
		return c.ClickhouseDelivery.Publish(ctx, key, event)
	}

	return nil
}

// Close drains the batcher. ctx is ignored to avoid leaking the flush goroutine.
func (c *ClickhouseDelivery) Close(_ context.Context) error {
	return c.batcher.Stop()
}

func (c *ClickhouseDelivery) batchInserter(ctx context.Context, events []SandboxEvent) error {
	attr := trace.WithAttributes(attribute.Int("batch.size", len(events)))
	ctx, span := tracer.Start(ctx, "Flush sandbox events batch to Clickhouse", attr)
	defer span.End()

	if settings := chconfig.InsertSettings(ctx, c.ff, c.batcherName, nil); settings != nil {
		ctx = clickhouse.Context(ctx, clickhouse.WithSettings(settings))
	}

	batch, err := c.conn.PrepareBatch(ctx, InsertSandboxEventQuery, driver.WithReleaseConnection())
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "prepare batch failed")

		return fmt.Errorf("error preparing batch: %w", err)
	}
	defer batch.Close()

	for _, event := range events {
		err := batch.Append(
			event.Timestamp,
			event.SandboxID,
			event.SandboxExecutionID,
			event.SandboxTemplateID,
			event.SandboxBuildID,
			event.SandboxTeamID,
			event.EventData,
			event.Type,
			event.Version,
			event.ID,
			event.EventsTTLDays,
		)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "append failed")

			return fmt.Errorf("error appending %d event to batch: %w", len(events), err)
		}
	}

	if err = batch.Send(); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "send failed")

		return fmt.Errorf("error sending %d events batch: %w", len(events), err)
	}

	return nil
}
