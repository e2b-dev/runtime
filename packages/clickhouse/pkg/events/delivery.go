package events

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	chconfig "github.com/e2b-dev/infra/packages/clickhouse/pkg"
	"github.com/e2b-dev/infra/packages/clickhouse/pkg/batcher"
	"github.com/e2b-dev/infra/packages/shared/pkg/events"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
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

var meter = otel.Meter("github.com/e2b-dev/infra/packages/clickhouse/pkg/events")

// mEventDropped counts sandbox lifecycle events dropped before reaching
// ClickHouse, broken out by event type and whether the type is terminal, so a
// loss of terminal (billing/audit) events is directly alertable rather than
// hidden inside the generic batcher.items.dropped counter.
var mEventDropped = utils.Must(meter.Int64Counter(
	"sandbox_events.delivery.dropped",
	metric.WithDescription("Sandbox lifecycle events dropped before reaching ClickHouse, by event type and terminality"),
	metric.WithUnit("{event}"),
))

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

func (c *ClickhouseDelivery) Publish(ctx context.Context, _ string, event events.SandboxEvent) error {
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

	row := SandboxEvent{
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
	}

	if err := c.enqueue(ctx, event.Type, row); err != nil {
		mEventDropped.Add(ctx, 1, metric.WithAttributes(
			attribute.String("event_type", event.Type),
			attribute.Bool("terminal", isTerminalEvent(event.Type)),
		))

		return err
	}

	return nil
}

// enqueue routes an event into the batcher. Terminal lifecycle events (sandbox
// killed/paused/checkpointed) are the terminal, billing/audit-relevant surfaces,
// so they use a bounded-blocking push that applies back-pressure instead of
// dropping the event the instant the queue is full. All other (high-volume,
// non-terminal) events keep the non-blocking push, so the hot path is unchanged.
// The publish runs in a TrackWork-guarded goroutine, so bounded blocking here
// does not block the Delete RPC and is awaited on graceful drain.
func (c *ClickhouseDelivery) enqueue(ctx context.Context, eventType string, row SandboxEvent) error {
	if !isTerminalEvent(eventType) {
		return c.batcher.Push(row)
	}

	timeout := terminalPushTimeout(ctx, c.ff)
	if timeout <= 0 {
		return c.batcher.Push(row)
	}

	pushCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), timeout)
	defer cancel()

	return c.batcher.PushWithContext(pushCtx, row)
}

// isTerminalEvent reports whether an event type is the terminal/irrecoverable
// lifecycle surface whose loss corrupts billing or audit. Scoped to killed —
// the event the sandbox cannot be running after and the one the incident in
// #3770 lost. paused/checkpointed also stop billing and may warrant the same
// treatment, but they are recoverable (a paused sandbox can resume) and can be
// higher volume, so they are intentionally left on the non-blocking path for
// now; widen this set (ideally behind a per-type flag) if they prove lossy too.
func isTerminalEvent(eventType string) bool {
	return eventType == events.SandboxKilledEvent
}

// terminalPushTimeout reads the bounded-blocking push timeout for terminal
// events. Non-positive disables blocking (legacy non-blocking drop).
func terminalPushTimeout(ctx context.Context, ff *featureflags.Client) time.Duration {
	if ff == nil {
		return 0
	}
	ms := ff.IntFlag(ctx, featureflags.ClickhouseBatcherTerminalPushTimeoutMs)
	if ms <= 0 {
		return 0
	}

	return time.Duration(ms) * time.Millisecond
}

func (c *GatedClickhouseDelivery) Publish(ctx context.Context, key string, event events.SandboxEvent) error {
	if c.ff != nil && c.ff.BoolFlag(ctx, featureflags.ClickhouseWriteFanoutFlag) {
		return c.ClickhouseDelivery.Publish(ctx, key, event)
	}

	return nil
}

// Close drains the batcher, bounding the final flush by the shutdown ctx and the
// drain-timeout flag so a ClickHouse stall during a graceful stop cannot outlast
// the scheduler's kill window. A non-positive timeout keeps the legacy unbounded
// drain. The flush goroutine is always awaited (never leaked); ctx only caps how
// long the last batch is retried before it is dropped.
func (c *ClickhouseDelivery) Close(ctx context.Context) error {
	timeout := drainTimeout(ctx, c.ff)
	if timeout <= 0 {
		// Unbounded drain, but still honor ctx: a forced shutdown (closeCtx
		// already cancelled) ends the drain immediately instead of hanging.
		return c.batcher.StopWithContext(ctx)
	}

	// On a forced shutdown closeCtx is already cancelled, so the drain ends
	// immediately instead of waiting the full budget; on a graceful stop the
	// timeout is the bound.
	drainCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	return c.batcher.StopWithContext(drainCtx)
}

// drainTimeout reads the shutdown drain budget. Non-positive disables bounding.
func drainTimeout(ctx context.Context, ff *featureflags.Client) time.Duration {
	if ff == nil {
		return 0
	}
	ms := ff.IntFlag(ctx, featureflags.ClickhouseBatcherDrainTimeoutMs)
	if ms <= 0 {
		return 0
	}

	return time.Duration(ms) * time.Millisecond
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
