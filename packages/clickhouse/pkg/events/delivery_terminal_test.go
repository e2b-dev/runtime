package events

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/launchdarkly/go-sdk-common/v3/ldvalue"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/clickhouse/pkg/batcher"
	sharedevents "github.com/e2b-dev/infra/packages/shared/pkg/events"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
)

func TestIsTerminalEvent(t *testing.T) {
	t.Parallel()

	require.True(t, isTerminalEvent(sharedevents.SandboxKilledEvent), "killed should be terminal")

	// Scoped to killed for now: paused/checkpointed are recoverable and kept on
	// the non-blocking path (see isTerminalEvent doc).
	nonTerminal := []string{
		sharedevents.SandboxPausedEvent,
		sharedevents.SandboxCheckpointedEvent,
		sharedevents.SandboxCreatedEvent,
		sharedevents.SandboxResumedEvent,
		sharedevents.SandboxUpdatedEvent,
		"",
		"something.else",
	}
	for _, et := range nonTerminal {
		require.Falsef(t, isTerminalEvent(et), "%s should not be terminal", et)
	}
}

func TestTerminalPushTimeout(t *testing.T) {
	t.Parallel()

	require.Zero(t, terminalPushTimeout(t.Context(), nil), "nil flags disables blocking")

	ff, source := newTestFeatureFlags(t)
	source.Update(source.Flag(featureflags.ClickhouseBatcherTerminalPushTimeoutMs.Key()).ValueForAll(ldvalue.Int(0)))
	require.Zero(t, terminalPushTimeout(t.Context(), ff), "non-positive flag disables blocking")

	source.Update(source.Flag(featureflags.ClickhouseBatcherTerminalPushTimeoutMs.Key()).ValueForAll(ldvalue.Int(250)))
	require.Equal(t, 250*time.Millisecond, terminalPushTimeout(t.Context(), ff))
}

// On a full queue, a terminal event applies bounded back-pressure (blocks up to
// the configured timeout) while a non-terminal event drops immediately.
func TestClickhouseDelivery_TerminalBlocksNonTerminalDrops(t *testing.T) {
	t.Parallel()

	const timeout = 150 * time.Millisecond
	ff, source := newTestFeatureFlags(t)
	source.Update(source.Flag(featureflags.ClickhouseBatcherTerminalPushTimeoutMs.Key()).ValueForAll(ldvalue.Int(int(timeout.Milliseconds()))))

	release := make(chan struct{})
	entered := make(chan struct{})
	var once sync.Once
	b, err := batcher.NewBatcher(func(_ context.Context, _ []SandboxEvent) error {
		once.Do(func() {
			close(entered)
			<-release
		})

		return nil
	}, batcher.BatcherOptions{Name: "term-test", MaxDelay: time.Hour, MaxBatchSize: 1, QueueSize: 1})
	require.NoError(t, err)
	require.NoError(t, b.Start(t.Context()))
	defer func() { close(release); _ = b.Stop() }()

	d := &ClickhouseDelivery{batcher: b, ff: ff, batcherName: "term-test"}

	// Fill the pipeline: first event is pulled into Func (blocked), second fills
	// the cap-1 queue.
	require.NoError(t, d.Publish(t.Context(), "k", sharedevents.SandboxEvent{Type: sharedevents.SandboxCreatedEvent, SandboxID: "s1"}))
	<-entered
	require.NoError(t, d.Publish(t.Context(), "k", sharedevents.SandboxEvent{Type: sharedevents.SandboxCreatedEvent, SandboxID: "s2"}))

	// Non-terminal on a full queue: dropped immediately.
	start := time.Now()
	err = d.Publish(t.Context(), "k", sharedevents.SandboxEvent{Type: sharedevents.SandboxCreatedEvent, SandboxID: "s3"})
	require.ErrorIs(t, err, batcher.ErrBatcherQueueFull)
	require.Less(t, time.Since(start), timeout/2, "non-terminal event must not block")

	// Terminal on a still-full queue: blocks ~timeout, then drops.
	start = time.Now()
	err = d.Publish(t.Context(), "k", sharedevents.SandboxEvent{Type: sharedevents.SandboxKilledEvent, SandboxID: "s4"})
	require.ErrorIs(t, err, batcher.ErrBatcherQueueFull)
	require.GreaterOrEqual(t, time.Since(start), timeout*3/4, "terminal event must apply bounded back-pressure before dropping")
}
