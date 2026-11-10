package clickhouse

import (
	"context"
	"errors"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/google/uuid"

	"github.com/e2b-dev/infra/packages/shared/pkg/events"
)

var ErrSandboxNotFound = errors.New("sandbox not found")

// SandboxLifecycle is a sandbox's team and the type of its latest event that
// changes its state: created, resumed, paused or killed.
type SandboxLifecycle struct {
	TeamID    uuid.UUID
	EventType string
}

// stateEventTypes leaves out updated and checkpointed events, which a running
// sandbox emits without changing state.
var stateEventTypes = []string{
	events.SandboxCreatedEvent,
	events.SandboxResumedEvent,
	events.SandboxPausedEvent,
	events.SandboxKilledEvent,
}

// sandbox_events is ordered by (sandbox_id, timestamp), so this reads one key
// range.
const sandboxLifecycleSelectQuery = `
SELECT sandbox_team_id, type
FROM sandbox_events
WHERE sandbox_id = {sandbox_id:String}
  AND type IN {types:Array(String)}
ORDER BY timestamp DESC
LIMIT 1
`

// QuerySandboxLifecycle returns ErrSandboxNotFound once the sandbox's events
// have expired.
func (c *Client) QuerySandboxLifecycle(ctx context.Context, sandboxID string) (SandboxLifecycle, error) {
	rows, err := c.conn.Query(ctx, sandboxLifecycleSelectQuery,
		clickhouse.Named("sandbox_id", sandboxID),
		clickhouse.Named("types", stateEventTypes),
	)
	if err != nil {
		return SandboxLifecycle{}, fmt.Errorf("query sandbox lifecycle: %w", err)
	}
	defer rows.Close()

	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return SandboxLifecycle{}, fmt.Errorf("query sandbox lifecycle: %w", err)
		}

		return SandboxLifecycle{}, fmt.Errorf("sandbox %q: %w", sandboxID, ErrSandboxNotFound)
	}

	var lifecycle SandboxLifecycle
	if err := rows.Scan(&lifecycle.TeamID, &lifecycle.EventType); err != nil {
		return SandboxLifecycle{}, fmt.Errorf("scan sandbox lifecycle: %w", err)
	}

	return lifecycle, nil
}
