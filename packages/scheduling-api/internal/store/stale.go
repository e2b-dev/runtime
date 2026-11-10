package store

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	queries "github.com/e2b-dev/infra/packages/scheduling-api/internal/db/queries/gen"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

const staleTransitionAfter = 95 * time.Second

type StaleTransition struct {
	ProjectID    uuid.UUID
	ActorID      uuid.UUID
	TransitionID uuid.UUID
	Action       TransitionAction
	TransitionAt time.Time
	ExecutionID  string
	NodeID       string
	ClusterID    uuid.UUID
}

// StaleCursor is the position after which ListStaleTransitions continues.
// The zero value starts at the oldest transition.
type StaleCursor struct {
	TransitionAt time.Time
	ActorID      uuid.UUID
}

// ListStaleTransitions returns up to limit stale pauses and kills of all
// projects, oldest first, after the position after. The list is not a
// snapshot: transitions that end, are claimed or begin between pages can be
// missed or listed again.
func (s *Store) ListStaleTransitions(ctx context.Context, after StaleCursor, limit int32) ([]StaleTransition, error) {
	return telemetry.Observe1(ctx, tracer, "store.ListStaleTransitions", func(ctx context.Context) ([]StaleTransition, error) {
		if limit <= 0 {
			return nil, fmt.Errorf("list stale transitions: limit must be positive, got %d", limit)
		}
		rows, err := s.reads.ListStaleTransitions(ctx, queries.ListStaleTransitionsParams{
			StaleAfter:        interval(staleTransitionAfter),
			AfterTransitionAt: after.TransitionAt,
			AfterActorID:      after.ActorID,
			MaxRows:           limit,
		})
		if err != nil {
			return nil, fmt.Errorf("list stale transitions: %w", err)
		}

		transitions := make([]StaleTransition, 0, len(rows))
		for _, row := range rows {
			transitions = append(transitions, StaleTransition{
				ProjectID:    row.ProjectID,
				ActorID:      row.ActorID,
				TransitionID: row.TransitionID,
				Action:       TransitionAction(row.TransitionAction),
				TransitionAt: row.TransitionAt,
				ExecutionID:  row.ExecutionID,
				NodeID:       row.NodeID,
				ClusterID:    row.ClusterID,
			})
		}

		return transitions, nil
	})
}
