package store

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	queries "github.com/e2b-dev/infra/packages/scheduling-api/internal/db/queries/gen"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

const tombstoneRetention = 5 * time.Minute

// PurgeTombstones deletes up to maxRows gone actors that are past
// tombstoneRetention and returns how many it deleted. A
// gone actor is not counted, so the counters do not change.
func (s *Store) PurgeTombstones(ctx context.Context, maxRows int32) (int64, error) {
	return telemetry.Observe1(ctx, tracer, "store.PurgeTombstones", func(ctx context.Context) (int64, error) {
		if maxRows <= 0 {
			return 0, fmt.Errorf("purge tombstones: maxRows must be positive, got %d", maxRows)
		}
		var deleted int64
		err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
			q := s.queries.WithTx(tx)
			if err := q.SetCleanupTimeouts(ctx); err != nil {
				return fmt.Errorf("set timeouts: %w", err)
			}
			if err := q.PurgeExpiredKillClaims(ctx, maxRows); err != nil {
				return fmt.Errorf("purge expired kill claims: %w", err)
			}
			var err error
			deleted, err = q.PurgeTombstones(ctx, queries.PurgeTombstonesParams{
				Retention: interval(tombstoneRetention),
				MaxRows:   maxRows,
			})

			return err
		})
		if err != nil {
			return 0, fmt.Errorf("purge tombstones: %w", err)
		}

		return deleted, nil
	})
}
