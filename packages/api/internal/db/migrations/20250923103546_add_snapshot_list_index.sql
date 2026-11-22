-- +goose NO TRANSACTION
-- +goose Up
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_snapshots_team_time_id
    ON public.snapshots (team_id, sandbox_started_at DESC, sandbox_id);
DROP INDEX CONCURRENTLY IF EXISTS idx_snapshots_time_id;

-- +goose Down
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_snapshots_time_id
    ON public.snapshots (sandbox_started_at DESC, sandbox_id);
DROP INDEX CONCURRENTLY IF EXISTS idx_snapshots_team_time_id;
