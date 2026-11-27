-- +goose NO TRANSACTION
-- +goose Up
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_snapshots_env_id
    ON public.snapshots (env_id);

-- +goose Down
DROP INDEX CONCURRENTLY IF EXISTS idx_snapshots_env_id;
