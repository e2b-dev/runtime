-- +goose NO TRANSACTION
-- +goose Up
CREATE INDEX CONCURRENTLY IF NOT EXISTS snapshots_base_env_id_idx
    ON public.snapshots (base_env_id);

-- +goose Down
DROP INDEX CONCURRENTLY IF EXISTS snapshots_base_env_id_idx;
