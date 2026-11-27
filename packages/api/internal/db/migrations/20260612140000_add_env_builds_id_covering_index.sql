-- +goose Up
-- +goose NO TRANSACTION
-- Makes build lookups in dashboard template tag queries index-only.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_env_builds_id_covering
    ON public.env_builds (id) INCLUDE (status_group, created_at, finished_at);

-- +goose Down
-- +goose NO TRANSACTION
DROP INDEX CONCURRENTLY IF EXISTS idx_env_builds_id_covering;
