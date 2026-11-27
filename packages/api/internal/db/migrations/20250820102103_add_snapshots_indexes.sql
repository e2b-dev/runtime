-- +goose NO TRANSACTION
-- +goose Up
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_snapshots_time_id
  ON public.snapshots (sandbox_started_at DESC, sandbox_id);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_env_builds_env_status_created
    ON public.env_builds (env_id, status, created_at DESC);
-- Redundant with (env_id, status, created_at DESC)
DROP INDEX CONCURRENTLY IF EXISTS idx_envs_builds_envs;

-- +goose Down
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_envs_builds_envs
  ON public.env_builds (env_id);

DROP INDEX CONCURRENTLY IF EXISTS idx_env_builds_env_status_created;
DROP INDEX CONCURRENTLY IF EXISTS idx_snapshots_time_id;
