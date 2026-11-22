-- +goose Up
-- +goose NO TRANSACTION

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_env_builds_team_status_group
  ON public.env_builds (team_id, status_group);

-- +goose Down
-- +goose NO TRANSACTION

DROP INDEX CONCURRENTLY IF EXISTS idx_env_builds_team_status_group;
