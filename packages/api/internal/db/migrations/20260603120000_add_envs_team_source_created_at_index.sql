-- +goose Up
-- +goose NO TRANSACTION

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_envs_team_source_created_at
    ON "public"."envs" (team_id, source, created_at DESC, id DESC);

-- +goose Down
-- +goose NO TRANSACTION

DROP INDEX CONCURRENTLY IF EXISTS idx_envs_team_source_created_at;
