-- +goose Up
-- +goose NO TRANSACTION

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_envs_team_updated_at_templates
    ON "public"."envs" (team_id, updated_at DESC, id DESC)
    WHERE source = 'template';

-- +goose Down
-- +goose NO TRANSACTION

DROP INDEX CONCURRENTLY IF EXISTS idx_envs_team_updated_at_templates;
