-- +goose Up
-- +goose NO TRANSACTION
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_env_builds_created_at ON env_builds (created_at DESC);

-- +goose Down
DROP INDEX CONCURRENTLY IF EXISTS idx_env_builds_created_at;
