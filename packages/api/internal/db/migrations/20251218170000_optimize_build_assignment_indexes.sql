-- +goose Up
-- +goose NO TRANSACTION

-- Add index on build_id for efficient lookups by build
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_env_build_assignments_build
    ON env_build_assignments (build_id);

-- Drop the composite (env_id, build_id) index as it's now redundant:
-- - env_id lookups are covered by idx_env_build_assignments_env_tag_created (env_id, tag, created_at DESC)
-- - build_id lookups are covered by the new idx_env_build_assignments_build
DROP INDEX CONCURRENTLY IF EXISTS idx_env_build_assignments_env_build;

-- +goose Down
-- Restore the original composite index
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_env_build_assignments_env_build
    ON env_build_assignments (env_id, build_id);

-- Drop the build_id index
DROP INDEX CONCURRENTLY IF EXISTS idx_env_build_assignments_build;

