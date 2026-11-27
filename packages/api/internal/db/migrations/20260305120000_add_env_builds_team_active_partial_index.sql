-- +goose Up
-- +goose NO TRANSACTION

-- Partial index for efficiently finding active (pending/in_progress) builds per team.
-- Used by GetInProgressTemplateBuildsByTeam (get_inprogress_builds.sql).
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_env_builds_team_active
    ON env_builds (team_id)
    WHERE status_group IN ('pending', 'in_progress');

-- +goose Down
-- +goose NO TRANSACTION

DROP INDEX CONCURRENTLY IF EXISTS idx_env_builds_team_active;
