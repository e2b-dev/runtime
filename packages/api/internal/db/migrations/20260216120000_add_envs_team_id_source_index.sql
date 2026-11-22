-- +goose Up
-- +goose NO TRANSACTION

-- Composite index for queries that filter envs by team_id and source
-- (e.g. ListTeamSnapshotTemplates, GetTeamTemplates).
-- Supersedes the single-column idx_teams_envs (team_id) index.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_envs_team_id_source
    ON "public"."envs" (team_id, source);

DROP INDEX CONCURRENTLY IF EXISTS idx_teams_envs;

-- +goose Down
-- +goose NO TRANSACTION

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_teams_envs
    ON "public"."envs" (team_id);

DROP INDEX CONCURRENTLY IF EXISTS idx_envs_team_id_source;
