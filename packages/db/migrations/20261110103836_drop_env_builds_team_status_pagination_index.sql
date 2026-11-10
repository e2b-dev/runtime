-- +goose NO TRANSACTION
-- +goose Up

-- idx_env_builds_team_created_id serves the team build list on the same keys,
-- without the INCLUDE (status, status_group) copies this index carries and
-- nothing reads. Keep this one unless that index is valid. Dropped here,
-- concurrently, it also stays out of the env_builds.status drop, which would
-- take it inside that statement's exclusive lock.
-- +goose StatementBegin
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT FROM pg_catalog.pg_index
        WHERE indexrelid = to_regclass('public.idx_env_builds_team_created_id') AND indisvalid
    ) THEN
        RAISE EXCEPTION 'public.idx_env_builds_team_created_id is missing or not valid; build it with the statement in 20261110103628 before dropping the index it replaces';
    END IF;
END $$;
-- +goose StatementEnd

-- DROP waits on in-flight lock holders rather than building anything; bound it
-- so it neither hangs unbounded nor dies to a short session default.
SET statement_timeout = '1h';

DROP INDEX CONCURRENTLY IF EXISTS public.idx_env_builds_team_status_pagination;

-- The migrator session is reused for subsequent migrations: restore its baseline.
SET statement_timeout = '3h';

-- +goose Down

-- The rebuild runs for a long time at this table's size; rolling back is a
-- deliberate manual operation with an operator present, so it is unbounded on
-- purpose.
SET statement_timeout = 0;

-- An interrupted rebuild leaves an INVALID index that IF NOT EXISTS would then
-- skip forever: clear any leftover first.
DROP INDEX CONCURRENTLY IF EXISTS public.idx_env_builds_team_status_pagination;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_env_builds_team_status_pagination
    ON public.env_builds (team_id, created_at DESC, id DESC) INCLUDE (status, status_group);

SET statement_timeout = '3h';
