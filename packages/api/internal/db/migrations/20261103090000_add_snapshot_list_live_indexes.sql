-- +goose NO TRANSACTION
-- +goose Up

-- Each concurrent build can legitimately run long at this table's size, and a
-- mid-build timeout would strand an INVALID index and leave the migration
-- unrecorded. The migrator connection carries a 3h default
-- (scripts/migrator.go), so lift the bound for the builds.
SET statement_timeout = 0;

-- Partial copies of the three indexes behind the sandbox list, holding only
-- snapshots whose env is live (env_deleted_at IS NULL), in the same columns,
-- order and direction as the full ones. The list queries filter on the same
-- predicate, so they walk live rows only instead of reading and discarding the
-- snapshots of deleted envs.
--
-- Run scripts/backfill_snapshots_env_deleted_at.sql to completion first, after
-- every service runs the release that stamps snapshots, so this ships in a
-- later release than 20261027070100.
-- A row stamped after these are built leaves its entry behind until vacuum,
-- and the freed space stays in the index, so a later backfill leaves them near
-- full size until each is rebuilt with REINDEX INDEX CONCURRENTLY.
--
-- Each build scans the whole table twice, and services that check the schema
-- version wait for all three. On a large table, build them beforehand with the
-- CREATE statements below: a valid index of the same name is kept, and only
-- what is missing is built here.
--
-- With env_deleted_at in these predicates, stamping it is never a heap-only
-- update: each stamped row gets a new entry in every index on snapshots that
-- has no predicate. The full copies stay until the queries have run on these
-- for a while; a later migration drops them.
--
-- An interrupted CONCURRENTLY build leaves an INVALID index that IF NOT EXISTS
-- would then skip forever. DROP INDEX CONCURRENTLY cannot run in a DO block,
-- so an INVALID leftover is renamed aside here and dropped below, while a
-- valid index keeps its name and is not rebuilt. One set aside by an earlier
-- run that did not finish is dropped first, so the rename finds the name free.
-- A build still running by hand is INVALID too, so while an index build runs in
-- this database the migration stops instead of dropping it. Dropping a leftover
-- waits on every index build on snapshots, so with one present that check runs
-- first, rather than wait out a build of hours with statement_timeout lifted.
-- +goose StatementBegin
DO $$
DECLARE
    builder integer;
BEGIN
    IF EXISTS (
        SELECT FROM pg_catalog.pg_class
        WHERE relnamespace = 'public'::regnamespace AND relname IN (
            'idx_snapshots_team_time_id_live_invalid',
            'idx_snapshots_team_base_env_time_id_live_invalid',
            'idx_snapshots_team_metadata_gin_live_invalid'
        )
    ) THEN
        -- relid reads NULL for another role's build without pg_read_all_stats.
        SELECT pid INTO builder FROM pg_catalog.pg_stat_progress_create_index
        WHERE datname = current_database()
            AND (relid = 'public.snapshots'::regclass OR relid IS NULL)
        LIMIT 1;
        IF FOUND THEN
            RAISE EXCEPTION 'backend % is building an index on public.snapshots, which dropping a set-aside index would wait on; rerun once that build finishes or is cancelled', builder;
        END IF;
    END IF;
END $$;
-- +goose StatementEnd

DROP INDEX CONCURRENTLY IF EXISTS public.idx_snapshots_team_time_id_live_invalid;
DROP INDEX CONCURRENTLY IF EXISTS public.idx_snapshots_team_base_env_time_id_live_invalid;
DROP INDEX CONCURRENTLY IF EXISTS public.idx_snapshots_team_metadata_gin_live_invalid;

-- +goose StatementBegin
DO $$
DECLARE
    name text;
    builder integer;
BEGIN
    FOREACH name IN ARRAY ARRAY[
        'idx_snapshots_team_time_id_live',
        'idx_snapshots_team_base_env_time_id_live',
        'idx_snapshots_team_metadata_gin_live'
    ] LOOP
        IF EXISTS (
            SELECT FROM pg_catalog.pg_index
            WHERE indexrelid = to_regclass('public.' || name) AND NOT indisvalid
        ) THEN
            -- index_relid reads NULL for another role's build without pg_read_all_stats.
            SELECT pid INTO builder FROM pg_catalog.pg_stat_progress_create_index
            WHERE datname = current_database()
                AND (index_relid = to_regclass('public.' || name) OR index_relid IS NULL)
            LIMIT 1;
            IF FOUND THEN
                RAISE EXCEPTION 'public.% is not valid yet and backend % is building an index; rerun once that build finishes or is cancelled', name, builder;
            END IF;
            EXECUTE format('ALTER INDEX public.%I RENAME TO %I', name, name || '_invalid');
        END IF;
    END LOOP;
END $$;
-- +goose StatementEnd

DROP INDEX CONCURRENTLY IF EXISTS public.idx_snapshots_team_time_id_live_invalid;
DROP INDEX CONCURRENTLY IF EXISTS public.idx_snapshots_team_base_env_time_id_live_invalid;
DROP INDEX CONCURRENTLY IF EXISTS public.idx_snapshots_team_metadata_gin_live_invalid;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_snapshots_team_time_id_live
    ON public.snapshots (team_id, sandbox_started_at DESC, sandbox_id)
    WHERE env_deleted_at IS NULL;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_snapshots_team_base_env_time_id_live
    ON public.snapshots (
        team_id,
        base_env_id,
        sandbox_started_at DESC,
        sandbox_id ASC
    )
    WHERE env_deleted_at IS NULL;

-- team_id in a GIN index needs btree_gin, which the full index's migration
-- installed.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_snapshots_team_metadata_gin_live
    ON public.snapshots USING gin (team_id, metadata)
    WHERE env_deleted_at IS NULL;

-- The migrator session is reused for subsequent migrations: restore its baseline.
SET statement_timeout = '3h';

-- +goose Down

-- DROP waits on in-flight lock holders rather than building anything; bound it so
-- it neither hangs unbounded nor dies to a short session default.
SET statement_timeout = '1h';

DROP INDEX CONCURRENTLY IF EXISTS public.idx_snapshots_team_metadata_gin_live;
DROP INDEX CONCURRENTLY IF EXISTS public.idx_snapshots_team_base_env_time_id_live;
DROP INDEX CONCURRENTLY IF EXISTS public.idx_snapshots_team_time_id_live;

SET statement_timeout = '3h';
