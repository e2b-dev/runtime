-- +goose NO TRANSACTION
-- +goose Up

-- The concurrent build can legitimately run long at this table's size, and a
-- mid-build timeout would strand an INVALID index and leave the migration
-- unrecorded. The migrator connection carries a 3h default
-- (scripts/migrator.go), so lift the bound for the build.
SET statement_timeout = 0;

-- The expiry sweep finds live paused sandboxes whose expires_at has passed,
-- oldest first. Only rows with a TTL carry an expiry, so the index holds those
-- alone and stays small.
--
-- The build scans the whole table twice, and services that check the schema
-- version wait for it. On a large table, build it beforehand with the CREATE
-- statement below: a valid index of the same name is kept.
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
    IF to_regclass('public.idx_snapshots_expires_at_invalid') IS NOT NULL THEN
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

DROP INDEX CONCURRENTLY IF EXISTS public.idx_snapshots_expires_at_invalid;

-- +goose StatementBegin
DO $$
DECLARE
    builder integer;
BEGIN
    IF EXISTS (
        SELECT FROM pg_catalog.pg_index
        WHERE indexrelid = to_regclass('public.idx_snapshots_expires_at') AND NOT indisvalid
    ) THEN
        -- index_relid reads NULL for another role's build without pg_read_all_stats.
        SELECT pid INTO builder FROM pg_catalog.pg_stat_progress_create_index
        WHERE datname = current_database()
            AND (index_relid = to_regclass('public.idx_snapshots_expires_at') OR index_relid IS NULL)
        LIMIT 1;
        IF FOUND THEN
            RAISE EXCEPTION 'public.idx_snapshots_expires_at is not valid yet and backend % is building an index; rerun once that build finishes or is cancelled', builder;
        END IF;
        ALTER INDEX public.idx_snapshots_expires_at RENAME TO idx_snapshots_expires_at_invalid;
    END IF;
END $$;
-- +goose StatementEnd

DROP INDEX CONCURRENTLY IF EXISTS public.idx_snapshots_expires_at_invalid;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_snapshots_expires_at
    ON public.snapshots (expires_at)
    WHERE env_deleted_at IS NULL AND expires_at IS NOT NULL;

-- A build started by hand that was still waiting for its table lock when the
-- checks above ran has no index yet. The CREATE then queues behind it, and if
-- that build fails it skips the INVALID leftover by name. Fail rather than
-- record the migration as applied; a rerun sets the leftover aside.
-- +goose StatementBegin
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT FROM pg_catalog.pg_index
        WHERE indexrelid = to_regclass('public.idx_snapshots_expires_at') AND indisvalid
    ) THEN
        RAISE EXCEPTION 'public.idx_snapshots_expires_at is not valid; rerun to replace it';
    END IF;
END $$;
-- +goose StatementEnd

-- The migrator session is reused for subsequent migrations: restore its baseline.
SET statement_timeout = '3h';

-- +goose Down

-- DROP waits on in-flight lock holders rather than building anything; bound it
-- so it neither hangs unbounded nor dies to a short session default.
SET statement_timeout = '1h';

DROP INDEX CONCURRENTLY IF EXISTS public.idx_snapshots_expires_at;

SET statement_timeout = '3h';
