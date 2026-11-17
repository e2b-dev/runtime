-- +goose NO TRANSACTION
-- +goose Up

-- Concurrent builds can exceed the migrator's 3h timeout.
SET statement_timeout = 0;

-- Dropping an index waits for other index builds on the same table.
-- +goose StatementBegin
DO $$
DECLARE
    builder integer;
BEGIN
    IF to_regclass('public.idx_envs_active_id_invalid') IS NOT NULL THEN
        -- relid reads NULL for another role's build without pg_read_all_stats.
        SELECT pid INTO builder FROM pg_catalog.pg_stat_progress_create_index
        WHERE datname = current_database()
            AND (relid = 'public.envs'::regclass OR relid IS NULL)
        LIMIT 1;
        IF FOUND THEN
            RAISE EXCEPTION 'backend % is building an index on public.envs, which dropping a set-aside index would wait on; rerun once that build finishes or is cancelled', builder;
        END IF;
    END IF;
END $$;
-- +goose StatementEnd

DROP INDEX CONCURRENTLY IF EXISTS public.idx_envs_active_id_invalid;

-- IF NOT EXISTS skips invalid indexes left by interrupted builds.
-- Rename them for DROP INDEX CONCURRENTLY, which cannot run in a DO block.
-- +goose StatementBegin
DO $$
DECLARE
    builder integer;
BEGIN
    IF EXISTS (
        SELECT FROM pg_catalog.pg_index
        WHERE indexrelid = to_regclass('public.idx_envs_active_id') AND NOT indisvalid
    ) THEN
        -- index_relid reads NULL for another role's build without pg_read_all_stats.
        SELECT pid INTO builder FROM pg_catalog.pg_stat_progress_create_index
        WHERE datname = current_database()
            AND (index_relid = to_regclass('public.idx_envs_active_id') OR index_relid IS NULL)
        LIMIT 1;
        IF FOUND THEN
            RAISE EXCEPTION 'public.idx_envs_active_id is not valid yet and backend % is building an index; rerun once that build finishes or is cancelled', builder;
        END IF;
        ALTER INDEX public.idx_envs_active_id RENAME TO idx_envs_active_id_invalid;
    END IF;
END $$;
-- +goose StatementEnd

DROP INDEX CONCURRENTLY IF EXISTS public.idx_envs_active_id_invalid;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_envs_active_id
    ON public.envs (id)
    WHERE deleted_at IS NULL;

-- A competing build can fail after the prechecks; IF NOT EXISTS still skips
-- its invalid index.
-- +goose StatementBegin
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT FROM pg_catalog.pg_index
        WHERE indexrelid = to_regclass('public.idx_envs_active_id') AND indisvalid
    ) THEN
        RAISE EXCEPTION 'public.idx_envs_active_id is not valid; rerun to replace it';
    END IF;
END $$;
-- +goose StatementEnd

-- The migrator reuses this session for subsequent migrations.
SET statement_timeout = '3h';

-- +goose Down

SET statement_timeout = '1h';

DROP INDEX CONCURRENTLY IF EXISTS public.idx_envs_active_id;

SET statement_timeout = '3h';
