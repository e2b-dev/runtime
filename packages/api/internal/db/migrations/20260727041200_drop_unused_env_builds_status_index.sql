-- +goose Up
-- +goose NO TRANSACTION

-- DROP waits on in-flight lock holders rather than building anything; bound
-- it so it neither hangs unbounded nor dies to a short session default.
SET statement_timeout = '1h';

-- The plain status index is effectively unread (single-digit scans over a
-- multi-month usage window): status-filtered queries run through per-entity
-- joins or the status_group family of indexes instead, while every build
-- write maintains this copy on one of the hottest-write tables in the
-- schema. status_group remains separately indexed and is not touched.
DROP INDEX CONCURRENTLY IF EXISTS public.idx_env_builds_status;

-- The migrator session is reused for subsequent migrations: restore its
-- baseline (scripts/migrator.go sets 3h per connection).
SET statement_timeout = '3h';

-- +goose Down
-- +goose NO TRANSACTION

-- The concurrent rebuild legitimately runs for a long time at this table's
-- size; a mid-build timeout would strand an INVALID index. Rolling back is
-- a deliberate manual operation with an operator present — unbounded on
-- purpose.
SET statement_timeout = 0;

-- An interrupted CONCURRENTLY build leaves an INVALID index that IF NOT
-- EXISTS would then skip forever — clear any leftover before rebuilding.
DROP INDEX CONCURRENTLY IF EXISTS public.idx_env_builds_status;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_env_builds_status
    ON public.env_builds (status);

SET statement_timeout = '3h';
