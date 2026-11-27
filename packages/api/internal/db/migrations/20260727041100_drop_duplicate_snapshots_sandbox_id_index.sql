-- +goose Up
-- +goose NO TRANSACTION

-- DROP waits on in-flight lock holders rather than building anything; bound
-- it so it neither hangs unbounded nor dies to a short session default.
SET statement_timeout = '1h';

-- Exact duplicate of snapshots_sandbox_id_unique (same key column): the
-- unique, constraint-backed twin serves every lookup, while this plain copy
-- shows no scans over a multi-month usage window and every snapshot write
-- maintains both. Uniqueness enforcement is untouched.
DROP INDEX CONCURRENTLY IF EXISTS public.idx_snapshots_sandbox_id;

-- The migrator session is reused for subsequent migrations: restore its
-- baseline (scripts/migrator.go sets 3h per connection).
SET statement_timeout = '3h';

-- +goose Down
-- +goose NO TRANSACTION

-- The concurrent rebuild can legitimately run long at this table's size; a
-- mid-build timeout would strand an INVALID index. Rolling back is a
-- deliberate manual operation with an operator present — unbounded on
-- purpose.
SET statement_timeout = 0;

-- An interrupted CONCURRENTLY build leaves an INVALID index that IF NOT
-- EXISTS would then skip forever — clear any leftover before rebuilding.
DROP INDEX CONCURRENTLY IF EXISTS public.idx_snapshots_sandbox_id;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_snapshots_sandbox_id
    ON public.snapshots (sandbox_id);

SET statement_timeout = '3h';
