-- +goose Up
-- +goose NO TRANSACTION

-- DROP waits on in-flight lock holders rather than building anything; bound
-- it so it neither hangs unbounded nor dies to a short session default.
SET statement_timeout = '1h';

-- Follow-up to 20260727032500 (the additive half): the 4-column
-- idx_env_build_assignments_env_tag_created_build shares this index's exact
-- ordering prefix, so every scan the 3-column copy served runs identically
-- on it — keeping both only doubles write maintenance on a hot-insert
-- table. Merge only after the 4-column index has soaked in production.
DROP INDEX CONCURRENTLY IF EXISTS idx_env_build_assignments_env_tag_created;

-- The migrator session is reused for subsequent migrations: restore its
-- baseline (scripts/migrator.go sets 3h per connection).
SET statement_timeout = '3h';

-- +goose Down
-- +goose NO TRANSACTION

-- The concurrent rebuild legitimately runs long at this table's size; a
-- mid-build timeout would strand an INVALID index. Rolling back is a
-- deliberate manual operation with an operator present — unbounded on
-- purpose.
SET statement_timeout = 0;

-- An interrupted CONCURRENTLY build leaves an INVALID index that IF NOT
-- EXISTS would then skip forever — clear any leftover before rebuilding.
DROP INDEX CONCURRENTLY IF EXISTS idx_env_build_assignments_env_tag_created;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_env_build_assignments_env_tag_created
    ON public.env_build_assignments (env_id, tag, created_at DESC);

SET statement_timeout = '3h';
