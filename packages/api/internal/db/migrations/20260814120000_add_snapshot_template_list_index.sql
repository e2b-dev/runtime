-- +goose NO TRANSACTION
-- +goose Up

-- The concurrent build can legitimately run long at this table's size -- the index
-- is expected to be roughly 19 GB -- and a mid-build timeout would strand an
-- INVALID index and leave the migration unrecorded. The migrator connection
-- carries a 3h default (scripts/migrator.go), so lift the bound for the build.
SET statement_timeout = 0;

-- An interrupted CONCURRENTLY build leaves an INVALID index that IF NOT EXISTS
-- would then skip forever -- clear any leftover before rebuilding.
DROP INDEX CONCURRENTLY IF EXISTS public.idx_snapshots_team_base_env_time_id;

-- Serves the template-filtered sandbox list: team_id and base_env_id are
-- equalities, followed by the keyset columns in the order the descending query
-- returns them. The ascending query is the exact reverse, so a backward scan of
-- the same index serves it too.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_snapshots_team_base_env_time_id
    ON public.snapshots (
        team_id,
        base_env_id,
        sandbox_started_at DESC,
        sandbox_id ASC
    );

-- The migrator session is reused for subsequent migrations: restore its baseline.
SET statement_timeout = '3h';

-- +goose Down

-- DROP waits on in-flight lock holders rather than building anything; bound it so
-- it neither hangs unbounded nor dies to a short session default.
SET statement_timeout = '1h';

DROP INDEX CONCURRENTLY IF EXISTS public.idx_snapshots_team_base_env_time_id;

SET statement_timeout = '3h';
