-- +goose Up
-- +goose NO TRANSACTION

-- CREATE INDEX CONCURRENTLY and ANALYZE cannot run inside a transaction;
-- the migrator's session default (3h, scripts/migrator.go) bounds them.

-- The current-tip lookup orders by (created_at DESC, build_id DESC) under an
-- (env_id, tag) equality. That exact shape belongs to an internal
-- first-party consumer of this table whose queries are not in this
-- repository's tree (searching only this repo won't find it); the
-- latest-build laterals here order by created_at alone and ride the same
-- prefix unchanged. The existing index stops at created_at: the planner
-- still needs a top-N sort, so its plan choice rides on per-column
-- estimates that every autoanalyze re-samples. Completing the index to the
-- full sort key turns the lookup into a descend-and-stop ordered index scan
-- (LIMIT 1, no sort), which stays the cheapest plan under any statistics
-- roll; newer-assignment probes ride the same prefix.
-- This migration is deliberately ADDITIVE: the superseded 3-column index
-- (idx_env_build_assignments_env_tag_created, identical ordering prefix) is
-- dropped by a separate follow-up migration once the new index has soaked.
-- A failed CONCURRENTLY build leaves an INVALID index that a plain retry
-- would trip over; drop it first.
DROP INDEX CONCURRENTLY IF EXISTS idx_env_build_assignments_env_tag_created_build;
CREATE INDEX CONCURRENTLY idx_env_build_assignments_env_tag_created_build
    ON public.env_build_assignments (env_id, tag, created_at DESC, build_id DESC);

-- Stabilize the estimate inputs themselves: larger per-column samples cut the
-- roll-to-roll variance of n_distinct/MCV on the lookup's key columns (same
-- treatment as env_builds.status_group).
ALTER TABLE public.env_build_assignments ALTER COLUMN env_id SET STATISTICS 2000;
ALTER TABLE public.env_build_assignments ALTER COLUMN tag SET STATISTICS 2000;

-- Rebuild stats immediately rather than waiting for the next autoanalyze.
ANALYZE public.env_build_assignments;

-- +goose Down
-- +goose NO TRANSACTION

DROP INDEX CONCURRENTLY IF EXISTS idx_env_build_assignments_env_tag_created_build;

ALTER TABLE public.env_build_assignments ALTER COLUMN env_id SET STATISTICS -1;
ALTER TABLE public.env_build_assignments ALTER COLUMN tag SET STATISTICS -1;

ANALYZE public.env_build_assignments;
