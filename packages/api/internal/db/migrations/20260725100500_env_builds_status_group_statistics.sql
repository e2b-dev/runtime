-- +goose Up
-- +goose NO TRANSACTION

-- ANALYZE cannot run inside a transaction; bound the pass so it neither
-- hangs unbounded nor dies to a short session default.
SET statement_timeout = '1h';

-- status_group='pending' is rare enough (well under a hundredth of a percent
-- of the table, measured in production) that the default statistics sample
-- statistically never catches it: 'pending' is absent from the column's MCV
-- list, so the planner underestimates it by orders of magnitude and drives
-- InvalidateUnstartedTemplateBuilds from the pending-scan side — tens of
-- thousands of index probes into env_build_assignments per execution instead
-- of one selective (env_id, tag) lookup. A larger per-column sample makes
-- rare-but-hot status groups visible and flips the join order back.
ALTER TABLE public.env_builds ALTER COLUMN status_group SET STATISTICS 2000;

-- Rebuild the column stats immediately: autoanalyze would otherwise wait
-- for the change threshold, leaving the misestimate live for hours.
ANALYZE public.env_builds;

-- The migrator session is reused for subsequent migrations: restore its
-- baseline (scripts/migrator.go sets 3h per connection).
SET statement_timeout = '3h';

-- +goose Down
-- +goose NO TRANSACTION

SET statement_timeout = '1h';

ALTER TABLE public.env_builds ALTER COLUMN status_group SET STATISTICS -1;

ANALYZE public.env_builds;

SET statement_timeout = '3h';
