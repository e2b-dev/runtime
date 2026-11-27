-- +goose Up
-- +goose NO TRANSACTION

-- ANALYZE cannot run inside a transaction; bound the pass so it neither
-- hangs unbounded nor dies to a short session default.
SET statement_timeout = '1h';

-- Same treatment as status_group (20260725100500): status has a handful of
-- values with heavily skewed frequencies, and the rare ones can drop out of
-- the column's MCV list on an unlucky autoanalyze sample, leaving the
-- planner to underestimate them by orders of magnitude on queries that
-- filter by status. A larger per-column sample keeps every value visible
-- across re-samples.
ALTER TABLE public.env_builds ALTER COLUMN status SET STATISTICS 2000;

-- Rebuild the column stats immediately rather than waiting for the next
-- autoanalyze threshold.
ANALYZE public.env_builds;

-- The migrator session is reused for subsequent migrations: restore its
-- baseline (scripts/migrator.go sets 3h per connection).
SET statement_timeout = '3h';

-- +goose Down
-- +goose NO TRANSACTION

SET statement_timeout = '1h';

ALTER TABLE public.env_builds ALTER COLUMN status SET STATISTICS -1;

ANALYZE public.env_builds;

SET statement_timeout = '3h';
