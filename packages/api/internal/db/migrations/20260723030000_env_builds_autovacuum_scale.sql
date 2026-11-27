-- +goose Up
-- The default autovacuum scale factors trigger a pass only after dead tuples
-- reach a fixed fraction of the table, which on a large, update-heavy table is
-- far too late: index lookups pay visibility checks on the accumulated dead
-- entries long before a pass starts. Trigger vacuum/analyze much earlier
-- instead. Storage-parameter change only: takes a brief ShareUpdateExclusive
-- lock, rewrites nothing, affects autovacuum scheduling alone.
ALTER TABLE public.env_builds SET (
    autovacuum_vacuum_scale_factor = 0.02,
    autovacuum_analyze_scale_factor = 0.01
);

-- +goose Down
ALTER TABLE public.env_builds RESET (
    autovacuum_vacuum_scale_factor,
    autovacuum_analyze_scale_factor
);
