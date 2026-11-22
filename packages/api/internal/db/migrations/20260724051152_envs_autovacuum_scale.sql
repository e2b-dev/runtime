-- +goose Up
-- Same per-table autovacuum override env_builds (20260723030000) received:
-- the default scale factors trigger a pass only after dead tuples reach a
-- fixed fraction of the table, which on a large, update-heavy table is far
-- too late — index lookups pay visibility checks on the accumulated dead
-- entries long before a pass starts. Storage-parameter change only: brief
-- ShareUpdateExclusive lock, rewrites nothing, affects scheduling alone.
ALTER TABLE public.envs SET (
    autovacuum_vacuum_scale_factor = 0.02,
    autovacuum_analyze_scale_factor = 0.01
);

-- +goose Down
ALTER TABLE public.envs RESET (
    autovacuum_vacuum_scale_factor,
    autovacuum_analyze_scale_factor
);
