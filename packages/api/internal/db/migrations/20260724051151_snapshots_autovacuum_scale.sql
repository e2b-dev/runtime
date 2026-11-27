-- +goose Up
-- Same treatment env_builds received (20260723030000): the default autovacuum
-- scale factors trigger a pass only after dead tuples reach a fixed fraction
-- of the table, which on a large table defers cleanup far too long — index
-- lookups pay visibility checks on the accumulated dead entries the whole
-- time. Storage-parameter change only: takes a brief ShareUpdateExclusive
-- lock, rewrites nothing, affects autovacuum scheduling alone.
-- Inserts outnumber updates on this table, so the insert-driven threshold
-- is lowered alongside the dead-tuple one: it keeps the visibility map
-- fresh for the hot snapshot read paths between update-driven passes.
ALTER TABLE public.snapshots SET (
    autovacuum_vacuum_scale_factor = 0.02,
    autovacuum_vacuum_insert_scale_factor = 0.02,
    autovacuum_analyze_scale_factor = 0.01
);

-- +goose Down
ALTER TABLE public.snapshots RESET (
    autovacuum_vacuum_scale_factor,
    autovacuum_vacuum_insert_scale_factor,
    autovacuum_analyze_scale_factor
);
