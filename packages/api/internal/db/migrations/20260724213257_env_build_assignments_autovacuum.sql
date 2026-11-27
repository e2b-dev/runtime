-- +goose Up
-- Append-mostly table (rows are never updated; deletes happen only as
-- occasional cascades), so the INSERT-driven threshold is the trigger that
-- matters day to day and the analyze factor is what keeps planner stats
-- fresh on a hot join table. The dead-tuple factor is lowered too so a
-- large cascade-delete burst triggers cleanup promptly instead of waiting
-- for insert volume. Same per-table override family as env_builds
-- (20260723030000). Scheduling-only; brief ShareUpdateExclusive lock.
ALTER TABLE public.env_build_assignments SET (
    autovacuum_vacuum_insert_scale_factor = 0.02,
    autovacuum_vacuum_scale_factor = 0.02,
    autovacuum_analyze_scale_factor = 0.01
);

-- +goose Down
ALTER TABLE public.env_build_assignments RESET (
    autovacuum_vacuum_insert_scale_factor,
    autovacuum_vacuum_scale_factor,
    autovacuum_analyze_scale_factor
);
