-- +goose NO TRANSACTION
-- +goose Up
CREATE INDEX CONCURRENTLY idx_snapshots_sandbox_id ON public.snapshots (sandbox_id);

-- +goose Down
DROP INDEX CONCURRENTLY idx_snapshots_sandbox_id;
