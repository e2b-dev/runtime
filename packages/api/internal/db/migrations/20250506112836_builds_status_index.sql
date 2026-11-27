-- +goose NO TRANSACTION
-- +goose Up
-- The index creation takes a lot of time
CREATE INDEX CONCURRENTLY idx_env_builds_status ON public.env_builds(status);

-- +goose Down
DROP INDEX CONCURRENTLY public.idx_env_builds_status;
