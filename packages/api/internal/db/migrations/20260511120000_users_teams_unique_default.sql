-- +goose Up
-- +goose NO TRANSACTION

CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS users_teams_user_id_is_default_idx
    ON public.users_teams (user_id)
    WHERE is_default = true;

-- +goose Down
-- +goose NO TRANSACTION

DROP INDEX CONCURRENTLY IF EXISTS users_teams_user_id_is_default_idx;
