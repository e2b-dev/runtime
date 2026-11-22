-- +goose Up
-- +goose NO TRANSACTION

CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS user_identities_user_id_oidc_iss_idx
    ON public.user_identities (user_id, oidc_iss);

-- +goose Down
-- +goose NO TRANSACTION

DROP INDEX CONCURRENTLY IF EXISTS user_identities_user_id_oidc_iss_idx;
