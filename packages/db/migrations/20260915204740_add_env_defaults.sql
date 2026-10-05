-- +goose Up
-- +goose StatementBegin

CREATE TABLE IF NOT EXISTS public.env_defaults (
    env_id TEXT PRIMARY KEY REFERENCES public.envs(id),
    description TEXT
);

-- +goose StatementEnd

-- +goose Down
-- The table may hold data written under the separate dashboard migration
-- history. Retain it on rollback so applications using it continue to work.
