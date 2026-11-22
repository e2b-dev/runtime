-- +goose Up
-- +goose StatementBegin

ALTER TABLE public.users DROP CONSTRAINT IF EXISTS users_email_key;
ALTER TABLE public.users DROP COLUMN IF EXISTS email;

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin

ALTER TABLE public.users
    ADD COLUMN IF NOT EXISTS email text NOT NULL DEFAULT ('deprecated-' || gen_random_uuid()::text || '@e2b.local');

-- +goose StatementEnd
