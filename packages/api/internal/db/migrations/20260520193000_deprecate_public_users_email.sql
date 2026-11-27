-- +goose Up
-- +goose StatementBegin

ALTER TABLE public.users DROP CONSTRAINT IF EXISTS users_email_key;
ALTER TABLE public.users
    ALTER COLUMN email SET DEFAULT ('deprecated-' || gen_random_uuid()::text || '@e2b.local');

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin

ALTER TABLE public.users
    ALTER COLUMN email DROP DEFAULT;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'users_email_key'
          AND conrelid = 'public.users'::regclass
    ) THEN
        ALTER TABLE public.users ADD CONSTRAINT users_email_key UNIQUE (email);
    END IF;
END $$;

-- +goose StatementEnd
