-- +goose Up
-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS public.users (
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    id uuid NOT NULL,
    email text NOT NULL,
    PRIMARY KEY (id),
    UNIQUE (email)
);

-- Databases created before this migration keep their users in auth.users,
-- which later migrations' foreign keys need in public.users.
DO $copy$
BEGIN
    IF to_regclass('auth.users') IS NOT NULL THEN
        INSERT INTO public.users (id, email)
        SELECT id, email FROM auth.users;
    END IF;
END
$copy$;

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS public.users;
-- +goose StatementEnd
