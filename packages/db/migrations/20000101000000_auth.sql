-- +goose Up
-- +goose StatementBegin
CREATE SCHEMA IF NOT EXISTS auth;

-- Roles are server-wide: another database on the same server may have
-- created it already.
DO $role$
BEGIN
    CREATE ROLE authenticated;
EXCEPTION WHEN duplicate_object THEN
    NULL;
END
$role$;

CREATE FUNCTION auth.uid() RETURNS uuid AS $func$
BEGIN
    RETURN gen_random_uuid();
END;
$func$ LANGUAGE plpgsql;

-- Grant execute on auth.uid() to postgres role
GRANT EXECUTE ON FUNCTION auth.uid() TO postgres;

CREATE TABLE auth.users (
    id uuid NOT NULL DEFAULT gen_random_uuid(),
    email text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    raw_app_meta_data jsonb,
    raw_user_meta_data jsonb,
    PRIMARY KEY (id)
);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
-- +goose StatementEnd
