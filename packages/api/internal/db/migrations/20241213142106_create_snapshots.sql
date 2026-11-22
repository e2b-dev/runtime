-- +goose Up
-- +goose StatementBegin

-- Create "snapshots" table
CREATE TABLE IF NOT EXISTS "public"."snapshots" (
    created_at timestamp with time zone null,
    env_id text not null,
    sandbox_id text not null,
    id uuid not null default gen_random_uuid(),
    metadata jsonb null,
    base_env_id text not null,
    constraint snapshots_pkey primary key (id)
);

COMMIT;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
-- +goose StatementEnd
