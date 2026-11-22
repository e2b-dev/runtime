-- +goose Up
-- +goose NO TRANSACTION

-- +goose StatementBegin
-- Create snapshot_templates table to track which sandbox a snapshot template was created from
CREATE TABLE IF NOT EXISTS "public"."snapshot_templates" (
    env_id text NOT NULL PRIMARY KEY REFERENCES envs(id) ON DELETE CASCADE,
    sandbox_id text NOT NULL,
    created_at timestamp with time zone DEFAULT now()
);

-- +goose StatementEnd

-- Create index for looking up snapshot templates by source sandbox
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_snapshot_templates_sandbox_id
    ON "public"."snapshot_templates" (sandbox_id);

-- +goose Down
-- +goose NO TRANSACTION

DROP INDEX CONCURRENTLY IF EXISTS idx_snapshot_templates_sandbox_id;

-- +goose StatementBegin
DROP TABLE IF EXISTS "public"."snapshot_templates";
-- +goose StatementEnd
