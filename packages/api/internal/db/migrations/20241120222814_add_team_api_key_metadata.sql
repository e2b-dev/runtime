-- +goose Up
-- +goose StatementBegin

-- Modify "team_api_keys" table
ALTER TABLE "public"."team_api_keys"
    ADD COLUMN IF NOT EXISTS "updated_at" timestamptz NULL,
    ADD COLUMN IF NOT EXISTS "name" text NOT NULL DEFAULT 'Unnamed API Key',
    ADD COLUMN IF NOT EXISTS "last_used" timestamptz NULL,
    ADD COLUMN IF NOT EXISTS "created_by" uuid NULL;

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
-- +goose StatementEnd
