-- +goose Up
-- +goose StatementBegin

-- Modify "envs" table
ALTER TABLE "public"."envs"
    ADD COLUMN IF NOT EXISTS "created_by" uuid NULL;

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
-- +goose StatementEnd
