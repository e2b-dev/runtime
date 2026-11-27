-- +goose Up
-- +goose StatementBegin

-- Modify "env_builds" table
ALTER TABLE "public"."env_builds" ADD COLUMN IF NOT EXISTS "version" text NULL;

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin

-- Remove "version" column from "env_builds" table
ALTER TABLE "public"."env_builds" DROP COLUMN IF EXISTS "version";

-- +goose StatementEnd
