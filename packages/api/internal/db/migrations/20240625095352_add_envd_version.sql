-- +goose Up
-- +goose StatementBegin

-- Modify "env_builds" table
ALTER TABLE "public"."env_builds" ADD COLUMN IF NOT EXISTS "envd_version" text NULL;

-- Populate "envd_version" column if it was just added
UPDATE "public"."env_builds" SET "envd_version" = 'v0.0.1'
WHERE "envd_version" IS NULL;

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
-- +goose StatementEnd
