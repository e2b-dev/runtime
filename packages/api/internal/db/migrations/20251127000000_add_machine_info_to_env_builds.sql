-- +goose Up
-- +goose StatementBegin

-- Add machine info columns to env_builds table
ALTER TABLE "public"."env_builds"
    ADD COLUMN IF NOT EXISTS "cpu_architecture" text NULL,
    ADD COLUMN IF NOT EXISTS "cpu_family" text NULL,
    ADD COLUMN IF NOT EXISTS "cpu_model" text NULL,
    ADD COLUMN IF NOT EXISTS "cpu_model_name" text NULL,
    ADD COLUMN IF NOT EXISTS "cpu_flags" text[] NULL;

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin

-- Remove machine info columns from env_builds table
ALTER TABLE "public"."env_builds"
    DROP COLUMN IF EXISTS "cpu_architecture",
    DROP COLUMN IF EXISTS "cpu_family",
    DROP COLUMN IF EXISTS "cpu_model",
    DROP COLUMN IF EXISTS "cpu_model_name",
    DROP COLUMN IF EXISTS "cpu_flags";

-- +goose StatementEnd