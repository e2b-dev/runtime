-- +goose Up
-- +goose StatementBegin

-- Modify "tiers" table
ALTER TABLE "public"."tiers" ADD COLUMN IF NOT EXISTS "max_length_hours" bigint NULL;

-- Update existing records
UPDATE "public"."tiers" SET "max_length_hours" = 1 WHERE "max_length_hours" IS NULL;

-- Make max_length_hours NOT NULL
ALTER TABLE "public"."tiers" ALTER COLUMN "max_length_hours" SET NOT NULL;

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
-- +goose StatementEnd
