-- +goose Up
-- +goose StatementBegin

-- Add concurrent_template_builds column to tiers table
ALTER TABLE "public"."access_tokens" ALTER COLUMN "id" SET NOT NULL;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin

-- Drop the constraint and column
ALTER TABLE "public"."access_tokens" ALTER COLUMN "id" DROP NOT NULL;
-- +goose StatementEnd
