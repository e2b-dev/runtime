-- +goose Up
ALTER TABLE "public"."teams"
    ADD COLUMN "sandbox_scheduling_labels" text[] NOT NULL DEFAULT '{}';

-- +goose Down
ALTER TABLE "public"."teams"
    DROP COLUMN "sandbox_scheduling_labels";
