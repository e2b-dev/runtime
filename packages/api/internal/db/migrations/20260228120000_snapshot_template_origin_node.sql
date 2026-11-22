-- +goose Up
ALTER TABLE "public"."snapshot_templates"
    ADD COLUMN "origin_node_id" TEXT,
    ADD COLUMN "build_id" UUID;

-- +goose Down
ALTER TABLE "public"."snapshot_templates"
    DROP COLUMN "origin_node_id",
    DROP COLUMN "build_id";
