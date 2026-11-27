-- +goose Up
-- +goose StatementBegin
UPDATE "public"."snapshots" SET origin_node_id = 'unknown' WHERE origin_node_id IS NULL;
ALTER TABLE "public"."snapshots" ALTER COLUMN origin_node_id SET NOT NULL;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
ALTER TABLE "public"."snapshots" ALTER COLUMN origin_node_id DROP NOT NULL;
UPDATE "public"."snapshots" SET origin_node_id = NULL WHERE origin_node_id = 'unknown';
-- +goose StatementEnd
