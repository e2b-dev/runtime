-- +goose Up
-- +goose StatementBegin
ALTER TABLE "public"."env_builds" ALTER COLUMN cluster_node_id DROP NOT NULL;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
UPDATE "public"."env_builds" SET cluster_node_id = 'unknown' WHERE cluster_node_id IS NULL;
ALTER TABLE "public"."env_builds" ALTER COLUMN cluster_node_id SET NOT NULL;
-- +goose StatementEnd
