-- +goose Up
-- +goose StatementBegin
ALTER TABLE env_builds
    ADD COLUMN IF NOT EXISTS cluster_node_id TEXT NULL;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
ALTER TABLE env_builds DROP COLUMN IF EXISTS cluster_node_id;
-- +goose StatementEnd
