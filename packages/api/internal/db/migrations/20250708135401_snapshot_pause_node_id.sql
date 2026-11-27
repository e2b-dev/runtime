-- +goose Up
-- +goose StatementBegin
ALTER TABLE snapshots ADD COLUMN origin_node_id text;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
ALTER TABLE snapshots DROP COLUMN IF EXISTS origin_node_id;
-- +goose StatementEnd
