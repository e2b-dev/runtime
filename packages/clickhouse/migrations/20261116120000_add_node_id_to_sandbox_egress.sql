-- +goose Up
-- +goose StatementBegin
ALTER TABLE sandbox_egress_local
    ADD COLUMN IF NOT EXISTS node_id LowCardinality(String) DEFAULT '' CODEC (ZSTD(1));
-- +goose StatementEnd

-- +goose StatementBegin
ALTER TABLE sandbox_egress_local
    ADD INDEX IF NOT EXISTS idx_node_id node_id TYPE bloom_filter GRANULARITY 4;
-- +goose StatementEnd

-- +goose StatementBegin
ALTER TABLE sandbox_egress
    ADD COLUMN IF NOT EXISTS node_id LowCardinality(String) DEFAULT '' CODEC (ZSTD(1));
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
ALTER TABLE sandbox_egress DROP COLUMN IF EXISTS node_id;
-- +goose StatementEnd

-- +goose StatementBegin
ALTER TABLE sandbox_egress_local DROP INDEX IF EXISTS idx_node_id;
-- +goose StatementEnd

-- +goose StatementBegin
ALTER TABLE sandbox_egress_local DROP COLUMN IF EXISTS node_id;
-- +goose StatementEnd
