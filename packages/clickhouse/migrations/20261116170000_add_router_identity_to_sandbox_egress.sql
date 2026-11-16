-- +goose Up
-- +goose StatementBegin
ALTER TABLE sandbox_egress_local
    ADD COLUMN IF NOT EXISTS router_id LowCardinality(String) DEFAULT '' CODEC (ZSTD(1)),
    ADD COLUMN IF NOT EXISTS portable_ip String DEFAULT '' CODEC (ZSTD(1));
-- +goose StatementEnd

-- +goose StatementBegin
ALTER TABLE sandbox_egress
    ADD COLUMN IF NOT EXISTS router_id LowCardinality(String) DEFAULT '' CODEC (ZSTD(1)),
    ADD COLUMN IF NOT EXISTS portable_ip String DEFAULT '' CODEC (ZSTD(1));
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
ALTER TABLE sandbox_egress
    DROP COLUMN IF EXISTS portable_ip,
    DROP COLUMN IF EXISTS router_id;
-- +goose StatementEnd

-- +goose StatementBegin
ALTER TABLE sandbox_egress_local
    DROP COLUMN IF EXISTS portable_ip,
    DROP COLUMN IF EXISTS router_id;
-- +goose StatementEnd
