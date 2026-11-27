-- +goose Up
-- +goose StatementBegin
ALTER TABLE clusters
    ADD COLUMN IF NOT EXISTS sandbox_proxy_domain TEXT;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
ALTER TABLE clusters
    DROP COLUMN IF EXISTS sandbox_proxy_domain;
-- +goose StatementEnd
