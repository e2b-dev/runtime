-- +goose Up
-- +goose StatementBegin

ALTER TABLE snapshots
    ADD COLUMN config jsonb NULL;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
ALTER TABLE snapshots
    DROP COLUMN IF EXISTS config;
-- +goose StatementEnd
