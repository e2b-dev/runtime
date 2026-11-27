-- +goose Up
-- +goose StatementBegin

ALTER TABLE snapshots
    ADD COLUMN auto_pause boolean NOT NULL default false;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
ALTER TABLE snapshots
    DROP COLUMN IF EXISTS auto_pause;
-- +goose StatementEnd
