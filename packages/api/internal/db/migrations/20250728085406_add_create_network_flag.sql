-- +goose Up
-- +goose StatementBegin

ALTER TABLE snapshots
    ADD COLUMN allow_internet_access boolean NULL;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
ALTER TABLE snapshots
    DROP COLUMN IF EXISTS allow_internet_access;
-- +goose StatementEnd
