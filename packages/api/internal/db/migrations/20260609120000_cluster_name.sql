-- +goose Up
-- +goose StatementBegin
ALTER TABLE clusters ADD COLUMN IF NOT EXISTS name TEXT NOT NULL DEFAULT '';
ALTER TABLE clusters ALTER COLUMN name DROP DEFAULT;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
ALTER TABLE clusters DROP COLUMN IF EXISTS name;
-- +goose StatementEnd
