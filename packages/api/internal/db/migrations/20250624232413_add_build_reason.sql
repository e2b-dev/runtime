-- +goose Up
-- +goose StatementBegin
ALTER TABLE env_builds
ADD COLUMN reason TEXT;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
ALTER TABLE env_builds
DROP COLUMN reason;
-- +goose StatementEnd
