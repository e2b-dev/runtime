-- +goose Up
-- +goose StatementBegin

-- When the caller decided the stored revision; NULL when it did not say.
SET LOCAL lock_timeout = '5s';
ALTER TABLE projection.project_limits ADD COLUMN decided_at timestamptz;
ALTER TABLE projection.project_blocks ADD COLUMN decided_at timestamptz;

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin

SET LOCAL lock_timeout = '5s';
ALTER TABLE projection.project_blocks DROP COLUMN decided_at;
ALTER TABLE projection.project_limits DROP COLUMN decided_at;

-- +goose StatementEnd
