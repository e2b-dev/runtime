-- +goose Up
-- +goose StatementBegin

-- Modify "envs" table
ALTER TABLE "public"."envs"
ALTER COLUMN "firecracker_version" SET DEFAULT 'v1.7.0-dev_8bb88311';

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
-- +goose StatementEnd
