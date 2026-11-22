-- +goose Up
-- +goose StatementBegin
-- Drop firecracker version default in env_builds table
ALTER TABLE "public"."env_builds"
ALTER COLUMN "firecracker_version" DROP DEFAULT;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
-- Restore firecracker version default in env_builds table
ALTER TABLE "public"."env_builds"
ALTER COLUMN "firecracker_version" SET DEFAULT 'v1.10.1_1fcdaec';
-- +goose StatementEnd
