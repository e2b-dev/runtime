-- +goose Up
-- +goose StatementBegin

-- Modify "envs" table
ALTER TABLE "public"."envs"
    ADD COLUMN IF NOT EXISTS "vcpu" bigint NULL,
    ADD COLUMN IF NOT EXISTS "ram_mb" bigint NULL,
    ADD COLUMN IF NOT EXISTS "free_disk_size_mb" bigint NULL,
    ADD COLUMN IF NOT EXISTS "total_disk_size_mb" bigint NULL;

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
-- +goose StatementEnd
