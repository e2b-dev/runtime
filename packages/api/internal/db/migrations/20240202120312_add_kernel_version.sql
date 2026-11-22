-- +goose Up
-- +goose StatementBegin

-- Modify "envs" table
ALTER TABLE "public"."envs" ADD COLUMN IF NOT EXISTS "kernel_version" character varying NULL;

-- Update existing records
UPDATE "public"."envs" SET "kernel_version" = 'vmlinux-5.10.186-old' WHERE "kernel_version" IS NULL;

-- Make kernel_version NOT NULL and set default
ALTER TABLE "public"."envs" 
    ALTER COLUMN "kernel_version" SET NOT NULL,
    ALTER COLUMN "kernel_version" SET DEFAULT 'vmlinux-5.10.186';

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
-- +goose StatementEnd
