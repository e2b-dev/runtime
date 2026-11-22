-- +goose Up
-- +goose StatementBegin

-- Modify "env_aliases" table
ALTER TABLE IF EXISTS "public"."env_aliases" RENAME COLUMN "is_name" TO "is_renamable";
ALTER TABLE IF EXISTS "public"."env_aliases" ALTER COLUMN "env_id" SET NOT NULL;

-- Create "env_builds" table
CREATE TABLE "public"."env_builds" ("id" uuid NOT NULL DEFAULT gen_random_uuid(), "created_at" timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP, "updated_at" timestamptz NOT NULL, "finished_at" timestamptz NULL, "status" text NOT NULL DEFAULT 'waiting', "dockerfile" text NULL, "start_cmd" text NULL, "vcpu" bigint NOT NULL, "ram_mb" bigint NOT NULL, "free_disk_size_mb" bigint NOT NULL, "total_disk_size_mb" bigint NULL, "kernel_version" text NOT NULL DEFAULT 'vmlinux-5.10.186', "firecracker_version" text NOT NULL DEFAULT 'v1.7.0-dev_8bb88311', "env_id" text NULL, PRIMARY KEY ("id"), CONSTRAINT "env_builds_envs_builds" FOREIGN KEY ("env_id") REFERENCES "public"."envs" ("id") ON UPDATE NO ACTION ON DELETE CASCADE);

-- Populate "env_builds" table
INSERT INTO "public"."env_builds"(updated_at, finished_at, status, dockerfile, start_cmd, vcpu, ram_mb, free_disk_size_mb, total_disk_size_mb, kernel_version, firecracker_version, env_id)
SELECT CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 'success', dockerfile, NULL, vcpu, ram_mb, free_disk_size_mb, total_disk_size_mb, kernel_version, firecracker_version, id
FROM "public"."envs";

-- Modify "envs" table
ALTER TABLE IF EXISTS "public"."envs" 
    DROP COLUMN IF EXISTS "dockerfile",
    DROP COLUMN IF EXISTS "build_id",
    DROP COLUMN IF EXISTS "vcpu",
    DROP COLUMN IF EXISTS "ram_mb",
    DROP COLUMN IF EXISTS "free_disk_size_mb",
    DROP COLUMN IF EXISTS "total_disk_size_mb",
    DROP COLUMN IF EXISTS "kernel_version",
    DROP COLUMN IF EXISTS "firecracker_version";

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
-- +goose StatementEnd
