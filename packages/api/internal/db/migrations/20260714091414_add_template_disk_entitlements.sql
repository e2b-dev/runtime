-- +goose Up
-- +goose StatementBegin

ALTER TABLE "public"."tiers"
    -- Free-rootfs target in the existing MiB convention.
    ADD COLUMN IF NOT EXISTS "default_free_disk_size_mb" bigint,
    -- Total logical-rootfs ceiling before active add-ons.
    ADD COLUMN IF NOT EXISTS "max_disk_size_mb" bigint;

ALTER TABLE "public"."addons"
    -- Total-ceiling increment in the existing MiB convention.
    ADD COLUMN IF NOT EXISTS "extra_max_disk_size_mb" bigint;

-- Populate existing tiers in every environment before the effective limits are exposed.
UPDATE "public"."tiers"
SET "default_free_disk_size_mb" = "disk_mb",
    "max_disk_size_mb" = "disk_mb" + 25000;

-- Populate rows that predate the schema expansion in every environment. Keep the column nullable
-- because legacy writers may still omit it during the rollout.
UPDATE "public"."addons"
SET "extra_max_disk_size_mb" = "extra_disk_mb"
WHERE "extra_max_disk_size_mb" IS NULL;

-- Every existing row was populated above, and every future row must supply both values.
ALTER TABLE "public"."tiers"
    ALTER COLUMN "default_free_disk_size_mb" SET NOT NULL,
    ALTER COLUMN "max_disk_size_mb" SET NOT NULL,
    ADD CONSTRAINT "tiers_default_free_disk_size_mb_check"
        CHECK (default_free_disk_size_mb >= 0),
    ADD CONSTRAINT "tiers_max_disk_size_mb_check"
        CHECK (max_disk_size_mb > 0),
    ADD CONSTRAINT "tiers_default_free_disk_size_lte_max_check"
        CHECK (default_free_disk_size_mb <= max_disk_size_mb);

ALTER TABLE "public"."addons"
    ADD CONSTRAINT "addons_extra_max_disk_size_mb_check"
        CHECK (extra_max_disk_size_mb >= 0);

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin

ALTER TABLE "public"."tiers"
    DROP CONSTRAINT IF EXISTS "tiers_default_free_disk_size_lte_max_check",
    DROP CONSTRAINT IF EXISTS "tiers_max_disk_size_mb_check",
    DROP CONSTRAINT IF EXISTS "tiers_default_free_disk_size_mb_check",
    DROP COLUMN IF EXISTS "max_disk_size_mb",
    DROP COLUMN IF EXISTS "default_free_disk_size_mb";

ALTER TABLE "public"."addons"
    DROP CONSTRAINT IF EXISTS "addons_extra_max_disk_size_mb_check",
    DROP COLUMN IF EXISTS "extra_max_disk_size_mb";

-- +goose StatementEnd
