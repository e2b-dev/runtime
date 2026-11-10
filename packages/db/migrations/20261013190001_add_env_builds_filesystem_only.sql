-- +goose Up
-- +goose StatementBegin

-- Marks a snapshot build that persists only the rootfs, so any start from it
-- cold-boots. Builds that predate the column read false, whatever their kind.
--
-- A constant default makes this a catalog-only change, but it still takes an
-- ACCESS EXCLUSIVE lock on env_builds: bound the wait rather than queue every
-- writer behind a long transaction.
SET LOCAL lock_timeout = '5s';
ALTER TABLE "public"."env_builds"
    ADD COLUMN IF NOT EXISTS filesystem_only boolean NOT NULL DEFAULT false;

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin

SET LOCAL lock_timeout = '5s';
ALTER TABLE "public"."env_builds" DROP COLUMN IF EXISTS filesystem_only;

-- +goose StatementEnd
