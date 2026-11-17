-- +goose Up
-- +goose StatementBegin

-- When a paused sandbox with a TTL expires: now() plus its TTL, written each
-- time one of its runs ends. NULL without a TTL. Stored rather than computed
-- from the TTL, because an index cannot hold timestamptz + interval, which is
-- not immutable.
--
-- Nullable with no default is a catalog-only change, but it still takes an
-- ACCESS EXCLUSIVE lock on snapshots: bound the wait rather than queue every
-- reader and writer behind a long transaction.
SET LOCAL lock_timeout = '5s';
ALTER TABLE "public"."snapshots" ADD COLUMN IF NOT EXISTS expires_at timestamptz NULL;

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin

SET LOCAL lock_timeout = '5s';
ALTER TABLE "public"."snapshots" DROP COLUMN IF EXISTS expires_at;

-- +goose StatementEnd
