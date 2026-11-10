-- +goose Up
-- +goose StatementBegin

-- Marks a snapshot build that was taken while the sandbox guest agent did
-- not answer the pause's health probe: its memory image holds an
-- unresponsive agent, so the API refuses a memory resume of it and asks for
-- one without memory. Written only when the node reports the miss; NULL on
-- every build from before the column and on every build whose agent
-- answered or was not probed, which the readers treat alike.
--
-- Nullable with no default is a catalog-only change, but it still takes an
-- ACCESS EXCLUSIVE lock on env_builds: bound the wait rather than queue every
-- reader and writer behind a long transaction.
SET LOCAL lock_timeout = '5s';
ALTER TABLE "public"."env_builds" ADD COLUMN IF NOT EXISTS envd_unresponsive boolean NULL;

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin

SET LOCAL lock_timeout = '5s';
ALTER TABLE "public"."env_builds" DROP COLUMN IF EXISTS envd_unresponsive;

-- +goose StatementEnd
