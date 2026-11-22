-- +goose Up
-- +goose StatementBegin

-- Mirrors envs.deleted_at of the snapshot's own env (env_id), NULL while that
-- env is live, so a reader can skip a deleted env's snapshots, and an index
-- can leave them out, without probing envs row by row. No trigger keeps it in
-- step: every statement that soft-deletes envs stamps their snapshots in the
-- same statement.
--
-- Nullable with no default is a catalog-only change, but it still takes an
-- ACCESS EXCLUSIVE lock on snapshots: bound the wait rather than queue every
-- reader and writer behind a long transaction. Existing rows are stamped by
-- the next migration, after this one has committed and released the lock.
SET LOCAL lock_timeout = '5s';
ALTER TABLE "public"."snapshots" ADD COLUMN IF NOT EXISTS env_deleted_at timestamptz NULL;

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin

SET LOCAL lock_timeout = '5s';
ALTER TABLE "public"."snapshots" DROP COLUMN IF EXISTS env_deleted_at;

-- +goose StatementEnd
