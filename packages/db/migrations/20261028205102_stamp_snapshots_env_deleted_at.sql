-- +goose Up
-- +goose StatementBegin

-- Stamps the snapshots of envs deleted before soft deletes stamped them, but
-- only while the table is small enough to rewrite in one statement. It runs
-- apart from the column add so that it holds row locks on the rows it stamps,
-- not the table lock that would stall every pause, resume and list behind it.
--
-- A larger table is stamped in batches by
-- scripts/backfill_snapshots_env_deleted_at.sql. Every install, a small one
-- too, runs that script once after every service runs this release: this
-- migration runs before the rollout, so nothing but the script reliably stamps
-- what the previous release soft-deletes while this one rolls out. Until then those
-- rows read NULL, so readers must keep checking envs.
--
-- The size is the heap's on disk, which is always known, not the planner's row
-- estimate, which reads -1 until the table is first vacuumed or analyzed, as
-- after a restore from a dump. 256 MB holds 0.6 to 1.5 million rows: a row
-- takes 170 to 430 bytes, depending on its metadata and config. Dead rows and
-- free space count too, which only sends a table to the script sooner. The
-- subquery runs once, before any row is read, so a large table is not scanned.
UPDATE "public"."snapshots" s
SET env_deleted_at = e.deleted_at
FROM "public"."envs" e
WHERE e.id = s.env_id
  AND e.deleted_at IS NOT NULL
  AND s.env_deleted_at IS NULL
  AND (
      SELECT pg_catalog.pg_relation_size('public.snapshots'::regclass)
  ) < pg_catalog.pg_size_bytes('256 MB');

-- +goose StatementEnd

-- +goose Down
-- Nothing to undo: the previous migration's Down drops the column.
SELECT 1;
