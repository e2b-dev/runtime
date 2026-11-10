-- name: GetSnapshotFilesystemOnly :one
-- The kind recorded on a sandbox's snapshot row, false when the row predates
-- the config column. No row is a not-found error the caller treats as false.
SELECT COALESCE((config->>'filesystemOnly')::boolean, FALSE)::boolean AS filesystem_only
FROM "public"."snapshots"
WHERE sandbox_id = @sandbox_id;
