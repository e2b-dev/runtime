-- name: UpdateSnapshotFilesystemOnly :exec
-- Records the kind of the build that just became the row's latest ready one.
-- Written on success only, so a refused or failed checkpoint never changes
-- what the previous ready build is resumed as.
UPDATE "public"."snapshots"
SET config = jsonb_set(COALESCE(config, '{}'::jsonb), '{filesystemOnly}', to_jsonb(@filesystem_only::boolean))
WHERE sandbox_id = @sandbox_id;
