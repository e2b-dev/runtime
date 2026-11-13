-- name: BoundSnapshotExpiry :exec
-- Sets a resumed sandbox's expiry to the latest its run can end plus its TTL,
-- before the run starts. The run's end then only brings it forward, so a stamp
-- that fails leaves a later expiry, never an earlier one. A NULL bound clears it.
UPDATE "public"."snapshots"
SET expires_at = sqlc.narg(expires_at)::timestamptz
WHERE sandbox_id = @sandbox_id
    AND team_id = @team_id
    AND env_deleted_at IS NULL;
