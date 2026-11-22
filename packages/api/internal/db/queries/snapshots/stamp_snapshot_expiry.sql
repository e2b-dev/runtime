-- name: StampSnapshotExpiry :execrows
-- Restarts a paused sandbox's expiry clock when one of its runs ends, by a
-- pause or otherwise: stores the TTL the run carried and sets expires_at from
-- it, or clears both when the run had none. A sandbox never paused has no row.
UPDATE "public"."snapshots"
SET
    config = CASE
        WHEN sqlc.narg(paused_ttl_seconds)::bigint IS NULL THEN COALESCE(config, '{}'::jsonb) - 'pausedTtl'
        ELSE jsonb_set(COALESCE(config, '{}'::jsonb), '{pausedTtl}', to_jsonb(sqlc.narg(paused_ttl_seconds)::bigint))
    END,
    expires_at = now() + make_interval(secs => sqlc.narg(paused_ttl_seconds)::bigint)
WHERE sandbox_id = @sandbox_id
    AND team_id = @team_id
    AND env_deleted_at IS NULL;
