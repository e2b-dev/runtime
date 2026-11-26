-- name: ListExpiredPausedSnapshots :many
-- One page of the expiry sweep: the live snapshots whose expiry passed by
-- as_of, after the (expires_at, sandbox_id) cursor, oldest first. The
-- expires_at bounds are what idx_snapshots_expires_at serves; the sandbox_id
-- tiebreak keeps rows the sweep skipped from filling every page.
SELECT sandbox_id, team_id, expires_at::timestamptz AS expires_at
FROM public.snapshots
WHERE env_deleted_at IS NULL
    AND expires_at IS NOT NULL
    AND expires_at <= @as_of::timestamptz
    AND expires_at >= @after_expires_at::timestamptz
    AND (expires_at > @after_expires_at::timestamptz OR sandbox_id > @after_sandbox_id::text)
ORDER BY expires_at, sandbox_id
LIMIT @batch_size;
