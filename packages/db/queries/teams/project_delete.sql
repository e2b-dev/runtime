-- Project deletion, run in one transaction. The row lock serializes a retried
-- deletion behind the one in flight.
-- name: LockProjectForDeletion :one
SELECT id
FROM public.teams
WHERE id = sqlc.arg(project_id)::uuid
FOR UPDATE;

-- The row stays for history. Blocking it is what stops workloads, and the
-- cluster assignment goes so the project no longer holds a cluster. Name and
-- email are scrubbed, and the slug is released for reuse: it becomes
-- 'deleted-' and the project ID, which is unique by construction, fits the
-- 63-character slug limit, and does not depend on the old slug, so repeating
-- it never stacks the prefix. Runs on every deletion, so a retry also undoes a
-- block reason, cluster assignment or rename that a write racing the first
-- deletion left behind; the first deletion time is kept.
-- name: MarkProjectDeleted :exec
UPDATE public.teams
SET deleted_at = COALESCE(deleted_at, now()),
    is_blocked = true,
    blocked_reason = sqlc.arg(blocked_reason)::text,
    cluster_id = NULL,
    name = 'DELETED',
    email = 'DELETED',
    slug = 'deleted-' || id::text
WHERE id = sqlc.arg(project_id)::uuid;

-- Soft-deletes the project's templates, snapshot templates and paused sandbox
-- snapshots the way a template deletion does, stamping their snapshots in the
-- same statement.
-- name: SoftDeleteProjectEnvironments :many
WITH deleted AS (
    UPDATE public.envs e
    SET deleted_at = NOW(), updated_at = NOW()
    WHERE e.team_id = sqlc.arg(project_id)::uuid
      AND e.deleted_at IS NULL
    RETURNING e.id
),
-- NOW() is the transaction's start time, the deleted_at just written. The
-- array keeps this an env_id index lookup per env: joining the CTE lets the
-- planner hash-join a sequential scan of every snapshot once it expects
-- many envs.
stamped AS (
    UPDATE public.snapshots s
    SET env_deleted_at = NOW()
    WHERE s.env_id = ANY (ARRAY(SELECT id FROM deleted))
)
SELECT id FROM deleted;

-- name: DeleteProjectMembers :exec
DELETE FROM public.users_teams
WHERE team_id = sqlc.arg(project_id)::uuid;

-- Returns the deleted keys' hashes so their cached auth entries can be evicted.
-- name: DeleteProjectAPIKeys :many
DELETE FROM public.team_api_keys
WHERE team_id = sqlc.arg(project_id)::uuid
RETURNING api_key_hash;
