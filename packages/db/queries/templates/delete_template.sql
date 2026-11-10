-- name: SoftDeleteTemplate :one
-- Step 1 of soft delete (run in a tx). The UPDATE locks the env row, so a
-- concurrent build registration holding that lock commits first; this then
-- soft-deletes. Returns the id (no row if already deleted or not owned). The
-- alias/active-build cleanup runs as separate statements afterwards so their
-- fresh snapshots see rows that racing registration committed during the wait.
-- The env's snapshots are stamped in the same statement, after the env row is
-- locked; nothing else keeps snapshots.env_deleted_at in step.
WITH deleted AS (
    UPDATE public.envs e
    SET deleted_at = NOW(), updated_at = NOW()
    WHERE e.id = @template_id AND e.team_id = @team_id AND e.deleted_at IS NULL
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

-- name: ReleaseTemplateAliases :many
-- Releases the env's aliases (so the name is reusable) and returns their cache keys.
DELETE FROM public.env_aliases
WHERE env_id = @template_id
RETURNING (CASE WHEN namespace IS NOT NULL THEN namespace || '/' || alias ELSE alias END)::text AS alias_key;

-- name: DeleteActiveTemplateBuilds :exec
-- Clears in-flight build tracking (active_template_builds no longer cascades
-- since the env row is kept) so a deleted template stops counting toward the
-- team build concurrency limit.
DELETE FROM public.active_template_builds WHERE template_id = @template_id;
