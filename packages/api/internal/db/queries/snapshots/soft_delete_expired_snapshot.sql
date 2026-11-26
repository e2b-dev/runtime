-- name: SoftDeleteExpiredSnapshot :one
-- SoftDeleteTemplate for the expiry sweep: soft-deletes the sandbox's snapshot
-- env only while its expiry has still passed, since a run that ended after the
-- sweep listed it restarted the clock. The caller holds the sandbox's kill
-- claim, so no run can start and move expires_at while this reads it.
-- Run in a tx, followed by ReleaseTemplateAliases and DeleteActiveTemplateBuilds.
WITH deleted AS (
    UPDATE public.envs e
    SET deleted_at = NOW(), updated_at = NOW()
    WHERE e.team_id = @team_id
        AND e.deleted_at IS NULL
        AND e.id = (
            SELECT s.env_id FROM public.snapshots s
            WHERE s.sandbox_id = @sandbox_id
                AND s.team_id = @team_id
                AND s.env_deleted_at IS NULL
                AND s.expires_at <= now()
        )
    RETURNING e.id
),
-- The array keeps this an env_id index lookup, as in SoftDeleteTemplate.
stamped AS (
    UPDATE public.snapshots s
    SET env_deleted_at = NOW()
    WHERE s.env_id = ANY (ARRAY(SELECT id FROM deleted))
)
SELECT id FROM deleted;
