-- name: UpdateEnvBuildStatus :exec
UPDATE "public"."env_builds"
SET status_group = @status_group,
    finished_at = @finished_at,
    reason = sqlc.narg(reason),
    version = @version
WHERE id = @build_id;

-- name: FailTemplateBuildAndDeactivate :one
-- Several pollers watch the same build, and it keeps the first terminal outcome
-- recorded. A write that loses must not release the team's concurrency slot
-- either, so the deactivation reads from the update instead of running on its
-- own.
WITH failed AS (
    UPDATE "public"."env_builds"
    SET status_group = @status_group,
        finished_at = @finished_at,
        reason = sqlc.narg(reason),
        version = @version
    WHERE id = @build_id
      AND status_group NOT IN ('ready', 'failed')
    RETURNING id
), deactivated AS (
    DELETE FROM public.active_template_builds
    WHERE build_id IN (SELECT id FROM failed)
)
SELECT EXISTS (SELECT 1 FROM failed) AS recorded;
