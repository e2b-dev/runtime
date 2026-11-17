-- name: CheckTeamTemplateExists :one
-- Whether the team owns the template and it is not deleted.
SELECT EXISTS (
    SELECT 1
    FROM public.active_envs AS e
    WHERE e.id = sqlc.arg(template_id)
      AND e.team_id = sqlc.arg(team_id)
      AND e.source IN ('template', 'snapshot_template')
)::boolean AS exists;
