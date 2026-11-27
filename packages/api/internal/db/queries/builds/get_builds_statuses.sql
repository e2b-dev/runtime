-- name: GetBuildsStatusesByTeam :many
SELECT
  b.id,
  b.status_group,
  b.reason,
  b.finished_at
FROM public.env_builds b
WHERE b.team_id = sqlc.arg(team_id)::uuid
  AND b.id = ANY(COALESCE(sqlc.arg(build_ids)::uuid[], ARRAY[]::uuid[]));
