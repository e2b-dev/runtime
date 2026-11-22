-- name: GetBuildInfoByTeamAndBuildID :one
SELECT
  b.created_at,
  b.finished_at,
  b.status_group,
  b.reason,
  COALESCE(ea.names, ARRAY[]::text[])::text[] AS names
FROM public.env_builds b
JOIN LATERAL (
  SELECT a.env_id
  FROM public.env_build_assignments a
  -- active_envs (not envs): the build's env must exist and not be soft-deleted
  JOIN public.active_envs e ON e.id = a.env_id
  WHERE a.build_id = b.id
    AND e.team_id = sqlc.arg(team_id)::uuid
  ORDER BY a.created_at DESC, a.id DESC
  LIMIT 1
) assignment ON TRUE
LEFT JOIN LATERAL (
  SELECT ARRAY_AGG(
    CASE
      WHEN namespace IS NOT NULL THEN namespace || '/' || alias
      ELSE alias
    END
    ORDER BY alias
  ) AS names
  FROM public.env_aliases
  WHERE env_id = assignment.env_id
) ea ON TRUE
WHERE b.id = sqlc.arg(build_id)::uuid;
