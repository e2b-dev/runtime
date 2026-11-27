-- name: GetTeamBuildsPage :many
SELECT
  b.id,
  b.status_group,
  b.reason,
  b.created_at,
  b.finished_at,
  b.vcpu,
  b.ram_mb,
  b.total_disk_size_mb,
  b.envd_version,
  eba.env_id AS template_id,
  COALESCE(ea.alias, '') AS template_alias
FROM public.env_builds b
JOIN LATERAL (
  SELECT a.env_id
  FROM public.env_build_assignments a
  -- active_envs (not envs): the build's env must exist and not be soft-deleted
  JOIN public.active_envs e ON e.id = a.env_id
  WHERE a.build_id = b.id
  ORDER BY a.created_at DESC, a.id DESC
  LIMIT 1
) eba ON TRUE
LEFT JOIN LATERAL (
  SELECT x.alias
  FROM public.env_aliases x
  WHERE x.env_id = eba.env_id
  ORDER BY x.alias ASC
  LIMIT 1
) ea ON TRUE
WHERE b.team_id = sqlc.arg(team_id)::uuid
  AND (b.created_at, b.id) < (
    sqlc.arg(cursor_created_at)::timestamptz,
    sqlc.arg(cursor_id)::uuid
  )
  AND b.status_group = ANY(sqlc.arg(statuses)::text[])
ORDER BY b.created_at DESC, b.id DESC
LIMIT sqlc.arg(limit_plus_one)::int;

-- name: GetTeamBuildingBuildsPage :many
SELECT
  b.id,
  b.status_group,
  b.reason,
  b.created_at,
  b.finished_at,
  b.vcpu,
  b.ram_mb,
  b.total_disk_size_mb,
  b.envd_version,
  eba.env_id AS template_id,
  COALESCE(ea.alias, '') AS template_alias
FROM public.env_builds b
JOIN LATERAL (
  SELECT a.env_id
  FROM public.env_build_assignments a
  -- active_envs (not envs): the build's template must exist and not be soft-deleted
  JOIN public.active_envs e ON e.id = a.env_id
  WHERE a.build_id = b.id
  ORDER BY a.created_at DESC, a.id DESC
  LIMIT 1
) eba ON TRUE
LEFT JOIN LATERAL (
  SELECT x.alias
  FROM public.env_aliases x
  WHERE x.env_id = eba.env_id
  ORDER BY x.alias ASC
  LIMIT 1
) ea ON TRUE
WHERE b.team_id = sqlc.arg(team_id)::uuid
  AND (b.created_at, b.id) < (
    sqlc.arg(cursor_created_at)::timestamptz,
    sqlc.arg(cursor_id)::uuid
  )
  -- Keep this list identical to the predicate of idx_env_builds_team_active:
  -- with a parameter here the planner cannot use that partial index.
  AND b.status_group IN ('pending', 'in_progress')
ORDER BY b.created_at DESC, b.id DESC
LIMIT sqlc.arg(limit_plus_one)::int;

-- name: GetTeamBuildsPageByBuildID :many
SELECT
  b.id,
  b.status_group,
  b.reason,
  b.created_at,
  b.finished_at,
  b.vcpu,
  b.ram_mb,
  b.total_disk_size_mb,
  b.envd_version,
  eba.env_id AS template_id,
  COALESCE(ea.alias, '') AS template_alias
FROM public.env_builds b
JOIN LATERAL (
  SELECT a.env_id
  FROM public.env_build_assignments a
  -- active_envs (not envs): the build's env must exist and not be soft-deleted
  JOIN public.active_envs e ON e.id = a.env_id
  WHERE a.build_id = b.id
  ORDER BY a.created_at DESC, a.id DESC
  LIMIT 1
) eba ON TRUE
LEFT JOIN LATERAL (
  SELECT x.alias
  FROM public.env_aliases x
  WHERE x.env_id = eba.env_id
  ORDER BY x.alias ASC
  LIMIT 1
) ea ON TRUE
WHERE b.team_id = sqlc.arg(team_id)::uuid
  AND b.id = sqlc.arg(build_id)::uuid
  AND (b.created_at, b.id) < (
    sqlc.arg(cursor_created_at)::timestamptz,
    sqlc.arg(cursor_id)::uuid
  )
  AND b.status_group = ANY(sqlc.arg(statuses)::text[])
ORDER BY b.created_at DESC, b.id DESC
LIMIT sqlc.arg(limit_plus_one)::int;

-- name: GetTeamBuildsPageByTemplateID :many
SELECT
  b.id,
  b.status_group,
  b.reason,
  b.created_at,
  b.finished_at,
  b.vcpu,
  b.ram_mb,
  b.total_disk_size_mb,
  b.envd_version,
  eba.env_id AS template_id,
  COALESCE(ea.alias, '') AS template_alias
FROM public.env_builds b
JOIN LATERAL (
  SELECT a.env_id
  FROM public.env_build_assignments a
  -- active_envs (not envs): the build's env must exist and not be soft-deleted
  JOIN public.active_envs e ON e.id = a.env_id
  WHERE a.build_id = b.id
    AND a.env_id = sqlc.arg(template_id)::text
  ORDER BY a.created_at DESC, a.id DESC
  LIMIT 1
) eba ON TRUE
LEFT JOIN LATERAL (
  SELECT x.alias
  FROM public.env_aliases x
  WHERE x.env_id = eba.env_id
  ORDER BY x.alias ASC
  LIMIT 1
) ea ON TRUE
WHERE b.team_id = sqlc.arg(team_id)::uuid
  AND (b.created_at, b.id) < (
    sqlc.arg(cursor_created_at)::timestamptz,
    sqlc.arg(cursor_id)::uuid
  )
  AND b.status_group = ANY(sqlc.arg(statuses)::text[])
ORDER BY b.created_at DESC, b.id DESC
LIMIT sqlc.arg(limit_plus_one)::int;

-- name: GetTeamBuildsPageByTemplateAlias :many
SELECT
  b.id,
  b.status_group,
  b.reason,
  b.created_at,
  b.finished_at,
  b.vcpu,
  b.ram_mb,
  b.total_disk_size_mb,
  b.envd_version,
  eba.env_id AS template_id,
  COALESCE(ea.alias, '') AS template_alias
FROM public.env_builds b
JOIN LATERAL (
  SELECT a.env_id
  FROM public.env_build_assignments a
  -- active_envs (not envs): the build's env must exist and not be soft-deleted
  JOIN public.active_envs e ON e.id = a.env_id
  WHERE a.build_id = b.id
    AND EXISTS (
      SELECT 1
      FROM public.env_aliases af
      WHERE af.env_id = a.env_id
        AND af.alias = sqlc.arg(template_alias)::text
    )
  ORDER BY a.created_at DESC, a.id DESC
  LIMIT 1
) eba ON TRUE
LEFT JOIN LATERAL (
  SELECT x.alias
  FROM public.env_aliases x
  WHERE x.env_id = eba.env_id
  ORDER BY x.alias ASC
  LIMIT 1
) ea ON TRUE
WHERE b.team_id = sqlc.arg(team_id)::uuid
  AND (b.created_at, b.id) < (
    sqlc.arg(cursor_created_at)::timestamptz,
    sqlc.arg(cursor_id)::uuid
  )
  AND b.status_group = ANY(sqlc.arg(statuses)::text[])
ORDER BY b.created_at DESC, b.id DESC
LIMIT sqlc.arg(limit_plus_one)::int;
