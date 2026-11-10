-- name: GetTeamTemplates :many
SELECT sqlc.embed(e),
       COALESCE(eb.id, '00000000-0000-0000-0000-000000000000'::uuid) as build_id,
       COALESCE(eb.vcpu, 0) as build_vcpu,
       eb.max_vcpu as build_max_vcpu,
       COALESCE(eb.ram_mb, 0) as build_ram_mb,
       eb.total_disk_size_mb as build_total_disk_size_mb,
       eb.envd_version as build_envd_version,
       COALESCE(eb.firecracker_version, 'N/A') as build_firecracker_version,
       COALESCE(latest_build.status_group, 'pending') as build_status,
       e.created_by as creator_id,
       COALESCE(ea.aliases, ARRAY[]::text[])::text[] AS aliases,
       COALESCE(ea.names, ARRAY[]::text[])::text[] AS names
FROM public.active_envs AS e
LEFT JOIN LATERAL (
    SELECT 
        ARRAY_AGG(alias ORDER BY alias) AS aliases,
        ARRAY_AGG(CASE WHEN namespace IS NOT NULL THEN namespace || '/' || alias ELSE alias END ORDER BY alias) AS names
    FROM public.env_aliases
    WHERE env_id = e.id
) ea ON TRUE
LEFT JOIN LATERAL (
    SELECT b.*
    FROM public.env_build_assignments AS ba
    JOIN public.env_builds AS b ON b.id = ba.build_id
    WHERE ba.env_id = e.id AND ba.tag = 'default'
    ORDER BY ba.created_at DESC
    LIMIT 1
) latest_build ON TRUE
LEFT JOIN LATERAL (
    SELECT b.*
    FROM public.env_build_assignments AS ba
    JOIN public.env_builds AS b ON b.id = ba.build_id
    WHERE ba.env_id = e.id AND ba.tag = 'default' AND b.status_group = 'ready'
    ORDER BY ba.created_at DESC
    LIMIT 1
) eb ON TRUE
WHERE
  e.team_id = sqlc.arg(team_id)
  AND e.source = 'template'
ORDER BY e.created_at ASC
LIMIT sqlc.narg(limit_plus_one)::int;
