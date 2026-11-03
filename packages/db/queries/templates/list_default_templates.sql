-- name: ListDefaultTemplates :many
-- Lists the default templates (env_defaults) that are not deleted, ordered by
-- template ID. The newest build tagged default in any state gives
-- build_status; the newest ready build tagged default gives the resources a
-- sandbox started from the template runs, zero when there is none. The LEFT
-- JOINs inside the laterals are for sqlc, which does not carry a LEFT JOIN
-- LATERAL's nullability to the columns.
SELECT
    e.id AS template_id,
    e.team_id,
    e.created_at,
    e.updated_at,
    e.public,
    e.build_count,
    e.spawn_count,
    e.last_spawned_at,
    e.created_by AS creator_id,
    TRUE::boolean AS is_default,
    COALESCE(eb.id, '00000000-0000-0000-0000-000000000000'::uuid)::uuid AS build_id,
    COALESCE(eb.vcpu, 0)::bigint AS build_vcpu,
    eb.max_vcpu AS build_max_vcpu,
    COALESCE(eb.ram_mb, 0)::bigint AS build_ram_mb,
    eb.total_disk_size_mb AS build_total_disk_size_mb,
    eb.envd_version AS build_envd_version,
    COALESCE(latest_build.status_group, 'pending') AS build_status,
    COALESCE(ea.aliases, ARRAY[]::text[])::text[] AS aliases,
    COALESCE(ea.names, ARRAY[]::text[])::text[] AS names,
    d.description AS default_description
FROM public.env_defaults AS d
JOIN public.active_envs AS e ON e.id = d.env_id
LEFT JOIN LATERAL (
    SELECT
        ARRAY_AGG(alias ORDER BY alias) AS aliases,
        ARRAY_AGG(CASE WHEN namespace IS NOT NULL THEN namespace || '/' || alias ELSE alias END ORDER BY alias) AS names
    FROM public.env_aliases
    WHERE env_id = e.id
) ea ON TRUE
LEFT JOIN LATERAL (
    SELECT b.status_group
    FROM public.env_build_assignments AS ba
    LEFT JOIN public.env_builds AS b ON b.id = ba.build_id
    WHERE ba.env_id = e.id AND ba.tag = 'default'
    ORDER BY ba.created_at DESC
    LIMIT 1
) latest_build ON TRUE
LEFT JOIN LATERAL (
    SELECT b.id, b.vcpu, b.max_vcpu, b.ram_mb, b.total_disk_size_mb, b.envd_version
    FROM public.env_build_assignments AS ba
    LEFT JOIN public.env_builds AS b ON b.id = ba.build_id
    WHERE ba.env_id = e.id AND ba.tag = 'default' AND b.status_group = 'ready'
    ORDER BY ba.created_at DESC
    LIMIT 1
) eb ON TRUE
ORDER BY e.id;
