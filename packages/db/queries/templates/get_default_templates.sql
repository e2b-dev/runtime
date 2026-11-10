-- name: GetDefaultTemplates :many
SELECT
    e.id AS template_id,
    e.created_at,
    e.public,
    e.build_count,
    e.spawn_count,
    b.id AS build_id,
    b.ram_mb,
    b.vcpu,
    b.total_disk_size_mb,
    b.envd_version
FROM public.env_defaults ed
JOIN public.active_envs e ON e.id = ed.env_id
JOIN LATERAL (
    SELECT a.build_id
    FROM public.env_build_assignments a
    JOIN public.env_builds eb ON eb.id = a.build_id
    WHERE a.env_id = e.id
      AND eb.status = 'uploaded'
    ORDER BY a.created_at DESC, a.id DESC
    LIMIT 1
) latest_assignment ON TRUE
JOIN public.env_builds b ON b.id = latest_assignment.build_id;
