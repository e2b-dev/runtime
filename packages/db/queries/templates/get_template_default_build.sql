-- name: GetTemplateDefaultBuild :one
-- The build and default-template columns of a template list row, for one
-- template: the newest build tagged default in any state gives build_status,
-- and the newest ready build tagged default gives the resources, zero when
-- there is none. The LEFT JOINs inside the laterals are for sqlc, which does
-- not carry a LEFT JOIN LATERAL's nullability to the columns.
SELECT
    COALESCE(eb.id, '00000000-0000-0000-0000-000000000000'::uuid)::uuid AS build_id,
    COALESCE(eb.vcpu, 0)::bigint AS build_vcpu,
    eb.max_vcpu AS build_max_vcpu,
    COALESCE(eb.ram_mb, 0)::bigint AS build_ram_mb,
    eb.total_disk_size_mb AS build_total_disk_size_mb,
    eb.envd_version AS build_envd_version,
    COALESCE(latest_build.status_group, 'pending') AS build_status,
    (d.env_id IS NOT NULL)::boolean AS is_default,
    d.description AS default_description
FROM (SELECT sqlc.arg(template_id)::text AS id) AS e
LEFT JOIN public.env_defaults AS d ON d.env_id = e.id
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
) eb ON TRUE;
