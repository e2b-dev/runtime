-- name: GetLastSnapshot :one
SELECT COALESCE(ea.aliases, ARRAY[]::text[])::text[] AS aliases, COALESCE(ea.names, ARRAY[]::text[])::text[] AS names, sqlc.embed(s), sqlc.embed(eb)
FROM "public"."snapshots" s
JOIN "public"."active_envs" e ON e.id = s.env_id
JOIN LATERAL (
    SELECT eba.build_id
    FROM "public"."env_build_assignments" eba
    JOIN "public"."env_builds" eb_inner ON eb_inner.id = eba.build_id AND eb_inner.status_group = 'ready'
    WHERE eba.env_id = s.env_id AND eba.tag = 'default'
    ORDER BY eba.created_at DESC
    LIMIT 1
) latest_eba ON TRUE
JOIN "public"."env_builds" eb ON eb.id = latest_eba.build_id
LEFT JOIN LATERAL (
    SELECT
        ARRAY_AGG(alias ORDER BY alias) AS aliases,
        ARRAY_AGG(CASE WHEN namespace IS NOT NULL THEN namespace || '/' || alias ELSE alias END ORDER BY alias) AS names
    FROM "public"."env_aliases"
    WHERE env_id = s.base_env_id
) ea ON TRUE
WHERE s.sandbox_id = $1;
