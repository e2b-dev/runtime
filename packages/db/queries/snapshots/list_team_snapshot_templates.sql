-- name: ListTeamSnapshotTemplates :many
-- Lists all persistent snapshot templates for a team with cursor-based pagination.
-- Snapshot templates are envs with source='snapshot_template'.
SELECT 
    e.id as snapshot_id,
    st.sandbox_id,
    e.created_at,
    e.team_id,
    eb.id as build_id,
    eb.vcpu,
    eb.max_vcpu,
    eb.ram_mb,
    eb.total_disk_size_mb,
    eb.envd_version,
    eb.kernel_version,
    eb.firecracker_version,
    eb.cluster_node_id,
    eb.tag,
    COALESCE(ea.names, ARRAY[]::text[])::text[] AS names
FROM "public"."active_envs" e
JOIN "public"."snapshot_templates" st ON st.env_id = e.id
JOIN LATERAL (
    -- When a tag filter is set, pick the newest build with that tag; the inner
    -- join then also drops snapshots with no successful build for the tag.
    SELECT b.*, ba.tag
    FROM "public"."env_build_assignments" ba
    JOIN "public"."env_builds" b ON b.id = ba.build_id
    WHERE ba.env_id = e.id
      AND b.status_group = 'ready'
      AND (sqlc.narg(tag)::text IS NULL OR ba.tag = sqlc.narg(tag)::text)
    ORDER BY ba.created_at DESC
    LIMIT 1
) eb ON TRUE
LEFT JOIN LATERAL (
    -- ORDER BY is required: Names[0] is used as the snapshot identifier in the API response.
    SELECT ARRAY_AGG(CASE WHEN namespace IS NOT NULL THEN namespace || '/' || alias ELSE alias END ORDER BY namespace NULLS LAST, alias) AS names
    FROM "public"."env_aliases"
    WHERE env_id = e.id
) ea ON TRUE
WHERE e.team_id = @team_id
AND e.source = 'snapshot_template'
AND (
    sqlc.narg(sandbox_id)::text IS NULL
    OR st.sandbox_id = sqlc.narg(sandbox_id)::text
)
AND (
    sqlc.narg(env_id)::text IS NULL
    OR e.id = sqlc.narg(env_id)::text
)
AND (e.created_at, e.id) < (@cursor_time, @cursor_id::text)
ORDER BY e.created_at DESC, e.id DESC
LIMIT @page_limit;
