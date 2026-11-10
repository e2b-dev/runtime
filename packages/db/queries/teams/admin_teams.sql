-- At most one of by_search_id and by_search_text is set. An empty sort_by
-- orders newest first. Teams missing from the running arrays run no sandboxes.
-- name: ListAdminTeams :many
WITH running AS (
    -- Set-returning functions in one select list advance together, pairing
    -- the arrays element by element.
    SELECT
        unnest(sqlc.arg(running_team_ids)::uuid[]) AS team_id,
        unnest(sqlc.arg(running_sandbox_counts)::bigint[]) AS sandbox_count
)
SELECT
    t.id, t.email, t.name, t.is_banned, t.is_blocked, t.created_at,
    (SELECT COUNT(*) FROM public.users_teams ut WHERE ut.team_id = t.id)::bigint AS member_count,
    COALESCE(r.sandbox_count, 0)::bigint AS running_sandbox_count
FROM public.teams t
LEFT JOIN running r ON r.team_id = t.id
WHERE
    (NOT sqlc.arg(by_project_ids)::boolean OR t.id = ANY(sqlc.arg(project_ids)::uuid[]))
    AND CASE
        WHEN sqlc.arg(by_search_id)::boolean THEN t.id = sqlc.arg(search_id)::uuid
        WHEN sqlc.arg(by_search_text)::boolean THEN
            t.email ILIKE '%' || sqlc.arg(search_text)::text || '%'
            OR t.name ILIKE '%' || sqlc.arg(search_text)::text || '%'
            OR t.slug ILIKE '%' || sqlc.arg(search_text)::text || '%'
        ELSE true
    END
    AND (NOT sqlc.arg(hide_banned)::boolean OR NOT t.is_banned)
    AND (NOT sqlc.arg(hide_blocked)::boolean OR NOT t.is_blocked)
    AND (NOT sqlc.arg(by_created_after)::boolean OR t.created_at >= sqlc.arg(created_after)::timestamptz)
    AND (NOT sqlc.arg(by_created_before)::boolean OR t.created_at <= sqlc.arg(created_before)::timestamptz)
ORDER BY
    CASE WHEN sqlc.arg(sort_by)::text = 'name' AND NOT sqlc.arg(descending)::boolean THEN t.name END ASC,
    CASE WHEN sqlc.arg(sort_by)::text = 'name' AND sqlc.arg(descending)::boolean THEN t.name END DESC,
    CASE WHEN sqlc.arg(sort_by)::text = 'email' AND NOT sqlc.arg(descending)::boolean THEN t.email END ASC,
    CASE WHEN sqlc.arg(sort_by)::text = 'email' AND sqlc.arg(descending)::boolean THEN t.email END DESC,
    CASE WHEN sqlc.arg(sort_by)::text = 'createdAt' AND NOT sqlc.arg(descending)::boolean THEN t.created_at END ASC,
    CASE WHEN sqlc.arg(sort_by)::text = 'createdAt' AND sqlc.arg(descending)::boolean THEN t.created_at END DESC,
    CASE WHEN sqlc.arg(sort_by)::text = 'runningSandboxes' AND NOT sqlc.arg(descending)::boolean THEN COALESCE(r.sandbox_count, 0) END ASC,
    CASE WHEN sqlc.arg(sort_by)::text = 'runningSandboxes' AND sqlc.arg(descending)::boolean THEN COALESCE(r.sandbox_count, 0) END DESC,
    t.created_at DESC,
    t.id DESC
LIMIT sqlc.arg(page_size)::bigint OFFSET sqlc.arg(offset_count)::bigint;

-- Takes the same filters as ListAdminTeams.
-- name: CountAdminTeams :one
SELECT COUNT(*)::bigint AS total
FROM public.teams t
WHERE
    (NOT sqlc.arg(by_project_ids)::boolean OR t.id = ANY(sqlc.arg(project_ids)::uuid[]))
    AND CASE
        WHEN sqlc.arg(by_search_id)::boolean THEN t.id = sqlc.arg(search_id)::uuid
        WHEN sqlc.arg(by_search_text)::boolean THEN
            t.email ILIKE '%' || sqlc.arg(search_text)::text || '%'
            OR t.name ILIKE '%' || sqlc.arg(search_text)::text || '%'
            OR t.slug ILIKE '%' || sqlc.arg(search_text)::text || '%'
        ELSE true
    END
    AND (NOT sqlc.arg(hide_banned)::boolean OR NOT t.is_banned)
    AND (NOT sqlc.arg(hide_blocked)::boolean OR NOT t.is_blocked)
    AND (NOT sqlc.arg(by_created_after)::boolean OR t.created_at >= sqlc.arg(created_after)::timestamptz)
    AND (NOT sqlc.arg(by_created_before)::boolean OR t.created_at <= sqlc.arg(created_before)::timestamptz);

-- name: GetAdminTeam :one
SELECT
    t.id, t.email, t.name, t.slug, t.created_at,
    t.is_blocked, t.is_banned, t.blocked_reason,
    t.sandbox_scheduling_labels, t.cluster_id,
    c.name AS cluster_name
FROM public.teams t
LEFT JOIN public.clusters c ON c.id = t.cluster_id
WHERE t.id = sqlc.arg(team_id)::uuid;

-- name: SetTeamSchedulingLabels :execrows
UPDATE public.teams
SET sandbox_scheduling_labels = sqlc.arg(labels)::text[]
WHERE id = sqlc.arg(team_id)::uuid;
