-- The cluster records themselves come from the cluster service; this counts
-- each cluster's live teams.
-- name: CountLiveTeamsByCluster :many
SELECT cluster_id::uuid AS cluster_id, COUNT(*)::bigint AS team_count
FROM public.teams
WHERE cluster_id IS NOT NULL
  AND deleted_at IS NULL
GROUP BY cluster_id;

-- name: ListAdminClusterTeams :many
SELECT t.id, t.email, t.name, t.slug, t.is_banned, t.created_at
FROM public.teams t
WHERE t.cluster_id = sqlc.arg(cluster_id)::uuid
  AND t.deleted_at IS NULL
ORDER BY
    CASE WHEN sqlc.arg(sort_by)::text = 'name' AND NOT sqlc.arg(descending)::boolean THEN t.name END ASC,
    CASE WHEN sqlc.arg(sort_by)::text = 'name' AND sqlc.arg(descending)::boolean THEN t.name END DESC,
    CASE WHEN sqlc.arg(sort_by)::text = 'createdAt' AND NOT sqlc.arg(descending)::boolean THEN t.created_at END ASC,
    CASE WHEN sqlc.arg(sort_by)::text = 'createdAt' AND sqlc.arg(descending)::boolean THEN t.created_at END DESC,
    t.id ASC
LIMIT sqlc.arg(page_size)::bigint OFFSET sqlc.arg(offset_count)::bigint;

-- name: CountAdminClusterTeams :one
SELECT COUNT(*)::bigint AS total
FROM public.teams
WHERE cluster_id = sqlc.arg(cluster_id)::uuid
  AND deleted_at IS NULL;
