-- A sort column cannot be a bind parameter, so each sortable column gets a pair
-- of ORDER BY terms guarded by sort_by and descending. Only one term in a pair
-- is ever non-NULL; the rest are NULL for every row and tie, leaving the next
-- term to decide. Team counts collide often, so that sort breaks ties on the
-- endpoint first. The trailing c.id keeps paging stable.
-- name: ListAdminClusters :many
SELECT
    c.id, c.name, c.endpoint, c.sandbox_proxy_domain,
    COUNT(t.id)::bigint AS team_count
FROM public.clusters c
LEFT JOIN public.teams t ON t.cluster_id = c.id AND t.deleted_at IS NULL
WHERE
    NOT sqlc.arg(by_search)::boolean
    OR c.name ILIKE '%' || sqlc.arg(search)::text || '%'
    OR c.endpoint ILIKE '%' || sqlc.arg(search)::text || '%'
GROUP BY c.id
ORDER BY
    CASE WHEN sqlc.arg(sort_by)::text = 'name' AND NOT sqlc.arg(descending)::boolean THEN c.name END ASC,
    CASE WHEN sqlc.arg(sort_by)::text = 'name' AND sqlc.arg(descending)::boolean THEN c.name END DESC,
    CASE WHEN sqlc.arg(sort_by)::text = 'teamCount' AND NOT sqlc.arg(descending)::boolean THEN COUNT(t.id) END ASC,
    CASE WHEN sqlc.arg(sort_by)::text = 'teamCount' AND sqlc.arg(descending)::boolean THEN COUNT(t.id) END DESC,
    CASE WHEN sqlc.arg(sort_by)::text = 'endpoint' AND NOT sqlc.arg(descending)::boolean THEN c.endpoint END ASC,
    CASE WHEN sqlc.arg(sort_by)::text = 'endpoint' AND sqlc.arg(descending)::boolean THEN c.endpoint END DESC,
    CASE WHEN sqlc.arg(sort_by)::text = 'teamCount' THEN c.endpoint END ASC,
    c.id ASC
LIMIT sqlc.arg(page_size)::bigint OFFSET sqlc.arg(offset_count)::bigint;

-- Takes the same filter as ListAdminClusters.
-- name: CountAdminClusters :one
SELECT COUNT(*)::bigint AS total
FROM public.clusters c
WHERE
    NOT sqlc.arg(by_search)::boolean
    OR c.name ILIKE '%' || sqlc.arg(search)::text || '%'
    OR c.endpoint ILIKE '%' || sqlc.arg(search)::text || '%';

-- The token is left out on purpose: no admin read returns it.
-- name: GetAdminCluster :one
SELECT id, name, endpoint, endpoint_tls, sandbox_proxy_domain, auth_org_id, deletion_protection
FROM public.clusters
WHERE id = sqlc.arg(cluster_id)::uuid;

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

-- name: CountAdminClusterTemplates :one
SELECT COUNT(*)::bigint AS total
FROM public.envs
WHERE cluster_id = sqlc.arg(cluster_id)::uuid
  AND deleted_at IS NULL;

-- Writes only the columns whose set_ flag is true. No row means no cluster.
-- name: UpdateAdminCluster :one
UPDATE public.clusters
SET
    name = CASE WHEN sqlc.arg(set_name)::boolean THEN sqlc.arg(name)::text ELSE name END,
    endpoint = CASE WHEN sqlc.arg(set_endpoint)::boolean THEN sqlc.arg(endpoint)::text ELSE endpoint END,
    endpoint_tls = CASE WHEN sqlc.arg(set_endpoint_tls)::boolean THEN sqlc.arg(endpoint_tls)::boolean ELSE endpoint_tls END,
    sandbox_proxy_domain = CASE WHEN sqlc.arg(set_sandbox_proxy_domain)::boolean THEN sqlc.narg(sandbox_proxy_domain)::text ELSE sandbox_proxy_domain END,
    auth_org_id = CASE WHEN sqlc.arg(set_auth_org_id)::boolean THEN sqlc.narg(auth_org_id)::text ELSE auth_org_id END,
    deletion_protection = CASE WHEN sqlc.arg(set_deletion_protection)::boolean THEN sqlc.arg(deletion_protection)::boolean ELSE deletion_protection END
WHERE id = sqlc.arg(cluster_id)::uuid
RETURNING id, name, endpoint, endpoint_tls, sandbox_proxy_domain, auth_org_id, deletion_protection;

-- name: UpdateAdminClusterToken :execrows
UPDATE public.clusters
SET token = sqlc.arg(token)::text
WHERE id = sqlc.arg(cluster_id)::uuid;
