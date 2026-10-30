-- name: CreateCluster :one
-- deletion_protection is set only on a new row. Registering an existing cluster
-- again leaves its stored protection unchanged.
INSERT INTO public.clusters (
    id,
    name,
    endpoint,
    endpoint_tls,
    token,
    sandbox_proxy_domain,
    auth_org_id,
    deletion_protection
)
VALUES (
    COALESCE(sqlc.narg(cluster_id)::uuid, gen_random_uuid()),
    sqlc.arg(name)::text,
    sqlc.arg(endpoint)::text,
    sqlc.arg(endpoint_tls)::boolean,
    sqlc.arg(token)::text,
    sqlc.narg(sandbox_proxy_domain)::text,
    sqlc.narg(auth_org_id)::text,
    sqlc.arg(deletion_protection)::boolean
)
ON CONFLICT (id) DO UPDATE
SET id = EXCLUDED.id
WHERE clusters.name = EXCLUDED.name
  AND clusters.endpoint = EXCLUDED.endpoint
  AND clusters.endpoint_tls = EXCLUDED.endpoint_tls
  AND clusters.token = EXCLUDED.token
  AND clusters.sandbox_proxy_domain IS NOT DISTINCT FROM EXCLUDED.sandbox_proxy_domain
  AND clusters.auth_org_id IS NOT DISTINCT FROM EXCLUDED.auth_org_id
RETURNING id;

-- name: RegisterManagedCluster :one
-- Management registration of an existing cluster with the same name and auth
-- organization updates its connection details and credential, so a cluster
-- that moves to a new domain keeps its identity. Assignments and stored
-- deletion protection survive.
INSERT INTO public.clusters (
    id,
    name,
    endpoint,
    endpoint_tls,
    token,
    sandbox_proxy_domain,
    auth_org_id,
    deletion_protection
)
VALUES (
    COALESCE(sqlc.narg(cluster_id)::uuid, gen_random_uuid()),
    sqlc.arg(name)::text,
    sqlc.arg(endpoint)::text,
    sqlc.arg(endpoint_tls)::boolean,
    sqlc.arg(token)::text,
    sqlc.narg(sandbox_proxy_domain)::text,
    sqlc.narg(auth_org_id)::text,
    sqlc.arg(deletion_protection)::boolean
)
ON CONFLICT (id) DO UPDATE
SET token = EXCLUDED.token,
    endpoint = EXCLUDED.endpoint,
    endpoint_tls = EXCLUDED.endpoint_tls,
    sandbox_proxy_domain = EXCLUDED.sandbox_proxy_domain
WHERE clusters.name = EXCLUDED.name
  AND clusters.auth_org_id IS NOT DISTINCT FROM EXCLUDED.auth_org_id
RETURNING id;

-- name: LockClusterDeletionProtection :one
-- Locks the row for the rest of the transaction, so a concurrent protection
-- change commits before or after the caller's check, never between.
SELECT deletion_protection
FROM public.clusters
WHERE id = sqlc.arg(cluster_id)::uuid
FOR UPDATE;

-- name: RequestClusterDeletion :exec
UPDATE public.clusters
SET deletion_requested_at = COALESCE(deletion_requested_at, now())
WHERE id = sqlc.arg(cluster_id)::uuid;

-- name: ListClusterTeamIDs :many
SELECT id
FROM public.teams
WHERE cluster_id = sqlc.arg(cluster_id)::uuid
  AND deleted_at IS NULL
ORDER BY id;

-- name: ClusterDeletionProtected :one
SELECT EXISTS (
    SELECT FROM public.clusters
    WHERE id = sqlc.arg(cluster_id)::uuid
      AND deletion_protection
);

-- name: ClusterUsedByMultipleTeams :one
-- True when more than one team is assigned to the cluster or owns live
-- environments on it. One team's own environments do not count: deleting the
-- cluster soft-deletes them.
SELECT COUNT(DISTINCT team_id) > 1
FROM (
    SELECT id AS team_id FROM public.teams WHERE cluster_id = sqlc.arg(cluster_id)::uuid
    UNION
    SELECT team_id FROM public.active_envs WHERE cluster_id = sqlc.arg(cluster_id)::uuid
) AS cluster_teams;

-- name: SoftDeleteClusterEnvironments :many
-- Step 1 of cluster environment cleanup (run in a tx): soft-deletes every live
-- template, snapshot template and paused sandbox snapshot on the cluster, and
-- stamps their snapshots in the same statement. The UPDATE waits on rows a
-- concurrent build registration has locked, so the alias and active-build
-- cleanup run as separate statements afterwards: their fresh snapshots see
-- rows that registration committed during the wait.
WITH deleted AS (
    UPDATE public.envs e
    SET deleted_at = NOW(), updated_at = NOW()
    WHERE e.cluster_id = sqlc.arg(cluster_id)::uuid
      AND e.deleted_at IS NULL
    RETURNING e.id
),
-- NOW() is the transaction's start time, the deleted_at just written. The
-- array keeps this an env_id index lookup per env: joining the CTE lets the
-- planner hash-join a sequential scan of every snapshot once it expects
-- many envs.
stamped AS (
    UPDATE public.snapshots s
    SET env_deleted_at = NOW()
    WHERE s.env_id = ANY (ARRAY(SELECT id FROM deleted))
)
SELECT id FROM deleted;

-- name: ReleaseEnvironmentsAliases :exec
DELETE FROM public.env_aliases
WHERE env_id = ANY(sqlc.arg(env_ids)::text[]);

-- name: DeleteEnvironmentsActiveBuilds :exec
DELETE FROM public.active_template_builds
WHERE template_id = ANY(sqlc.arg(env_ids)::text[]);

-- name: DetachDeletedTemplatesFromCluster :exec
UPDATE public.envs
SET cluster_id = NULL
WHERE cluster_id = sqlc.arg(cluster_id)::uuid
  AND deleted_at IS NOT NULL;

-- name: DetachDeletedTeamsFromCluster :exec
-- Deleting a project clears its team's cluster, but a team deleted before that
-- still holds it and would block the cluster delete.
UPDATE public.teams
SET cluster_id = NULL
WHERE cluster_id = sqlc.arg(cluster_id)::uuid
  AND deleted_at IS NOT NULL;

-- name: DeleteCluster :execrows
DELETE FROM public.clusters
WHERE id = sqlc.arg(cluster_id)::uuid;

-- name: AssignTeamCluster :one
WITH locked_team AS MATERIALIZED (
    SELECT cluster_id
    FROM public.teams
    WHERE id = sqlc.arg(team_id)::uuid
    FOR UPDATE
),
assigned AS (
    UPDATE public.teams AS team
    SET cluster_id = sqlc.arg(cluster_id)::uuid
    FROM locked_team
    WHERE team.id = sqlc.arg(team_id)::uuid
      AND (
          NOT sqlc.arg(preserve_existing)::boolean
          OR locked_team.cluster_id IS NULL
          OR locked_team.cluster_id = sqlc.arg(cluster_id)::uuid
      )
    RETURNING TRUE
)
SELECT locked_team.cluster_id,
       EXISTS (SELECT FROM assigned) AS assigned
FROM locked_team;

-- name: DetachTeamCluster :one
WITH locked_team AS MATERIALIZED (
    SELECT cluster_id
    FROM public.teams
    WHERE id = sqlc.arg(team_id)::uuid
    FOR UPDATE
),
detached AS (
    UPDATE public.teams AS team
    SET cluster_id = NULL
    FROM locked_team
    WHERE team.id = sqlc.arg(team_id)::uuid
      AND locked_team.cluster_id = sqlc.arg(cluster_id)::uuid
    RETURNING TRUE
)
SELECT locked_team.cluster_id,
       EXISTS (SELECT FROM detached) AS detached
FROM locked_team;
