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
-- template, snapshot template and paused sandbox snapshot on the cluster. The
-- UPDATE waits on rows a concurrent build registration has locked, so the
-- alias and active-build cleanup run as separate statements afterwards: their
-- fresh snapshots see rows that registration committed during the wait.
UPDATE public.envs
SET deleted_at = NOW(), updated_at = NOW()
WHERE cluster_id = sqlc.arg(cluster_id)::uuid
  AND deleted_at IS NULL
RETURNING id;

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

-- name: DeleteCluster :execrows
DELETE FROM public.clusters
WHERE id = sqlc.arg(cluster_id)::uuid;

-- name: TeamClusterAssignment :one
SELECT cluster_id
FROM public.teams
WHERE id = sqlc.arg(team_id)::uuid
  AND cluster_id IS NOT NULL;

-- name: AssignTeamCluster :one
WITH locked_team AS MATERIALIZED (
    SELECT cluster_id,
           POSITION('enterprise' IN LOWER(tier)) > 0 AS enterprise_eligible
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
          locked_team.cluster_id IS NOT DISTINCT FROM sqlc.arg(cluster_id)::uuid
          OR (
              locked_team.enterprise_eligible
              AND (
                  NOT sqlc.arg(preserve_existing)::boolean
                  OR locked_team.cluster_id IS NULL
              )
          )
      )
    RETURNING TRUE
)
SELECT locked_team.cluster_id,
       EXISTS (
           SELECT
           FROM locked_team AS eligible_team
           WHERE eligible_team.enterprise_eligible
              OR eligible_team.cluster_id IS NOT DISTINCT FROM sqlc.arg(cluster_id)::uuid
       ) AS assignment_eligible,
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
