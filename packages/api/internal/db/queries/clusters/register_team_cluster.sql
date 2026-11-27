-- name: ListClusterTeamIDs :many
SELECT id
FROM public.teams
WHERE cluster_id = sqlc.arg(cluster_id)::uuid
  AND deleted_at IS NULL
ORDER BY id;

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
-- still holds it. Clearing it leaves no row pointing at a deleted cluster.
UPDATE public.teams
SET cluster_id = NULL
WHERE cluster_id = sqlc.arg(cluster_id)::uuid
  AND deleted_at IS NOT NULL;
