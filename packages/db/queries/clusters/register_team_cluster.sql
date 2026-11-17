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

-- name: LockTeamCluster :one
SELECT cluster_id
FROM public.teams
WHERE id = sqlc.arg(team_id)::uuid
FOR UPDATE;

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

-- name: LockTeamClusterSync :exec
-- Serializes the syncs of one team's cluster to the cluster service, so each
-- reads the team only after the sync before it has finished.
SELECT pg_advisory_xact_lock(hashtextextended('team cluster sync ' || sqlc.arg(team_id)::uuid::text, 0));

-- name: GetLiveTeamCluster :one
-- A deleted team holds no cluster, whatever its row still says.
SELECT cluster_id
FROM public.teams
WHERE id = sqlc.arg(team_id)::uuid
  AND deleted_at IS NULL;

-- name: ListLiveTeamClusters :many
-- A page of the live teams on a remote cluster, after the given team.
SELECT id, cluster_id::uuid AS cluster_id
FROM public.teams
WHERE cluster_id IS NOT NULL
  AND deleted_at IS NULL
  AND id > sqlc.arg(after)::uuid
ORDER BY id
LIMIT sqlc.arg(page_size)::int;
